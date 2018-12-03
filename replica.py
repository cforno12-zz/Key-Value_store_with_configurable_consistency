import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct
import time
import threading

'''

    TODO:

        1.) Restructure to utilize threads
        2.) Handle disconnects and keep track of active replicas
        3.) Test "get" consistency levels
        4.) Read-repair
        5.) Hinted-handoff

'''

class Replica:
    def __init__(self, replica_num, server_socket, mech):
        self.storeLock = Lock()
        self.logName = "writeAhead" + replica_num + ".txt"
        self.coordinator = -1            #Determines whether to wait or send
        self.clientSocket = server_socket
        self.replica_num = replica_num
        self.hostName = ""
        self.portNum = 0
        self.keyValStore = {} #key => val:time_stamp
        self.replicaList = [None] * 4       #Store IP/Port of all other replicas
        self.neighborSockets = [None] * 4
        self.const_mech = mech # 0 => read repair -- 1 => hinted handoff

    def parseReplicaFile(self):

        file = open("replicas.txt", "r")

        for line in file:

            splitLines = line.split(" ")

            if splitLines[0] == self.replica_num:

                self.hostName = splitLines[1]
                self.portNum = int(splitLines[2])

                index = int(self.replica_num)
                self.replicaList[index] = (self.hostName, self.portNum)

            else:

                index = int(splitLines[0])
                self.replicaList[index] = (splitLines[1], int(splitLines[2]))

        file.close()

    #Determines the first replica in the cluster
    def bytePartitioner(self, key):

        firstRep = -1

        if key in range(64):
            firstRep = 0
        elif key in range(64, 128):
            firstRep = 1
        elif key in range(128, 192):
            firstRep = 2
        elif key in range(192, 256):
            firstRep = 3
        else:
            print("Key not in range")
            sys.exit(1)

        return firstRep

    def get(self, key, level, client_socket):

        if key in self.keyValStore:
            val_to_send = self.keyValStore[key].split(":")
            val_to_send = val_to_send[0]
            time_stamp = val_to_send[1]
        else:
            #we contact the next replica in the ring
            sock_to_contact = self.neighborSockets[(self.replica_num + 1) % 4]
            msg_to_send = store.Msg()
            msg_to_send.get.key = key
            sock_to_contact.sendall(msg_to_send.SerializeToString())
            val_to_send = sock_to_contact.recv(1024)
            #return null
            print("No value associated with key: " + key)

        msg_to_send = store.Msg()
        msg_to_send.string_val.val = val_to_send
        client_socket.sendall(msg_to_send.SerializeToString())

    #Put a value into the key/val store
    def put(self, key, val, level, client_socket):

        #Determine how many replicas are needed before returning to client
        replicasForConsistency = level + 1

        #First replica determined by the byte partitioner
        firstReplica = self.bytePartitioner(key)

        #Second and third are clockwise in the cluster
        secondReplica = (firstReplica + 1) % 4
        thirdReplica = (firstReplica + 2) % 4

        operationReplicas = [firstReplica, secondReplica, thirdReplica]

        print("I will be storing this data on")
        print(str(firstReplica) + " " + str(secondReplica) + " " + str(thirdReplica))

        #Amount of replicas that have successfully completed the request
        successfulPuts = 0

        #Loop through all four replcias
        for i in range(4):

            #Send to only those replicas desired by coordinator
            if(i in operationReplicas):

                #Create message to send to replicas
                msg = store.Msg()
                msg.pair.key = key
                msg.pair.val = val

                index = operationReplicas.index(i)

                #This replica is required to store the info
                if(operationReplicas[index] == int(self.replica_num)):

                    #Write to log before adding to local memory

                    self.storeLock.acquire()

                    writeLogInfo = open(self.logName, "a")
                    writeLogInfo.write(str(key) + ":" + val + ":" + str(time.time()) +"\n")
                    writeLogInfo.close()

                    self.keyValStore[key] = val;

                    self.storeLock.release()

                    #"Put" was successful, return to client if consistency level is met
                    if key in self.keyValStore:

                        print("Added: " + val + " to location: " + str(key))
                        successfulPuts += 1

                        #We have reached QUORUM or ONE consistency
                        if(successfulPuts == replicasForConsistency):

                                msg_to_send = store.Msg()
                                msg_to_send.suc.success = True
                                client_socket.sendall(msg_to_send.SerializeToString())

                    continue

                #Find socket connecting to other replica
                repSock = self.neighborSockets[operationReplicas[index]]

                #Sleep to see effect of consistency level
                time.sleep(2)

                #Send message containing key and val to replica
                repSock.sendall(msg.SerializeToString())

                #Receive response
                msg = repSock.recv(1024)

                store_msg = store.Msg()
                store_msg.ParseFromString(msg)
                msgType = store_msg.WhichOneof("msg")
                valid = self.parse_msg(repSock, ("", ""), store_msg)

                #The put in a replica was successful
                if(valid):

                    print("Successful copy to " + operationReplicas[index])

                    successfulPuts += 1

                    #We have enough for the consistency, send success to client
                    if(successfulPuts == replicasForConsistency):

                            msg_to_send = store.Msg()
                            msg_to_send.suc.success = True
                            client_socket.sendall(msg_to_send.SerializeToString())

                    continue

                else:

                    print("Failed copy to " + operationReplicas[index])

            #Message to get unused replcias out of the wait function
            else:

                if(i == int(self.replica_num)):
                    continue

                msg = store.Msg()
                msg.pair.key = -1
                msg.pair.val = "None"
                repSock = self.neighborSockets[i]
                time.sleep(0.5)
                repSock.sendall(msg.SerializeToString())


        #Send failure to client
        if(successfulPuts < replicasForConsistency):

            print("Didn't get to consistency level")

            msg_to_send = store.Msg()
            msg_to_send.suc.success = False
            client_socket.sendall(msg_to_send.SerializeToString())

    #Parse write-ahead log at beginning of execution
    def parseWriteLog(self):

        writeLogInfo = open(self.logName, "r")


        #If file is empty, return
        emptyCheck = writeLogInfo.read(1)
        writeLogInfo.close()

        if not emptyCheck:
            writeLogInfo.close()
            return

        writeLogInfo = open(self.logName, "r")
        for line in writeLogInfo:

            components = line.split(":")
            key = int(components[0])
            val = components[1][:-1]


            self.keyValStore[key] = val

        writeLogInfo.close()

    #Function to parse all protobuf messages
    def parse_msg(self, client_socket, client_add, msg):

        if not msg:

            print ("Error: null message")
            return False

        msg_type = msg.WhichOneof("msg")

        if msg_type == "put":

            self.put(msg.put.key, msg.put.val, msg.put.level, client_socket)

        elif msg_type == "get":

            self.get(msg.get.key, msg.get.level, client_socket)

        elif msg_type == "string_val":

            pass


        #Used to communicate between replicas
        elif msg_type == "pair":

            key = msg.pair.key
            val = msg.pair.val

            if(key == -1):
                return False

            self.storeLock.acquire()

            writeLogInfo = open("writeAhead.txt", "a")
            writeLogInfo = open(self.logName, "a")
            writeLogInfo.write(str(key) + ":" + val + "\n")
            writeLogInfo.close()

            self.keyValStore[key] = val;

            self.storeLock.release()

            if key in self.keyValStore:
                print("Added: " + val + " to location: " + str(key))
                return True

            else:

                return False

        #Used to determine if an operation was successful or not
        elif msg_type == "suc":

            outcome = msg.suc.success
            return outcome

        #Initializes the coordinator for a request
        elif msg_type == "init":
            self.coordinator = msg.init.coordinator

        else:

            print("Unrecognized message type: " + str(msg_type))

    #Wait for coordinator to assign a task
    def waitForInstruction(self):

        print("Waiting for coordinator <" + str(self.coordinator) + "> to assign a request")

        try:

            coordinatorSocket = self.neighborSockets[self.coordinator]

            msg = coordinatorSocket.recv(1024)

            store_msg = store.Msg()
            store_msg.ParseFromString(msg)
            msgType = store_msg.WhichOneof("msg")
            valid = self.parse_msg(coordinatorSocket, ("", ""), store_msg)

            if(not valid):

                return

            else:

                msg = store.Msg()
                msg.suc.success = True
                coordinatorSocket.sendall(msg.SerializeToString())

        except KeyboardInterrupt:
            self.clientSocket.close()
            for sock in self.neighborSockets:
                sock.close()

    #Wait for replica to connect
    def listenForReplica(self, replica):

        index = self.replicaList.index(replica)
        print("Listening for connection from " + str(index) + "...")

        incomingConn, incomingAddr = self.clientSocket.accept()

        #Store info for later communication
        self.neighborSockets[index] = incomingConn

        print("Got connection from " + str(index) + ": {" + incomingAddr[0] + ", " + str(incomingAddr[1]) + "}")
        print("")

    #Try to connect to given replica
    def connectToReplica(self, replica):

        while True:

            try:

                newSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                newSocket.connect((replica))

            except socket.error:

                continue

            break

        #Store socket info in list for later communication
        index = self.replicaList.index(replica)
        print(str(index))
        self.neighborSockets[index] = newSocket

        print("Connected to " + str(index) + ": {" + newSocket.getsockname()[0] + ", " + str(newSocket.getsockname()[1]) + "}")
        print("")

    #Create connections among all replicas
    def initializeNeighboringSockets(self):

        waitForConnection = []
        connectTo = []

        for i in range(4):

            if i == int(self.replica_num):

                waitForConnection = self.replicaList[:i]
                connectTo = self.replicaList[i + 1:]
                break

        for replica in waitForConnection:
            self.listenForReplica(replica)

        for replica in reversed(connectTo):
            self.connectToReplica(replica)

    def run(self):

        self.clientSocket.bind(('', self.portNum))

        print("Starting Replica " + self.replica_num + ": {" + self.hostName + ":" + str(self.portNum) + "}")

        self.neighborSockets[int(self.replica_num)] = self.clientSocket

        self.clientSocket.listen(30)

        print("---------------Connecting All Replicas---------------")
        print("")

        self.initializeNeighboringSockets()

        print("--------------Ready To Receive Requests--------------")

        #Check for write-log file
        writeLog = Path("./" + self.logName)
        if(writeLog.exists()):
            self.parseWriteLog()

        while True:

            client_socket = None
            client_add = None

            try:

                while(self.coordinator == -1):

                    client_sock, client_add = self.clientSocket.accept()

                    msg = client_sock.recv(1024)

                    if msg:

                        store_msg = store.Msg()
                        store_msg.ParseFromString(msg)
                        msgType = store_msg.WhichOneof("msg")
                        self.parse_msg(client_sock, client_add, store_msg)


                #Replica
                if(self.replica_num != str(self.coordinator)):

                    replicaThread = threading.Thread(target=self.waitForInstruction)

                    replciaThread.daemon = True

                    replicaThread.start()

                #coordinator
                else:

                    print("Connected to client as coordinator: {" + client_add[0] + ":" + str(client_add[1]) + "}")

                    infoMsg = client_sock.recv(1024)

                    if infoMsg:

                        store_msg = store.Msg()
                        store_msg.ParseFromString(infoMsg)

                        coordinatorThread = threading.Thread(target=self.parse_msg, args=(client_sock,client_add,store_msg)

                        coordinatorThread.daemon = True

                        coordinatorThread.start()


            except KeyboardInterrupt:
                for sock in self.neighborSockets:
                    sock.close()
                self.clientSocket.close()
                break

            self.coordinator = -1;
            client_sock.close()

def main(args):

    if len(args) != 3:
        print("python3 replica.py <replica number>")
        sys.exit(1)

    replicaNumber = args[1]
    mech = args[2]
    if(mech == "repair"):
        mech = 0
    elif (mech == "hinted"):
        mech = 1

    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    replica_server = Replica(replicaNumber, clientSocket, mech)
    replica_server.parseReplicaFile()
    replica_server.run()


if __name__ == "__main__":
    main(sys.argv)
