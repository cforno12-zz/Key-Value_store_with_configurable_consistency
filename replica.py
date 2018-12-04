import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct
import time
import threading
import _thread as thread

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
        self.storeLock = threading.Lock()
        self.logName = "writeAhead" + replica_num + ".txt"
        self.coordinator = -1                   #Determines whether to wait or send
        self.clientSocket = server_socket
        self.replica_num = replica_num
        self.hostName = ""
        self.portNum = 0
        self.keyValStore = {}                   #key => val:time_stamp
        self.replicaList = [None] * 4           #Store IP/Port of all other replicas
        self.neighborSockets = [None] * 4
        self.const_mech = mech                  # 0 => read repair -- 1 => hinted handoff
        self.OKs = []
        self.successfulPuts = 0
        self.hints = [[], [], [], []]           #2d Array of hints

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

    def get_consistency_helper(self, key, socket, contact):
        msg = store.Msg()
        msg.pair_read.key = key
        msg.pair_read.val = self.keyValStore[key].split(":")[0]

        socket.sendall(msg.SerializeToString())

        response = socket.recv(1024)

        msg = store.Msg()
        if response:
            msg.ParseFromString(response)
            if msg.WhichOneof("msg") == "suc":
                if msg.suc.success == True:
                    self.OKs.append(True)
                else:
                    pass

                    # do the consistency mechanism
                    # only one of the threads should do this
        thread.exit()
    def reset_msg(self, socket, contact):
        msg = store.Msg()
        msg.pair_write.key = -1
        msg.pair_write.val = ":)"
        socket.sendall(msg.SerializeToString())
        thread.exit()

    def get_consistency(self, key, level):
        if level == 0:
            OK_len = 1
        elif level == 1:
            OK_len = 2

        self.OKs = []
        begins = self.bytePartitioner(key)
        #contact all three
        for i in range(4):
            contact = (int(begins) + i) % 4
            if int(contact) == int(self.replica_num):
                continue
            sock = self.neighborSockets[contact]
            if i == 4:
                thread.start_new_thread(self.reset_msg, (sock, contact))
            else:
                thread.start_new_thread(self.get_consistency_helper, (int(key), sock, contact))

        counter = 0 # use this as a time out mechanism
        while len(self.OKs) < OK_len and counter < 5: # we only need one OK from the replicas
            print("number of OKs: " + str(len(self.OKs)))
            time.sleep(0.5)
            counter += 1

        if len(self.OKs) < OK_len:
            pass
            # what do we do when none of them OK?
            # do we call the consistency mechanism?
            # then we call this function again?

    def get(self, key, level, client_socket):

        if key in self.keyValStore:
            val_to_send = self.keyValStore[key].split(":")
            val_to_send = val_to_send[0]
            print("this is the value we are sending: " + val_to_send)
            time_stamp = val_to_send[1]

            self.get_consistency(key, level)
        else:
            #we contact the next replica in the ring
            sock_to_contact = self.neighborSockets[(int(self.replica_num) + 1) % 4]
            msg_to_send = store.Msg()
            msg_to_send.get.key = key
            msg_to_send.get.level = level
            print("key is not in this node, asking next node...")
            sock_to_contact.sendall(msg_to_send.SerializeToString())
            val_to_send = sock_to_contact.recv(1024)

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
                msg.pair_write.key = key
                msg.pair_write.val = val

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
                time.sleep(0.5)

                #Send message containing key and val to replica
                try:

                    repSock.sendall(msg.SerializeToString())

                except socket.error:

                    self.neighborSockets[i].close()
                    print("Failed to connect to " + str(i))

                    if(self.const_mech == 1):
                        print("Storing <" + str(key) + val + "> as a hint")

                        self.hints[i].append((key, val))

                        print(self.hints)

                    continue


                #Sleep to see effect of consistency level
                time.sleep(2)

                #Receive response
                try:

                    msg = repSock.recv(1024)

                except socket.error:

                    self.neighborSockets[i].close()
                    print("Failed to connect to " + str(i))
                    if(self.const_mech == 1):
                        print("Storing <" + str(key) + val + "> as a hint")

                        self.hints[i].append((key, val))

                        print(self.hints)

                    continue

                store_msg = store.Msg()
                store_msg.ParseFromString(msg)
                msgType = store_msg.WhichOneof("msg")
                valid = self.parse_msg(repSock, ("", ""), store_msg)

                #The put in a replica was successful
                if(valid):

                    print("Successful copy to " + str(operationReplicas[index]))

                    successfulPuts += 1

                    #We have enough for the consistency, send success to client
                    if(successfulPuts == replicasForConsistency):

                            msg_to_send = store.Msg()
                            msg_to_send.suc.success = True
                            client_socket.sendall(msg_to_send.SerializeToString())

                    continue

                else:

                    print("Failed copy to " + str(operationReplicas[index]))

                    #Append hint to list for replica that failed
                    self.neighborSockets[i].close()
                    print("Failed to connect to " + str(i))
                    if(self.const_mech == 1):
                        print("Storing <" + str(key) + val + "> as a hint")

                        self.hints[i].append((key, val))

                        print(self.hints)

                    continue

            #Message to get unused replcias out of the wait function
            else:

                if(i == int(self.replica_num)):
                    continue

                msg = store.Msg()
                msg.pair_write.key = -1
                msg.pair_write.val = "None"
                repSock = self.neighborSockets[i]
                time.sleep(0.5)
                repSock.sendall(msg.SerializeToString())

        #Send failure to client
        if(successfulPuts < replicasForConsistency):

            print("Didn't get to consistency level")

            msg_to_send = store.Msg()
            msg_to_send.suc.success = False
            client_socket.sendall(msg_to_send.SerializeToString())

    def compare_pair(self, key, val, sock):
        msg = store.Msg()
        if key in self.keyValStore:
            if self.keyValStore[key] == val:
                msg.suc.success = True
            else:
                msg.suc.success = False
        else:
            msg.suc.success = False

        sock.sendall(msg.SerializeToString())

    #Parse write-ahead log at beginning of execution
    def parseWriteLog(self):

        writeLogInfo = open(self.logName, "r")

        emptyCheck = writeLogInfo.read(1).strip()
        writeLogInfo.close()

        if not emptyCheck:
            writeLogInfo.close()
            return

        writeLogInfo = open(self.logName, "r")
        for line in writeLogInfo:

            components = line.strip().split(":")
            key = int(components[0])
            val = components[1]
            self.keyValStore[key] = val

        writeLogInfo.close()

    #Adds hints to coordinators keyValStore
    def appendHints(self, hKeys, hVals):

        for i in range(len(hKeys)):

            writeLogInfo = open(self.logName, "a")
            writeLogInfo.write(str(hKeys[i]) + ":" + hVals[i] + ":" + str(time.time()) +"\n")
            writeLogInfo.close()

            self.keyValStore[hKeys[i]] = hVals[i]

            print("Added <" + str(hKeys[i]) + " " + hVals[i] + ">")

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

        #Used to communicate between replicas
        elif msg_type == "pair_write":

            key = msg.pair_write.key
            val = msg.pair_write.val

            if(key == -1):

                return False

            self.storeLock.acquire()

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

        elif msg_type == "pair_read":
            self.compare_pair(msg.pair_read.key, msg.pair_read.val, client_socket)
            # use the function i created --cris

        elif msg_type == "hint":

                hKeys = []
                hVals = []

                hKeys.extend(msg.hint.hintKey)
                hVals.extend(msg.hint.hintValue)

                if(hKeys[0] == -1):

                    return False

                self.appendHints(hKeys, hVals)

                return True

        else:

            print("Unrecognized message type: " + str(msg_type))

    def performHintedHandoff(self):

        if(len(self.hints[self.coordinator]) > 0):

            print("I have hints for the coordinator")
            msg = store.Msg()

            k = []
            v = []
            for i in range(len(self.hints[self.coordinator])):

                k.append(self.hints[self.coordinator][i][0])
                v.append(self.hints[self.coordinator][i][1])

            msg.hint.hintKey.extend(k)
            msg.hint.hintValue.extend(v)

            self.neighborSockets[self.coordinator].sendall(msg.SerializeToString())

            print(self.hints[self.coordinator])
            self.hints[self.coordinator].clear()
            print(self.hints)

        else:

            msg = store.Msg()
            print("No hints for the coordinator")
            noHint = []
            noHint.append(-1)
            noVal = []
            noVal.append("-")
            msg.hint.hintKey.extend(noHint)
            msg.hint.hintValue.extend(noVal)

            self.neighborSockets[self.coordinator].sendall(msg.SerializeToString())

    #Wait for coordinator to assign a task
    def waitForInstruction(self):

        print("Waiting for coordinator <" + str(self.coordinator) + "> to assign a request")

        #Hinted Handoff Consistency Mechanism
        if(self.const_mech == 1):

            performHintedHandoff()

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

        except KeyboardInterrupt or OSError:

            if self.clientSocket:

                self.clientSocket.close()
                self.clientSocket = None

            for sock in self.neighborSockets:

                if sock:
                    sock.close()
                    sock = None

            sys.exit()

    def receiveHintedHandoff(self):

        print("")
        print("---------------Handling Hints---------------")
        print("")

        hintMessagesReceived = 0

        for socket in self.neighborSockets:

            if(self.neighborSockets.index(socket) == self.coordinator):
                continue
            else:
                msg = socket.recv(1024)
                if msg:

                    store_msg = store.Msg()
                    store_msg.ParseFromString(msg)
                    msgType = store_msg.WhichOneof("msg")

                    if(self.parse_msg(socket, client_add, store_msg)):
                        print("Got a hint from " + str(self.neighborSockets.index(socket)))

                    else:
                        print("No hint from " + str(self.neighborSockets.index(socket)))

        print(self.keyValStore)

        print("")
        print("-------------Done Handling Hints-------------")
        print("")

    def coordinatorFunction(self, client_sock, client_add, store_msg):

        #Wait to receive hints if consistency mechanism is hinted handoff
        if(self.const_mech == 1):

            receiveHintedHandoff()

        infoMsg = client_sock.recv(1024)

        if infoMsg:

            store_msg = store.Msg()
            store_msg.ParseFromString(infoMsg)

        self.parse_msg(client_sock, client_add, store_msg)

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

            client_sock = None
            client_add = None

            try:

                while(self.coordinator == -1):

                    try:

                        client_sock, client_add = self.clientSocket.accept()

                        msg = client_sock.recv(1024)

                        if msg:

                            store_msg = store.Msg()
                            store_msg.ParseFromString(msg)
                            msgType = store_msg.WhichOneof("msg")
                            self.parse_msg(client_sock, client_add, store_msg)

                    except KeyboardInterrupt or AttributeError:

                        if self.clientSocket:
                            self.clientSocket.close()

                        for sock in self.neighborSockets:

                            if sock:

                                sock.close()

                        break


                #Replica
                if(self.replica_num != str(self.coordinator)):

                    self.waitForInstruction()

                #coordinator
                else:

                    print("Connected to client as coordinator: {" + client_add[0] + ":" + str(client_add[1]) + "}")

                    self.coordinatorFunction(client_sock, client_add, store_msg)

            except KeyboardInterrupt:

                for sock in self.neighborSockets:

                    if sock:
                        sock.close()

                if self.clientSocket:
                    self.clientSocket.close()

                break

            self.coordinator = -1;
            client_sock.close()

def main(args):

    if len(args) != 3:
        print("python3 replpica.py <replica number> <consistency mechanism>")
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
