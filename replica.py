import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct
import time
import _thread as thread

class Replica:
    def __init__(self, replica_num, server_socket, mech):
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
        self.OKs = []

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
        msg.pair.key = key
        msg.pair.val = self.keyValStore[key].split(":")[0]
        
        print("using " + str(contact) + "to send message")
        socket.sendall(msg.SerializeToString())

        response = socket.recv(1024)

        msg = store.Msg()
        if response:
            msg.ParseFromString(response)
            if msg.WhichOneof("msg") == "succ":
                if msg.suc.success == True:
                    print("Recieved SUCCESS from replica " + contact)
                    self.OKs.append(True)
                else:
                    pass
                
                    # do the consistency mechanism
                    # only one of the threads should do this
        thread.exit()

    def get_consistency(self, key, level):
        if level == 0:
            OK_len = 1
        elif level == 1:
            OK_len = 2

        self.OKs = []
        begins = self.bytePartitioner(key)
        #contact all three
        for i in range(3):
            contact = (int(begins) + i) % 4
            print("HERE: " + str(contact) + " and " + str(self.replica_num))
            if int(contact) == int(self.replica_num):
                print("we dont want to contact ourselves" + contact + " and " + self.replica_num)
                continue
            sock = self.neighborSockets[contact]
            print("contacting replica: " + str(contact))
            thread.start_new_thread(self.get_consistency_helper, (int(key), sock, contact))

        counter = 0 # use this as a time out mechanism
        while len(self.OKs) < OK_len or counter < 5: # we only need one OK from the replicas
            time.sleep(0.5)
            counter += 1
            continue
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
        print("message:" + msg_to_send)
        client_socket.sendall(msg_to_send.SerializeToString())
        print("message sent")

    def put(self, key, val, level):

        #First replica determined by the byte partitioner
        firstReplica = self.bytePartitioner(key)

        #Second and third are clockwise in the cluster
        secondReplica = (firstReplica + 1) % 4
        thirdReplica = (firstReplica + 2) % 4

        operationReplicas = [firstReplica, secondReplica, thirdReplica]

        print("I will be storing this data on")
        print(str(firstReplica) + " " + str(secondReplica) + " " + str(thirdReplica))

        for i in range(4):

            if(i in operationReplicas):

                msg = store.Msg()
                msg.pair.key = key
                msg.pair.val = val

                index = operationReplicas.index(i)
                if(operationReplicas[index] == int(self.replica_num)):

                    print("I'm one of the desired replicas")

                    self.keyValStore[key] = val;
                    print("Added: " + val + " to location: " + str(key))

                    writeLogInfo = open(self.logName, "a")
                    writeLogInfo.write(str(key) + ":" + val + ":" + str(time.time()) +"\n")
                    writeLogInfo.close()

                    continue

                repSock = self.neighborSockets[operationReplicas[index]]
                time.sleep(0.5)
                repSock.sendall(msg.SerializeToString())

            else:

                if(i == int(self.replica_num)):
                    continue

                msg = store.Msg()
                msg.pair.key = -1
                msg.pair.val = "None"
                repSock = self.neighborSockets[i]
                time.sleep(0.5)
                repSock.sendall(msg.SerializeToString())

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

    def parseWriteLog(self):

        print(self.logName)

        writeLogInfo = open(self.logName, "r")

        emptyCheck = writeLogInfo.read(1).strip()
        writeLogInfo.close()
        writeLogInfo = open(self.logName, "r")
        if not emptyCheck:
            writeLogInfo.close()
            return

        for line in writeLogInfo:

            components = line.strip().split(":")
            key = int(components[0])
            val = components[1]
            

            self.keyValStore[key] = val

        writeLogInfo.close()

    def parse_msg(self, client_socket, client_add, msg):

        if not msg:

            print ("Error: null message")
            return -1

        msg_type = msg.WhichOneof("msg")

        if msg_type == "put":

            self.put(msg.put.key, msg.put.val, msg.put.level)

        elif msg_type == "get":

            self.get(msg.get.key, msg.get.level, client_socket)

        elif msg_type == "pair":

            print("we got a pair message")

            self.compare_pair(msg.pair.key, msg.pair.val, client_socket)

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

            if(valid == -1):

                return

        except KeyboardInterrupt:
            self.clientSocket.close()
            for sock in self.neighborSockets:
                sock.close()

    def listen_for_message(self, client_socket, client_add):

        print("Ready to take a request")

        msg = client_socket.recv(1024)

        if msg:

            store_msg = store.Msg()
            store_msg.ParseFromString(msg)
            self.parse_msg(client_socket, client_add, store_msg)

        else:

            print("i got nothing")

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

        print(self.keyValStore)

        #Figure out how to reset
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

                    self.waitForInstruction()

                #coordinator
                else:

                    print("Connected to client as coordinator: {" + client_add[0] + ":" + str(client_add[1]) + "}")

                    infoMsg = client_sock.recv(1024)

                    if infoMsg:

                        store_msg = store.Msg()
                        store_msg.ParseFromString(infoMsg)
                        self.parse_msg(client_sock, client_add, store_msg)
                        #self.listen_for_message(client_sock, client_add)


            except KeyboardInterrupt:
                for sock in self.neighborSockets:
                    sock.close()
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
