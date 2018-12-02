import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct
import time

class Replica:
    def __init__(self, replica_num, server_socket):
        self.coordinator = -1            #Determines whether to wait or send
        self.clientSocket = server_socket
        self.replica_num = replica_num
        self.hostName = ""
        self.portNum = 0
        self.keyValStore = {}
        self.replicaList = [None] * 4       #Store IP/Port of all other replicas
        self.neighborSockets = [None] * 4

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
            val_to_send = self.keyValStore[key]
            msg_to_send = store.Msg()
            msg_to_send.string_val.val = val_to_send

        else:
            #return null
            print("No value associated with key: " + key)

    def put(self, key, val, level):

        #First replica determined by the byte partitioner
        firstReplica = self.bytePartitioner(key)

        #Second and third are clockwise in the cluster
        secondReplica = (firstReplica + 1) % 4
        thirdReplica = (firstReplica + 2) % 4

        operationReplicas = [firstReplica, secondReplica, thirdReplica]

        print("I will be storing this data on")
        print(str(firstReplica) + " " + str(secondReplica) + " " + str(thirdReplica))

        msg = store.Msg()
        msg.pair.key = key
        msg.pair.val = val

        for currSock in operationReplicas:
            repSock = self.neighborSockets[currSock]
            #import pdb; pdb.set_trace();
            time.sleep(0.5)
            repSock.sendall(msg.SerializeToString())

        '''for rep in operationReplicas:

            index = operationReplicas.index(rep) + 1

            if(rep == int(self.replica_num)):

                self.keyValStore[key] = val;
                print("Added: " + val + " to location: " + str(key))

                writeLogInfo = open("writeAhead.txt", "a")
                writeLogInfo.write(str(key) + ":" + val + "\n")
                writeLogInfo.close()

                continue

            msg = store.Msg()
            msg.pair.key = key
            msg.pair.val = val

            repSock = self.neighborSockets[rep]

            print(repSock)

            repSock.sendall(msg.SerializeToString())'''

    def parseWriteLog(self):

        writeLogInfo = open("./writeAhead.txt", "r")

        emptyCheck = writeLogInfo.read(1)
        if not emptyCheck:
            writeLogInfo.close()
            return

        for line in writeLogInfo:

            components = line.split(":")
            key = int(components[0])
            val = components[1][:-1]

            self.keyValStore[key] = val

        writeLogInfo.close()

    def parse_msg(self, client_socket, client_add, msg):
        if not msg:
            print ("Error: null message")
            return
        msg_type = msg.WhichOneof("msg")

        if msg_type == "put":
            self.put(msg.put.key, msg.put.val, msg.put.level)
        elif msg_type == "get":
            self.get(msg.get.key, msg.get.level, client_socket)
        elif msg_type == "string_val":
            pass
        elif msg_type == "pair":

            key = msg.pair.key
            val = msg.pair.val

            self.keyValStore[key] = val;
            print("Added: " + val + " to location: " + str(key))

            writeLogInfo = open("writeAhead.txt", "a")
            writeLogInfo.write(str(key) + ":" + val + "\n")
            writeLogInfo.close()

        elif msg_type == "suc":
            pass
        elif msg_type == "init":
            print("msg info: " + str(msg.init.coordinator))
            self.coordinator = msg.init.coordinator
        else:
            print("Unrecognized message type: " + str(msg_type))

    #Wait for coordinator to assign a task
    def waitForInstruction(self):

        try:

            coordinatorSocket = self.neighborSockets[self.coordinator]

            print("Waiting for coordinator instruction")

            msg = coordinatorSocket.recv(1024)
            print(msg.decode())

            store_msg = store.Msg()
            store_msg.ParseFromString(msg)
            msgType = store_msg.WhichOneof("msg")
            self.parse_msg(coordinatorSocket, ("", ""), store_msg)

        except KeyboardInterrupt:
            self.clientSocket.close()
            for sock in self.neighborSockets:
                sock.close()

    def listen_for_message(self, client_socket, client_add):

        msg = client_socket.recv(1024)

        if msg:

            store_msg = store.Msg()
            store_msg.ParseFromString(msg)
            self.parse_msg(client_socket, client_add, store_msg)

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
        writeLog = Path("./writeAhead.txt")
        if(writeLog.exists()):
            self.parseWriteLog()

        #Figure out how to reset
        while True:

            try:

                while(self.coordinator == -1):

                    print("waiting for coordinator")

                    client_sock, client_add = self.clientSocket.accept()

                    msg = client_sock.recv(1024)

                    if msg:

                        store_msg = store.Msg()
                        store_msg.ParseFromString(msg)
                        msgType = store_msg.WhichOneof("msg")
                        self.parse_msg(client_sock, client_add, store_msg)


                if(self.replica_num != str(self.coordinator)):

                    self.waitForInstruction()

                else:

                    print("Connected to client as coordinator: {" + client_add[0] + ":" + str(client_add[1]) + "}")

                    self.listen_for_message(client_sock, client_add)


            except KeyboardInterrupt:
                for sock in self.neighborSockets:
                    sock.close()
                self.clientSocket.close()
                break

            self.coordinator = -1;
            client_sock.close()

def main(args):

    if len(args) != 2:
        print("python3 replica.py <replica number>")
        sys.exit(1)

    replicaNumber = args[1]

    clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    replica_server = Replica(replicaNumber, clientSocket)
    replica_server.parseReplicaFile()
    replica_server.run()


if __name__ == "__main__":
    main(sys.argv)
