import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct

class Replica:
    def __init__(self, replica_num, server_socket):
        self.coordinator = False            #Determines whether to wait or send
        self.socket = server_socket
        self.replica_num = replica_num
        self.hostName = ""
        self.portNum = 0
        self.keyValStore = {}
        self.replicaList = [None] * 5       #Store IP/Port of all other replicas

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

    def sendMessage(self, client_socket, msg):

        if(type(msg) == store.Msg):
            msgString = msg.SerializeToString()
            msgLen = len(msgString)
            msgHeader = struct.pack(">I", msgLen)
            client_socket.sendall(msgHeader + msgString)
        else:
            print("Failed to send")


    def get(self, key, level, client_socket):

        if key in self.keyValStore:
            val_to_send = self.keyValStore[key]
            msg_to_send = store.Msg()
            msg_to_send.string_val.val = val_to_send
            self.sendMessage(client_socket, msg_to_send)
        else:
            #return null
            print("No value associated with key: " + key)

    def put(self, key, val, level):

        self.keyValStore[key] = val;
        print("Added: " + val + " to location: " + str(key))

        writeLogInfo = open("writeAhead.txt", "a")
        writeLogInfo.write(str(key) + ":" + val + "\n")
        writeLogInfo.close()

    def parseWriteLog(self):

        writeLogInfo = open("./writeAhead.txt", "r")

        for line in writeLogInfo:

            components = line.split(":")
            key = components[0]
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
            pass
        elif msg_type == "suc":
            pass
        else:
            print("Unrecognized message type: " + str(msg_type))

    #Wait for coordinator to assign a task
    def waitForInstruction(self):

        print("Ill just wait here then")

        incomingConnection, incomingAddress = self.socket.accept()

        #print("Got a connection from " + branch.name + ": {" + branch.ip + ", " + str(branch.port) + "}")

    def listen_for_message(self, client_socket, client_add):

        print("Im listening")

        while True:

            msg = client_socket.recv(1024)

            if msg:

                store_msg = store.Msg()
                store_msg.ParseFromString(msg)
                self.parse_msg(client_socket, client_add, store_msg)

    def run(self):

        self.socket.bind((self.hostName, self.portNum))

        print("Starting Replica " + self.replica_num + ": {" + self.hostName + ":" + str(self.portNum) + "}")

        self.socket.listen(10)

        #Check for write-log file
        writeLog = Path("./writeAhead.txt")
        if(writeLog.exists()):
            self.parseWriteLog()

        while True:

            try:

                client_sock, client_add = self.socket.accept()

                print("Connected to client: {" + client_add[0] + ":" + str(client_add[1]) + "}")

                self.listen_for_message(client_sock, client_add)


            except KeyboardInterrupt:
                self.socket.close()
                break

def main(args):

    if len(args) != 2:
        print("python3 replica.py <replica number>")
        sys.exit(1)

    replicaNumber = args[1]

    replicaSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    replica_server = Replica(replicaNumber, replicaSocket)
    replica_server.parseReplicaFile()
    replica_server.run()


if __name__ == "__main__":
    main(sys.argv)
