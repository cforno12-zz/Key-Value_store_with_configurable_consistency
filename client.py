import sys
import socket
import time


sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import store_pb2

class Client:
    def __init__(self, replicaList):
        self.replicaList = replicaList      #Store IP/Port of all replicas
        self.coordinatorSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def send_put_msg(self, key, val):
        msg = store_pb2.Msg()
        msg.put.key = key
        msg.put.val = val
        msg.put.level = 0
        print ("here:",  msg)

        self.coordinatorSocket.sendall(msg.SerializeToString())

    def send_get_req(self, key, consistency):
        msg = store_pb2.Msg()
        msg.get.key = key
        msg.get.level = consistency
        print("sending get message")
        self.coordinatorSocket.sendall(msg.SerializeToString())
        val = self.socket.recv(1024)
        if val:
            s = store_pb2.Msg()
            s.ParseFromString(val)
            print ("get value: ", s)

    def run(self):

        while True:

            coordinator = int(input("Enter replica to contact: "))
            request = input("Enter request (get/put): ")

            coordinatorIP = replicaList[coordinator][0]
            print(coordinatorIP)
            coordinatorPort = replicaList[coordinator][1]
            print(coordinatorPort)

            self.coordinatorSocket.connect((coordinatorIP, coordinatorPort))

            if(request == "put"):

                key = int(input("Key: "))
                val = input("Val: ")

                self.send_put_msg(key, val)

            if(request == "get"):

                key = int(input("Key: "))
                cL = input("Consistency Level: ")

                consistency = -1
                if(cL == "ONE"):
                    consistency = 0
                if(cL == "QUORUM"):
                    consistency = 1

                self.send_get_req(key, consistency)

def parseReplicaFile():

    replicaList = [None] * 5

    file = open("replicas.txt", "r")

    for line in file:

        splitLines = line.split(" ")

        index = int(splitLines[0])
        replicaList[index] = (splitLines[1], int(splitLines[2]))

    file.close()

    return replicaList


if __name__ == "__main__":

    replicaList = parseReplicaFile()

    client = Client(replicaList)
    client.run()

    print("Closing socket...")
    mysocket.close()
