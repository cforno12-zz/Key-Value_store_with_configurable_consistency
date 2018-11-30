import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct

class Replica:
    def __init__(self, replica_num, server_socket):
        self.socket = server_socket
        self.replica_num = replica_num
        self.hostName = ""
        self.portNum = 0
        self.keyValStore = {}

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

    def listen_for_message(self, client_socket, client_add):
        while True:
            msg = client_socket.recv(1024)
            if msg:
                store_msg = store.Msg()
                store_msg.ParseFromString(msg)
                self.parse_msg(client_socket, client_add, store_msg)
    def run(self):
        self.socket.bind(("", 0))
            
        self.hostName = socket.getfqdn()
        self.portNum = self.socket.getsockname()[1]

        print("Starting Replica " + self.replica_num + ": {" + self.hostName + ":" + str(self.portNum) + "}")
        
        self.socket.listen(10)

        #Check for write-log file
        writeLog = Path("./writeAhead.txt")
        if(writeLog.exists()):
            self.parseWriteLog()

        while True:
            try:
                client_sock, client_add = self.socket.accept()
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
    replica_server.run()
    

if __name__ == "__main__":
    main(sys.argv)
