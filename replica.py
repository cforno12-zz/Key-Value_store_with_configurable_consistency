import sys
sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import socket
from pathlib import Path
import store_pb2 as store
import struct

keyValStore = {}
replicaNumber = -1

def sendMessage(socket, msg):

    if(type(msg) == store.Msg):
        msgString = msg.SerializeToString()
        msgLen = len(msgString)
        msgHeader = struct.pack(">I", msgLen)
        socket.sendall(msgHeader + msgString)
        print("sent message")
    else:
        print("Failed to send")


def get(key, level, client_socket):

    if key in keyValStore:
        print("key is in map")
        val_to_send = keyValStore[key]
        msg_to_send = store.Msg()
        msg_to_send.string_val.val = val_to_send
        sendMessage(client_socket, msg_to_send)
    else:
        #return null
        print("No value associated with key: " + key)

    return

def put(key, val, level):

    keyValStore[key] = val;
    print("Added: " + val + " to location: " + str(key))
    
    writeLogInfo = open("writeAhead.txt", "a")
    writeLogInfo.write(str(key) + ":" + val + "\n")
    writeLogInfo.close()
    return
    
def parseWriteLog():

    writeLogInfo = open("./writeAhead.txt", "r")
    
    for line in writeLogInfo:
        components = line.split(":")
        key = components[0]
        val = components[1][:-1]
    
        keyValStore[key] = val

    writeLogInfo.close()

def parse_msg(client_socket, client_add, msg):
    if not msg:
        print ("Error: null message")
        return
    msg_type = msg.WhichOneof("msg")
    
    if msg_type == "put":
        put(msg.put.key, msg.put.val, msg.put.level)
        print("returned from put function")
    elif msg_type == "get":
        print("recieved get message")
        get(msg.get.key, msg.get.level, client_socket)
    elif msg_type == "string_val":
        pass
    elif msg_type == "pair":
        pass
    elif msg_type == "suc":
        pass
    else:
        print("Unrecognized message type: " + str(msg_type))

def listen_for_message(client_socket, client_add):
    print("we are listening for message")
    while True:
        msg = client_socket.recv(1024)
        if msg:
            store_msg = store.Msg()
            store_msg.ParseFromString(msg)
            parse_msg(client_socket, client_add, store_msg)
            print("we finished parsing message")


def main(args):
    global replicaNumber

    if len(args) != 2:
        print("python3 replica.py <replica number>")
        sys.exit(1)

    replicaNumber = args[1]

    replicaSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    replicaSocket.bind(("", 0))
    
    hostName = socket.getfqdn()
    portNum = replicaSocket.getsockname()[1]

    print("Starting Replica " + replicaNumber + ": {" + hostName + ":" + str(portNum) + "}")

    replicaSocket.listen(10)

    #Check for write-log file
    writeLog = Path("./writeAhead.txt")
    if(writeLog.exists()):
        parseWriteLog()
        
        request = ""

    while True:
        try:
            print("waiting for connection")
            client_sock, client_add = replicaSocket.accept()
            listen_for_message(client_sock, client_add)
        except KeyboardInterrupt:
            replicaSocket.close()
            break

if __name__ == "__main__":
    main(sys.argv)
