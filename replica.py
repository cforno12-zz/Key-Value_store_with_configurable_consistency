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

	else:

		print("Failed to send")

def readMessage(socket):

	msgHeader = socket.recv(4)
	msgLen = struct.unpack(">I", msgHeader)[0]
	msgString = socket.recv(msgLen)

	msgTemplate = store.Msg()
	msgTemplate.ParseFromString(msgString)

	msgType = msgTemplate.WhichOneof("msg")
	
	formattedMessage = eval("msgTemplate." + msgType)

	return formattedMessage

def get(key):

    if key in keyValStore:

        return keyValStore[key]

    else:

        #return null
        print("No value associated with key: " + key)

def put(key, val):

    keyValStore[key] = val;
    print("Added: " + val + " to location: " + str(key))

    writeLogInfo = open("writeAhead.txt", "a")
    writeLogInfo.write(str(key) + ":" + val + "\n")
    writeLogInfo.close()

def parseWriteLog():

    writeLogInfo = open("writeAhead.txt", "r")

    for line in writeLogInfo:
        components = line.split(":")
        key = components[0]
        val = components[1][:-1]

        keyValStore[key] = val

    writeLogInfo.close()


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

        if(request == "get"):

            get(key)

        elif(request == "put"):

            put(key, val)
            break

    print(keyValStore)


if __name__ == "__main__":
    main(sys.argv)
