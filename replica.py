import socket
import sys
from pathlib import Path

sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')

keyValStore = {}

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


def main():
    replicaSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    replicaSocket.bind(("", 0))

    hostName = socket.getfqdn()
    portNum = replicaSocket.getsockname()[1]
    print('\n')
    print("Starting Server on " + hostName + " with port " + str(portNum))
    print('\n')

    replicaSocket.listen(10)

    #Check for write-log file
    writeLog = Path("./writeAhead.txt")
    if(writeLog.exists()):
        parseWriteLog()

    request = "put"

    print(keyValStore)

    while True:

        if(request == "get"):

            get(key)

        elif(request == "put"):

            put(key, val)

            request = ""
            break

    print(keyValStore)


if __name__ == "__main__":
    main()
