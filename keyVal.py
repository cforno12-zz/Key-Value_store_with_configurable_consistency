import replicaSocket
import sys

keyValStore = {}

def get(key):

    if key in keyValStore:

        return keyValStore[key]

    else:
        
        print("No value associated with key: " + key)

def put(key, val):

    keyValStore[key] = val;
    print("Added: " + val + " to location: " + key)


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

    request = ""
    while True:

        if(request == "get"):
            get(key)
        elif(request == "put"):
            put(key, val)


if __name__ == "__main__":
    main()
