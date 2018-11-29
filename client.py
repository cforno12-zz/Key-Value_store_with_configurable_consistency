import sys
import socket
import time


sys.path.append('/home/vchaska1/protobuf/protobuf-3.5.1/python')
import store_pb2

class Client:
    def __init__(self, ip, port, sock):
        self.ip = ip
        self.port = port
        self.socket = sock

    def send_put_msg(self):
        msg = store_pb2.Msg()
        msg.put.key = 12
        msg.put.val = "mouse"
        cons = store_pb2.Consistency()
        cons.level = store_pb2.Consistency.ONE
        print("consistency level: ", store_pb2.Consistency.ONE)
        msg.put.level = store_pb2.Consistency.ONE
        print ("here:",  msg)

        self.socket.sendall(msg.SerializeToString())

        

    def run(self):
        self.socket.connect((socket.gethostbyname(socket.gethostname()), int(self.port)))
        self.send_put_msg()
        
        


if __name__ == "__main__":
    
    mysocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = Client(socket.gethostbyname(socket.gethostname()), sys.argv[1], mysocket)
    client.run()

    print("Closing socket...")
    mysocket.close()
