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
        msg.put.level = 0
        print ("here:",  msg)

        self.socket.sendall(msg.SerializeToString())

    def send_get_req(self):
        msg = store_pb2.Msg()
        msg.get.key = 12
        msg.get.level = 0
        print("sending get message")
        self.socket.sendall(msg.SerializeToString())
        val = self.socket.recv(1024)
        if val:
            s = store_pb2.Msg()
            s.ParseFromString(val)
            print ("get value: ", s)
        

        

    def run(self):
        self.socket.connect((socket.gethostbyname(socket.gethostname()), int(self.port)))
        self.send_put_msg()
        time.sleep(1)
        self.send_get_req()
        
        


if __name__ == "__main__":
    
    mysocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client = Client(socket.gethostbyname(socket.gethostname()), sys.argv[1], mysocket)
    client.run()

    print("Closing socket...")
    mysocket.close()
