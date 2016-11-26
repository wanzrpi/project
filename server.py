from SocketServer import TCPServer, BaseRequestHandler
import socket
import json
import sys

#golobal variables
log_loc = 'log.txt'
file_sys = {}
server_addr = {}
server_id = 0

##functions

def send_msg(m,sock): ##function to write msg
    ##todo


def read_msg(m,sock): ##function to read msg
    ##todo


def create(file_name):
    ##todo


def delete(file_name):
    ##todo


def read(file_name):
    ##todo


def append(file_name, msg):
    ##todo


def init(): ##init when process starts
    ##todo


##server handler
class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        ##todo
        
    
##main    
if __name__ == '__main__':
    server_id = sys.argv[1]
    init()
    server = ThreadedTCPServer((HOST,PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
