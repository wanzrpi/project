from SocketServer import TCPServer, BaseRequestHandler
import socket
import json
import sys
"""
# CA instance
DNS_1 = 'ec2-54-67-101-22.us-west-1.compute.amazonaws.com'
# Virginia instance
DNS_2 = 'ec2-52-87-177-8.compute-1.amazonaws.com'
# Oregon instance
DNS_3 = 'ec2-52-37-12-187.us-west-2.compute.amazonaws.com'
PORT = 4400
"""

#golobal variables
log_loc = 'log.txt'
file_sys = {}
neighbor=[]
server_addr = {}
server_id = 0
history=[]
leader=0

def election(server_id, epoch, counter, server_list):
    """
    Bully Election Algrorithm,
    largest id becomes the primary copy
    at failure to connect to primary copy
    the server sends a election message to each replication server
        if a server has a higher ID it replies with an election message
          then sends another election message
        else if no server replies then server becomes the primary copy
          then sends out an elected message
    """
    election_msg="ELECTION\n{}\n{}".format(epoch, counter)
    has_highest_id=True

    for server in server_list:
        try:
            sock = socket.socket()
            sock.connect(server[0],server[1])
            sock.send(election_msg)
            response = sock.recv(1024)
            if(not response == "\n"):
                has_highest_id = False
        except:
            pass

    if (has_highest_id == True):
        elected(server_id, server_list)
        return server_id


def elected(server_id, server_list):
    elected_msg = "ELECTED\n{}".format(server_id)
    broadcast(server_list, elected_msg)

##functions
def broadcast(server_list, msg):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for server in server_list:
        try:
            sock = socket.socket()
            sock.connect(server[0], server[1])
            sock.send(msg)
        except:
            pass
def process_command(command):
    if command[0]=='a':
        command = command.split(' ', 2)
    else:
        command = command.split(' ', 1)
    if command[0] == 'create':
        file_sys[command[1]] = ""
    elif command[0] == 'delete':
        del file_sys[command[1]]
    elif command[0] == 'append':
        file_sys[command[1]] += command[2]

        
def add_history(lines):
    msg=lines.split(' ', 2)
    epoch=msg[0]
    counter=msg[1]
    operation=msg[2]
    if epoch == len(history):
        history.append([])
        history[epoch].append(msg[2])
            
def get_history(log_loc):
    f = open(log_loc, 'r')
    for lines in f.readlines():
        add_history(lines)
    f.close()

def write_history(log_loc,history):
    f.open(log_loc, 'w')
    for i in range (0,len(history)):
        for j in range (0,len(history[i])):
            f.write('{} {} {}\n'.format(i, j, history[i][j]))
    
def recovery(leader):
    history_max = []
    for i in history:
        history_max.append(len(i))
    (new_max, new_history) = update(leader,json.dumps(history_max))
    new_max = json.loads(new_max)
    for i in range (0,len(new_max)):
        while len(history[i])>new_max[i]:
            history[i].pop[len(history[i])-1]
    for lines in json.loads(new_history):
        add_history(lines)
    write_history(log_loc, history)
    for lines in history:
        process_command(lines)

def init(): ##init when process starts
    history=get_history(log_loc)
    leader=election()##todo
    recovery(leader)
    

##server handler
class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):

    def handle(self):
        # self.request is the TCP socket connected to the client
        self.data = self.request.recv(1024)
        print "{} get request:".format(server_id)
        print self.data


        if ("ELECTION" in self.data):
            #checks if the id in the message is larger than current id
            #If the election requests Id is larger reply with a \n
            if(int(self.data.split("\n")[1]) > self.epoch):
                self.request.send("\n")
            elif(int(self.data.split("\n")[1]) == self.epoch && int(self.data.split("\n")[2]) > self.counter):
                self.request.send("\n")
            else:
                self.request.send("Bigger zxid")
                #start an election
                election(self.server_id, self.epoch, self.counter, self.server_list)

        elif ("ELECTED" in self.data):
            leader = self.data.split("\n")[1]
            self.leader = leader

        elif("WRITE" in authenticated_requst):
            #print(authenticated_requst)
            write_file(self, authenticated_requst)
            #if it is a primary copy then the server propagates the updated file
            #to replicas, if a server can not be progagate too it is deamed failed and removed from
            #the replica server list
            if(self.leader == True):
                file_name=authenticated_requst.split("\n")[1]
                failed_servers=propagate_write_2(file_server.server_list,file_name, auth_port)
                file_server.server_list.remove(failed_servers)


        elif("READ" in authenticated_requst):
            if(file_server.primary_copy == True):
                #send replica server info to client
                self.request.send(file_server.server_list[file_server.last_replica])
                #iterates to the next replica server, so that the following read requests are distributed fairly bewtween the replicas
                if(len(file_server.server_list)==file_server.last_replica):
                    file_server.last_replica=0
                else:
                    file_server.last_replica+=1
            else:
                #print(authenticated_requst)
                read_file(self, authenticated_requst)

def parse_string_server_list(server_list):
    server_list=[]
    #host,port
    server_strings=server_list.split("\n")
    for servers in server_strings:
        server_address=servers.split(",")
        #adding the host and port (in int form) to the server list
        server_list.append((server_address[0],int(server_address[1])))
    return server_list
##flo
##main    
if __name__ == '__main__':
    server_id = sys.argv[1]
    init()
    server = ThreadedTCPServer((HOST,PORT), ThreadedTCPRequestHandler)
    ip, port = server.server_address
    server.epoch = 0 
    server.counter = 0
    server.server_id = server_id
    #server id is used in the bully election algorithm
    file_server.server_id= file_port

    print "Setting up server on port: {}".format(port)
    server.server_list=[]
    server_num = 5
    #server.host = socket.gethostbyname(DNS_3)
    #server.port = ***********
    #server.leader = ***********
    for index in range(0, server_num):
        if index != server_id:
            remote_ip = socket.gethostbyname(DNS_3)
            server.server_list.append((remote_ip, PORT+index))
    server_thread = threading.Thread(target = server.serve_forever)
    server_thread.daemon = True
    server_thread.start()


