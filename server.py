from SocketServer import TCPServer, BaseRequestHandler
import SocketServer
import socket
import json
import sys

# CA instance
DNS_1 = 'ec2-54-67-101-22.us-west-1.compute.amazonaws.com'
# Virginia instance
DNS_2 = 'ec2-52-87-177-8.compute-1.amazonaws.com'
# Oregon instance
DNS_3 = 'ec2-52-37-12-187.us-west-2.compute.amazonaws.com'
PORT = 4400

#TODO: change epoch and counter to reading from file 
#global variables
log_loc = 'log.txt'
server_addr = {}
server_id = 0
#leader_status: leading/following/waiting
setting = {'init':False, 'leader':0,'file_sys':{},'neighbor':{}, 'neighbor_failed' : [], 'history':[],'applied':[], 'election':''}

#Wan
def send_msg(m,server): ##function to write msg
    ##todo
    size = len(m)
    server.wfile.write(str(size))
    ret=server.request.recv(1024)
    for i in range (0, int(math.floor(float(size) / 1024))):
        server.wfile.write(msg[i*1024:(i+1)*1024])
    if int(math.floor(float(size) / 1024))*1024 != size:
        server.wfile.write(msg[int(math.floor(float(size) / 1024))*1024:size])

def read_msg(server): ##function to read msg
    size = int(server.request.recv(1024))
    server.wfile.write(str(size))
    msg = ""
    for i in range (0, int(math.floor(float(size) / 1024))):
        buff = server.request.recv(1024)
        msg += buff
    if int(math.floor(float(size) / 1024))*1024 != size:
        buff = server.request.recv(size - int(math.floor(float(size) / 1024)*1024))
        msg += buff
    return msg

def create(file_name):
    if file_name in file_sys:
        return "file {} already exists!".format(file_name)
    else:
        file_sys[file_name]=""
        return "file {} created successfully!"


def delete(file_name):
    if file_name in file_sys:
        del file_sys[file_name]
        return "file {} deleted successfully!".format(file_name)
    else:
        return "file {} does not exist!".format(file_name)


def read(file_name):
    if file_name in file_sys:
        return file_sys[file_name]
    else:
        return "file {} does not exist!".format(file_name)

def append(file_name, msg):
    if file_name in file_sys:
        file_sys[file_name] += msg
        return "message appended successfully!"
    else:
        return "file {} does not exist!".format(file_name)

def process_command(command):
    if command[0]=='a':
        command = command.split(' ', 2)
    else:
        command = command.split(' ', 1)
    if command[0] == 'create':
        return create(command[1])
    elif command[0] == 'delete':
        return delete(command[1])
    elif command[0] == 'append':
        return append(command[1], command[2])
    elif command[0] == 'read':
        return read(command[1])

        
def add_history(lines):
    msg=lines.split(' ', 2)
    epoch=msg[0]
    counter=msg[1]
    operation=msg[2]
    if epoch == len(setting['history']):
        setting['history'].append([])
        setting['history'][epoch].append(msg[2])
            
def get_history(log_loc):
    f = open(log_loc, 'r')
    for lines in f.readlines():
        add_history(lines)
    f.close()


def get_history_max():
    history_max=[]
    for i in setting['history']:
        history_max.append(len(i))
    return history_max

        
def write_history():
    f.open(log_loc, 'a')
    for i in range (0,len(setting['history'])):
        for j in range (0,len(setting['history'][i])):
            f.write('{} {} {}\n'.format(i, j, setting['history'][i][j]))

def update(leader, history_max):
    msg = json.dumps(history_max)
    send_msg('history_match', leader)
    rcv = read_msg(leader)
    if rcv == 'history_match ACK':
        send_msg(msg, leader)
        new_max = json.loads(read_msg(leader))
        new_history = json.loads(read_msg(leader))
        return (new_max, new_history)
    elif rcv == 'NOT LEADER':
        return (None,None)
    
def recovery(leader):
    history_max = get_history_max()
    (new_max, new_history) = update(leader,history_max)
    if new_max == None:
        return False
    for i in range (0,len(new_max)):
        while len(setting['history'][i])>new_max[i]:
            setting['history'][i].pop[len(setting['history'][i])-1]
    for lines in json.loads(new_history):
        add_history(lines)
    write_history()
    for lines in setting['history']:
        return process_command(lines)
    return True


#San


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
    election_msg="ELECTION {} {}".format(epoch, counter)
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


def elected(server_id, server_list):
    elected_msg = "ELECTED {}".format(server_id)
    setting['leader'] = server_id
    setting['election'] = "leading"
    broadcast(server_list, elected_msg)

##functions
def broadcast(msg):
    for neighbor_id, neighbor in setting['neighbor']:
        try:
            neighbor.request.send(msg)
        except:
            neighbor_failed.append(neighbor_id)
            del setting['neighbor'][neighbor_id]

def init(): ##init when process starts
    ## process election to learn leader
    history=get_history(log_loc)
    if recovery(leader):
        ## start servering
        return
    else:
        ##newelection
        return

##server handler
class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        if setting['init'] == False:
            init()
            setting['init'] = True

        # self.request is the TCP socket connected to the client
        epoch = len(setting['history'])
        counter = len(setting['history'][epoch-1])
        
        data = self.request.recv(1024)
        if data == "":
            sys.exit()

        
        if ("Client" in self.data):
            message = self.data.split(' ', 1)[1]
            broadcast(message)

        elif ("ELECTION" in self.data):
            #checks if the id in the message is larger than current id
            #If the election requests Id is larger reply with a \n
            broadcast("Alive?")
            if setting['leader'] != 0 and (setting['leader'] in setting['neighbor']) == False:
                elected_msg = "ELECTED {}".format(setting['leader'])
                broadcast(server_list, elected_msg)

            else:
                setting['election'] = "waiting"
                if(int(self.data.split()[1]) > epoch):
                    self.request.send("\n")
                    
                elif (int(self.data.split()[1]) == epoch) and (int(self.data.split()[2]) > counter):
                    self.request.send("\n")
                    
                else:
                    self.request.send("Bigger zxid")
                    #start an election
                    election(self.server_id, epoch, counter, self.server_list)
                

        elif ("ELECTED" in self.data):
            leader = self.data.split()[1]
            setting['leader'] = leader
            setting['election'] = "following"
        """
        elif("CREATE" in self.data):

        elif("DELET" in self.data):

        elif("READ" in self.data):

        elif("WRITE" in self.data):
        

        elif("APPEND" in self.data):
        

        elif("EXIT" in self.data):
     
        else:
            return
        """

##main    
if __name__ == '__main__':
    server_id = sys.argv[1]
    
    #print "Setting up server on port: {}".format(port)
    host = socket.gethostbyname(DNS_3)
    server_num = 5
    ip_adds = [host]*server_num
    ports = range(4400, 4400+server_num)
    
    host = ip_adds[server_id-1]
    port = ports[server_id-1]
    server = ThreadedTCPServer((host,port), ThreadedTCPRequestHandler)
    ip, port = server.server_address
    server.server_id = server_id

    
    #server.port = ***********
    #server.leader = ***********
    for index in range(0, server_num):
        if index+1 != server_id:
            try:
                s = socket.socket()
                s.connect(ip_add[index], port[index])
                setting['neighbor'][index+1] = s
            except:
                setting['neighbor_failed'].append(index+1)
    """      
    server_thread = threading.Thread(target = server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    """

