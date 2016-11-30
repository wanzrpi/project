from socket import *
import errno
from socket import error as socket_error
from SocketServer import ThreadingTCPServer, StreamRequestHandler 
import json
import time
import sys

# CA instance
DNS_1 = 'ec2-54-67-101-22.us-west-1.compute.amazonaws.com'
# Virginia instance
DNS_2 = 'ec2-52-87-177-8.compute-1.amazonaws.com'
# Oregon instance
DNS_3 = 'ec2-52-37-12-187.us-west-2.compute.amazonaws.com'
PORT = 4400

server_addr = {1:("127.0.0.1",8010),
               2:("127.0.0.1",8020),
               3:("127.0.0.1",8030),
               4:("127.0.0.1",8040),
               5:("127.0.0.1",8050)}
server_id = 0
server_num=3
DEBUG = 1
log_loc=""
#leader_status: leading/following/waiting
setting = {'TODO':[],'init':'false','leader':0,'file_sys':{}, 'neighbor':{},
           'neighbor_failed' : [], 'history':[],'applied':[], 'election':'',
           'proposed':{},'zxid':{}}


def create(file_name):
    setting['file_sys'][file_name]=""
    return "file {} created successfully!".format(file_name)


def delete(file_name): 
    del setting['file_sys'][file_name]
    return "file {} deleted successfully!".format(file_name)


def read(file_name):
    print setting['file_sys']
    print setting['file_sys'][file_name]
    return "{}={}\n".format(file_name,setting['file_sys'][file_name])

def append(file_name, msg):
    setting['file_sys'][file_name] += msg
    return "message appended successfully!"


def check_command(command):
    passed = True
    msg = ""
    command = command.split(' ',2)
    if (command [0] not in ['create','read','append','delete']):
        passed = False
        msg = "unknown command"
    if (command[0] == 'create') and (command[1] in setting['file_sys']):
        passed = False
        msg = "file {} already exists!".format(command[1])
    if (command[0] == 'delete') and (command[1] not in setting['file_sys']):
        msg =  "file {} does not exist!".format(command[1])
        passed = False
    if (command[0] == 'read') and (command[1] not in setting['file_sys']):
        msg =  "file {} does not exist!".format(command[1])
        passed = False
    if (command[0] == 'append') and (command[1] not in setting['file_sys']):
        msg =  "file {} does not exist!".format(command[1])
        passed = False    
    return (passed,msg)
    
def process_command(command):
    command = command.split(' ',1)
    if command[0] == 'create':
        return create(command[1])
    elif command[0] == 'delete':
        return delete(command[1])
    elif command[0] == 'append':
        return append(command[1].split(' ',1)[0], command[1].split(' ',1)[1])

        
def add_history(lines):
    msg=lines.split(' ', 2)
    epoch=int(msg[0])
    counter=int(msg[1])
    operation=msg[2]
    if epoch > len(setting['history']):
        setting['history'].append([])
    setting['history'][epoch-1].append(msg[2])
    setting['zxid'][0]=epoch
    setting['zxid'][1]=counter
    
def get_history():
    f = open(log_loc, 'r')
    for lines in f.readlines():
        add_history(lines)
    f.close()


def get_history_max():
    history_max=[]
    for i in setting['history']:
        history_max.append(len(i))
    return history_max

def write_history(line):
     f=open(log_loc, 'a')
     f.write(line)
     f.close()
     
def write_new_history():
    f=open(log_loc, 'w')
    for i in range (0,len(setting['history'])):
        for j in range (0,len(setting['history'][i])):
            f.write('{} {} {}\n'.format(i+1, j+1, setting['history'][i][j]))
    f.close()

def recv_update(rserver, wserver):
    if setting['leader']!=server_id:
        wserver.write('NOT LEADER')
        return True
    else:
        wserver.write('history_match ACK')
        (epoch, counter) = json.loads(rserver.recv(1024))
        zxid = [epoch, len(setting['history'][epoch-1])]
        msg = json.dumps(zxid)
        print msg
        wserver.write(msg)
        msg = rserver.recv(1024)
        if msg !='accept':
            return False
        new_history=[]
        for i in range (counter,len(setting['history'][epoch-1])):
            new_history.append("{} {} {}".format(epoch, i+1,
                                                 setting['history'][epoch-1][i]))
        for i in range (epoch, setting['zxid'][0]):
            for j in range (0, len(setting['history'][i])):
                new_history.append("{} {} {}".format(i+1, j+1,
                                                     setting['history'][i][j]))
        wserver.write(len(new_history))
        for i in new_history:
            wserver.write(i)
            msg = rserver.recv(1024)
            if msg != 'accept':
                return False
        print "my_history={}\n".format(setting['history'])
        return True
        
def update(leader, history_max):
    msg = json.dumps(history_max)
    leader.send('history_match')
    rcv = leader.recv(1024)
    print rcv
    new_history=[]
    if rcv == 'history_match ACK':
        leader.send(msg)
        last_max = json.loads(leader.recv(1024))
        leader.send('accept')
        num_history = int(leader.recv(1024))
        leader.send('accept')
        for i in range (0,num_history):
            new_history.append(leader.recv(1024))
            leader.send('accept')
        return (last_max, new_history)
    elif rcv == 'NOT LEADER':
        return (None,None)
    
def recovery(leader, epoch, counter):
    (last_max, new_history) = update(leader,(epoch,counter))
    if last_max == None:
        return False
    (new_epoch,new_counter) = last_max
    print "history={}\nzxid={}\nlast_max={}\n".format(setting['history'],setting['zxid'],last_max)
    for i in range (new_counter,len(setting['history'][new_epoch-1])):
        setting['history'][new_epoch-1].pop(len(setting['history'][new_epoch-1])-1)
    for lines in new_history:
        add_history(lines)
    write_new_history()
    return True
        
def election(epoch, counter):
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

    for neighbor_id, neighbor in setting['neighbor'].items():
        try:
            neighbor.send(election_msg)
            response = neighbor.recv(1024)
            print "response {} = {}\n".format(neighbor_id,response)
            if(not response == "wait"):
                has_highest_id = False
            if ("ELECTED" in response):
                leader = int(response.split()[1])
                setting['leader'] = leader
                setting['election'] = "following"
        except:
            print ("Error sending election\n")

    if (has_highest_id == True):
        elected_msg = "ELECTED {}".format(server_id)
        if setting['leader'] != server_id:
            setting['leader'] = server_id
            setting['election'] = "leading"
            setting['history'].append([])
            setting['zxid'][0]+=1
            setting['zxid'][1]=0
            if DEBUG:
                print "LEADING NOW\n"
        broadcast(elected_msg)


##functions
def broadcast(msg):
    for index in setting['neighbor_failed']:
        try:
            s = socket() 
            s.connect(server_addr[index])
            s.send("Server {}".format(server_id))
            setting['neighbor'][index] = s
            setting['neighbor_failed'].remove(index)
        except:
            print "reconnection failed\n"
    for neighbor_id, neighbor in setting['neighbor'].items():
        try:
            neighbor.send(msg)
        except:
            setting['neighbor_failed'].append(neighbor_id)
            del setting['neighbor'][neighbor_id]

def init(): ##init when process starts
    setting['init'] = 'true'
    ##get_history(log_loc)
    epoch = len(setting['history'])
    counter = 0
    if epoch != 0:
        counter = len(setting['history'][epoch-1])
    setting['zxid'][0] = epoch
    setting['zxid'][1] = counter
    for index in range(0, server_num):
        if index+1 != server_id:
            try:
                s = socket() 
                s.connect(server_addr[index+1])
                setting['neighbor'][index+1] = s
                s.send("Server {}".format(server_id))
            except:
                setting['neighbor_failed'].append(index+1)
                print "connecting failed\n"
    if (setting['leader']==0)and(setting['election']!='waiting'):
        election(epoch, counter)
    recovered = False
    if setting['leader']==server_id:
        recovered = True
    while not recovered:
        time.sleep(2)
        if setting['leader']!=0:
            recovered=recovery(setting['neighbor'][setting['leader']],
                               epoch,counter)
        else:
            election(epoch,counter)
    for lines in setting['history']:
        for commands in lines:
            msg = process_command(commands)
    print "new_history={}\n".format(setting['history'])
    
##server handler
class ThreadedTCPRequestHandler(StreamRequestHandler):
    def handle(self):
        data = self.request.recv(1024)
        print "first data recv:{}\n".format(data)
        if setting['init'] == 'false':
            init()
            print "init successful\n"
        if (data == 'Client'):
            if DEBUG:
                print "Get message from Client!\n"
            self.wfile.write('Accepted')
            while 1:
                time.sleep(2)
                data = self.request.recv(1024)
                print "Message from Client = {}\n".format(data)
                if data == '':
                    break
                if setting['leader'] == 0:
                    election(setting['zxid'][0], setting['zxid'][1])
                leader = setting['leader']
                (res,msg) = check_command(data)
                if (not res):
                    self.wfile.write(msg)
                elif data.split(' ')[0]=='read':
                    self.wfile.write(read(data.split(' ')[1]))
                else:
                    if leader == server_id:
                        setting['zxid'][1]+=1
                        msg ="{} {} {}".format(setting['zxid'][0],
                                               setting['zxid'][1], data)
                        setting['proposed'][msg] = 1
                        message = "Request {}".format(data)
                        broadcast(message)
                        time.sleep(2)
                    else:
                        try:
                            fwd = "Forward "+data
                            setting['neighbor'][leader].send(fwd)
                            self.wfile.write("Request submitted!")
                        except:
                            del setting['neighbor'][setting['leader']]
                            setting['neighbor_failed'].append(setting['leader'])
                            setting['leader']=0
                            setting['election']=''
                            self.wfile.write("leader down, wait!") 
                            election(setting['zxid'][0], setting['zxid'][1])                           

        if (data.split()[0] == 'Server'):
            their_id = int(data.split()[1])
            while 1:
                data = self.request.recv(1024)
                if data == "":
                    break
                print ("recv msg in server {} handler:{}\n").format(
                    their_id,data)
                data = data.split (' ',1)
                if ("Forward" == data[0]):
                    if (server_id != setting['leader']):
                        self.wfile.write("ELECTED {}".format(setting['leader']))
                    else:
                        setting['zxid'][1] += 1
                        msg = "{} {} {}".format(setting['zxid'][0],
                                                setting['zxid'][1],data[1])
                        setting['proposed'][msg] = 1
                        message = "Request {}".format(msg)
                        broadcast(message)
                        time.sleep(2)

                elif ("history_match" == data[0]):
                    if recv_update(self.request, self.wfile):
                        print "Sync success \n"
                    else:
                        print "Sync fail\n"
                elif ("ELECTED" == data[0]):
                    if setting['leader'] != int(data[1]):
                        setting['leader'] = int(data[1])
                        if setting['leader'] == server_id:
                            setting['election']='leading'
                            setting['history'].append([])
                            setting['zxid'][0]+=1
                            setting['zxid'][1]=0
                            print"My election status is leading\n"
                        else:
                            print"My election status is following\n"
                            setting['election']='following'

                elif ("ELECTION" == data[0]):
                    #checks if the id in the message is larger than current id
                    if setting['leader'] != 0 and (setting['leader'] in setting['neighbor_failed']) == False:
                        elected_msg = "ELECTED {}".format(setting['leader'])
                        self.wfile.write(elected_msg)
                    else:
                        zxid = data[1].split()
                        setting['election'] = "waiting"
                        if(int(zxid[0]) > setting['zxid'][0]):
                            self.wfile.write("wait")
                            
                        elif (int(zxid[0]) == setting['zxid'][0]) and (int(zxid[1]) >= setting['zxid'][1]):
                            self.wfile.write("wait")
                        else:
                            self.wfile.write("Bigger zxid")
                            #start an election
                            election(setting['zxid'][0], setting['zxid'][1])

                elif ("Request" == data[0]):
                    msg = data[1].split(' ',2)
                    if int(msg[0]<len(setting['history'])):
                        setting['neighbor'][their_id].send(
                            "ELECTED {}".format(setting['leader']))
                    else:
                        setting['neighbor'][their_id].send(
                            "ACK {}".format(data[1]))
                        
                elif ("ACK" == data[0]):
                    if data[1] in setting['proposed']:
                        setting['proposed'][data[1]]+=1
                        if setting['proposed'][data[1]]> (server_num-1)/2:
                            setting['TODO'].append(data[1])
                            del setting['proposed'][data[1]]
                            for i in setting['TODO']:
                                add_history(i)
                                write_history(i)
                                print process_command(i.split(' ',2)[2])
                                setting['TODO'].pop(0)
                            commit_msg = "Commit {}".format(data[1])
                            broadcast(commit_msg)
                            print "new_history={}\n".format(setting['history'])
                        time.sleep(2)
                        
                elif  ("Commit" == data[0]):
                    setting['TODO'].append(data[1])
                    for i in setting['TODO']:
                        add_history(i)
                        write_history(i)
                        print process_command(i.split(' ',2)[2])
                        setting['TODO'].pop(0)
                    print "new_history={}\n".format(setting['history'])
                    time.sleep(2)

##main    
if __name__ == '__main__':
    server_id = int(sys.argv[1])
    log_loc = "log{}.txt".format(server_id)
    if server_id == 1:
        setting['history']=[['create aa','create ab'],['create ac']]
    if server_id == 2:
        setting['history']=[['create aa','create ab','create ae']]
    if server_id == 3:
        setting['history']=[['create aa','create ab'],['create ac','delete aa']]
    server = ThreadingTCPServer(server_addr[server_id], ThreadedTCPRequestHandler)
    server.serve_forever()


