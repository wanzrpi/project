from socket import * 

ipadd={1:('172.31.26.49', 8080)}
ipadd[2]=('172.31.26.49', 8060)
ipadd[3]=('172.31.43.12', 8080)
ipadd[4]=('172.31.43.12', 8060)
ipadd[5]=('172.31.11.239', 8060)
# CA instance
DNS_1 = 'ec2-54-67-101-22.us-west-1.compute.amazonaws.com'
# Virginia instance
DNS_2 = 'ec2-52-87-177-8.compute-1.amazonaws.com'
# Oregon instance
DNS_3 = 'ec2-52-37-12-187.us-west-2.compute.amazonaws.com'

host = socket.gethostbyname(DNS_3)
PORT = 4400

#addr = ipadd[int(raw_input('which node to connect?\n'))]
addr = (host, PORT)
bufsize = 1024 
client = socket(AF_INET,SOCK_STREAM) 
client.connect(addr)
client.send("client")
while True:
    data = client.recv(bufsize)
    if not data: 
        break 
    print data
    data = raw_input("create <filename> to create a file\ndelete <filename> to delete a file\nread <filename> to read a file\nappend <filename> <line> to append a line\n")
    if not data: 
        break 
    client.send(data) 
client.close() 