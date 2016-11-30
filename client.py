from socket import * 

server_addr = {1:("127.0.0.1",8010),
               2:("127.0.0.1",8020),
               3:("127.0.0.1",8030),
               4:("127.0.0.1",8040),
               5:("127.0.0.1",8050)}


addr = server_addr[int(raw_input('which node to connect?\n'))]
bufsize = 1024 
client = socket(AF_INET,SOCK_STREAM) 
client.connect(addr)
client.send("Client")
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

