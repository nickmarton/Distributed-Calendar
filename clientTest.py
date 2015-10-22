import socket

HOST = "192.168.1.214"
PORT = 9000

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


sock.connect((HOST, PORT))

while(1):
    print("Client started")
    BUFFER_SIZE = 1024
    MESSAGE = raw_input("")

    if ( MESSAGE == "quit" or MESSAGE == "terminate"):
    	sock.send(MESSAGE)
    	break

    sock.send(MESSAGE)

    data = sock.recv(BUFFER_SIZE)
    data = data.decode('UTF-8')
    print "Recieved data:" + data

sock.close()
    