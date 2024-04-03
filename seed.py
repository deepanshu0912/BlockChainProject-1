import socket
import threading

MY_IP = "localhost"                                               # IP for listening
PORT = int(input())                                             # Port No. for listening               
peer_list = []

socket = socket.socket()

ADDRESS = (MY_IP, PORT)
socket.bind(ADDRESS)

def remove_dead_node(message):
    print("Deleted Message is", message)
    
    # Write the dead node message to the output file
    with open("outputseed.txt", "a") as file:
        file.write(message + "\n")
    
    # Split the message
    message_parts = message.split(":")
    dead_node = f"{message_parts[1]}:{message_parts[2]}"
    
    # Remove the dead node from the peer list if it exists
    if dead_node in peer_list:
        peer_list.remove(dead_node)


def handle_peer(conn, addr):
    while True:
        try:
            message = conn.recv(1024).decode('utf-8')      
            if message:
                if "Dead Node" == message[0:9]:
                    remove_dead_node(message)
                else:
                    message = message.split(":")
                    peer_list.append(str(addr[0])+":"+str(message[1]))
                    output = "Received Connection from " + str(addr[0])+":"+str(message[1])
                    print(output)
                    file = open("outputseed.txt", "a")  
                    file.write(output + "\n") 
                    file.close()
                    PeerList=""
                    for i in peer_list:
                        PeerList+=(i+ "," )
                    conn.send(PeerList.encode('utf-8'))
        except:
            break
    conn.close()
print("Seed is Listening")
socket.listen(5)
while True:
        conn, addr = socket.accept()
        thread = threading.Thread(target=handle_peer, args=(conn,addr))
        thread.start()