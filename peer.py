import socket
import threading
import time
import hashlib 
import random
from queue import Queue 
import re  

no_of_threads = 3                                          
jobs = [1, 2, 3]                                             
queue = Queue()                                            
MessageList = []                                            
MY_IP = "localhost"                                         
PORT = int(input())                                        
seeds_addr = set()                                         
MaxLimit=4
peers_connected = []                                       

connect_seed_addr = []                                     
peer_set_from_seed = set()                                 


class Peer: 
    notResponded = 1                                                  #To Check for 3 liveliness messages
    address = ""
    def __init__(self, addr): 
        self.address = addr 

file = open("/Users/deepanshubissu/Desktop/Assignment1/config.txt","r")
SeedList=file.read()
numOfSeeds = len(SeedList.split("\n"))                               #We ended each Seed node data in config.txt with new lines to Total no of seed nodes == no of Lines in That File
file.close()

#To Store all seed Node Details In seeds_addr
sList=SeedList.split('\n')
for addr in sList:
    addr = addr.split(":")                                  #Now addr is list of ["seedip","seedPort"]
    addr = "127.0.0.1:" + str(addr[1])                      #Replacing seed ip with local host to run in my pc
    seeds_addr.add(addr)                                    #Storing All Available Seed node adress

#Generate K random Number To Connect Given Seed addr and peers

def generate_k_random_numbers_in_range(lower, higher, k):
    # Check if k is greater than the range size
    range_size = higher - lower + 1
    if k > range_size:
        raise ValueError("k should be less than or equal to the range size")

    # Generate k random numbers without replacement
    random_numbers = random.sample(range(lower, higher + 1), k)
    return set(random_numbers)

        
# To handle different connected peers in different thread.It recieves messages from peer.
# According to the type of message received take appropriate actions
def handle_peer(conn, addr):
    while True:
        try:
            # Receive message from the connection
            message = conn.recv(1024).decode('utf-8')
            received_data = message
            if message:
                message_parts = re.split(r":",message)
                message_type = message_parts[0]
                
                if message_type == "New Connect Request From":      
                    if len(peers_connected) < MaxLimit:
                        conn.send("New Connect Accepted".encode('utf-8'))
                        new_peer = Peer(f"{addr[0]}:{message_parts[2]}")
                        peers_connected.append(new_peer)
                    else:
                        conn.send("Connection Not Accepted. Already More Than 4 Connections".encode('utf-8'))    
                
                elif message_type == "Live":            
                    # If it's a liveness request then give back liveness reply              
                    liveness_reply = f"Live:{message_parts[1]}:{message_parts[2]}:{MY_IP}"
                    conn.send(liveness_reply.encode('utf-8'))
                
                else:                 
                    # If it's a gossip message then forward it if it's not in ML list
                    forward_gossip_message(received_data)
        
        except Exception as e:
            print(f"Error handling peer: {e}")
            conn.close()
            break

            
# This function receives complete list of peers and set of random index of peers to connect to and connect to them
def connectPeers(complete_peer_list, selected_peer_nodes_index):
    for index in selected_peer_nodes_index:
        try:
            # Create a socket
            sock = socket.socket()
            
            # Split peer address into IP and port
            peer_addr = re.split(':', complete_peer_list[index])
            ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
            
            # Connect to the peer
            sock.connect(ADDRESS)
            
            # Add the connected peer to the list of connected peers
            peers_connected.append(Peer(complete_peer_list[index]))
            
            # Send new connection request message
            message = f"New Connect Request From:{MY_IP}:{PORT}"
            sock.send(message.encode('utf-8'))
            
            # Receive and print response
            print(sock.recv(1024).decode('utf-8'))
            
            # Close the socket
            sock.close()
        except Exception as e:
            print(f"Peer Connection Error: {e}")
            
# This function takes complete list of peers and find a random no. of peers to connect to b/w 1 and 4 and then generate a set of random no. that size    
def join4Peers(complete_peer_list):
    
    if len(complete_peer_list) > 0:
        # Determine the limit for the number of peers to connect to
        limit = min(MaxLimit, random.randint(1, len(complete_peer_list)))
        
        # Generate indices of randomly selected peers to connect to
        selected_peer_nodes_index = generate_k_random_numbers_in_range(0, len(complete_peer_list) - 1, limit)
        
        # Connect to the selected peers
        connectPeers(complete_peer_list, selected_peer_nodes_index)
    else:
        print("Peer List Is Empty. Nothing to Connect With")
        
# It take complete peer list separated by comma from each seed and union them all
def union_peer_lists(complete_peer_list):
    global MY_IP
    
    # Split complete_peer_list by ','
    complete_peer_list = re.split(r',', complete_peer_list)
    
    # Remove the last element and extract MY_IP from it
    temp = complete_peer_list.pop()
    temp = re.split(r":", temp)
    MY_IP = temp[0]
    
    # Add non-empty elements to peer_set_from_seed
    for i in complete_peer_list:
        if i:
            peer_set_from_seed.add(i)
    
    # Convert peer_set_from_seed to a list and return it
    complete_peer_list = list(peer_set_from_seed)
    return complete_peer_list

# This function is used to connect to seed and send our IP address and port info to seed and then receives a list of peers connected to that seed separated by comma



def connect_seeds():
    for seed_address in connect_seed_addr:
        try:
            # Create a socket object
            sock = socket.socket()
            
            # Split the seed address into IP and port
            seed_ip, seed_port = re.split(r':', seed_address)
            ADDRESS = (seed_ip, int(seed_port))
            
            # Connect to the seed
            sock.connect(ADDRESS)
            
            # Send your address to the seed
            MY_ADDRESS = f"{MY_IP}:{PORT}"
            sock.send(MY_ADDRESS.encode('utf-8'))
            
            # Receive the peer list from the seed
            message = sock.recv(10240).decode('utf-8')
            
            # Union received peer list with current peer list
            complete_peer_list = union_peer_lists(message)
            
            # Output and store peers
            for peer in complete_peer_list:
                print(peer)
                file = open("outputpeer.txt", "a")  
                file.write(peer + "\n")
                file.close()
            
            # Close the socket connection
            sock.close()
            
        except Exception as e:
            print(f"Seed Connection Error: {e}")

    # Join at most 4 peers
    if(len(complete_peer_list)>0):
        join4Peers(complete_peer_list)

    
    
# This function is used to register the peer to (floor(n / 2) + 1) random seeds
def ConnectWithseeds():
    global seeds_addr
    seeds_addr = list(seeds_addr)
    
    # Determine the number of seeds to connect to (k = floor(n / 2) + 1)
    k = len(seeds_addr) // 2 + 1
    
    # Generate indices of randomly selected seed nodes
    seed_nodes_index = generate_k_random_numbers_in_range(0, len(seeds_addr) - 1, k)
    seed_nodes_index = list(seed_nodes_index)
    
    # Add selected seed addresses to connect_seed_addr list
    for i in seed_nodes_index:
        connect_seed_addr.append(seeds_addr[i])
    
    # Connect with the selected seeds
    connect_seeds()

# This function takes address of peer which is down. Generate dead node message and send it to all connected seeds
def dead(peer):
    # Construct dead message
    dead_message = f"Dead Node:{peer}:{time.time()}:{MY_IP}"

    # Iterate through seed addresses and report dead node
    for seed in connect_seed_addr:
        try:
            # Create a socket
            sock = socket.socket()
            
            # Split seed address into IP and port
            seed_address = re.split(r':', seed)
            ADDRESS = (str(seed_address[0]), int(seed_address[1]))
            
            # Connect to the seed
            sock.connect(ADDRESS)
            
            # Send dead message
            sock.send(dead_message.encode('utf-8'))
            
            # Close the socket
            sock.close()
        except Exception as e:
            print(f"Seed Not Receiving dead node message {seed}: {e}")
    file = open("outputpeer.txt", "a")  
    file.write(dead_message + "\n") 
    file.close()
    print(dead_message)
            
# This function generates liveness request and it to all connected peers at interval of 13 sec
def liveness_testing():    
    while True:
        liveness_request = f"Live:{time.time()}:{MY_IP}"
        print(liveness_request)
        
        for peer in peers_connected:
            try:                
                # Create a socket
                sock = socket.socket()
                
                # Split peer address into IP and port
                peer_addr = re.split(r':', peer.address)
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                
                # Connect to the peer
                sock.connect(ADDRESS)
                
                # Send liveness request
                sock.send(liveness_request.encode('utf-8'))
                
                # Receive and print response
                print(sock.recv(1024).decode('utf-8'))
                
                # Close the socket
                sock.close()  
                
                # Reset consecutive failure count if liveness request succeeded
                peer.notResponded = 1
            except Exception as e:
                # Increment consecutive failure count for the peer
                peer.notResponded += 1
                
                # Report dead peer and remove from connected peer list if three consecutive failures
                if peer.notResponded == 4:
                    dead(peer.address)
                    peers_connected.remove(peer)
                    
        # Sleep for 13 seconds before next iteration
        time.sleep(13)


def generate_hash(message):
    # Create an MD5 hash object
    
    hash_object = hashlib.md5()
    
    # Update the hash object with the message bytes
    hash_object.update(message.encode())
    
    # Get the hexadecimal digest of the hash
    hash_hex_digest = hash_object.hexdigest()
    
    return hash_hex_digest       

def forward_gossip_message(received_message):
    # Generate hash for the received message
    message_hash = generate_hash(received_message) 
    
    # Check if the hash of the received message is already in Message List
    if message_hash in MessageList:
        print("Message Already Broadcasted")
    else:
        # Append the hash to Message List if it's received for the first time
        MessageList.append(str(message_hash))
        
        # Print and write the received message to the output file
        print(received_message)
        with open("outputpeer.txt", "a") as file:
            file.write(received_message + "\n")
        
        # Forward gossip message to all connected peers
        for peer in peers_connected:
            try:
                # Create a socket
                sock = socket.socket()
                
                # Split peer address into IP and port
                peer_addr = re.split(r':', peer.address)
                ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
                
                # Connect to the peer and send the message
                sock.connect(ADDRESS)
                sock.send(received_message.encode('utf-8'))
                sock.close()
            except Exception as e:
                # If an error occurs, continue to the next peer
                print(f"Error forwarding gossip message to {peer.address}: {e}")
                continue

# Generate gossip message and send it to connected peers
def generate_send_gossip_message(i):
    # Construct gossip message
    gossip_message = f"{time.time()}:{MY_IP}:{PORT}:gossip{i+1}"
    
    # Add hash of generated gossip to MessageList
    MessageList.append(str(generate_hash(gossip_message)))
    
    # Iterate through connected peers and send the gossip message
    for peer in peers_connected:
        try:
            # Create a socket
            sock = socket.socket()
            
            # Split peer address into IP and port
            peer_addr = peer.address.split(":")
            ADDRESS = (str(peer_addr[0]), int(peer_addr[1]))
            
            # Connect to the peer
            sock.connect(ADDRESS)
            
            # Send the gossip message
            sock.send(gossip_message.encode('utf-8'))
            
            # Close the socket
            sock.close() 
        except Exception as e:
            print(f"Peer Not Receiving Message: {peer.address}, Error: {e}")
# Generate 10 gossip messages at an interval of 5 sec each
def gossip():
    for i in range(10):
        generate_send_gossip_message(i)
        time.sleep(5)
        
#  work(), create_jobs() : Referred from internet and CHAT GPT
# Source : https://github.com/attreyabhatt/Reverse-Shell/blob/master/Multi_Client%20(%20ReverseShell%20v2)/server.py


# Do next job that is in the queue (handle connections, liveness testing, gossip)
def work():
   while True:
       task = queue.get()
       if task == 1:
           global sock
           sock = socket.socket()
           sock.bind((MY_IP, PORT))
           sock.listen(MaxLimit)
           print("Peer is Listening")
           while True:
                conn, addr = sock.accept()
                sock.setblocking(1)
                thread = threading.Thread(target = handle_peer, args = (conn,addr))
                thread.start()
       elif task == 2:
           liveness_testing()
       elif task == 3:
           gossip() 
       queue.task_done()

# Create jobs in queue
def create_jobs():
    # Put each job number into the queue
    global jobs
    for job_number in jobs:
        queue.put(job_number)
    
    # Wait for all jobs to be processed
    queue.join()


ConnectWithseeds()                        # Where k = floor(n / 2) + 1

for _ in range(no_of_threads):
    thread = threading.Thread(target = work)
    thread.daemon = True
    thread.start()
create_jobs()
