import sys
import socket

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, serialize_DEL
from node_ring import NodeRing
#Rhw for the part1
from RHW import Ring
#consistenthash for part2
from consistentHash import ConsistentHashRing
from lru import LRUCache
import bloomfilter
BUFFER_SIZE = 1024

class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)       

    def send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()


def process(udp_clients):
    client_ring = Ring(udp_clients) #For Part1 ONLY
    print(udp_clients)
    hash_codes = set()
    
    put(client_ring, hash_codes)
    hash_codes_tup = tuple(hash_codes)
    get(client_ring, hash_codes_tup)
    delete(client_ring, hash_codes)
'''
    # PUT all users.
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        response = client_ring.get_node(key).send(data_bytes)
        print(response)
        hash_codes.add(str(response.decode()))


    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={len(hash_codes)}")
    
    # GET all users.
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        response = client_ring.get_node(key).send(data_bytes)
        print(response)

    print("Got all the users")
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_DEL(hc)
        response = client_ring.get_node(key).send(data_bytes)
        print(response)
'''
def put (client_ring, hash_codes):
    for u in USERS:
        data_bytes, key = serialize_PUT(u)
        response = client_ring.get_node(key).send(data_bytes)
        print(response)
        hash_codes.add(str(response.decode()))
        

def get(client_ring, hash_codes):
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_GET(hc)
        
        response = client_ring.get_node(key).send(data_bytes)
        print(response)

def delete(client_ring, hash_codes):
    for hc in hash_codes:
        print(hc)
        data_bytes, key = serialize_DEL(hc)
        
        response = client_ring.get_node(key).send(data_bytes)
        print(response)


if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    process(clients)

