import mmh3
import socket
import struct
from server_config import NODES

def ip2long(ip):
    """Convert an IP string to long."""
    packedIP = socket.inet_aton(ip)
    return struct.unpack("!L", packedIP)[0]


def murmur(key):
    """Return murmur3 hash of the key as 32 bit signed int."""
    return mmh3.hash(key)


def weight(node, key):
    """Return the weight for the key on node.
    Uses the weighing algorithm as prescibed in the original HRW white paper.
    @params:
        node : 32 bit signed int representing IP of the node.
        key : string to be hashed.
    """
    a = 1103515245
    b = 12345
    hash = murmur(key)
    return (a * ((a * node + b) ^ hash) + b) % (2^31)


class Ring():
    """A ring of nodes supporting rendezvous hashing based node selection."""
    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes

    def get_node(self, key):
        """Return the node to which the given key hashes to."""
        assert len(self.nodes) > 0
        weights = []
        for node in self.nodes:
            n = int(key, 16)
            w = weight(n, key)
            weights.append((w, node))
        max = 0
        maxind = -1
        for i in range (len(weights)):
            if weights[i][0] > max:
                max = weights[i][0]
                maxind = i
        node = weights[maxind][1]
        return node