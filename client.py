import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.client_id = nrand()
        self.request_id = 0
        self.lock = threading.Lock()

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        args = GetArgs(key)
        nshards = self.cfg.nservers
        nreplicas = self.cfg.nreplicas
        # shard = int(key) % nshards
        try:
            shard = int(key) % nshards
        except ValueError:
            shard = ord(key[0]) % nshards
        servers_l = [(shard + i) % nshards for i in range(nreplicas)]
        while True:
            for i in servers_l:
                try:
                    reply = self.servers[i].call("KVServer.Get", args)
                    if reply is not None and isinstance(reply, GetReply):
                        return reply.value
                except TimeoutError:
                    continue
        return ""

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        with self.lock:
            self.request_id += 1
            req_id = self.request_id

        args = PutAppendArgs(key, value, self.client_id, req_id)
        nshards = self.cfg.nservers
        nreplicas = self.cfg.nreplicas
        # shard = int(key) % nshards
        try:
            shard = int(key) % nshards
        except ValueError:
            shard = ord(key[0]) % nshards
        servers_l = [(shard + i) % nshards for i in range(nreplicas)]
        while True:
            for i in servers_l:
                try:
                    reply = self.servers[i].call("KVServer."+op, args)
                    if reply is not None and isinstance(reply, PutAppendReply):
                        return reply.value
                except TimeoutError:
                    continue
        return ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")

    
######refs#####

#for the fifth test case, i kept getting the error ValueError: invalid literal for int() with base 10: 'k'. to fix it, I used ChatGPT, which suggested to change shard = int(key) % nshards to try except catch error 
