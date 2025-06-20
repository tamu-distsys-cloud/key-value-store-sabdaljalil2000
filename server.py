import logging
import threading
from typing import Dict, Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    def __init__(self, key, value, client_id, request_id):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.request_id = request_id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    def __init__(self, key, client_id, request_id):
        self.key = key
        self.client_id = client_id
        self.request_id = request_id

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv_dict = {}
        self.client_last = {}
        self.nshards = cfg.nservers
        self.nreplicas = cfg.nreplicas

    def Get(self, args: GetArgs):
        server_id = self.cfg.kvservers.index(self)
        try:
            shard = int(args.key) % self.nshards
        except ValueError:
            shard = ord(args.key[0]) % self.nshards
        group = []
        for i in range(self.nreplicas):
            group.append((shard + i) % self.nshards)

        if server_id not in group:
            return None
        primary_id = None
        for rid in group:
            if rid in self.cfg.running_servers:
                primary_id = rid
                break
        # if primary_id != server_id:
        #     return None
        with self.mu:
            if args.client_id in self.client_last:
                last_id, last_reply = self.client_last[args.client_id]
                if args.request_id <= last_id:
                    return last_reply
            val = self.kv_dict.get(args.key, "")
            reply = GetReply(val)
            self.client_last[args.client_id] = (args.request_id, reply)
            return reply


    def Put(self, args: PutAppendArgs):
        server_id = self.cfg.kvservers.index(self)
        try:
            shard = int(args.key) % self.nshards
        except ValueError:
            shard = ord(args.key[0]) % self.nshards
        group = []
        for i in range(self.nreplicas):
            group.append((shard + i) % self.nshards)
        if server_id not in group:
            return None
        primary_id = None
        for rid in group:
            if rid in self.cfg.running_servers:
                primary_id = rid
                break
        # if primary_id != server_id:
        #     return None
        with self.mu:
            if args.client_id in self.client_last:
                last_id, last_reply = self.client_last[args.client_id]
                if args.request_id <= last_id:
                    return last_reply
            self.kv_dict[args.key] = args.value
            reply = PutAppendReply("")
            self.client_last[args.client_id] = (args.request_id, reply)
        for rid in group:
            if rid == server_id or rid not in self.cfg.running_servers:
                continue
            replica = self.cfg.kvservers[rid]
            with replica.mu:
                last = replica.client_last.get(args.client_id, (-1, None))[0]
                if args.request_id > last:
                    replica.kv_dict[args.key] = args.value
                    replica.client_last[args.client_id] = (args.request_id, PutAppendReply(""))
        return reply

    def Append(self, args: PutAppendArgs):
        server_id = self.cfg.kvservers.index(self)
        try:
            shard = int(args.key) % self.nshards
        except ValueError:
            shard = ord(args.key[0]) % self.nshards
        group = []
        for i in range(self.nreplicas):
            group.append((shard + i) % self.nshards)
            
        if server_id not in group:
            return None
        primary_id = None
        for rid in group:
            if rid in self.cfg.running_servers:
                primary_id = rid
                break
        # if primary_id != server_id:
        #     return None
        with self.mu:
            if args.client_id in self.client_last:
                last_id, last_reply = self.client_last[args.client_id]
                if args.request_id <= last_id:
                    return last_reply
            old = self.kv_dict.get(args.key, "")
            new = old + args.value
            self.kv_dict[args.key] = new
            reply = PutAppendReply(old)
            self.client_last[args.client_id] = (args.request_id, reply)
        for rid in group:
            if rid == server_id or rid not in self.cfg.running_servers:
                continue
            replica = self.cfg.kvservers[rid]
            with replica.mu:
                last = replica.client_last.get(args.client_id, (-1, None))[0]
                if args.request_id > last:
                    rep_old = replica.kv_dict.get(args.key, "")
                    replica.kv_dict[args.key] = rep_old + args.value
                    replica.client_last[args.client_id] = (args.request_id, PutAppendReply(rep_old))
        return reply

######refs#####
#for the fifth test, i kept getting the error ValueError: invalid literal for int() with base 10: 'k'. to fix it, I used CHatGPT, which suggested to change 'shard = int(key) % nshards to try except catch error 
