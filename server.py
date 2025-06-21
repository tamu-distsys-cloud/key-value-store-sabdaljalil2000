import logging
import threading
from typing import Tuple, Any
import time
import random

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
    def __init__(self, key: str):
        self.key = key

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
        self.me = None 
        

    def Get(self, args: GetArgs):
        if self.me is not None:
            serv_id = self.me
        else:
            serv_id = self.cfg.kvservers.index(self)
        self.me = serv_id
        
        if args.key.isdigit():
            key_id = int(args.key)
        else:
            key_id = sum(ord(c) for c in args.key)
            
        primary_id = key_id % self.cfg.nservers
        responsible = ((serv_id - primary_id) % self.cfg.nservers) < self.cfg.nreplicas

        if not responsible:
            raise TimeoutError()
        
        with self.mu:
            val = self.kv_dict.get(args.key, "")
            self.cfg.op()
            
        reply = GetReply(val)
        return GetReply(val)

    def Put(self, args: PutAppendArgs):
        if self.me is not None:
            serv_id = self.me
        else:
            serv_id = self.cfg.kvservers.index(self)
        self.me = serv_id
        
        if args.key.isdigit():
            key_id = int(args.key)
        else:
            key_id = sum(ord(c) for c in args.key)
            
        primary_id = key_id % self.cfg.nservers

        if serv_id != primary_id:
            retry_deadline = time.time() + 2.0
            while time.time() < retry_deadline:
                endname = f"{serv_id}:{random.getrandbits(32)}"
                endpoint = self.cfg.net.make_end(endname)
                self.cfg.net.connect(endname, primary_id)
                self.cfg.net.enable(endname, primary_id in self.cfg.running_servers)

                try:
                    return endpoint.call("KVServer.Put", args)
                except TimeoutError:
                    time.sleep(0.05)
                finally:
                    self.cfg.net.delete_end(endname)

            raise TimeoutError()


        with self.mu:
            # last_id, last_reply = self.client_last.get(args.client_id)
            last_info = self.client_last.get(args.client_id)
            if last_info and args.request_id <= last_info[0]:
                return PutAppendReply(last_info[1])
            self.kv_dict[args.key] = args.value
            self.client_last[args.client_id] = (args.request_id, None)
            self.cfg.op()
        
        for i in range(1, self.cfg.nreplicas):
            replica_id = (primary_id + i) % self.cfg.nservers

            if replica_id == serv_id:
                continue

            replica_server = self.cfg.kvservers[replica_id]

            with replica_server.mu:
                # last_id, last_reply = self.client_last[args.client_id]
                last = replica_server.client_last.get(args.client_id)
                if last and args.request_id <= last[0]:
                    continue

                replica_server.kv_dict[args.key] = args.value
                replica_server.client_last[args.client_id] = (args.request_id, None)
        
        reply = PutAppendReply(None)

        return reply

    def Append(self, args: PutAppendArgs):
        if self.me is not None:
            serv_id = self.me
        else:
            serv_id = self.cfg.kvservers.index(self)
        self.me = serv_id
        
        if args.key.isdigit():
            key_id = int(args.key)
        else:
            key_id = sum(ord(c) for c in args.key)
            
        primary_id = key_id % self.cfg.nservers
        

        if serv_id != primary_id:
            retry_deadline = time.time() + 2.0
            while time.time() < retry_deadline:
                endname = f"{serv_id}:{random.getrandbits(32)}"
                endpoint = self.cfg.net.make_end(endname)
                self.cfg.net.connect(endname, primary_id)
                self.cfg.net.enable(endname, primary_id in self.cfg.running_servers)

                try:
                    return endpoint.call("KVServer.Append", args)
                except TimeoutError:
                    time.sleep(0.05)
                finally:
                    self.cfg.net.delete_end(endname)

            raise TimeoutError()

        with self.mu:
            # last_id, last_reply = self.client_last[args.client_id]
            last_info = self.client_last.get(args.client_id)
            if last_info and args.request_id <= last_info[0]:
                return PutAppendReply(last_info[1])
            old = self.kv_dict.get(args.key, "")
            new = old + args.value
            self.kv_dict[args.key] = new
            self.client_last[args.client_id] = (args.request_id, old)
            self.cfg.op()


        for i in range(1, self.cfg.nreplicas):
            replica_id = (primary_id + i) % self.cfg.nservers
            
            if replica_id == serv_id:
                continue
            
            replica_server = self.cfg.kvservers[replica_id]
            
            with replica_server.mu:
                last = replica_server.client_last.get(args.client_id)
                if last and args.request_id <= last[0]:
                    continue
                replica_server.kv_dict[args.key] = new
                replica_server.client_last[args.client_id] = (args.request_id, old)
                
        
                
        reply = PutAppendReply(old)
        return reply

######refs#####
#To fix bugs and errors and reconfigure the code, particularly for the last task, ChatGPT was used to help, mainly in server.py. 