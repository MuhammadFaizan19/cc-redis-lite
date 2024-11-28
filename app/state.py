import time
import argparse
import threading
import collections
from typing import List, Tuple

from app.rdb_parser import RDBParser
from app.connection import Connection
from app.utils import generate_random_string

class State:
    def __init__(self):
        self.store = collections.defaultdict(str)
        self.conf = { 'host': 'localhost' }
        self.repl = { 'role': 'master' }
        self.load_default()
    
    def load_default(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('--port', type=int)
        parser.add_argument('--dir', type=str)
        parser.add_argument('--dbfilename', type=str)
        parser.add_argument('--replicaof', type=str)

        if args := parser.parse_args():
            self.conf['port'] = args.port or 6379
            self.conf['dir'] = args.dir
            self.conf['dbfilename'] = args.dbfilename
            if args.dir and args.dbfilename: 
                self.load_rdb(f"{self.conf['dir']}/{self.conf['dbfilename']}")
            if args.replicaof:
                host, path = args.replicaof.split(' ')
                self.repl['role'] = 'slave' if args.replicaof else 'master'
                master_connection = Connection(host, path)
                master_connection.handshake(self.conf['port'])
            self.repl['master_replid'] = generate_random_string(40)
            self.repl['master_repl_offset'] = 0
    
    def set_replication_info(self, items: List[Tuple[str, str]]):
        for key, value in items:
            self.replication_info[key] = value
    
    def set_store(self, items: List[Tuple[str, str]]):
        for key, value in items:
            self.store[key] = value
    
    def set_store_key_expiry(self, items: List[Tuple[str, int]]):
        current_time_ms = int(time.time() * 1000)
        for key, exp in items:
            if exp < current_time_ms:
                self.store.pop(key)
                continue
            threading.Timer((exp - int(time.time() * 1000)) / 1000, lambda: self.store.pop(key)).start()
    
    def delete_key(self, key: str):
        self.store.pop(key)

    def load_rdb(self, filepath: str):
        items = RDBParser(filepath).parse()
        
        self.set_store([(key, val) for key, (val, exp) in items.items()])
        self.set_store_key_expiry([(key, exp) for key, (val, exp) in items.items() if exp])