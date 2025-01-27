import time
import socket
import threading
import collections

from app.constants import Constants
from app.utils import RDBParser

class Store:
    def __init__(self) -> None:
        self.store = collections.defaultdict(lambda: (None, None))
    
    def save(self, key: str, value: str, ttl: int = None):
        self.store[key] = (value, ttl)
    
    def get(self, key: str) -> str | None:
        return self.store[key][0] if not self.is_expired(key) else None

    def delete(self, key: str) -> None:
        self.store.pop(key, None)
    
    def is_expired(self, key: str) -> bool:
        ttl = self.store[key][1]
        if ttl and ttl < time.time() * 1000:
            self.delete(key)
            return True
        return False

    def exists(self, key: str) -> bool:
        return key in self.store
    
    def keys(self) -> list[str]:
        return list(self.store.keys())
    
    def flush(self) -> None:
        self.store.clear()
    
    def get_type(self, key: str) -> str:
        if key not in self.store or self.is_expired(key):
            return 'none'
        
        value = self.store[key][0]
        if isinstance(value, dict):
            return 'stream'
        
        return 'string' 

    def save_stream(self, key: str, id: str, key_value_pairs) -> None:
        if key not in self.store:
            self.save(key, {})
        self.store[key][0][id] = key_value_pairs

class State(Store):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.role = Constants.SLAVE if self.config['is_replica'] else Constants.MASTER
        self.buffers = {}
        self.replica_present = False
        self.offset_lock = threading.Lock()
        self.master_repl_offset = 0
        self.repl_connections_lock = threading.Lock()
        self.repl_connections: list[socket.socket] = []
        self.ack_count_lock = threading.Lock()
        self.ack_count = 0
        self.load_rdb_file()
    
    def get_config(self, key: str) -> str | None:
        return self.config[key] if key in self.config else ''

    def get_info(self):
        return ''.join([
            f'role:{self.role}\r\n',
            f'master_replid:{self.config["master_replid"]}\r\n',
            f'master_repl_offset:{self.master_repl_offset}\r\n'
        ])

    def is_master(self):
        return self.role == Constants.MASTER
        
    def add_command_buffer(self, command):
        for k,_ in self.buffers.items():
            self.buffers[k].append(command)        
        return 0
    
    def add_new_replica(self, connection: socket.socket) -> int:
        self.replica_present = True
        with self.repl_connections_lock:
            self.buffers[connection] = collections.deque([])
            self.repl_connections.append(connection)
        return id

    def load_rdb_file(self):
        if not self.config['dir'] or not self.config['dbfilename']:
            return
        filepath = f"{self.config['dir']}/{self.config['dbfilename']}"
        data = RDBParser.read_rdb(filepath)
        self.load_rdb(data)
    
    def load_rdb(self, data: bytes):
        parser = RDBParser()
        items = parser.parse(data)
    
        for key, (value, ttl) in items.items():
            self.save(key, value, ttl)

    def increment_repl_offset(self, bytes_processed: int):
        with self.offset_lock:
            self.master_repl_offset += bytes_processed
    
    def increment_ack_count(self):
        with self.ack_count_lock:
            self.ack_count += 1

    def reset_ack_count(self):
        with self.ack_count_lock:
            self.ack_count = 0

    def get_ack_count(self):
        with self.ack_count_lock:
            return self.ack_count
    
    def add_stream(self, key: str, id: str, fields) -> None:
        key_value_pairs = {}
        for i in range(0, len(fields), 2):
            key_value_pairs[fields[i]] = fields[i+1]
       
        self.save_stream(key, id, key_value_pairs)
