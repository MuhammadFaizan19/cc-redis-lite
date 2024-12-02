import time
import threading
import collections

from app.constants import Constants
from app.utils import RDBParser

class Store:
    def __init__(self) -> None:
        self.store = collections.defaultdict(lambda: ('', None))
    
    def save(self, key: str, value: str, ttl: int = None):
        self.store[key] = (value, ttl)
    
    def get(self, key: str) -> str | None:
        return self.store[key][0] if not self.is_expired(key) else ''

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
        

class State(Store):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.role = Constants.SLAVE if self.config['is_replica'] else Constants.MASTER
        self.buffers = {}
        self.repl_connections_lock = threading.Lock()
        self.repl_ports: list[(str, int)] = []
        self.replica_present = False
        self.master_repl_offset = 0
        self.load_rdb()
    
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
    
    def add_new_replica(self) -> int:
        self.replica_present = True
        id = len(self.buffers)
        self.buffers[id] = collections.deque([])
        return id

    def load_rdb(self):
        if not self.config['dir'] or not self.config['dbfilename']:
            return
        filepath = f"{self.config['dir']}/{self.config['dbfilename']}"
        items = RDBParser(filepath).parse()
    
        for key, (value, ttl) in items.items():
            self.save(key, value, ttl)
    
    def increment_repl_offset(self, bytes_processed: int):
        self.master_repl_offset += bytes_processed