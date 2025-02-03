import time
import socket
import asyncio
import threading
import collections

from app.constants import Constants
from app.utils import RDBParser, is_numeric

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
        if isinstance(value, list):
            return 'stream'
        
        return 'string' 

    def validate_stream(self, key: str, id: str) -> str:
        last_id = self.store[key][0][-1][0] if self.store[key][0] else '0-0'
        
        if id == '0-0':
            return Constants.ERROR_MIN_STREAM_ID
        if id <= last_id:
            return Constants.ERROR_STREAM_KEY

    def save_stream(self, key: str, entry_id: str, fields: list) -> None:
        if key not in self.store:
            self.save(key, [])

        if error:= self.validate_stream(key, entry_id):
            return error

        current_time = int(time.time() * 1000)
        self.store[key][0].append([entry_id, fields, current_time])
        return entry_id

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
    
    def generate_stream_entry_id(self, key: str, id: str) -> str:
        if '*' not in id:
            return id

        if id == '*':
            return str(int(time.time() * 1000)) + '-0'
        
        time_part = id.split('-')[0]
        if key in self.store:
            last_id = self.store[key][0][-1][0] if self.store[key][0] else '0-0'
            parts = last_id.split('-')
            
            if parts[0] == time_part:
                return f"{parts[0]}-{int(parts[1]) + 1}"
        
        return time_part + '-0' if int(time_part) > 0 else time_part + '-1'

    def get_stream_entries(self, key: str, start: str, end: str) -> list:
        if key not in self.store or self.store[key][0] is None or len(self.store[key][0]) == 0:
            return []
        
        # return all entries if start and end are open ranges
        if start == '-' and end == '+':
            return self.store[key][0]
        
        all_entries = self.store[key][0]
        result = []
        start_has_sequence = '-' in start
        end_has_sequence = '-' in end

        i = 0

        while i < len(all_entries):
            id = all_entries[i][0]
            # if start range has sequence then compare whole id, else compare only time part
            # exit the loop if entries are required from the beginning of the stream i.e. start is '-'
            if ((start_has_sequence and id < start) or (not start_has_sequence and id.split('-')[0] <= start)) and start != '-':
                i += 1
                continue
            break
        
        while i < len(all_entries):
            id = all_entries[i][0]
            # if end range has sequence then compare whole id, otherwise compare only time part
            # do not exit the loop if entries are required till the end of the stream i.e. end is '+'
            if ((end_has_sequence and id > end) or (not end_has_sequence and id.split('-')[0] > end)) and end != '+':
                break
            result.append(all_entries[i][:2])
            i += 1

        return result
    
    def read_multiple_streams(self, keys_and_ids: list) -> list:
        streams = []
        n = len(keys_and_ids) // 2

        for i in range(n):
            key = keys_and_ids[i]
            id = keys_and_ids[n + i]
            streams.append((key, id))

        result = []
        for stream_key, id in streams:
            entries = self.get_stream_entries(stream_key, id, '+')
            result.append([stream_key, entries])
        return result

    async def read_blocking_streams(self, keys_and_ids: list, timeout: int) -> list:
        task = asyncio.create_task(self.read_stream_task(keys_and_ids, timeout))
        return await task
        
    async def read_stream_task(self, keys_and_ids: list, timeout: int) -> list:
            current_time = int(time.time() * 1000)
            stream_key = keys_and_ids[0]
            last_entry = None

            
            if timeout == 0:
                timeout = float('inf') # block until new entry is added

            while int(time.time() * 1000) - current_time < timeout:
                all_entries = self.store[stream_key][0]
                last_entry = all_entries[-1] if len(all_entries) > 0 else None
                if last_entry and last_entry[2] > current_time:
                    break
                last_entry = None
                await asyncio.sleep(0.05)
            
            return [[stream_key, [last_entry[:2]]]] if last_entry else None
    
    def incr(self, key: str) -> int | str:
        if self.get_type(key) == 'none':
            self.save(key, '1')
            return 1

        if is_numeric(self.store[key][0]):
            curr_value, ttl = self.store[key]
            self.save(key, str(int(curr_value) + 1), ttl)
            return int(curr_value) + 1
        
        return Constants.ERROR_NON_INT