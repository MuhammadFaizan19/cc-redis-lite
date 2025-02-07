import time
import asyncio

from app.constants import Constants, ValueTypes
from app.context.config import load_config
from app.context.stream_store import StreamStore
from app.context.replication_manager import ReplicationManager
from app.utils import RDBParser, is_numeric

class State(StreamStore, ReplicationManager):
    def __init__(self):
        StreamStore.__init__(self)
        ReplicationManager.__init__(self)
        
        self.config = load_config()
        self.role = Constants.SLAVE if self.config.get("is_replica") else Constants.MASTER
        self.load_rdb_file()
    
    def is_master(self) -> bool:
        return self.role == Constants.MASTER

    def get_config(self, key: str) -> str | None:
        return self.config.get(key, "")

    def get_info(self):
        return ''.join([
            f'role:{self.role}\r\n',
            f'master_replid:{self.config.get("master_replid", "")}\r\n',
            f'master_repl_offset:{self.master_repl_offset}\r\n'
        ])
    
    def get_type(self, key: str) -> str:
        if self.is_expired(key) or self.get(key) is None:
            return ValueTypes.NONE
        
        return ValueTypes.STREAM if isinstance(self.store[key][0], list) else ValueTypes.STRING

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

    def incr(self, key: str) -> int | str:
        if self.get_type(key) == ValueTypes.NONE:
            self.save(key, '1')
            return 1

        value, ttl = self.store[key]
        
        if is_numeric(value):
            new_value = int(value) + 1
            self.save(key, str(new_value), ttl)
            return new_value

        return Constants.ERROR_NON_INT


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