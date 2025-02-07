import time
import select
import socket
import asyncio
from threading import Thread

from app.context import State
from app.utils import RESPParser
from app.constants import Constants

class Controller(Thread):
    def __init__(self, state: State, connection: socket.socket = None) -> None:
        super().__init__()
        self.state = state
        self.connection = connection if connection or self.state.is_master() else self.handshake()
        self.talking_to_replica = False

    def run(self):
        buffer = b''
        while True:
            try:
                if self.state.is_master() and self.talking_to_replica:
                    break

                raw_message = self.connection.recv(8000)
                if not raw_message:
                    break

                buffer += raw_message
                commands, buffer = RESPParser.decode(buffer)
                
                for command, _ in commands:
                    result = self.process_command(command)
                    
                    for message in result:
                        self.send(message)


            except Exception as e:
                print(f'Error processing command: {e}')
                self.connection.sendall(RESPParser.encode(f'-Err: {e}').encode())
                break
        
        if self.talking_to_replica and self.state.is_master():
            self.run_sync_replica()
        self.connection.close()

    
    def process_command(self, command: list) -> list:
        if (
            self.state.role == Constants.MASTER and
            self.state.replica_present and
            any(key in command for key in [Constants.SET, Constants.DEL, Constants.INCR])
            ):
            self.state.add_command_buffer(command)

        match command:
            case [Constants.PING]:
                return ['PONG']

            case [Constants.ECHO, message]:
                return [message]
            
            case [Constants.GET, key]:
                return [self.state.get(key)]
            
            case [Constants.SET, key, value]:
                self.state.save(key, value)
                return [Constants.OK] if self.state.is_master() else []
            
            case [Constants.SET, key, value, Constants.PX, ttl]:
                self.state.save(key, value, int(ttl) + time.time() * 1000)
                return [Constants.OK] if self.state.is_master() else []
            
            case [Constants.TYPE, key]:
                return [self.state.get_type(key)]

            case [Constants.DEL, key]:
                self.state.delete(key)
                return [Constants.OK] if self.state.is_master() else []
            
            case [Constants.CONFIG, Constants.GET, key]:
                return [[key, self.state.config[key]]]
            
            case [Constants.KEYS, pattern]:
                return [self.state.keys()]
            
            case [Constants.INFO, section]:
                return [self.state.get_info()]
            
            case [Constants.REPL_CONF, Constants.GETACK, _]:
                return [[Constants.REPL_CONF, Constants.ACK, str(self.state.master_repl_offset)]]
            
            case [Constants.REPL_CONF, key, val]:
                if key == Constants.ACK: self.state.increment_ack_count()
                else: return [Constants.OK]
            
            case [Constants.PYSNC, _, _]:
                full_resync =f'FULLRESYNC {self.state.config['master_replid']} 0'
                self.talking_to_replica = True
                self.state.add_new_replica(self.connection)
                return [full_resync, Constants.EMPTY_RDB]
            
            case [Constants.XADD, stream_key, id, *fields]:
                entry_id = self.state.generate_stream_entry_id(stream_key, id)
                res = self.state.save_stream(stream_key, entry_id, fields)
                return [res]

            case [Constants.XRANGE, stream_key, start, end]:
                res = self.state.get_stream_entries(stream_key, start, end)
                return [res]
            
            case [Constants.XREAD, *data]:
                # default to empty list if no BLOCK is present
                res = None if Constants.BLOCK in data else []
                if Constants.BLOCK in data:
                    res = asyncio.run(self.state.read_blocking_streams(data[3:], int(data[1])))
                else:
                    res = self.state.read_multiple_streams(data[1:])
                return [res]

            case [Constants.INCR, key]:
                return [self.state.incr(key)]

            case [Constants.WAIT, num_replicas, timeout]:
                return self.handle_wait([num_replicas, timeout])

            case _:
                return [Constants.NULL]


    def run_sync_replica(self):
        while True:
            thread_queue = self.state.buffers[self.connection]
            if len(thread_queue) > 0:
                command = thread_queue[0]
                self.connection.sendall(RESPParser.encode(command).encode())
                thread_queue.popleft()

    def handle_wait(self, args: list) -> None:
        num_replicas = int(args[0])
        timeout = int(args[1])

        start_time = time.time()
        self.request_acks_from_replicas(timeout)

        while time.time() - start_time < timeout:
            if self.state.get_ack_count() >= num_replicas:
                break
            time.sleep(0.1)
        
        self.connection.sendall(RESPParser.encode([Constants.ACK, Constants.OK]).encode())
        self.state.reset_ack_count()
        return [] # return empty list to avoid sending response to client
        
    
    def request_acks_from_replicas(self, timeout: int) -> None:
        for connection in self.state.repl_connections:
            try:
                while len(self.state.buffers[connection]) > 0:
                    time.sleep(0.1)

                connection.sendall(RESPParser.encode([Constants.REPL_CONF, Constants.GETACK, '*']).encode())

                ready_to_read, _, _ = select.select([connection], [], [], timeout)
                if ready_to_read:
                    response, _ = RESPParser.decode(connection.recv(1024))
                    if Constants.ACK in response[0][0]:
                        self.state.increment_ack_count()

            except Exception as e:
                print(f"Error communicating with replica: {e}")
    
    def handshake(self):
        try:
            connection = socket.create_connection((self.state.config['master_host'], self.state.config['master_port']))
            
            connection.sendall(RESPParser.encode(['PING']).encode())
            connection.recv(1024)
            connection.sendall(RESPParser.encode(['REPLCONF', 'listening-port', str(self.state.config['port'])]).encode())
            connection.recv(1024)
            connection.sendall(RESPParser.encode(['REPLCONF', 'capa', 'psync2']).encode())
            connection.recv(1024)
            connection.sendall(RESPParser.encode(['PSYNC', '?', '-1']).encode())
            r1 = connection.recv(148)
            res, rdb = RESPParser.decode(r1)

            if not rdb:
                rdb = connection.recv(93)

            self.state.load_rdb(rdb[5:])

            return connection
        except Exception as e:
            connection.close()
            print(f'Error during handshake: {e}')

    def send(self, message) -> None:
        if isinstance(message, bytes):
            self.connection.sendall(message)
        else:
            self.connection.sendall(RESPParser.encode(message).encode())