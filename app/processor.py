import time
import select
import socket
import asyncio
from threading import Thread

from app.state import State
from app.utils import RESPParser
from app.constants import Constants

class CommandProcessor(Thread):
    def __init__(self, connection: socket.socket, state: State, config) -> None:
        super().__init__()
        self.state = state
        self.config = config
        self.connection = connection
        self.talking_to_replica = False
    
    def run(self):
        buffer = b''
        while True:
            try:
                if self.talking_to_replica:
                    break

                original_message = self.connection.recv(8000)
                if not original_message:  # Connection closed
                    break

                buffer += original_message
                commands, buffer = RESPParser.decode(buffer)

                for command, _ in commands:
                    if self.state.replica_present and Constants.SET in command or Constants.DEL in command:
                        self.state.add_command_buffer(command)

                    if command[0] == Constants.WAIT:
                        self.handle_wait(command)
                        continue
                    
                    self.process(command)
            except Exception as e:
                self.connection.sendall(RESPParser.encode(f'Error: {e}').encode())
                break

        if self.talking_to_replica and self.state.is_master():
            self.run_sync_replica()
        self.connection.close()

    
    def process(self, command):
        match command:
            case [Constants.PING]:
                self.send('PONG')

            case [Constants.ECHO, message]:
                self.send(message)
            
            case [Constants.GET, key]:
                self.send(self.state.get(key))
            
            case [Constants.SET, key, value]:
                self.state.save(key, value)
                self.send(Constants.OK)
            
            case [Constants.SET, key, value, Constants.PX, ttl]:
                self.state.save(key, value, int(ttl) + time.time() * 1000)
                self.send(Constants.OK)
            
            case [Constants.TYPE, key]:
                self.send(self.state.get_type(key))

            case [Constants.DEL, key]:
                self.state.delete(key)
                self.send(Constants.OK)
            
            case [Constants.CONFIG, Constants.GET, key]:
                self.send([key, self.config[key]])
            
            case [Constants.KEYS, pattern]:
                self.send(self.state.keys())
            
            case [Constants.INFO, section]:
                self.send(self.state.get_info())
            
            case [Constants.REPL_CONF, key, val]:
                if key == Constants.ACK: self.state.increment_ack_count()
                else: self.send(Constants.OK)
            
            case [Constants.PYSNC, _, _]:
                full_resync =f'FULLRESYNC {self.config['master_replid']} 0'
                self.send(full_resync)
                self.send(Constants.EMPTY_RDB)

                self.talking_to_replica = True
                self.state.add_new_replica(self.connection)
            
            case [Constants.XADD, stream_key, id, *fields]:
                entry_id = self.state.generate_stream_entry_id(stream_key, id)
                res = self.state.save_stream(stream_key, entry_id, fields)
                self.send(res)

            case [Constants.XRANGE, stream_key, start, end]:
                res = self.state.get_stream_entries(stream_key, start, end)
                self.send(res)
            
            case [Constants.XREAD, *data]:
                # default to empty list if no BLOCK is present
                res = None if Constants.BLOCK in data else []
                if Constants.BLOCK in data:
                    res = asyncio.run(self.state.read_blocking_streams(data[3:], int(data[1])))
                else:
                    res = self.state.read_multiple_streams(data[1:])
                self.send(res)
            case _:
                return [Constants.NULL]
    
    def run_sync_replica(self):
        while True:
            thread_queue = self.state.buffers[self.connection]
            if len(thread_queue) > 0:
                command = thread_queue[0]
                self.connection.sendall(RESPParser.encode(command).encode())
                thread_queue.popleft()

    def handle_wait(self, command):
        num_replicas = int(command[1])
        timeout = int(command[2]) / 1000

        start_time = time.time()
        self.request_acks_from_replicas(timeout)

        while time.time() - start_time < timeout:
            if self.state.get_ack_count() >= num_replicas:
                break
            time.sleep(0.1)

        self.connection.sendall(RESPParser.encode((self.state.get_ack_count())).encode())
        self.state.reset_ack_count()


    def request_acks_from_replicas(self, timeout: int):
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
    
    def send(self, message):
        if isinstance(message, bytes):
            return self.connection.sendall(message)
        return self.connection.sendall(RESPParser.encode(message).encode())


class SlaveCommandProcessor(Thread):
    def __init__(self, state: State, config, connection: socket.socket = None):
        super().__init__()
        self.state = state
        self.config = config
        self.connection = connection if connection else self.handshake()
    
    def run(self):
        buffer = b''
        while True and self.connection is not None:
            original_message = self.connection.recv(1024)

            if not original_message:
                break

            buffer += original_message
            commands, buffer = RESPParser.decode(buffer)

            for command, bytes in commands:
                self.process(command)
                self.state.increment_repl_offset(bytes)

        if self.connection: self.connection.close()

    def process(self, command):
        match command:
            case [Constants.PING]:
                pass
            case [Constants.GET, key]:
                self.send(self.state.get(key))
            case [Constants.SET, key, value]:
                self.state.save(key, value)
            case [Constants.SET, key, value, Constants.px, ttl]:
                self.state.save(key, value, int(ttl) + time.time() * 1000)
            case [Constants.DEL, key]:
                self.state.delete(key)
            case [Constants.INFO, section]:
                self.send(self.state.get_info())
            case [Constants.REPL_CONF, Constants.GETACK, _]:
                self.send([Constants.REPL_CONF, Constants.ACK, str(self.state.master_repl_offset)])
            case _:
                self.send(Constants.NULL)
            
    def handshake(self):
        try:
            connection = socket.create_connection((self.config['master_host'], self.config['master_port']))
            
            connection.sendall(RESPParser.encode(['PING']).encode())
            connection.recv(1024)
            connection.sendall(RESPParser.encode(['REPLCONF', 'listening-port', str(self.config['port'])]).encode())
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

    def send(self, message):
        return self.connection.sendall(RESPParser.encode(message).encode())