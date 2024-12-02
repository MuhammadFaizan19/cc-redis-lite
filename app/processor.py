import time
import socket
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
        self.buffer_id = None
    
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

                for command, bytes in commands:
                    cmds = self.process(command)
                    add_wait = len(cmds) > 1
                    for response in self.process(command):
                        self.connection.sendall(response)
                        if add_wait: time.sleep(0.1)
                    if self.state.replica_present and Constants.SET in command or Constants.DEL in command:
                        self.state.add_command_buffer(command)
            except Exception as e:
                self.connection.sendall(RESPParser.encode(f'Error: {e}').encode())
                break
        if self.talking_to_replica and self.state.is_master():
            self.run_sync_replica()
        self.connection.close()
            

    
    def process(self, command):
        match command:
            case [Constants.PING]:
                return [RESPParser.encode('PONG').encode()]
            case [Constants.ECHO, message]:
                return [RESPParser.encode(message).encode()]
            case [Constants.GET, key]:
                return [RESPParser.encode(self.state.get(key)).encode()]
            case [Constants.SET, key, value]:
                self.state.save(key, value)
                return [Constants.OK]
            case [Constants.SET, key, value, Constants.PX, ttl]:
                self.state.save(key, value, int(ttl) + time.time() * 1000)
                return [Constants.OK]
            case [Constants.DEL, key]:
                self.state.delete(key)
                return [Constants.OK]
            case [Constants.CONFIG, Constants.GET, key]:
                return [RESPParser.encode([key, self.config[key]]).encode()]
            case [Constants.KEYS, pattern]:
                return [RESPParser.encode(self.state.keys()).encode()]
            case [Constants.INFO, section]:
                return [RESPParser.encode(self.state.get_info()).encode()]
            case [Constants.WAIT, _, time]:
                time.sleep(int(time) / 1000)
                return [RESPParser.encode(len(self.state.repl_ports)).encode()]
            case [Constants.REPL_CONF, key, val]:
                if key == Constants.LISTENING_PORT:
                    replica = (self.connection.getpeername()[0], int(val))
                    if replica not in self.state.repl_ports: self.state.repl_ports.append(replica)
                return [Constants.OK]
            case [Constants.PYSNC, master_id, repl_offset]:
                master_replid = self.config['master_replid']
                full_resync = RESPParser.encode(f'FULLRESYNC {master_replid} 0').encode()
                rdb = f'${len(Constants.EMPTY_RDB)}\r\n'.encode() + Constants.EMPTY_RDB
                self.talking_to_replica = True
                self.buffer_id = self.state.add_new_replica()
                return [full_resync, rdb]
        print(f'Command not recognized: {command}')
        return Constants.NULL
    
    def run_sync_replica(self):
        while True:
            thread_queue = self.state.buffers[self.buffer_id]
            if len(thread_queue) > 0:
                command = thread_queue.popleft()
                self.connection.sendall(RESPParser.encode(command).encode())


class SlaveCommandProcessor(Thread):
    def __init__(self, state: State, config):
        super().__init__()
        self.state = state
        self.config = config
        self.connection = self.handshake()
    
    def run(self):
        buffer = b''
        while True and self.connection is not None:
            original_message = self.connection.recv(1024)

            if not original_message:
                break

            buffer += original_message
            commands, buffer = RESPParser.decode(buffer)

            for command, bytes in commands:
                print(f'Command: {command} - Bytes: {bytes}')
                response = self.process(command)
                self.state.increment_repl_offset(bytes)
                if response: self.connection.sendall(response)

        if self.connection: self.connection.close()

    def process(self, command):
        match command:
            case [Constants.PING]:
                print('Received PING')
                return
            case [Constants.ECHO, message]:
                print(f'Received ECHO {message}')
                return 
            case [Constants.GET, key]:
                print(f'Received GET {key}')
                return
            case [Constants.SET, key, value]:
                self.state.save(key, value)
                return
            case [Constants.SET, key, value, Constants.px, ttl]:
                self.state.save(key, value, int(ttl) + time.time() * 1000)
                return
            case [Constants.DEL, key]:
                self.state.delete(key)
                return
            case [Constants.REPL_CONF, Constants.GETACK, pattern]:
                if pattern == '*':
                    return RESPParser.encode([Constants.REPL_CONF, Constants.ACK, str(self.state.master_repl_offset)]).encode()
        print(f'Command not recognized: {command}') 
        return Constants.NULL
            
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
            r1 = connection.recv(153)
            res, rdb = RESPParser.decode(r1)

            if not rdb:
                rdb = connection.recv(93)

            self.state.load_rdb(rdb[5:])
            print('Handshake successful')

            return connection
        except Exception as e:
            connection.close()
            print(f'Error during handshake: {e}')