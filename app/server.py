import socket
import threading
import time

from app.state import State
from app.resp_utils import encode_resp, decode_resp

class Server:
    def __init__(self):
        self.state = State()
        self.server_socket: socket.socket = None
        self.connection: socket.socket = None

    def start_server(self):
        host, port = self.state.conf['host'], self.state.conf['port']
        self.server_socket = socket.create_server((host, port), reuse_port=True)

        while True:
            connection: socket.socket = self.server_socket.accept()[0]
            thread: threading.Thread = threading.Thread(target=self.connect, args=(connection,))
            thread.start()
    
    def connect(self, connection: socket.socket):
        with connection:
            connected: bool = True
            while connected:
                command: str = connection.recv(8000).decode()
                connected = bool(command)

                command = decode_resp(command)

                try:
                    self.handle_command(connection, command)
                except Exception as e:
                    connection.sendall(encode_resp(e).encode())

    def handle_command(self, connection: socket.socket, command):
        
        def send_response(data):
            connection.sendall((encode_resp(data)).encode())

        match command:
            case ['PING']:
                send_response('PONG')
            case ['ECHO', message]:
                send_response(message)
            case ['GET', key]:
                send_response(self.state.store.get(key, ''))
            case ['SET', key, value]:
                self.state.set_store([(key, value)])
                send_response('OK')
            case ['SET', key, value, expiry, ttl]:
                self.state.set_store([(key, value)])
                self.state.set_store_key_expiry([(key, int(ttl) + time.time() * 1000)])
                send_response('OK')
            case ['DEL', key]:
                self.state.delete_key(key)
                send_response('OK')
            case ['CONFIG', 'GET', key]:
                send_response([key, self.state.conf.get(key, '')])
            case ['KEYS', pattern]:
                send_response(list(self.state.store.keys())) # default to *
            case ['INFO', section]:
                if section == 'replication':
                    string = ''.join([f'{key}:{value}\r\n' for key, value in self.state.repl.items()])
                    send_response(string)
            case ['REPLCONF', conf_key, val]:
                send_response('OK')
            case ['PSYNC', master_id, repl_offset]:
                send_response(f'FULLRESYNC {self.state.repl.get('master_replid')} 0')
                empty_rdb_content = b'REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2'
                connection.sendall(f'${len(empty_rdb_content)}\r\n'.encode() + empty_rdb_content)

