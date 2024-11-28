import socket

from app.resp_utils import encode_resp, decode_resp

class Connection:
    def __init__(self, host: str, port: int):
        self.connection = socket.create_connection((host, port))

    def send(self, data: str, expect: str | None = None):
        self.connection.sendall(encode_resp(data).encode())
        if expect:
            response = decode_resp(self.connection.recv(8000).decode())
            if response != expect:
                raise ValueError(f'Expected {expect}, got {response}')
    
    def send_multiple(self, data: list[(str, str | None)], expect: str | None = None):
        for command, response in data:
            self.send(command, response)

    def handshake(self, port: int):
        requests = [
            (['PING'], 'PONG'),
            (['REPLCONF', 'listening-port', str(port)], 'OK'),
            (['REPLCONF', 'capa', 'psync2'], 'OK'),
            (['PSYNC', '?', '-1'], None)
        ]
        
        self.send_multiple(requests)