import socket  # noqa: F401
import threading
import collections
import argparse
import time

from app.resp_utils import encode_resp, decode_resp
from app.rdb_parser import RDBParser
from app.utils import generate_random_string

replication_info = {
    'role': 'master'
}
config = {
    'port': 6379,
    'dir': None,
    'dbfilename': None,
}
storage = collections.defaultdict(str)

def main():
    server_socket = socket.create_server(("localhost", config['port']), reuse_port=True)
    
    while True:
        connection: socket.socket
        address: tuple[str, str]
        connection, address = server_socket.accept()

        thread: threading.Thread = threading.Thread(target=connect, args=(connection,))
        thread.start()


def connect(connection: socket.socket):
    with connection:
        connected: bool = True
        while connected:
            command: str = connection.recv(8000).decode()
            connected = bool(command)

            response: str
            try:
                decoded_command = decode_resp(command)
                print(f'decoded - {decoded_command}')

                match decoded_command:
                    case ['PING']:
                        response = encode_resp('PONG')
                    case ['ECHO', message]:
                        response = encode_resp(message)
                    case ['GET', key]:
                        print(f'key - {key} - {storage[key]}')
                        response = encode_resp(storage[key])
                    case ['SET', key, value]:
                        storage[key] = value
                        response = encode_resp('OK')
                    case ['SET', key, value, expiry, ttl]:
                        storage[key] = value
                        response = encode_resp('OK')
                        threading.Timer(int(ttl) / 1000, lambda: storage.pop(key)).start()
                    case ['DEL', key]:
                        response = encode_resp(f'mock_del_{key}')
                    case ['CONFIG', command, key]:
                        if command == 'GET':
                            response = encode_resp([key, config.get(key, '')])
                    case ['KEYS', pattern]:
                        if pattern == '*':
                            response = encode_resp(list(storage.keys()))
                        else:
                            response = encode_resp(parser.parsed_data.get(pattern, ''))
                    case ['INFO', section]:
                        if section == 'replication':
                            string = ''.join([f'{k}:{v}\r\n' for k, v in replication_info.items()])
                            response = encode_resp(string)
                    case _:
                        response = encode_resp(Exception('Unknown command'))
            except Exception as e:
                response = encode_resp(e)

            print(f'sent - {response}')
            connection.sendall(response.encode())

def load_rdb(filepath: str):
    parser = RDBParser(filepath)
    for key, (val, exp) in parser.parse().items():
        decoded_key = key.decode('utf-8', errors='replace')
        decoded_val = val.decode('utf-8', errors='replace')
        storage[decoded_key] = decoded_val
        current_time_ms = int(time.time() * 1000)
   
        if exp:
            if exp < current_time_ms:
                storage.pop(decoded_key)
            else:
                threading.Timer((exp - int(time.time() * 1000)) / 1000, lambda: storage.pop(decoded_key)).start()

def connect_master(host, path):
    master = socket.create_connection((host, path))
    handshake_requests = [
        (encode_resp(['PING']).encode(), 'PONG'),
        (encode_resp(['REPLCONF', 'listening-port', str(config['port'])]).encode(), 'OK'),
        (encode_resp(['REPLCONF', 'capa', 'psync2']).encode(), 'OK'),
        (encode_resp(['psync', '?', '-1']).encode(), None)
    ]

    for req, res in handshake_requests:
        master.sendall(req)
        if res:
            response = decode_resp(master.recv(8000).decode())

            if response == res:
                continue
            raise Exception(f'Error: Unexpected response from master {response}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int)
    parser.add_argument('--dir', type=str)
    parser.add_argument('--dbfilename', type=str)
    parser.add_argument('--replicaof', type=str)

    if args := parser.parse_args():
        # Update the config with the provided arguments
        config['port'] = args.port or config['port']
        config['dir'] = args.dir or config['dir']
        config['dbfilename'] = args.dbfilename or config['dbfilename']
        if config['dir'] and config['dbfilename']:
            file = config['dir'] + '/' + config['dbfilename']
            load_rdb(file)  
        # update replication info
        if args.replicaof:
            replication_info['role'] = 'slave' if args.replicaof else 'master'
            host, path = args.replicaof.split(' ')
            connect_master(host, path)
        replication_info['master_replid'] = generate_random_string(40)
        replication_info['master_repl_offset'] = 0

    main()
