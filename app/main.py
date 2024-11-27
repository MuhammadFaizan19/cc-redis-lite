import socket  # noqa: F401
import threading
import collections
import argparse
from app.resp_utils import encode_resp, decode_resp
from app.rdb_parser import RDBParser

config = {
    'dir': None,
    'dbfilename': None,
}
storage = collections.defaultdict(str)

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
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
            print(f'recieved - {command}')
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

                    case _:
                        response = encode_resp(Exception('Unknown command'))
            except Exception as e:
                response = encode_resp(e)

            connection.sendall(response.encode())
            print(f'sent - {response}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=str)
    parser.add_argument('--dbfilename', type=str)

    if args := parser.parse_args():
        config['dir'] = args.dir or config['dir']
        config['dbfilename'] = args.dbfilename or config['dbfilename']
        if config['dir'] and config['dbfilename']:
            file = config['dir'] + '/' + config['dbfilename']
            parser = RDBParser(file)
            parser.parse()
            storage = parser.parsed_data

    main()
