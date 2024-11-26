import socket  # noqa: F401
import threading
from app.resp_utils import encode_resp, decode_resp

storage = {}

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
                        if key not in storage:
                            response = encode_resp(None)
                        else:
                            response = encode_resp(storage[key])
                    case ['SET', key, value]:
                        storage[key] = value
                        response = encode_resp('OK')
                    case ['DEL', key]:
                        response = encode_resp(f'mock_del_{key}')
                    case _:
                        response = encode_resp(Exception('Unknown command'))
            except Exception as e:
                response = encode_resp(e)

            connection.sendall(response.encode())
            print(f'sent - {response}')

if __name__ == "__main__":
    main()
