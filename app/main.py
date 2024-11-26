import socket  # noqa: F401
import threading


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
            match command:
                case '*1\r\n$4\r\nPING\r\n':
                    response = '+PONG\r\n'
            
            connection.sendall(response.encode())
            print(f'sent - {response}')


if __name__ == "__main__":
    main()
