import socket

from app.constants import Constants
from app.controllers import Controller
from app.context import State, load_config

def main():
    config = load_config()
    state = State()
    server = socket.create_server((config['host'], config['port']), reuse_port=True)


    if state.role == Constants.SLAVE:
       Controller(state).start()
       while True:
            connection, _ = server.accept()
            Controller(state, connection).start()
    
    else:
        while True:
            connection, _ = server.accept()
            Controller(state, connection).start()


if __name__ == '__main__':
    main()