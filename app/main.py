import socket
from app.constants import Constants
from app.state import State
from app.processor import CommandProcessor, SlaveCommandProcessor
from app.config import load_config

def main():
    config = load_config()
    state = State(config)
    server = socket.create_server((config['host'], config['port']), reuse_port=True)

    if state.role == Constants.SLAVE:
       SlaveCommandProcessor(state, config).start()
       while True:
            connection, _ = server.accept()
            SlaveCommandProcessor(state, config, connection).start()
    
    else:
        while True:
            connection, _ = server.accept()
            CommandProcessor(connection, state, config).start()


if __name__ == '__main__':
    main()