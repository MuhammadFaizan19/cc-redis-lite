import socket
import threading
import collections

class ReplicationManager:
    def __init__(self):
        self.replica_present = False
        self.offset_lock = threading.Lock()
        self.master_repl_offset = 0
        self.repl_connections_lock = threading.Lock()
        self.repl_connections: list[socket.socket] = []
        self.ack_count_lock = threading.Lock()
        self.ack_count = 0
        self.buffers: dict[socket.socket, collections.deque] = {}

    def add_command_buffer(self, command):
        for k, _ in self.buffers.items():
            self.buffers[k].append(command)        

    def add_new_replica(self, connection: socket.socket) -> int:
        self.replica_present = True
        with self.repl_connections_lock:
            self.buffers[connection] = collections.deque([])
            self.repl_connections.append(connection)
        return id

    def increment_repl_offset(self, bytes_processed: int):
        with self.offset_lock:
            self.master_repl_offset += bytes_processed

    def increment_ack_count(self):
        with self.ack_count_lock:
            self.ack_count += 1

    def reset_ack_count(self):
        with self.ack_count_lock:
            self.ack_count = 0

    def get_ack_count(self):
        with self.ack_count_lock:
            return self.ack_count
