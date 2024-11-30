import os
from typing import Dict, Tuple, Optional


class RESPParser:
    """A class to encode and decode messages in Redis Serialization Protocol (RESP) format."""

    class IncompleteRESPError(Exception):
        """Raised when a RESP message is incomplete and more data is needed."""
        pass

    @staticmethod
    def encode(data):
        """
        Encode data in RESP format.
        :param data: Data to encode (str, int, list, or Exception).
        :return: RESP-encoded string.
        """
        if data is None:
            return '$-1\r\n'
        if isinstance(data, str):
            return f'${len(data)}\r\n{data}\r\n' if data else '$-1\r\n'
        if isinstance(data, int):
            return f':{data}\r\n'
        if isinstance(data, list):
            elements = ''.join(RESPParser.encode(item) for item in data)
            return f'*{len(data)}\r\n{elements}'
        if isinstance(data, Exception):
            return f'-{str(data)}\r\n'
        
        raise ValueError('Unsupported type for RESP encoding')

    @staticmethod
    def decode(buffer):
        """
        Decode data from a RESP-encoded buffer.
        :param buffer: Byte buffer containing RESP-encoded data.
        :return: Tuple of (decoded_messages, remaining_buffer).
        """
        decoded_messages = []
        current_index = 0

        while current_index < len(buffer):
            try:
                message, current_index = RESPParser._parse_value(buffer, current_index)
                if isinstance(message, list) and message and isinstance(message[0], str):
                    message[0] = message[0].lower()  # Normalize the command name
                decoded_messages.append(message)
            except RESPParser.IncompleteRESPError:
                return decoded_messages, buffer[current_index:]

        return decoded_messages, b''

    @staticmethod
    def _parse_value(buffer, index):
        """Parse a RESP value starting at the given index."""
        while index < len(buffer) and buffer[index:index + 2] == b'\r\n':
            index += 2

        if index >= len(buffer):
            raise RESPParser.IncompleteRESPError()

        prefix = buffer[index]
        if prefix == ord('+'):
            return RESPParser._parse_simple_string(buffer, index)
        elif prefix == ord(':'):
            return RESPParser._parse_integer(buffer, index)
        elif prefix == ord('$'):
            return RESPParser._parse_bulk_string(buffer, index)
        elif prefix == ord('*'):
            return RESPParser._parse_array(buffer, index)
        else:
            raise ValueError(f"Unsupported RESP type at index {index}: {buffer[index:index + 10]}")

    @staticmethod
    def _parse_simple_string(buffer, index):
        """Parse a simple string starting at the given index."""
        end = buffer.find(b'\r\n', index)
        if end == -1:
            raise RESPParser.IncompleteRESPError()
        return buffer[index + 1:end].decode(), end + 2

    @staticmethod
    def _parse_integer(buffer, index):
        """Parse an integer starting at the given index."""
        end = buffer.find(b'\r\n', index)
        if end == -1:
            raise RESPParser.IncompleteRESPError()
        return int(buffer[index + 1:end]), end + 2

    @staticmethod
    def _parse_bulk_string(buffer, index):
        """Parse a bulk string starting at the given index."""
        length_end = buffer.find(b'\r\n', index)
        if length_end == -1:
            raise RESPParser.IncompleteRESPError()

        length = int(buffer[index + 1:length_end])
        if length == -1:
            return None, length_end + 2

        string_start = length_end + 2
        string_end = string_start + length
        if len(buffer) < string_end + 2:
            raise RESPParser.IncompleteRESPError()

        try:
            return buffer[string_start:string_end].decode('utf-8'), string_end + 2
        except UnicodeDecodeError:
            # Handle invalid UTF-8 sequences gracefully
            decoded_string = buffer[string_start:string_end].decode('utf-8', errors='replace')  # Replace invalid bytes
            return decoded_string, string_end + 2
    
    @staticmethod
    def _parse_array(buffer, index):
        """Parse an array starting at the given index."""
        num_elements_end = buffer.find(b'\r\n', index)
        if num_elements_end == -1:
            raise RESPParser.IncompleteRESPError()

        num_elements = int(buffer[index + 1:num_elements_end])
        if num_elements == -1:
            return None, num_elements_end + 2

        elements = []
        current = num_elements_end + 2
        for _ in range(num_elements):
            value, current = RESPParser._parse_value(buffer, current)
            elements.append(value)
        return elements, current


class RDBParser:
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.store: Dict[str, Tuple[str, Optional[int]]] = {}

    def _parse_db_len(self, data: bytes, pos: int) -> Tuple[int, int]:
        first = data[pos]
        pos += 1
        start = first >> 6
        if start == 0b00:
            length = first
        elif start == 0b01:
            first &= 0b00111111
            second = data[pos]
            pos += 1
            length = (first << 8) + second
        elif start == 0b10:
            length = int.from_bytes(data[pos:pos + 4], "little")
            pos += 4
        elif start == 0b11:
            first &= 0b00111111
            length = 2**first
        else:
            raise ValueError(f"Unknown DB length type {start} at position {pos}")
        return length, pos

    def _parse_db_string(self, data: bytes, pos: int) -> Tuple[str, int]:
        length, pos = self._parse_db_len(data, pos)
        value = (data[pos:pos + length]).decode('utf-8', errors='replace')
        pos += length
        return value, pos

    def _parse_keyvalue(self, data: bytes, pos: int) -> Tuple[str, str, int]:
        vtype = data[pos]
        if vtype not in (0, 9, 10, 11, 12, 13):
            raise ValueError(f"Unsupported value type {vtype} at position {pos}")
        pos += 1
        key, pos = self._parse_db_string(data, pos)
        val, pos = self._parse_db_string(data, pos)
        return key, val, pos

    def parse(self) -> Dict[str, Tuple[str, Optional[int]]]:
        if not os.path.exists(self.file_path):
            print(f"Error: File {self.file_path} not found")
            return self.store

        with open(self.file_path, "rb") as db_file:
            data = db_file.read()

        if data[:5] != b"REDIS":
            raise ValueError("Incorrect RDB format")

        pos = 9  # Skip "REDIS" magic and version

        while pos < len(data):
            op = data[pos]
            pos += 1
            if op == 0xFA:  # Auxiliary data
                key, pos = self._parse_db_string(data, pos)
                val, pos = self._parse_db_string(data, pos)
            elif op == 0xFE:  # Select DB
                db_num, pos = self._parse_db_len(data, pos)
            elif op == 0xFB:  # Resize DB
                _, pos = self._parse_db_len(data, pos)
                _, pos = self._parse_db_len(data, pos)
            elif op == 0xFD:  # Expire time in seconds
                exp = int.from_bytes(data[pos:pos + 4], "little") * 1_000
                pos += 4
                key, val, pos = self._parse_keyvalue(data, pos)
                self.store[key] = (val, exp)
            elif op == 0xFC:  # Expire time in milliseconds
                exp = int.from_bytes(data[pos:pos + 8], "little")
                pos += 8
                key, val, pos = self._parse_keyvalue(data, pos)
                self.store[key] = (val, exp)
            elif op == 0xFF:  # End of file
                break
            else:  # Default parsing for unknown types
                pos -= 1  # Backtrack
                key, val, pos = self._parse_keyvalue(data, pos)
                self.store[key] = (val, None)

        print(f"Finished parsing RDB with {len(self.store)} keys")
        return self.store


def generate_alphanumeric_string(length):
    import random
    import string
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))
