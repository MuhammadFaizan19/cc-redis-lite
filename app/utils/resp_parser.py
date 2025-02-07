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
                inital = current_index
                message, current_index = RESPParser._parse_value(buffer, current_index)
                if isinstance(message, list) and message and isinstance(message[0], str):
                    message[0] = message[0].upper()  # Normalize the command name
                decoded_messages.append((message, current_index - inital))
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
