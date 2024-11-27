import struct


class RDBParser:
    def __init__(self, file_path):
        self.file_path = file_path
        self.data = b""  # Entire file data as bytes
        self.offset = 0  # Current reading position in data
        self.parsed_data = {}
        self.aux_fields = {}

    def load_file(self):
        """Load the RDB file into memory."""
        with open(self.file_path, "rb") as f:
            self.data = f.read()

    def read_bytes(self, n):
        """Read n bytes from the in-memory data."""
        if self.offset + n > len(self.data):
            raise ValueError("Unexpected end of RDB file!")
        result = self.data[self.offset:self.offset + n]
        self.offset += n
        return result

    def read_length_encoded_int(self):
        """Decode a length-encoded integer."""
        first_byte = self.data[self.offset]
        self.offset += 1
        type_flag = (first_byte & 0xC0) >> 6

        if type_flag == 0:  # 6-bit integer
            return first_byte & 0x3F
        elif type_flag == 1:  # 14-bit integer
            second_byte = self.data[self.offset]
            self.offset += 1
            return ((first_byte & 0x3F) << 8) | second_byte
        elif type_flag == 2:  # 32-bit integer
            value = struct.unpack(">I", self.read_bytes(4))[0]
            return value
        elif type_flag == 3:  # Special encoding
            special_type = first_byte & 0x3F
            return self.handle_special_encoding(special_type)
        else:
            raise ValueError("Unsupported length encoding!")

    def handle_special_encoding(self, special_type):
        """Handle special encoding based on the type."""
        if special_type == 0:  # Example: Special Integer Encoding
            value = self.data[self.offset]
            self.offset += 1
            return value
        else:
            raise ValueError(f"Unsupported special encoding type: {special_type}")

    def read_string_or_special(self):
        """Determine if the data is a string or special encoded value."""
        first_byte = self.data[self.offset]
        if (first_byte & 0xC0) >> 6 == 3:  # Special encoding
            self.offset += 1
            special_type = first_byte & 0x3F
            return self.handle_special_encoding(special_type)
        else:
            return self.read_string()

    def read_string(self):
        """Decode a Redis string."""
        length = self.read_length_encoded_int()
        if length == -1:
            raise ValueError("Compressed string not implemented!")
        return self.read_bytes(length).decode('utf-8', errors='replace')

    def parse_header(self):
        """Parse the RDB header."""
        magic = self.read_bytes(5).decode('ascii')
        if magic != "REDIS":
            raise ValueError("Invalid RDB file magic header!")
        version = int(self.read_bytes(4).decode('ascii'))

    def parse_opcodes(self):
        """Parse opcodes in the RDB file."""
        while self.offset < len(self.data):
            op = self.data[self.offset]
            self.offset += 1
            if op == 0xFE:  # SELECTDB opcode
                db_number = self.read_length_encoded_int()
            elif op == 0xFA:  # AUX opcode
                key = self.read_string()
                value = self.read_string_or_special()
                self.aux_fields[key] = value
            elif op == 0xFD:  # EXPIRETIME opcode
                expiry = struct.unpack(">I", self.read_bytes(4))[0]
            elif op == 0xFC:  # EXPIRETIMEMS opcode
                expiry = struct.unpack(">Q", self.read_bytes(8))[0]
            elif op == 0xFF:  # EOF opcode
                break
            else:
                # Handle key-value pairs
                value_type = op
                key = self.read_string()
                value = self.parse_value(value_type)
                if value is not None:
                    self.parsed_data[key] = value

    def parse_value(self, value_type):
        """Parse a value based on its type."""
        if value_type == 0:  # String
            return self.read_string()
        elif value_type in (1, 2):  # List or Set
            size = self.read_length_encoded_int()
            return [self.read_string() for _ in range(size)]
        elif value_type == 3:  # Sorted Set
            size = self.read_length_encoded_int()
            return {self.read_string(): self.read_bytes(8) for _ in range(size)}
        elif value_type == 0xFB:  # Handle the special type 0xFB (251)
            return None  # Handle as needed
        else:
            raise NotImplementedError(f"Value type {value_type} not implemented.")

    def parse(self):
        """Main parsing function."""
        self.load_file()  # Load the file into memory
        self.parse_header()
        self.parse_opcodes()
    
    def getKeys(self):
        return list(self.parsed_data.keys())
