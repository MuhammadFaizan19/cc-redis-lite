import os
from typing import Dict, Tuple, Optional

class RDBParser:
    def __init__(self):
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
    
    @staticmethod
    def read_rdb(file_path):
        if not os.path.exists(file_path):
            print(f"Error: File {file_path} not found")
            return None
        with open(file_path, "rb") as db_file:
            data = db_file.read()

        return data

    def parse(self, data) -> Dict[str, Tuple[str, Optional[int]]]:
        if not data:
            print("Error: Empty RDB file")
            return {}

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
