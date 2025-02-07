import time

from app.context.store import Store
from app.constants import Constants

class StreamStore(Store):
    def __init__(self):
        super().__init__()

    def generate_stream_entry_id(self, key: str, id: str) -> str:
        if '*' not in id:
            return id

        if id == '*':
            return str(int(time.time() * 1000)) + '-0'
        
        time_part = id.split('-')[0]
        if self.get(key) is not None:
            last_id = self.store[key][0][-1][0] if self.store[key][0] else '0-0'
            parts = last_id.split('-')
            
            if parts[0] == time_part:
                return f"{parts[0]}-{int(parts[1]) + 1}"
        
        return time_part + '-0' if int(time_part) > 0 else time_part + '-1'

    def validate_stream(self, key: str, id: str) -> str:
        last_id = self.get(key)[-1][0] if self.get(key) else '0-0'
        
        if id == '0-0':
            return Constants.ERROR_MIN_STREAM_ID
        if id <= last_id:
            return Constants.ERROR_STREAM_KEY

    def save_stream(self, key: str, entry_id: str, fields: list) -> None:
        if self.get(key) is None:
            self.save(key, [])

        if error:= self.validate_stream(key, entry_id):
            return error

        current_time = int(time.time() * 1000)
        self.store[key][0].append([entry_id, fields, current_time])
        return entry_id

    def get_stream_entries(self, key: str, start: str, end: str) -> list:
        entries = self.get(key)
        
        if not entries:
            return []

        # Return all entries if both start and end are open ranges
        if start == '-' and end == '+':
            return entries

        start_has_seq = '-' in start
        end_has_seq = '-' in end

        def within_start_range(entry_id: str) -> bool:
            return start == '-' or (
                start_has_seq and entry_id >= start
            ) or (
                not start_has_seq and entry_id.split('-')[0] > start
            )

        def within_end_range(entry_id: str) -> bool:
            return end == '+' or (
                end_has_seq and entry_id <= end
            ) or (
                not end_has_seq and entry_id.split('-')[0] <= end
            )

        return [
            entry[:2] for entry in entries
            if within_start_range(entry[0]) and within_end_range(entry[0])
        ]

