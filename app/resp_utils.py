def encode_resp(data: str | int | list | Exception) -> str:
    """
    Return data encoded in RESP format
    """
    if isinstance(data, str):
        if data == '':
            return '$-1\r\n'
        return f'+{data}\r\n'
    if isinstance(data, int):
        return f':{data}\r\n'
    if isinstance(data, list):
        length = len(data)
        elements = ''.join(encode_resp(item) for item in data)
        return f'*{length}\r\n{elements}'
    if isinstance(data, Exception):
        return f'-{str(data)}\r\n'
    raise ValueError('Unsupported type for RESP encoding')

def decode_resp(message):
    """
    Return data decoded from a RESP-encoded string 
    """
    if message.startswith('+'): # Simple String
        return message[1:].strip()
    if message.startswith(':'): # Integer
        return int(message[1:].strip())
    if message.startswith('*'): # Array
        lines = message.strip().split('\r\n')
        num_elements = int(lines[0][1:])
        if num_elements == 0:
            return []
        array = []
        index = 1
        while index < len(lines) and len(array) < num_elements:
            line = lines[index]
            if line.startswith('$'): # Bulk String
                length = int(line[1:])
                array.append(lines[index + 1][:length])
                index += 2
            else:
                array.append(decode_resp(line))
                index += 1

        # Normalize the command name to upper case if it exists
        if len(array) > 0 and isinstance(array[0], str):
            array[0] = array[0].upper()

        return array
    if message.startswith('$'):
        length = int(message[1:].split('\r\n')[0])
        if length == -1:
            return None
        return message.split('\r\n')[1][:length]
    raise ValueError('Unsupported RESP type')

