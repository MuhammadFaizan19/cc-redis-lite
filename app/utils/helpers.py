import random
import string

def generate_alphanumeric_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def is_numeric(s):
    try:
        float(s)  # Convert to float
        return True
    except ValueError:
        return False