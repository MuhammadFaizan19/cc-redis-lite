import math
import random

def generate_random_string(length):
    alphanumeric = '0123456789abcdefghijklmnopqrstuvxyz'
    length_dict = len(alphanumeric)
    string = ''

    for _ in range(length):
        string += alphanumeric[math.floor(random.random() * length_dict)]
    
    return string
