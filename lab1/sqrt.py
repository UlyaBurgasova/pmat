#!/usr/bin/env python3

import sys
import math

try:
    try:
        number = float(input())
    except:
        raise Exception("Wrong input")
    
    if number < 0:
        raise ValueError("Cannot calculate square root of a negative number.")
    
    result = math.sqrt(number)
    
    with open('output.txt', 'w') as f:
        f.write(f"{result}\n")
except ValueError as e:
    sys.stderr.write(f"{str(e)}\n")
except Exception as e:
    sys.stderr.write(f"{str(e)}\n")
