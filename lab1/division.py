#!/usr/bin/env python3

import random
import sys

try:
    try:
        A = int(input())
    except:
        raise Exception("Wrong input")
    B = random.randint(-10, 10)
    
    if B == 0:
        raise ZeroDivisionError("Attempted to divide by zero.")
    
    result = A / B
    print(result)
except ZeroDivisionError as e:
    sys.stderr.write(f"Division by zero\n")
except Exception as e:
    sys.stderr.write(f"{str(e)}\n")
