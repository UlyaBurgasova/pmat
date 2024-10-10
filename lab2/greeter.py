#!/usr/bin/env python3

import sys

def greet_from_file():
    error_log = open("error.txt", "w")
    for line in sys.stdin:
        name = line.strip()
        if not name:
            continue
        if not name[0].isupper():
            error_log.write(f"Error: Name '{name}' needs to start in uppercase!\n")
        elif not name.isalpha():
            error_log.write(f"Error: Name '{name}' contains invalid characters!\n")
        else:
            print(f"Nice to see you {name}!")
    error_log.close()

def greet_interactive():
    try:
        while True:
            name = input("Hey, what's your name? ")
            print(f"Nice to see you {name}!")
    except KeyboardInterrupt:
        print("\nGoodbye!")

if __name__ == "__main__":
    if sys.stdin.isatty():
        greet_interactive()
    else:
        greet_from_file()
