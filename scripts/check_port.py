#!/usr/bin/env python3
"""Simple port checker for shell scripts."""
import socket
import sys

if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} HOST PORT", file=sys.stderr)
    sys.exit(1)

host = sys.argv[1]
port = int(sys.argv[2])

try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex((host, port))
    sock.close()
    sys.exit(0 if result == 0 else 1)
except Exception:
    sys.exit(1)
