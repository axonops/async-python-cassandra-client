#!/usr/bin/env python3
"""Quick port checker for Cassandra readiness."""

import socket
import sys


def check_port(host="localhost", port=9042, timeout=1):
    """Check if a port is open."""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except Exception:
        return False


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9042

    if check_port(host, port):
        sys.exit(0)
    else:
        sys.exit(1)
