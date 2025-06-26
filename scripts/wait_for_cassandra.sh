#!/bin/bash
# Fast Cassandra readiness check script

HOST=${1:-localhost}
PORT=${2:-9042}
MAX_ATTEMPTS=${3:-30}

echo "Checking Cassandra on $HOST:$PORT..."

# First, check if port is open
for i in $(seq 1 $MAX_ATTEMPTS); do
    if nc -z $HOST $PORT 2>/dev/null; then
        echo "Port $PORT is open (attempt $i/$MAX_ATTEMPTS)"
        # Give it a moment to fully initialize
        sleep 2

        # Try a simple CQL command
        if echo "SELECT now() FROM system.local;" | cqlsh $HOST $PORT 2>/dev/null | grep -q "(1 rows)"; then
            echo "Cassandra is ready!"
            exit 0
        fi
    fi

    if [ $i -lt $MAX_ATTEMPTS ]; then
        echo "Waiting for Cassandra... ($i/$MAX_ATTEMPTS)"
        sleep 1
    fi
done

echo "Cassandra failed to start within $MAX_ATTEMPTS seconds"
exit 1
