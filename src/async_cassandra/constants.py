"""
Constants used throughout the async-cassandra library.
"""

# Default values
DEFAULT_FETCH_SIZE = 1000
DEFAULT_EXECUTOR_THREADS = 4
DEFAULT_CONNECTION_TIMEOUT = 30.0  # Increased for larger heap sizes
DEFAULT_REQUEST_TIMEOUT = 120.0

# Limits
MAX_CONCURRENT_QUERIES = 100
MAX_RETRY_ATTEMPTS = 3

# Thread pool settings
MIN_EXECUTOR_THREADS = 1
MAX_EXECUTOR_THREADS = 128
