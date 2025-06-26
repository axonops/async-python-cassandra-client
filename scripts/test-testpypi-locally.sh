#!/bin/bash
# Script to test TestPyPI package locally

set -e

echo "Testing async-cassandra from TestPyPI"
echo "===================================="

# Create a temporary virtual environment
TEMP_DIR=$(mktemp -d)
echo "Creating test environment in: $TEMP_DIR"

cd "$TEMP_DIR"
python3 -m venv test_env
source test_env/bin/activate

# Install from TestPyPI
echo ""
echo "Installing async-cassandra from TestPyPI..."
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple async-cassandra

# Test imports
echo ""
echo "Testing imports..."
python -c "
import async_cassandra
print(f'✅ Package version: {async_cassandra.__version__}')

from async_cassandra import AsyncCluster, AsyncSession
print('✅ Core classes imported successfully')
"

# Test basic functionality
echo ""
echo "Testing basic functionality..."
cat > test_basic.py << 'EOF'
import asyncio
from async_cassandra import AsyncCluster

async def test():
    print("Creating AsyncCluster instance...")
    cluster = AsyncCluster(['127.0.0.1'])
    print("✅ AsyncCluster created successfully")
    print(f"   Cluster type: {type(cluster)}")

asyncio.run(test())
EOF

python test_basic.py

# Cleanup
deactivate
cd -
rm -rf "$TEMP_DIR"

echo ""
echo "✅ All tests passed! Package is working correctly from TestPyPI"
