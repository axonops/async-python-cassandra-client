#!/usr/bin/env python3
"""Debug FastAPI test issues."""

import asyncio
import sys

sys.path.insert(0, ".")

from main import app, session


async def test_lifespan():
    """Test if lifespan is triggered."""
    print(f"Initial session: {session}")

    # Manually trigger lifespan
    async with app.router.lifespan_context(app):
        print(f"Session after lifespan: {session}")

        # Test a simple query
        if session:
            result = await session.execute("SELECT now() FROM system.local")
            print(f"Query result: {result}")


if __name__ == "__main__":
    asyncio.run(test_lifespan())
