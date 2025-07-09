#!/usr/bin/env python
"""
Test script to demonstrate enhanced Cassandra error detection in FastAPI app.
"""

import asyncio

import httpx


async def test_error_detection():
    """Test various error scenarios to demonstrate proper error detection."""

    async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
        print("Testing Enhanced Cassandra Error Detection")
        print("=" * 50)

        # Test 1: Health check
        print("\n1. Testing health check endpoint...")
        response = await client.get("/health")
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.json()}")

        # Test 2: Create a user (should work if Cassandra is up)
        print("\n2. Testing user creation...")
        user_data = {"name": "Test User", "email": "test@example.com", "age": 30}
        try:
            response = await client.post("/users", json=user_data)
            print(f"   Status: {response.status_code}")
            if response.status_code == 201:
                print(f"   Created user: {response.json()['id']}")
            else:
                print(f"   Error: {response.json()}")
        except Exception as e:
            print(f"   Request failed: {e}")

        # Test 3: Invalid query (should get 500, not 503)
        print("\n3. Testing invalid UUID handling...")
        try:
            response = await client.get("/users/not-a-uuid")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.json()}")
        except Exception as e:
            print(f"   Request failed: {e}")

        # Test 4: Non-existent user (should get 404, not 503)
        print("\n4. Testing non-existent user...")
        try:
            response = await client.get("/users/00000000-0000-0000-0000-000000000000")
            print(f"   Status: {response.status_code}")
            print(f"   Response: {response.json()}")
        except Exception as e:
            print(f"   Request failed: {e}")

        print("\n" + "=" * 50)
        print("Error detection test completed!")
        print("\nKey observations:")
        print("- 503 errors: Cassandra unavailability (connection issues)")
        print("- 500 errors: Other server errors (invalid queries, etc.)")
        print("- 400/404 errors: Client errors (invalid input, not found)")


if __name__ == "__main__":
    print("Starting FastAPI app error detection test...")
    print("Make sure the FastAPI app is running on http://localhost:8000")
    print()

    asyncio.run(test_error_detection())
