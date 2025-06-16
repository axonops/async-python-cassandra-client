# Behavior-Driven Development (BDD) Guide

## What is BDD?

Behavior-Driven Development (BDD) is a testing approach that describes system behavior from the user's perspective using natural language. In async-python-cassandra-client, we use BDD to ensure our library behaves correctly in real-world scenarios.

## Why BDD for async-python-cassandra-client?

Traditional unit tests verify individual components work correctly. BDD tests verify the entire system works as users expect:

- **User-focused**: Tests describe what users can do, not implementation details
- **Real integration**: Tests use actual Cassandra, not mocks
- **Living documentation**: Tests serve as examples of how to use the library
- **Confidence**: If BDD tests pass, the library works in production scenarios

## BDD Principles We Follow

### 1. Test Real Behavior, Not Implementation

❌ **Bad**: "Test that execute() calls driver.execute_async()"
✅ **Good**: "When I execute a query, I get the expected results"

### 2. Use Real Dependencies

- Always test against real Cassandra instances
- Never mock the database or network layer
- Test actual async behavior, not simulated

### 3. Write Tests as User Stories

```gherkin
Feature: Streaming Large Result Sets
  As a developer
  I want to stream large query results
  So that my application doesn't run out of memory

  Scenario: Stream a million rows without memory exhaustion
    Given a table with 1 million rows
    When I stream the results with a fetch size of 1000
    Then memory usage stays below 100MB
    And all rows are processed successfully
```

## Test Structure

### Feature Files
- Located in `tests/bdd/features/`
- Written in Gherkin syntax
- Focus on user-facing behavior and integration scenarios

### Test Implementation
- Located in `tests/bdd/test_bdd_*.py`
- Uses real Cassandra containers (no mocks)
- Tests actual async behavior and integration points

## Running BDD Tests

### Locally
```bash
# Run all BDD tests
pytest tests/bdd -v

# Run specific feature
pytest tests/bdd/test_bdd_connection.py -v

# Run with specific markers
pytest tests/bdd -m "not slow" -v
```

### In CI
- BDD tests only run on Pull Requests (not on commits)
- This is due to their longer execution time (~5-10 minutes)
- All BDD tests must pass before a PR can be merged

## Test Categories

### 1. Connection Management (`test_bdd_connection.py`)
- Tests connection lifecycle
- Concurrent connection handling
- Connection pool behavior

### 2. Query Execution (`test_bdd_query.py`)
- Basic CRUD operations
- Prepared statements
- Query consistency levels

### 3. Error Handling (`test_bdd_error.py`)
- Network failures
- Invalid queries
- Timeout scenarios
- Recovery behavior

### 4. FastAPI Integration (`test_bdd_fastapi.py`)
- REST API endpoints
- Concurrent HTTP requests
- Application lifecycle
- Resource cleanup

### 5. Production Reliability (`test_bdd_production.py`)
- Thread pool exhaustion prevention
- Memory leak detection
- Traffic spike handling
- Performance under load

## Known Limitations

### 1. Single Node Testing
- Tests run against a single Cassandra node
- Multi-node cluster behaviors are not tested
- Replication and consistency scenarios are limited

### 2. Docker/Podman Dependency
- Requires container runtime (Docker or Podman)
- Container startup adds ~20-30 seconds overhead
- May fail in environments without container support

### 3. Performance Constraints
- Tests are slower than unit tests (~5-10 minutes total)
- Thread pool tests are limited by CI environment resources
- Memory measurements may vary across environments

### 4. Async Testing Challenges
- Uses `run_async()` helper to bridge sync/async contexts
- Event loop handling adds complexity
- Some async edge cases may not be fully covered

### 5. Limited Cassandra Features
- Does not test all Cassandra data types
- Limited testing of advanced features (TTL, LWT, etc.)
- No testing of Cassandra-specific optimizations

## Best Practices

### 1. Use Real Cassandra
- Never use mocks in BDD tests
- Test against actual database behavior
- Verify data persistence and consistency

### 2. Test User Scenarios
- Focus on end-user workflows
- Test the full stack (API to database)
- Verify error messages and recovery

### 3. Clean Test Isolation
- Each test should be independent
- Clean up test data after each scenario
- Use unique identifiers to avoid conflicts

### 4. Performance Awareness
- BDD tests are integration tests, not unit tests
- Accept that they will be slower
- Use markers to skip slow tests in development

### 5. Clear Assertions
- Avoid placeholder assertions (`assert True`)
- Test specific behavior, not just "no errors"
- Include meaningful error messages

## Common Issues

### Container Already Running
**Problem**: Port 9042 already in use
**Solution**: The fixture reuses existing containers. Kill old containers if needed:
```bash
docker ps | grep 9042
docker stop <container_id>
```

### Memory Test Failures
**Problem**: Memory assertions fail in CI
**Solution**: Memory limits are relaxed for CI environments. Local limits may be stricter.

### Thread Pool Tests
**Problem**: Thread pool tests timeout
**Solution**: Reduce concurrent operations or increase timeouts for slower environments

### Event Loop Errors
**Problem**: "RuntimeError: This event loop is already running"
**Solution**: Use the `run_async()` helper function, not `asyncio.run()`

## Writing Good BDD Tests

### 1. Focus on User Value

Ask yourself:
- What problem does this solve for users?
- How would a user describe this feature?
- What would make a user confident this works?

### 2. Test Complete Workflows

Don't test individual methods. Test complete user scenarios:

```gherkin
# ❌ Too granular
Scenario: prepare() returns PreparedStatement

# ✅ Complete workflow
Scenario: Execute thousands of queries efficiently with prepared statements
  Given I need to insert 10,000 users
  When I prepare an insert statement once
  And execute it 10,000 times with different parameters
  Then all users are inserted successfully
  And the operation completes in under 5 seconds
```

### 3. Make Tests Self-Documenting

Someone who knows nothing about the implementation should understand:
- What the test is verifying
- Why it matters
- How to use the feature

### 4. Test Edge Cases and Failures

Good BDD tests cover:
- Happy path (normal usage)
- Error conditions
- Edge cases
- Recovery scenarios

Example:
```gherkin
Scenario: Streaming continues despite transient network errors
  Given I'm streaming a large result set
  When a network timeout occurs after 1000 rows
  And the connection recovers
  Then streaming resumes automatically
  And all rows are eventually processed
```

## Adding New BDD Tests

1. **Create Feature File**
   ```gherkin
   Feature: New Feature Name
     As a developer
     I want to test specific behavior
     So that I can ensure reliability

     Scenario: Specific test case
       Given initial condition
       When action occurs
       Then expected outcome
   ```

2. **Implement Test Steps**
   ```python
   @scenario('features/new_feature.feature', 'Specific test case')
   def test_new_scenario():
       pass

   @given("initial condition")
   def setup_condition(context):
       # Setup code
       pass

   @when("action occurs")
   def perform_action(context):
       # Action code
       pass

   @then("expected outcome")
   def verify_outcome(context):
       # Assertion code
       assert actual == expected
   ```

3. **Use Real Cassandra**
   - Import cassandra_container fixture
   - Create actual tables and data
   - Test against real database behavior

4. **Handle Async Code**
   ```python
   def run_async(coro, loop):
       return loop.run_until_complete(coro)

   @when("I execute async operation")
   def async_operation(context, event_loop):
       async def _operation():
           result = await async_function()
           return result

       result = run_async(_operation(), event_loop)
       context['result'] = result
   ```

## Adding New BDD Tests - Best Practices

When adding new BDD tests:

1. **Identify User Need**: What problem are we solving?
2. **Write Feature File**: Describe behavior in Gherkin
3. **Implement Steps**: Write Python code that tests real behavior
4. **Verify in CI**: Ensure tests pass in CI environment
5. **Document**: Update documentation if you discover new patterns

Remember: BDD tests are not just tests - they're executable documentation that proves our library works as users expect in real-world scenarios.

## Maintenance

### Regular Tasks
- Review and update timeout values
- Check for flaky tests and fix root causes
- Update Cassandra container version as needed
- Monitor test execution times

### When Tests Fail
1. Check if Cassandra container is healthy
2. Verify no port conflicts
3. Check for recent code changes
4. Review test logs for specific errors
5. Run locally to reproduce

### Performance Optimization
- Use prepared statements where possible
- Avoid unnecessary setup/teardown
- Reuse connections and sessions
- Clean up resources properly
