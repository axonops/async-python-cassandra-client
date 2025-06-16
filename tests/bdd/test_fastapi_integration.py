"""BDD tests for FastAPI integration."""

from pytest_bdd import scenarios

# Load all scenarios from the feature file
scenarios("features/fastapi_integration.feature")
