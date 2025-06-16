"""BDD tests for production reliability."""

from pytest_bdd import scenarios

# Load all scenarios from the feature file
scenarios("features/production_reliability.feature")
