# Test Suite for orchestra-dbt

This directory contains the test suite for the orchestra-dbt CLI tool. The tests are organized following best practices for CLI tool testing.

## Running Tests

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run only integration tests
pytest tests/integration/

# Run with coverage
pytest --cov=orchestra_dbt --cov-report=html

# Run specific test file
pytest tests/unit/test_cache.py

# Run specific test
pytest tests/unit/test_cache.py::TestLoadState::test_load_state_success
```
