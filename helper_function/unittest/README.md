# Databricks Helper - Unit Tests

Comprehensive unit test suite for Databricks Helper functionality.

## Overview

This test suite validates all features of the Databricks Helper including:
- Connection and authentication (Azure CLI)
- SQL query execution
- DataFrame operations
- Spark transformations and aggregations

## Test Structure

```
unittest/
├── main_unittest.py                    # Main test runner
├── test_connection.py                  # Connection and auth tests
├── test_sql_execution.py              # SQL query tests
├── test_dataframe_operations.py       # DataFrame operation tests
├── test_spark_transformations.py      # Transformation tests
└── README.md                          # This file
```

## Prerequisites

### 1. Azure CLI Authentication
```bash
# Login to Azure (required!)
az login

# Verify login
az account show
```

### 2. Running Cluster
Ensure your Databricks cluster (specified in config.json) is running or can auto-start.

### 3. Python Dependencies
```bash
pip install -r ../requirements.txt
```

## Running Tests

### Run All Tests
```bash
cd helper_function/unittest
python main_unittest.py
```

This will:
- Execute all test modules sequentially
- Display real-time progress
- Generate detailed test results
- Save results to JSON file

### Run Individual Test Module
```bash
# Connection tests only
python test_connection.py

# SQL execution tests only
python test_sql_execution.py

# DataFrame operations tests only
python test_dataframe_operations.py

# Spark transformations tests only
python test_spark_transformations.py
```

## Test Modules

### 1. Connection Tests (test_connection.py)
Tests basic connectivity and authentication.

**Tests:**
- Config file structure validation
- Azure CLI authentication verification
- Databricks session creation
- Cluster connectivity and responsiveness

**Expected Results:**
- All config fields present and valid
- Azure CLI installed and logged in
- Session created successfully
- Cluster responds to operations

### 2. SQL Execution Tests (test_sql_execution.py)
Tests SQL query execution capabilities.

**Tests:**
- Simple SELECT query
- Aggregation queries (COUNT, SUM, AVG)
- Filter queries (WHERE clause)
- JOIN operations
- Temporary view creation and querying

**Expected Results:**
- Queries execute without errors
- Results match expected values
- Aggregations calculate correctly
- JOINs produce correct result sets

### 3. DataFrame Operations Tests (test_dataframe_operations.py)
Tests basic DataFrame operations.

**Tests:**
- Create DataFrame from list
- Select columns
- Filter rows
- Add columns (withColumn)
- Collect results
- Display (show)

**Expected Results:**
- DataFrames created successfully
- Operations return correct results
- Columns added/selected as expected
- Filters produce correct row counts

### 4. Spark Transformations Tests (test_spark_transformations.py)
Tests advanced Spark transformations.

**Tests:**
- GroupBy with aggregation
- Multiple aggregations
- OrderBy transformation
- Join transformation
- Distinct transformation
- Union transformation

**Expected Results:**
- GroupBy produces correct aggregates
- Multiple aggregations work together
- OrderBy sorts correctly
- Joins produce expected results
- Distinct removes duplicates
- Union combines DataFrames correctly

## Output Format

### Console Output
```
============================================================
Databricks Helper - Unit Test Suite
============================================================
Start Time: 2025-01-15 10:30:00

============================================================
Running: Connection Tests
============================================================

Test 1: Config File Structure
  [PASS] Config structure is valid

Test 2: Azure CLI Authentication
  [PASS] Azure CLI authenticated

Test 3: Databricks Session Creation
  [PASS] Session created (Spark 3.x.x)

Test 4: Cluster Connectivity
  [PASS] Cluster is responsive (2.34s)

[PASS] Connection Tests: 4/4 tests passed

============================================================
Test Summary
============================================================
Total Tests Run: 20
Passed: 20
Failed: 0
Duration: 45.67 seconds
Success Rate: 100.0%

Module Breakdown:
  [PASS] Connection Tests: 4/4
  [PASS] SQL Execution Tests: 5/5
  [PASS] DataFrame Operations Tests: 6/6
  [PASS] Spark Transformations Tests: 6/6

============================================================
Overall Status: ALL TESTS PASSED
============================================================
```

### JSON Output
Test results are automatically saved to:
```
test_results_YYYYMMDD_HHMMSS.json
```

Format:
```json
{
  "timestamp": "2025-01-15T10:30:00",
  "duration": 45.67,
  "total_passed": 20,
  "total_failed": 0,
  "total_tests": 20,
  "modules": [
    {
      "module": "Connection Tests",
      "status": "PASS",
      "passed": 4,
      "failed": 0,
      "total": 4,
      "duration": 12.34,
      "details": [...]
    }
  ]
}
```

## Expected vs Actual Comparison

Each test compares expected results with actual results:

**Example:**
```python
{
  "test": "Simple SQL Query",
  "status": "PASS",
  "expected": "message='Hello Databricks'",
  "actual": "message='Hello Databricks'",
  "duration": 1.23
}
```

Failed tests include error details:
```python
{
  "test": "Config Structure",
  "status": "FAIL",
  "expected": "auth_type: azure-cli",
  "actual": "auth_type: token",
  "error": "Invalid auth_type"
}
```

## Troubleshooting

### "Azure CLI not available"
```bash
# Install Azure CLI
# Windows: https://aka.ms/installazurecliwindows
# macOS: brew install azure-cli
# Linux: See https://docs.microsoft.com/cli/azure/install-azure-cli-linux

# Then login
az login
```

### "Cluster not responding"
- Verify cluster is running in Databricks workspace
- Check cluster_id in config.json is correct
- Ensure Azure CLI has access to Databricks workspace

### "Session creation failed"
- Verify databricks-connect version matches cluster runtime
- Check config.json has correct host and cluster_id
- Ensure Azure login is active

### "Import errors"
```bash
# Install missing dependencies
pip install databricks-connect==14.3.0
pip install databricks-sdk
```

## Integration with CI/CD

The test suite can be integrated into CI/CD pipelines:

```bash
# Run tests and exit with appropriate code
python main_unittest.py
# Exit code 0 = all tests passed
# Exit code 1 = one or more tests failed
```

## Test Development

To add new tests:

1. Create new test file in unittest/ directory
2. Follow the pattern:
   ```python
   class MyTest:
       def setup(self): ...
       def teardown(self): ...
       def test_something(self): ...

   def run_tests():
       # Return standard result format
       return {
           "status": "PASS" or "FAIL",
           "passed": count,
           "failed": count,
           "total": count,
           "duration": seconds,
           "details": [...]
       }
   ```
3. Import in main_unittest.py
4. Add to test_modules list

## Notes

- Tests run sequentially to avoid cluster contention
- Each test module creates its own Spark session
- Sessions are properly cleaned up after tests
- Test results include timing information
- Failed tests don't stop subsequent tests from running

## Support

For issues or questions:
1. Check test output for specific error messages
2. Verify prerequisites are met (Azure CLI, cluster status)
3. Review configuration in databricks_config/config.json
4. Ensure dependencies are installed correctly
