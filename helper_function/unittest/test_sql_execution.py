#!/usr/bin/env python3
"""
SQL Execution Tests for Databricks Helper
Tests SQL query execution capabilities
"""

import sys
import json
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks.connect import DatabricksSession


class SQLExecutionTest:
    def __init__(self):
        self.config_path = Path(__file__).parent.parent.parent / "databricks_config" / "config.json"
        self.config = None
        self.spark = None
        self.results = []

    def setup(self):
        """Setup Spark session"""
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)

            self.spark = DatabricksSession.builder \
                .remote(
                    host=self.config['databricks']['host'],
                    cluster_id=self.config['databricks']['cluster_id']
                ) \
                .getOrCreate()
            return True
        except Exception as e:
            print(f"Setup failed: {str(e)}")
            return False

    def teardown(self):
        """Clean up Spark session"""
        if self.spark:
            self.spark.stop()

    def test_simple_sql_query(self):
        """Test 1: Execute simple SQL query"""
        print("\nTest 1: Simple SQL Query")

        try:
            start_time = time.time()
            result = self.spark.sql("SELECT 'Hello Databricks' as message")
            rows = result.collect()
            duration = time.time() - start_time

            expected_message = "Hello Databricks"
            actual_message = rows[0]['message']

            if actual_message == expected_message:
                self.results.append({
                    "test": "Simple SQL Query",
                    "status": "PASS",
                    "expected": f"message='{expected_message}'",
                    "actual": f"message='{actual_message}'",
                    "duration": duration
                })
                print(f"  [PASS] Query executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "Simple SQL Query",
                    "status": "FAIL",
                    "expected": f"message='{expected_message}'",
                    "actual": f"message='{actual_message}'"
                })
                print(f"  [FAIL] Unexpected result")
                return False

        except Exception as e:
            self.results.append({
                "test": "Simple SQL Query",
                "status": "FAIL",
                "error": str(e),
                "expected": "Query executes successfully",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_sql_with_aggregation(self):
        """Test 2: SQL query with aggregation"""
        print("\nTest 2: SQL Query with Aggregation")

        try:
            start_time = time.time()
            result = self.spark.sql("""
                SELECT
                    COUNT(*) as count,
                    SUM(value) as total,
                    AVG(value) as average
                FROM (
                    SELECT 1 as value UNION ALL
                    SELECT 2 as value UNION ALL
                    SELECT 3 as value UNION ALL
                    SELECT 4 as value UNION ALL
                    SELECT 5 as value
                )
            """)
            rows = result.collect()
            duration = time.time() - start_time

            expected_count = 5
            expected_total = 15
            expected_average = 3.0

            actual_count = rows[0]['count']
            actual_total = rows[0]['total']
            actual_average = rows[0]['average']

            if (actual_count == expected_count and
                actual_total == expected_total and
                actual_average == expected_average):
                self.results.append({
                    "test": "SQL Aggregation",
                    "status": "PASS",
                    "expected": f"count={expected_count}, total={expected_total}, avg={expected_average}",
                    "actual": f"count={actual_count}, total={actual_total}, avg={actual_average}",
                    "duration": duration
                })
                print(f"  [PASS] Aggregation executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "SQL Aggregation",
                    "status": "FAIL",
                    "expected": f"count={expected_count}, total={expected_total}, avg={expected_average}",
                    "actual": f"count={actual_count}, total={actual_total}, avg={actual_average}"
                })
                print(f"  [FAIL] Unexpected aggregation results")
                return False

        except Exception as e:
            self.results.append({
                "test": "SQL Aggregation",
                "status": "FAIL",
                "error": str(e),
                "expected": "Aggregation query executes successfully",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_sql_with_filter(self):
        """Test 3: SQL query with WHERE clause"""
        print("\nTest 3: SQL Query with Filter")

        try:
            start_time = time.time()
            result = self.spark.sql("""
                SELECT value
                FROM (
                    SELECT 1 as value UNION ALL
                    SELECT 2 as value UNION ALL
                    SELECT 3 as value UNION ALL
                    SELECT 4 as value UNION ALL
                    SELECT 5 as value
                )
                WHERE value > 3
            """)
            rows = result.collect()
            duration = time.time() - start_time

            expected_count = 2
            expected_values = [4, 5]
            actual_values = sorted([row['value'] for row in rows])

            if actual_values == expected_values:
                self.results.append({
                    "test": "SQL Filter",
                    "status": "PASS",
                    "expected": f"values={expected_values}",
                    "actual": f"values={actual_values}",
                    "duration": duration
                })
                print(f"  [PASS] Filter executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "SQL Filter",
                    "status": "FAIL",
                    "expected": f"values={expected_values}",
                    "actual": f"values={actual_values}"
                })
                print(f"  [FAIL] Unexpected filter results")
                return False

        except Exception as e:
            self.results.append({
                "test": "SQL Filter",
                "status": "FAIL",
                "error": str(e),
                "expected": "Filter query executes successfully",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_sql_with_join(self):
        """Test 4: SQL query with JOIN"""
        print("\nTest 4: SQL Query with JOIN")

        try:
            start_time = time.time()
            result = self.spark.sql("""
                SELECT a.id, a.name, b.score
                FROM (
                    SELECT 1 as id, 'Alice' as name UNION ALL
                    SELECT 2 as id, 'Bob' as name UNION ALL
                    SELECT 3 as id, 'Charlie' as name
                ) a
                JOIN (
                    SELECT 1 as id, 95 as score UNION ALL
                    SELECT 2 as id, 87 as score UNION ALL
                    SELECT 3 as id, 92 as score
                ) b
                ON a.id = b.id
            """)
            rows = result.collect()
            duration = time.time() - start_time

            expected_count = 3
            expected_names = ['Alice', 'Bob', 'Charlie']
            actual_names = sorted([row['name'] for row in rows])

            if len(rows) == expected_count and actual_names == expected_names:
                self.results.append({
                    "test": "SQL JOIN",
                    "status": "PASS",
                    "expected": f"count={expected_count}, names={expected_names}",
                    "actual": f"count={len(rows)}, names={actual_names}",
                    "duration": duration
                })
                print(f"  [PASS] JOIN executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "SQL JOIN",
                    "status": "FAIL",
                    "expected": f"count={expected_count}, names={expected_names}",
                    "actual": f"count={len(rows)}, names={actual_names}"
                })
                print(f"  [FAIL] Unexpected JOIN results")
                return False

        except Exception as e:
            self.results.append({
                "test": "SQL JOIN",
                "status": "FAIL",
                "error": str(e),
                "expected": "JOIN query executes successfully",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_sql_create_temp_view(self):
        """Test 5: Create and query temporary view"""
        print("\nTest 5: Create and Query Temporary View")

        try:
            # Create temp view
            start_time = time.time()
            df = self.spark.createDataFrame(
                [(1, 'Apple'), (2, 'Banana'), (3, 'Cherry')],
                ['id', 'fruit']
            )
            df.createOrReplaceTempView("test_fruits")

            # Query the temp view
            result = self.spark.sql("SELECT fruit FROM test_fruits WHERE id = 2")
            rows = result.collect()
            duration = time.time() - start_time

            expected_fruit = "Banana"
            actual_fruit = rows[0]['fruit']

            if actual_fruit == expected_fruit:
                self.results.append({
                    "test": "SQL Temporary View",
                    "status": "PASS",
                    "expected": f"fruit='{expected_fruit}'",
                    "actual": f"fruit='{actual_fruit}'",
                    "duration": duration
                })
                print(f"  [PASS] Temp view created and queried ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "SQL Temporary View",
                    "status": "FAIL",
                    "expected": f"fruit='{expected_fruit}'",
                    "actual": f"fruit='{actual_fruit}'"
                })
                print(f"  [FAIL] Unexpected result from temp view")
                return False

        except Exception as e:
            self.results.append({
                "test": "SQL Temporary View",
                "status": "FAIL",
                "error": str(e),
                "expected": "Temp view works correctly",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False


def run_tests():
    """Run all SQL execution tests"""
    print("\n" + "="*60)
    print("SQL Execution Tests")
    print("="*60)

    tester = SQLExecutionTest()
    start_time = time.time()

    # Setup
    if not tester.setup():
        return {
            "status": "FAIL",
            "passed": 0,
            "failed": 1,
            "total": 1,
            "duration": time.time() - start_time,
            "details": [{"test": "Setup", "status": "FAIL", "error": "Failed to create Spark session"}]
        }

    # Run tests
    tests = [
        tester.test_simple_sql_query,
        tester.test_sql_with_aggregation,
        tester.test_sql_with_filter,
        tester.test_sql_with_join,
        tester.test_sql_create_temp_view
    ]

    passed = 0
    failed = 0

    for test in tests:
        if test():
            passed += 1
        else:
            failed += 1

    # Teardown
    tester.teardown()

    duration = time.time() - start_time
    status = "PASS" if failed == 0 else "FAIL"

    return {
        "status": status,
        "passed": passed,
        "failed": failed,
        "total": passed + failed,
        "duration": duration,
        "details": tester.results
    }


if __name__ == "__main__":
    result = run_tests()
    print(f"\nResult: {result['status']} ({result['passed']}/{result['total']} passed)")
    sys.exit(0 if result['status'] == "PASS" else 1)
