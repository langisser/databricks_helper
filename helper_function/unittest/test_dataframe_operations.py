#!/usr/bin/env python3
"""
DataFrame Operations Tests for Databricks Helper
Tests DataFrame creation and basic operations
"""

import sys
import json
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, lit


class DataFrameOperationsTest:
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

    def test_create_dataframe_from_list(self):
        """Test 1: Create DataFrame from list"""
        print("\nTest 1: Create DataFrame from List")

        try:
            start_time = time.time()
            data = [
                (1, "Alice", 25),
                (2, "Bob", 30),
                (3, "Charlie", 35)
            ]
            df = self.spark.createDataFrame(data, ["id", "name", "age"])
            count = df.count()
            duration = time.time() - start_time

            expected_count = 3
            if count == expected_count:
                self.results.append({
                    "test": "Create DataFrame from List",
                    "status": "PASS",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}",
                    "duration": duration
                })
                print(f"  [PASS] DataFrame created successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "Create DataFrame from List",
                    "status": "FAIL",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}"
                })
                print(f"  [FAIL] Unexpected count")
                return False

        except Exception as e:
            self.results.append({
                "test": "Create DataFrame from List",
                "status": "FAIL",
                "error": str(e),
                "expected": "DataFrame created from list",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_dataframe_select(self):
        """Test 2: Select columns from DataFrame"""
        print("\nTest 2: Select Columns")

        try:
            start_time = time.time()
            data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
            df = self.spark.createDataFrame(data, ["id", "name", "age"])

            # Select specific columns
            result_df = df.select("name", "age")
            columns = result_df.columns
            duration = time.time() - start_time

            expected_columns = ["name", "age"]
            if columns == expected_columns:
                self.results.append({
                    "test": "DataFrame Select",
                    "status": "PASS",
                    "expected": f"columns={expected_columns}",
                    "actual": f"columns={columns}",
                    "duration": duration
                })
                print(f"  [PASS] Select executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "DataFrame Select",
                    "status": "FAIL",
                    "expected": f"columns={expected_columns}",
                    "actual": f"columns={columns}"
                })
                print(f"  [FAIL] Unexpected columns")
                return False

        except Exception as e:
            self.results.append({
                "test": "DataFrame Select",
                "status": "FAIL",
                "error": str(e),
                "expected": "Select operation works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_dataframe_filter(self):
        """Test 3: Filter DataFrame rows"""
        print("\nTest 3: Filter DataFrame")

        try:
            start_time = time.time()
            data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
            df = self.spark.createDataFrame(data, ["id", "name", "age"])

            # Filter rows
            result_df = df.filter(col("age") > 28)
            count = result_df.count()
            duration = time.time() - start_time

            expected_count = 2
            if count == expected_count:
                self.results.append({
                    "test": "DataFrame Filter",
                    "status": "PASS",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}",
                    "duration": duration
                })
                print(f"  [PASS] Filter executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "DataFrame Filter",
                    "status": "FAIL",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}"
                })
                print(f"  [FAIL] Unexpected filter result")
                return False

        except Exception as e:
            self.results.append({
                "test": "DataFrame Filter",
                "status": "FAIL",
                "error": str(e),
                "expected": "Filter operation works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_dataframe_withcolumn(self):
        """Test 4: Add new column to DataFrame"""
        print("\nTest 4: Add Column with withColumn")

        try:
            start_time = time.time()
            data = [(1, "Alice", 25), (2, "Bob", 30)]
            df = self.spark.createDataFrame(data, ["id", "name", "age"])

            # Add new column
            result_df = df.withColumn("age_plus_10", col("age") + 10)
            rows = result_df.collect()
            duration = time.time() - start_time

            expected_value = 35  # Alice's age (25) + 10
            actual_value = rows[0]["age_plus_10"]

            if actual_value == expected_value and "age_plus_10" in result_df.columns:
                self.results.append({
                    "test": "DataFrame withColumn",
                    "status": "PASS",
                    "expected": f"new column added, first value={expected_value}",
                    "actual": f"column added, first value={actual_value}",
                    "duration": duration
                })
                print(f"  [PASS] withColumn executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "DataFrame withColumn",
                    "status": "FAIL",
                    "expected": f"value={expected_value}",
                    "actual": f"value={actual_value}"
                })
                print(f"  [FAIL] Unexpected result")
                return False

        except Exception as e:
            self.results.append({
                "test": "DataFrame withColumn",
                "status": "FAIL",
                "error": str(e),
                "expected": "withColumn operation works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_dataframe_collect(self):
        """Test 5: Collect DataFrame rows"""
        print("\nTest 5: Collect DataFrame Rows")

        try:
            start_time = time.time()
            data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
            df = self.spark.createDataFrame(data, ["id", "name"])

            rows = df.collect()
            duration = time.time() - start_time

            expected_count = 3
            expected_names = ["Alice", "Bob", "Charlie"]
            actual_names = [row["name"] for row in rows]

            if len(rows) == expected_count and actual_names == expected_names:
                self.results.append({
                    "test": "DataFrame Collect",
                    "status": "PASS",
                    "expected": f"count={expected_count}, names={expected_names}",
                    "actual": f"count={len(rows)}, names={actual_names}",
                    "duration": duration
                })
                print(f"  [PASS] Collect executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "DataFrame Collect",
                    "status": "FAIL",
                    "expected": f"count={expected_count}, names={expected_names}",
                    "actual": f"count={len(rows)}, names={actual_names}"
                })
                print(f"  [FAIL] Unexpected collect result")
                return False

        except Exception as e:
            self.results.append({
                "test": "DataFrame Collect",
                "status": "FAIL",
                "error": str(e),
                "expected": "Collect operation works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_dataframe_show(self):
        """Test 6: Show DataFrame (display functionality)"""
        print("\nTest 6: Show DataFrame")

        try:
            start_time = time.time()
            data = [(1, "Alice"), (2, "Bob")]
            df = self.spark.createDataFrame(data, ["id", "name"])

            # This will print to stdout but we just verify it doesn't error
            df.show()
            duration = time.time() - start_time

            self.results.append({
                "test": "DataFrame Show",
                "status": "PASS",
                "expected": "Show executes without error",
                "actual": "Show executed successfully",
                "duration": duration
            })
            print(f"  [PASS] Show executed successfully ({duration:.2f}s)")
            return True

        except Exception as e:
            self.results.append({
                "test": "DataFrame Show",
                "status": "FAIL",
                "error": str(e),
                "expected": "Show operation works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False


def run_tests():
    """Run all DataFrame operations tests"""
    print("\n" + "="*60)
    print("DataFrame Operations Tests")
    print("="*60)

    tester = DataFrameOperationsTest()
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
        tester.test_create_dataframe_from_list,
        tester.test_dataframe_select,
        tester.test_dataframe_filter,
        tester.test_dataframe_withcolumn,
        tester.test_dataframe_collect,
        tester.test_dataframe_show
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
