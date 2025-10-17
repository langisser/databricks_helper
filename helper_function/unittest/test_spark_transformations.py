#!/usr/bin/env python3
"""
Spark Transformations Tests for Databricks Helper
Tests advanced transformations, aggregations, and groupBy operations
"""

import sys
import json
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max, min as spark_min


class SparkTransformationsTest:
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

    def test_groupby_aggregation(self):
        """Test 1: GroupBy with aggregation"""
        print("\nTest 1: GroupBy with Aggregation")

        try:
            start_time = time.time()
            data = [
                ("Sales", 100),
                ("Sales", 200),
                ("Marketing", 150),
                ("Marketing", 250),
                ("IT", 300)
            ]
            df = self.spark.createDataFrame(data, ["department", "amount"])

            result_df = df.groupBy("department").agg(spark_sum("amount").alias("total"))
            rows = result_df.collect()
            duration = time.time() - start_time

            # Convert to dict for easier checking
            results_dict = {row["department"]: row["total"] for row in rows}

            expected = {"Sales": 300, "Marketing": 400, "IT": 300}
            if results_dict == expected:
                self.results.append({
                    "test": "GroupBy Aggregation",
                    "status": "PASS",
                    "expected": str(expected),
                    "actual": str(results_dict),
                    "duration": duration
                })
                print(f"  [PASS] GroupBy aggregation executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "GroupBy Aggregation",
                    "status": "FAIL",
                    "expected": str(expected),
                    "actual": str(results_dict)
                })
                print(f"  [FAIL] Unexpected aggregation result")
                return False

        except Exception as e:
            self.results.append({
                "test": "GroupBy Aggregation",
                "status": "FAIL",
                "error": str(e),
                "expected": "GroupBy aggregation works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_multiple_aggregations(self):
        """Test 2: Multiple aggregations on same groupBy"""
        print("\nTest 2: Multiple Aggregations")

        try:
            start_time = time.time()
            data = [
                ("A", 10),
                ("A", 20),
                ("A", 30),
                ("B", 40),
                ("B", 50)
            ]
            df = self.spark.createDataFrame(data, ["category", "value"])

            result_df = df.groupBy("category").agg(
                spark_sum("value").alias("total"),
                avg("value").alias("average"),
                count("value").alias("count")
            )
            rows = result_df.collect()
            duration = time.time() - start_time

            # Find results for category A
            a_result = [r for r in rows if r["category"] == "A"][0]

            expected_total = 60
            expected_avg = 20.0
            expected_count = 3

            if (a_result["total"] == expected_total and
                a_result["average"] == expected_avg and
                a_result["count"] == expected_count):
                self.results.append({
                    "test": "Multiple Aggregations",
                    "status": "PASS",
                    "expected": f"total={expected_total}, avg={expected_avg}, count={expected_count}",
                    "actual": f"total={a_result['total']}, avg={a_result['average']}, count={a_result['count']}",
                    "duration": duration
                })
                print(f"  [PASS] Multiple aggregations executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "Multiple Aggregations",
                    "status": "FAIL",
                    "expected": f"total={expected_total}, avg={expected_avg}, count={expected_count}",
                    "actual": f"total={a_result['total']}, avg={a_result['average']}, count={a_result['count']}"
                })
                print(f"  [FAIL] Unexpected aggregation results")
                return False

        except Exception as e:
            self.results.append({
                "test": "Multiple Aggregations",
                "status": "FAIL",
                "error": str(e),
                "expected": "Multiple aggregations work",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_orderby_transformation(self):
        """Test 3: OrderBy transformation"""
        print("\nTest 3: OrderBy Transformation")

        try:
            start_time = time.time()
            data = [(3, "Charlie"), (1, "Alice"), (2, "Bob")]
            df = self.spark.createDataFrame(data, ["id", "name"])

            result_df = df.orderBy("id")
            rows = result_df.collect()
            duration = time.time() - start_time

            expected_names = ["Alice", "Bob", "Charlie"]
            actual_names = [row["name"] for row in rows]

            if actual_names == expected_names:
                self.results.append({
                    "test": "OrderBy Transformation",
                    "status": "PASS",
                    "expected": f"names={expected_names}",
                    "actual": f"names={actual_names}",
                    "duration": duration
                })
                print(f"  [PASS] OrderBy executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "OrderBy Transformation",
                    "status": "FAIL",
                    "expected": f"names={expected_names}",
                    "actual": f"names={actual_names}"
                })
                print(f"  [FAIL] Unexpected order")
                return False

        except Exception as e:
            self.results.append({
                "test": "OrderBy Transformation",
                "status": "FAIL",
                "error": str(e),
                "expected": "OrderBy works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_join_transformation(self):
        """Test 4: Join transformation"""
        print("\nTest 4: Join Transformation")

        try:
            start_time = time.time()
            df1 = self.spark.createDataFrame(
                [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
                ["id", "name"]
            )
            df2 = self.spark.createDataFrame(
                [(1, 95), (2, 87), (3, 92)],
                ["id", "score"]
            )

            result_df = df1.join(df2, "id")
            rows = result_df.collect()
            duration = time.time() - start_time

            expected_count = 3
            # Check if Alice's score is 95
            alice_score = [r["score"] for r in rows if r["name"] == "Alice"][0]

            if len(rows) == expected_count and alice_score == 95:
                self.results.append({
                    "test": "Join Transformation",
                    "status": "PASS",
                    "expected": f"count={expected_count}, Alice's score=95",
                    "actual": f"count={len(rows)}, Alice's score={alice_score}",
                    "duration": duration
                })
                print(f"  [PASS] Join executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "Join Transformation",
                    "status": "FAIL",
                    "expected": f"count={expected_count}, Alice's score=95",
                    "actual": f"count={len(rows)}, Alice's score={alice_score}"
                })
                print(f"  [FAIL] Unexpected join result")
                return False

        except Exception as e:
            self.results.append({
                "test": "Join Transformation",
                "status": "FAIL",
                "error": str(e),
                "expected": "Join works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_distinct_transformation(self):
        """Test 5: Distinct transformation"""
        print("\nTest 5: Distinct Transformation")

        try:
            start_time = time.time()
            data = [(1, "A"), (2, "B"), (1, "A"), (3, "C"), (2, "B")]
            df = self.spark.createDataFrame(data, ["id", "category"])

            result_df = df.distinct()
            count = result_df.count()
            duration = time.time() - start_time

            expected_count = 3  # Only 3 distinct rows
            if count == expected_count:
                self.results.append({
                    "test": "Distinct Transformation",
                    "status": "PASS",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}",
                    "duration": duration
                })
                print(f"  [PASS] Distinct executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "Distinct Transformation",
                    "status": "FAIL",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}"
                })
                print(f"  [FAIL] Unexpected distinct count")
                return False

        except Exception as e:
            self.results.append({
                "test": "Distinct Transformation",
                "status": "FAIL",
                "error": str(e),
                "expected": "Distinct works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_union_transformation(self):
        """Test 6: Union transformation"""
        print("\nTest 6: Union Transformation")

        try:
            start_time = time.time()
            df1 = self.spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
            df2 = self.spark.createDataFrame([(3, "C"), (4, "D")], ["id", "value"])

            result_df = df1.union(df2)
            count = result_df.count()
            duration = time.time() - start_time

            expected_count = 4
            if count == expected_count:
                self.results.append({
                    "test": "Union Transformation",
                    "status": "PASS",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}",
                    "duration": duration
                })
                print(f"  [PASS] Union executed successfully ({duration:.2f}s)")
                return True
            else:
                self.results.append({
                    "test": "Union Transformation",
                    "status": "FAIL",
                    "expected": f"count={expected_count}",
                    "actual": f"count={count}"
                })
                print(f"  [FAIL] Unexpected union count")
                return False

        except Exception as e:
            self.results.append({
                "test": "Union Transformation",
                "status": "FAIL",
                "error": str(e),
                "expected": "Union works",
                "actual": f"Error: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False


def run_tests():
    """Run all Spark transformations tests"""
    print("\n" + "="*60)
    print("Spark Transformations Tests")
    print("="*60)

    tester = SparkTransformationsTest()
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
        tester.test_groupby_aggregation,
        tester.test_multiple_aggregations,
        tester.test_orderby_transformation,
        tester.test_join_transformation,
        tester.test_distinct_transformation,
        tester.test_union_transformation
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
