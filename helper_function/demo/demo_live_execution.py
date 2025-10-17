#!/usr/bin/env python3
"""
Demo: Live Databricks Connect Execution for Claude
Shows real-time Spark execution with immediate results
"""

import sys
import time
from pathlib import Path
sys.path.append('../')

import json
from databricks.connect import DatabricksSession


def demo_live_spark_execution():
    """Demonstrate live Spark execution for Claude analysis."""
    print("LIVE DATABRICKS CONNECT EXECUTION DEMO")
    print("=" * 50)

    # Load config from centralized location
    config_path = Path(__file__).parent.parent.parent / "databricks_config" / "config.json"
    with open(config_path, 'r') as f:
        config = json.load(f)

    try:
        # Create session - this connects directly to cluster using Azure CLI auth
        print("1. Creating Databricks Connect session (Azure CLI auth)...")
        spark = DatabricksSession.builder \
            .remote(
                host=config['databricks']['host'],
                cluster_id=config['databricks']['cluster_id']
            ) \
            .getOrCreate()

        print("   SUCCESS: Connected to cluster using Azure CLI authentication!")

        # Demo 1: Immediate DataFrame operations
        print("\n2. Live DataFrame execution...")
        print("   Creating DataFrame...")
        data = [
            ("Engineering", "Alice", 85000),
            ("Sales", "Bob", 70000),
            ("Engineering", "Charlie", 95000),
            ("Marketing", "Diana", 65000),
            ("Sales", "Eve", 75000)
        ]
        df = spark.createDataFrame(data, ["department", "name", "salary"])

        print(f"   Rows created: {df.count()}")
        print("   Sample data:")
        df.show()

        # Demo 2: Live SQL operations
        print("\n3. Live SQL execution...")
        df.createOrReplaceTempView("employees")

        print("   SQL: Average salary by department")
        avg_salary = spark.sql("""
            SELECT department,
                   AVG(salary) as avg_salary,
                   COUNT(*) as employee_count
            FROM employees
            GROUP BY department
            ORDER BY avg_salary DESC
        """)
        avg_salary.show()

        # Demo 3: Live transformations
        print("\n4. Live data transformations...")
        high_earners = df.filter(df.salary > 70000)
        print(f"   High earners (>70k): {high_earners.count()}")
        high_earners.select("name", "department", "salary").show()

        # Demo 4: Live aggregations
        print("\n5. Live aggregations...")
        summary = spark.sql("""
            SELECT
                MIN(salary) as min_salary,
                MAX(salary) as max_salary,
                AVG(salary) as avg_salary,
                COUNT(*) as total_employees
            FROM employees
        """)
        print("   Company salary summary:")
        summary.show()

        # Demo 5: Live complex query
        print("\n6. Live complex analysis...")
        analysis = spark.sql("""
            SELECT
                department,
                COUNT(*) as headcount,
                AVG(salary) as avg_salary,
                CASE
                    WHEN AVG(salary) > 80000 THEN 'High Pay'
                    WHEN AVG(salary) > 70000 THEN 'Medium Pay'
                    ELSE 'Standard Pay'
                END as pay_category
            FROM employees
            GROUP BY department
            ORDER BY avg_salary DESC
        """)
        print("   Department analysis:")
        analysis.show()

        spark.stop()

        print("\n" + "=" * 50)
        print("SUCCESS: LIVE EXECUTION COMPLETE!")
        print("All operations executed in real-time on cluster!")
        print("Results available immediately for Claude analysis!")
        print("=" * 50)

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("Starting live execution demo...")
    success = demo_live_spark_execution()

    if success:
        print("\nREADY: Claude can now analyze live Databricks results!")
    else:
        print("\nERROR: Live execution failed")
        sys.exit(1)