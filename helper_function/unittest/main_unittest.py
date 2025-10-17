#!/usr/bin/env python3
"""
Main Unit Test Runner for Databricks Helper
Executes all test modules and reports consolidated results
"""

import sys
import json
import time
import warnings
from pathlib import Path
from datetime import datetime

# Suppress Databricks Connect session cleanup warnings
warnings.filterwarnings('ignore', category=UserWarning, module='pyspark.sql.connect.client.reattach')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import test modules
from unittest import test_connection
from unittest import test_sql_execution
from unittest import test_dataframe_operations
from unittest import test_spark_transformations
from unittest import test_html_export


class TestRunner:
    def __init__(self):
        self.results = []
        self.start_time = None
        self.end_time = None

    def run_test_module(self, module, module_name):
        """Run a test module and capture results"""
        print(f"\n{'='*60}")
        print(f"Running: {module_name}")
        print(f"{'='*60}")

        try:
            # Each test module should have a run_tests() function
            result = module.run_tests()

            self.results.append({
                "module": module_name,
                "status": result.get("status", "UNKNOWN"),
                "passed": result.get("passed", 0),
                "failed": result.get("failed", 0),
                "total": result.get("total", 0),
                "duration": result.get("duration", 0),
                "details": result.get("details", []),
                "timestamp": datetime.now().isoformat()
            })

            # Print summary
            status_emoji = "PASS" if result.get("status") == "PASS" else "FAIL"
            print(f"\n[{status_emoji}] {module_name}: {result.get('passed')}/{result.get('total')} tests passed")

            return result.get("status") == "PASS"

        except Exception as e:
            print(f"\n[ERROR] Failed to run {module_name}: {str(e)}")
            self.results.append({
                "module": module_name,
                "status": "ERROR",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            return False

    def run_all_tests(self):
        """Run all test modules"""
        self.start_time = time.time()

        print("\n" + "="*60)
        print("Databricks Helper - Unit Test Suite")
        print("="*60)
        print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Test modules to run (in order)
        test_modules = [
            (test_connection, "Connection Tests"),
            (test_sql_execution, "SQL Execution Tests"),
            (test_dataframe_operations, "DataFrame Operations Tests"),
            (test_spark_transformations, "Spark Transformations Tests"),
            (test_html_export, "HTML Log Export Tests"),
        ]

        # Run each test module
        all_passed = True
        for module, name in test_modules:
            passed = self.run_test_module(module, name)
            if not passed:
                all_passed = False

        self.end_time = time.time()

        # Print final summary
        self.print_summary()

        # Save results to JSON
        self.save_results()

        return all_passed

    def print_summary(self):
        """Print overall test summary"""
        print("\n" + "="*60)
        print("Test Summary")
        print("="*60)

        total_passed = sum(r.get("passed", 0) for r in self.results)
        total_failed = sum(r.get("failed", 0) for r in self.results)
        total_tests = sum(r.get("total", 0) for r in self.results)
        duration = self.end_time - self.start_time

        print(f"Total Tests Run: {total_tests}")
        print(f"Passed: {total_passed}")
        print(f"Failed: {total_failed}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Success Rate: {(total_passed/total_tests*100) if total_tests > 0 else 0:.1f}%")

        # Module breakdown
        print(f"\nModule Breakdown:")
        for result in self.results:
            status = result.get("status", "UNKNOWN")
            module = result.get("module", "Unknown")
            passed = result.get("passed", 0)
            total = result.get("total", 0)

            status_symbol = "PASS" if status == "PASS" else "FAIL" if status == "FAIL" else "ERROR"
            print(f"  [{status_symbol}] {module}: {passed}/{total}")

        # Overall status
        print(f"\n{'='*60}")
        overall_status = "ALL TESTS PASSED" if total_failed == 0 else f"{total_failed} TEST(S) FAILED"
        print(f"Overall Status: {overall_status}")
        print(f"{'='*60}\n")

    def save_results(self):
        """Save test results to JSON file"""
        output_file = Path(__file__).parent / f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        summary = {
            "timestamp": datetime.now().isoformat(),
            "duration": self.end_time - self.start_time,
            "total_passed": sum(r.get("passed", 0) for r in self.results),
            "total_failed": sum(r.get("failed", 0) for r in self.results),
            "total_tests": sum(r.get("total", 0) for r in self.results),
            "modules": self.results
        }

        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)

        print(f"Results saved to: {output_file}")


def main():
    """Main entry point"""
    runner = TestRunner()
    success = runner.run_all_tests()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
