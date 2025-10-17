#!/usr/bin/env python3
"""
Base Test Class for Databricks Helper Unit Tests
Provides common functionality and utilities for all test modules
"""

import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from databricks.connect import DatabricksSession


class BaseTest:
    """Base class for all Databricks unit tests with shared functionality."""

    def __init__(self, test_name: str):
        """
        Initialize base test with common setup.

        Args:
            test_name: Name of the test suite (e.g., "Connection Tests")
        """
        self.test_name = test_name
        self.config_path = Path(__file__).parent.parent.parent / "databricks_config" / "config.json"
        self.config = None
        self.results = []
        self.spark = None

    def load_config(self) -> bool:
        """
        Load configuration file from standard location.

        Returns:
            True if config loaded successfully, False otherwise
        """
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            return True
        except Exception as e:
            self.add_result(
                test_name="Load Configuration",
                status="FAIL",
                expected="Config file loaded successfully",
                actual=f"Failed to load config: {str(e)}",
                error=str(e)
            )
            return False

    def create_spark_session(self) -> Optional[DatabricksSession]:
        """
        Create a Databricks session using config.

        Returns:
            DatabricksSession if successful, None otherwise
        """
        try:
            if not self.config:
                self.load_config()

            spark = DatabricksSession.builder \
                .remote(
                    host=self.config['databricks']['host'],
                    cluster_id=self.config['databricks']['cluster_id']
                ) \
                .getOrCreate()

            self.spark = spark
            return spark

        except Exception as e:
            print(f"  [ERROR] Failed to create Spark session: {str(e)}")
            return None

    def cleanup_spark_session(self) -> None:
        """Clean up Spark session if it exists."""
        if self.spark:
            try:
                self.spark.stop()
                self.spark = None
            except Exception:
                pass  # Ignore cleanup errors

    def add_result(
        self,
        test_name: str,
        status: str,
        expected: str,
        actual: str,
        error: Optional[str] = None,
        duration: Optional[float] = None
    ) -> None:
        """
        Add a test result to the results list.

        Args:
            test_name: Name of the test
            status: Test status (PASS/FAIL)
            expected: Expected behavior
            actual: Actual behavior
            error: Optional error message
            duration: Optional test duration in seconds
        """
        result = {
            "test": test_name,
            "status": status,
            "expected": expected,
            "actual": actual
        }

        if error:
            result["error"] = error

        if duration is not None:
            result["duration"] = duration

        self.results.append(result)

    def run_timed_test(self, test_func, test_name: str) -> bool:
        """
        Run a test function with timing.

        Args:
            test_func: Test function to execute
            test_name: Name of the test

        Returns:
            True if test passed, False otherwise
        """
        try:
            start_time = time.time()
            result = test_func()
            duration = time.time() - start_time

            # Update the last result with duration if it exists
            if self.results and self.results[-1]["test"] == test_name:
                self.results[-1]["duration"] = duration

            return result

        except Exception as e:
            self.add_result(
                test_name=test_name,
                status="FAIL",
                expected="Test execution without errors",
                actual=f"Exception raised: {str(e)}",
                error=str(e)
            )
            print(f"  [FAIL] {str(e)}")
            return False

    def print_header(self) -> None:
        """Print test suite header."""
        print("\n" + "=" * 60)
        print(self.test_name)
        print("=" * 60)

    def generate_summary(self, start_time: float) -> Dict:
        """
        Generate test summary dictionary.

        Args:
            start_time: Start time from time.time()

        Returns:
            Dictionary with test summary
        """
        duration = time.time() - start_time
        passed = sum(1 for r in self.results if r.get("status") == "PASS")
        failed = sum(1 for r in self.results if r.get("status") == "FAIL")
        total = passed + failed
        status = "PASS" if failed == 0 else "FAIL"

        return {
            "status": status,
            "passed": passed,
            "failed": failed,
            "total": total,
            "duration": duration,
            "details": self.results
        }
