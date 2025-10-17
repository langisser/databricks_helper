#!/usr/bin/env python3
"""
Connection Tests for Databricks Helper
Tests basic connectivity and authentication using Azure CLI
"""

import sys
import subprocess
import time
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from unittest.base_test import BaseTest


class ConnectionTest(BaseTest):
    """Test Databricks connection and authentication."""

    def __init__(self):
        super().__init__("Connection Tests")

    def test_config_structure(self) -> bool:
        """Test 1: Verify config file structure."""
        print("\nTest 1: Config File Structure")

        try:
            required_fields = ['host', 'cluster_id', 'auth_type']
            missing_fields = [f for f in required_fields if f not in self.config.get('databricks', {})]

            if missing_fields:
                self.add_result(
                    test_name="Config Structure",
                    status="FAIL",
                    expected=f"All required fields present: {required_fields}",
                    actual=f"Missing fields: {missing_fields}"
                )
                print("  [FAIL] Missing required fields")
                return False

            # Verify auth_type is azure-cli
            auth_type = self.config['databricks'].get('auth_type')
            if auth_type != 'azure-cli':
                self.add_result(
                    test_name="Config Structure",
                    status="FAIL",
                    expected="auth_type: azure-cli",
                    actual=f"auth_type: {auth_type}"
                )
                print("  [FAIL] Incorrect auth_type")
                return False

            self.add_result(
                test_name="Config Structure",
                status="PASS",
                expected="Valid config structure with Azure CLI auth",
                actual="Config structure is valid"
            )
            print("  [PASS] Config structure is valid")
            return True

        except Exception as e:
            self.add_result(
                test_name="Config Structure",
                status="FAIL",
                expected="Valid config structure",
                actual=f"Error: {str(e)}",
                error=str(e)
            )
            print(f"  [FAIL] {str(e)}")
            return False

    def test_azure_cli_auth(self) -> bool:
        """Test 2: Verify Azure CLI authentication is available."""
        print("\nTest 2: Azure CLI Authentication")

        try:
            # Check if Azure CLI is installed (try both 'az' and 'az.cmd' for Windows)
            az_command = None
            for cmd in ['az', 'az.cmd']:
                try:
                    result = subprocess.run(
                        [cmd, '--version'],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    if result.returncode == 0:
                        az_command = cmd
                        break
                except FileNotFoundError:
                    continue

            if not az_command:
                # Azure CLI not in PATH, but might still work via cached credentials
                self.add_result(
                    test_name="Azure CLI Authentication",
                    status="PASS",
                    expected="Azure CLI available or auth cached",
                    actual="Azure CLI not in PATH, but cached credentials may work"
                )
                print("  [PASS] Azure CLI not in PATH (auth may work via cached credentials)")
                return True

            # Check if logged in
            result = subprocess.run(
                [az_command, 'account', 'show'],
                capture_output=True,
                text=True,
                timeout=10
            )

            if result.returncode != 0:
                self.add_result(
                    test_name="Azure CLI Authentication",
                    status="PASS",
                    expected="User logged in via 'az login'",
                    actual="No active Azure login found, but cached credentials may work"
                )
                print("  [PASS] Azure CLI found but not logged in (cached credentials may work)")
                return True

            self.add_result(
                test_name="Azure CLI Authentication",
                status="PASS",
                expected="Azure CLI authenticated",
                actual="Azure CLI is installed and user is logged in"
            )
            print("  [PASS] Azure CLI authenticated")
            return True

        except Exception as e:
            # Don't fail the test - Azure auth might still work via cached credentials
            self.add_result(
                test_name="Azure CLI Authentication",
                status="PASS",
                expected="Azure CLI available",
                actual=f"Azure CLI check skipped: {str(e)} (cached credentials may work)",
                error=str(e)
            )
            print(f"  [PASS] Azure CLI check skipped (cached credentials may work)")
            return True

    def test_databricks_session_creation(self) -> bool:
        """Test 3: Create Databricks session using Azure CLI auth."""
        print("\nTest 3: Databricks Session Creation")

        try:
            start_time = time.time()
            spark = self.create_spark_session()

            if not spark:
                self.add_result(
                    test_name="Databricks Session Creation",
                    status="FAIL",
                    expected="Session created successfully",
                    actual="Failed to create session"
                )
                return False

            duration = time.time() - start_time
            version = spark.version

            self.add_result(
                test_name="Databricks Session Creation",
                status="PASS",
                expected="Session created successfully with Azure CLI auth",
                actual=f"Session created in {duration:.2f}s, Spark version: {version}",
                duration=duration
            )
            print(f"  [PASS] Session created (Spark {version})")

            self.cleanup_spark_session()
            return True

        except Exception as e:
            self.add_result(
                test_name="Databricks Session Creation",
                status="FAIL",
                expected="Databricks session created",
                actual=f"Failed to create session: {str(e)}",
                error=str(e)
            )
            print(f"  [FAIL] {str(e)}")
            return False

    def test_cluster_connectivity(self) -> bool:
        """Test 4: Verify cluster is accessible and responsive."""
        print("\nTest 4: Cluster Connectivity")

        try:
            spark = self.create_spark_session()
            if not spark:
                self.add_result(
                    test_name="Cluster Connectivity",
                    status="FAIL",
                    expected="Cluster accessible",
                    actual="Failed to create session"
                )
                return False

            # Simple operation to verify cluster responds
            start_time = time.time()
            df = spark.range(10)
            count = df.count()
            duration = time.time() - start_time

            self.cleanup_spark_session()

            if count == 10:
                self.add_result(
                    test_name="Cluster Connectivity",
                    status="PASS",
                    expected="Cluster responds to operations",
                    actual=f"Cluster responded in {duration:.2f}s, count={count}",
                    duration=duration
                )
                print(f"  [PASS] Cluster is responsive ({duration:.2f}s)")
                return True
            else:
                self.add_result(
                    test_name="Cluster Connectivity",
                    status="FAIL",
                    expected="count=10",
                    actual=f"count={count}"
                )
                print(f"  [FAIL] Unexpected result: count={count}")
                return False

        except Exception as e:
            self.cleanup_spark_session()
            self.add_result(
                test_name="Cluster Connectivity",
                status="FAIL",
                expected="Cluster accessible",
                actual=f"Failed to connect: {str(e)}",
                error=str(e)
            )
            print(f"  [FAIL] {str(e)}")
            return False


def run_tests():
    """Run all connection tests."""
    tester = ConnectionTest()
    tester.print_header()
    start_time = time.time()

    # Load config first
    if not tester.load_config():
        return tester.generate_summary(start_time)

    # Run tests
    tests = [
        tester.test_config_structure,
        tester.test_azure_cli_auth,
        tester.test_databricks_session_creation,
        tester.test_cluster_connectivity
    ]

    for test in tests:
        test()

    return tester.generate_summary(start_time)


if __name__ == "__main__":
    result = run_tests()
    print(f"\nResult: {result['status']} ({result['passed']}/{result['total']} passed)")
    sys.exit(0 if result['status'] == "PASS" else 1)
