#!/usr/bin/env python3
"""
HTML Log Export Tests for Databricks Helper
Tests the HTML log export functionality for job runs using Databricks CLI
"""

import sys
import json
import time
import os
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from databricks_html_log_export import DatabricksLogExporter, export_html_log, extract_command_outputs


class HTMLExportTest:
    def __init__(self):
        self.config_path = Path(__file__).parent.parent.parent / "databricks_config" / "config.json"
        self.config = None
        self.exporter = None
        self.results = []
        self.test_run_id = "932160071027767"  # Recent successful run from wherecond job

    def load_config(self):
        """Load configuration file"""
        try:
            with open(self.config_path, 'r') as f:
                self.config = json.load(f)
            return True
        except Exception as e:
            self.results.append({
                "test": "Load Configuration",
                "status": "FAIL",
                "error": str(e),
                "expected": "Config file loaded successfully",
                "actual": f"Failed to load config: {str(e)}"
            })
            return False

    def test_exporter_initialization(self):
        """Test 1: Initialize HTML exporter with CLI"""
        print("\nTest 1: HTML Exporter Initialization (CLI-based)")

        try:
            self.exporter = DatabricksLogExporter()

            self.results.append({
                "test": "HTML Exporter Initialization",
                "status": "PASS",
                "expected": "Exporter initialized with Databricks CLI",
                "actual": "Exporter initialized successfully"
            })
            print("  [PASS] HTML exporter initialized with Databricks CLI")
            return True

        except Exception as e:
            self.results.append({
                "test": "HTML Exporter Initialization",
                "status": "FAIL",
                "error": str(e),
                "expected": "Exporter initialized",
                "actual": f"Failed: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_cli_availability(self):
        """Test 2: Verify Databricks CLI is available"""
        print("\nTest 2: Databricks CLI Availability")

        try:
            if self.exporter is None:
                self.exporter = DatabricksLogExporter()

            # CLI check is done in __init__, so if we got here, it's available
            self.results.append({
                "test": "CLI Availability",
                "status": "PASS",
                "expected": "Databricks CLI available",
                "actual": "CLI is installed and accessible"
            })
            print("  [PASS] Databricks CLI is available")
            return True

        except Exception as e:
            self.results.append({
                "test": "CLI Availability",
                "status": "FAIL",
                "error": str(e),
                "expected": "Databricks CLI available",
                "actual": f"CLI check failed: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_export_html_log(self):
        """Test 3: Export HTML log for a real job run"""
        print("\nTest 3: Export HTML Log (CLI-based)")

        try:
            start_time = time.time()

            # Create temp output directory for test
            output_dir = Path(__file__).parent / "test_output"
            output_dir.mkdir(exist_ok=True)

            # Export HTML log using CLI
            html_file = self.exporter.export_html(
                run_id=self.test_run_id,
                output_dir=str(output_dir)
            )

            duration = time.time() - start_time

            if html_file is None:
                raise Exception("Export returned None - check if run ID is valid")

            # Verify file exists
            if not Path(html_file).exists():
                raise Exception(f"HTML file not created: {html_file}")

            # Verify file has content
            file_size = Path(html_file).stat().st_size
            if file_size == 0:
                raise Exception("HTML file is empty")

            self.results.append({
                "test": "Export HTML Log",
                "status": "PASS",
                "expected": "HTML log exported successfully",
                "actual": f"HTML exported in {duration:.2f}s, size: {file_size} bytes",
                "duration": duration,
                "file_path": html_file
            })
            print(f"  [PASS] HTML log exported ({duration:.2f}s, {file_size} bytes)")

            # Cleanup test file
            try:
                Path(html_file).unlink()
            except:
                pass

            return True

        except Exception as e:
            self.results.append({
                "test": "Export HTML Log",
                "status": "FAIL",
                "error": str(e),
                "expected": "HTML log exported",
                "actual": f"Failed: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_export_function_interface(self):
        """Test 4: Test simple function interface"""
        print("\nTest 4: Function Interface")

        try:
            start_time = time.time()

            # Create temp output directory
            output_dir = Path(__file__).parent / "test_output"
            output_dir.mkdir(exist_ok=True)

            # Use simple function interface
            html_file = export_html_log(
                run_id=self.test_run_id,
                output_dir=str(output_dir)
            )

            duration = time.time() - start_time

            if html_file is None:
                raise Exception("Function returned None")

            if not Path(html_file).exists():
                raise Exception("HTML file not created")

            self.results.append({
                "test": "Function Interface",
                "status": "PASS",
                "expected": "Function interface works",
                "actual": f"Export succeeded in {duration:.2f}s",
                "duration": duration
            })
            print(f"  [PASS] Function interface works ({duration:.2f}s)")

            # Cleanup
            try:
                Path(html_file).unlink()
            except:
                pass

            return True

        except Exception as e:
            self.results.append({
                "test": "Function Interface",
                "status": "FAIL",
                "error": str(e),
                "expected": "Function interface works",
                "actual": f"Failed: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_extract_commands(self):
        """Test 5: Extract individual commands from notebook run"""
        print("\nTest 5: Extract Command Outputs")

        try:
            start_time = time.time()

            # Create temp output directory
            output_dir = Path(__file__).parent / "test_output"
            output_dir.mkdir(exist_ok=True)

            # Extract commands
            commands = extract_command_outputs(
                run_id=self.test_run_id,
                output_dir=str(output_dir)
            )

            duration = time.time() - start_time

            if not commands:
                raise Exception("No commands extracted")

            # Verify commands structure
            for cmd in commands:
                required_fields = ['index', 'guid', 'command', 'state']
                for field in required_fields:
                    if field not in cmd:
                        raise Exception(f"Missing required field: {field}")

            # Check if output directory was created
            run_dir = output_dir / f"run_{self.test_run_id}_commands"
            if not run_dir.exists():
                raise Exception("Command output directory not created")

            # Check if summary file exists
            summary_file = run_dir / "_summary.json"
            if not summary_file.exists():
                raise Exception("Summary file not created")

            self.results.append({
                "test": "Extract Command Outputs",
                "status": "PASS",
                "expected": "Commands extracted successfully",
                "actual": f"Extracted {len(commands)} commands in {duration:.2f}s",
                "duration": duration,
                "command_count": len(commands)
            })
            print(f"  [PASS] Extracted {len(commands)} commands ({duration:.2f}s)")

            # Cleanup
            try:
                import shutil
                shutil.rmtree(run_dir)
                output_dir.rmdir()
            except:
                pass

            return True

        except Exception as e:
            self.results.append({
                "test": "Extract Command Outputs",
                "status": "FAIL",
                "error": str(e),
                "expected": "Commands extracted",
                "actual": f"Failed: {str(e)}"
            })
            print(f"  [FAIL] {str(e)}")
            return False

    def test_invalid_run_id(self):
        """Test 6: Handle invalid run ID gracefully"""
        print("\nTest 6: Invalid Run ID Handling")

        try:
            output_dir = Path(__file__).parent / "test_output"
            output_dir.mkdir(exist_ok=True)

            # Try to export with invalid run ID
            invalid_run_id = "999999999999999"
            html_file = self.exporter.export_html(
                run_id=invalid_run_id,
                output_dir=str(output_dir)
            )

            # Should return None for invalid run ID
            if html_file is None:
                self.results.append({
                    "test": "Invalid Run ID Handling",
                    "status": "PASS",
                    "expected": "Returns None for invalid run ID",
                    "actual": "Correctly returned None"
                })
                print("  [PASS] Invalid run ID handled gracefully")
                return True
            else:
                self.results.append({
                    "test": "Invalid Run ID Handling",
                    "status": "FAIL",
                    "expected": "Returns None for invalid run ID",
                    "actual": f"Returned: {html_file}"
                })
                print("  [FAIL] Should have returned None for invalid run ID")
                return False

        except Exception as e:
            # Exception is acceptable for invalid run ID
            self.results.append({
                "test": "Invalid Run ID Handling",
                "status": "PASS",
                "expected": "Handles invalid run ID",
                "actual": f"Handled gracefully: {str(e)[:50]}"
            })
            print("  [PASS] Exception handled for invalid run ID")
            return True


def run_tests():
    """Run all HTML export tests"""
    print("\n" + "="*60)
    print("HTML Log Export Tests (CLI-based)")
    print("="*60)

    tester = HTMLExportTest()
    start_time = time.time()

    # Load config first
    if not tester.load_config():
        return {
            "status": "FAIL",
            "passed": 0,
            "failed": 1,
            "total": 1,
            "duration": time.time() - start_time,
            "details": tester.results
        }

    # Run tests
    tests = [
        tester.test_exporter_initialization,
        tester.test_cli_availability,
        tester.test_export_html_log,
        tester.test_export_function_interface,
        tester.test_extract_commands,
        tester.test_invalid_run_id
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"  [ERROR] Test raised unexpected exception: {str(e)}")
            failed += 1

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
