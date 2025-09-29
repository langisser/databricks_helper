#!/usr/bin/env python3
"""
Demo: HTML Log Export

This demo shows how to use the simplified HTML log export functionality
to export Databricks job run logs in HTML format.

Requirements:
- Valid Databricks credentials in ../../databricks_config/config.json
- A valid job run ID from your Databricks workspace

Usage:
    python demo_html_export.py
"""

import sys
import os
from pathlib import Path

# Add parent directory to path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from databricks_html_log_export import export_html_log, HTMLLogExporter


def demo_simple_export():
    """Demo the simple function interface."""
    print("=" * 60)
    print("Demo 1: Simple HTML Log Export")
    print("=" * 60)

    # Example run ID - replace with your actual run ID
    run_id = input("Enter a Databricks job run ID: ").strip()

    if not run_id:
        print("No run ID provided. Skipping simple export demo.")
        return

    print(f"Exporting HTML log for run ID: {run_id}")

    # Use the simple function interface
    html_file = export_html_log(run_id)

    if html_file:
        print(f"✅ Export successful!")
        print(f"   File saved to: {html_file}")

        # Show file size
        file_size = os.path.getsize(html_file) / 1024  # KB
        print(f"   File size: {file_size:.1f} KB")
    else:
        print("❌ Export failed!")


def demo_class_interface():
    """Demo the class-based interface with custom options."""
    print("\n" + "=" * 60)
    print("Demo 2: Class-based Interface with Custom Options")
    print("=" * 60)

    # Example run ID - replace with your actual run ID
    run_id = input("Enter another Databricks job run ID (or press Enter to skip): ").strip()

    if not run_id:
        print("No run ID provided. Skipping class interface demo.")
        return

    try:
        # Create exporter instance
        exporter = HTMLLogExporter()

        # Export to custom directory
        custom_dir = "exported_logs"
        print(f"Exporting to custom directory: {custom_dir}")

        html_file = exporter.export_run_html(run_id, output_dir=custom_dir)

        if html_file:
            print(f"✅ Export successful!")
            print(f"   File saved to: {html_file}")

            # Show file info
            file_path = Path(html_file)
            file_size = file_path.stat().st_size / 1024  # KB
            print(f"   File size: {file_size:.1f} KB")
            print(f"   Directory: {file_path.parent}")
        else:
            print("❌ Export failed!")

    except Exception as e:
        print(f"❌ Error during export: {e}")


def demo_configuration_info():
    """Show configuration information."""
    print("\n" + "=" * 60)
    print("Demo 3: Configuration Information")
    print("=" * 60)

    try:
        exporter = HTMLLogExporter()

        print("Current Configuration:")
        print(f"  Databricks Host: {exporter.host}")
        print(f"  Token (first 10 chars): {exporter.token[:10]}...")
        print(f"  Config file location: {Path(__file__).parent.parent.parent / 'databricks_config' / 'config.json'}")

    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        print("Make sure config.json exists in the databricks_config directory with valid credentials.")


def main():
    """Run all demos."""
    print("Databricks HTML Log Export Demo")
    print("This demo shows how to export job run logs in HTML format.")
    print()

    # Show configuration info first
    demo_configuration_info()

    # Run simple export demo
    demo_simple_export()

    # Run class interface demo
    demo_class_interface()

    print("\n" + "=" * 60)
    print("Demo completed!")
    print("=" * 60)
    print()
    print("Usage Summary:")
    print("1. Simple function: html_file = export_html_log('run_id')")
    print("2. Class interface: exporter = HTMLLogExporter(); html_file = exporter.export_run_html('run_id')")
    print("3. Command line: python html_log_export.py <run_id>")


if __name__ == "__main__":
    main()