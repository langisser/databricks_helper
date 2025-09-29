#!/usr/bin/env python3
"""
Configuration Setup Helper for Databricks Helper

This script helps users set up their personal Databricks configuration
by copying the template and guiding them through the setup process.

Usage:
    python setup_config.py
"""

import json
import shutil
from pathlib import Path


def setup_configuration():
    """Set up configuration for databricks_helper."""

    # Get paths
    script_dir = Path(__file__).parent
    template_path = script_dir / "config.template.json"
    config_path = script_dir / "config.json"

    print("=" * 60)
    print("Databricks Helper Configuration Setup")
    print("=" * 60)

    # Check if config already exists
    if config_path.exists():
        response = input(f"Configuration file already exists at {config_path}\nOverwrite? (y/N): ").strip().lower()
        if response != 'y':
            print("Setup cancelled.")
            return

    # Check if template exists
    if not template_path.exists():
        print(f"ERROR: Template file not found: {template_path}")
        return

    # Copy template to config
    try:
        shutil.copy2(template_path, config_path)
        print(f"✓ Created configuration file: {config_path}")
    except Exception as e:
        print(f"ERROR: Failed to copy template: {e}")
        return

    print("\n" + "=" * 60)
    print("Configuration Setup Instructions")
    print("=" * 60)
    print()
    print("1. Open the config file:")
    print(f"   {config_path}")
    print()
    print("2. Replace the following placeholders with your actual values:")
    print()
    print("   host: Your Databricks workspace URL")
    print("   Example: https://adb-1234567890123456.12.azuredatabricks.net")
    print()
    print("   token: Your personal access token")
    print("   - Go to Databricks workspace → User Settings → Access tokens")
    print("   - Generate New Token")
    print()
    print("   cluster_id: Your cluster ID (for live execution)")
    print("   - Go to Compute → Clusters → Select cluster → Copy ID from URL")
    print()
    print("   warehouse_id: Your SQL warehouse ID (for SQL queries)")
    print("   - Go to SQL → Warehouses → Select warehouse → Copy ID from URL")
    print()
    print("3. Save the file")
    print()
    print("=" * 60)
    print("Security Note")
    print("=" * 60)
    print()
    print("Your config.json file is ignored by git and will NOT be committed.")
    print("This keeps your credentials secure and private.")
    print()
    print("When using this helper in other projects via submodules,")
    print("each project will need to run this setup script to configure")
    print("their own credentials.")
    print()
    print("Setup completed! Edit your config.json file with your credentials.")


if __name__ == "__main__":
    setup_configuration()