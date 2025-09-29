#!/usr/bin/env python3
"""
Simple Databricks HTML Log Exporter

A simplified version for exporting Databricks job run logs in HTML format.
Uses the existing config.json configuration for authentication.

Usage:
    from html_log_export import export_html_log

    # Export HTML log for a specific run ID
    html_file = export_html_log("168138541977597")

    # Or use the class directly
    from html_log_export import HTMLLogExporter
    exporter = HTMLLogExporter()
    html_file = exporter.export_run_html("168138541977597")

Features:
- Simple function interface
- Uses existing config.json for authentication
- Automatic timestamped file naming
- Error handling with descriptive messages
"""

import json
import requests
import os
from datetime import datetime
from pathlib import Path


class HTMLLogExporter:
    """Simple HTML log exporter for Databricks job runs."""

    def __init__(self, config_path=None):
        """
        Initialize the HTML log exporter.

        Args:
            config_path (str, optional): Path to config.json file.
                                       Defaults to helper_function/config.json
        """
        if config_path is None:
            config_path = Path(__file__).parent / "config.json"

        self.config = self._load_config(config_path)
        self.host = self.config['databricks']['host'].rstrip('/')
        self.token = self.config['databricks']['token']

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def _load_config(self, config_path):
        """Load configuration from JSON file."""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Try to find template file and give helpful error
            template_path = config_path.parent / "config.template.json"
            if template_path.exists():
                raise Exception(f"""
Configuration file not found: {config_path}

To set up your configuration:
1. Copy the template: cp {template_path} {config_path}
2. Edit {config_path} with your Databricks credentials
3. Template available at: {template_path}

Your config.json file is ignored by git for security.
                """.strip())
            else:
                raise Exception(f"Configuration file not found: {config_path}")
        except json.JSONDecodeError as e:
            raise Exception(f"Invalid JSON in configuration file {config_path}: {e}")

    def export_run_html(self, run_id, output_dir="../tmp"):
        """
        Export HTML log for a specific Databricks job run.

        Args:
            run_id (str): Databricks job run identifier
            output_dir (str): Directory to save the HTML file (default: "../tmp")

        Returns:
            str: Path to the exported HTML file, or None if export fails
        """
        try:
            # Create output directory if it doesn't exist
            Path(output_dir).mkdir(exist_ok=True)

            # Export HTML using Databricks API
            export_url = f"{self.host}/api/2.1/jobs/runs/export"
            export_params = {
                "run_id": run_id,
                "views_to_export": "CODE",
                "format": "HTML"
            }

            print(f"Exporting HTML log for run ID: {run_id}")
            response = requests.get(export_url, headers=self.headers, params=export_params)
            response.raise_for_status()

            # Extract HTML content from response
            response_data = response.json()
            html_content = response_data['views'][0]['content']

            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_filename = f"run_{run_id}_{timestamp}.html"
            html_file_path = Path(output_dir) / html_filename

            # Write HTML content to file
            with open(html_file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"SUCCESS: HTML log exported successfully: {html_file_path}")
            return str(html_file_path)

        except requests.RequestException as e:
            print(f"ERROR: Error exporting HTML log: {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    print(f"   API Error: {error_data.get('message', 'Unknown error')}")
                except:
                    print(f"   HTTP Status: {e.response.status_code}")
            return None
        except Exception as e:
            print(f"ERROR: Unexpected error: {e}")
            return None


def export_html_log(run_id, output_dir="../tmp", config_path=None):
    """
    Simple function to export HTML log for a Databricks job run.

    Args:
        run_id (str): Databricks job run identifier
        output_dir (str): Directory to save the HTML file (default: "../tmp")
        config_path (str, optional): Path to config.json file

    Returns:
        str: Path to the exported HTML file, or None if export fails

    Example:
        >>> html_file = export_html_log("168138541977597")
        >>> print(f"Log saved to: {html_file}")
    """
    exporter = HTMLLogExporter(config_path)
    return exporter.export_run_html(run_id, output_dir)


# Command line interface
if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python html_log_export.py <RUN_ID>")
        print("Example: python html_log_export.py 168138541977597")
        sys.exit(1)

    run_id = sys.argv[1]
    result = export_html_log(run_id)

    if result:
        print(f"Success! HTML log exported to: {result}")
        sys.exit(0)
    else:
        print("Failed to export HTML log.")
        sys.exit(1)