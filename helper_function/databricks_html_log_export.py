#!/usr/bin/env python3
"""
Databricks Job Run Log Exporter

Export Databricks job run logs in HTML format and extract individual command outputs
for investigation. Uses Databricks CLI which automatically supports Azure CLI authentication.

Features:
- Export full HTML logs using Databricks CLI
- Extract individual command outputs from notebook runs
- Parse and decode base64+URL-encoded notebook model
- Azure CLI authentication support (no token required)
- Comprehensive error handling
- Windows-compatible output (no Unicode emojis in terminal)

Usage:
    # Export HTML log
    python databricks_html_log_export.py export <run_id> [output_dir]

    # Extract command outputs
    python databricks_html_log_export.py extract-commands <run_id> [output_dir]

    # Python API
    from databricks_html_log_export import export_html_log, extract_command_outputs

    html_file = export_html_log("123456")
    commands = extract_command_outputs("123456", output_dir="outputs")
"""

import json
import subprocess
import re
import base64
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


class DatabricksLogExporter:
    """Export and parse Databricks job run logs using Databricks CLI."""

    def __init__(self):
        """Initialize the exporter and verify CLI availability."""
        self.check_cli_available()

    def check_cli_available(self) -> bool:
        """
        Check if Databricks CLI is available and configured.

        Returns:
            True if CLI is available

        Raises:
            Exception if CLI is not available or not configured
        """
        try:
            result = subprocess.run(
                ['databricks', '--version'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode != 0:
                raise Exception("Databricks CLI is not properly configured")
            return True
        except FileNotFoundError:
            raise Exception(
                "Databricks CLI is not installed. "
                "Install with: pip install databricks-cli"
            )
        except subprocess.TimeoutExpired:
            raise Exception("Databricks CLI check timed out")
        except Exception as e:
            raise Exception(f"Error checking Databricks CLI: {str(e)}")

    def export_run_json(self, run_id: str) -> Dict:
        """
        Export job run using Databricks CLI and return parsed JSON.

        Args:
            run_id: Databricks job run identifier

        Returns:
            Dictionary with export data including views

        Raises:
            Exception if export fails or times out
        """
        try:
            print(f"Exporting run {run_id} using Databricks CLI...")

            result = subprocess.run(
                ['databricks', 'jobs', 'export-run', run_id,
                 '--views-to-export', 'CODE',
                 '--output', 'json'],
                capture_output=True,
                text=True,
                timeout=60
            )

            if result.returncode != 0:
                error_msg = result.stderr.strip()
                raise Exception(f"CLI export failed: {error_msg}")

            data = json.loads(result.stdout)
            return data

        except subprocess.TimeoutExpired:
            raise Exception("Export timed out after 60 seconds")
        except json.JSONDecodeError as e:
            raise Exception(f"Failed to parse JSON response: {str(e)}")
        except Exception as e:
            raise Exception(f"Export failed: {str(e)}")

    def export_html(self, run_id: str, output_dir: str = "../tmp") -> Optional[str]:
        """
        Export HTML log for a Databricks job run.

        Args:
            run_id: Databricks job run identifier
            output_dir: Directory to save the HTML file

        Returns:
            Path to the exported HTML file, or None if export fails
        """
        try:
            # Create output directory
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)

            # Get export data via CLI
            data = self.export_run_json(run_id)

            # Extract HTML content
            if 'views' not in data or len(data['views']) == 0:
                raise Exception("No views found in export data")

            html_content = data['views'][0]['content']

            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_filename = f"run_{run_id}_{timestamp}.html"
            html_file_path = output_path / html_filename

            # Write HTML content (already decoded by json.loads)
            with open(html_file_path, 'w', encoding='utf-8') as f:
                f.write(html_content)

            file_size = html_file_path.stat().st_size / 1024  # KB
            print(f"SUCCESS: HTML exported to {html_file_path} ({file_size:.1f} KB)")

            return str(html_file_path)

        except Exception as e:
            print(f"ERROR: {str(e)}")
            return None

    def extract_notebook_model(self, html_content: str) -> Optional[Dict]:
        """
        Extract the notebook model from HTML content.

        The notebook model is embedded in the HTML as:
        var __DATABRICKS_NOTEBOOK_MODEL = '<base64_encoded_url_encoded_json>'

        Decoding steps:
        1. Extract base64-encoded string from HTML
        2. Base64 decode to get URL-encoded JSON
        3. URL decode to get plain JSON
        4. Parse JSON to get notebook model

        Args:
            html_content: HTML content from export

        Returns:
            Parsed notebook model dictionary, or None if extraction fails
        """
        try:
            # Find the notebook model script tag
            pattern = r"var __DATABRICKS_NOTEBOOK_MODEL = '([^']+)'"
            match = re.search(pattern, html_content)

            if not match:
                return None

            encoded_data = match.group(1)

            # Step 1: Base64 decode (add padding if needed)
            padding = len(encoded_data) % 4
            if padding:
                encoded_data += '=' * (4 - padding)

            base64_decoded = base64.b64decode(encoded_data).decode('utf-8')

            # Step 2: URL decode (percent-decode)
            url_decoded = urllib.parse.unquote(base64_decoded)

            # Step 3: Parse JSON
            notebook_model = json.loads(url_decoded)

            return notebook_model

        except Exception as e:
            print(f"Warning: Could not extract notebook model: {str(e)}")
            return None

    def extract_command_outputs(
        self,
        run_id: str,
        output_dir: Optional[str] = None
    ) -> List[Dict]:
        """
        Extract individual command outputs from a notebook run.

        Args:
            run_id: Databricks job run identifier
            output_dir: Optional directory to save individual command outputs as JSON

        Returns:
            List of command information dictionaries with:
                - index: Command number
                - guid: Unique command identifier
                - command: Full command code
                - state: Execution state (finished, error, etc.)
                - has_results: Whether command has results
                - result_type: Type of result (table, text, listResults, etc.)
                - result_data: Actual result data
        """
        try:
            print(f"Extracting command outputs for run {run_id}...")

            # Get export data
            data = self.export_run_json(run_id)
            html_content = data['views'][0]['content']

            # Extract notebook model
            notebook_model = self.extract_notebook_model(html_content)

            if not notebook_model:
                print("Warning: Could not parse notebook model from HTML")
                return []

            # Extract commands
            commands = notebook_model.get('commands', [])

            if not commands:
                print("No commands found in notebook")
                return []

            print(f"Found {len(commands)} commands")

            # Parse each command with error handling
            command_info = []
            for idx, cmd in enumerate(commands, 1):
                try:
                    info = {
                        'index': idx,
                        'guid': cmd.get('guid', 'unknown'),
                        'command': cmd.get('command', ''),
                        'state': cmd.get('state', 'unknown'),
                        'position': cmd.get('position', 0),
                        'command_type': cmd.get('commandType', 'auto'),
                    }

                    # Extract results if available
                    if 'results' in cmd and cmd['results'] is not None:
                        results = cmd['results']
                        info['has_results'] = True
                        info['result_type'] = results.get('type', 'unknown')

                        # Extract result data based on type
                        result_type = results.get('type')
                        if result_type in ('table', 'text', 'listResults'):
                            info['result_data'] = results.get('data', [])
                        else:
                            # Store full results for unknown types
                            info['result_data'] = results
                    else:
                        info['has_results'] = False
                        info['result_data'] = None

                    command_info.append(info)

                    # Print summary (ASCII-safe for Windows)
                    status = "PASS" if cmd.get('state') == 'finished' else "FAIL"
                    cmd_preview = info['command'][:60].replace('\n', ' ')

                    # Use safe encoding for terminal output
                    try:
                        print(f"  [{status}] Command {idx}: {cmd_preview}...")
                    except UnicodeEncodeError:
                        # Fallback if command contains non-ASCII characters
                        safe_preview = cmd_preview.encode('ascii', 'ignore').decode('ascii')
                        print(f"  [{status}] Command {idx}: {safe_preview}...")

                except Exception as e:
                    print(f"  [ERROR] Command {idx}: Failed to parse - {str(e)}")
                    # Continue processing other commands

            # Save to files if output_dir specified
            if output_dir:
                self._save_command_outputs(run_id, command_info, output_dir)

            return command_info

        except Exception as e:
            print(f"ERROR: Failed to extract commands: {str(e)}")
            return []

    def _save_command_outputs(
        self,
        run_id: str,
        command_info: List[Dict],
        output_dir: str
    ) -> None:
        """
        Save individual command outputs to JSON files.

        Creates a directory structure:
        {output_dir}/
            run_{run_id}_commands/
                _summary.json          # Overview of all commands
                cmd_001_{guid}.json    # Individual command details
                cmd_002_{guid}.json
                ...

        Args:
            run_id: Job run identifier
            command_info: List of command dictionaries
            output_dir: Base output directory
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Create subdirectory for this run
        run_dir = output_path / f"run_{run_id}_commands"
        run_dir.mkdir(exist_ok=True)

        # Save each command as individual JSON file
        for cmd in command_info:
            guid_short = cmd['guid'][:8] if cmd['guid'] != 'unknown' else 'unknown'
            filename = f"cmd_{cmd['index']:03d}_{guid_short}.json"
            filepath = run_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(cmd, f, indent=2, ensure_ascii=False)

        # Save summary file
        summary_file = run_dir / "_summary.json"
        summary = {
            'run_id': run_id,
            'total_commands': len(command_info),
            'timestamp': datetime.now().isoformat(),
            'commands': [
                {
                    'index': cmd['index'],
                    'state': cmd['state'],
                    'has_results': cmd['has_results'],
                    'result_type': cmd.get('result_type', 'none')
                }
                for cmd in command_info
            ]
        }

        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)

        print(f"Command outputs saved to: {run_dir}")


# ============================================================================
# Convenience Functions
# ============================================================================

def export_html_log(run_id: str, output_dir: str = "../tmp") -> Optional[str]:
    """
    Export HTML log for a Databricks job run.

    Args:
        run_id: Databricks job run identifier
        output_dir: Directory to save the HTML file (default: ../tmp)

    Returns:
        Path to the exported HTML file, or None if export fails

    Example:
        >>> html_file = export_html_log("123456")
        >>> print(f"Log saved to: {html_file}")
    """
    exporter = DatabricksLogExporter()
    return exporter.export_html(run_id, output_dir)


def extract_command_outputs(
    run_id: str,
    output_dir: Optional[str] = None
) -> List[Dict]:
    """
    Extract individual command outputs from a notebook run.

    Args:
        run_id: Databricks job run identifier
        output_dir: Optional directory to save command outputs as JSON files

    Returns:
        List of command information dictionaries

    Example:
        >>> commands = extract_command_outputs("123456", output_dir="outputs")
        >>> for cmd in commands:
        ...     print(f"Command {cmd['index']}: {cmd['state']}")
    """
    exporter = DatabricksLogExporter()
    return exporter.extract_command_outputs(run_id, output_dir)


# ============================================================================
# Command Line Interface
# ============================================================================

def main():
    """Main CLI entry point."""
    import sys

    if len(sys.argv) < 2:
        print("Databricks Job Run Log Exporter")
        print()
        print("Usage:")
        print("  python databricks_html_log_export.py export <run_id> [output_dir]")
        print("  python databricks_html_log_export.py extract-commands <run_id> [output_dir]")
        print()
        print("Examples:")
        print("  python databricks_html_log_export.py export 123456")
        print("  python databricks_html_log_export.py export 123456 ./outputs")
        print("  python databricks_html_log_export.py extract-commands 123456 ./outputs")
        print()
        print("Note: Requires Databricks CLI and Azure CLI authentication")
        sys.exit(1)

    command = sys.argv[1]

    if command == "export":
        if len(sys.argv) < 3:
            print("ERROR: run_id required")
            print("Usage: python databricks_html_log_export.py export <run_id> [output_dir]")
            sys.exit(1)

        run_id = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else "../tmp"

        result = export_html_log(run_id, output_dir)
        sys.exit(0 if result else 1)

    elif command == "extract-commands":
        if len(sys.argv) < 3:
            print("ERROR: run_id required")
            print("Usage: python databricks_html_log_export.py extract-commands <run_id> [output_dir]")
            sys.exit(1)

        run_id = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else None

        commands = extract_command_outputs(run_id, output_dir)

        print()
        print(f"Extracted {len(commands)} commands")

        # Show success/failure summary
        if commands:
            finished = sum(1 for c in commands if c['state'] == 'finished')
            print(f"  {finished}/{len(commands)} commands finished successfully")

        sys.exit(0)

    else:
        print(f"ERROR: Unknown command '{command}'")
        print("Valid commands: export, extract-commands")
        sys.exit(1)


if __name__ == "__main__":
    main()
