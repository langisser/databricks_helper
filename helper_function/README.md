# Databricks Helper - Live Execution & Output Capture

This helper provides real-time execution capabilities for Databricks without needing to export HTML logs. Perfect for Claude to analyze execution results directly.

## Features

- ✅ **Live SQL Query Execution** - Execute SQL queries and get real-time results
- ✅ **Notebook Execution with Output Capture** - Run notebooks and capture outputs
- ✅ **Real-time stdout/stderr Capture** - Capture all execution output
- ✅ **Job Run Log Retrieval** - Get logs from specific job runs
- ✅ **HTML Log Export** - Export job run logs in HTML format for documentation
- ✅ **Command Output Extraction** - Extract individual notebook cell outputs for investigation
- ✅ **No HTML Export Needed** - Direct programmatic access to results
- ✅ **Comprehensive Unit Tests** - 27 tests covering all functionality (100% pass rate)

## Setup

### 1. Authenticate with Azure CLI
```bash
# Login to Azure (required for Databricks authentication)
az login
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Databricks Connection
Set up your Databricks workspace configuration:

```bash
# Run the setup script
cd databricks_config
python setup_config.py

# This creates config.json from the template
# Edit config.json with your workspace details
```

**Important**: Your `config.json` is ignored by git for security. Each user/project needs their own configuration.

### 4. Get Your Configuration Details

**Host**: Your Databricks workspace URL (e.g., `https://adb-1234567890123456.12.azuredatabricks.net/`)

**Cluster ID**: Cluster ID (for notebook execution and live execution)
- Go to Compute → Clusters → Select cluster → Copy ID from URL
- Or use Databricks CLI: `databricks clusters list --output json`

**Warehouse ID**: SQL Warehouse ID (for SQL queries, optional)
- Go to SQL → Warehouses → Select warehouse → Copy ID from URL

**Authentication**: Uses Azure CLI automatically (no token needed in config)

## Usage

### Quick Start
```python
from databricks_helper import execute_sql_query

# Execute SQL query with live output capture
result = execute_sql_query("SELECT 'Hello World' as message")
print(result['result'])  # Query results
print(result['stdout'])  # Live stdout output
```

### Advanced Usage
```python
from databricks_helper import DatabricksExecutor

# Create executor
executor = DatabricksExecutor("config.json")

# Execute SQL with live output
result = executor.execute_sql("SELECT COUNT(*) FROM my_table")

# Execute notebook with parameters
notebook_result = executor.execute_notebook(
    "/Shared/my_notebook",
    parameters={"param1": "value1"}
)

# Get job run logs
logs = executor.get_job_run_logs(run_id=123456)

# Export HTML logs for documentation
from databricks_html_log_export import export_html_log, extract_command_outputs

html_file = export_html_log("123456")
print(f"HTML log saved to: {html_file}")

# Extract individual command outputs for investigation
commands = extract_command_outputs("123456", output_dir="outputs")
print(f"Extracted {len(commands)} commands")
```

### Run Examples
```bash
cd helper_function

# Run live execution demo
python demo/demo_live_execution.py

# Run HTML export demo
python demo/demo_html_export.py

# Command line HTML export
python databricks_html_log_export.py export <run_id>

# Extract command outputs
python databricks_html_log_export.py extract-commands <run_id> ./outputs

# Run unit tests
cd unittest
python main_unittest.py
```

## Why This Approach?

**Traditional Databricks CLI:**
- ❌ Only HTML export available
- ❌ No real-time output
- ❌ Limited to completed jobs
- ❌ Claude can't analyze HTML files directly

**This Helper:**
- ✅ Real-time JSON/text output
- ✅ Live execution monitoring
- ✅ Direct programmatic access
- ✅ Claude can analyze results immediately

## File Structure

```
databricks_helper/
├── databricks_config/          # Configuration folder (centralized)
│   ├── config.template.json   # Configuration template (tracked, Azure CLI)
│   ├── config.json            # Your personal config (git ignored, Azure CLI)
│   └── setup_config.py        # Configuration setup helper
├── helper_function/
│   ├── databricks_helper.py            # Main helper module
│   ├── databricks_html_log_export.py   # HTML log export & command extraction
│   ├── demo/
│   │   ├── demo_live_execution.py      # Live execution demo (uses Azure CLI auth)
│   │   └── demo_html_export.py         # HTML export demo
│   ├── unittest/                        # Comprehensive unit test suite
│   │   ├── base_test.py                # Shared base class for tests
│   │   ├── main_unittest.py            # Test runner (27 tests, 100% pass)
│   │   ├── test_connection.py          # Connection & auth tests
│   │   ├── test_sql_execution.py       # SQL execution tests
│   │   ├── test_dataframe_operations.py # DataFrame tests
│   │   ├── test_spark_transformations.py # Spark tests
│   │   ├── test_html_export.py         # HTML export tests
│   │   └── OPTIMIZATION_SUMMARY.md     # Optimization details
│   ├── requirements.txt                # Python dependencies
│   └── README.md                      # This file
└── tmp/                      # Output folder (git ignored)
```

## Output Format

All functions return structured data with:
- `result`: Actual query/notebook results
- `stdout`: Captured standard output
- `stderr`: Captured error output
- `timestamp`: Execution timestamp
- `metadata`: Additional execution info

Example:
```json
{
  "result": {
    "status": "success",
    "data": [["Hello World"]],
    "schema": [{"name": "message", "type": "string"}]
  },
  "stdout": "Executing query...\n✓ Query completed",
  "stderr": "",
  "query": "SELECT 'Hello World' as message",
  "timestamp": 1640995200.0
}
```

## For Claude Integration

This helper is designed to work perfectly with Claude:

1. **Execute code**: `executor.execute_sql(query)`
2. **Get results**: Results are returned as structured JSON
3. **Analyze output**: Claude can directly analyze stdout/stderr
4. **No downloads**: No need to handle HTML files

Perfect for real-time data analysis and debugging workflows!

## Using as a Submodule in Other Projects

To use this helper in another project:

```bash
# Add as submodule
git submodule add https://github.com/langisser/databricks_helper.git lib/databricks_helper

# Authenticate with Azure CLI first
az login

# Set up configuration in your project
cd lib/databricks_helper/databricks_config
python setup_config.py
# Edit config.json with your workspace details (no token needed)

# Use in your project
from lib.databricks_helper.helper_function.databricks_html_log_export import export_html_log
html_file = export_html_log("run_id_123")
```

**Configuration Security**: Each project maintains its own `config.json` in the centralized `databricks_config/` folder. Authentication uses Azure CLI for better security (no tokens stored in files). Config files are never committed to git.