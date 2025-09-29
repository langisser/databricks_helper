# Databricks Helper - Live Execution & Output Capture

This helper provides real-time execution capabilities for Databricks without needing to export HTML logs. Perfect for Claude to analyze execution results directly.

## Features

- ✅ **Live SQL Query Execution** - Execute SQL queries and get real-time results
- ✅ **Notebook Execution with Output Capture** - Run notebooks and capture outputs
- ✅ **Real-time stdout/stderr Capture** - Capture all execution output
- ✅ **Job Run Log Retrieval** - Get logs from specific job runs
- ✅ **HTML Log Export** - Export job run logs in HTML format for documentation
- ✅ **No HTML Export Needed** - Direct programmatic access to results

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Databricks Connection
Update `config.json` with your Databricks credentials:

```json
{
  "databricks": {
    "host": "https://your-workspace.databricks.com",
    "token": "your-access-token",
    "cluster_id": "your-cluster-id",
    "warehouse_id": "your-warehouse-id"
  },
  "settings": {
    "log_level": "INFO",
    "timeout_seconds": 300,
    "max_output_size": "5MB"
  }
}
```

### 3. Get Your Credentials

**Host**: Your Databricks workspace URL (e.g., `https://dbc-12345678-9abc.cloud.databricks.com`)

**Token**: Personal Access Token
- Go to Databricks workspace → User Settings → Access Tokens
- Generate New Token

**Warehouse ID**: SQL Warehouse ID (for SQL queries)
- Go to SQL → Warehouses → Select warehouse → Copy ID from URL

**Cluster ID**: Cluster ID (for notebook execution)
- Go to Compute → Clusters → Select cluster → Copy ID from URL

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
from databricks_html_log_export import export_html_log
html_file = export_html_log("123456")
print(f"HTML log saved to: {html_file}")
```

### Run Examples
```bash
cd helper_function

# Run live execution demo
python demo/demo_live_execution.py

# Run HTML export demo
python demo/demo_html_export.py

# Command line HTML export
python databricks_html_log_export.py <run_id>
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
helper_function/
├── config.json              # Configuration file
├── databricks_helper.py      # Main helper module
├── databricks_html_log_export.py # HTML log export functionality
├── demo/
│   ├── demo_live_execution.py  # Live execution demo
│   └── demo_html_export.py     # HTML export demo
├── requirements.txt          # Python dependencies
└── README.md                # This file
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