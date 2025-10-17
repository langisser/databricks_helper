# Databricks Connect Helper - Clean Structure

## 📁 Project Structure

```
databricks_helper/
├── databricks_config/          # Configuration folder (centralized)
│   ├── config.json            # Databricks connection config (Azure CLI auth)
│   ├── config.template.json   # Configuration template
│   └── setup_config.py        # Configuration setup helper
├── databricks_cli/             # Databricks CLI tools
├── helper_function/
│   ├── databricks_clean.py     # Main helper for connection testing
│   ├── databricks_connect_direct.py # Advanced real-time execution
│   ├── databricks_html_log_export.py # HTML log export functionality
│   ├── test_clean.py           # Basic connection test
│   ├── test_databricks_connect_simple.py # Simple Connect test
│   ├── requirements.txt        # Python dependencies
│   ├── README.md              # Detailed usage guide
│   ├── unittest/               # Unit test suite
│   │   ├── main_unittest.py   # Main test runner
│   │   ├── test_connection.py # Connection tests
│   │   ├── test_sql_execution.py # SQL execution tests
│   │   ├── test_dataframe_operations.py # DataFrame tests
│   │   ├── test_spark_transformations.py # Spark tests
│   │   └── test_html_export.py # HTML export tests
│   └── demo/
│       ├── demo_live_execution.py # Live execution demo (Azure CLI auth)
│       └── demo_html_export.py    # HTML export demo
└── README.md                   # Project overview
```

## 🚀 Key Files for Databricks Connect

### Core Files:
- **`databricks_config/config.json`** - Your Databricks workspace config (uses Azure CLI auth, no tokens)
- **`databricks_clean.py`** - Main helper with connection utilities
- **`databricks_connect_direct.py`** - Real-time Spark execution engine
- **`databricks_html_log_export.py`** - HTML log export for job runs

### Test Files:
- **`test_clean.py`** - Verify basic connection
- **`test_databricks_connect_simple.py`** - Test simple operations
- **`unittest/main_unittest.py`** - Comprehensive test suite runner

### Unit Test Suite:
- **`test_connection.py`** - Connection and authentication tests
- **`test_sql_execution.py`** - SQL query execution tests
- **`test_dataframe_operations.py`** - DataFrame operations tests
- **`test_spark_transformations.py`** - Spark transformations tests
- **`test_html_export.py`** - HTML log export tests

### Demo:
- **`demo/demo_live_execution.py`** - Full live execution demonstration
- **`demo/demo_html_export.py`** - HTML export demonstration

## 🎯 Usage

### Authenticate First:
```bash
# Login with Azure CLI (required)
az login
```

### Quick Test:
```bash
cd helper_function
python test_clean.py
```

### Run Unit Tests:
```bash
cd helper_function/unittest
python main_unittest.py
```

### Live Execution Demo:
```bash
cd helper_function/demo
python demo_live_execution.py
```

### Export HTML Logs:
```bash
cd helper_function
python databricks_html_log_export.py <run_id>
```

### Real-time Spark Session:
```python
from databricks_connect_direct import create_direct_session
spark = create_direct_session()
# Use spark for immediate execution
```

## ✅ Ready for Claude Integration

All files provide structured JSON output perfect for Claude analysis:
- Real-time execution results
- Immediate output capture
- No HTML export needed
- Direct programmatic access to Spark cluster
- Secure Azure CLI authentication (no tokens in config files)

## 🔧 Dependencies

Install with: `pip install -r requirements.txt`

Key packages:
- `databricks-connect==14.3.0` (matches Runtime 14.3 LTS)
- `databricks-sdk`
- Supporting packages for real-time execution