# Databricks Connect Helper - Clean Structure

## 📁 Project Structure

```
databrick_helper/
├── databricks.exe              # Databricks CLI executable
├── helper_function/
│   ├── config.json             # Databricks connection configuration
│   ├── databricks_clean.py     # Main helper for connection testing
│   ├── databricks_connect_direct.py # Advanced real-time execution
│   ├── test_clean.py           # Basic connection test
│   ├── test_databricks_connect_simple.py # Simple Connect test
│   ├── requirements.txt        # Python dependencies
│   ├── README.md              # Detailed usage guide
│   └── demo/
│       └── demo_live_execution.py # Live execution demo
└── README.md                   # Project overview
```

## 🚀 Key Files for Databricks Connect

### Core Files:
- **`config.json`** - Your Databricks credentials and cluster ID
- **`databricks_clean.py`** - Main helper with connection utilities
- **`databricks_connect_direct.py`** - Real-time Spark execution engine

### Test Files:
- **`test_clean.py`** - Verify basic connection
- **`test_databricks_connect_simple.py`** - Test simple operations

### Demo:
- **`demo/demo_live_execution.py`** - Full live execution demonstration

## 🎯 Usage

### Quick Test:
```bash
cd helper_function
python test_clean.py
```

### Live Execution Demo:
```bash
cd helper_function/demo
python demo_live_execution.py
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

## 🔧 Dependencies

Install with: `pip install -r requirements.txt`

Key packages:
- `databricks-connect==14.3.0` (matches Runtime 14.3 LTS)
- `databricks-sdk`
- Supporting packages for real-time execution