# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Databricks Connect helper system that enables real-time Spark execution and analysis directly from Python, eliminating the need for HTML log exports. The system uses Databricks Connect to establish live connections to Databricks clusters for immediate code execution and result capture.

## Key Commands

### Setup and Installation
```bash
# Authenticate with Azure CLI (required for Databricks connection)
az login

# Install Python dependencies (requires databricks-connect==14.3.0)
cd helper_function
pip install -r requirements.txt

# Install additional required package
pip install databricks-connect==14.3.0
```

### Running Live Demonstrations
```bash
# Execute the main demo showing all capabilities
cd helper_function/demo
python demo_live_execution.py
```

### Cluster Discovery (using Databricks CLI)
```bash
# Ensure you're logged in with Azure CLI
az login

# Find cluster ID from cluster name
export DATABRICKS_HOST=https://your-workspace.databricks.net/
databricks clusters list --output json
```

## Architecture

### Core Components

**Configuration System (`databricks_config/config.json`)**
- Contains Databricks workspace host and cluster identifiers
- Requires: host, cluster_id (warehouse_id optional)
- Uses Azure CLI for authentication (no token needed in config)
- Used by all execution components for cluster connection
- Single centralized config file for all functionality

**Live Execution Engine (`helper_function/demo/demo_live_execution.py`)**
- Primary interface for real-time Databricks Spark execution
- Uses `DatabricksSession.builder.remote()` for direct cluster connection
- Demonstrates DataFrame operations, SQL queries, transformations, and aggregations
- Returns immediate results without job submission overhead

**Databricks CLI Tools (`databricks_cli/`)**
- Portable CLI executable for cluster discovery and management
- Used to translate cluster names to cluster IDs
- Separate from main helper function to keep CLI tools isolated

### Execution Flow

1. **Session Creation**: `DatabricksSession.builder.remote()` establishes direct connection to specified cluster
2. **Live Operations**: Code executes immediately on remote cluster like local Python
3. **Result Capture**: stdout/stderr and structured results are captured in real-time
4. **Analysis Ready**: Results are immediately available in JSON format for analysis

### Key Differences from Traditional Approaches

- **Direct Execution**: No job submission/waiting cycle - code runs immediately
- **Real-time Results**: Output appears instantly, not after job completion
- **Structured Output**: JSON results instead of HTML exports
- **Live Session**: Maintains connection for interactive operations

## Important Implementation Details

### Authentication
- Uses Azure CLI for authentication (token-based auth has been replaced)
- Must run `az login` before executing any Databricks operations
- Databricks Connect automatically uses Azure CLI credentials when no token is provided
- Ensures better security by leveraging Azure's authentication system

### Databricks Connect Version Compatibility
- Must use `databricks-connect==14.3.0` to match Databricks Runtime 14.3 LTS
- Version mismatch will cause connection failures with explicit error messages

### Cluster Requirements
- Cluster must be running or able to auto-start
- Uses existing cluster ID (found via `databricks clusters list`)
- Session creation will wait for cluster startup if needed

### Configuration Management
- Configuration loaded from `databricks_config/config.json` (centralized location)
- Contains workspace host and cluster identifiers (no sensitive tokens)
- Authentication handled automatically via Azure CLI
- Cluster ID must match exact cluster identifier, not cluster name
- Single config file used by all components (live execution and HTML export)

### Output Format
All execution functions return structured dictionaries with:
- `result`: Actual execution results (DataFrames, query outputs)
- `stdout`: Captured print statements and execution logs
- `stderr`: Error messages and warnings
- `timestamp`: Execution timestamp
- Additional metadata specific to operation type

## File Organization

```
databricks_helper/
├── databricks_config/       # Configuration files
│   ├── config.json         # Databricks credentials (centralized)
│   └── config.template.json # Template for new setups
├── databricks_cli/          # CLI tools (isolated)
├── helper_function/         # Main functionality
│   └── demo/
│       └── demo_live_execution.py  # Primary execution interface
└── tmp/                    # Archived files (ignore for analysis)
```

The `tmp/` folder contains previous iterations and test files that should not be analyzed or modified for normal operations.

## Known Issues and Prevention

### File Locking Issues on Windows
- **Issue**: Write tool may occasionally fail with "Error writing file" due to Windows file locking
- **Root Cause**: Temporary file system locks from antivirus, Windows Search indexer, or concurrent processes
- **Prevention**:
  - If Write tool fails, check if operation actually succeeded before retrying
  - Use Read tool first to verify current file state
  - Wait a moment and retry if necessary
  - Consider using Edit tool instead of Write for existing files
- Just use normal status instead of emoji