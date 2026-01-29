# Data Integration MCP Server - Challenge 2 Solution

A Model Context Protocol (MCP) server for multi-source data integration with AI-powered natural language queries.

## Features

✅ **6 MCP Tools:**
- `query_data` - Query data from SQL, API, or files using natural language
- `list_sources` - List available data sources
- `execute_sql` - Direct SQL query execution
- `transform_data` - Filter, sort, aggregate, and limit data
- `export_data` - Export to JSON or CSV format
- `integrate_data` - Combine data from multiple sources with join operations

✅ **3+ Data Source Connectors:**
- **SQL Database** (SQLite) - Users & Orders tables
- **REST APIs** - Mock API with sample data
- **File Systems** - CSV, JSON file parsing

✅ **AI-Powered Features:**
- Natural Language to SQL conversion using Ollama llama3.2:3b
- Fallback logic when AI is unavailable
- Intelligent query suggestions

## Architecture
┌─────────────────┐ ┌─────────────────────┐ ┌──────────────┐
│ AI Agent │────│ MCP Server │────│ Data │
│ (Watsonx, etc.)│ │ (server_challenge2.py)│ │ Sources │
└─────────────────┘ └─────────────────────┘ └──────────────┘
│ │
┌───────┴───────┐ ┌───────┴───────┐
│ 6 MCP Tools │ │ SQL, API, File│
└───────────────┘ └───────────────┘


## Setup Instructions

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt

2. Install Ollama (optional for AI features):
   '''bash
    # Download from https://ollama.com
    ollama pull llama3.2:3b
3. Start the MCP server:
bash
python server_challenge2.py

4. Run the demo:
bash
python final_demo_fixed.py
