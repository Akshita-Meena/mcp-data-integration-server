# server_challenge2.py - Clean working version
import asyncio
import sys
import json
import os
import sqlite3
from mcp import Tool
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
import requests
import csv

# Try to import ollama
try:
    import ollama
    OLLAMA_AVAILABLE = True
except ImportError:
    OLLAMA_AVAILABLE = False
    print("⚠️  Ollama not available, using fallback NL to SQL", file=sys.stderr)

# ========== DATABASE SETUP ==========

def ensure_database():
    """Ensure database exists with sample data."""
    db_path = "data/sample.db"
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            email TEXT,
            country TEXT,
            signup_date DATE
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product TEXT,
            amount REAL,
            order_date DATE
        )
    """)
    
    # Check if we need to insert data
    cursor.execute("SELECT COUNT(*) FROM users")
    if cursor.fetchone()[0] == 0:
        # Insert sample users
        users = [
            (1, 'John Doe', 'john@example.com', 'USA', '2024-01-15'),
            (2, 'Jane Smith', 'jane@example.com', 'UK', '2024-02-20'),
            (3, 'Bob Wilson', 'bob@example.com', 'Canada', '2024-03-10'),
            (4, 'Alice Brown', 'alice@example.com', 'USA', '2024-01-25'),
            (5, 'Charlie Davis', 'charlie@example.com', 'Australia', '2024-02-28')
        ]
        cursor.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?)", users)
        
        # Insert sample orders
        orders = [
            (1, 1, 'Laptop', 999.99, '2024-01-20'),
            (2, 1, 'Mouse', 29.99, '2024-01-25'),
            (3, 2, 'Monitor', 399.99, '2024-02-25'),
            (4, 3, 'Keyboard', 89.99, '2024-03-15'),
            (5, 4, 'Laptop', 999.99, '2024-02-01')
        ]
        cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?)", orders)
    
    conn.commit()
    conn.close()
    return db_path

# Ensure database exists
DB_PATH = ensure_database()

# ========== TOOLS ==========

server = Server("challenge2-data-integration")

# Tool 1: Query Data (Natural Language or SQL)
query_data_tool = Tool(
    name="query_data",
    description="Query data using natural language or SQL from multiple sources",
    inputSchema={
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": "Natural language question or SQL query"
            },
            "source_type": {
                "type": "string",
                "description": "sql, api, or file",
                "default": "sql"
            }
        },
        "required": ["question"]
    }
)

# Tool 2: List Sources
sources_tool = Tool(
    name="list_sources",
    description="List available data sources",
    inputSchema={"type": "object", "properties": {}}
)

# Tool 3: Execute SQL
sql_tool = Tool(
    name="execute_sql",
    description="Execute SQL query directly",
    inputSchema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query to execute"
            }
        },
        "required": ["query"]
    }
)

# Tool 4: Transform Data
transform_tool = Tool(
    name="transform_data",
    description="Transform data (filter, sort, aggregate, etc.)",
    inputSchema={
        "type": "object",
        "properties": {
            "data": {
                "type": "array",
                "description": "Data to transform",
                "items": {"type": "object"}
            },
            "operation": {
                "type": "string",
                "description": "Operation: sort, filter, limit, aggregate",
                "default": "sort"
            },
            "params": {
                "type": "object",
                "description": "Operation parameters",
                "default": {}
            }
        },
        "required": ["data"]
    }
)

# Tool 5: Export Data
export_tool = Tool(
    name="export_data",
    description="Export data to JSON or CSV",
    inputSchema={
        "type": "object",
        "properties": {
            "data": {
                "type": "array",
                "description": "Data to export",
                "items": {"type": "object"}
            },
            "format": {
                "type": "string",
                "description": "Format: json or csv",
                "default": "json"
            }
        },
        "required": ["data"]
    }
)

# Tool 6: Integrate Data
integrate_tool = Tool(
    name="integrate_data",
    description="Combine data from multiple sources",
    inputSchema={
        "type": "object",
        "properties": {
            "datasets": {
                "type": "array",
                "description": "List of datasets to combine",
                "items": {"type": "object"}
            },
            "join_key": {
                "type": "string",
                "description": "Key to join on (e.g., 'id', 'user_id')"
            },
            "join_type": {
                "type": "string",
                "description": "inner, left, or right",
                "default": "inner"
            }
        },
        "required": ["datasets"]
    }
)

# ========== TOOL HANDLERS ==========

@server.list_tools()
async def handle_list_tools():
    return [query_data_tool, sources_tool, sql_tool, transform_tool, export_tool, integrate_tool]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict):
    try:
        if name == "query_data":
            question = arguments.get("question", "")
            source_type = arguments.get("source_type", "sql")
            
            if source_type == "sql":
                # Convert natural language to SQL if needed
                if OLLAMA_AVAILABLE and not question.strip().upper().startswith("SELECT"):
                    try:
                        response = ollama.generate(
                            model='llama3.2:3b',
                            prompt=f"Convert to SQL: {question}\nTables: users(id,name,email,country), orders(id,user_id,product,amount)\nReturn only SQL:",
                            options={'temperature': 0.1}
                        )
                        sql = response['response'].strip()
                    except:
                        # Simple fallback
                        if "USA" in question:
                            sql = "SELECT * FROM users WHERE country = 'USA'"
                        elif "count" in question.lower():
                            sql = "SELECT COUNT(*) as count FROM users"
                        else:
                            sql = "SELECT * FROM users"
                else:
                    sql = question
                
                # Execute SQL
                conn = sqlite3.connect(DB_PATH)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                data = [dict(row) for row in rows]
                conn.close()
                
                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "question": question,
                            "source_type": source_type,
                            "generated_sql": sql if sql != question else "Direct SQL",
                            "result": data,
                            "row_count": len(data)
                        }, indent=2)
                    }]
                }
            
            elif source_type == "api":
                # Mock API call - in real scenario, you'd have a real API
                try:
                    # For demo, return mock data
                    mock_api_data = [
                        {"id": 1, "api_name": "User 1", "status": "active"},
                        {"id": 2, "api_name": "User 2", "status": "inactive"},
                        {"id": 3, "api_name": "User 3", "status": "active"}
                    ]
                    return {
                        "content": [{
                            "type": "text",
                            "text": json.dumps({
                                "question": question,
                                "source_type": source_type,
                                "result": mock_api_data,
                                "note": "Mock API data - would connect to real API in production"
                            }, indent=2)
                        }]
                    }
                except Exception as api_error:
                    return {
                        "content": [{
                            "type": "text",
                            "text": json.dumps({
                                "error": f"API Error: {str(api_error)}",
                                "source_type": source_type,
                                "question": question
                            }, indent=2)
                        }],
                        "isError": True
                    }
            
            elif source_type == "file":
                # Handle file queries
                try:
                    if question.endswith(".csv"):
                        file_path = os.path.join("data", question)
                        data = []
                        with open(file_path, 'r') as f:
                            reader = csv.DictReader(f)
                            for row in reader:
                                data.append(row)
                    elif question.endswith(".json"):
                        file_path = os.path.join("data", question)
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                    else:
                        # Assume it's a file name without extension
                        try:
                            file_path = os.path.join("data", question + ".json")
                            with open(file_path, 'r') as f:
                                data = json.load(f)
                        except:
                            file_path = os.path.join("data", question + ".csv")
                            data = []
                            with open(file_path, 'r') as f:
                                reader = csv.DictReader(f)
                                for row in reader:
                                    data.append(row)
                    
                    return {
                        "content": [{
                            "type": "text",
                            "text": json.dumps({
                                "question": question,
                                "source_type": source_type,
                                "result": data,
                                "row_count": len(data)
                            }, indent=2)
                        }]
                    }
                except Exception as file_error:
                    return {
                        "content": [{
                            "type": "text",
                            "text": json.dumps({
                                "error": f"File Error: {str(file_error)}",
                                "source_type": source_type,
                                "question": question
                            }, indent=2)
                        }],
                        "isError": True
                    }
            
            else:
                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "error": f"Unsupported source type: {source_type}",
                            "supported_types": ["sql", "api", "file"]
                        }, indent=2)
                    }],
                    "isError": True
                }
        
        elif name == "list_sources":
            sources = [
                {"type": "sql", "name": "sample_db", "tables": ["users", "orders"], "description": "SQLite database with user and order data"},
                {"type": "api", "name": "mock_api", "endpoint": "http://localhost:3000", "description": "Mock REST API for testing"},
                {"type": "file", "name": "csv_files", "files": ["users.csv"], "description": "CSV files with user data"},
                {"type": "file", "name": "json_files", "files": ["products.json", "orders.json"], "description": "JSON files with product and order data"}
            ]
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({"sources": sources}, indent=2)
                }]
            }
        
        elif name == "execute_sql":
            query = arguments.get("query", "")
            conn = sqlite3.connect(DB_PATH)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            data = [dict(row) for row in rows]
            conn.close()
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "query": query,
                        "result": data,
                        "row_count": len(data)
                    }, indent=2)
                }]
            }
        
        elif name == "transform_data":
            data = arguments.get("data", [])
            operation = arguments.get("operation", "sort")
            params = arguments.get("params", {})
            
            if operation == "sort" and data:
                # Sort by specified field or default to id
                sort_key = params.get("by", "id")
                reverse = params.get("reverse", False)
                data = sorted(data, key=lambda x: x.get(sort_key, 0), reverse=reverse)
            
            elif operation == "filter" and data:
                # Filter by condition
                field = params.get("field", "id")
                value = params.get("value", 1)
                condition = params.get("condition", ">")
                
                if condition == ">":
                    data = [item for item in data if item.get(field, 0) > value]
                elif condition == "=":
                    data = [item for item in data if item.get(field) == value]
                elif condition == "<":
                    data = [item for item in data if item.get(field, 0) < value]
                elif condition == "contains":
                    data = [item for item in data if value in str(item.get(field, ""))]
            
            elif operation == "limit" and data:
                # Limit number of items
                limit = params.get("limit", 3)
                data = data[:limit]
            
            elif operation == "aggregate" and data:
                # Simple aggregation
                agg_field = params.get("field", "value")
                agg_type = params.get("type", "sum")
                
                values = [item.get(agg_field, 0) for item in data if agg_field in item]
                if agg_type == "sum":
                    result = sum(values)
                elif agg_type == "avg":
                    result = sum(values) / len(values) if values else 0
                elif agg_type == "count":
                    result = len(data)
                elif agg_type == "max":
                    result = max(values) if values else 0
                elif agg_type == "min":
                    result = min(values) if values else 0
                
                data = [{"aggregation_type": agg_type, "field": agg_field, "result": result}]
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "operation": operation,
                        "params": params,
                        "result": data,
                        "count": len(data)
                    }, indent=2)
                }]
            }
        
        elif name == "export_data":
            data = arguments.get("data", [])
            format_type = arguments.get("format", "json")
            
            if format_type == "json":
                export_text = json.dumps(data, indent=2)
            elif format_type == "csv" and data:
                import io
                output = io.StringIO()
                if data and isinstance(data[0], dict):
                    writer = csv.DictWriter(output, fieldnames=data[0].keys())
                    writer.writeheader()
                    writer.writerows(data)
                    export_text = output.getvalue()
                else:
                    export_text = "Cannot convert to CSV"
            else:
                export_text = f"Unsupported format: {format_type}"
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "format": format_type,
                        "export": export_text[:500] + "..." if len(export_text) > 500 else export_text,
                        "note": "Truncated if longer than 500 characters"
                    }, indent=2)
                }]
            }
        
        elif name == "integrate_data":
            datasets = arguments.get("datasets", [])
            join_key = arguments.get("join_key", "id")
            join_type = arguments.get("join_type", "inner")
            
            if len(datasets) < 2:
                return {
                    "content": [{
                        "type": "text",
                        "text": json.dumps({
                            "error": "Need at least 2 datasets to integrate",
                            "datasets_received": len(datasets)
                        }, indent=2)
                    }],
                    "isError": True
                }
            
            # Simple integration logic
            integrated_data = []
            
            if join_type == "inner":
                # Find common keys
                all_keys = set()
                for dataset in datasets:
                    for item in dataset:
                        if join_key in item:
                            all_keys.add(item[join_key])
                
                # Create integrated records
                for key in all_keys:
                    record = {join_key: key}
                    for i, dataset in enumerate(datasets):
                        for item in dataset:
                            if item.get(join_key) == key:
                                # Add all fields with dataset prefix
                                for k, v in item.items():
                                    if k != join_key:
                                        record[f"dataset{i+1}_{k}"] = v
                                break
                    integrated_data.append(record)
            
            elif join_type == "left":
                # Use first dataset as base
                base_dataset = datasets[0]
                for base_item in base_dataset:
                    record = base_item.copy()
                    base_key = base_item.get(join_key)
                    
                    # Join with other datasets
                    for i, dataset in enumerate(datasets[1:], 1):
                        found = False
                        for item in dataset:
                            if item.get(join_key) == base_key:
                                for k, v in item.items():
                                    if k != join_key:
                                        record[f"dataset{i+1}_{k}"] = v
                                found = True
                                break
                        
                        if not found:
                            for k in item.keys():
                                if k != join_key:
                                    record[f"dataset{i+1}_{k}"] = None
                    
                    integrated_data.append(record)
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "integration_type": join_type,
                        "join_key": join_key,
                        "datasets_count": len(datasets),
                        "integrated_records": len(integrated_data),
                        "result": integrated_data[:10],  # Limit output
                        "note": f"Showing first 10 of {len(integrated_data)} records"
                    }, indent=2)
                }]
            }
        
        else:
            return {
                "content": [{"type": "text", "text": f"Unknown tool: {name}"}],
                "isError": True
            }
    
    except Exception as e:
        return {
            "content": [{"type": "text", "text": f"Error: {str(e)}"}],
            "isError": True
        }

# ========== MAIN ==========

async def main():
    print("=" * 70, file=sys.stderr)
    print("🚀 CHALLENGE 2: DATA INTEGRATION MCP SERVER", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print(f"🤖 AI: {'Ollama llama3.2:3b' if OLLAMA_AVAILABLE else 'Fallback'}", file=sys.stderr)
    print("📊 6 Tools:", file=sys.stderr)
    print("  1. query_data - Query data from SQL, API, or files", file=sys.stderr)
    print("  2. list_sources - List available data sources", file=sys.stderr)
    print("  3. execute_sql - Direct SQL queries", file=sys.stderr)
    print("  4. transform_data - Transform results (filter, sort, aggregate)", file=sys.stderr)
    print("  5. export_data - Export to JSON/CSV", file=sys.stderr)
    print("  6. integrate_data - Combine data from multiple sources", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print("📁 Data Sources:", file=sys.stderr)
    print("  • SQL: data/sample.db (users, orders tables)", file=sys.stderr)
    print("  • Files: data/users.csv, data/products.json, data/orders.json", file=sys.stderr)
    print("  • API: Mock REST API endpoint", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    
    try:
        async with stdio_server() as (read_stream, write_stream):
            print("✅ Server running (stdio mode)", file=sys.stderr)
            
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="challenge2-data-integration",
                    server_version="2.0.0",
                    capabilities={}
                )
            )
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n✅ Server stopped", file=sys.stderr)
