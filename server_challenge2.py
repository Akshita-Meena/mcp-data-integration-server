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

# Tool 1: Natural Language Query
nl_tool = Tool(
    name="query_natural_language",
    description="Convert natural language to SQL and execute it",
    inputSchema={
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": "Question in natural language (e.g., 'Show users from USA')"
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
    description="Transform data (filter, sort, etc.)",
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
                "description": "Operation: filter, sort, limit",
                "default": "sort"
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

# ========== TOOL HANDLERS ==========

@server.list_tools()
async def handle_list_tools():
    return [nl_tool, sources_tool, sql_tool, transform_tool, export_tool]

@server.call_tool()
async def handle_call_tool(name: str, arguments: dict):
    try:
        if name == "query_natural_language":
            question = arguments.get("question", "")
            
            # Convert NL to SQL
            if OLLAMA_AVAILABLE:
                try:
                    response = ollama.generate(
                        model='llama3.2:3b',
                        prompt=f"Convert to SQL: {question}\nTables: users(id,name,email,country), orders(id,user_id,product,amount)\nReturn only SQL:",
                        options={'temperature': 0.1}
                    )
                    sql = response['response'].strip()
                except:
                    sql = "SELECT * FROM users"
            else:
                # Simple fallback
                if "USA" in question:
                    sql = "SELECT * FROM users WHERE country = 'USA'"
                elif "count" in question.lower():
                    sql = "SELECT COUNT(*) as count FROM users"
                else:
                    sql = "SELECT * FROM users"
            
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
                        "generated_sql": sql,
                        "result": data,
                        "row_count": len(data)
                    }, indent=2)
                }]
            }
        
        elif name == "list_sources":
            sources = [
                {"type": "sql", "name": "sample_db", "tables": ["users", "orders"]},
                {"type": "file", "name": "csv_files", "files": ["users.csv"]},
                {"type": "file", "name": "json_files", "files": ["products.json"]}
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
            
            if operation == "sort" and data:
                # Sort by id if exists
                data = sorted(data, key=lambda x: x.get('id', 0))
            elif operation == "filter" and data:
                # Filter: keep only items with id > 1
                data = [item for item in data if item.get('id', 0) > 1]
            elif operation == "limit" and data:
                # Limit to 3 items
                data = data[:3]
            
            return {
                "content": [{
                    "type": "text",
                    "text": json.dumps({
                        "operation": operation,
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
                import csv
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
                        "export": export_text[:500] + "..." if len(export_text) > 500 else export_text
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
    print("📊 5 Tools:", file=sys.stderr)
    print("  1. query_natural_language - NL to SQL with AI", file=sys.stderr)
    print("  2. list_sources - List data sources", file=sys.stderr)
    print("  3. execute_sql - Direct SQL queries", file=sys.stderr)
    print("  4. transform_data - Transform results", file=sys.stderr)
    print("  5. export_data - Export to JSON/CSV", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    print("📁 Database: data/sample.db (users, orders tables)", file=sys.stderr)
    print("=" * 70, file=sys.stderr)
    
    try:
        async with stdio_server() as (read_stream, write_stream):
            print("✅ Server running (stdio mode)", file=sys.stderr)
            
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="challenge2-data-integration",
                    server_version="1.0.0",
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