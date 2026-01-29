# server_final.py - Fixed initialization
import asyncio
import sys
import json
from mcp import Tool
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server

# Create server
server = Server("data-integration-mcp")

# Define tools
list_sources_tool = Tool(
    name="list_sources",
    description="List available data sources",
    inputSchema={
        "type": "object",
        "properties": {}
    }
)

query_data_tool = Tool(
    name="query_data",
    description="Query data from sources",
    inputSchema={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Query to execute"
            },
            "source_type": {
                "type": "string",
                "description": "Type of source: sql, api, file",
                "default": "sql"
            }
        },
        "required": ["query"]
    }
)

# Tool handler
@server.list_tools()
async def handle_list_tools():
    """Return available tools."""
    return [list_sources_tool, query_data_tool]

# Tool execution handler
@server.call_tool()
async def handle_call_tool(name: str, arguments: dict) -> dict:
    """Execute a tool."""
    try:
        if name == "list_sources":
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps({
                            "sources": [
                                {"type": "sql", "name": "sample_db", "description": "SQLite database"},
                                {"type": "api", "name": "mock_api", "description": "REST API"},
                                {"type": "file", "name": "csv_files", "description": "CSV files"}
                            ]
                        }, indent=2)
                    }
                ]
            }
        elif name == "query_data":
            query = arguments.get("query", "")
            source_type = arguments.get("source_type", "sql")
            
            # Simulate query execution
            result = {
                "query": query,
                "source_type": source_type,
                "result": f"Executed {source_type.upper()} query: {query}",
                "sample_data": [
                    {"id": 1, "name": "Sample 1"},
                    {"id": 2, "name": "Sample 2"}
                ]
            }
            
            return {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(result, indent=2)
                    }
                ]
            }
        else:
            return {
                "content": [
                    {
                        "type": "text",
                        "text": f"Error: Unknown tool '{name}'"
                    }
                ],
                "isError": True
            }
    except Exception as e:
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Error executing tool '{name}': {str(e)}"
                }
            ],
            "isError": True
        }

async def main():
    """Run the MCP server with stdio."""
    print("🚀 Starting Data Integration MCP Server...", file=sys.stderr)
    print("📊 Tools available: list_sources, query_data", file=sys.stderr)
    print("🛑 Press Ctrl+C to stop", file=sys.stderr)
    
    try:
        async with stdio_server() as (read_stream, write_stream):
            print("✅ Server connected to stdio streams", file=sys.stderr)
            
            # Create proper initialization options
            init_options = InitializationOptions(
                server_name="data-integration-mcp",
                server_version="1.0.0",
                capabilities={}
            )
            
            await server.run(
                read_stream,
                write_stream,
                init_options
            )
    except asyncio.CancelledError:
        print("\n⏹️  Server cancelled", file=sys.stderr)
    except Exception as e:
        print(f"\n❌ Server error: {e}", file=sys.stderr)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏹️  Server stopped by user", file=sys.stderr)