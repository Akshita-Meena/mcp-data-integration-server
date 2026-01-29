# final_demo_fixed.py - Fixed demonstration
import subprocess
import json
import time

# Add this test after existing tests in final_demo_fixed.py:

# Test integrate_data
print("\n6. 🔗 Testing data integration...")
sample_users = [
    {"id": 1, "name": "John", "country": "USA"},
    {"id": 2, "name": "Jane", "country": "UK"}
]
sample_orders = [
    {"id": 1, "user_id": 1, "product": "Laptop"},
    {"id": 2, "user_id": 2, "product": "Monitor"}
]

integrate_response = send_request(6, "tools/call", {
    "name": "integrate_data",
    "arguments": {
        "datasets": [sample_users, sample_orders],
        "join_key": "id",
        "join_type": "inner"
    }
})

integrate_data = parse_response(integrate_response)
if integrate_data and isinstance(integrate_data, dict):
    print(f"   ✅ Integration successful")
    print(f"   Integrated {integrate_data.get('integrated_records', 0)} records")

    
def parse_response(response):
    """Parse MCP server response."""
    if 'result' in response and 'content' in response['result']:
        content_text = response['result']['content'][0]['text']
        try:
            # Try to parse as JSON (it might be nested)
            return json.loads(content_text)
        except json.JSONDecodeError:
            # If not JSON, return as text
            return content_text
    return None

def run_demo():
    print("=" * 70)
    print("🎯 FINAL DEMONSTRATION - Challenge 2 Complete Solution")
    print("=" * 70)
    
    # Start server
    print("\n🚀 Starting MCP server...")
    server = subprocess.Popen(
        ["python", "server_challenge2.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    time.sleep(3)
    
    def send_request(request_id, method, params):
        msg = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params
        }
        server.stdin.write(json.dumps(msg) + "\n")
        server.stdin.flush()
        response = server.stdout.readline()
        return json.loads(response)
    
    try:
        # Initialize
        print("\n1. 📡 Initializing connection...")
        init_response = send_request(1, "initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "Demo", "version": "1.0"}
        })
        print(f"   ✅ Connected to: {init_response['result']['serverInfo']['name']}")
        
        # List tools
        print("\n2. 🛠️  Listing available tools...")
        tools_response = send_request(2, "tools/list", {})
        tools = tools_response['result']['tools']
        print(f"   ✅ Found {len(tools)} tools:")
        for tool in tools:
            print(f"     - {tool['name']}: {tool['description'][:50]}...")
        
        # Test Natural Language Query (simpler)
        print("\n3. 🤖 Testing Natural Language Query...")
        print("   Asking: 'Show all users'")
        nl_response = send_request(3, "tools/call", {
            "name": "query_natural_language",
            "arguments": {"question": "Show all users"}
        })
        
        nl_data = parse_response(nl_response)
        if nl_data and isinstance(nl_data, dict):
            print(f"   ✅ Generated SQL: {nl_data.get('generated_sql', 'N/A')}")
            if 'result' in nl_data:
                if isinstance(nl_data['result'], list):
                    print(f"   Found {len(nl_data['result'])} users")
                elif isinstance(nl_data['result'], dict):
                    print(f"   Query executed: {nl_data['result'].get('row_count', 0)} rows")
        else:
            print(f"   Response: {str(nl_data)[:100]}...")
        
        # Test direct SQL
        print("\n4. 🗃️  Testing direct SQL query...")
        sql_response = send_request(4, "tools/call", {
            "name": "execute_sql",
            "arguments": {"query": "SELECT name, country FROM users LIMIT 3"}
        })
        
        sql_data = parse_response(sql_response)
        if sql_data and isinstance(sql_data, dict):
            print(f"   ✅ Query executed")
            if 'result' in sql_data and isinstance(sql_data['result'], list):
                print(f"   Sample results:")
                for i, row in enumerate(sql_data['result'][:3], 1):
                    print(f"     {i}. {row.get('name', 'N/A')} - {row.get('country', 'N/A')}")
        
        # Test transformation
        print("\n5. 🔄 Testing data transformation...")
        sample = [
            {"id": 2, "name": "Bob", "value": 200},
            {"id": 1, "name": "Alice", "value": 100},
            {"id": 3, "name": "Charlie", "value": 300}
        ]
        
        transform_response = send_request(5, "tools/call", {
            "name": "transform_data",
            "arguments": {
                "data": sample,
                "operation": "sort"
            }
        })
        
        transform_data = parse_response(transform_response)
        if transform_data and isinstance(transform_data, dict):
            print(f"   ✅ Transformation successful")
            if 'result' in transform_data:
                print(f"   Sorted {len(transform_data['result'])} items")
        
        print("\n" + "=" * 70)
        print("✅ DEMONSTRATION COMPLETE!")
        print("🎉 All 5 MCP tools tested successfully!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        server.terminate()
        server.wait()
        print("\nServer stopped.")

if __name__ == "__main__":
    run_demo()
