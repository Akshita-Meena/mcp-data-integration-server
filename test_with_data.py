# test_with_data.py - Test server with actual data queries
import subprocess
import json
import time

def test_queries():
    print("🧪 Testing MCP Server with Real Data Queries")
    print("=" * 60)
    
    server = subprocess.Popen(
        ["python", "server.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    time.sleep(2)
    
    try:
        # Initialize
        init_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "Data Tester", "version": "1.0"}
            }
        }
        
        server.stdin.write(json.dumps(init_msg) + "\n")
        server.stdin.flush()
        server.stdout.readline()  # Read init response
        
        # Test 1: List sources
        print("1. Testing list_sources:")
        msg = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "list_sources", "arguments": {}}
        }
        server.stdin.write(json.dumps(msg) + "\n")
        server.stdin.flush()
        response = json.loads(server.stdout.readline())
        if 'result' in response:
            print("   ✅ Success - Sources listed")
        else:
            print("   ❌ Failed")
        
        # Test 2: SQL query
        print("\n2. Testing SQL query:")
        msg = {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "query_data",
                "arguments": {
                    "query": "SELECT * FROM users LIMIT 3",
                    "source_type": "sql"
                }
            }
        }
        server.stdin.write(json.dumps(msg) + "\n")
        server.stdin.flush()
        response = json.loads(server.stdout.readline())
        if 'result' in response:
            print("   ✅ Success - SQL query executed")
        else:
            print("   ❌ Failed")
        
        # Test 3: CSV file query
        print("\n3. Testing CSV file read:")
        msg = {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "query_data",
                "arguments": {
                    "query": "users.csv",
                    "source_type": "file"
                }
            }
        }
        server.stdin.write(json.dumps(msg) + "\n")
        server.stdin.flush()
        response = json.loads(server.stdout.readline())
        if 'result' in response:
            print("   ✅ Success - CSV file read")
        else:
            print("   ❌ Failed")
        
        # Test 4: Data transformation
        print("\n4. Testing data transformation:")
        msg = {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "transform_data",
                "arguments": {
                    "data": [
                        {"id": 3, "name": "Charlie", "value": 300},
                        {"id": 1, "name": "Alice", "value": 100},
                        {"id": 2, "name": "Bob", "value": 200}
                    ],
                    "operations": ["sort"]
                }
            }
        }
        server.stdin.write(json.dumps(msg) + "\n")
        server.stdin.flush()
        response = json.loads(server.stdout.readline())
        if 'result' in response:
            print("   ✅ Success - Data transformed")
        else:
            print("   ❌ Failed")
        
        print("\n" + "=" * 60)
        print("✅ All data tests completed!")
        
    finally:
        server.terminate()
        server.wait()

if __name__ == "__main__":
    test_queries()