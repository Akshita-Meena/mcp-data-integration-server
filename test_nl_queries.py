# test_nl_debug.py - Debug NL to SQL conversion
import subprocess
import json
import time

def debug_nl_queries():
    print("🔍 Debugging Natural Language to SQL")
    print("=" * 60)
    
    server = subprocess.Popen(
        ["python", "server_enhanced.py"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    
    time.sleep(3)
    
    try:
        # Initialize
        init_msg = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "Debugger", "version": "1.0"}
            }
        }
        
        server.stdin.write(json.dumps(init_msg) + "\n")
        server.stdin.flush()
        init_response = server.stdout.readline()
        print(f"Init response: {init_response[:100]}...")
        
        # Test a simple query
        print("\n🔧 Testing: 'Show me all users'")
        nl_msg = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "query_natural_language",
                "arguments": {"question": "Show me all users"}
            }
        }
        
        server.stdin.write(json.dumps(nl_msg) + "\n")
        server.stdin.flush()
        response = server.stdout.readline()
        
        print(f"Full response length: {len(response)}")
        print(f"Response preview: {response[:200]}...")
        
        try:
            parsed = json.loads(response)
            if 'result' in parsed:
                content = parsed['result']['content'][0]['text']
                print(f"\n📄 Content received:")
                print(content[:500] + "..." if len(content) > 500 else content)
                
                # Try to parse the content as JSON
                try:
                    content_json = json.loads(content)
                    print(f"\n✅ Parsed JSON successfully!")
                    print(f"Natural Language: {content_json.get('natural_language')}")
                    print(f"Generated SQL: {content_json.get('generated_sql')}")
                    if 'result' in content_json:
                        result = content_json['result']
                        if result.get('success'):
                            print(f"Query success: {result.get('row_count', 0)} rows returned")
                        else:
                            print(f"Query error: {result.get('error')}")
                except json.JSONDecodeError:
                    print(f"\n❌ Content is not valid JSON")
                    print(f"Raw content type: {type(content)}")
            else:
                print(f"\n❌ No 'result' in response")
                print(f"Response keys: {parsed.keys()}")
                
        except json.JSONDecodeError as e:
            print(f"\n❌ Failed to parse response as JSON: {e}")
            print(f"Raw response: {response}")
        
    finally:
        server.terminate()
        server.wait()
        print("\n" + "=" * 60)
        print("🔍 Debug completed")

if __name__ == "__main__":
    debug_nl_queries()