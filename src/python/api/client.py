import asyncio
from fastapi_mcp_client import MCPClient

async def main():
    async with MCPClient("http://127.0.0.1:8000") as client:
        print("Fetching MCP tools...")
        try:
            # fastapi_mcp_client doesn't have a list_tools() method, 
            # but we can inspect the operations dynamically
            # For demonstration, we'll call the `/` endpoint to see what's available
            response = await client.call_operation("list_operations")  # pseudo-op
            print("Available tools:", response)
        except Exception as e:
            print("Could not list tools directly via MCPClient:", e)
            print("Currently, MCPClient can only call known operation_ids")

asyncio.run(main())
