"""
MCP Server for Incorta Upgrade Validation Agent.
Exposes tools to Claude Desktop.
"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp import types

from workflows.pre_upgrade_validation import run_validation


app = Server("incorta-upgrade-agent")


@app.list_tools()
async def list_tools():
    return [
        types.Tool(
            name="run_pre_upgrade_validation",
            description="Validates Incorta cluster readiness before upgrade. Checks service status, memory usage, and generates a report.",
            inputSchema={
                "type": "object",
                "required": ["cluster_name"],
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "Name of the cluster to validate (e.g., 'customCluster')"
                    }
                }
            }
        ),
        # ------   expand as needed
        """ 
        the tools here uses the langraph workflows, so each tool must represent a larger block of actions
             from the user of the agent that will use this mcp, so for example we can group all the "pre-upgrade" to one tool
             or any other organization model ,just we aim to reduce the noise from the mcp and direct as possible 
        """
    
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    if name == "run_pre_upgrade_validation":
        cluster_name = arguments.get("cluster_name")
        if not cluster_name:
            return [types.TextContent(type="text", text="Error: cluster_name is required")]
        
        # run the workflow (blocking call wrapped for async)
        report = await asyncio.get_event_loop().run_in_executor(
            None, run_validation, cluster_name
        )
        
        return [types.TextContent(type="text", text=report)]
    
    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
