# Incorta Upgrade Agent

AI agent to assist in upgrading incorta enviroments

## The Idea

We define LangGraph workflows where large sequence actions are nodes inside the workflow. This gives us the ability sequentialize the actions or even  add agentic decisions on those outputs, and with more complex endpoints we can deal with their output internally before passing to the next node.

and this workflow is capsulated inside a tool in an mcp server, rather than making all actions in a workflow as a tools and noise the agent using this, so we transform the communication with incorta and processing logic to the workflows, and the mcp tool is just a wrapper to the workflow.

This repo is an example of **one tool that uses one workflow** - the pre-upgrade validation. The pattern can be extended to more tools and more complex workflows.

## Project Structure

```
incorta-upgrade-agent/
├── server.py                          # MCP server (Claude Desktop / Nexus)
├── clients/
│   └── cmc_client.py                  # CMC API wrapper (Basic Auth + JWT)
├── workflows/
│   └── pre_upgrade_validation.py      # LangGraph workflow
├── tools/
│   └── validation_checks.py           # Validation logic
├── tests/
│   ├── test_cmc_client.py             # Test CMC connection
│   └── test_workflow.py               # Test workflow standalone
└── claude_desktop_config.json         # MCP config example
```

## Setup with uv

```bash
# Create venv with uv
uv venv
source .venv/bin/activate

# Install deps
uv pip install -r requirements.txt

# Copy env and edit
cp .env.example .env
```

## Development & Testing

Test components individually without running the full MCP server:

```bash
# Test CMC client connection
python tests/test_cmc_client.py

# Test the workflow (this is the main test during development)
python tests/test_workflow.py
```

The workflow test will call the CMC API and generate a validation report - you can see exactly what Claude will receive.

## Running the MCP Server

For Claude Desktop, add to your config (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "incorta-upgrade-agent": {
      "command": "python3",
      "args": ["/path/to/incorta-upgrade-agent/server.py"],
      "env": {
        "CMC_URL": "https://your-cluster.cloudstaging.incortalabs.com/cmc",
        "CMC_USER": "admin",
        "CMC_PASSWORD": "your-password",
        "CMC_CLUSTER_NAME": "customCluster"
      }
    }
  }
}
```

See `claude_desktop_config.json` for a full example.

## How It Works

1. **MCP Tool** → User asks Claude to validate a cluster
2. **Workflow** → LangGraph runs the validation nodes:
   - Fetch cluster data from CMC
   - Run validation checks (services, memory)
   - Generate markdown report
3. **Report** → Claude receives the formatted report

## Extending

To add more checks or workflows:

1. Add check functions in `tools/validation_checks.py`
2. Add new nodes in `workflows/pre_upgrade_validation.py`
3. The workflow has placeholder nodes showing where to add future logic
4. For complex multi-step operations, add conditional edges between nodes
