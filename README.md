# Incorta Upgrade Validation Agent

AI-powered automation agent that validates Incorta cluster readiness before upgrades.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Copy and edit config
cp .env.example .env

# Test CMC connection
python tests/test_cmc_client.py

# Test workflow (without MCP)
python tests/test_workflow.py

# Run MCP server
python server.py
```

## Claude Desktop Setup

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "incorta-upgrade-agent": {
      "command": "python3",
      "args": ["/path/to/incorta-upgrade-agent/server.py"]
    }
  }
}
```

See `claude_desktop_config.json` for full example.

## Usage

In Claude Desktop:
> "Run pre-upgrade validation for customCluster"

## Project Structure

```
incorta-upgrade-agent/
├── server.py                    # MCP server entry
├── clients/
│   └── cmc_client.py            # CMC API wrapper
├── workflows/
│   └── pre_upgrade_validation.py # LangGraph workflow
├── tools/
│   └── validation_checks.py     # Check logic
└── tests/
    ├── test_cmc_client.py       # Test CMC connection
    └── test_workflow.py         # Test workflow
```
