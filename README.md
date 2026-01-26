# Incorta Upgrade Agent

AI agent to assist in upgrading Incorta environments

## The Idea

We define LangGraph workflows where large sequence actions are nodes inside the workflow. This gives us the ability to sequentialize the actions or even add agentic decisions on those outputs, and with more complex endpoints we can deal with their output internally before passing to the next node.

This workflow is encapsulated inside a tool in an MCP server, rather than making all actions in a workflow as tools and noising the agent using this. We transform the communication with Incorta and processing logic to the workflows, and the MCP tool is just a wrapper to the workflow.

## Features

### 🤖 **Auto-Detection Tools** 
- **Cluster Metadata Extraction**: Automatically extracts upgrade-relevant metadata
  - **Zero user questions** - infers everything from CMC cluster data
  - Deployment type (Cloud: GCP/AWS/Azure vs On-Prem)
  - Database type (MySQL/Oracle) + migration requirements
  - Topology (Typical/Clustered, node count, HA status)
  - Features (Notebook, Spark, SQLi, connectors)
  - Infrastructure (Spark mode: K8s/External, Zookeeper)
  - Service status (all services)
  - **Risk assessment**: AUTO-CLASSIFY as HIGH/MEDIUM/LOW
  - **Output formats**: JSON (structured) + Markdown (readable report)

### 🔍 **Validation Tools**
- **Pre-Upgrade Validation**: Comprehensive cluster health checks via CMC API
  - Service status checks
  - Memory usage validation
  - Node topology analysis
  - Infrastructure services verification
  - Database migration status

### 📚 **Knowledge Search Tools**
- **Semantic Search**: Search docs, community, and support using RAG
  - Upgrade guides and release notes
  - Known issues and troubleshooting
  - Community experiences and tips
  
### 📊 **Data Analysis Tools**
- **Zendesk Queries**: Query customer support tickets via Incorta SQL
  - Customer-reported upgrade issues
  - Support trends and patterns
  
- **Jira Queries**: Query development issues via Incorta SQL
  - Upgrade-related bugs and features
  - Development work tracking

### 🔄 **Workflows**
- **Pre-Upgrade Validation**: Complete cluster readiness check
- **Upgrade Research**: Multi-source research on upgrade paths
  - Official documentation search
  - Known issues discovery
  - Community insights gathering

## Project Structure

```
incorta-upgrade-agent/
├── server.py                          # MCP server (Claude Desktop / Nexus)
├── context/
│   └── user_context.py                # User credentials context
├── clients/
│   └── cmc_client.py                  # CMC API wrapper (Basic Auth + JWT)
├── workflows/
│   ├── pre_upgrade_validation.py      # LangGraph validation workflow
│   └── upgrade_research.py            # LangGraph research workflow
├── tools/
│   ├── validation_checks.py           # Validation logic
│   ├── extract_cluster_metadata.py    # Auto-detection & metadata extraction
│   ├── qdrant_tool.py                 # Semantic search
│   └── incorta_tools.py               # Zendesk/Jira queries
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

## Environment Variables

```bash
# CMC API (for cluster validation)
CMC_URL=https://your-cluster.cloudstaging.incortalabs.com/cmc
CMC_USER=admin
CMC_PASSWORD=your-password
CMC_CLUSTER_NAME=customCluster

# Qdrant (for semantic search)
QDRANT_URL=https://your-qdrant-instance.cloud
QDRANT_API_KEY=your-qdrant-api-key

# Incorta (for Zendesk/Jira queries)
INCORTA_ENV_URL=https://your-incorta-env.com
INCORTA_TENANT=your-tenant
INCORTA_USERNAME=your-username
INCORTA_PASSWORD=your-password
```

## Development & Testing

Test components individually without running the full MCP server:

```bash
# Test CMC client connection
python tests/test_cmc_client.py

# Test validation workflow
python tests/test_workflow.py

# Test upgrade research workflow
python workflows/upgrade_research.py
```

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
        "CMC_CLUSTER_NAME": "customCluster",
        "QDRANT_URL": "https://your-qdrant-instance.cloud",
        "QDRANT_API_KEY": "your-qdrant-api-key",
        "INCORTA_ENV_URL": "https://your-incorta-env.com",
        "INCORTA_TENANT": "your-tenant",
        "INCORTA_USERNAME": "your-username",
        "INCORTA_PASSWORD": "your-password"
      }
    }
  }
}
```

See `claude_desktop_config.json` for a full example.

## Available MCP Tools

### 1. `extract_cluster_metadata` 
Automatically extracts upgrade-relevant metadata from cluster data. **Zero user questions needed!**

**Input:**
- `cluster_name`: Name of cluster to analyze
- `format`: Output format - "json", "markdown", or "both" (default)

**Output:** 
- **Deployment**: Cloud provider (GCP/AWS/Azure) or On-Prem detection
- **Database**: Type (MySQL/Oracle) + migration requirements
- **Topology**: Typical/Clustered, node count, HA status
- **Features**: Notebook, Spark, SQLi, connectors (enabled/disabled)
- **Infrastructure**: Spark mode (K8s/External/Embedded), Zookeeper mode
- **Service Status**: All services (Analytics, Loader, Notebook, SQLi)
- **Risk Assessment**: HIGH/MEDIUM/LOW with blockers/warnings/info

**Example Usage:**
```python
# In Claude Desktop:
"Analyze the customCluster and tell me what's configured"
# → Returns: Cloud (GCP), MySQL, 2 nodes (HA), Notebook enabled, 
#    Spark (K8s), HIGH risk (services stopped)
```

### 2. `run_pre_upgrade_validation`
Validates cluster readiness before upgrade.

**Input:**
- `cluster_name`: Name of cluster to validate

**Output:** Markdown report with validation results

### 2. `research_upgrade_path`
Research an upgrade path between two versions.

**Input:**
- `from_version`: Current version (e.g., "2024.1.0")
- `to_version`: Target version (e.g., "2024.7.0")

**Output:** Markdown report with:
- Official documentation
- Known issues
- Community experiences

### 3. `search_upgrade_knowledge`
Semantic search for upgrade-related information.

**Input:**
- `query`: Search query
- `limit`: Number of results (default: 10)

**Output:** JSON with search results

### 4. `get_zendesk_schema`
Get Zendesk schema for querying tickets.

**Input:**
- `fetch_schema`: Set to true

**Output:** Schema with tables and columns

### 5. `query_upgrade_tickets`
Query Zendesk tickets using SQL.

**Input:**
- `spark_sql`: SQL query

**Output:** Query results

### 6. `get_jira_schema`
Get Jira schema for querying issues.

**Input:**
- `fetch_schema`: Set to true

**Output:** Schema with tables and columns

### 7. `query_upgrade_issues`
Query Jira issues using SQL.

**Input:**
- `spark_sql`: SQL query

**Output:** Query results

## Usage Examples

### With Claude Desktop

```
User: "Analyze my customCluster and tell me about its configuration"
Claude: [Uses extract_cluster_metadata tool]
→ Returns: Cloud (GCP), MySQL, 2 HA nodes, Notebook/Spark enabled, 
   Kubernetes Spark, HIGH risk (services stopped)

User: "Should customer Acme Corp upgrade from 2024.1.5 to 2024.7.0?"
Claude: 
  1. [Uses extract_cluster_metadata to auto-detect configuration]
  2. [Uses run_pre_upgrade_validation for health check]
  3. [Uses search_upgrade_knowledge for release notes]
  4. [Uses query_upgrade_tickets for customer issues]
  5. [Uses query_upgrade_issues for linked Jira bugs]
  → Provides comprehensive upgrade recommendation

User: "Validate the production cluster before upgrading"
Claude: [Uses run_pre_upgrade_validation tool]

User: "Research upgrade path from 2024.1.0 to 2024.7.0"
Claude: [Uses research_upgrade_path workflow]

User: "Search for upgrade issues with analytics service"
Claude: [Uses search_upgrade_knowledge tool]

User: "Find all Zendesk tickets about upgrade failures"
Claude: [Uses get_zendesk_schema + query_upgrade_tickets]
```

## How It Works

1. **MCP Tools** → User asks Claude to validate or research
2. **LangGraph Workflow** → Runs validation or research nodes:
   - Fetch cluster data from CMC
   - Search docs/community/support via Qdrant
   - Query Zendesk/Jira via Incorta
   - Generate markdown report
3. **Report** → Claude receives and discusses findings

## Extending

To add more checks or workflows:

1. Add check functions in `tools/validation_checks.py`
2. Add new nodes in workflow files
3. Create new workflow in `workflows/`
4. Add tool definition in `server.py`
5. Add tool handler in `@app.call_tool()`

## Architecture

```
Claude Desktop
    ↓ MCP Protocol
Upgrade Agent Server
    ↓
LangGraph Workflows
    ├─→ CMC API (cluster data)
    ├─→ Qdrant (semantic search)
    └─→ Incorta (Zendesk/Jira queries)
    ↓
Combined Reports
```

## Notes

- The semantic search requires your Qdrant collection to be populated with scraped docs
- Incorta queries require proper schemas (ZendeskTickets, Jira_F) to be set up
- All tools are designed to be used via Claude Desktop's MCP integration
