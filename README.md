# Incorta Upgrade Assistant

An MCP (Model Context Protocol) server that automates Incorta cluster upgrade assessments. It connects to CMC, Cloud Portal, and Incorta Analytics APIs to gather cluster data, run health checks, search documentation, query customer tickets, and test datasource connections — then produces a unified readiness report.

## Architecture

```
AI Client (Claude Desktop / any MCP client)
    |
    | MCP Protocol (Streamable HTTP)
    v
server.py (FastMCP)
    |
    +---> clients/
    |       cmc_client.py          CMC REST API (JWT auth)
    |       cloud_portal_client.py Cloud Portal API (OAuth2 + PKCE via Auth0)
    |
    +---> tools/
    |       extract_cluster_metadata.py   Auto-detect deployment, DB, topology, risk
    |       validation_checks.py          10 pre-upgrade health checks
    |       test_connection.py            Datasource connectivity testing
    |       incorta_tools.py              Zendesk/Jira SQL queries via Incorta
    |       qdrant_tool.py                Semantic search over docs (RAG)
    |       jira_helpers.py               Jira query builders
    |       zendesk_helpers.py            Zendesk query builders
    |
    +---> workflows/                LangGraph workflows (multi-step orchestration)
    |       readiness_report.py     Full upgrade readiness assessment
    |       pre_upgrade_validation.py  Health check workflow
    |       collect_zendesk_issues.py   Customer ticket analysis
    |       collect_jira_issues.py      Bug tracking analysis
    |       upgrade_research.py         Knowledge base research
    |       checklist_workflow.py        Excel checklist export
    |
    +---> context/
            user_context.py         Thread-safe user credential storage
```

## Quick Start

```bash
# 1. Clone and set up
git clone <repo-url>
cd Incorta-Upgrade-Assistant

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env with your credentials (see Configuration section below)

# 5. Run the server
python server.py
# Server starts at http://127.0.0.1:8000
```

## Configuration

The server authenticates against three Incorta systems. Each can be configured via `.env` or through browser-based login portals at runtime.

### Option A: Login Portals (recommended for development)

No `.env` credentials needed. The server provides browser-based login forms:

| Portal | URL | Credentials |
|--------|-----|-------------|
| CMC | `http://localhost:8000/cmc-login` | CMC URL, username, password, cluster name |
| Cloud Portal | OAuth via browser | Incorta Cloud account (Auth0) |
| Analytics + Test Connection | `http://localhost:8000/test-connection-login` | CMC creds + Analytics tenant, username, password |

Call the corresponding MCP tool (`cmc_login`, `cloud_portal_login`, `test_datasource_connections`) to get the portal URL, open it in your browser, authenticate, then call the tool again.

### Option B: Environment Variables (for CI/Docker/headless)

```bash
# CMC (cluster configuration, health checks)
CMC_URL=https://yourcluster.cloud2.incorta.com/cmc
CMC_USER=admin
CMC_PASSWORD=your-password
CMC_CLUSTER_NAME=customCluster

# Cloud Portal (instance metadata, sizing, consumption)
# Auto-configured from CMC URL. Override only if needed:
# CLOUD_PORTAL_TOKEN=static-bearer-token
# CLOUD_PORTAL_USER_ID=your-uuid

# Incorta Analytics (Zendesk/Jira queries)
INCORTA_ENV_URL=https://yourcluster.cloud2.incorta.com/incorta
INCORTA_TENANT=your-tenant
INCORTA_USERNAME=your-username
INCORTA_PASSWORD=your-password

# Qdrant (knowledge base search)
QDRANT_URL=https://your-qdrant-instance.cloud
QDRANT_API_KEY=your-key

# SSL verification (default: true)
VERIFY_SSL=true
```

See `.env.example` for all available options including Auth0 overrides, headless mode, and Docker settings.

### Environment Auto-Detection

The server auto-detects staging vs production from the CMC URL:
- `cloudstaging.incortalabs.com` --> Staging (Auth0: `auth-staging.incortalabs.com`)
- `cloud2.incorta.com` --> Production (Auth0: `auth.incorta.com`)

## Authentication Flows

### 1. CMC -- JWT via Basic Auth
- Authenticates against `{CMC_URL}/api/v1/auth/login`
- JWT cached at `~/.incorta_cmc_token.json` (auto-expires with 5-min buffer)
- Used for: cluster metadata, service status, node topology, connectors

### 2. Cloud Portal -- OAuth 2.0 + PKCE via Auth0
- Browser opens Auth0 login, redirects to `localhost:8910/callback`
- PKCE challenge prevents code interception
- Tokens cached at `~/.incorta_cloud_token.json` with silent refresh
- Used for: instance details, versions, sizing, consumption, feature flags, users

### 3. Incorta Analytics -- Session Cookies + CSRF
- Form POST to `{URL}/authservice/login` with tenant/username/password
- Extracts JSESSIONID + XSRF-TOKEN + accessToken
- Session is ephemeral (in-memory, not cached to disk)
- Used for: Zendesk/Jira SQL queries, datasource connection testing

## MCP Tools Reference

### Core Workflow
| Tool | Purpose |
|------|---------|
| `generate_upgrade_readiness_report` | Full upgrade assessment: combines all data sources into a unified readiness report with rating (READY / READY WITH CAVEATS / NOT READY) |
| `write_checklist_excel` | Export checklist data to Excel file |

### Authentication
| Tool | Purpose |
|------|---------|
| `cmc_login` | Authenticate with CMC via browser login portal |
| `cloud_portal_login` | Authenticate with Cloud Portal via OAuth |
| `test_datasource_connections` | Combined CMC + Analytics login, then test all datasource connections |

### Cluster Analysis
| Tool | Purpose |
|------|---------|
| `extract_cluster_metadata_tool` | Auto-detect deployment type, DB, topology, features, risk level from CMC data |
| `run_pre_upgrade_validation` | Run 11 health checks (services, memory, topology, connectors, DB migration, datasource connectivity, etc.) |
| `get_cloud_metadata` | Fetch cloud instance metadata (versions, sizing, features, consumption, users) |

### Knowledge & Data
| Tool | Purpose |
|------|---------|
| `search_upgrade_knowledge` | Semantic search over Incorta docs, release notes, community articles |
| `get_zendesk_schema_tool` | Get Zendesk table schema for writing SQL queries |
| `query_upgrade_tickets` | Execute Spark SQL on Zendesk customer tickets |
| `get_jira_schema_tool` | Get Jira table schema for writing SQL queries |
| `query_upgrade_issues` | Execute Spark SQL on Jira bugs/features |

## Project Structure

```
Incorta-Upgrade-Assistant/
+-- server.py                           MCP server entry point (FastMCP, routes, tools)
+-- requirements.txt                    Python dependencies
+-- .env.example                        Environment config template
+-- Dockerfile                          Container deployment
|
+-- clients/
|   +-- cmc_client.py                   CMC API client (JWT auth, token caching)
|   +-- cloud_portal_client.py          Cloud Portal client (OAuth2 + PKCE, Auth0)
|
+-- tools/
|   +-- extract_cluster_metadata.py     Auto-detection: deployment, DB, topology, risk
|   +-- validation_checks.py           10 pre-upgrade health checks
|   +-- test_connection.py              Datasource connectivity testing
|   +-- incorta_tools.py                Zendesk/Jira login + SQL query execution
|   +-- qdrant_tool.py                  Vector search (BGE embeddings + Qdrant)
|   +-- jira_helpers.py                 Jira-specific query builders
|   +-- zendesk_helpers.py              Zendesk-specific query builders
|
+-- workflows/
|   +-- readiness_report.py             Main orchestration (all data sources -> report)
|   +-- pre_upgrade_validation.py       LangGraph health check workflow
|   +-- collect_zendesk_issues.py       Customer ticket analysis pipeline
|   +-- collect_jira_issues.py          Bug tracking analysis pipeline
|   +-- upgrade_research.py             Knowledge base research workflow
|   +-- checklist_workflow.py           Excel checklist generation
|
+-- context/
|   +-- user_context.py                 Thread-safe user credential context
|
+-- templates/
|   +-- pre_upgrade_checklist.xlsx       Excel template for checklist export
|
+-- scripts/
|   +-- cloud_login.py                  Standalone Cloud Portal login script
|   +-- test_cp_search.py               Cloud Portal search test
|
+-- tests/
    +-- test_cmc_client.py              CMC client tests
    +-- test_workflow.py                Workflow integration tests
    +-- test_metadata_extraction.py     Metadata extraction tests
    +-- test_enhanced_metadata.py       Enhanced metadata tests
```

## How Workflows Work

The server uses [LangGraph](https://github.com/langchain-ai/langgraph) to orchestrate multi-step operations as directed graphs. Each node is a function that takes state, performs an action (API call, data processing), and returns updated state.

**Example: Readiness Report Workflow**
```
1. Load CMC cluster data (deployment, DB, topology, features)
2. Load Cloud Portal data (versions, sizing, features, consumption)
3. Search knowledge base (release notes, upgrade guides)
4. Query Zendesk (customer-reported issues for upgrade path)
5. Query Jira (bugs linked to customer tickets + versions)
6. Run pre-upgrade validation (11 health checks)
7. Generate unified report with overall rating + recommendations
```

The MCP tool `generate_upgrade_readiness_report` wraps this entire workflow.

## Docker Deployment

```bash
docker build -t incorta-upgrade-assistant .
docker run -p 8080:8080 \
  -e CMC_URL=https://yourcluster.cloud2.incorta.com/cmc \
  -e CMC_USER=admin \
  -e CMC_PASSWORD=your-password \
  -e CMC_CLUSTER_NAME=customCluster \
  -e QDRANT_URL=https://your-qdrant.cloud \
  -e QDRANT_API_KEY=your-key \
  -v $(pwd)/data:/app/data \
  incorta-upgrade-assistant
```

**Note:** Docker runs in headless mode (`HEADLESS=true`). Cloud Portal OAuth requires either:
- A pre-obtained `CLOUD_PORTAL_TOKEN` passed as env var, or
- A persistent `MCP_PUBLIC_URL` domain registered in Auth0

## Connecting an MCP Client

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "incorta-upgrade-assistant": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8000/mcp/"],
      "env": {
        "CMC_URL": "https://yourcluster.cloud2.incorta.com/cmc",
        "CMC_USER": "admin",
        "CMC_PASSWORD": "your-password",
        "CMC_CLUSTER_NAME": "customCluster",
        "QDRANT_URL": "https://your-qdrant.cloud",
        "QDRANT_API_KEY": "your-key"
      }
    }
  }
}
```

### Any MCP Client

The server exposes a Streamable HTTP transport at `http://localhost:8000/mcp/`. Any MCP-compatible client can connect.

## Development

```bash
# Run individual tests
python tests/test_cmc_client.py
python tests/test_workflow.py

# Test a specific workflow
python workflows/upgrade_research.py

# Run the server with auto-reload (during development)
python server.py
```

## Key Dependencies

| Package | Purpose |
|---------|---------|
| `mcp` | MCP server framework (FastMCP) |
| `langgraph` | Workflow orchestration |
| `requests` | HTTP client for all API calls |
| `PyJWT` | JWT decoding for Auth0/CMC tokens |
| `qdrant-client` | Vector database for semantic search |
| `sentence-transformers` | BGE embeddings for RAG |
| `openpyxl` | Excel checklist generation |
| `python-dotenv` | Environment configuration |

## Notes

- The semantic search requires a Qdrant collection populated with scraped Incorta docs (collection: `docs2`, model: `BAAI/bge-base-en-v1.5`)
- Incorta SQL queries require the Zendesk (`ZendeskTickets`) and Jira (`Jira_F`) schemas to be configured in the Incorta Analytics instance
- Token caches (`~/.incorta_cmc_token.json`, `~/.incorta_cloud_token.json`) auto-expire and refresh — no manual rotation needed
