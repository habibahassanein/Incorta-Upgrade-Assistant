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
from workflows.upgrade_research import research_upgrade_path
from tools.qdrant_tool import search_knowledge_base
from tools.incorta_tools import query_zendesk, query_jira, get_zendesk_schema, get_jira_schema
from tools.extract_cluster_metadata import extract_cluster_metadata, format_metadata_report
from clients.cloud_portal_client import CloudPortalClient


app = Server("incorta-upgrade-agent")


@app.list_tools()
async def list_tools():
    return [
        # ==========================================
        # CUSTOMER UPGRADE RECOMMENDATION WORKFLOW
        # ==========================================
        # Use these tools in sequence for customer-specific upgrade analysis:
        # 1. run_pre_upgrade_validation (baseline health)
        # 2. search_upgrade_knowledge (Release Support Policy + version considerations)
        # 3. query_upgrade_tickets (customer's existing issues via Zendesk)
        # 4. query_upgrade_issues (linked Jira bugs and fixes)
        # 5. Synthesize into upgrade readiness report with risk assessment
        
        # === STEP 1: Cluster Health Baseline ===
        types.Tool(
            name="run_pre_upgrade_validation",
            description=(
                "🔍 [STEP 1 - ALWAYS RUN FIRST] Validates Incorta cluster health before upgrade. "
                "Performs comprehensive pre-upgrade health checks including: service status (Analytics, Loader), "
                "memory usage, node topology, infrastructure services (Spark, Zookeeper, DB), connectors, tenants, "
                "email configuration, and database migration status. "
                "\n\n"
                "💡 USE CASE: Run at the START of any upgrade conversation to baseline cluster health. "
                "Results identify BLOCKERS (critical issues that must be resolved before upgrade) and "
                "WARNINGS (issues to monitor). Store results for comparison with post-upgrade validation. "
                "\n\n"
                "📊 OUTPUT: Markdown report with ✅ Healthy, ⚠️ Warnings, ❌ Blockers sections. "
                "Use blockers to determine upgrade risk level (HIGH/MEDIUM/LOW)."
            ),
            inputSchema={
                "type": "object",
                "required": ["cluster_name"],
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "CMC cluster name (e.g., 'customCluster'). ⚠️ Use CMC name, not Cloud Portal name."
                    }
                }
            }
        ),
        
        # === NEW: Auto-Extract Cluster Metadata ===
        # IMPORTANT: This tool uses CMC API - requires CMC cluster name (not Cloud Portal name)
        types.Tool(
            name="extract_cluster_metadata",
            description=(
                "🤖 [AUTO-DETECTION] Automatically extracts upgrade-relevant metadata from cluster data. "
                "Eliminates need to ask user questions - infers everything from CMC cluster JSON! "
                "\n\n"
                "⚠️ CLUSTER NAMING: This tool uses the CMC API and requires the CMC cluster name. "
                "Example: If CMC shows 'customCluster', use that name (NOT the Cloud Portal name like 'habibascluster'). "
                "The CMC and Cloud Portal may use different names for the same physical cluster. "
                "If you get a 404 error, the cluster name is wrong for CMC - try 'get_cloud_metadata' to verify the Cloud Portal name. "
                "\n\n"
                "💡 AUTO-DETECTS (No User Input Needed): "
                "• Deployment Type: Cloud (GCP/AWS/Azure) vs On-Premises (from storage path) "
                "• Database Type: MySQL/Oracle/PostgreSQL + migration requirements "
                "• Topology: Typical (1 node) vs Clustered/Custom (2+ nodes), HA status "
                "• Features: Notebook, Spark, SQLi, Kyuubi enabled/disabled status "
                "• Infrastructure: Spark mode (K8s/External/Embedded), Zookeeper (External/Embedded) "
                "• Service Status: All service states (Analytics, Loader, Notebook, SQLi) "
                "• Connectors: List of enabled connectors "
                "• Risk Assessment: AUTO-CLASSIFY as HIGH/MEDIUM/LOW risk based on blockers "
                "\n\n"
                "🎯 USE CASE: Run IMMEDIATELY after 'run_pre_upgrade_validation' to auto-extract structured metadata. "
                "Use the extracted metadata to populate upgrade questionnaire automatically instead of asking user. "
                "\n\n"
                "⚙️ DETECTION LOGIC: "
                "• path='gs://' → Google Cloud | 's3://' → AWS | 'file://' → On-Prem "
                "• db_type='oracle' → Migration needed | 'mysql' → No migration "
                "• Node count: 1 → Typical | 2+ → Clustered "
                "• SPARK_MASTER_URL='k8s://' → Kubernetes Spark | else External/Embedded "
                "• Service status: Error/Stopped → BLOCKER | Running → Healthy "
                "• Node status: offline → BLOCKER (all nodes must be online) "
                "\n\n"
                "📊 OUTPUT: "
                "• JSON with structured metadata (deployment_type, database, topology, features, infrastructure, service_status, risks) "
                "• Formatted markdown report with all auto-detected information "
                "• Risk assessment: blockers (must fix), warnings (monitor), info (FYI) "
                "• Risk level: HIGH (has blockers), MEDIUM (has warnings), LOW (all healthy) "
                "\n\n"
                "✨ BENEFITS: "
                "• Zero user questions - 100% automated detection "
                "• Instant metadata extraction (<1 second) "
                "• Accurate (from API, not user input) "
                "• Use extracted data to pre-populate other tools/workflows"
            ),
            inputSchema={
                "type": "object",
                "required": ["cluster_name"],
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "CMC cluster name (e.g., 'customCluster'). ⚠️ IMPORTANT: Use the CMC cluster name, NOT the Cloud Portal name. This queries CMC_URL/api/v1/clusters/{name}. If unsure, ask the user for their CMC cluster name."
                    },
                    "format": {
                        "type": "string",
                        "enum": ["json", "markdown", "both"],
                        "description": "Output format: 'json' (structured data only), 'markdown' (readable report only), 'both' (default: both)"
                    }
                }
            }
        ),
        
        # === STEP 2: Version Path & Considerations ===
        types.Tool(
            name="research_upgrade_path",
            description=(
                "📚 [STEP 2 - VERSION RESEARCH] Research upgrade path between two Incorta versions. "
                "Uses semantic search to find release notes, known issues, and community experiences. "
                "\n\n"
                "⚠️ CRITICAL: For customer-specific recommendations, use 'search_upgrade_knowledge' multiple times instead "
                "to build a SEQUENTIAL path ordered by RELEASE DATE (not version number). "
                "Example: 2024.1.x (Jan) comes BEFORE 2024.7.x (Oct) even though 7 > 1. "
                "\n\n"
                "💡 USE THIS TOOL FOR: Quick research of a single upgrade path (non-customer-specific). "
                "Provides a comprehensive report with official docs, known issues, and community tips. "
                "\n\n"
                "💡 FOR CUSTOMER RECOMMENDATIONS: Instead, call 'search_upgrade_knowledge' for: "
                "1) 'Incorta Release Support Policy' (get chronological version list with dates) "
                "2) '[VERSION] release notes' for EACH version in path "
                "3) '[VERSION] upgrade considerations' for EACH version "
                "4) 'upgrade from [V1] to [V2]' for each transition "
                "5) '[VERSION] known issues' for target version "
                "\n\n"
                "📊 OUTPUT: Markdown report with documentation, known issues, and community insights."
            ),
            inputSchema={
                "type": "object",
                "required": ["from_version", "to_version"],
                "properties": {
                    "from_version": {
                        "type": "string",
                        "description": "Current Incorta version (e.g., '2024.1.0')"
                    },
                    "to_version": {
                        "type": "string",
                        "description": "Target Incorta version (e.g., '2024.7.0')"
                    }
                }
            }
        ),
        
        # === KNOWLEDGE BASE SEARCH ===
        types.Tool(
            name="search_upgrade_knowledge",
            description=(
                "🔎 [GRANULAR VERSION SEARCH] Search Incorta documentation, community, and support articles. "
                "Primary tool for building customer-specific upgrade recommendations. "
                "\n\n"
                "🎯 REQUIRED SEARCHES FOR CUSTOMER UPGRADES: "
                "1. START: 'Incorta Release Support Policy' → Get ALL versions with release dates, support end dates, EOL dates "
                "2. BUILD PATH: Identify ALL versions between current → target (ordered by release date, NOT version number) "
                "3. FOR EACH VERSION: Search '[VERSION] release notes', '[VERSION] upgrade considerations', '[VERSION] known issues' "
                "4. DEPENDENCIES: '[VERSION] python version', '[VERSION] spark version', '[VERSION] zookeeper requirements' "
                "5. TRANSITIONS: 'upgrade from [V1] to [V2]' for critical version jumps "
                "\n\n"
                "💡 CRITICAL VERSION ORDERING: "
                "- ALWAYS order by RELEASE DATE from Release Support Policy "
                "- 2024.1.0 (Jan 2024) → 2024.1.8 (Feb 2024) → 2024.7.0 (Oct 2024) → 2024.7.5 (Nov 2024) "
                "- Never assume 2024.7 comes before 2024.10 based on numbers alone! "
                "\n\n"
                "🚨 IDENTIFY CRITICAL TRANSITIONS: "
                "- Spark version changes (requires spark-upgrade-issues-detector.sh) "
                "- Python version changes (system update required) "
                "- Zookeeper SSL upgrades (configuration changes) "
                "- Database schema migrations (pre-upgrade validation required) "
                "- Major architecture changes (extended preparation) "
                "\n\n"
                "📊 OUTPUT: JSON with search results including title, URL, relevance score, and text snippets. "
                "Preserve exact version numbers, dates, and technical details for upgrade report."
            ),
            inputSchema={
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query (e.g., 'Incorta Release Support Policy', '2024.7.0 release notes', '2024.7.0 upgrade considerations')"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results to return (default: 10, use 5-10 for version-specific searches)",
                        "default": 10
                    }
                }
            }
        ),
        
        # === ZENDESK SCHEMA ===
        types.Tool(
            name="get_zendesk_schema",
            description=(
                "📋 [STEP 3A - BEFORE CUSTOMER TICKETS] Get Zendesk schema to understand available fields. "
                "Call this BEFORE querying customer tickets to see table structure. "
                "\n\n"
                "💡 KEY TABLES FOR UPGRADES: "
                "- **ticket** (44 cols): Main ticket data - id, subject, priority, status, organization_id, has_jira, created_at "
                "- **ticket_customfields_v** (15 cols): Severity, Deployment_Type, Release, Fixed_in, Escalation_Status, Fix_Versions "
                "- **Ticket_Current_Release** (2 cols): ticket_id, custom_field_value (current version) "
                "- **Ticket_Target_Release** (2 cols): ticket_id, custom_field_value (target version) "
                "- **organization** (15 cols): Customer data - id, name, region "
                "- **ticket_jira_links** (5 cols): Zendesk ↔ Jira linkage - ticket_id, issue_key, issue_id "
                "- **ticket_tags** (2 cols): ticket_id, tag (may contain version info) "
                "\n\n"
                "📊 OUTPUT: Schema with 57 tables, column names, and data types."
            ),
            inputSchema={
                "type": "object",
                "required": ["fetch_schema"],
                "properties": {
                    "fetch_schema": {
                        "type": "boolean",
                        "description": "Set to true to fetch schema"
                    }
                }
            }
        ),
        
        # === ZENDESK QUERIES ===
        types.Tool(
            name="query_upgrade_tickets",
            description=(
                "🎫 [STEP 3B - CUSTOMER ISSUES] Query Zendesk for customer-reported support tickets. "
                "Essential for customer-specific upgrade recommendations. Schema: ZendeskTickets "
                "\n\n"
                "🎯 CRITICAL QUERIES FOR UPGRADES: "
                "\n"
                "**1️⃣ CUSTOMER'S OPEN ISSUES (Blockers & Warnings):**\n"
                "```sql\n"
                "SELECT \n"
                "    t.id,\n"
                "    t.subject,\n"
                "    t.priority,\n"
                "    t.status,\n"
                "    t.has_jira,\n"
                "    tcf.Severity,\n"
                "    tcf.Escalation_Status,\n"
                "    tcf.Escalation_Level,\n"
                "    tcf.Fixed_in,\n"
                "    o.name AS customer_name,\n"
                "    t.created_at\n"
                "FROM ZendeskTickets.ticket t\n"
                "JOIN ZendeskTickets.organization o ON t.organization_id = o.id\n"
                "LEFT JOIN ZendeskTickets.ticket_customfields_v tcf ON t.id = tcf.ticket_id\n"
                "WHERE o.name LIKE '%[CUSTOMER_NAME]%'\n"
                "AND t.status IN ('open', 'pending', 'hold')\n"
                "ORDER BY t.priority DESC, t.created_at DESC\n"
                "LIMIT 50\n"
                "```\n"
                "\n"
                "**2️⃣ CUSTOMER'S CURRENT VERSION:**\n"
                "```sql\n"
                "SELECT DISTINCT\n"
                "    tcr.custom_field_value AS current_release,\n"
                "    o.name AS customer_name,\n"
                "    COUNT(t.id) AS ticket_count\n"
                "FROM ZendeskTickets.ticket t\n"
                "JOIN ZendeskTickets.organization o ON t.organization_id = o.id\n"
                "LEFT JOIN ZendeskTickets.Ticket_Current_Release tcr ON t.id = tcr.ticket_id\n"
                "WHERE o.name LIKE '%[CUSTOMER_NAME]%'\n"
                "AND tcr.custom_field_value IS NOT NULL\n"
                "GROUP BY tcr.custom_field_value, o.name\n"
                "ORDER BY ticket_count DESC\n"
                "LIMIT 10\n"
                "```\n"
                "\n"
                "**3️⃣ TICKETS WITH JIRA LINKS (Cross-reference bugs):**\n"
                "```sql\n"
                "SELECT \n"
                "    t.id AS ticket_id,\n"
                "    t.subject,\n"
                "    t.priority,\n"
                "    t.status,\n"
                "    tjl.issue_key AS jira_key,\n"
                "    tcf.Fixed_in,\n"
                "    tcf.Severity,\n"
                "    o.name AS customer_name\n"
                "FROM ZendeskTickets.ticket t\n"
                "JOIN ZendeskTickets.organization o ON t.organization_id = o.id\n"
                "JOIN ZendeskTickets.ticket_jira_links tjl ON t.id = tjl.ticket_id\n"
                "LEFT JOIN ZendeskTickets.ticket_customfields_v tcf ON t.id = tcf.ticket_id\n"
                "WHERE o.name LIKE '%[CUSTOMER_NAME]%'\n"
                "AND t.has_jira = 1\n"
                "ORDER BY t.priority DESC\n"
                "LIMIT 50\n"
                "```\n"
                "\n"
                "**4️⃣ HISTORICAL UPGRADE ISSUES:**\n"
                "```sql\n"
                "SELECT \n"
                "    t.id,\n"
                "    t.subject,\n"
                "    t.status,\n"
                "    t.priority,\n"
                "    tcr.custom_field_value AS from_version,\n"
                "    ttr.custom_field_value AS to_version,\n"
                "    o.name AS customer_name,\n"
                "    t.created_at\n"
                "FROM ZendeskTickets.ticket t\n"
                "JOIN ZendeskTickets.organization o ON t.organization_id = o.id\n"
                "LEFT JOIN ZendeskTickets.Ticket_Current_Release tcr ON t.id = tcr.ticket_id\n"
                "LEFT JOIN ZendeskTickets.Ticket_Target_Release ttr ON t.id = ttr.ticket_id\n"
                "WHERE o.name LIKE '%[CUSTOMER_NAME]%'\n"
                "AND (t.subject LIKE '%upgrade%' OR t.subject LIKE '%migration%')\n"
                "ORDER BY t.created_at DESC\n"
                "LIMIT 30\n"
                "```\n"
                "\n\n"
                "💡 CLASSIFICATION FOR RISK ASSESSMENT: "
                "- **Escalation_Status** = 'Escalated' + priority = 'urgent' → UPGRADE BLOCKER ❌ "
                "- **Severity** = 'Critical' or 'High' → WARNING ⚠️ "
                "- **Fixed_in** field populated → May be resolved in target version ✅ "
                "- **has_jira** = 1 → Cross-reference with Jira to check fix status "
                "\n\n"
                "📊 OUTPUT: Query results with ticket data. Link to Jira via issue_key for fix tracking."
            ),
            inputSchema={
                "type": "object",
                "required": ["spark_sql"],
                "properties": {
                    "spark_sql": {
                        "type": "string",
                        "description": "Spark SQL query to execute on ZendeskTickets schema"
                    }
                }
            }
        ),
        
        # === JIRA SCHEMA ===
        types.Tool(
            name="get_jira_schema",
            description=(
                "📋 [STEP 4A - BEFORE JIRA QUERIES] Get Jira schema to understand available fields. "
                "Call this BEFORE querying bugs/features to see table structure. "
                "\n\n"
                "💡 KEY TABLES FOR UPGRADES: "
                "- **Issues** (324 cols): Main issue data - Key, Summary, StatusName, PriorityName, Customer, Organizations, "
                "  ZendeskTicketStatus, Target_Version_s_, Fix_Version_s_, Supported_Versions, ResolutionDate "
                "- **IssueFixVersions** (10 cols): Which versions fix each issue - IssueKey, Name (version), Released, ReleaseDate "
                "- **IssueAffectedVersions** (8 cols): Versions affected by bugs - IssueKey, Name (version), ReleaseDate "
                "- **IssueLinks** (11 cols): Issue relationships - InwardIssueKey, OutwardIssueKey, TypeName "
                "- **IssueComponents** (7 cols): Product areas - Name, Description "
                "\n\n"
                "📊 OUTPUT: Schema with 32 tables, column names, and data types."
            ),
            inputSchema={
                "type": "object",
                "required": ["fetch_schema"],
                "properties": {
                    "fetch_schema": {
                        "type": "boolean",
                        "description": "Set to true to fetch schema"
                    }
                }
            }
        ),
        
        # === JIRA QUERIES ===
        types.Tool(
            name="query_upgrade_issues",
            description=(
                "🐛 [STEP 4B - BUG TRACKING & FIXES] Query Jira for engineering bugs, features, and fixes. "
                "Critical for determining if customer's issues are fixed in target version. Schema: Jira_F "
                "\n\n"
                "🎯 CRITICAL QUERIES FOR UPGRADES: "
                "\n"
                "**1️⃣ CUSTOMER-LINKED ISSUES (What bugs affect this customer?):**\n"
                "```sql\n"
                "SELECT \n"
                "    i.Key AS issue_key,\n"
                "    i.Summary,\n"
                "    i.IssueTypeName,\n"
                "    i.StatusName,\n"
                "    i.PriorityName,\n"
                "    i.Customer,\n"
                "    i.Organizations,\n"
                "    i.ZendeskTicketStatus,\n"
                "    i.Target_Version_s_,\n"
                "    i.Fix_Version_s_,\n"
                "    i.ResolutionDate,\n"
                "    i.Created\n"
                "FROM Jira_F.Issues i\n"
                "WHERE (i.Customer LIKE '%[CUSTOMER_NAME]%' \n"
                "   OR i.Organizations LIKE '%[CUSTOMER_NAME]%')\n"
                "ORDER BY i.PriorityName DESC, i.Created DESC\n"
                "LIMIT 50\n"
                "```\n"
                "\n"
                "**2️⃣ BUGS FIXED IN TARGET VERSION (Will upgrade resolve issues?):**\n"
                "```sql\n"
                "SELECT \n"
                "    i.Key AS issue_key,\n"
                "    i.Summary,\n"
                "    i.IssueTypeName,\n"
                "    i.StatusName,\n"
                "    i.PriorityName,\n"
                "    ifv.Name AS fix_version,\n"
                "    ifv.Released,\n"
                "    ifv.ReleaseDate,\n"
                "    i.Customer,\n"
                "    i.ResolutionDate\n"
                "FROM Jira_F.Issues i\n"
                "JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey\n"
                "WHERE ifv.Name = '[TARGET_VERSION]'\n"
                "AND i.IssueTypeName = 'Bug'\n"
                "ORDER BY i.PriorityName DESC\n"
                "LIMIT 100\n"
                "```\n"
                "\n"
                "**3️⃣ KNOWN ISSUES IN TARGET VERSION (Will upgrade introduce bugs?):**\n"
                "```sql\n"
                "SELECT \n"
                "    i.Key AS issue_key,\n"
                "    i.Summary,\n"
                "    i.StatusName,\n"
                "    i.PriorityName,\n"
                "    iav.Name AS affected_version,\n"
                "    iav.ReleaseDate,\n"
                "    i.Created\n"
                "FROM Jira_F.Issues i\n"
                "JOIN Jira_F.IssueAffectedVersions iav ON i.Key = iav.IssueKey\n"
                "WHERE iav.Name = '[TARGET_VERSION]'\n"
                "AND i.StatusName IN ('Open', 'In Progress', 'Reopened')\n"
                "AND i.IssueTypeName = 'Bug'\n"
                "ORDER BY i.PriorityName DESC\n"
                "LIMIT 50\n"
                "```\n"
                "\n"
                "**4️⃣ CROSS-REFERENCE ZENDESK ↔ JIRA (Match tickets to bugs):**\n"
                "```sql\n"
                "SELECT \n"
                "    i.Key AS issue_key,\n"
                "    i.Summary,\n"
                "    i.StatusName,\n"
                "    i.PriorityName,\n"
                "    i.ZendeskTicketStatus,\n"
                "    i.Customer,\n"
                "    ifv.Name AS fix_version,\n"
                "    ifv.Released,\n"
                "    i.ResolutionDate\n"
                "FROM Jira_F.Issues i\n"
                "LEFT JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey\n"
                "WHERE i.ZendeskTicketStatus IS NOT NULL\n"
                "AND (i.Customer LIKE '%[CUSTOMER_NAME]%' OR i.Organizations LIKE '%[CUSTOMER_NAME]%')\n"
                "ORDER BY i.PriorityName DESC\n"
                "LIMIT 50\n"
                "```\n"
                "\n"
                "**5️⃣ UPGRADE-SPECIFIC ISSUES (Known upgrade problems):**\n"
                "```sql\n"
                "SELECT \n"
                "    i.Key,\n"
                "    i.Summary,\n"
                "    i.StatusName,\n"
                "    i.PriorityName,\n"
                "    i.Target_Version_s_,\n"
                "    i.Supported_Versions,\n"
                "    ifv.Name AS fix_version\n"
                "FROM Jira_F.Issues i\n"
                "LEFT JOIN Jira_F.IssueFixVersions ifv ON i.Key = ifv.IssueKey\n"
                "WHERE (i.Summary LIKE '%upgrade%' OR i.Summary LIKE '%migration%')\n"
                "AND (i.Supported_Versions LIKE '%[VERSION]%' \n"
                "   OR i.Target_Version_s_ LIKE '%[VERSION]%')\n"
                "ORDER BY i.PriorityName DESC\n"
                "LIMIT 50\n"
                "```\n"
                "\n\n"
                "💡 DECISION LOGIC: "
                "- If customer's Zendesk ticket has `has_jira=1` AND Jira issue has `fix_version=[TARGET]` → Issue will be RESOLVED ✅ "
                "- If target version has P0/P1 bugs in 'Open' status in IssueAffectedVersions → RECOMMEND DOT RELEASE "
                "- If customer has many issues with fix_version in interim version → RECOMMEND THAT VERSION as target "
                "- If issue has `ZendeskTicketStatus='closed'` but Jira `StatusName='Open'` → Bug reopened, needs investigation ⚠️ "
                "\n\n"
                "📊 OUTPUT: Query results with issue data. Cross-reference fix_version with customer's Zendesk tickets."
            ),
            inputSchema={
                "type": "object",
                "required": ["spark_sql"],
                "properties": {
                    "spark_sql": {
                        "type": "string",
                        "description": "Spark SQL query to execute on Jira_F schema"
                    }
                }
            }
        ),
        
        # === CLOUD PORTAL METADATA ===
        # IMPORTANT: This tool uses Cloud Portal API - requires Cloud Portal cluster name (not CMC name)
        types.Tool(
            name="get_cloud_metadata",
            description=(
                "☁️ [CLOUD-SPECIFIC DATA] Get cloud metadata from Cloud Portal API. "
                "Provides data NOT available in CMC API. Call AFTER extract_cluster_metadata. "
                "\n\n"
                "⚠️ CLUSTER NAMING: This tool uses the Cloud Portal API and requires the Cloud Portal cluster name. "
                "Example: If Cloud Portal UI shows 'habibascluster', use that name (NOT the CMC name like 'customCluster'). "
                "The CMC and Cloud Portal may use different names for the same physical cluster. "
                "If you get 'Cluster not found' error, verify the name in the Cloud Portal web UI. "
                "\n\n"
                "💡 NEW DATA AVAILABLE: "
                "• 💰 Consumption & Cost: Daily/monthly power units, usage trends "
                "• 👥 User Management: Authorized users, roles (owner/developer), last login times "
                "• 📦 Build Details: Custom build IDs, exact versions (Spark, Python) "
                "• 🚩 Feature Flags: MLflow, Chat/OpenAI, Delta Share, Data Agent status "
                "• 📈 Upgrade History: Last upgrade timestamp, upgrade patterns "
                "• 🔢 Instance UUID: Permanent cluster identifier "
                "• ⚙️ Spark Executors: Min/max executor configuration "
                "• 💾 Storage Operations: Detailed storage metrics "
                "• ☁️ Cloud Platform: GCP/AWS/Azure, region, zone, K8s cluster "
                "\n\n"
                "🎯 USE FOR UPGRADE DECISIONS: "
                "• Cost Impact: Estimate downtime cost based on consumption trends "
                "• User Coordination: Identify who to notify before upgrade "
                "• Version Validation: Check build compatibility, feature support "
                "• Upgrade Timing: Choose low-usage windows based on consumption patterns "
                "• Risk Assessment: Compare features enabled vs. target version requirements "
                "\n\n"
                "⚠️ AUTHENTICATION: Requires CLOUD_PORTAL_TOKEN environment variable (Bearer token). "
                "Token expires after ~24 hours."
            ),
            inputSchema={
                "type": "object",
                "required": ["cluster_name"],
                "properties": {
                    "cluster_name": {
                        "type": "string",
                        "description": "Cloud Portal cluster name (e.g., 'habibascluster'). ⚠️ IMPORTANT: Use the Cloud Portal name, NOT the CMC cluster name. This is the name shown in the cloudstaging.incortalabs.com web UI. If unsure, ask the user for their Cloud Portal cluster name."
                    },
                    "include_consumption": {
                        "type": "boolean",
                        "description": "Include consumption/cost data (default: true)",
                        "default": True
                    },
                    "include_users": {
                        "type": "boolean",
                        "description": "Include authorized users (default: true)",
                        "default": True
                    }
                }
            }
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict):
    import json
    
    if name == "run_pre_upgrade_validation":
        cluster_name = arguments.get("cluster_name")
        if not cluster_name:
            return [types.TextContent(type="text", text="Error: cluster_name is required")]
        
        # run the workflow (blocking call wrapped for async)
        report = await asyncio.get_event_loop().run_in_executor(
            None, run_validation, cluster_name
        )
        return [types.TextContent(type="text", text=report)]
    
    elif name == "extract_cluster_metadata":
        cluster_name = arguments.get("cluster_name")
        output_format = arguments.get("format", "both")
        
        if not cluster_name:
            return [types.TextContent(type="text", text="Error: cluster_name is required")]
        
        try:
            # Fetch cluster data from CMC
            from clients.cmc_client import CMCClient
            client = CMCClient()
            cluster_data = client.get_cluster(cluster_name)
            
            # Extract metadata
            metadata = extract_cluster_metadata(cluster_data)
            
            # Format output based on requested format
            if output_format == "json":
                output = json.dumps(metadata, indent=2)
            elif output_format == "markdown":
                output = format_metadata_report(metadata)
            else:  # "both" is default
                markdown_report = format_metadata_report(metadata)
                json_data = json.dumps(metadata, indent=2)
                output = f"{markdown_report}\n\n---\n\n## Raw Metadata (JSON)\n\n```json\n{json_data}\n```"
            
            return [types.TextContent(type="text", text=output)]
        
        except Exception as e:
            error_msg = f"Error extracting cluster metadata: {str(e)}"
            return [types.TextContent(type="text", text=error_msg)]
    
    elif name == "research_upgrade_path":
        from_version = arguments.get("from_version")
        to_version = arguments.get("to_version")
        if not from_version or not to_version:
            return [types.TextContent(type="text", text="Error: from_version and to_version are required")]
        
        # run the workflow (blocking call wrapped for async)
        report = await asyncio.get_event_loop().run_in_executor(
            None, research_upgrade_path, from_version, to_version
        )
        return [types.TextContent(type="text", text=report)]
    
    elif name == "search_upgrade_knowledge":
        # Call semantic search
        result = search_knowledge_base(arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
    
    elif name == "get_zendesk_schema":
        result = get_zendesk_schema(arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
    
    elif name == "query_upgrade_tickets":
        result = query_zendesk(arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
    
    elif name == "get_jira_schema":
        result = get_jira_schema(arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
    
    elif name == "query_upgrade_issues":
        result = query_jira(arguments)
        return [types.TextContent(type="text", text=json.dumps(result, indent=2))]
    
    elif name == "get_cloud_metadata":
        cluster_name = arguments.get("cluster_name")
        include_consumption = arguments.get("include_consumption", True)
        include_users = arguments.get("include_users", True)
        
        if not cluster_name:
            return [types.TextContent(type="text", text="Error: cluster_name is required")]
        
        try:
            user_id = os.getenv("CLOUD_PORTAL_USER_ID")
            if not user_id:
                return [types.TextContent(
                    type="text",
                    text="Error: CLOUD_PORTAL_USER_ID environment variable not set"
                )]
            
            cloud_client = CloudPortalClient()
            clusters_info = cloud_client.get_clusters_info(user_id)
            
            our_cluster = None
            for item in clusters_info.get("instances", []):
                instance = item.get("instance", {})
                if instance.get("name") == cluster_name:
                    our_cluster = instance
                    break
            
            if not our_cluster:
                return [types.TextContent(
                    type="text",
                    text=f"Error: Cluster '{cluster_name}' not found in Cloud Portal"
                )]
            
            instance_uuid = our_cluster.get("id")
            
            report_lines = [
                f"# Cloud Portal Metadata: {cluster_name}",
                "",
                "## Instance Details",
                f"- **UUID:** `{instance_uuid}`",
                f"- **Build:** {our_cluster.get('customBuild')} ({our_cluster.get('customBuildID')})",
                f"- **Platform:** {our_cluster.get('platform')} - {our_cluster.get('region')}/{our_cluster.get('zone')}",
                f"- **K8s Cluster:** {our_cluster.get('k8sClusterCode')}",
                f"- **Status:** {our_cluster.get('status')}",
                "",
                "## Software Versions",
                f"- **Spark:** {our_cluster.get('incortaSparkVersion')}",
                f"- **Python:** {our_cluster.get('pythonVersion')}",
                "",
            ]
            
            if include_consumption:
                try:
                    consumption = cloud_client.get_consumption(user_id, instance_uuid)
                    total_pu = consumption.get('consumptionAgg', {}).get('totalAgg', 0)
                    daily_data = consumption.get('consumptionAgg', {}).get('total', {}).get('daily', [])
                    
                    if daily_data:
                        avg_pu = sum(d.get('powerUnit', 0) for d in daily_data) / len(daily_data)
                        recent_7 = daily_data[-7:] if len(daily_data) >= 7 else daily_data
                        
                        report_lines.extend([
                            "## 💰 Consumption & Cost",
                            f"- **Total (This Month):** {total_pu:.6f} Power Units",
                            f"- **Daily Average:** {avg_pu:.6f} PU/day",
                            f"- **Estimated Downtime Cost (4h):** {(avg_pu / 24 * 4):.6f} PU",
                            "",
                            "**Recent Trend (Last 7 Days):**",
                        ])
                        
                        for day in recent_7:
                            date = day.get('startTime', 'Unknown')
                            pu = day.get('powerUnit', 0)
                            report_lines.append(f"  - {date}: {pu:.6f} PU")
                        
                        report_lines.append("")
                
                except Exception as e:
                    report_lines.extend([
                        "## 💰 Consumption & Cost",
                        f"⚠️ Could not fetch consumption data: {str(e)}",
                        ""
                    ])
            
            if include_users:
                try:
                    users_data = cloud_client.get_authorized_users(user_id, cluster_name)
                    users_list = users_data.get('authorizedUserRoles', [])
                    
                    report_lines.extend([
                        f"## 👥 Authorized Users ({len(users_list)} total)",
                        ""
                    ])
                    
                    for user_item in users_list:
                        user = user_item.get('user', {})
                        role = user_item.get('authorizedRoles', [{}])[0].get('role', 'unknown')
                        status = user_item.get('status', 'unknown')
                        email = user.get('email')
                        last_login = user.get('lastLoginAt', 'Never')
                        
                        report_lines.append(
                            f"- **{email}** ({role.capitalize()}) - Status: {status} - Last login: {last_login}"
                        )
                    
                    report_lines.append("")
                
                except Exception as e:
                    report_lines.extend([
                        "## 👥 Authorized Users",
                        f"⚠️ Could not fetch user data: {str(e)}",
                        ""
                    ])
            
            report_lines.extend([
                "## 🚩 Feature Flags",
                f"- **SQLi:** {'✅ Enabled' if our_cluster.get('sqliEnabled') else '❌ Disabled'}",
                f"- **Incorta X:** {'✅ Enabled' if our_cluster.get('incortaXEnabled') else '❌ Disabled'}",
                f"- **Data Agent:** {'✅ Enabled' if our_cluster.get('enableDataAgent') else '❌ Disabled'}",
                f"- **Chat/OpenAI:** {'✅ Enabled' if our_cluster.get('enableChat') else '❌ Disabled'}",
                f"- **MLflow:** {'✅ Enabled' if our_cluster.get('mlflowEnabled') else '❌ Disabled'}",
                f"- **Delta Share:** {'✅ Enabled' if our_cluster.get('enableDeltaShare') else '❌ Disabled'}",
                "",
                "## ⚙️ Spark Configuration",
                f"- **Min Executors:** {our_cluster.get('minExecutors', 'N/A')}",
                f"- **Max Executors:** {our_cluster.get('maxExecutors', 'N/A')}",
                "",
                "## 💾 Storage",
                f"- **Data Size:** {our_cluster.get('dsize')} GB",
                f"- **Loader Size:** {our_cluster.get('dsizeLoader')} GB",
                f"- **CMC Size:** {our_cluster.get('dsizeCmc')} GB",
                f"- **Available Disk:** {our_cluster.get('availableDisk')} GB",
                f"- **Consumed Data:** {our_cluster.get('consumedData')} GB",
                "",
                "## 📈 Upgrade History",
                f"- **Last Upgrade:** {our_cluster.get('initiatedUpgradeAt') or 'Never'}",
                f"- **Created:** {our_cluster.get('createdAt')}",
                f"- **Last Updated:** {our_cluster.get('updatedAt')}",
                f"- **Last Running:** {our_cluster.get('runningAt')}",
            ])
            
            report = "\n".join(report_lines)
            return [types.TextContent(type="text", text=report)]
        
        except Exception as e:
            error_msg = f"Error fetching Cloud Portal metadata: {str(e)}\n\nMake sure CLOUD_PORTAL_TOKEN is set and valid."
            return [types.TextContent(type="text", text=error_msg)]
    
    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    asyncio.run(main())
