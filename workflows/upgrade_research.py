"""
Upgrade Research Workflow
Uses semantic search and Incorta queries to research upgrade paths.
"""

import os
import sys
from typing import TypedDict
from langgraph.graph import StateGraph, END

# add parent to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.qdrant_tool import search_knowledge_base


class UpgradeResearchState(TypedDict):
    from_version: str
    to_version: str
    docs_results: list
    known_issues: list
    community_insights: list
    report: str


# --- Workflow Nodes ---

def search_release_notes(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 1: Search for release notes and upgrade guides"""
    query = f"upgrade from {state['from_version']} to {state['to_version']} release notes"
    
    result = search_knowledge_base({"query": query, "limit": 10})
    
    return {**state, "docs_results": result.get("results", [])}


def search_known_issues(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 2: Search for known upgrade issues"""
    query = f"{state['to_version']} upgrade known issues problems"
    
    result = search_knowledge_base({"query": query, "limit": 8})
    
    return {**state, "known_issues": result.get("results", [])}


def search_community_experiences(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 3: Search for community upgrade experiences"""
    query = f"upgrade {state['from_version']} to {state['to_version']} experience tips"
    
    result = search_knowledge_base({"query": query, "limit": 5})
    
    return {**state, "community_insights": result.get("results", [])}


def synthesize_research(state: UpgradeResearchState) -> UpgradeResearchState:
    """Node 4: Combine all findings into a comprehensive report"""
    
    report_parts = []
    
    # Header
    report_parts.append(f"# Upgrade Path Research: {state['from_version']} → {state['to_version']}\n")
    
    # Official Documentation
    report_parts.append("## 📚 Official Documentation & Release Notes\n")
    if state['docs_results']:
        for i, doc in enumerate(state['docs_results'][:5], 1):
            report_parts.append(f"{i}. **[{doc['title']}]({doc['url']})**")
            report_parts.append(f"   - Score: {doc['score']:.3f}")
            report_parts.append(f"   - {doc.get('text', '')[:250]}...\n")
    else:
        report_parts.append("⚠️ No documentation found.\n")
    
    # Known Issues
    report_parts.append("\n## ⚠️ Known Issues & Considerations\n")
    if state['known_issues']:
        for i, issue in enumerate(state['known_issues'][:5], 1):
            report_parts.append(f"{i}. **[{issue['title']}]({issue['url']})**")
            report_parts.append(f"   - Score: {issue['score']:.3f}")
            report_parts.append(f"   - {issue.get('text', '')[:250]}...\n")
    else:
        report_parts.append("✅ No known issues found.\n")
    
    # Community Insights
    report_parts.append("\n## 💡 Community Experiences & Tips\n")
    if state['community_insights']:
        for i, insight in enumerate(state['community_insights'][:5], 1):
            report_parts.append(f"{i}. **[{insight['title']}]({insight['url']})**")
            report_parts.append(f"   - Score: {insight['score']:.3f}")
            report_parts.append(f"   - {insight.get('text', '')[:250]}...\n")
    else:
        report_parts.append("No community discussions found.\n")
    
    # Summary
    report_parts.append("\n## 📋 Summary\n")
    report_parts.append(f"Total documents found: {len(state['docs_results'])}\n")
    report_parts.append(f"Total known issues: {len(state['known_issues'])}\n")
    report_parts.append(f"Community insights: {len(state['community_insights'])}\n")
    
    report = "\n".join(report_parts)
    
    return {**state, "report": report}


# --- Build the Workflow ---

def build_upgrade_research_workflow():
    """Build the LangGraph workflow for upgrade research"""
    workflow = StateGraph(UpgradeResearchState)
    
    # Add nodes
    workflow.add_node("search_docs", search_release_notes)
    workflow.add_node("search_issues", search_known_issues)
    workflow.add_node("search_community", search_community_experiences)
    workflow.add_node("synthesize", synthesize_research)
    
    # Define flow
    workflow.set_entry_point("search_docs")
    workflow.add_edge("search_docs", "search_issues")
    workflow.add_edge("search_issues", "search_community")
    workflow.add_edge("search_community", "synthesize")
    workflow.add_edge("synthesize", END)
    
    return workflow.compile()


def research_upgrade_path(from_version: str, to_version: str) -> str:
    """
    Run the upgrade research workflow.
    
    Args:
        from_version: Current Incorta version (e.g., "2024.1.0")
        to_version: Target Incorta version (e.g., "2024.7.0")
    
    Returns:
        Formatted markdown report with research findings
    """
    workflow = build_upgrade_research_workflow()
    
    initial_state = {
        "from_version": from_version,
        "to_version": to_version,
        "docs_results": [],
        "known_issues": [],
        "community_insights": [],
        "report": ""
    }
    
    result = workflow.invoke(initial_state)
    
    return result["report"]


# Example usage for testing
if __name__ == "__main__":
    # Test the workflow
    report = research_upgrade_path("2024.1.0", "2024.7.0")
    print(report)
