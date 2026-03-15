
import os
import re
from typing import Dict, Any

from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer
from context.user_context import user_context


# Global resources (loaded once at module level)
_embedding_model = None
_qdrant_client = None


def get_embedding_model():
    """Lazy load embedding model."""
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = SentenceTransformer(
            "BAAI/bge-base-en-v1.5",
            device="cpu"
        )
    return _embedding_model


def get_qdrant_client():
    """Get or create Qdrant client."""
    global _qdrant_client
    if _qdrant_client is None:
        # Get credentials from context or environment
        ctx = user_context.get()
        qdrant_url = ctx.get("qdrant_url") or os.getenv("QDRANT_URL")
        qdrant_api_key = ctx.get("qdrant_api_key") or os.getenv("QDRANT_API_KEY", "")

        _qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)
    return _qdrant_client


def search_knowledge_base(arguments: Dict[str, Any]) -> dict:
    """
    Search the knowledge base using vector similarity.

    Knowledge base contains:
    - Incorta Community articles
    - Documentation
    - Support articles

    Args:
        query (str): Search query
        limit (int): Number of results to return (default: 5)

    Returns:
        dict: Search results with relevance scores
    """
    query = arguments["query"]
    limit = arguments.get("limit", 5)

    # Get clients
    embedding_model = get_embedding_model()
    qdrant_client = get_qdrant_client()

    # Encode query (bge models need instruction prefix for retrieval queries)
    query_vector = embedding_model.encode(
        ["Represent this sentence for searching relevant passages: " + query]
    )[0]

    # Extract version-like patterns (e.g., "2026", "2026.1.0") for filtered search
    version_patterns = re.findall(r'\b(20\d{2})\b', query)

    seen_ids = set()
    results = []

    # Phase 1: If query mentions a version/year, do a filtered search on title
    if version_patterns:
        for version in version_patterns:
            filtered_hits = qdrant_client.search(
                collection_name="docs2",
                query_vector=("Knowledge_Base", query_vector),
                query_filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="title",
                            match=models.MatchText(text=version),
                        )
                    ]
                ),
                limit=limit,
                with_payload=True,
            )
            for hit in filtered_hits:
                if hit.id not in seen_ids:
                    seen_ids.add(hit.id)
                    results.append({
                        "title": hit.payload.get("title", ""),
                        "url": hit.payload.get("url", ""),
                        "text": hit.payload.get("text", ""),
                        "score": hit.score,
                        "source": "knowledge_base"
                    })

    # Phase 2: Regular vector search to fill remaining slots
    remaining = limit - len(results)
    if remaining > 0:
        search_result = qdrant_client.search(
            collection_name="docs2",
            query_vector=("Knowledge_Base", query_vector),
            limit=remaining + len(seen_ids),
            with_payload=True
        )
        for hit in search_result:
            if hit.id not in seen_ids and len(results) < limit:
                seen_ids.add(hit.id)
                results.append({
                    "title": hit.payload.get("title", ""),
                    "url": hit.payload.get("url", ""),
                    "text": hit.payload.get("text", ""),
                    "score": hit.score,
                    "source": "knowledge_base"
                })

    return {
        "source": "knowledge_base",
        "results": results,
        "result_count": len(results)
    }
