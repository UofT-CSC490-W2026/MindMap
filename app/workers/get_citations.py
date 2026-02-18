import os
import requests
from typing import List, Dict, Any, Optional

S2_BASE = "https://api.semanticscholar.org/graph/v1"

def _s2_headers() -> Dict[str, str]:
    key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY")
    return {"x-api-key": key} if key else {}

def get_citations(paper_id: str, limit: int = 100) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch both references and citations for a paper_id using Semantic Scholar Graph API.
    Returns structured lists with minimal fields needed for graph building.
    """
    fields = "paperId,title,year,authors"
    # references = papers this paper cites
    ref_url = f"{S2_BASE}/paper/{paper_id}/references"
    cit_url = f"{S2_BASE}/paper/{paper_id}/citations"

    params = {"fields": fields, "limit": limit}

    refs = requests.get(ref_url, headers=_s2_headers(), params=params, timeout=30)
    refs.raise_for_status()
    cits = requests.get(cit_url, headers=_s2_headers(), params=params, timeout=30)
    cits.raise_for_status()

    ref_data = refs.json().get("data", [])
    cit_data = cits.json().get("data", [])

    def _norm_ref(item):
        p = item.get("citedPaper") or item.get("paper") or {}
        return {
            "paperId": p.get("paperId"),
            "title": p.get("title"),
            "year": p.get("year"),
            "authors": [a.get("name") for a in (p.get("authors") or [])],
        }

    def _norm_cit(item):
        p = item.get("citingPaper") or item.get("paper") or {}
        return {
            "paperId": p.get("paperId"),
            "title": p.get("title"),
            "year": p.get("year"),
            "authors": [a.get("name") for a in (p.get("authors") or [])],
        }

    return {
        "references": [_norm_ref(x) for x in ref_data],
        "citations": [_norm_cit(x) for x in cit_data],
    }
