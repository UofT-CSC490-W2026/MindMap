# Feature: mindmap-serving-layer-refactor, Property 1
# Feature: mindmap-serving-layer-refactor, Property 2
"""Property-based tests for graph_service."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from hypothesis import given, settings
from hypothesis import strategies as st

from app.services.contracts import GraphExpandResponse, GraphNode, GraphResponse
from app.services import graph_service


def _make_snowflake_conn(paper_rows=None, edge_rows=None):
    """Build a mock Snowflake connection whose cursor returns configurable rows."""
    cursor = MagicMock()
    # fetchall is called multiple times: first for paper rows, then for edges
    cursor.fetchall.side_effect = [
        paper_rows if paper_rows is not None else [],
        edge_rows if edge_rows is not None else [],
    ]
    cursor.fetchone.return_value = (1,)  # paper exists check in expand_graph
    cursor.execute.return_value = None
    cursor.close.return_value = None

    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close.return_value = None
    return conn


def _paper_row(paper_id: int):
    """Return a minimal Silver+Bronze paper row tuple."""
    import json
    raw_payload = json.dumps({
        "authors": ["Author A"],
        "year": 2020,
        "citationCount": 10,
    })
    return (paper_id, f"Title {paper_id}", f"arxiv_{paper_id}", raw_payload, 1, "Cluster A", 5)


# ---------------------------------------------------------------------------
# Property 1: GraphResponse schema conformance
# Validates: Requirements 1.1, 1.5
# ---------------------------------------------------------------------------

@given(query=st.text(min_size=1, max_size=200))
@settings(max_examples=100)
def test_graph_response_schema_conformance(query):
    """For any non-empty query, query_graph returns a valid GraphResponse."""
    paper_id = 42
    search_return = [{"id": paper_id}]

    conn = _make_snowflake_conn(
        paper_rows=[_paper_row(paper_id)],
        edge_rows=[],
    )

    with patch(
        "app.services.graph_service.semantic_search.remote",
        new_callable=MagicMock,
        create=True,
    ) as mock_remote, patch(
        "app.services.graph_service.connect_to_snowflake",
        return_value=conn,
    ):
        mock_remote.aio = AsyncMock(return_value=search_return)

        result = asyncio.run(
            graph_service.query_graph(query)
        )

    # Must validate as GraphResponse
    validated = GraphResponse.model_validate(result.model_dump())
    assert validated.graph_id, "graph_id must be non-empty"
    assert isinstance(result.nodes, list)
    assert all(isinstance(n, GraphNode) for n in result.nodes)
    assert isinstance(result.links, list)
    assert result.meta is not None


# ---------------------------------------------------------------------------
# Property 2: GraphExpandResponse schema conformance
# Validates: Requirements 2.1, 2.4
# ---------------------------------------------------------------------------

@given(paper_id=st.integers(min_value=1))
@settings(max_examples=100)
def test_graph_expand_response_schema_conformance(paper_id):
    """For any valid paper_id, expand_graph returns a valid GraphExpandResponse."""
    neighbor_id = paper_id + 1000
    neighbor_return = [{"id": neighbor_id}]

    import json
    raw_payload = json.dumps({"authors": ["Author B"], "year": 2021, "citationCount": 5})

    # expand_graph calls fetchone (paper exists), fetchall (paper rows), fetchall (edges)
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)
    cursor.fetchall.side_effect = [
        [_paper_row(neighbor_id)],
        [],  # edges
    ]
    cursor.execute.return_value = None
    cursor.close.return_value = None

    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.close.return_value = None

    graph_id = "test-graph-id"

    with patch(
        "app.services.graph_service.get_related_papers.remote",
        new_callable=MagicMock,
        create=True,
    ) as mock_remote, patch(
        "app.services.graph_service.connect_to_snowflake",
        return_value=conn,
    ):
        mock_remote.aio = AsyncMock(return_value=neighbor_return)

        result = asyncio.run(
            graph_service.expand_graph(graph_id, paper_id)
        )

    validated = GraphExpandResponse.model_validate(result.model_dump())
    assert isinstance(result.new_nodes, list)
    assert isinstance(result.new_links, list)
    assert result.graph_id == graph_id
    assert result.paper_id == str(paper_id)
