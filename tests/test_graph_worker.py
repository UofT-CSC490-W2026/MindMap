"""
Tests for workers/graph_worker.py

sklearn, numpy, services.llm_client, transformers, and torch are mocked in
sys.modules before the module is imported so the heavy ML deps are never loaded.
"""

import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Inject heavy-ML stubs before graph_worker is imported
# ---------------------------------------------------------------------------

_sklearn_mock = MagicMock()
_sklearn_cluster_mock = MagicMock()
_sklearn_feature_extraction_mock = MagicMock()
_sklearn_feature_extraction_text_mock = MagicMock()
_numpy_mock = MagicMock()
_llm_client_mock = MagicMock()
_transformers_mock = MagicMock()
_torch_mock = MagicMock()

sys.modules.setdefault("sklearn", _sklearn_mock)
sys.modules.setdefault("sklearn.cluster", _sklearn_cluster_mock)
sys.modules.setdefault("sklearn.feature_extraction", _sklearn_feature_extraction_mock)
sys.modules.setdefault("sklearn.feature_extraction.text", _sklearn_feature_extraction_text_mock)
sys.modules.setdefault("numpy", _numpy_mock)
sys.modules.setdefault("services.llm_client", _llm_client_mock)
sys.modules.setdefault("transformers", _transformers_mock)
sys.modules.setdefault("torch", _torch_mock)

from workers.graph_worker import (  # noqa: E402
    _normalize_json_list,
    _normalize_ids,
    _dedupe_edges,
    build_knowledge_graph,
    run_topic_clustering,
)


# ---------------------------------------------------------------------------
# _normalize_json_list
# ---------------------------------------------------------------------------

def test_normalize_json_list_none():
    assert _normalize_json_list(None) == []


def test_normalize_json_list_string_json():
    assert _normalize_json_list('[1, 2, 3]') == [1, 2, 3]


def test_normalize_json_list_invalid_string():
    assert _normalize_json_list('not json') == []


def test_normalize_json_list_already_list():
    assert _normalize_json_list([1, 2]) == [1, 2]


# ---------------------------------------------------------------------------
# _normalize_ids
# ---------------------------------------------------------------------------

def test_normalize_ids_mixed():
    # 1 -> 1, "2" -> 2, None -> skipped, "bad" -> skipped
    assert _normalize_ids([1, "2", None, "bad"]) == [1, 2]


# ---------------------------------------------------------------------------
# _dedupe_edges
# ---------------------------------------------------------------------------

def test_dedupe_edges_removes_duplicates():
    edges = [
        (1, 2, "SIMILAR", 0.9, None),
        (1, 2, "SIMILAR", 0.8, None),  # duplicate, lower strength
    ]
    result = _dedupe_edges(edges)
    assert len(result) == 1
    assert result[0][3] == 0.9  # keeps higher strength


def test_dedupe_edges_removes_self_loops():
    edges = [(1, 1, "SIMILAR", 1.0, None)]
    result = _dedupe_edges(edges)
    assert result == []


# ---------------------------------------------------------------------------
# build_knowledge_graph — empty paper set
# ---------------------------------------------------------------------------

def test_build_knowledge_graph_empty():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        # DESC TABLE GOLD_PAPER_RELATIONSHIPS
        [("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",), ("STRENGTH",), ("REASON",)],
        # existing edges SELECT
        [],
        # DESC TABLE SILVER_PAPERS for _fetch_papers
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        # papers SELECT — no papers
        [],
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.graph_worker.RelationshipClassifier", MagicMock()):
            result = build_knowledge_graph(paper_id=None)

    assert result["papers_processed"] == 0


# ---------------------------------------------------------------------------
# run_topic_clustering — no embeddings → skipped
# ---------------------------------------------------------------------------

def test_run_topic_clustering_no_embeddings():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        # DESC TABLE SILVER_PAPERS
        [("ID",), ("TITLE",), ("ABSTRACT",), ("EMBEDDING",)],
        # no embedded papers
        [],
    ]
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        result = run_topic_clustering(n_clusters=3)

    assert result["status"] == "skipped"


# ---------------------------------------------------------------------------
# _citation_targets — with ss_ids
# ---------------------------------------------------------------------------

def test_citation_targets_with_ss_ids():
    from workers.graph_worker import _citation_targets
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("SS_ID",)],  # DESC TABLE SILVER_PAPERS
        [(42,), (43,)],         # matching paper ids
    ]
    citations = [{"ss_paper_id": "abc123"}, {"ss_paper_id": "def456"}]
    result = _citation_targets(mock_cursor, citations)
    assert result == [42, 43]


def test_citation_targets_empty():
    from workers.graph_worker import _citation_targets
    mock_cursor = MagicMock()
    result = _citation_targets(mock_cursor, [])
    assert result == []


def test_citation_targets_no_ss_ids():
    from workers.graph_worker import _citation_targets
    mock_cursor = MagicMock()
    citations = [{"title": "Some paper"}]  # no ss_paper_id
    result = _citation_targets(mock_cursor, citations)
    assert result == []


# ---------------------------------------------------------------------------
# _bulk_merge_edges
# ---------------------------------------------------------------------------

def test_bulk_merge_edges_empty():
    from workers.graph_worker import _bulk_merge_edges
    mock_cursor = MagicMock()
    result = _bulk_merge_edges(mock_cursor, [])
    assert result == 0
    mock_cursor.execute.assert_not_called()


def test_bulk_merge_edges_with_edges():
    from workers.graph_worker import _bulk_merge_edges
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",), ("STRENGTH",), ("REASON",),
    ]
    edges = [(1, 2, "SIMILAR", 0.9, None), (1, 3, "CITES", 1.0, None)]
    result = _bulk_merge_edges(mock_cursor, edges)
    assert result == 2
    mock_cursor.execute.assert_called()


# ---------------------------------------------------------------------------
# build_knowledge_graph — with one paper that has similar_ids
# ---------------------------------------------------------------------------

def test_build_knowledge_graph_with_paper():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        # DESC TABLE GOLD_PAPER_RELATIONSHIPS
        [("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",), ("STRENGTH",), ("REASON",)],
        [],  # existing edges
        # DESC TABLE SILVER_PAPERS for _fetch_papers
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        # one paper row: id=1, citations=None, similar_ids=[2,3], conclusion="Some conclusion"
        [(1, None, "[2, 3]", "Some conclusion")],
        # DESC TABLE SILVER_PAPERS for sim_cols in loop
        [("ID",), ("CONCLUSION",)],
        # fetchone for target paper 2 conclusion
    ]
    mock_cursor.fetchone.return_value = None  # no conclusion for target papers
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.graph_worker.RelationshipClassifier", MagicMock()):
            with patch("workers.graph_worker._bulk_merge_edges", return_value=2) as mock_merge:
                result = build_knowledge_graph(paper_id=None)

    assert result["papers_processed"] == 1
