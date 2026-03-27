"""
Tests for workers/graph_worker.py

sklearn, numpy, services.llm_client, transformers, and torch are mocked in
sys.modules before the module is imported so the heavy ML deps are never loaded.
"""

import sys
import pytest
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
    _fetch_papers,
    _require_columns,
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


def test_require_columns_raises():
    with pytest.raises(RuntimeError, match="Missing required columns"):
        _require_columns({"id": '"ID"'}, ["id", "missing"], "SILVER")


def test_fetch_papers_with_specific_paper_id():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        [(1, "[]", "[]", "Conclusion")],
    ]

    result = _fetch_papers(mock_cursor, paper_id=1)

    assert result == [(1, "[]", "[]", "Conclusion")]


def test_fetch_papers_raises_when_conclusion_column_missing():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [
        ("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",),
    ]

    with pytest.raises(RuntimeError, match="Missing required columns"):
        _fetch_papers(mock_cursor, paper_id=None)


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


def test_citation_targets_skips_non_dict_items():
    from workers.graph_worker import _citation_targets
    mock_cursor = MagicMock()
    result = _citation_targets(mock_cursor, [None, "bad"])
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


def test_build_knowledge_graph_runs_classifier_for_new_semantic_edges():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",), ("STRENGTH",), ("REASON",)],
        [],
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        [(1, None, "[2]", "Source conclusion")],
        [("ID",), ("CONCLUSION",)],
    ]
    mock_cursor.fetchone.return_value = ("Target conclusion",)
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    classifier = MagicMock()
    classifier.classify.map.return_value = [("SUPPORT", "Aligned findings")]

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.graph_worker.RelationshipClassifier", return_value=classifier):
            with patch("workers.graph_worker._bulk_merge_edges", return_value=2):
                result = build_knowledge_graph(paper_id=None)

    assert result["edges_merged"] == 2
    classifier.classify.map.assert_called_once()


def test_build_knowledge_graph_adds_citation_edges():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",)],
        [],
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        [(1, '[{"ss_paper_id": "ss-2"}]', "[]", "Source conclusion")],
        [("ID",), ("SS_ID",)],
        [(2,)],
        [("ID",), ("CONCLUSION",)],
    ]
    mock_cursor.fetchone.return_value = None
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    captured = {}

    def fake_merge(cur, edges, database="DB"):
        captured["edges"] = edges
        return len(edges)

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.graph_worker.RelationshipClassifier", return_value=MagicMock()):
            with patch("workers.graph_worker._bulk_merge_edges", side_effect=fake_merge):
                result = build_knowledge_graph(paper_id=None)

    assert result["edges_merged"] == 1
    assert captured["edges"][0][:3] == (1, 2, "CITES")


def test_build_knowledge_graph_skips_classifier_for_existing_semantic_edge():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",)],
        [(1, 2, "SUPPORT")],
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        [(1, None, "[2]", "Source conclusion")],
        [("ID",), ("CONCLUSION",)],
    ]
    mock_cursor.fetchone.return_value = ("Target conclusion",)
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    classifier = MagicMock()
    classifier.classify.map.return_value = [("SUPPORT", "Aligned findings")]

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.graph_worker.RelationshipClassifier", return_value=classifier):
            with patch("workers.graph_worker._bulk_merge_edges", return_value=1):
                result = build_knowledge_graph(paper_id=None)

    assert result["edges_merged"] == 1
    classifier.classify.map.assert_not_called()


def test_build_knowledge_graph_ignores_invalid_similar_ids():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.side_effect = [
        [("SOURCE_PAPER_ID",), ("TARGET_PAPER_ID",), ("RELATIONSHIP_TYPE",), ("STRENGTH",), ("REASON",)],
        [],
        [("ID",), ("CITATION_LIST",), ("SIMILAR_EMBEDDINGS_IDS",), ("CONCLUSION",)],
        [(1, None, '["bad", null, 2]', "Source conclusion")],
        [("ID",), ("CONCLUSION",)],
    ]
    mock_cursor.fetchone.return_value = None
    mock_cursor.execute.return_value = None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.commit.return_value = None

    captured = {}

    def fake_merge(cur, edges, database="DB"):
        captured["edges"] = edges
        return len(edges)

    with patch("workers.graph_worker.connect_to_snowflake", return_value=mock_conn):
        with patch("workers.graph_worker.RelationshipClassifier", return_value=MagicMock()):
            with patch("workers.graph_worker._bulk_merge_edges", side_effect=fake_merge):
                result = build_knowledge_graph(paper_id=None)

    assert result["edges_merged"] == 1
    assert captured["edges"][0][:3] == (1, 2, "SIMILAR")


def test_run_topic_clustering_success():
    import types

    mock_silver_cursor = MagicMock()
    mock_silver_cursor.fetchall.side_effect = [
        [("ID",), ("TITLE",), ("ABSTRACT",), ("EMBEDDING",)],
        [
            (1, "Paper One", "Topic alpha", "[0.1, 0.2]"),
            (2, "Paper Two", "Topic beta", [0.2, 0.3]),
        ],
    ]
    mock_silver_cursor.execute.return_value = None

    mock_gold_cursor = MagicMock()
    mock_gold_cursor.fetchall.return_value = [
        ("PAPER_ID",), ("CLUSTER_ID",), ("CLUSTER_LABEL",), ("CLUSTER_NAME",), ("CLUSTER_DESCRIPTION",),
    ]
    mock_gold_cursor.execute.return_value = None

    mock_silver_conn = MagicMock()
    mock_silver_conn.cursor.return_value = mock_silver_cursor
    mock_gold_conn = MagicMock()
    mock_gold_conn.cursor.return_value = mock_gold_cursor
    mock_gold_conn.commit.return_value = None

    fake_np = types.SimpleNamespace(array=lambda values, dtype=None: values, float32="float32")

    class FakeScores:
        def __init__(self, values):
            self.values = values

        def argsort(self):
            return sorted(range(len(self.values)), key=lambda idx: self.values[idx])

    class FakeRow:
        def __init__(self, values):
            self.values = values

        def toarray(self):
            return [FakeScores(self.values)]

    class FakeMatrix:
        def __getitem__(self, idx):
            return FakeRow([0.1 + idx, 0.2 + idx, 0.3 + idx])

    vectorizer_instance = MagicMock()
    vectorizer_instance.fit_transform.return_value = FakeMatrix()
    vectorizer_instance.get_feature_names_out.return_value = ["alpha", "beta", "gamma"]

    kmeans_instance = MagicMock()
    kmeans_instance.fit_predict.return_value = [0, 1]

    llm_instance = MagicMock()
    llm_instance._call_openai.side_effect = [
        ('NAME: Cluster A\nDESCRIPTION: Desc A', {}),
        ('NAME: Cluster B\nDESCRIPTION: Desc B', {}),
    ]

    with patch.dict(
        sys.modules,
        {
            "numpy": fake_np,
            "sklearn.cluster": types.SimpleNamespace(KMeans=MagicMock(return_value=kmeans_instance)),
            "sklearn.feature_extraction.text": types.SimpleNamespace(TfidfVectorizer=MagicMock(return_value=vectorizer_instance)),
            "services.llm_client": types.SimpleNamespace(LLMClient=MagicMock(return_value=llm_instance)),
        },
    ):
        with patch("workers.graph_worker.connect_to_snowflake", side_effect=[mock_silver_conn, mock_gold_conn]):
            result = run_topic_clustering(n_clusters=5)

    assert result["status"] == "ok"
    assert result["papers_clustered"] == 2
    assert result["n_clusters"] == 2


def test_relationship_classifier_methods_via_reloaded_module():
    import importlib
    import modal
    import workers.graph_worker as gw

    old_method = modal.method
    old_enter = modal.enter
    modal.method = lambda *args, **kwargs: (lambda fn: fn)
    modal.enter = lambda *args, **kwargs: (lambda fn: fn)

    try:
        gw = importlib.reload(gw)

        fake_pipeline = MagicMock(return_value="PIPE")
        fake_transformers = MagicMock()
        fake_transformers.pipeline = fake_pipeline
        fake_torch = MagicMock()
        fake_torch.float16 = "fp16"

        with patch.dict(sys.modules, {"transformers": fake_transformers, "torch": fake_torch}):
            classifier = gw.RelationshipClassifier()
            classifier.load_model()

        assert classifier.pipe == "PIPE"

        classifier.pipe = MagicMock(
            return_value=[
                {
                    "generated_text": [
                        {"content": "ignored"},
                        {"content": "LABEL: SUPPORT\nREASON: They agree."},
                    ]
                }
            ]
        )

        assert classifier.classify(("", "target")) == ("NEUTRAL", "")
        assert classifier.classify(("source", "target")) == ("SUPPORT", "They agree.")

        classifier.pipe = MagicMock(
            return_value=[
                {
                    "generated_text": [
                        {"content": "ignored"},
                        {"content": "LABEL: CONTRADICT\nREASON: They conflict."},
                    ]
                }
            ]
        )

        assert classifier.classify(("source", "target")) == ("CONTRADICT", "They conflict.")
    finally:
        modal.method = old_method
        modal.enter = old_enter
        importlib.reload(gw)
