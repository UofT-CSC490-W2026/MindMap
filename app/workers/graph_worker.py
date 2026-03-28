"""
Build Gold layer relationships (citations + similarity) from Silver layer.
"""
import json
from typing import Iterable, List, Optional, Tuple

try:
    from app.config import app, image, snowflake_secret, DATABASE, qualify_table
    from app.config import clustering_image, llm_image, openai_secret, APP_DIR
    from app.utils import connect_to_snowflake
except ModuleNotFoundError:
    # Support direct execution paths such as:
    # modal run app/workers/graph_worker.py::build_knowledge_graph
    from config import app, image, snowflake_secret, DATABASE, qualify_table
    from config import clustering_image, llm_image, openai_secret, APP_DIR
    from utils import connect_to_snowflake


def _silver_table(database: str = DATABASE) -> str:
    return qualify_table("SILVER_PAPERS", database=database)


def _gold_table(database: str = DATABASE) -> str:
    return qualify_table("GOLD_CONNECTIONS", database=database)


def _quote_ident(identifier: str) -> str:
    escaped = str(identifier).replace('"', '""')
    return f'"{escaped}"'


def _resolve_table_columns(cur, table_name: str) -> dict[str, str]:
    cur.execute(f"DESC TABLE {table_name}")
    columns = [row[0] for row in cur.fetchall() if row and row[0]]
    return {str(name).lower(): _quote_ident(str(name)) for name in columns}


def _require_columns(column_map: dict[str, str], required: list[str], table_name: str) -> dict[str, str]:
    missing = [name for name in required if name not in column_map]
    if missing:
        raise RuntimeError(f"Missing required columns in {table_name}: {missing}")
    return {name: column_map[name] for name in required}

# def _fetch_papers(cur, paper_id: Optional[int], database: str = DATABASE) -> List[Tuple[int, object, object]]:
#     # identify the silver table name based on the current environment (dev/prod)
#     silver = _silver_table(database=database)
    
#     # validate that the required columns for graph building exist in the snowflake schema
#     cols = _require_columns(
#         _resolve_table_columns(cur, silver),
#         ["id", "citation_list", "similar_embeddings_ids"],
#         silver,
#     )
    
#     # branch logic: fetch a single specific paper or scan the entire table
#     if paper_id is not None:
#         # retrieve connectivity data for a targeted update of a single node
#         # this is for use cases like: after ingesting and embedding a new paper, we want to quickly update the graph with its relationships without waiting for a full graph rebuild
#         cur.execute(
#             f'SELECT {cols["id"]} AS id, {cols["citation_list"]} AS citation_list, {cols["similar_embeddings_ids"]} AS similar_embeddings_ids FROM {silver} WHERE {cols["id"]} = %s',
#             (int(paper_id),),
#         )
#     else:
#         # batch mode: fetch all papers that have at least one type of relationship to map
#         # this adds edges for all papers that have cached relationships, and is intended to be run periodically to keep the graph up to date as new papers are ingested and embedded
#         cur.execute(
#             f"""
#             SELECT {cols["id"]} AS id, {cols["citation_list"]} AS citation_list, {cols["similar_embeddings_ids"]} AS similar_embeddings_ids
#             FROM {silver}
#             WHERE {cols["citation_list"]} IS NOT NULL OR {cols["similar_embeddings_ids"]} IS NOT NULL
#             """
#         )
        
#     # return the result set as a list of tuples for the graph worker to iterate through
#     return cur.fetchall()

def _fetch_papers(cur, paper_id: Optional[int], database: str = DATABASE) -> List[Tuple]:
    # Profiled because: DESC TABLE is called to resolve column names on every
    # invocation, then the SELECT pulls conclusion text for every paper —
    # fetching large text columns for the full corpus is the heaviest query
    # in build_knowledge_graph before any edge logic runs.

    silver = _silver_table(database=database)
    col_map = _resolve_table_columns(cur, silver)
    cols = _require_columns(
        col_map,
        ["id", "similar_embeddings_ids", "conclusion"],
        silver,
    )
    references_col = col_map.get("reference_list")
    citations_col = col_map.get("citation_list")
    if not references_col and not citations_col:
        raise RuntimeError(
            f"Missing required citation source columns in {silver}: "
            "expected at least one of ['reference_list', 'citation_list']."
        )
    if references_col and citations_col:
        citation_source_expr = f"COALESCE({references_col}, {citations_col})"
        citation_filter_expr = f"{references_col} IS NOT NULL OR {citations_col} IS NOT NULL"
    else:
        only_col = references_col or citations_col
        citation_source_expr = str(only_col)
        citation_filter_expr = f"{only_col} IS NOT NULL"

    query = (
        f'SELECT {cols["id"]}, {citation_source_expr}, '
        f'{cols["similar_embeddings_ids"]}, {cols["conclusion"]} FROM {silver}'
    )

    if paper_id is not None:
        cur.execute(f"{query} WHERE {cols['id']} = %s", (int(paper_id),))
    else:
        cur.execute(
            f"{query} WHERE {citation_filter_expr} OR {cols['similar_embeddings_ids']} IS NOT NULL"
        )

    return cur.fetchall()

def _normalize_json_list(value) -> list:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    return value if isinstance(value, list) else []


def _normalize_ids(value) -> List[int]:
    ids = []
    for item in _normalize_json_list(value):
        try:
            ids.append(int(item))
        except (TypeError, ValueError):
            continue
    return ids


def _citation_targets(cur, citations: Iterable[dict], database: str = DATABASE) -> List[int]:
    ss_ids: List[str] = []
    arxiv_ids: List[str] = []
    dois: List[str] = []
    for citation in citations:
        if not isinstance(citation, dict):
            continue
        ss_id = citation.get("ss_paper_id")
        if ss_id:
            ss_ids.append(str(ss_id))
        arxiv_id = citation.get("arxiv_id")
        if arxiv_id:
            arxiv_ids.append(str(arxiv_id))
        doi = citation.get("doi")
        if doi:
            dois.append(str(doi).lower())

    if not ss_ids and not arxiv_ids and not dois:
        print("[graph][citation_targets] no identifiers found in citation payload")
        return []

    silver = _silver_table(database=database)
    col_map = _resolve_table_columns(cur, silver)
    cols = _require_columns(col_map, ["id"], silver)
    targets: set[int] = set()
    ss_matches = 0
    arxiv_matches = 0
    doi_matches = 0

    print(
        "[graph][citation_targets] identifiers extracted: "
        f"ss_ids={len(ss_ids)} arxiv_ids={len(arxiv_ids)} dois={len(dois)} "
        f"sample_ss={ss_ids[:3]} sample_arxiv={arxiv_ids[:3]} sample_doi={dois[:2]}"
    )

    if ss_ids and "ss_id" in col_map:
        values_sql = ", ".join(["(%s)"] * len(ss_ids))
        cur.execute(
            f"""
            WITH source_ss_ids(ss_id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT DISTINCT sp.{cols["id"]}
            FROM source_ss_ids src
            JOIN {silver} sp
              ON sp.{col_map["ss_id"]} = src.ss_id
            """,
            ss_ids,
        )
        rows = [int(row[0]) for row in cur.fetchall()]
        ss_matches = len(rows)
        targets.update(rows)

    if arxiv_ids and "arxiv_id" in col_map:
        values_sql = ", ".join(["(%s)"] * len(arxiv_ids))
        cur.execute(
            f"""
            WITH source_arxiv_ids(arxiv_id) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT DISTINCT sp.{cols["id"]}
            FROM source_arxiv_ids src
            JOIN {silver} sp
              ON sp.{col_map["arxiv_id"]} = src.arxiv_id
            """,
            arxiv_ids,
        )
        rows = [int(row[0]) for row in cur.fetchall()]
        arxiv_matches = len(rows)
        targets.update(rows)

    if dois and "doi" in col_map:
        values_sql = ", ".join(["(%s)"] * len(dois))
        cur.execute(
            f"""
            WITH source_dois(doi) AS (SELECT column1 FROM VALUES {values_sql})
            SELECT DISTINCT sp.{cols["id"]}
            FROM source_dois src
            JOIN {silver} sp
              ON LOWER(sp.{col_map["doi"]}) = src.doi
            """,
            dois,
        )
        rows = [int(row[0]) for row in cur.fetchall()]
        doi_matches = len(rows)
        targets.update(rows)

    resolved = list(targets)
    print(
        "[graph][citation_targets] match results: "
        f"ss={ss_matches} arxiv={arxiv_matches} doi={doi_matches} "
        f"resolved_total={len(resolved)} resolved_sample={resolved[:10]}"
    )
    return resolved


def _dedupe_edges(edges: Iterable[Tuple]) -> List[Tuple]:
    seen = {}
    for edge in edges:
        source_id, target_id, rel_type, strength = edge[0], edge[1], edge[2], edge[3]
        reason = edge[4] if len(edge) > 4 else None
        if source_id == target_id:
            continue
        key = (int(source_id), int(target_id), rel_type)
        if key not in seen or float(strength) > seen[key][0]:
            seen[key] = (float(strength), reason)
    return [(sid, tid, rel, strength, reason) for (sid, tid, rel), (strength, reason) in seen.items()]

def _bulk_merge_edges(cur, edges: List[Tuple], database: str = DATABASE) -> int:
    """
    Perform a high-performance bulk upsert of relationship edges into the GOLD layer.

    This function uses a SQL MERGE statement to atomically handle both the insertion 
    of new semantic/citation links and the updating of existing relationship strengths. 
    By batching all edges into a single database transaction, it minimizes network 
    overhead and Snowflake compute costs.

    Args:
        cur: An active Snowflake cursor object.
        edges: A list of tuples where each tuple represents (source_id, target_id, type, strength).
               - source_id (int): The internal ID of the originating paper.
               - target_id (int): The internal ID of the referenced/similar paper.
               - type (str): The relationship category (e.g., 'CITES', 'SIMILAR').
               - strength (float): A weight between 0.0 and 1.0 representing link confidence.
               ex. (123, 456, 'CITES', 1.0) or (123, 789, 'SIMILAR', 0.8)
        database (str): The name of the Snowflake database to target.

    Returns:
        int: The total count of edges processed and merged into the GOLD layer.

    Note:
        The function is idempotent; if an edge with the same source, target, and type 
        already exists, its strength will be updated to the latest value rather 
        than creating a duplicate row.
    """
    # return early if there are no new relationships to process to save a database trip
    if not edges:
        return 0

    # identify the correct gold-layer table based on the environment
    gold = _gold_table(database=database)

    # safety check: verify that the target table actually has the columns needed for graph edges
    col_map = _resolve_table_columns(cur, gold)
    cols = _require_columns(
        col_map,
        ["source_paper_id", "target_paper_id", "relationship_type", "strength"],
        gold,
    )
    has_reason = "reason" in col_map

    if has_reason:
        values_sql = ", ".join(["(%s, %s, %s, %s, %s)"] * len(edges))
        params = []
        for edge in edges:
            source_id, target_id, rel_type, strength = edge[0], edge[1], edge[2], edge[3]
            reason = edge[4] if len(edge) > 4 else None
            params.extend([source_id, target_id, rel_type, strength, reason])
        cur.execute(
            f"""
            MERGE INTO {gold} AS target
            USING (
                SELECT
                    column1 AS source_paper_id,
                    column2 AS target_paper_id,
                    column3 AS relationship_type,
                    column4 AS strength,
                    column5 AS reason
                FROM VALUES {values_sql}
            ) AS source
            ON target.{cols["source_paper_id"]} = source.source_paper_id
               AND target.{cols["target_paper_id"]} = source.target_paper_id
               AND target.{cols["relationship_type"]} = source.relationship_type
            WHEN MATCHED THEN
                UPDATE SET target.{cols["strength"]} = source.strength,
                           target.{col_map["reason"]} = source.reason
            WHEN NOT MATCHED THEN
                INSERT ({cols["source_paper_id"]}, {cols["target_paper_id"]}, {cols["relationship_type"]}, {cols["strength"]}, {col_map["reason"]})
                VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength, source.reason)
            """,
            params,
        )
    else:
        values_sql = ", ".join(["(%s, %s, %s, %s)"] * len(edges))
        params = []
        for edge in edges:
            source_id, target_id, rel_type, strength = edge[0], edge[1], edge[2], edge[3]
            params.extend([source_id, target_id, rel_type, strength])
        cur.execute(
            f"""
            MERGE INTO {gold} AS target
            USING (
                SELECT
                    column1 AS source_paper_id,
                    column2 AS target_paper_id,
                    column3 AS relationship_type,
                    column4 AS strength
                FROM VALUES {values_sql}
            ) AS source
            ON target.{cols["source_paper_id"]} = source.source_paper_id
               AND target.{cols["target_paper_id"]} = source.target_paper_id
               AND target.{cols["relationship_type"]} = source.relationship_type
            WHEN MATCHED THEN
                UPDATE SET target.{cols["strength"]} = source.strength
            WHEN NOT MATCHED THEN
                INSERT ({cols["source_paper_id"]}, {cols["target_paper_id"]}, {cols["relationship_type"]}, {cols["strength"]})
                VALUES (source.source_paper_id, source.target_paper_id, source.relationship_type, source.strength)
            """,
            params,
        )
    
    # return the count of processed edges for the orchestrator's telemetry report
    return len(edges)

@app.function(image=image, secrets=[snowflake_secret])
def build_knowledge_graph(paper_id: int = None, database: str = DATABASE):
    """
    Populate Gold layer with citation and semantic similarity relationships.
    If paper_id is None, process all papers with cached relationships.
    """

    # Connect to Snowflake GOLD schema
    conn = connect_to_snowflake(database=database, schema="GOLD")
    cur = conn.cursor()
    try:
        # Fetch existing edges from Gold to avoid re-running the classifier
        gold = _gold_table(database=database)
        gold_cols = _require_columns(
            _resolve_table_columns(cur, gold),
            ["source_paper_id", "target_paper_id", "relationship_type"],
            gold,
        )
        cur.execute(
            f"SELECT {gold_cols['source_paper_id']}, {gold_cols['target_paper_id']}, {gold_cols['relationship_type']} FROM {gold}"
        )
        existing_edges: set = {(int(r[0]), int(r[1]), r[2]) for r in cur.fetchall()}
        print(f"Found {len(existing_edges)} existing edges in Gold, will skip classifier for those.")

        # Fetch all relevant papers from the SILVER layer (optionally filter by paper_id)
        print(f"Fetching papers from SILVER to build graph relationships (paper_id={paper_id})...")
        papers = _fetch_papers(cur, paper_id, database=database)
        edges: List[Tuple[int, int, str, float]] = []  # List to accumulate all edges to be created

        print(f"Processing {len(papers)} papers from SILVER to build relationships in GOLD...")

        classify_queue: List[Tuple[int, int, str, str]] = []

        # Iterate over each paper and extract relationships
        for pid, citations, similar_ids, p_conclusion in papers:
            print("----------------------------------------")
            print(f"paper {pid}: {len(_normalize_json_list(citations))} citations, {len(_normalize_json_list(similar_ids))} similar papers")

            # For each citation, add a CITES edge from this paper to the cited paper
            citation_entries = _normalize_json_list(citations)
            citation_targets = _citation_targets(cur, citation_entries, database=database)
            print(
                f"[graph] paper {pid} citation_entries={len(citation_entries)} "
                f"resolved_targets={len(citation_targets)} targets={citation_targets[:10]}"
            )
            if citation_entries and not citation_targets:
                print(f"[graph][warn] paper {pid} has citations but no resolvable targets in SILVER")
            for target_id in citation_targets:
                edges.append((int(pid), target_id, "CITES", 1.0, None))

            # For each similar paper, add a SIMILAR edge with decreasing strength
            for idx, sim_id in enumerate(_normalize_ids(similar_ids)):
                # Strength decays with rank (first neighbor = 1.0, second = 0.9, ...)
                strength = max(0.0, 1.0 - (idx * 0.1))
                edges.append((int(pid), sim_id, "SIMILAR", strength, None))

            # SEMANTIC RELATIONSHIP LOGIC
            # Collect pairs that need classification (skip already-computed edges)
            sim_ids = _normalize_ids(similar_ids)
            silver = _silver_table(database)
            sim_cols = _require_columns(
                _resolve_table_columns(cur, silver),
                ["id", "conclusion"],
                silver,
            )
            for idx, target_id in enumerate(sim_ids[:3]):
                if any((int(pid), target_id, lbl) in existing_edges for lbl in ["SUPPORT", "CONTRADICT", "NEUTRAL"]):
                    print(f"Skipping classifier for paper {pid} vs {target_id} (edge already exists)")
                    continue
                cur.execute(
                    f'SELECT {sim_cols["conclusion"]} FROM {silver} WHERE {sim_cols["id"]} = %s',
                    (target_id,),
                )
                target_row = cur.fetchone()
                if target_row and target_row[0]:
                    classify_queue.append((int(pid), target_id, p_conclusion, target_row[0]))

        # Phase 1: Persist deterministic edges first so CITES/SIMILAR never depend on LLM success.
        base_edges = _dedupe_edges(edges)
        base_merged_count = _bulk_merge_edges(cur, base_edges, database=database)
        conn.commit()
        print(
            "Graph build phase 1 complete. "
            f"Deterministic edges merged into GOLD: {base_merged_count}"
        )

        semantic_merged_count = 0
        semantic_error = None

        # Phase 2: Best-effort semantic classification. Failures here should not roll back phase 1.
        if classify_queue:
            try:
                classifier = RelationshipClassifier()
                print("Initialized relationship classifier for semantic edge inference.")
                print(f"Running batch classification for {len(classify_queue)} pairs...")
                inputs = [(src_conc, tgt_conc) for _, _, src_conc, tgt_conc in classify_queue]
                results = list(classifier.classify.map(inputs))
                semantic_edges: List[Tuple[int, int, str, float, Optional[str]]] = []
                for (pid, target_id, _, _), (label, reason) in zip(classify_queue, results):
                    print(f"Classifier output for paper {pid} vs {target_id}: {label} — {reason}")
                    semantic_edges.append((pid, target_id, label, 1.0, reason))

                semantic_merged_count = _bulk_merge_edges(
                    cur, _dedupe_edges(semantic_edges), database=database
                )
                conn.commit()
                print(
                    "Graph build phase 2 complete. "
                    f"Semantic edges merged into GOLD: {semantic_merged_count}"
                )
            except Exception as e:
                conn.rollback()
                semantic_error = str(e)
                print(
                    "Graph build phase 2 failed; deterministic edges were already committed. "
                    f"semantic_error={semantic_error}"
                )

        total_merged = int(base_merged_count) + int(semantic_merged_count)
        return {
            "papers_processed": len(papers),
            "edges_merged": total_merged,
            "base_edges_merged": int(base_merged_count),
            "semantic_edges_merged": int(semantic_merged_count),
            "semantic_error": semantic_error,
        }
    finally:
        # Always close the cursor and connection
        cur.close()
        conn.close()


def _gold_clusters_table(database: str = DATABASE) -> str:
    return qualify_table("GOLD_PAPER_CLUSTERS", database=database)


def _ensure_gold_clusters_table(cur, database: str = DATABASE) -> str:
    """
    Ensure GOLD_PAPER_CLUSTERS exists for environments where Terraform hasn't created it yet.
    """
    clusters_table = _gold_clusters_table(database=database)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {clusters_table} (
            "paper_id" NUMBER,
            "cluster_id" NUMBER,
            "cluster_label" STRING,
            "cluster_name" STRING,
            "cluster_description" STRING
        )
        """
    )
    return clusters_table


@app.function(image=clustering_image.pip_install("openai"), secrets=[snowflake_secret, openai_secret])
def run_topic_clustering(n_clusters: int = 5, database: str = DATABASE):
    """
    Cluster papers by embedding similarity and label each cluster with
    top TF-IDF keywords from titles/abstracts. Writes results to GOLD_PAPER_CLUSTERS.
    """
    import json
    import numpy as np
    from sklearn.cluster import KMeans
    from sklearn.feature_extraction.text import TfidfVectorizer

    conn = connect_to_snowflake(database=database, schema="SILVER")
    cur = conn.cursor()
    try:
        silver = _silver_table(database=database)
        cols = _require_columns(
            _resolve_table_columns(cur, silver),
            ["id", "title", "abstract", "embedding"],
            silver,
        )

        cur.execute(
            f"SELECT {cols['id']}, {cols['title']}, {cols['abstract']}, {cols['embedding']} "
            f"FROM {silver} WHERE {cols['embedding']} IS NOT NULL"
        )
        rows = cur.fetchall()

        if not rows:
            print("No embedded papers found, skipping clustering.")
            return {"status": "skipped", "reason": "no embeddings"}

        ids, titles, abstracts, raw_embeddings = [], [], [], []
        for pid, title, abstract, emb in rows:
            ids.append(int(pid))
            titles.append(title or "")
            abstracts.append(abstract or "")
            # Snowflake returns VECTOR as a JSON string like "[0.1, 0.2, ...]"
            if isinstance(emb, str):
                emb = json.loads(emb)
            raw_embeddings.append(emb)

        X = np.array(raw_embeddings, dtype=np.float32)

        # Cap n_clusters to the number of papers available
        k = min(n_clusters, len(ids))
        print(f"Running K-Means with k={k} on {len(ids)} papers...")
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto")
        labels = kmeans.fit_predict(X)

        # Label each cluster using top TF-IDF terms from titles + abstracts
        cluster_texts = [""] * k
        for i, label in enumerate(labels):
            cluster_texts[label] += f" {titles[i]} {abstracts[i]}"

        vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words="english",
            token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z]{2,}\b",
        )
        tfidf_matrix = vectorizer.fit_transform(cluster_texts)
        feature_names = vectorizer.get_feature_names_out()

        cluster_labels = []
        for cluster_idx in range(k):
            top_indices = tfidf_matrix[cluster_idx].toarray()[0].argsort()[-3:][::-1]
            top_terms = ", ".join(feature_names[i] for i in top_indices)
            cluster_labels.append(top_terms)
            print(f"Cluster {cluster_idx} keywords: {top_terms}")

        # Generate human-readable cluster names and descriptions via LLM
        from services.llm_client import LLMClient
        llm = LLMClient(model="gpt-4o-mini")
        cluster_names = []
        cluster_descriptions = []
        for cluster_idx in range(k):
            sample_titles = [titles[i] for i, lbl in enumerate(labels) if lbl == cluster_idx][:5]
            titles_str = "\n".join(f"- {t}" for t in sample_titles)
            prompt = (
                f"You are summarizing a cluster of research papers for a knowledge graph interface.\n"
                f"Keywords: {cluster_labels[cluster_idx]}\n"
                f"Sample titles:\n{titles_str}\n\n"
                f"Respond in this exact format:\n"
                f"NAME: <concise name, 4-6 words>\n"
                f"DESCRIPTION: <1-2 sentences describing what this research area is about, "
                f"what problem it solves, and where the field currently stands>"
            )
            response, _ = llm._call_openai(prompt, max_tokens=120, temperature=0.3)
            name, description = "", ""
            for line in response.strip().splitlines():
                if line.upper().startswith("NAME:"):
                    name = line.split(":", 1)[1].strip().strip('"')
                elif line.upper().startswith("DESCRIPTION:"):
                    description = line.split(":", 1)[1].strip()
            cluster_names.append(name or cluster_labels[cluster_idx])
            cluster_descriptions.append(description)
            print(f"Cluster {cluster_idx} name: {name}")
            print(f"Cluster {cluster_idx} description: {description}")

        # Bulk upsert into GOLD_PAPER_CLUSTERS
        gold_conn = connect_to_snowflake(database=database, schema="GOLD")
        gold_cur = gold_conn.cursor()
        try:
            clusters_table = _ensure_gold_clusters_table(gold_cur, database=database)
            cols_c = _require_columns(
                _resolve_table_columns(gold_cur, clusters_table),
                ["paper_id", "cluster_id", "cluster_label", "cluster_name", "cluster_description"],
                clusters_table,
            )
            values_sql = ", ".join(["(%s, %s, %s, %s, %s)"] * len(ids))
            params = []
            for pid, label_idx in zip(ids, labels):
                params.extend([pid, int(label_idx), cluster_labels[int(label_idx)], cluster_names[int(label_idx)], cluster_descriptions[int(label_idx)]])

            gold_cur.execute(
                f"""
                MERGE INTO {clusters_table} AS target
                USING (
                    SELECT column1 AS paper_id, column2 AS cluster_id, column3 AS cluster_label, column4 AS cluster_name, column5 AS cluster_description
                    FROM VALUES {values_sql}
                ) AS source
                ON target.{cols_c["paper_id"]} = source.paper_id
                WHEN MATCHED THEN
                    UPDATE SET target.{cols_c["cluster_id"]} = source.cluster_id,
                               target.{cols_c["cluster_label"]} = source.cluster_label,
                               target.{cols_c["cluster_name"]} = source.cluster_name,
                               target.{cols_c["cluster_description"]} = source.cluster_description
                WHEN NOT MATCHED THEN
                    INSERT ({cols_c["paper_id"]}, {cols_c["cluster_id"]}, {cols_c["cluster_label"]}, {cols_c["cluster_name"]}, {cols_c["cluster_description"]})
                    VALUES (source.paper_id, source.cluster_id, source.cluster_label, source.cluster_name, source.cluster_description)
                """,
                params,
            )
            gold_conn.commit()
            print(f"Topic clustering complete. {len(ids)} papers assigned to {k} clusters.")
            return {"status": "ok", "papers_clustered": len(ids), "n_clusters": k, "cluster_labels": cluster_labels, "cluster_names": cluster_names}
        finally:
            gold_cur.close()
            gold_conn.close()
    finally:
        cur.close()
        conn.close()




# @app.function(image=image, secrets=[snowflake_secret])
# def classify_relationship(source_conclusion: str, target_conclusion: str) -> str:
#     """
#     LLM-as-a-judge to identify if Paper A supports or contradicts Paper B.
#     """
#     if not source_conclusion or not target_conclusion:
#         return "NEUTRAL"

#     prompt = f"""
#     Compare the following research findings:
#     Paper A: {source_conclusion}
#     Paper B: {target_conclusion}
    
#     Does Paper A SUPPORT, CONTRADICT, or is it NEUTRAL relative to Paper B?
#     Answer with ONLY the word: SUPPORT, CONTRADICT, or NEUTRAL.
#     """
#     # Logic to call your LLM (e.g., GPT-4o-mini or a quantized Llama-3) goes here
#     # For now, we return the label based on the model response.
#     return "NEUTRAL"

from modal import method, enter
import modal

inference_image = (
    modal.Image.debian_slim(python_version="3.10")
    .pip_install("transformers>=4.40.0", "accelerate", "bitsandbytes", "torch", "snowflake-connector-python")
    .add_local_file(APP_DIR / "config.py", remote_path="/root/config.py", copy=True)
    .add_local_file(APP_DIR / "utils.py", remote_path="/root/utils.py", copy=True)
    .add_local_dir(APP_DIR / "services", remote_path="/root/services", copy=True)
    .env({"PYTHONPATH": "/root"})
)

hf_secret = modal.Secret.from_name("huggingface-secret")

@app.cls(
    image=inference_image,
    gpu="A10G",
    scaledown_window=300,
    secrets=[snowflake_secret, hf_secret]
)
class RelationshipClassifier:
    @enter()
    def load_model(self):
        """Loads the quantized model into GPU memory once per container."""
        import torch
        from transformers import pipeline

        print("Loading Qwen2.5-7B-Instruct into GPU...")
        self.pipe = pipeline(
            "text-generation",
            model="Qwen/Qwen2.5-7B-Instruct",
            model_kwargs={
                "dtype": torch.float16,
            },
            device_map="auto",
        )

    @method()
    def classify(self, inputs: tuple) -> tuple:
        """Runs the actual 'LLM-as-a-judge' inference, returning (label, reason).
        Accepts a (source_conclusion, target_conclusion) tuple for use with .map().
        """
        source_conclusion, target_conclusion = inputs
        if not source_conclusion or not target_conclusion:
            return ("NEUTRAL", "")

        prompt = f"""Compare these research findings and respond in exactly this format:
LABEL: <SUPPORT, CONTRADICT, or NEUTRAL>
REASON: <one sentence explaining how they relate>

Paper A: {source_conclusion}
Paper B: {target_conclusion}"""

        messages = [{"role": "user", "content": prompt}]

        print("Running inference on the relationship classifier...")
        outputs = self.pipe(
            messages,
            max_new_tokens=60,
            max_length=None,
            temperature=0.1,
        )

        response = outputs[0]["generated_text"][-1]["content"].strip()
        print("response:", response)

        label = "NEUTRAL"
        reason = ""
        for line in response.splitlines():
            if line.upper().startswith("LABEL:"):
                val = line.split(":", 1)[1].strip().upper()
                if "SUPPORT" in val:
                    label = "SUPPORT"
                elif "CONTRADICT" in val:
                    label = "CONTRADICT"
            elif line.upper().startswith("REASON:"):
                reason = line.split(":", 1)[1].strip()

        return (label, reason)
