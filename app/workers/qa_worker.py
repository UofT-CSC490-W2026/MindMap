"""Single-paper conversational QA worker grounded in retrieved chunks only."""

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

try:
    from app.config import DATABASE, app, openai_secret, rag_image, snowflake_secret, qualify_table
    from app.services.llm_client import LLMClient
    from app.workers.semantic_search_worker import retrieve_similar_chunks_local
    from app.utils import connect_to_snowflake
except ModuleNotFoundError:
    from config import DATABASE, app, openai_secret, rag_image, snowflake_secret, qualify_table
    from services.llm_client import LLMClient
    from workers.semantic_search_worker import retrieve_similar_chunks_local
    from utils import connect_to_snowflake

SCHEMA = "APP"

logger = logging.getLogger(__name__)

MAX_HISTORY_MESSAGES = 6
MAX_QA_CONTEXT_CHARS = 18000
UNRELATED_KEYWORDS = {
    "weather",
    "restaurant",
    "movie",
    "stock",
    "sports",
    "recipe",
    "vacation",
    "dating",
    "politics",
    "bitcoin",
}


def _qa_logs_table(database: str = DATABASE, schema: str = SCHEMA) -> str:
    return qualify_table("APP_QA_LOGS", database=database, schema=schema)


def _ensure_qa_logs_table(cur, database: str = DATABASE, schema: str = SCHEMA) -> None:
    table = _qa_logs_table(database=database, schema=schema)
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            log_id INT IDENTITY(1,1) PRIMARY KEY,
            session_id STRING NOT NULL,
            paper_id INT NOT NULL,
            role STRING NOT NULL,
            message TEXT NOT NULL,
            rewritten_query STRING,
            cited_chunk_ids VARIANT,
            created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
        """
    )


def _load_history(
    cur,
    session_id: str,
    paper_id: int,
    database: str = DATABASE,
    schema: str = SCHEMA,
    limit_messages: int = MAX_HISTORY_MESSAGES,
) -> List[Dict[str, Any]]:
    table = _qa_logs_table(database=database, schema=schema)
    cur.execute(
        f"""
        SELECT role, message, rewritten_query, cited_chunk_ids
        FROM {table}
        WHERE session_id = %s
          AND paper_id = %s
        ORDER BY created_at DESC, log_id DESC
        LIMIT %s
        """,
        (session_id, int(paper_id), int(limit_messages)),
    )
    rows = list(reversed(cur.fetchall()))
    history = []
    for role, message, rewritten_query, cited_chunk_ids in rows:
        if isinstance(cited_chunk_ids, str):
            try:
                cited_chunk_ids = json.loads(cited_chunk_ids)
            except json.JSONDecodeError:
                cited_chunk_ids = []
        history.append(
            {
                "role": role,
                "message": message,
                "rewritten_query": rewritten_query,
                "cited_chunk_ids": cited_chunk_ids or [],
            }
        )
    return history


def _store_message(
    cur,
    session_id: str,
    paper_id: int,
    role: str,
    message: str,
    rewritten_query: Optional[str] = None,
    cited_chunk_ids: Optional[List[int]] = None,
    database: str = DATABASE,
    schema: str = SCHEMA,
) -> None:
    table = _qa_logs_table(database=database, schema=schema)
    cur.execute(
        f"""
        INSERT INTO {table}
        (session_id, paper_id, role, message, rewritten_query, cited_chunk_ids)
        VALUES (%s, %s, %s, %s, %s, PARSE_JSON(%s))
        """,
        (
            session_id,
            int(paper_id),
            role,
            message,
            rewritten_query,
            json.dumps(cited_chunk_ids or []),
        ),
    )


def _format_history(history: List[Dict[str, Any]]) -> str:
    lines = []
    for item in history[-MAX_HISTORY_MESSAGES:]:
        lines.append(f"{item['role'].upper()}: {item['message']}")
    return "\n".join(lines)


def _looks_ambiguous(question: str) -> bool:
    lowered = (question or "").strip().lower()
    if not lowered:
        return False
    tokens = lowered.split()
    pronouns = {"it", "they", "this", "that", "these", "those", "he", "she"}
    return len(tokens) <= 8 or any(token in pronouns for token in tokens)


def _looks_unrelated(question: str) -> bool:
    lowered = (question or "").lower()
    return any(keyword in lowered for keyword in UNRELATED_KEYWORDS)


@app.function(image=rag_image, secrets=[snowflake_secret, openai_secret], timeout=60 * 12)
def answer_paper_question(
    paper_id: int,
    question: str,
    session_id: str | None = None,
    top_k: int = 6,
    model_name: str = "gpt-4o-mini",
    database: str = DATABASE,
    schema: str = SCHEMA,
) -> Dict[str, Any]:
    question = (question or "").strip()
    if not question:
        return {
            "status": "error",
            "paper_id": paper_id,
            "error": "empty_question",
        }

    if _looks_unrelated(question):
        refusal = "I can only answer questions about the selected research paper."
        return {
            "status": "refused",
            "paper_id": paper_id,
            "session_id": session_id,
            "answer": refusal,
            "cited_chunk_ids": [],
        }

    session_id = session_id or str(uuid.uuid4())
    conn = connect_to_snowflake(database=database, schema=schema)
    cur = conn.cursor()

    try:
        _ensure_qa_logs_table(cur, database=database, schema=schema)
        history = _load_history(cur, session_id=session_id, paper_id=paper_id, database=database, schema=schema)
        formatted_history = _format_history(history)

        rewritten_query = question
        if history and _looks_ambiguous(question):
            try:
                llm = LLMClient(model=model_name, max_tokens=900)
                rewritten_query = llm.rewrite_followup_question(
                    history=formatted_history,
                    question=question,
                )
            except Exception as e:
                logger.warning(f"Query rewrite failed for paper {paper_id}: {e}")
                rewritten_query = question

        logger.info(f"Paper {paper_id}: retrieving chunks for QA")
        retrieved_chunks = retrieve_similar_chunks_local(
            query_text=rewritten_query,
            top_k=top_k,
            paper_id=paper_id,
            score_threshold=0.2,
            max_context_chars=MAX_QA_CONTEXT_CHARS,
            database=database,
            schema=schema,
        )

        if not retrieved_chunks:
            answer = "The information is not available in the provided paper context."
            _store_message(cur, session_id, paper_id, "user", question, rewritten_query=rewritten_query, database=database, schema=schema)
            _store_message(cur, session_id, paper_id, "assistant", answer, cited_chunk_ids=[], database=database, schema=schema)
            conn.commit()
            return {
                "status": "ok",
                "paper_id": paper_id,
                "session_id": session_id,
                "rewritten_query": rewritten_query if rewritten_query != question else None,
                "answer": answer,
                "cited_chunk_ids": [],
                "chunks_used": 0,
            }

        chunk_ids = [chunk["chunk_id"] for chunk in retrieved_chunks]
        context_blocks = []
        for chunk in retrieved_chunks:
            context_blocks.append(
                f"[Chunk ID: {chunk['chunk_id']} | Type: {chunk['chunk_type']}]\n{chunk['chunk_text']}"
            )
        context = "\n\n".join(context_blocks)

        logger.info(
            f"Paper {paper_id}: answering QA with {len(retrieved_chunks)} chunks "
            f"(approx_chars={len(context)})"
        )

        llm = LLMClient(model=model_name, max_tokens=900)
        qa_result = llm.answer_grounded_question(
            question=rewritten_query,
            context=context,
            chunk_ids=chunk_ids,
            history=formatted_history,
            retry_count=2,
        )
        grounded_answer = qa_result["result"]
        valid_citations = [chunk_id for chunk_id in grounded_answer.cited_chunk_ids if chunk_id in set(chunk_ids)]

        _store_message(cur, session_id, paper_id, "user", question, rewritten_query=rewritten_query, database=database, schema=schema)
        _store_message(
            cur,
            session_id,
            paper_id,
            "assistant",
            grounded_answer.answer,
            cited_chunk_ids=valid_citations,
            database=database,
            schema=schema,
        )
        conn.commit()

        return {
            "status": "ok",
            "paper_id": paper_id,
            "session_id": session_id,
            "rewritten_query": rewritten_query if rewritten_query != question else None,
            "answer": grounded_answer.answer,
            "cited_chunk_ids": valid_citations,
            "chunks_used": len(retrieved_chunks),
        }
    except Exception as e:
        conn.rollback()
        logger.error(f"Paper {paper_id}: QA failed: {e}")
        return {
            "status": "error",
            "paper_id": paper_id,
            "session_id": session_id,
            "error": str(e)[:200],
        }
    finally:
        cur.close()
        conn.close()
