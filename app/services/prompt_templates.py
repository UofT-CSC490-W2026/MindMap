"""Reusable prompt templates for paper summarization."""

import json
from typing import Iterable, Mapping, Optional


def build_summary_extraction_prompt(context: str, prompt_version: str = "v1") -> str:
    """
    Build a prompt for structured paper summary extraction.
    
    Args:
        context: Paper context (title, abstract, chunks)
        prompt_version: Version identifier for reproducibility
        
    Returns:
        Complete prompt string ready for LLM
    """
    
    if prompt_version == "v1":
        return _build_summary_extraction_prompt_v1(context)
    else:
        raise ValueError(f"Unknown prompt version: {prompt_version}")


def _build_summary_extraction_prompt_v1(context: str) -> str:
    """
    Version 1: Simple, direct extraction prompt.
    Instructs the model to:
    - Use only provided context
    - Return valid JSON
    - Leave fields empty if unsupported
    - Be concise and technical
    """
    
    json_schema = {
        "research_question": "string or empty",
        "methods": "list of strings or empty",
        "main_claims": "list of strings or empty",
        "key_findings": "list of strings or empty",
        "limitations": "list of strings or empty",
        "conclusion": "string or empty",
    }
    
    return f"""You are an expert research paper analyzer. Your task is to extract structured information from the provided paper context.

INSTRUCTIONS:
1. Extract information ONLY from the provided paper content below.
2. Do NOT invent, assume, or add information not explicitly stated.
3. Return a valid JSON object matching the schema below.
4. For list fields, return a list of strings. Leave empty list [] if none are found.
5. For string fields, return a string or empty string "" if not found.
6. Be concise and technical in your extractions.
7. Focus on substance: avoid generic statements.

OUTPUT SCHEMA:
{json.dumps(json_schema, indent=2)}

PAPER CONTENT:
{context}

Return ONLY the JSON object, no additional text or explanation.
JSON:"""


def build_grounded_qa_prompt(
    question: str,
    context: str,
    chunk_ids: Iterable[int],
    history: str = "",
    prompt_version: str = "v1",
) -> str:
    if prompt_version != "v1":
        raise ValueError(f"Unknown prompt version: {prompt_version}")

    output_schema = {
        "answer": "string",
        "cited_chunk_ids": [1, 2],
    }

    history_block = f"\nRECENT CONVERSATION:\n{history}\n" if history else ""
    return f"""You are answering questions about a single research paper.

RULES:
1. Use ONLY the provided retrieved chunks as evidence.
2. Do NOT use outside knowledge, prior assumptions, or unstated facts.
3. If the answer is not supported by the chunks, say the information is not available in the provided paper context.
4. If the user asks something unrelated to the paper, politely refuse and say you can only answer questions about this paper.
5. Cite only chunk ids that directly support the answer.
6. Return valid JSON only.

OUTPUT SCHEMA:
{json.dumps(output_schema, indent=2)}

VALID CHUNK IDS:
{json.dumps(list(chunk_ids))}
{history_block}
QUESTION:
{question}

RETRIEVED PAPER CHUNKS:
{context}

Return ONLY the JSON object.
JSON:"""


def build_query_rewrite_prompt(history: str, question: str, prompt_version: str = "v1") -> str:
    if prompt_version != "v1":
        raise ValueError(f"Unknown prompt version: {prompt_version}")

    return f"""Rewrite the follow-up question into a standalone question about the same research paper.

RULES:
1. Use only the conversation history below.
2. Preserve the user's intent.
3. Do not add new facts.
4. Return only the rewritten standalone question as plain text.

CONVERSATION HISTORY:
{history}

FOLLOW-UP QUESTION:
{question}

STANDALONE QUESTION:"""


def format_chunk_context(chunks: Iterable[Mapping[str, object]], max_chars: Optional[int] = None) -> str:
    parts = []
    current_chars = 0

    for chunk in chunks:
        chunk_id = chunk.get("chunk_id")
        chunk_type = chunk.get("chunk_type") or chunk.get("section_name") or "body"
        chunk_text = str(chunk.get("chunk_text") or "").strip()
        if not chunk_text:
            continue

        block = f"[Chunk ID: {chunk_id} | Type: {chunk_type}]\n{chunk_text}"
        block_len = len(block)
        if max_chars is not None and parts and current_chars + block_len > max_chars:
            break

        parts.append(block)
        current_chars += block_len + 2

    return "\n\n".join(parts)


def build_summary_refinement_prompt(
    summary: dict,
    context: str,
    feedback: str = None,
    prompt_version: str = "v1"
) -> str:
    """
    Build a prompt for refining an existing summary (future use).
    
    Args:
        summary: Existing summary to refine
        context: Paper context
        feedback: Optional feedback on what to improve
        prompt_version: Version identifier
        
    Returns:
        Refinement prompt string
    """
    # Placeholder for refinement logic in future versions
    raise NotImplementedError("Refinement prompt not yet implemented")
