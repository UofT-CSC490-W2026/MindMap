"""LLM client wrapper for structured and freeform research LLM calls."""

import json
import logging
import os
import time
from typing import Any, Optional, Type, TYPE_CHECKING

try:
    import httpx
except ModuleNotFoundError:
    class _HTTPXCompat:
        class TimeoutException(Exception):
            """Fallback timeout exception when httpx is unavailable."""
        Client = None

    httpx = _HTTPXCompat()

if TYPE_CHECKING:
    from pydantic import BaseModel
else:
    BaseModel = Any

try:
    from app.services.prompt_templates import (
        build_grounded_qa_prompt,
        build_query_rewrite_prompt,
        build_summary_extraction_prompt,
    )
except ModuleNotFoundError:
    from services.prompt_templates import (
        build_grounded_qa_prompt,
        build_query_rewrite_prompt,
        build_summary_extraction_prompt,
    )

logger = logging.getLogger(__name__)

# OpenAI API constants
OPENAI_API_URL = "https://api.openai.com/v1/chat/completions"
DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_TEMPERATURE = 0.3  # Low temperature for consistency
DEFAULT_MAX_TOKENS = 1500
DEFAULT_TIMEOUT_SECONDS = 45.0
DEFAULT_SYSTEM_PROMPT = (
    "You are a careful research assistant. Follow the user's grounding constraints "
    "exactly and never use information outside the provided context."
)


def _load_pydantic():
    try:
        from pydantic import ValidationError
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "pydantic is required for structured LLM workflows. "
            "Install it to use summary generation or grounded QA."
        ) from exc
    return ValidationError


def _load_structured_models():
    try:
        from app.services.summary_schema import PaperSummary
        from app.services.qa_schema import GroundedAnswer
    except ModuleNotFoundError:
        from services.summary_schema import PaperSummary
        from services.qa_schema import GroundedAnswer
    return PaperSummary, GroundedAnswer


class LLMClient:
    """
    OpenAI-based LLM client for structured and freeform research workflows.

    The model is selected at runtime so callers can evaluate different chat models
    without changing call sites.
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = DEFAULT_MODEL,
        temperature: float = DEFAULT_TEMPERATURE,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ):
        """
        Initialize LLM client.
        
        Args:
            api_key: OpenAI API key (if None, tries OPENAI_API_KEY env var)
            model: Chat model name used for requests
            temperature: Sampling temperature (default: 0.3)
            max_tokens: Max tokens in response (default: 1500)
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API key not provided. Set OPENAI_API_KEY environment variable or pass api_key."
            )
        if getattr(httpx, "Client", None) is None:
            raise ModuleNotFoundError("httpx is required to use LLMClient.")
        
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.timeout_seconds = timeout_seconds
        self.client = httpx.Client(
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=timeout_seconds,
        )
    
    def generate_structured_summary(
        self,
        context: str,
        title: Optional[str] = None,
        prompt_version: str = "v1",
        retry_count: int = 2,
    ) -> dict:
        """
        Generate a structured summary from paper context using LLM.
        
        Args:
            context: Paper context string (title, abstract, chunks)
            title: Paper title (used in logging)
            prompt_version: Prompt template version
            retry_count: Number of retry attempts on JSON parse failure
            
        Returns:
            PaperSummary object with extracted fields
            
        Raises:
            ValueError: If LLM response cannot be parsed after retries
        """
        PaperSummary, _ = _load_structured_models()
        prompt = build_summary_extraction_prompt(context, prompt_version=prompt_version)
        return self._generate_validated_json(
            prompt=prompt,
            response_model=PaperSummary,
            title=title,
            retry_count=retry_count,
        )

    def answer_grounded_question(
        self,
        question: str,
        context: str,
        chunk_ids: list[int],
        history: str = "",
        prompt_version: str = "v1",
        retry_count: int = 2,
    ) -> dict:
        _, GroundedAnswer = _load_structured_models()
        prompt = build_grounded_qa_prompt(
            question=question,
            context=context,
            chunk_ids=chunk_ids,
            history=history,
            prompt_version=prompt_version,
        )
        return self._generate_validated_json(
            prompt=prompt,
            response_model=GroundedAnswer,
            title="grounded_qa",
            retry_count=retry_count,
            max_tokens=min(self.max_tokens, 900),
        )

    def rewrite_followup_question(
        self,
        history: str,
        question: str,
        prompt_version: str = "v1",
    ) -> str:
        prompt = build_query_rewrite_prompt(history=history, question=question, prompt_version=prompt_version)
        response_text, _ = self._call_openai(prompt, max_tokens=200, temperature=0.1)
        return response_text.strip()

    def generate_text(
        self,
        prompt: str,
        system_prompt: str = DEFAULT_SYSTEM_PROMPT,
        retry_count: int = 1,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> dict:
        """
        Generate freeform text for experiment or analysis tasks.

        Returns a dictionary containing the generated text, usage metadata, and
        attempt count. Existing structured call sites remain unchanged.
        """
        attempts_allowed = max(1, int(retry_count) + 1)

        for attempt in range(1, attempts_allowed + 1):
            try:
                response_text, usage = self._call_openai(
                    prompt,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    system_prompt=system_prompt,
                )
                cleaned = (response_text or "").strip()
                if not cleaned:
                    raise ValueError("empty_response")
                return {
                    "text": cleaned,
                    "attempts": attempt,
                    "usage": usage,
                    "raw_response": response_text,
                }
            except (ValueError, httpx.TimeoutException) as e:
                if attempt < attempts_allowed:
                    logger.warning(
                        f"Freeform generation failed (attempt {attempt}/{attempts_allowed}): "
                        f"{str(e)[:160]}. Retrying..."
                    )
                    time.sleep(1)
                    continue
                raise

    def _generate_validated_json(
        self,
        prompt: str,
        response_model: Type[BaseModel],
        title: Optional[str],
        retry_count: int,
        max_tokens: Optional[int] = None,
    ) -> dict:
        ValidationError = _load_pydantic()
        attempts_allowed = max(1, int(retry_count) + 1)

        for attempt in range(1, attempts_allowed + 1):
            try:
                response, usage = self._call_openai(prompt, max_tokens=max_tokens)
                summary_dict = self._parse_json_response(response)
                parsed = response_model(**summary_dict)

                logger.info(
                    f"Validated LLM response for {title or 'unknown'} "
                    f"(attempt {attempt}/{attempts_allowed})"
                )
                return {
                    "result": parsed,
                    "attempts": attempt,
                    "usage": usage,
                    "raw_response": response,
                }

            except (json.JSONDecodeError, ValidationError, ValueError) as e:
                if attempt < attempts_allowed:
                    logger.warning(
                        f"Failed to validate LLM response for {title or 'unknown'} "
                        f"(attempt {attempt}/{attempts_allowed}): {str(e)[:140]}. Retrying..."
                    )
                    time.sleep(1)
                else:
                    logger.error(
                        f"Failed to validate LLM response for {title or 'unknown'} "
                        f"after {attempts_allowed} attempts: {str(e)[:200]}"
                    )
                    raise ValueError(f"Cannot validate LLM response: {str(e)}")
            except httpx.TimeoutException as e:
                logger.error(f"LLM timeout for {title or 'unknown'}: {str(e)[:160]}")
                raise TimeoutError(f"LLM request timed out: {str(e)}")
            except Exception as e:
                logger.error(
                    f"Error generating LLM output for {title or 'unknown'}: {str(e)}"
                )
                raise
    
    def _call_openai(
        self,
        prompt: str,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        system_prompt: str = DEFAULT_SYSTEM_PROMPT,
    ) -> tuple[str, dict]:
        """
        Make API call to OpenAI.
        
        Args:
            prompt: Complete prompt string
            
        Returns:
            Response text from LLM
            
        Raises:
            httpx.HTTPError: If API call fails
        """
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": system_prompt,
                }
            ],
            "temperature": self.temperature if temperature is None else temperature,
            "max_tokens": self.max_tokens if max_tokens is None else max_tokens,
        }
        
        # Add user message with the prompt
        payload["messages"].append({"role": "user", "content": prompt})
        
        logger.debug(f"Calling OpenAI with model={self.model}")
        
        response = self.client.post(OPENAI_API_URL, json=payload)
        response.raise_for_status()
        
        result = response.json()
        return result["choices"][0]["message"]["content"], result.get("usage") or {}
    
    def _parse_json_response(self, response_text: str) -> dict:
        """
        Parse JSON from LLM response text.
        
        Attempts to extract JSON from the response, handling cases where
        the model includes extra text before/after the JSON.
        
        Args:
            response_text: Raw response from LLM
            
        Returns:
            Parsed JSON dictionary
            
        Raises:
            json.JSONDecodeError: If JSON cannot be extracted
        """
        # Try parsing directly first
        try:
            return json.loads(response_text)
        except json.JSONDecodeError:
            pass
        
        # Try extracting JSON from response (in case of extra text)
        response_text = response_text.strip()
        
        # Look for JSON block (common for LLMs to include markdown formatting)
        if "```json" in response_text:
            json_start = response_text.index("```json") + 7
            json_end = response_text.index("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        elif "```" in response_text:
            json_start = response_text.index("```") + 3
            json_end = response_text.index("```", json_start)
            response_text = response_text[json_start:json_end].strip()
        
        # Try to find JSON object boundaries
        if "{" in response_text:
            json_start = response_text.index("{")
            # Find the last closing brace
            json_end = response_text.rfind("}") + 1
            if json_end > json_start:
                response_text = response_text[json_start:json_end]
        
        return json.loads(response_text)
    
    def __del__(self):
        """Clean up HTTP client."""
        if hasattr(self, "client"):
            self.client.close()
