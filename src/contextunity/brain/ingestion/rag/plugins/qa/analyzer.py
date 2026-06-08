"""Question analysis component for QA plugin."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core import BrainConfig

from ...utils.llm import llm_generate

logger = get_contextunit_logger(__name__)


class QuestionAnalyzer:
    """Handles question extraction, validation, and analysis."""

    def __init__(self, core_cfg: BrainConfig) -> None:
        """Initialize a new instance of QuestionAnalyzer.

        Args:
            core_cfg (BrainConfig): The core cfg parameter.
        """
        self.core_cfg: BrainConfig = core_cfg

    def _validate_question_with_llm(self, question_text: str) -> bool:
        """Use LLM to validate if text is a proper question.

        Args:
            question_text (str): The question text parameter.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            # Import here to avoid circular imports
            from ...core.prompts import qa_validate_question_prompt

            prompt = qa_validate_question_prompt(raw_text=question_text, answer_context="")
            response = llm_generate(
                core_cfg=self.core_cfg,
                prompt=prompt,
                model=self.core_cfg.models.ingestion.preprocess.model,
                max_tokens=100,
                temperature=0.0,
            )

            # Simple yes/no check
            response_lower = str(response).lower().strip()
            return "yes" in response_lower or "true" in response_lower

        except Exception as e:
            logger.debug("LLM question validation failed: %s", e)
            # Fallback to simple heuristics
            return self._is_question_like(question_text)

    def _is_question_like(self, text: str) -> bool:
        """Simple heuristic to check if text looks like a question.

        Args:
            text (str): The text parameter.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        text = text.strip()
        if not text:
            return False

        # Check for question marks
        if "?" in text:
            return True

        # Check for question words
        question_words = [
            "what",
            "how",
            "why",
            "when",
            "where",
            "who",
            "which",
            "can",
            "could",
            "would",
            "should",
            "do",
            "does",
            "did",
            "is",
            "are",
            "was",
            "were",
        ]
        first_word = text.lower().split()[0] if text else ""

        return first_word in question_words

    def _validate_answer_with_llm(self, question_text: str, answer_text: str) -> bool:
        """Use LLM to validate if answer is valuable for the question.

        Args:
            question_text (str): The question text parameter.
            answer_text (str): The answer text parameter.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            # Import here to avoid circular imports
            from ...core.prompts import qa_validate_answer_prompt

            prompt = qa_validate_answer_prompt(answer_text=answer_text, topic=question_text)
            response = llm_generate(
                core_cfg=self.core_cfg,
                prompt=prompt,
                model=self.core_cfg.models.ingestion.preprocess.model,
                max_tokens=100,
                temperature=0.0,
            )

            # Simple yes/no check
            response_lower = str(response).lower().strip()
            return "yes" in response_lower or "true" in response_lower

        except Exception as e:
            logger.debug("LLM answer validation failed: %s (keeping answer)", e)
            # Default to keeping the answer
            return True

    def analyze_segments_batch(self, chunks: list[JsonDict]) -> dict[int, dict[str, str]]:
        """Analyze conversation segments in batch using LLM.

        Args:
            chunks (list[JsonDict]): The chunks parameter.

        Returns:
            dict[int, dict[str, str]]: A dictionary containing the results.
        """
        try:
            # Import here to avoid circular imports
            from ...core.prompts import qa_batch_analysis_prompt

            # Prepare content for batch analysis
            content_parts: list[str] = []
            for i, chunk in enumerate(chunks):
                if is_json_dict(chunk) and "text" in chunk:
                    content_parts.append(f"[{i}] {as_str(chunk.get('text'))}")

            content = "\n".join(content_parts)

            prompt = qa_batch_analysis_prompt(items_text=content)
            response = llm_generate(
                core_cfg=self.core_cfg,
                prompt=prompt,
                model=self.core_cfg.models.ingestion.preprocess.model,
                max_tokens=2048,
                temperature=0.0,
            )

            return self._parse_batch_analysis(response, chunks)

        except Exception as e:
            logger.warning("LLM batch analysis failed: %s, using fallback", e)
            return self._fallback_analysis(chunks)

    def _parse_batch_analysis(
        self, _response: object, chunks: list[JsonDict]
    ) -> dict[int, dict[str, str]]:
        """Parse batch analysis response.

        Args:
            _response (object): The response payload containing results.
            chunks (list[JsonDict]): The chunks parameter.

        Returns:
            dict[int, dict[str, str]]: A dictionary containing the results.
        """
        results: dict[int, dict[str, str]] = {}

        # Simple parsing - can be enhanced
        for i, chunk in enumerate(chunks):
            text = as_str(chunk.get("text"))
            results[i] = {
                "question": text[:100],  # Placeholder
                "answer": text[100:],  # Placeholder
            }

        return results

    def _fallback_analysis(self, chunks: list[JsonDict]) -> dict[int, dict[str, str]]:
        """Fallback analysis when LLM fails.

        Args:
            chunks (list[JsonDict]): The chunks parameter.

        Returns:
            dict[int, dict[str, str]]: A dictionary containing the results.
        """
        results: dict[int, dict[str, str]] = {}

        for i, chunk in enumerate(chunks):
            text = as_str(chunk.get("text"))
            # Simple split: first sentence as question, rest as answer
            sentences = text.split(".", 1)
            if len(sentences) >= 2:
                question = sentences[0].strip() + "?"
                answer = sentences[1].strip()
            else:
                question = text[:50] + "..."
                answer = text[50:]

            results[i] = {
                "question": question,
                "answer": answer,
            }

        return results
