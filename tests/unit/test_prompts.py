"""Tests for ingestion prompt templates — truncation behavior only."""

from __future__ import annotations

from contextunity.brain.ingestion.rag import (
    qa_rephrase_question_prompt,
    qa_validate_question_prompt,
    video_validate_segment_prompt,
)


class TestPromptTruncation:
    """Prompt functions must enforce character limits on long inputs."""

    def test_truncates_long_answer_context(self) -> None:
        long_answer = "A" * 1000
        prompt = qa_validate_question_prompt(
            raw_text="Question?",
            answer_context=long_answer,
        )
        assert long_answer not in prompt
        assert "A" * 500 in prompt

    def test_truncates_long_segment_text(self) -> None:
        long_text = "X" * 2000
        prompt = video_validate_segment_prompt(
            segment_text=long_text,
            video_title="Test",
        )
        assert "X" * 1500 in prompt
        assert long_text not in prompt

    def test_truncates_long_answer_in_rephrase(self) -> None:
        long_answer = "B" * 1000
        prompt = qa_rephrase_question_prompt(
            question="Question?",
            answer=long_answer,
        )
        assert long_answer not in prompt
