"""Speaker processing component for QA plugin."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger

from contextunity.brain.core import BrainConfig
from contextunity.brain.core.exceptions import BrainIngestionError

from ...utils.llm import llm_generate

logger = get_contextunit_logger(__name__)


class SpeakerProcessor:
    """Handles speaker detection and conversation segmentation."""

    def __init__(self, core_cfg: BrainConfig) -> None:
        """Initialize a new instance of SpeakerProcessor.

        Args:
            core_cfg (BrainConfig): The core cfg parameter.
        """
        self.core_cfg: BrainConfig = core_cfg

    def split_by_speakers_llm(self, content: str) -> list[dict[str, str]]:
        """Use LLM to split conversation by speakers.

        Args:
            content (str): The content parameter.

        Returns:
            list[dict[str, str]]: A list of list[dict[str, str]].

        Raises:
            BrainIngestionError: If a validation error occurs.
        """
        try:
            # Import here to avoid circular imports
            from ...core.prompts import qa_speaker_detection_prompt

            prompt = qa_speaker_detection_prompt(transcript=content)
            text = llm_generate(
                core_cfg=self.core_cfg,
                prompt=prompt,
                model=self.core_cfg.models.ingestion.preprocess.model,
                max_tokens=4096,
                temperature=0.0,
            )

            return self._parse_speaker_output(str(text))
        except Exception as e:
            logger.error("LLM speaker detection failed: %s", e, exc_info=True)
            raise BrainIngestionError(f"Speaker detection failed: {e}") from e

    def _parse_speaker_output(self, text: str) -> list[dict[str, str]]:
        """Parse LLM output into speaker segments.

        Args:
            text (str): The text parameter.

        Returns:
            list[dict[str, str]]: A list of list[dict[str, str]].
        """
        interactions: list[dict[str, str]] = []

        for line in text.splitlines():
            line = line.strip()
            if not line or not line.startswith("["):
                continue

            try:
                # Parse [Speaker: Text] format
                if "]" in line:
                    speaker_part, text_part = line.split("]", 1)
                    speaker = speaker_part.strip("[").strip()
                    text_content = text_part.strip()

                    if speaker and text_content:
                        interactions.append({"speaker": speaker, "text": text_content})
            except Exception as e:
                logger.debug("Failed to parse speaker line '%s': %s", line[:50], e)
                continue

        return interactions

    def merge_interruptions(self, interactions: list[dict[str, str]]) -> list[dict[str, str]]:
        """Merge short interruptions with previous speaker turns.

        Args:
            interactions (list[dict[str, str]]): The interactions parameter.

        Returns:
            list[dict[str, str]]: A list of list[dict[str, str]].
        """
        if not interactions:
            return interactions

        MIN_STANDALONE_WORDS = 5
        merged: list[dict[str, str]] = []

        for interaction in interactions:
            text = interaction.get("text", "")
            word_count = len(text.split())

            if word_count >= MIN_STANDALONE_WORDS:
                # Keep as separate turn
                merged.append(interaction)
            elif merged:
                # Merge with previous turn
                prev = merged[-1]
                prev["text"] += f" {text}"
            else:
                # First turn is short, keep it anyway
                merged.append(interaction)

        return merged

    def _split_by_speakers_llm_chunk(self, _chunk: str, _context: str = "") -> list[dict[str, str]]:
        """Split a chunk of text by speakers using LLM.

        Args:
            _chunk (str): The chunk parameter.
            _context (str): The request context payload.

        Returns:
            list[dict[str, str]]: A list of list[dict[str, str]].
        """
        # This is a simplified version - full implementation would use LLM
        # For now, return empty list to be implemented
        logger.warning("Chunk speaker splitting not fully implemented")
        return []

    def _is_speaker_name(self, text: str) -> bool:
        """Check if text looks like a speaker name.

        Args:
            text (str): The text parameter.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        # Simple heuristic - can be enhanced
        text = text.strip()
        if not text:
            return False

        # Check for common speaker patterns
        if ":" in text or text.istitle():
            return True

        return False
