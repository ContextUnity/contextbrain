"""Q&A ingestion plugin for conversational transcripts."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError
from contextunity.core.types import JsonDict
from typing_extensions import override

if TYPE_CHECKING:
    from .transformer import QATransformer

from contextunity.core.narrowing import (
    as_json_dict_list,
    as_str,
    as_str_list,
    json_dict_list_as_json,
)

from contextunity.brain.core import BrainConfig
from contextunity.brain.core.types import StructData

from ...core import (
    IngestionPlugin,
    RawData,
    ShadowRecord,
    register_plugin,
)
from ...core.loaders import FileLoaderMixin, read_text_file
from ...core.prompts import (
    qa_batch_analysis_prompt,
    qa_speaker_detection_prompt,
    qa_validate_answer_prompt,
    qa_validate_question_prompt,
)
from ...core.types import GraphEnrichmentResult, QAStructData
from ...core.utils import (
    clean_str_list,
    get_graph_enrichment,
    llm_generate_tsv,
    load_taxonomy_safe,
    parse_tsv_line,
)
from ...settings import RagIngestionConfig
from ...utils.llm import llm_generate

logger = get_contextunit_logger(__name__)

# Minimum words for a speaker turn to be considered a standalone segment
# Shorter turns (like "Right", "Yeah") are merged with the previous segment
MIN_STANDALONE_WORDS = 5


@register_plugin("qa")
class QAPlugin(IngestionPlugin, FileLoaderMixin):
    """Plugin for processing conversational Q&A transcripts with speaker identification."""

    @property
    @override
    def source_type(self) -> str:
        """Source type.

        Returns:
            str: The resulting string value.
        """
        return "qa"

    @property
    @override
    def default_source_dir(self) -> str:
        """Default source dir.

        Returns:
            str: The resulting string value.
        """
        return "qa"

    def __init__(self) -> None:
        """Initialize a new instance of QAPlugin."""
        super().__init__()
        self._core_cfg: BrainConfig | None = None
        self._transformer: QATransformer | None = None

    def set_core_cfg(self, core_cfg: BrainConfig) -> None:
        """Configure the core cfg settings.

        Args:
            core_cfg (BrainConfig): The core cfg parameter.
        """
        self._core_cfg = core_cfg

    def _require_core_cfg(self) -> BrainConfig:
        """require core cfg.

        Returns:
            BrainConfig: An instance of BrainConfig.

        Raises:
            ValueError: If parameter values are invalid.
        """
        if self._core_cfg is None:
            raise ConfigurationError(
                "QAPlugin requires core_cfg (contextunity.brain.core.config.BrainConfig)"
            )
        return self._core_cfg

    def _get_transformer(self) -> QATransformer:
        """Lazy initialization of transformer.

        Returns:
            QATransformer: An instance of QATransformer.
        """
        if self._transformer is None:
            from .transformer import QATransformer

            self._transformer = QATransformer(self._require_core_cfg())
        return self._transformer

    @override
    def load(self, assets_path: str) -> list[RawData]:
        """Load Q&A transcripts from text files.

        Args:
            assets_path (str): The assets path parameter.

        Returns:
            list[RawData]: A list of list[RawData].
        """
        # Resolve directory with q&a fallback (supports both "qa" and "q&a" directory names)
        if not (source_dir := self._resolve_source_dir(assets_path, alternatives=("q&a",))):
            return []

        raw_data: list[RawData] = []

        for text_file in self._iter_files(source_dir, extensions=(".txt", ".md")):
            try:
                if not (loaded := read_text_file(text_file)):
                    continue
                content = loaded.content

                content = content.strip()
                if not content:
                    logger.warning("Empty file, skipping: %s", text_file.name)
                    continue

                # Derive base title from filename (deterministic).
                # UI-visible session_title is built later by appending the detected speaker.
                source_title = self._derive_session_title(text_file)

                metadata: JsonDict = {
                    "session_title": source_title,
                    "source_title": source_title,
                }

                raw_data.append(
                    RawData(
                        content=content,
                        source_type="qa",
                        metadata=metadata,
                    )
                )

            except Exception as e:
                logger.error("Failed to load QA file %s: %s", text_file, e)
                # Continue processing other files
                continue

        return raw_data

    def _extract_session_title(self, content: str, path: Path | None = None) -> str:
        """Extract session title from content first line or filename.

        Args:
            content: File content
            path: Optional file path for fallback

        Returns:
            Session title string
        """
        # Try first line if it looks like a title
        lines = content.split("\n")
        if lines and lines[0].strip():
            first_line = lines[0].strip()
            # Use first line if it's reasonable length and not a speaker name
            if len(first_line) < 200 and not self._is_speaker_name(first_line):
                return first_line

        # Fallback to filename if path provided
        if path:
            return self._derive_session_title(path)

        return "Q&A Session"

    def _derive_session_title(self, path: Path) -> str:
        """Derive session title from filename.

        Args:
            path (Path): The filesystem path.

        Returns:
            str: The resulting string value.
        """
        title = re.sub(r"[_\-]+", " ", path.stem)
        title = re.sub(r"\s*transcript\s*[\d\s]*", "", title, flags=re.IGNORECASE)
        title = re.sub(r"\s+\d{5,}", "", title)
        title = re.sub(r"\s+", " ", title).strip()
        return title or path.stem

    @override
    def transform(
        self,
        data: list[RawData],
        enrichment_func: Callable[[str], GraphEnrichmentResult],
        taxonomy_path: Path | None = None,
        config: RagIngestionConfig | None = None,
        core_cfg: BrainConfig | None = None,
        **kwargs: object,
    ) -> list[ShadowRecord]:
        """Transform conversational transcripts into Q&A chunks.

        Args:
            data (list[RawData]): The raw data dictionary or object.
            enrichment_func (Callable[[str], GraphEnrichmentResult]): The enrichment func parameter.
            taxonomy_path (Path | None): The taxonomy path parameter.
            config (RagIngestionConfig | None): The configuration settings dict or object.
            core_cfg (BrainConfig | None): The core cfg parameter.

        Returns:
            list[ShadowRecord]: A list of list[ShadowRecord].
        """
        _ = kwargs
        self._core_cfg = core_cfg
        _ = self._require_core_cfg()

        shadow_records: list[ShadowRecord] = []
        total_items = len(data)

        persona_name = ""
        if config is not None:
            persona_name = config.persona.persona_name.strip()
        if persona_name.lower() in {"", "speaker name"}:
            persona_name = ""

        # Load taxonomy for category-based headers
        taxonomy = load_taxonomy_safe(taxonomy_path)
        taxonomy_categories = self._get_taxonomy_categories(taxonomy) if taxonomy else []

        for idx, raw in enumerate(data, 1):
            # Extract session title (prefer metadata, then first line, then default)
            session_title = str(
                raw.metadata.get("session_title") or self._extract_session_title(raw.content)
            )

            logger.info(
                "  Processing QA session %d/%d: %s",
                idx,
                total_items,
                session_title[:50],
            )

            # Extract session host/interviewer (identified during preprocess)
            # Used for fallback headers: "Speaker Name sharing thoughts on 'topic' with {Host}"
            session_host = str(raw.metadata.get("session_host") or "").strip()

            # 1. Split by speaker changes (prefer precomputed turns from preprocess)
            interactions: list[dict[str, str]] = []
            for it_raw in as_json_dict_list(raw.metadata.get("interactions", [])):
                speaker = as_str(it_raw.get("speaker"))
                text = as_str(it_raw.get("text"))
                if text.strip():
                    interactions.append(
                        {
                            "speaker": speaker.strip() or "Unknown",
                            "text": text.strip(),
                        }
                    )

            if interactions:
                logger.info("    Using precomputed speaker turns from CleanText metadata")
            else:
                logger.info("    Detecting speakers with LLM...")  # qa: llm speaker detect
                interactions = self.split_by_speakers_llm(raw.content)
                logger.info("    ✓ Speaker detection completed")

            if not interactions:
                logger.warning("No speaker interactions found in %s", session_title)
                # Final fallback: create single chunk from entire content
                interactions = [{"speaker": "Unknown", "text": raw.content}]

            # 2. Merge short interruptions with previous segment
            interactions = self.merge_interruptions(interactions)

            # 3. Group interactions into logical chunks
            grouped_chunks = self._group_interactions(interactions, min_chars=400, max_chars=1500)

            if not grouped_chunks:
                logger.warning("No valid chunks created from %s", session_title)
                continue

            # 4. Batch LLM processing for topic and primary speaker identification
            analyses = self._analyze_segments_batch(grouped_chunks, taxonomy_categories)

            # 5. Create ShadowRecords (with answer validation)
            for i, chunk in enumerate(grouped_chunks):
                analysis = analyses.get(
                    i,
                    {
                        "topic": "Discussion",
                        "primary_speaker": as_str(chunk.get("primary_speaker"), default="Unknown"),
                        "category": "",
                    },
                )
                record = self._create_shadow_record(
                    chunk=chunk,
                    analysis=analysis,
                    session_title=session_title,
                    enrichment_func=enrichment_func,
                    _persona_name=persona_name,
                    session_host=session_host,
                    initial_keywords=raw.metadata.get("keywords", []),
                    config=config,
                )
                if record is not None:
                    shadow_records.append(record)

        return shadow_records

    def _get_taxonomy_categories(self, taxonomy: StructData) -> list[str]:
        """Extract category names from taxonomy for prompt.

        Args:
            taxonomy (StructData): The taxonomy parameter.

        Returns:
            list[str]: A list of list[str].
        """
        categories = taxonomy.get("categories", {})
        if not isinstance(categories, dict):
            return []
        return [cat_name.replace("_", " ").title() for cat_name in categories.keys()]

    def merge_interruptions(self, interactions: list[dict[str, str]]) -> list[dict[str, str]]:
        """Merge short speaker interruptions with previous segment.

        If a speaker turn has fewer than MIN_STANDALONE_WORDS words,
        merge it with the previous segment to preserve conversational flow.

        Args:
            interactions: List of speaker interactions

        Returns:
            List with short interruptions merged
        """
        if len(interactions) <= 1:
            return interactions

        merged: list[dict[str, str]] = []

        for inter in interactions:
            text = inter["text"].strip()
            word_count = len(text.split())

            if merged and word_count < MIN_STANDALONE_WORDS:
                # Merge with previous segment
                prev = merged[-1]
                # Do not inject speaker tags for short interruptions; it causes tone/persona
                # drift and makes the resulting chunk harder to read.
                prev["text"] = f"{prev['text']} {text}"
                logger.debug(
                    "Merged short interruption (%d words) from %s",
                    word_count,
                    inter["speaker"],
                )
            else:
                merged.append(inter.copy())

        return merged

    def _create_shadow_record(
        self,
        chunk: JsonDict,
        analysis: dict[str, str],
        session_title: str,
        enrichment_func: Callable[[str], GraphEnrichmentResult],
        _persona_name: str,
        *,
        session_host: str = "",
        initial_keywords: list[str] | object | None = None,
        config: RagIngestionConfig | None = None,
    ) -> ShadowRecord | None:
        """Create a ShadowRecord from a chunk with analysis.

        Args:
            chunk (JsonDict): The chunk parameter.
            analysis (dict[str, str]): The analysis parameter.
            session_title (str): The session title parameter.
            enrichment_func (Any): The enrichment func parameter.
            _persona_name (str): The persona name parameter.

        Returns:
            ShadowRecord | None: An instance of ShadowRecord | None.
        """
        combined_text = as_str(chunk.get("combined_text"))
        topic = as_str(analysis.get("topic"), default="Discussion")

        interactions = as_json_dict_list(chunk.get("interactions"))

        # Load speaker corrections (from video.corrections or qa.corrections)
        # These handle transcription errors like "John Doe" → "Speaker Name"
        corrections: dict[str, str] = {}
        if config is not None:
            corrections.update(config.video.corrections)
            corrections.update(config.qa.corrections)

        def apply_corrections(name: str) -> str:
            """Apply corrections to speaker name.

            Args:
                name (str): The name of the entity.

            Returns:
                str: The resulting string value.
            """
            for wrong, correct in corrections.items():
                if wrong.casefold() in name.casefold():
                    name = name.replace(wrong, correct)
                    name = name.replace(wrong.lower(), correct)
                    name = name.replace(wrong.upper(), correct)
            return name.strip()

        # Build speaker stats: word counts and normalized name mapping
        from collections import Counter

        speaker_word_counts: Counter[str] = Counter()
        norm_to_original: dict[str, str] = {}

        for it in interactions:
            sp = as_str(it.get("speaker"))
            tx = as_str(it.get("text"))
            if not sp or not tx:
                continue
            # Apply corrections to speaker name before normalizing
            corrected_sp = apply_corrections(sp.strip()) or "Unknown"
            norm = corrected_sp.casefold()
            _ = norm_to_original.setdefault(norm, corrected_sp)
            speaker_word_counts[norm] += len(tx.split())

        # Infer answer speaker: dominant speaker in THIS chunk by word count
        # (persona is used for name correction, not for forcing speaker)
        if speaker_word_counts:
            dominant_norm = speaker_word_counts.most_common(1)[0][0]
            answer_speaker = norm_to_original.get(dominant_norm, "Unknown")
        else:
            answer_speaker = "Unknown"

        # Infer question speaker: strongest non-answer speaker
        ans_norm = answer_speaker.casefold()
        other_speakers = [(n, c) for n, c in speaker_word_counts.items() if n != ans_norm]
        question_speaker = (
            norm_to_original.get(max(other_speakers, key=lambda x: x[1])[0], "Unknown")
            if other_speakers
            else "Unknown"
        )

        # Build answer text from answer speaker only
        answer_parts: list[str] = []
        for it in interactions:
            sp = as_str(it.get("speaker"))
            tx = as_str(it.get("text"))
            if tx.strip() and sp.strip().casefold() == ans_norm:
                answer_parts.append(tx.strip())

        answer_text = " ".join(answer_parts).strip()
        if not answer_text:
            # Fallback if we failed to isolate: keep the full chunk content.
            answer_text = combined_text

        # Validate answer content - filter out meaningless answers
        llm_answer_validation_enabled = (
            config.qa.llm_answer_validation_enabled if config is not None else True
        )

        if llm_answer_validation_enabled and answer_text:
            try:
                validation_prompt = qa_validate_answer_prompt(
                    answer_text=answer_text[:1500],
                    topic=topic,
                )
                validated = llm_generate(
                    core_cfg=self._require_core_cfg(),
                    prompt=validation_prompt,
                    model=self._require_core_cfg().models.ingestion.preprocess.model,
                    max_tokens=32,
                    temperature=0.0,
                )
                if isinstance(validated, str) and "NOT_VALUABLE" in validated.strip().upper():
                    logger.debug("Answer rejected as not valuable: %s...", answer_text[:50])
                    return None
            except Exception as e:
                logger.debug("LLM answer validation failed: %s (keeping answer)", e)

        # Build question text from question speaker's interactions
        # Extract question text from question speaker's turns
        q_norm = question_speaker.casefold()
        question_parts: list[str] = []
        if question_speaker != "Unknown":
            for it in interactions:
                sp = as_str(it.get("speaker"))
                tx = as_str(it.get("text"))
                if tx.strip() and sp.strip().casefold() == q_norm:
                    question_parts.append(tx.strip())
        raw_question_text = " ".join(question_parts).strip()

        # Validate question using LLM - filters out:
        # - Non-questions (statements, declarations)
        # - Promotional/administrative content
        # - Conversational filler
        llm_question_validation_enabled = (
            config.qa.llm_question_validation_enabled if config is not None else True
        )

        question_text = ""
        if raw_question_text and question_speaker != "Unknown" and answer_text:
            if llm_question_validation_enabled:
                try:
                    validation_prompt = qa_validate_question_prompt(
                        raw_text=raw_question_text[:500],  # Limit input size
                        answer_context=answer_text,
                    )
                    validated = llm_generate(
                        core_cfg=self._require_core_cfg(),
                        prompt=validation_prompt,
                        model=self._require_core_cfg().models.ingestion.preprocess.model,
                        max_tokens=256,
                        temperature=0.0,  # Deterministic
                    )
                    if isinstance(validated, str):
                        validated = validated.strip()
                        # Check if LLM rejected the question
                        if validated.upper() != "NOT_A_QUESTION" and len(validated) > 10:
                            question_text = validated[:300] if len(validated) > 300 else validated
                            logger.debug("Question validated: %s", question_text[:50])
                        else:
                            logger.debug(
                                "Question rejected as non-question: %s...",
                                raw_question_text[:50],
                            )
                except Exception as e:
                    logger.debug("LLM question validation failed: %s", e)
                    # Fallback: use raw text truncated
                    question_text = (
                        raw_question_text[:297].rstrip() + "..."
                        if len(raw_question_text) > 300
                        else raw_question_text
                    )
            else:
                # Validation disabled - just truncate
                question_text = (
                    raw_question_text[:297].rstrip() + "..."
                    if len(raw_question_text) > 300
                    else raw_question_text
                )

        # Determine question/header for record
        # Priority: 1) Validated real question, 2) Topic-based header if answer is valuable
        if question_text:
            question = question_text
        else:
            # No valid question, but answer passed validation (is valuable)
            # Use topic-based header for discoverability
            # Format: "{Speaker} and {Host} sharing thoughts on {topic}"
            topic_str = topic if topic and topic != "Discussion" else "the topic"
            if answer_speaker != "Unknown" and session_host and session_host != answer_speaker:
                question = f"{answer_speaker} and {session_host} sharing thoughts on {topic_str}"
            elif answer_speaker != "Unknown":
                question = f"{answer_speaker} sharing thoughts on {topic_str}"
            else:
                question = f"Thoughts on {topic_str}"

        # Enrichment (graph context) - use combined answer+question for better context
        enrichment_text = f"{question_text} {answer_text}".strip() if question_text else answer_text
        keywords, _, parent_categories = get_graph_enrichment(
            text=enrichment_text, enrichment_func=enrichment_func
        )
        base = as_str_list(initial_keywords)
        keywords = list(dict.fromkeys([*base, *keywords]))[:10]

        # Build shadow context (input_text)
        input_text = self._build_input_text(
            content=answer_text,
            keywords=keywords,
            topic=topic,
            speaker=answer_speaker,
            parent_categories=parent_categories,
        )

        # Generate stable ID
        record_id = (
            f"qa-{hashlib.sha256((session_title + answer_text).encode('utf-8')).hexdigest()[:16]}"
        )

        # Clean answer text: replace newlines with spaces for better UI display
        clean_answer = answer_text.replace("\n", " ").strip()
        clean_answer = " ".join(clean_answer.split())

        # UI fields:
        # - session_title: stable title derived from filename (no speaker suffix)
        # - question: actual question text (or fallback to speaker label)
        source_title = session_title
        display_session_title = source_title

        # Build struct_data matching frontend schema
        struct_data: QAStructData = {
            "source_type": "qa",
            # snake_case for retriever/citations compatibility
            "session_title": display_session_title,
            "source_title": source_title,
            "speaker": answer_speaker,
            "question": question,  # Shown as card header
            "answer": clean_answer,  # Actual transcript segment (cleaned)
            "keywords": clean_str_list(keywords, limit=10),
        }

        return ShadowRecord(
            id=record_id,
            input_text=input_text,
            struct_data=dict(struct_data) if struct_data else {},
            title=display_session_title,
            source_type="qa",
        )

    def _build_input_text(
        self,
        content: str,
        keywords: list[str],
        topic: str = "",
        speaker: str = "",
        parent_categories: list[str] | None = None,
    ) -> str:
        """Build shadow context input_text from components with natural language enrichment.

        Args:
            topic: Topic summary
            speaker: Primary speaker name
            content: Transcript content
            keywords: Graph keywords
            parent_categories: Taxonomy categories from graph enrichment

        Returns:
            Formatted input_text string
        """
        parts = [
            f"Topic: {topic}",
            f"Speaker: {speaker}",
            f"Content: {content}",
        ]

        # Add taxonomy categories from graph enrichment
        if parent_categories:
            cat_str = ", ".join(parent_categories[:5])
            parts.append(f"Categories: {cat_str}")

        # Add natural language enrichment for keywords
        if keywords:
            top_keywords = keywords[:10]
            if len(top_keywords) == 1:
                parts.append(f"Additional Knowledge: This text is related to {top_keywords[0]}.")
            elif len(top_keywords) <= 3:
                keywords_str = ", ".join(top_keywords[:-1])
                parts.append(
                    f"Additional Knowledge: This text is related to {keywords_str} and {top_keywords[-1]}."
                )
            else:
                keywords_str = ", ".join(top_keywords[:5])
                parts.append(
                    f"Additional Knowledge: This text is related to {keywords_str}, and other concepts."
                )

        return "\n".join(parts)

    def _is_speaker_name(self, text: str) -> bool:
        """Check if text looks like a speaker name (e.g., 'Speaker A').

        Args:
            text (str): The text parameter.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        # Pattern: FirstName LastName (2-3 words, capitalized)
        pattern = r"^[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2}$"
        return bool(re.match(pattern, text.strip()))

    def split_by_speakers_llm(self, content: str) -> list[dict[str, str]]:
        """Split content by speaker changes using LLM.

        Args:
            content (str): The content parameter.

        Returns:
            list[dict[str, str]]: A list of list[dict[str, str]].
        """
        try:
            # Process in chunks if content is very long (to avoid token limits)
            max_chunk_size = 100000  # Process up to 100k chars at a time

            if len(content) <= max_chunk_size:
                return self._split_by_speakers_llm_chunk(content, is_full=True)

            # For very long transcripts, process in chunks with overlap
            logger.info("Processing long transcript in chunks (%d chars)", len(content))
            all_interactions: list[dict[str, str]] = []
            overlap_size = 500  # Overlap to avoid breaking mid-conversation

            for i in range(0, len(content), max_chunk_size):
                # Calculate chunk boundaries with overlap
                chunk_start = max(0, i - overlap_size) if i > 0 else 0
                chunk_end = min(len(content), i + max_chunk_size)
                chunk = content[chunk_start:chunk_end]

                chunk_interactions = self._split_by_speakers_llm_chunk(chunk, is_full=(i == 0))
                all_interactions.extend(chunk_interactions)

            return all_interactions

        except Exception as e:
            logger.error("LLM speaker detection failed: %s", e, exc_info=True)
            return []

    def _split_by_speakers_llm_chunk(
        self, content: str, is_full: bool = True
    ) -> list[dict[str, str]]:
        """Process a single chunk of content for speaker detection.

        Args:
            content (str): The content parameter.
            is_full (bool): The is full parameter.

        Returns:
            list[dict[str, str]]: A list of list[dict[str, str]].
        """
        # Truncate to avoid token limits while preserving context
        truncated = content[:80000] if len(content) > 80000 else content
        if len(content) > 80000:
            truncated += "\n\n[... content truncated for speaker detection ...]"

        prompt = qa_speaker_detection_prompt(transcript=truncated)
        text = llm_generate_tsv(
            core_cfg=self._require_core_cfg(),
            prompt=prompt,
            model=self._require_core_cfg().models.ingestion.preprocess.model,
            max_tokens=32768,
            temperature=0.1,
            retries=3,
        )

        interactions: list[dict[str, str]] = []
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            parts = parse_tsv_line(ln)
            if len(parts) < 2:
                continue
            speaker = parts[0].strip()
            spoken_text = parts[1].strip()
            # Only include if both speaker and text are present and substantial
            if speaker and len(spoken_text) > 10:
                interactions.append({"speaker": speaker, "text": spoken_text})

        if interactions:
            logger.info(
                "LLM detected %d speaker segments%s",
                len(interactions),
                " (full transcript)" if is_full else "",
            )
        else:
            logger.warning("LLM speaker detection returned empty list")
        return interactions

    def _group_interactions(
        self,
        interactions: list[dict[str, str]],
        min_chars: int = 400,
        max_chars: int = 1500,
    ) -> list[JsonDict]:
        """Group consecutive speaker interactions into logical chunks.

        Args:
            interactions (list[dict[str, str]]): The interactions parameter.
            min_chars (int): The min chars parameter.
            max_chars (int): The max chars parameter.

        Returns:
            list[JsonDict]: A list of list[JsonDict].
        """
        if not interactions:
            return []

        groups: list[JsonDict] = []
        current_group: list[JsonDict] = []
        current_len = 0
        current_speaker: str | None = None

        for inter in interactions:
            speaker = inter["speaker"]
            text = inter["text"]
            text_len = len(text)

            # Check if adding this interaction would exceed max_chars
            if current_len + text_len > max_chars and current_len >= min_chars:
                # Finalize current group
                combined_text = self._combine_interactions(
                    [
                        {"speaker": as_str(row.get("speaker")), "text": as_str(row.get("text"))}
                        for row in current_group
                    ]
                )
                groups.append(
                    {
                        "combined_text": combined_text,
                        "primary_speaker": (
                            current_speaker or as_str(current_group[0].get("speaker"))
                            if current_group
                            else "Unknown"
                        ),
                        "interactions": json_dict_list_as_json(current_group),
                    }
                )
                current_group = [{"speaker": speaker, "text": text}]
                current_len = text_len
                current_speaker = speaker
            else:
                # Add to current group
                current_group.append({"speaker": speaker, "text": text})
                current_len += text_len
                # Track the first speaker in the group as primary
                if current_speaker is None:
                    current_speaker = speaker

        # Add final group if it meets minimum size
        if current_group and current_len >= min_chars:
            combined_text = self._combine_interactions(
                [
                    {"speaker": as_str(row.get("speaker")), "text": as_str(row.get("text"))}
                    for row in current_group
                ]
            )
            groups.append(
                {
                    "combined_text": combined_text,
                    "primary_speaker": current_speaker or as_str(current_group[0].get("speaker")),
                    "interactions": json_dict_list_as_json(current_group),
                }
            )
        elif current_group and current_len < min_chars and groups:
            # Merge small final group with last group if possible
            last_group: JsonDict = groups[-1]
            last_combined = as_str(last_group.get("combined_text"))
            final_combined = self._combine_interactions(
                [
                    {"speaker": as_str(row.get("speaker")), "text": as_str(row.get("text"))}
                    for row in current_group
                ]
            )
            merged = f"{last_combined}\n\n{final_combined}"
            if len(merged) <= max_chars:
                prior = as_json_dict_list(last_group.get("interactions"))
                groups[-1] = {
                    "combined_text": merged,
                    "primary_speaker": as_str(last_group.get("primary_speaker")),
                    "interactions": json_dict_list_as_json(prior + current_group),
                }
            else:
                # Can't merge, create separate small chunk
                groups.append(
                    {
                        "combined_text": final_combined,
                        "primary_speaker": current_speaker
                        or as_str(current_group[0].get("speaker")),
                        "interactions": json_dict_list_as_json(current_group),
                    }
                )

        return groups

    def _combine_interactions(self, interactions: list[dict[str, str]]) -> str:
        """Combine interactions into clean transcript format.

        Args:
            interactions (list[dict[str, str]]): The interactions parameter.

        Returns:
            str: The resulting string value.
        """
        parts: list[str] = []
        for inter in interactions:
            speaker = inter["speaker"]
            text = inter["text"].strip()

            # Format: "Speaker: text"
            parts.append(f"{speaker}: {text}")

        return "\n\n".join(parts)

    def _analyze_segments_batch(
        self,
        chunks: list[JsonDict],
        taxonomy_categories: list[str] | None = None,
    ) -> dict[int, dict[str, str]]:
        """Batch LLM processing to identify topic, primary speaker, and category for each chunk.

        Args:
            chunks (list[JsonDict]): The chunks parameter.
            taxonomy_categories (list[str] | None): The taxonomy categories parameter.

        Returns:
            dict[int, dict[str, str]]: A dictionary containing the results.
        """
        if not chunks:
            return {}

        total_chunks = len(chunks)
        logger.info("    Analyzing %d chunks with LLM...", total_chunks)

        # Prepare batch prompt
        batch_items: list[JsonDict] = []
        for i, chunk in enumerate(chunks):
            combined_text = as_str(chunk.get("combined_text"))
            # Truncate to avoid token limits (keep first 2000 chars)
            truncated = combined_text[:2000] + ("..." if len(combined_text) > 2000 else "")
            batch_items.append(
                {
                    "index": i,
                    "text": truncated,
                }
            )

        try:
            # Build batch prompt
            items_text = "\n\n---\n\n".join(
                f"CHUNK {item['index']}:\n{item['text']}" for item in batch_items
            )
            prompt = qa_batch_analysis_prompt(
                items_text=items_text,
                taxonomy_categories=taxonomy_categories,
            )

            logger.info("    Calling LLM for batch analysis...")
            text = llm_generate_tsv(
                core_cfg=self._require_core_cfg(),
                prompt=prompt,
                model=self._require_core_cfg().models.ingestion.preprocess.model,
                max_tokens=16384,
                temperature=0.1,
                retries=3,
            )
            logger.info("    ✓ LLM analysis completed")

            # Parse TSV: index<TAB>topic<TAB>primary_speaker<TAB>category
            analyses: dict[int, dict[str, str]] = {}
            for ln in text.splitlines():
                ln = ln.strip()
                if not ln:
                    continue
                parts = parse_tsv_line(ln)
                if len(parts) < 3:
                    continue
                try:
                    idx = int(parts[0].strip())
                except ValueError:
                    continue
                if 0 <= idx < len(chunks):
                    analyses[idx] = {
                        "topic": parts[1].strip() or "Discussion",
                        "primary_speaker": parts[2].strip()
                        or as_str(chunks[idx].get("primary_speaker"), default="Unknown"),
                        "category": parts[3].strip() if len(parts) > 3 else "",
                    }

            # Fill in missing analyses with fallback
            for i in range(len(chunks)):
                if i not in analyses:
                    analyses[i] = {
                        "topic": "Discussion",
                        "primary_speaker": as_str(
                            chunks[i].get("primary_speaker"), default="Unknown"
                        ),
                        "category": "",
                    }

            return analyses

        except Exception as e:
            logger.warning("LLM batch analysis failed: %s, using fallback", e)
            return self._fallback_analysis(chunks)

    def _fallback_analysis(self, chunks: list[JsonDict]) -> dict[int, dict[str, str]]:
        """Fallback analysis when LLM fails - extract topic from first sentence.

        Args:
            chunks (list[JsonDict]): The chunks parameter.

        Returns:
            dict[int, dict[str, str]]: A dictionary containing the results.
        """
        analyses: dict[int, dict[str, str]] = {}
        for i, chunk in enumerate(chunks):
            combined_text = as_str(chunk.get("combined_text"))
            primary_speaker = as_str(chunk.get("primary_speaker"), default="Unknown")

            # Extract first sentence as topic (truncate to ~50 chars)
            first_sentence = combined_text.split(".")[0].strip()
            if len(first_sentence) > 50:
                first_sentence = first_sentence[:47] + "..."

            analyses[i] = {
                "topic": first_sentence or "Discussion",
                "primary_speaker": primary_speaker,
            }
        return analyses
