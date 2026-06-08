"""Main QA transformer that orchestrates all QA processing components."""

from __future__ import annotations

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import as_str, str_list_as_json
from contextunity.core.types import JsonDict, is_json_dict

from contextunity.brain.core import BrainConfig
from contextunity.brain.core.types import StructData

from ...core.types import ShadowRecord
from .analyzer import QuestionAnalyzer
from .speaker import SpeakerProcessor
from .taxonomy_mapper import TaxonomyMapper

logger = get_contextunit_logger(__name__)


class QATransformer:
    """Main QA transformer that orchestrates all QA processing."""

    def __init__(self, core_cfg: BrainConfig, taxonomy: StructData | None = None) -> None:
        """Initialize a new instance of QATransformer.

        Args:
            core_cfg (BrainConfig): The core cfg parameter.
            taxonomy (StructData | None): The taxonomy parameter.
        """
        self.core_cfg: BrainConfig = core_cfg
        self.speaker_processor: SpeakerProcessor = SpeakerProcessor(core_cfg)
        self.question_analyzer: QuestionAnalyzer = QuestionAnalyzer(core_cfg)
        self.taxonomy_mapper: TaxonomyMapper = TaxonomyMapper(taxonomy)

    def transform_content(
        self,
        content: str,
        taxonomy: StructData | None = None,
        *,
        session_title: str = "",
    ) -> list[JsonDict]:
        """Transform raw QA content into structured Q&A pairs.

        Args:
            content (str): The content parameter.
            taxonomy (StructData | None): The taxonomy parameter.

        Returns:
            list[JsonDict]: A list of list[JsonDict].
        """
        try:
            # Step 1: Split by speakers
            interactions = self.speaker_processor.split_by_speakers_llm(content)

            # Step 2: Merge short interruptions
            merged_interactions = self.speaker_processor.merge_interruptions(interactions)

            # Step 3: Analyze segments for Q&A pairs
            if merged_interactions:
                segments: list[JsonDict] = [dict(item) for item in merged_interactions]
                analyses = self.question_analyzer.analyze_segments_batch(segments)

                # Step 4: Create structured records
                records: list[JsonDict] = []
                for i, analysis in analyses.items():
                    if i < len(merged_interactions):
                        interaction = merged_interactions[i]

                        # Map to taxonomy
                        taxonomy_categories = self.taxonomy_mapper.map_to_taxonomy(
                            analysis.get("question", "") + " " + analysis.get("answer", ""),
                            taxonomy,
                        )

                        record: JsonDict = {
                            # Expected by downstream struct_data builders (Vertex requires it).
                            "session_title": session_title,
                            "source_title": session_title,
                            "question": as_str(analysis.get("question")),
                            "answer": as_str(analysis.get("answer")),
                            "speaker": as_str(interaction.get("speaker")),
                            "taxonomy_categories": str_list_as_json(taxonomy_categories),
                            "source_type": "qa",
                        }
                        records.append(record)

                return records

        except Exception as e:
            logger.error("QA transformation failed: %s", e)
            return []

        return []

    def _combine_interactions(self, interactions: list[dict[str, str]]) -> str:
        """Combine interactions back into text format.

        Args:
            interactions (list[dict[str, str]]): The interactions parameter.

        Returns:
            str: The resulting string value.
        """
        parts: list[str] = []
        for interaction in interactions:
            speaker = interaction.get("speaker", "")
            text = interaction.get("text", "")
            if speaker and text:
                parts.append(f"[{speaker}]: {text}")

        return "\n".join(parts)

    def _build_input_text(
        self, shadow_records: list[object], taxonomy: StructData | None = None
    ) -> str:
        """Build input text from shadow records for processing.

        Args:
            shadow_records (list[object]): The shadow records parameter.
            taxonomy (StructData | None): The taxonomy parameter.

        Returns:
            str: The resulting string value.
        """
        texts: list[str] = []

        for record in shadow_records:
            content: str | None = None
            if isinstance(record, ShadowRecord):
                content = record.input_text
            elif is_json_dict(record):
                content = as_str(record.get("content"))
            else:
                continue

            # Add taxonomy context if available
            if taxonomy:
                categories = self.taxonomy_mapper.map_to_taxonomy(content, taxonomy)
                if categories:
                    content = f"[Topics: {', '.join(categories)}] {content}"

            texts.append(content)

        return "\n\n".join(texts)
