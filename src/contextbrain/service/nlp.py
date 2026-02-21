"""NLP enrichment pipeline for document ingestion.

Provides entity extraction (spaCy NER) and topic/keyword extraction (KeyBERT)
to enrich documents during ingestion. Both are optional — gracefully degrade
if dependencies are not installed.

Usage:
    enricher = NLPEnricher()
    result = enricher.enrich(text)
    # result.entities: [Entity(text="Kyiv", label="GPE"), ...]
    # result.topics: ["semantic search", "vector database", ...]
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Data Classes ─────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class Entity:
    """A named entity extracted from text."""

    text: str
    label: str  # e.g., PERSON, ORG, GPE, DATE, PRODUCT, EVENT
    start: int = 0
    end: int = 0


@dataclass
class EnrichmentResult:
    """Combined result of NLP enrichment."""

    entities: list[Entity] = field(default_factory=list)
    topics: list[str] = field(default_factory=list)
    categories: list[tuple[str, float]] = field(default_factory=list)  # (label, score)
    language: str = ""

    @property
    def entity_map(self) -> dict[str, list[str]]:
        """Group entities by label. E.g., {"ORG": ["Traverse", "Abris"]}."""
        groups: dict[str, list[str]] = {}
        for e in self.entities:
            groups.setdefault(e.label, []).append(e.text)
        return groups

    @property
    def top_category(self) -> str | None:
        """Return highest-scoring category label."""
        return self.categories[0][0] if self.categories else None

    def to_metadata(self) -> dict:
        """Convert to flat metadata dict for storage."""
        meta: dict = {}
        if self.topics:
            meta["topics"] = self.topics
        if self.categories:
            meta["category"] = self.categories[0][0]
            meta["category_scores"] = {k: round(v, 3) for k, v in self.categories}
        if self.language:
            meta["language"] = self.language
        for label, values in self.entity_map.items():
            # e.g., "entities_ORG": ["Traverse", "Abris"]
            meta[f"entities_{label}"] = list(set(values))
        return meta


# ─── Entity Extractor (spaCy) ────────────────────────────────────────────────


class EntityExtractor:
    """Named Entity Recognition using spaCy.

    Lazily loads the spaCy model on first use.
    Falls back gracefully if spaCy is not installed.
    """

    # Map of language codes to spaCy model names
    MODELS = {
        "en": "en_core_web_sm",
        "uk": "uk_core_news_sm",
    }

    def __init__(self, language: str = "en"):
        self._language = language
        self._nlp = None
        self._available = True

    def _ensure_model(self):
        """Lazily load spaCy model, download if needed."""
        if self._nlp is not None:
            return self._nlp
        if not self._available:
            return None

        model_name = self.MODELS.get(self._language, "en_core_web_sm")

        try:
            import spacy

            try:
                self._nlp = spacy.load(model_name)
                logger.info("spaCy model loaded: %s", model_name)
            except OSError:
                # Model not downloaded yet — try to download it
                logger.info("Downloading spaCy model: %s", model_name)
                try:
                    spacy.cli.download(model_name)  # type: ignore[attr-defined]
                    self._nlp = spacy.load(model_name)
                    logger.info("spaCy model downloaded and loaded: %s", model_name)
                except Exception as dl_err:
                    logger.warning("Failed to download spaCy model %s: %s", model_name, dl_err)
                    self._available = False
        except ImportError:
            logger.debug("spaCy not installed — NER disabled")
            self._available = False

        return self._nlp

    def extract(self, text: str, max_length: int = 100_000) -> list[Entity]:
        """Extract named entities from text.

        Args:
            text: Input text to process.
            max_length: Truncate text to this length (spaCy default is 1M chars).

        Returns:
            List of Entity objects.
        """
        nlp = self._ensure_model()
        if nlp is None:
            return []

        # Truncate very long texts to avoid memory issues
        if len(text) > max_length:
            text = text[:max_length]

        doc = nlp(text)
        entities = []
        seen = set()

        for ent in doc.ents:
            # Deduplicate by (text, label)
            key = (ent.text.strip(), ent.label_)
            if key in seen or not ent.text.strip():
                continue
            seen.add(key)
            entities.append(
                Entity(
                    text=ent.text.strip(),
                    label=ent.label_,
                    start=ent.start_char,
                    end=ent.end_char,
                )
            )

        return entities

    @property
    def is_available(self) -> bool:
        return self._available


# ─── Topic Extractor (KeyBERT) ───────────────────────────────────────────────


class TopicExtractor:
    """Keyword/topic extraction using KeyBERT.

    Can optionally reuse Brain's existing embedder for consistency.
    Falls back gracefully if KeyBERT is not installed.
    """

    def __init__(self, embedding_model=None):
        """Initialize topic extractor.

        Args:
            embedding_model: Optional sentence-transformers model or
                             string model name. If None, uses KeyBERT default.
        """
        self._kw_model = None
        self._embedding_model = embedding_model
        self._available = True

    def _ensure_model(self):
        """Lazily load KeyBERT model."""
        if self._kw_model is not None:
            return self._kw_model
        if not self._available:
            return None

        try:
            from keybert import KeyBERT

            self._kw_model = KeyBERT(model=self._embedding_model)
            logger.info("KeyBERT loaded (model=%s)", self._embedding_model or "default")
        except ImportError:
            logger.debug("KeyBERT not installed — topic extraction disabled")
            self._available = False
        except Exception as e:
            logger.warning("Failed to initialize KeyBERT: %s", e)
            self._available = False

        return self._kw_model

    def extract(
        self,
        text: str,
        top_n: int = 5,
        ngram_range: tuple[int, int] = (1, 2),
        diversity: float = 0.5,
    ) -> list[str]:
        """Extract topics/keywords from text.

        Args:
            text: Input text.
            top_n: Number of topics to extract.
            ngram_range: Min/max ngram size for keyphrases.
            diversity: MMR diversity (0=similar, 1=diverse).

        Returns:
            List of topic strings.
        """
        model = self._ensure_model()
        if model is None:
            return []

        try:
            keywords = model.extract_keywords(
                text,
                keyphrase_ngram_range=ngram_range,
                top_n=top_n,
                use_mmr=True,
                diversity=diversity,
            )
            return [kw for kw, _score in keywords]
        except Exception as e:
            logger.warning("KeyBERT extraction failed: %s", e)
            return []

    @property
    def is_available(self) -> bool:
        return self._available


# ─── Zero-Shot Classifier ────────────────────────────────────────────────────


class ZeroShotClassifier:
    """Classify documents against predefined labels using embedding similarity.

    Uses cosine similarity between document embedding and label embeddings.
    Reuses Brain's existing embedding model — zero additional API calls
    if embeddings are already cached.

    Default labels cover ContextUnity service domains.
    """

    DEFAULT_LABELS = [
        "commerce",
        "infrastructure",
        "security",
        "documentation",
        "data engineering",
        "machine learning",
        "web development",
        "devops",
        "api design",
        "database",
        "observability",
        "testing",
    ]

    def __init__(self, labels: list[str] | None = None, embedding_model=None):
        self._labels = labels or self.DEFAULT_LABELS
        self._model = None
        self._embedding_model = embedding_model
        self._label_embeddings: dict[str, list[float]] | None = None
        self._available = True

    def _ensure_model(self):
        """Lazily load sentence-transformers model."""
        if self._model is not None:
            return self._model
        if not self._available:
            return None

        try:
            from sentence_transformers import SentenceTransformer

            model_name = self._embedding_model or "all-MiniLM-L6-v2"
            self._model = SentenceTransformer(model_name)
            logger.info("Zero-shot classifier loaded (model=%s)", model_name)

            # Pre-compute label embeddings (once)
            label_vecs = self._model.encode(self._labels)
            self._label_embeddings = {
                label: vec.tolist() for label, vec in zip(self._labels, label_vecs)
            }
        except ImportError:
            logger.debug("sentence-transformers not installed — zero-shot disabled")
            self._available = False
        except Exception as e:
            logger.warning("Failed to initialize zero-shot classifier: %s", e)
            self._available = False

        return self._model

    @staticmethod
    def _cosine_similarity(a: list[float], b: list[float]) -> float:
        """Compute cosine similarity between two vectors."""
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(x * x for x in b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def classify(
        self,
        text: str,
        top_n: int = 3,
        threshold: float = 0.2,
    ) -> list[tuple[str, float]]:
        """Classify text against predefined labels.

        Args:
            text: Input text.
            top_n: Number of top labels to return.
            threshold: Minimum similarity score to include.

        Returns:
            List of (label, score) tuples, sorted by score descending.
        """
        model = self._ensure_model()
        if model is None or self._label_embeddings is None:
            return []

        try:
            doc_vec = model.encode([text])[0].tolist()
            scores = [
                (label, self._cosine_similarity(doc_vec, label_vec))
                for label, label_vec in self._label_embeddings.items()
            ]
            scores.sort(key=lambda x: x[1], reverse=True)
            return [(label, score) for label, score in scores[:top_n] if score >= threshold]
        except Exception as e:
            logger.warning("Zero-shot classification failed: %s", e)
            return []

    @property
    def is_available(self) -> bool:
        return self._available


# ─── Combined Enricher ───────────────────────────────────────────────────────


class NLPEnricher:
    """Combined NLP enrichment: NER + topic extraction + zero-shot classification.

    Runs all pipelines and returns a unified EnrichmentResult.
    Gracefully degrades if any dependency is missing.
    """

    _instance: Optional["NLPEnricher"] = None

    def __init__(
        self,
        language: str = "en",
        embedding_model=None,
        category_labels: list[str] | None = None,
    ):
        self._ner = EntityExtractor(language=language)
        self._topics = TopicExtractor(embedding_model=embedding_model)
        self._classifier = ZeroShotClassifier(
            labels=category_labels, embedding_model=embedding_model
        )
        self._language = language

    @classmethod
    def get_instance(cls, language: str = "en") -> "NLPEnricher":
        if cls._instance is None:
            cls._instance = cls(language=language)
        return cls._instance

    def enrich(
        self,
        text: str,
        top_n_topics: int = 5,
        classify: bool = True,
    ) -> EnrichmentResult:
        """Run full NLP enrichment on text.

        Args:
            text: Document text to enrich.
            top_n_topics: Number of topics to extract.
            classify: Whether to run zero-shot classification.

        Returns:
            EnrichmentResult with entities, topics, and categories.
        """
        entities = self._ner.extract(text)
        topics = self._topics.extract(text, top_n=top_n_topics)
        categories = self._classifier.classify(text) if classify else []

        return EnrichmentResult(
            entities=entities,
            topics=topics,
            categories=categories,
            language=self._language,
        )

    @property
    def capabilities(self) -> dict[str, bool]:
        """Report which NLP capabilities are available."""
        return {
            "ner": self._ner.is_available,
            "topics": self._topics.is_available,
            "zero_shot": self._classifier.is_available,
        }


__all__ = [
    "Entity",
    "EnrichmentResult",
    "EntityExtractor",
    "TopicExtractor",
    "ZeroShotClassifier",
    "NLPEnricher",
]
