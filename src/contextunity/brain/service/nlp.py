"""NLP enrichment pipeline for document ingestion.
Provides entity extraction (spaCy NER) and topic/keyword extraction (KeyBERT)
to enrich documents during ingestion. Both are optional — gracefully degrade
if dependencies are not installed.
Usage:
    enricher = NLPEnricher()
    result = enricher.enrich(text)
"""

from __future__ import annotations

import importlib
import math
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import ClassVar, Protocol, final

from contextunity.core import get_contextunit_logger
from contextunity.core.narrowing import object_attr
from contextunity.core.types import is_object_iterable, is_object_list, is_object_pair

logger = get_contextunit_logger(__name__)


class _SpacyLanguage(Protocol):
    def __call__(self, text: str) -> _SpacyDocAdapter: ...


class _KeyBERTModel(Protocol):
    def extract_keywords(
        self,
        text: str,
        *,
        keyphrase_ngram_range: tuple[int, int],
        top_n: int,
        use_mmr: bool,
        diversity: float,
    ) -> Sequence[tuple[str, float]]: ...


class _EmbeddingVector(Protocol):
    def tolist(self) -> list[float]: ...


class _SentenceTransformer(Protocol):
    def encode(self, sentences: list[str]) -> Sequence[_EmbeddingVector]: ...


@final
class _SpacyEntityAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    @property
    def text(self) -> str:
        value: object = object_attr(self._inner, "text")
        return value if isinstance(value, str) else ""

    @property
    def label_(self) -> str:
        value: object = object_attr(self._inner, "label_")
        return value if isinstance(value, str) else ""

    @property
    def start_char(self) -> int:
        value: object = object_attr(self._inner, "start_char")
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else 0

    @property
    def end_char(self) -> int:
        value: object = object_attr(self._inner, "end_char")
        return int(value) if isinstance(value, int) and not isinstance(value, bool) else 0


@final
class _SpacyDocAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    @property
    def ents(self) -> tuple[_SpacyEntityAdapter, ...]:
        ents_obj: object = object_attr(self._inner, "ents")
        if not is_object_iterable(ents_obj):
            return ()
        return tuple(_SpacyEntityAdapter(ent) for ent in ents_obj)


@final
class _SpacyLanguageAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def __call__(self, text: str) -> _SpacyDocAdapter:
        call_fn_obj: object = object_attr(self._inner, "__call__")
        if not callable(call_fn_obj):
            raise TypeError("spaCy language model is not callable")
        call_fn: Callable[[str], object] = call_fn_obj
        doc_obj = call_fn(text)
        return _SpacyDocAdapter(doc_obj)


@final
class _EmbeddingVectorAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def tolist(self) -> list[float]:
        tolist_fn_obj: object = object_attr(self._inner, "tolist")
        if not callable(tolist_fn_obj):
            raise TypeError("embedding vector missing tolist()")
        tolist_fn: Callable[[], object] = tolist_fn_obj
        raw_obj = tolist_fn()
        if not is_object_list(raw_obj):
            raise TypeError("tolist() did not return a list")
        values: list[float] = []
        for value in raw_obj:
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values.append(float(value))
        return values


@final
class _SentenceTransformerAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def encode(self, sentences: list[str]) -> Sequence[_EmbeddingVector]:
        encode_fn_obj: object = object_attr(self._inner, "encode")
        if not callable(encode_fn_obj):
            raise TypeError("sentence-transformers model missing encode()")
        encode_fn: Callable[[list[str]], object] = encode_fn_obj
        vectors_obj = encode_fn(sentences)
        if not is_object_iterable(vectors_obj):
            raise TypeError("encode() did not return a sequence")
        return [_EmbeddingVectorAdapter(vector) for vector in vectors_obj]


@final
class _KeyBERTAdapter:
    _inner: object

    def __init__(self, inner: object) -> None:
        self._inner = inner

    def extract_keywords(
        self,
        text: str,
        *,
        keyphrase_ngram_range: tuple[int, int],
        top_n: int,
        use_mmr: bool,
        diversity: float,
    ) -> Sequence[tuple[str, float]]:
        extract_fn_obj: object = object_attr(self._inner, "extract_keywords")
        if not callable(extract_fn_obj):
            raise TypeError("KeyBERT model missing extract_keywords()")
        extract_fn: Callable[..., object] = extract_fn_obj
        raw_obj = extract_fn(
            text,
            keyphrase_ngram_range=keyphrase_ngram_range,
            top_n=top_n,
            use_mmr=use_mmr,
            diversity=diversity,
        )
        if not is_object_iterable(raw_obj):
            return ()
        pairs: list[tuple[str, float]] = []
        for item in raw_obj:
            if not is_object_pair(item):
                continue
            keyword_obj: object = item[0]
            score_obj: object = item[1]
            if isinstance(keyword_obj, str) and isinstance(score_obj, (int, float)):
                pairs.append((keyword_obj, float(score_obj)))
        return pairs


def _load_spacy_language(model_name: str) -> _SpacyLanguage | None:
    try:
        spacy_mod = importlib.import_module("spacy")
    except ImportError:
        return None
    load_fn_obj: object = object_attr(spacy_mod, "load")
    if not callable(load_fn_obj):
        return None
    load_fn: Callable[[str], object] = load_fn_obj
    loaded = load_fn(model_name)
    return _SpacyLanguageAdapter(loaded)


def _download_spacy_model(model_name: str) -> bool:
    try:
        spacy_mod = importlib.import_module("spacy")
    except ImportError:
        return False
    cli_obj: object = object_attr(spacy_mod, "cli")
    download_fn_obj: object = object_attr(cli_obj, "download")
    if not callable(download_fn_obj):
        return False
    download_fn: Callable[[str], object] = download_fn_obj
    _: object = download_fn(model_name)
    return True


def _load_keybert(embedding_model: object | None) -> _KeyBERTModel | None:
    try:
        keybert_mod = importlib.import_module("keybert")
    except ImportError:
        return None
    ctor_obj: object = object_attr(keybert_mod, "KeyBERT")
    if not callable(ctor_obj):
        return None
    ctor: Callable[..., object] = ctor_obj
    loaded = ctor(model=embedding_model)
    return _KeyBERTAdapter(loaded)


def _load_sentence_transformer(model_name: str) -> _SentenceTransformer | None:
    try:
        st_mod = importlib.import_module("sentence_transformers")
    except ImportError:
        return None
    ctor_obj: object = object_attr(st_mod, "SentenceTransformer")
    if not callable(ctor_obj):
        return None
    ctor: Callable[[str], object] = ctor_obj
    loaded = ctor(model_name)
    encode_fn_obj: object = object_attr(loaded, "encode")
    if not callable(encode_fn_obj):
        return None
    return _SentenceTransformerAdapter(loaded)


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
        """Group entities by label. E.g., {"ORG": ["Traverse", "Abris"]}.

        Returns:
            dict[str, list[str]]: A list of dict[str, list[str]].
        """
        groups: dict[str, list[str]] = {}
        for e in self.entities:
            groups.setdefault(e.label, []).append(e.text)
        return groups

    @property
    def top_category(self) -> str | None:
        """Return highest-scoring category label.

        Returns:
            str | None: An instance of str | None.
        """
        return self.categories[0][0] if self.categories else None

    def to_metadata(self) -> dict[str, object]:
        """Convert to flat metadata dict for storage.

        Returns:
            dict: The dictionary payload containing results.
        """
        meta: dict[str, object] = {}
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

    MODELS: ClassVar[dict[str, str]] = {
        "en": "en_core_web_sm",
        "uk": "uk_core_news_sm",
    }

    def __init__(self, language: str = "en") -> None:
        """Initialize a new instance of EntityExtractor."""
        self._language: str = language
        self._nlp: _SpacyLanguage | None = None
        self._available: bool = True

    def _ensure_model(self) -> _SpacyLanguage | None:
        """Lazily load spaCy model, download if needed."""
        if self._nlp is not None:
            return self._nlp
        if not self._available:
            return None

        model_name = self.MODELS.get(self._language, "en_core_web_sm")

        try:
            loaded = _load_spacy_language(model_name)
            if loaded is not None:
                self._nlp = loaded
                logger.info("spaCy model loaded: %s", model_name)
            else:
                raise OSError(f"spaCy model not available: {model_name}")
        except OSError:
            logger.info("Downloading spaCy model: %s", model_name)
            try:
                if not _download_spacy_model(model_name):
                    raise OSError(f"spacy.cli.download not available for {model_name}")
                loaded = _load_spacy_language(model_name)
                if loaded is None:
                    raise OSError(f"spaCy model not available after download: {model_name}")
                self._nlp = loaded
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

        if len(text) > max_length:
            text = text[:max_length]

        doc = nlp(text)
        entities: list[Entity] = []
        seen: set[tuple[str, str]] = set()

        for ent in doc.ents:
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
        """Check if the available condition is satisfied.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        return self._available


# ─── Topic Extractor (KeyBERT) ───────────────────────────────────────────────


class TopicExtractor:
    """Keyword/topic extraction using KeyBERT.

    Can optionally reuse Brain's existing embedder for consistency.
    Falls back gracefully if KeyBERT is not installed.
    """

    def __init__(self, embedding_model: object | None = None) -> None:
        """Initialize topic extractor."""
        self._kw_model: _KeyBERTModel | None = None
        self._embedding_model: object | None = embedding_model
        self._available: bool = True

    def _ensure_model(self) -> _KeyBERTModel | None:
        """Lazily load KeyBERT model."""
        if self._kw_model is not None:
            return self._kw_model
        if not self._available:
            return None

        try:
            loaded = _load_keybert(self._embedding_model)
            if loaded is None:
                raise ImportError("KeyBERT not installed")
            self._kw_model = loaded
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
        """Check if the available condition is satisfied.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        return self._available


# ─── Zero-Shot Classifier ────────────────────────────────────────────────────


class ZeroShotClassifier:
    """Classify documents against predefined labels using embedding similarity.

    Uses cosine similarity between document embedding and label embeddings.
    Reuses Brain's existing embedding model — zero additional API calls
    if embeddings are already cached.

    Default labels cover ContextUnity service domains.
    """

    DEFAULT_LABELS: ClassVar[list[str]] = [
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

    def __init__(
        self,
        labels: list[str] | None = None,
        embedding_model: object | None = None,
    ) -> None:
        """Initialize a new instance of ZeroShotClassifier."""
        self._labels: list[str] = labels or list(self.DEFAULT_LABELS)
        self._model: _SentenceTransformer | None = None
        self._embedding_model: object | None = embedding_model
        self._label_embeddings: dict[str, list[float]] | None = None
        self._available: bool = True

    def _ensure_model(self) -> _SentenceTransformer | None:
        """Lazily load sentence-transformers model."""
        if self._model is not None:
            return self._model
        if not self._available:
            return None

        try:
            model_name = (
                self._embedding_model
                if isinstance(self._embedding_model, str)
                else "all-MiniLM-L6-v2"
            )
            loaded = _load_sentence_transformer(model_name)
            if loaded is None:
                raise ImportError("sentence-transformers not installed")
            self._model = loaded
            logger.info("Zero-shot classifier loaded (model=%s)", model_name)

            label_vecs = loaded.encode(self._labels)
            self._label_embeddings = {
                label: vec.tolist() for label, vec in zip(self._labels, label_vecs, strict=True)
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
        """Compute cosine similarity between two vectors.

        Args:
            a (list[float]): The a parameter.
            b (list[float]): The b parameter.

        Returns:
            float: An instance of float.
        """
        dot = sum(x * y for x, y in zip(a, b, strict=False))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a == 0.0 or norm_b == 0.0:
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
        """Check if the available condition is satisfied.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        return self._available


# ─── Combined Enricher ───────────────────────────────────────────────────────


class NLPEnricher:
    """Combined NLP enrichment: NER + topic extraction + zero-shot classification.

    Runs all pipelines and returns a unified EnrichmentResult.
    Gracefully degrades if any dependency is missing.
    """

    _instance: NLPEnricher | None = None

    def __init__(
        self,
        language: str = "en",
        embedding_model: object | None = None,
        category_labels: list[str] | None = None,
    ) -> None:
        """Initialize a new instance of NLPEnricher."""
        self._ner: EntityExtractor = EntityExtractor(language=language)
        self._topics: TopicExtractor = TopicExtractor(embedding_model=embedding_model)
        self._classifier: ZeroShotClassifier = ZeroShotClassifier(
            labels=category_labels, embedding_model=embedding_model
        )
        self._language: str = language

    @classmethod
    def get_instance(cls, language: str = "en") -> NLPEnricher:
        """Retrieve the instance information.

        Args:
            language (str): The language parameter.

        Returns:
            NLPEnricher: An instance of NLPEnricher.
        """
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
        """Report which NLP capabilities are available.

        Returns:
            dict[str, bool]: A dictionary containing the results.
        """
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
