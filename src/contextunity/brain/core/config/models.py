"""Model and LLM configuration."""

from __future__ import annotations

from functools import partial
from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ModelSelectionStrategy = Literal["fallback", "parallel", "cost-priority"]


class ModelSelector(BaseModel):
    """Model selection + fallback for a single component."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")

    model: str
    fallback: list[str] = Field(default_factory=list)
    strategy: ModelSelectionStrategy = "fallback"


def _selector_factory(model: str):
    """`default_factory` must be a zero-arg callable; `partial` is perfect for this.

    Args:
        model (str): The model parameter.
    """
    return partial(ModelSelector, model=model)


class _ModelsGroup(BaseModel):
    """Shared base for model-group configs."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore")


class IngestionModelsConfig(_ModelsGroup):
    """Per-ingestion-stage model configuration (canonical).

    Keep ingestion model choices in core config so ingestion TOML stays about ingestion behavior
    (paths/workers/filters), not model selection.
    """

    taxonomy: ModelSelector = Field(default_factory=_selector_factory("vertex/gemini-2.5-flash"))
    preprocess: ModelSelector = Field(
        default_factory=_selector_factory("vertex/gemini-2.5-flash-lite")
    )
    graph: ModelSelector = Field(default_factory=_selector_factory("vertex/gemini-2.5-pro"))
    persona: ModelSelector = Field(default_factory=_selector_factory("vertex/gemini-2.5-flash"))
    json_model: ModelSelector = Field(default_factory=_selector_factory("vertex/gemini-2.5-flash"))

    @field_validator("json_model")
    @classmethod
    def _require_json_model(cls, v: ModelSelector) -> ModelSelector:
        """require json model.

        Args:
            v (ModelSelector): The v parameter.

        Returns:
            ModelSelector: An instance of ModelSelector.

        Raises:
            ValueError: If parameter values are invalid.
        """
        if not v.model.strip():
            raise ValueError("models.ingestion.json_model.model must be set")
        return v


class ModelsConfig(BaseModel):
    """Configuration settings for ModelsConfig."""

    model_config: ClassVar[ConfigDict] = ConfigDict(extra="ignore", populate_by_name=True)

    default_embeddings: str = "hf/sentence-transformers"

    # Canonical per-component configuration:
    ingestion: IngestionModelsConfig = Field(default_factory=IngestionModelsConfig)
