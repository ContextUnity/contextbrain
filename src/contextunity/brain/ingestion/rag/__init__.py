"""RAG ingestion module (staged pipeline + helpers).

This package includes optional heavy dependencies (parsing, graph building, etc.).
To keep `cu.brain --help` and non-ingestion usage lightweight, we lazy-import
public symbols on demand (similar to `modules.retrieval.rag`).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .core import (
        IngestionMetadata,
        IngestionPlugin,
        RawData,
        ShadowRecord,
    )
    from .settings import RagIngestionConfig

__all__ = [
    # Config
    "ensure_directories_exist",
    "get_assets_paths",
    "get_plugin_source_dir",
    "load_config",
    "RagIngestionConfig",
    # Core types / registry
    "IngestionMetadata",
    "IngestionPlugin",
    "RawData",
    "ShadowRecord",
    "get_all_plugins",
    "get_plugin_class",
    "register_plugin",
    # Core batch
    "BatchResult",
    "batch_transform",
    "batch_validate",
    "chunked",
    "filter_by_indices",
    # Core loaders
    "FileLoaderMixin",
    "LoadedFile",
    "iter_files",
    "load_text_files",
    "read_text_file",
    # Core prompts
    "qa_rephrase_question_prompt",
    "qa_validate_answer_prompt",
    "qa_validate_question_prompt",
    "video_validate_segment_prompt",
    # Core utils
    "normalize_ambiguous_unicode",
    "normalize_clean_text",
    "parallel_map",
    "resolve_workers",
    # Stages / pipeline functions
    "preprocess_to_clean_text",
    "build_persona",
    "build_taxonomy_from_clean_text",
    "build_ontology_from_taxonomy",
    "build_graph_from_clean_text",
    "build_shadow_records",
    "export_jsonl_per_type",
    "deploy_jsonl_files",
    "build_ingestion_report",
    "read_raw_data_jsonl",
    "write_shadow_records_jsonl",
]

_EXPORTS: dict[str, str] = {
    # Config
    "ensure_directories_exist": "contextunity.brain.ingestion.rag.config.ensure_directories_exist",
    "get_assets_paths": "contextunity.brain.ingestion.rag.config.get_assets_paths",
    "get_plugin_source_dir": "contextunity.brain.ingestion.rag.config.get_plugin_source_dir",
    "load_config": "contextunity.brain.ingestion.rag.config.load_config",
    "RagIngestionConfig": "contextunity.brain.ingestion.rag.settings.RagIngestionConfig",
    # Core
    "IngestionMetadata": "contextunity.brain.ingestion.rag.core.types.IngestionMetadata",
    "IngestionPlugin": "contextunity.brain.ingestion.rag.core.plugins.IngestionPlugin",
    "RawData": "contextunity.brain.ingestion.rag.core.types.RawData",
    "ShadowRecord": "contextunity.brain.ingestion.rag.core.types.ShadowRecord",
    "get_all_plugins": "contextunity.brain.ingestion.rag.core.plugins.get_all_plugins",
    "get_plugin_class": "contextunity.brain.ingestion.rag.core.plugins.get_plugin_class",
    "register_plugin": "contextunity.brain.ingestion.rag.core.plugins.register_plugin",
    # Core batch
    "BatchResult": "contextunity.brain.ingestion.rag.core.batch.BatchResult",
    "batch_transform": "contextunity.brain.ingestion.rag.core.batch.batch_transform",
    "batch_validate": "contextunity.brain.ingestion.rag.core.batch.batch_validate",
    "chunked": "contextunity.brain.ingestion.rag.core.batch.chunked",
    "filter_by_indices": "contextunity.brain.ingestion.rag.core.batch.filter_by_indices",
    # Core loaders
    "FileLoaderMixin": "contextunity.brain.ingestion.rag.core.loaders.FileLoaderMixin",
    "LoadedFile": "contextunity.brain.ingestion.rag.core.loaders.LoadedFile",
    "iter_files": "contextunity.brain.ingestion.rag.core.loaders.iter_files",
    "load_text_files": "contextunity.brain.ingestion.rag.core.loaders.load_text_files",
    "read_text_file": "contextunity.brain.ingestion.rag.core.loaders.read_text_file",
    # Core prompts
    "qa_rephrase_question_prompt": "contextunity.brain.ingestion.rag.core.prompts.qa_rephrase_question_prompt",
    "qa_validate_answer_prompt": "contextunity.brain.ingestion.rag.core.prompts.qa_validate_answer_prompt",
    "qa_validate_question_prompt": "contextunity.brain.ingestion.rag.core.prompts.qa_validate_question_prompt",
    "video_validate_segment_prompt": "contextunity.brain.ingestion.rag.core.prompts.video_validate_segment_prompt",
    # Core utils
    "normalize_ambiguous_unicode": "contextunity.brain.ingestion.rag.core.utils.normalize_ambiguous_unicode",
    "normalize_clean_text": "contextunity.brain.ingestion.rag.core.utils.normalize_clean_text",
    "parallel_map": "contextunity.brain.ingestion.rag.core.utils.parallel_map",
    # Utils
    "resolve_workers": "contextunity.brain.ingestion.rag.core.utils.resolve_workers",
    # Transformers
    "build_graph_from_clean_text": "contextunity.brain.transformers.graph.build_graph_from_clean_text",
    "build_ontology_from_taxonomy": "contextunity.brain.transformers.ontology.build_ontology_from_taxonomy",
    "build_shadow_records": "contextunity.brain.transformers.shadow.build_shadow_records",
    "build_taxonomy_from_clean_text": "contextunity.brain.transformers.taxonomy.build_taxonomy_from_clean_text",
    # Stages
    "deploy_jsonl_files": "contextunity.brain.ingestion.rag.stages.deploy.deploy_jsonl_files",
    "export_jsonl_per_type": "contextunity.brain.ingestion.rag.stages.export.export_jsonl_per_type",
    "build_persona": "contextunity.brain.ingestion.rag.stages.persona.build_persona",
    "preprocess_to_clean_text": "contextunity.brain.ingestion.rag.stages.preprocess.preprocess_to_clean_text",
    "build_ingestion_report": "contextunity.brain.ingestion.rag.stages.report.build_ingestion_report",
    "enrich_clean_text": "contextunity.brain.ingestion.rag.stages.enrich.enrich_clean_text",
    "read_raw_data_jsonl": "contextunity.brain.ingestion.rag.stages.store.read_raw_data_jsonl",
    "write_shadow_records_jsonl": "contextunity.brain.ingestion.rag.stages.store.write_shadow_records_jsonl",
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(name)
    path = _EXPORTS[name]
    mod_name, attr = path.rsplit(".", 1)
    try:
        mod = importlib.import_module(mod_name)
    except ModuleNotFoundError as e:
        # Provide a more helpful error for optional ingestion deps.
        raise ModuleNotFoundError(
            f"{e}. You may need to install ingestion extras: "
            "`pip install 'contextunity.brain[ingestion]'` (or `cu.brain[all]`)."
        ) from e
    return getattr(mod, attr)
