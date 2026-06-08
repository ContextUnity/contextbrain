"""RAG ingestion module (staged pipeline + helpers).

This package includes optional heavy dependencies (parsing, graph building, etc.).
To keep `contextunity.brain --help` and non-ingestion usage lightweight, we lazy-import
public symbols on demand (similar to `modules.retrieval.rag`).
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

from contextunity.core.narrowing import object_attr

if TYPE_CHECKING:
    from collections.abc import Callable

    from .config import (
        ensure_directories_exist,
        get_assets_paths,
        get_plugin_source_dir,
        load_config,
    )
    from .core.batch import BatchResult, batch_transform, batch_validate, chunked, filter_by_indices
    from .core.loaders import (
        FileLoaderMixin,
        LoadedFile,
        iter_files,
        load_text_files,
        read_text_file,
    )
    from .core.plugins import IngestionPlugin
    from .core.prompts import (
        qa_rephrase_question_prompt,
        qa_validate_answer_prompt,
        qa_validate_question_prompt,
        video_validate_segment_prompt,
    )
    from .core.registry import get_all_plugins, get_plugin_class, register_plugin
    from .core.types import IngestionMetadata, RawData, ShadowRecord
    from .core.utils import (
        normalize_ambiguous_unicode,
        normalize_clean_text,
        parallel_map,
        resolve_workers,
    )
    from .settings import RagIngestionConfig
    from .stages.deploy import deploy_jsonl_files
    from .stages.export import export_jsonl_per_type
    from .stages.persona import build_persona
    from .stages.preprocess import preprocess_to_clean_text
    from .stages.report import build_ingestion_report
    from .stages.store import read_raw_data_jsonl, write_shadow_records_jsonl

    build_graph_from_clean_text: Callable[..., object]
    build_ontology_from_taxonomy: Callable[..., object]
    build_shadow_records: Callable[..., object]
    build_taxonomy_from_clean_text: Callable[..., object]

__all__ = [
    # BrainConfig
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

_P = "contextunity.brain.ingestion.rag"  # package prefix
_T = "contextunity.brain.transformers"  # transformers prefix

_EXPORTS: dict[str, str] = {
    # BrainConfig
    "ensure_directories_exist": f"{_P}.config.ensure_directories_exist",
    "get_assets_paths": f"{_P}.config.get_assets_paths",
    "get_plugin_source_dir": f"{_P}.config.get_plugin_source_dir",
    "load_config": f"{_P}.config.load_config",
    "RagIngestionConfig": f"{_P}.settings.RagIngestionConfig",
    # Core
    "IngestionMetadata": f"{_P}.core.types.IngestionMetadata",
    "IngestionPlugin": f"{_P}.core.plugins.IngestionPlugin",
    "RawData": f"{_P}.core.types.RawData",
    "ShadowRecord": f"{_P}.core.types.ShadowRecord",
    "get_all_plugins": f"{_P}.core.registry.get_all_plugins",
    "get_plugin_class": f"{_P}.core.registry.get_plugin_class",
    "register_plugin": f"{_P}.core.registry.register_plugin",
    # Core batch
    "BatchResult": f"{_P}.core.batch.BatchResult",
    "batch_transform": f"{_P}.core.batch.batch_transform",
    "batch_validate": f"{_P}.core.batch.batch_validate",
    "chunked": f"{_P}.core.batch.chunked",
    "filter_by_indices": f"{_P}.core.batch.filter_by_indices",
    # Core loaders
    "FileLoaderMixin": f"{_P}.core.loaders.FileLoaderMixin",
    "LoadedFile": f"{_P}.core.loaders.LoadedFile",
    "iter_files": f"{_P}.core.loaders.iter_files",
    "load_text_files": f"{_P}.core.loaders.load_text_files",
    "read_text_file": f"{_P}.core.loaders.read_text_file",
    # Core prompts
    "qa_rephrase_question_prompt": f"{_P}.core.prompts.qa_rephrase_question_prompt",
    "qa_validate_answer_prompt": f"{_P}.core.prompts.qa_validate_answer_prompt",
    "qa_validate_question_prompt": f"{_P}.core.prompts.qa_validate_question_prompt",
    "video_validate_segment_prompt": f"{_P}.core.prompts.video_validate_segment_prompt",
    # Core utils
    "normalize_ambiguous_unicode": f"{_P}.core.utils.normalize_ambiguous_unicode",
    "normalize_clean_text": f"{_P}.core.utils.normalize_clean_text",
    "parallel_map": f"{_P}.core.utils.parallel_map",
    # Utils
    "resolve_workers": f"{_P}.core.utils.resolve_workers",
    # Transformers
    "build_graph_from_clean_text": f"{_T}.graph.build_graph_from_clean_text",
    "build_ontology_from_taxonomy": f"{_T}.ontology.build_ontology_from_taxonomy",
    "build_shadow_records": f"{_T}.shadow.build_shadow_records",
    "build_taxonomy_from_clean_text": f"{_T}.taxonomy.build_taxonomy_from_clean_text",
    # Stages
    "deploy_jsonl_files": f"{_P}.stages.deploy.deploy_jsonl_files",
    "export_jsonl_per_type": f"{_P}.stages.export.export_jsonl_per_type",
    "build_persona": f"{_P}.stages.persona.build_persona",
    "preprocess_to_clean_text": f"{_P}.stages.preprocess.preprocess_to_clean_text",
    "build_ingestion_report": f"{_P}.stages.report.build_ingestion_report",
    "enrich_clean_text": f"{_P}.stages.enrich.enrich_clean_text",
    "read_raw_data_jsonl": f"{_P}.stages.store.read_raw_data_jsonl",
    "write_shadow_records_jsonl": f"{_P}.stages.store.write_shadow_records_jsonl",
}


def __getattr__(name: str) -> object:
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
            + "`pip install 'contextunity.brain[ingestion]'` (or `contextunity.brain[all]`)."
        ) from e
    attr_obj = object_attr(mod, attr)
    return attr_obj
