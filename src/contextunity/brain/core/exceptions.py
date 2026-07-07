"""Exception hierarchy for contextunity.brain.

Service-specific exceptions with stable error codes for gRPC mapping.
All codes use the ``BRAIN_`` prefix so the prefix-based fallback in
``core/grpc_errors.py`` maps them to ``grpc.StatusCode.INTERNAL`` by default.

Base class and infrastructure exceptions (ConfigurationError, SecurityError, etc.)
live in ``contextunity.core.exceptions`` — import them directly from there.

Usage::

    from contextunity.brain.core.exceptions import (
        ContextbrainError,
        BrainIngestionError,
        EmbeddingError,
    )
    from contextunity.core.exceptions import StorageError
    from contextunity.core.grpc_errors import grpc_error_handler
"""

from __future__ import annotations

from contextunity.core.exceptions import ContextUnityError, register_error
from contextunity.core.faults import POLICY_FAULT


@register_error("BRAIN_ERROR")
class ContextbrainError(ContextUnityError):
    """Base exception for contextunity.brain.

    Inherits from ContextUnityError so that centralized gRPC error handlers
    in contextunity.core catch brain-specific exceptions automatically.
    """

    code: str = "BRAIN_ERROR"
    message: str = "Brain service error"


@register_error("BRAIN_EMBEDDING_FAILED")
class EmbeddingError(ContextbrainError):
    """Failed to generate embeddings."""

    code: str = "BRAIN_EMBEDDING_FAILED"
    message: str = "Failed to generate embeddings"


@register_error("BRAIN_INGESTION_FAILED")
class BrainIngestionError(ContextbrainError):
    """Document ingestion failed."""

    code: str = "BRAIN_INGESTION_FAILED"
    message: str = "Document ingestion failed"


@register_error("BRAIN_STORAGE_UNAVAILABLE")
class BrainStorageError(ContextbrainError):
    """Brain storage is unavailable."""

    code: str = "BRAIN_STORAGE_UNAVAILABLE"
    message: str = "Brain storage is unavailable"


@register_error("BRAIN_PLUGIN_ERROR")
class BrainPluginError(ContextbrainError):
    """Ingestion plugin failed."""

    code: str = "BRAIN_PLUGIN_ERROR"
    message: str = "Ingestion plugin failed"


@register_error("BRAIN_GRAPH_ERROR")
class BrainGraphError(ContextbrainError):
    """Knowledge graph operation failed."""

    code: str = "BRAIN_GRAPH_ERROR"
    message: str = "Knowledge graph operation failed"


@register_error("BRAIN_REGISTRY_ERROR")
class BrainRegistryError(ContextbrainError):
    """Registry lookup or registration failed."""

    code: str = "BRAIN_REGISTRY_ERROR"
    message: str = "Registry lookup or registration failed"


@register_error("BRAIN_VALIDATION_ERROR")
class BrainValidationError(ContextbrainError):
    """Input or data validation failed."""

    code: str = "BRAIN_VALIDATION_ERROR"
    message: str = "Input or data validation failed"


@register_error("BRAIN_SYNAPSES_DISABLED")
class SynapseFeatureDisabledError(ContextbrainError):
    """BrainSynapse RPCs were called while ``brain.yml: synapses.enabled`` is off."""

    code: str = "BRAIN_SYNAPSES_DISABLED"
    message: str = "BrainSynapse RPCs are disabled (brain.yml: synapses.enabled=false)"
    retryable: bool = False


@register_error("BRAIN_SYNAPSE_TENANT_MISMATCH")
class SynapseTenantMismatchError(ContextbrainError):
    """A Synapse payload's ``tenant_id`` contradicts the caller's token scope.

    Tenant spoofing is a ``policy_fault`` — it never touches Q-values and is
    never retryable.
    """

    code: str = "BRAIN_SYNAPSE_TENANT_MISMATCH"
    message: str = "Synapse payload tenant_id does not match token scope"
    fault_class: str = POLICY_FAULT
    event_type: str = "synapse.tenant_mismatch"
    retryable: bool = False


@register_error("BRAIN_SYNAPSE_DECAY_DISABLED")
class SynapseDecayDisabledError(ContextbrainError):
    """``decay_synapses`` was called while ``brain.yml: synapses.decay_enabled`` is off.

    The method exists as a stable future call site for Q-decay, but it must
    fail loudly rather than silently no-op while disabled or unimplemented.
    """

    code: str = "BRAIN_SYNAPSE_DECAY_DISABLED"
    message: str = "Synapse decay is disabled (brain.yml: synapses.decay_enabled=false)"
    retryable: bool = False
