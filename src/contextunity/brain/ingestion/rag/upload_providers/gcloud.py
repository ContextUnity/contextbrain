"""Google Cloud upload provider: GCS + Vertex AI Search."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import override

from contextunity.core import get_contextunit_logger
from contextunity.core.exceptions import ConfigurationError
from contextunity.core.narrowing import as_str
from contextunity.core.types import JsonDict

from contextunity.brain.core import get_core_config, get_env
from contextunity.brain.ingestion.rag.protocols import (
    discovery_engine_v1_bindings,
    gcs_storage_client,
)

from .base import (
    UploadProvider,
    UploadResult,
)

logger = get_contextunit_logger(__name__)


def _resolve_data_store_id(symbolic: str | None = None) -> str:
    """Resolve blue/green symbolic name or default data store ID from env.

    Args:
        symbolic: Optional symbolic name ('blue' or 'green') to resolve.
            If None, returns the default data store ID from env.

    Returns:
        Resolved data store ID.

    Raises:
        ValueError: If no data store ID can be resolved.
    """
    if symbolic and symbolic.lower() in ("blue", "green"):
        env_key = f"RAG_DATASTORE_{symbolic.upper()}"
        ds_id = get_env(env_key)
        if ds_id:
            return ds_id
        raise ConfigurationError(
            f"Cannot resolve symbolic data store '{symbolic}': env var {env_key} not set"
        )

    # Default: try common env vars
    for key in ("RAG_DATASTORE_ID", "CU_BRAIN_DATASTORE_ID", "VERTEX_DATASTORE_ID"):
        ds_id = get_env(key)
        if ds_id:
            return ds_id

    raise ConfigurationError(
        "Cannot resolve data store ID: set RAG_DATASTORE_ID or provide in config"
    )


class GCloudUploadProvider(UploadProvider):
    """Upload provider for Google Cloud (GCS + Vertex AI Search).

    Configuration priority (highest to lowest):
    1. Explicit config values in settings.toml [upload.gcloud]
    2. Environment variables (VERTEX_PROJECT_ID, VERTEX_LOCATION, RAG_GCS_BUCKET, RAG_DB_NAME)

    Supports blue/green symbolic names for data_store_id.
    """

    _project_id: str | None
    _location: str | None
    _gcs_bucket: str | None
    _data_store_id: str | None

    def __init__(self, config: JsonDict) -> None:
        """Initialize GCloud provider.

        Args:
            config: Provider-specific config from [upload.gcloud] section
        """
        _ = get_core_config()
        super().__init__(config)

        self._project_id = (
            as_str(config.get("project_id"))
            or get_env("VERTEX_PROJECT_ID")
            or get_env("CU_BRAIN_VERTEX_PROJECT_ID")
            or None
        )
        self._location = (
            as_str(config.get("location"))
            or get_env("VERTEX_LOCATION", "global")
            or get_env("CU_BRAIN_VERTEX_LOCATION", "global")
            or None
        )
        self._gcs_bucket = as_str(config.get("gcs_bucket")) or get_env("RAG_GCS_BUCKET") or None

        config_ds_id = as_str(config.get("data_store_id"))
        if config_ds_id:
            self._data_store_id = self._resolve_symbolic(config_ds_id)
        else:
            self._data_store_id = None

    def _resolve_symbolic(self, ds_id: str) -> str:
        """Resolve symbolic blue/green to actual datastore ID."""
        ds_lower = ds_id.lower().strip()
        if ds_lower in ("blue", "green"):
            return _resolve_data_store_id(ds_lower)
        return ds_id

    @property
    @override
    def name(self) -> str:
        """Name."""
        return "gcloud"

    def _get_data_store_id(self) -> str:
        """Get data store ID, resolving at runtime if needed."""
        if self._data_store_id:
            return self._data_store_id
        return _resolve_data_store_id()

    @override
    def upload_and_index(
        self,
        local_path: Path,
        *,
        wait: bool = False,
    ) -> UploadResult:
        """Upload JSONL to GCS and trigger Vertex AI Search import."""
        from google.api_core.client_options import ClientOptions

        de = discovery_engine_v1_bindings()

        if not self._project_id:
            return UploadResult(
                success=False,
                provider=self.name,
                details={},
                error="project_id not set (config or VERTEX_PROJECT_ID env)",
            )
        if not self._gcs_bucket:
            return UploadResult(
                success=False,
                provider=self.name,
                details={},
                error="gcs_bucket not set (config or RAG_GCS_BUCKET env)",
            )
        if not local_path.exists():
            return UploadResult(
                success=False,
                provider=self.name,
                details={},
                error=f"File not found: {local_path}",
            )

        try:
            data_store_id = self._get_data_store_id()
        except ConfigurationError as e:
            return UploadResult(
                success=False,
                provider=self.name,
                details={},
                error=f"Failed to resolve data_store_id: {e}",
            )

        logger.info(
            "GCloud upload: project=%s, bucket=%s, datastore=%s, location=%s",
            self._project_id,
            self._gcs_bucket,
            data_store_id,
            self._location,
        )

        try:
            date_folder = datetime.now(timezone.utc).strftime("%Y%m%d")
            blob_name = f"ingestion/{date_folder}/{local_path.name}"

            logger.info(
                "Uploading %s to gs://%s/%s ...", local_path.name, self._gcs_bucket, blob_name
            )
            storage_client = gcs_storage_client()
            bucket = storage_client.bucket(self._gcs_bucket)
            blob = bucket.blob(blob_name)
            _ = blob.upload_from_filename(str(local_path), content_type="application/json")
            gcs_uri = f"gs://{self._gcs_bucket}/{blob_name}"
            logger.info("Uploaded to %s", gcs_uri)

            logger.info(
                "Triggering import to datastore '%s' in location '%s'...",
                data_store_id,
                self._location,
            )

            location = self._location or "global"
            client_options = (
                ClientOptions(api_endpoint=f"{location}-discoveryengine.googleapis.com")
                if location != "global"
                else None
            )
            de_client = de.document_service_client(client_options=client_options)

            parent = de_client.branch_path(
                project=self._project_id,
                location=location,
                data_store=data_store_id,
                branch="default_branch",
            )

            request = de.import_documents_request(
                parent=parent,
                gcs_source=de.gcs_source(input_uris=[gcs_uri]),
                reconciliation_mode=de.reconciliation_incremental,
            )

            operation = de_client.import_documents(request=request)
            op_name = operation.operation.name
            logger.info("Import operation started: %s", op_name)

            if wait:
                logger.info("Waiting for import to complete (this may take several minutes)...")
                response = operation.result(timeout=3600)
                logger.info("Import completed successfully!")
                logger.info("Import result: %s", response)

            return UploadResult(
                success=True,
                provider=self.name,
                details={
                    "operation_name": op_name,
                    "gcs_uri": gcs_uri,
                    "data_store_id": data_store_id,
                    "date": date_folder,
                    "project_id": self._project_id,
                    "location": location,
                },
            )

        except Exception as e:
            logger.exception("GCloud upload failed")
            return UploadResult(
                success=False,
                provider=self.name,
                details={},
                error=str(e),
            )

    @override
    def get_config_summary(self) -> dict[str, str]:
        """Get summary of current configuration for logging."""
        try:
            ds_id = self._get_data_store_id()
        except ConfigurationError:
            ds_id = "<unresolved>"

        return {
            "provider": self.name,
            "project_id": as_str(self._project_id) or "<not set>",
            "location": as_str(self._location) or "<not set>",
            "gcs_bucket": as_str(self._gcs_bucket) or "<not set>",
            "data_store_id": ds_id,
        }
