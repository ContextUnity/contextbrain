"""Portable Archive v1 — Reader and Validator.
Validates archive structure, record schemas, and embedding consistency.
"""

from __future__ import annotations

from pathlib import Path

from contextunity.core import get_contextunit_logger

from .models import EmbeddingRecord, PortableManifest, parse_record

logger = get_contextunit_logger(__name__)


class BrainPortableArchiveReader:
    """Reads and validates a portable archive."""

    def __init__(self, archive_dir: Path):
        """Initialize a new instance of BrainPortableArchiveReader.

        Args:
            archive_dir (Path): The archive dir parameter.
        """
        self.archive_dir: Path = archive_dir
        self.manifest: PortableManifest | None = None

    def validate(self) -> list[str]:
        """Dry-run validation. Returns list of errors (empty = valid).

        Returns:
            list[str]: A list of list[str].
        """
        errors: list[str] = []

        manifest_path = self.archive_dir / "manifest.json"
        if not manifest_path.exists():
            errors.append("Missing manifest.json")
            return errors

        try:
            self.manifest = PortableManifest.model_validate_json(manifest_path.read_text())
        except Exception as e:
            errors.append(f"Invalid manifest: {e}")
            return errors

        records_path = self.archive_dir / "records.jsonl"
        if not records_path.exists():
            errors.append("Missing records.jsonl")
            return errors

        # Validate records
        emb_refs_used: set[str] = set()
        line_count = 0
        with open(records_path) as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = parse_record(line)
                    line_count += 1
                    ref_obj: object = getattr(rec, "embedding_ref", None)
                    if isinstance(ref_obj, str) and ref_obj:
                        emb_refs_used.add(ref_obj)
                except Exception as e:
                    errors.append(f"records.jsonl line {i}: {e}")
                    if len(errors) > 10:
                        errors.append("... truncated (too many errors)")
                        break

        # Validate embedding refs if embeddings.jsonl exists
        emb_path = self.archive_dir / "embeddings.jsonl"
        if emb_refs_used and emb_path.exists():
            emb_refs_found: set[str] = set()
            with open(emb_path) as f:
                for i, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        emb = EmbeddingRecord.model_validate_json(line)
                        emb_refs_found.add(emb.ref)
                        if len(emb.vector) != self.manifest.vector_dim:
                            errors.append(
                                (
                                    f"embeddings.jsonl line {i}: dim={len(emb.vector)}, "
                                    f"expected={self.manifest.vector_dim}"
                                )
                            )
                    except Exception as e:
                        errors.append(f"embeddings.jsonl line {i}: {e}")

            missing = emb_refs_used - emb_refs_found
            if missing:
                errors.append(f"Missing embeddings for {len(missing)} refs: {list(missing)[:5]}...")

        if not errors:
            logger.info(
                "Archive valid: %d records, tenants=%s",
                line_count,
                self.manifest.tenants,
            )
        return errors

    def iter_records(self):
        """Yield parsed records from the archive."""
        records_path = self.archive_dir / "records.jsonl"
        with open(records_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    yield parse_record(line)

    def iter_embeddings(self) -> dict[str, list[float]]:
        """Load embeddings as ref → vector map.

        Returns:
            dict[str, list[float]]: A list of dict[str, list[float]].
        """
        emb_map: dict[str, list[float]] = {}
        emb_path = self.archive_dir / "embeddings.jsonl"
        if not emb_path.exists():
            return emb_map
        with open(emb_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    emb = EmbeddingRecord.model_validate_json(line)
                    emb_map[emb.ref] = emb.vector
        return emb_map


__all__ = ["BrainPortableArchiveReader"]
