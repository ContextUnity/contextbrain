"""SQLite DDL schema management for local Brain backend."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from uuid import UUID

from contextunity.core import get_contextunit_logger
from contextunity.core.types import JsonDict, is_json_dict

logger = get_contextunit_logger(__name__)

# Current schema version — bump when adding tables/columns.
SCHEMA_VERSION = 12

# CP-1 breaking preflight rename map (legacy -> canonical), mirrors the
# Postgres preflight in `postgres/schema.py`. A developer's persistent
# `~/.contextunity/brain_local.sqlite3` predates this rename, so it must be
# converted in place — otherwise `CREATE TABLE IF NOT EXISTS cells` would
# create an empty canonical table alongside the untouched legacy one.
_TABLE_RENAMES: tuple[tuple[str, str], ...] = (
    ("blackboard_records", "blackboard"),
    ("knowledge_nodes", "cells"),
    ("knowledge_edges", "cell_edges"),
    ("agent_traces", "event_journal"),
    # sqlite-vec virtual table mirroring the `cells` rename above — without
    # this, an existing local DB's vector data stays orphaned in
    # `vec_knowledge_nodes` while `build_vector_ddl()` creates a new, empty
    # `vec_cells` and all search/insert code reads/writes only that one.
    ("vec_knowledge_nodes", "vec_cells"),
)
_COLUMN_RENAMES: tuple[tuple[str, str, str], ...] = (
    ("cells", "taxonomy_path", "scope_path"),
    ("cells", "node_kind", "cell_kind"),
)

# Event Journal v0 columns (storage only — no public Event Journal RPCs yet),
# added to Postgres by its preflight migration. Backfilled here the same way
# for an existing local SQLite DB that predates them; the CREATE TABLE below
# already includes them for a fresh DB.
# ``event_id`` has no UNIQUE constraint in SQLite: ``ALTER TABLE ADD COLUMN``
# does not support adding a UNIQUE constraint after table creation (a SQLite
# limitation, not an oversight), and nothing writes/reads this column yet.
_EXECUTION_TRACE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("graph_run_id", "TEXT"),
    ("payload_digest", "TEXT"),
    ("terminal_status", "TEXT"),
    ("terminal_reason", "TEXT"),
    ("trace_schema_version", "TEXT NOT NULL DEFAULT 'legacy_v0'"),
    ("prompt_evidence", "TEXT NOT NULL DEFAULT '[]'"),
    ("steps", "TEXT NOT NULL DEFAULT '[]'"),
    ("control_evidence", "TEXT NOT NULL DEFAULT '{}'"),
    ("final_verdict", "TEXT NOT NULL DEFAULT '{}'"),
)


def _rebuild_legacy_trace_table(db: sqlite3.Connection) -> None:
    """Map recognized legacy trace rows and reject generic-event semantics."""
    columns = {row[1] for row in db.execute("PRAGMA table_info(event_journal)").fetchall()}
    generic = {"event_type", "severity", "status", "payload", "source_refs"}
    if generic.issubset(columns):
        unknown = db.execute(
            """
            SELECT 1 FROM event_journal
            WHERE event_type <> 'trace.logged'
               OR severity <> 'info'
               OR status <> 'recorded'
               OR payload NOT IN ('{}', '')
               OR source_refs NOT IN ('[]', '')
            LIMIT 1
            """
        ).fetchone()
        if unknown is not None:
            raise sqlite3.IntegrityError("unmapped generic event row blocks trace migration")
    legacy_defaults: tuple[tuple[str, str], ...] = (
        ("session_id", "TEXT"),
        ("user_id", "TEXT"),
        ("graph_name", "TEXT"),
        ("tool_calls", "TEXT NOT NULL DEFAULT '[]'"),
        ("token_usage", "TEXT NOT NULL DEFAULT '{}'"),
        ("timing_ms", "INTEGER"),
        ("security_flags", "TEXT NOT NULL DEFAULT '{}'"),
        ("metadata", "TEXT NOT NULL DEFAULT '{}'"),
        ("provenance", "TEXT"),
        ("created_at", "TEXT"),
    )
    for column, ddl in legacy_defaults:
        if column not in columns:
            db.execute(f"ALTER TABLE event_journal ADD COLUMN {column} {ddl}")
    db.execute(
        """
        CREATE TABLE execution_traces_migrating (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            agent_id TEXT NOT NULL,
            session_id TEXT,
            user_id TEXT,
            graph_name TEXT,
            tool_calls TEXT NOT NULL DEFAULT '[]',
            token_usage TEXT NOT NULL DEFAULT '{}',
            timing_ms INTEGER,
            security_flags TEXT NOT NULL DEFAULT '{}',
            metadata TEXT NOT NULL DEFAULT '{}',
            provenance TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            graph_run_id TEXT,
            payload_digest TEXT,
            terminal_status TEXT CHECK (terminal_status IN ('succeeded','failed','cancelled')),
            terminal_reason TEXT,
            trace_schema_version TEXT NOT NULL DEFAULT 'legacy_v0',
            prompt_evidence TEXT NOT NULL DEFAULT '[]',
            steps TEXT NOT NULL DEFAULT '[]',
            control_evidence TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    db.execute(
        """
        INSERT INTO execution_traces_migrating
            (id, tenant_id, agent_id, session_id, user_id, graph_name,
             tool_calls, token_usage, timing_ms, security_flags, metadata,
             provenance, created_at, trace_schema_version)
        SELECT id, tenant_id, agent_id, session_id, user_id, graph_name,
               COALESCE(tool_calls, '[]'), COALESCE(token_usage, '{}'), timing_ms,
               COALESCE(security_flags, '{}'), COALESCE(metadata, '{}'),
               provenance, COALESCE(created_at, datetime('now')), 'legacy_v0'
        FROM event_journal
        """
    )
    db.execute("DROP TABLE event_journal")
    db.execute("ALTER TABLE execution_traces_migrating RENAME TO execution_traces")


def apply_preflight_renames(db: sqlite3.Connection) -> None:
    """Rename legacy tables/columns to canonical names, in place.

    Idempotent: only acts on objects that still exist under the legacy
    name, so it is a no-op on a fresh database or one already migrated.
    """
    existing = {
        row[0]
        for row in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    for legacy_domain_table in ("catalog_taxonomy", "gardener_pending"):
        if legacy_domain_table in existing:
            has_rows = db.execute(f"SELECT 1 FROM {legacy_domain_table} LIMIT 1").fetchone()
            if has_rows is not None:
                raise sqlite3.IntegrityError(
                    f"legacy {legacy_domain_table} contains data; Commerce migration required"
                )
            db.execute(f"DROP TABLE {legacy_domain_table}")
            existing.discard(legacy_domain_table)
    for old, new in _TABLE_RENAMES:
        if old in existing and new not in existing:
            db.execute(f"ALTER TABLE {old} RENAME TO {new}")
            existing.discard(old)
            existing.add(new)

    if "event_journal" in existing:
        if "execution_traces" in existing:
            raise sqlite3.IntegrityError("both legacy and canonical trace tables exist")
        _rebuild_legacy_trace_table(db)
        existing.discard("event_journal")
        existing.add("execution_traces")

    if "episodic_events" in existing:
        if "conversation_records" in existing:
            raise sqlite3.IntegrityError("both legacy and canonical conversation tables exist")
        _migrate_legacy_conversation_records(db)
        existing.discard("episodic_events")
        existing.add("conversation_records")

    for table, old_col, new_col in _COLUMN_RENAMES:
        if table not in existing:
            continue
        columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
        if old_col in columns and new_col not in columns:
            db.execute(f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col}")

    if "execution_traces" in existing:
        trace_columns = {
            row[1] for row in db.execute("PRAGMA table_info(execution_traces)").fetchall()
        }
        for column, ddl in _EXECUTION_TRACE_COLUMNS:
            if column not in trace_columns:
                db.execute(f"ALTER TABLE execution_traces ADD COLUMN {column} {ddl}")

    if "cells" in existing:
        cells_columns = {row[1] for row in db.execute("PRAGMA table_info(cells)").fetchall()}
        if "content_hash" not in cells_columns:
            db.execute("ALTER TABLE cells ADD COLUMN content_hash TEXT")
        if "source_ref" not in cells_columns:
            db.execute("ALTER TABLE cells ADD COLUMN source_ref TEXT")
        if "confidence" not in cells_columns:
            db.execute("ALTER TABLE cells ADD COLUMN confidence REAL NOT NULL DEFAULT 0.5")
        if "visibility" not in cells_columns:
            db.execute("ALTER TABLE cells ADD COLUMN visibility TEXT NOT NULL DEFAULT 'tenant'")


def apply_udb_digest_upgrade(db: sqlite3.Connection) -> None:
    """Fail closed for legacy UDB mutation rows that lack command digests."""
    for table in ("debug_case_mitigations", "debug_case_transitions"):
        exists = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
        ).fetchone()
        if exists is None:
            continue
        columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})").fetchall()}
        if "canonical_digest" not in columns:
            db.execute(f"ALTER TABLE {table} ADD COLUMN canonical_digest TEXT NOT NULL DEFAULT ''")


def _migrate_legacy_conversation_records(db: sqlite3.Connection) -> None:
    """Migrate recognized legacy rows without guessing modern provenance."""
    db.execute(
        """
        CREATE TABLE conversation_records_migrating (
            record_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            session_id TEXT,
            role TEXT NOT NULL,
            kind TEXT NOT NULL,
            content TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            source_hash TEXT NOT NULL,
            graph_run_id TEXT,
            metadata_version INTEGER NOT NULL CHECK (metadata_version = 1),
            idempotency_key TEXT NOT NULL,
            metadata TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (tenant_id, idempotency_key)
        )
        """
    )
    rows = db.execute(
        """
        SELECT id, tenant_id, user_id, session_id, content, metadata, created_at
        FROM episodic_events ORDER BY tenant_id, id
        """
    ).fetchall()
    source_identities: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        record_id, tenant_id, user_id, session_id, content, metadata_raw, created_at = row
        if not all(
            isinstance(value, str) and value for value in (record_id, tenant_id, user_id, content)
        ):
            raise sqlite3.IntegrityError("malformed legacy conversation row blocks migration")
        try:
            UUID(record_id)
            metadata_decoded: object = json.loads(metadata_raw or "{}")
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise sqlite3.IntegrityError(
                "malformed legacy conversation row blocks migration"
            ) from exc
        if not is_json_dict(metadata_decoded):
            raise sqlite3.IntegrityError("legacy conversation metadata must be an object")
        metadata_value: JsonDict = metadata_decoded
        migration_metadata = metadata_value.get("_conversation_migration")
        migration_fields: JsonDict = migration_metadata if is_json_dict(migration_metadata) else {}
        role_value = migration_fields.get("role", "legacy")
        kind_value = migration_fields.get("kind", "legacy_import")
        idempotency_value = migration_fields.get("idempotency_key", f"legacy:{record_id}")
        if not isinstance(role_value, str) or role_value not in {
            "user",
            "assistant",
            "system",
            "tool",
            "legacy",
        }:
            raise sqlite3.IntegrityError("invalid migrated conversation role")
        if not isinstance(kind_value, str) or kind_value not in {
            "message",
            "turn_summary",
            "conversation_note",
            "legacy_import",
        }:
            raise sqlite3.IntegrityError("invalid migrated conversation kind")
        if not isinstance(idempotency_value, str) or not idempotency_value:
            raise sqlite3.IntegrityError("invalid migrated conversation idempotency key")
        content_hash = "sha256:" + hashlib.sha256(content.encode("utf-8")).hexdigest()
        source_identities.setdefault(tenant_id, []).append((record_id, content_hash))
        candidate_source_hash = metadata_value.get("source_hash")
        source_hash = (
            candidate_source_hash
            if isinstance(candidate_source_hash, str)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", candidate_source_hash)
            else content_hash
        )
        candidate_run = metadata_value.get("graph_run_id")
        graph_run_id: str | None = None
        if isinstance(candidate_run, str) and candidate_run:
            try:
                graph_run_id = str(UUID(candidate_run))
            except ValueError:
                graph_run_id = None
        db.execute(
            """
            INSERT INTO conversation_records_migrating
                (record_id, tenant_id, user_id, session_id, role, kind, content,
                 content_hash, source_hash, graph_run_id, metadata_version,
                 idempotency_key, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (
                record_id,
                tenant_id,
                user_id,
                session_id,
                role_value,
                kind_value,
                content,
                content_hash,
                source_hash,
                graph_run_id,
                idempotency_value,
                json.dumps(
                    {
                        key: value
                        for key, value in metadata_value.items()
                        if key != "_conversation_migration"
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                created_at,
            ),
        )
    db.execute("DROP TABLE episodic_events")
    db.execute("DROP TABLE IF EXISTS vec_episodic_events")
    db.execute("ALTER TABLE conversation_records_migrating RENAME TO conversation_records")

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS conversation_migration_receipts (
            migration_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            source_count INTEGER NOT NULL,
            target_count INTEGER NOT NULL,
            source_digest TEXT NOT NULL,
            target_digest TEXT NOT NULL,
            completed_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (migration_id, tenant_id),
            CHECK (source_count = target_count),
            CHECK (source_digest = target_digest)
        )
        """
    )
    tenants = db.execute(
        "SELECT DISTINCT tenant_id FROM conversation_records ORDER BY tenant_id"
    ).fetchall()
    for (tenant_id,) in tenants:
        target_identities = db.execute(
            """
            SELECT record_id, content_hash FROM conversation_records
            WHERE tenant_id = ? ORDER BY record_id
            """,
            (tenant_id,),
        ).fetchall()
        source_rows = sorted(source_identities.get(tenant_id, []))
        target_rows = [
            (str(record_id), str(content_hash)) for record_id, content_hash in target_identities
        ]
        source_body = "|".join(
            f"{tenant_id}:{record_id}:{content_hash.removeprefix('sha256:')}"
            for record_id, content_hash in source_rows
        )
        target_body = "|".join(
            f"{tenant_id}:{record_id}:{content_hash.removeprefix('sha256:')}"
            for record_id, content_hash in target_rows
        )
        source_digest = hashlib.sha256(source_body.encode("utf-8")).hexdigest()
        target_digest = hashlib.sha256(target_body.encode("utf-8")).hexdigest()
        if len(source_rows) != len(target_rows) or source_digest != target_digest:
            raise sqlite3.IntegrityError("conversation migration reconciliation mismatch")
        db.execute(
            """
            INSERT INTO conversation_migration_receipts
                (migration_id, tenant_id, source_count, target_count,
                 source_digest, target_digest)
            VALUES ('contextunity.conversation-history/v1', ?, ?, ?, ?, ?)
            """,
            (
                tenant_id,
                len(source_rows),
                len(target_rows),
                source_digest,
                target_digest,
            ),
        )


def build_core_ddl() -> list[str]:
    """Core table DDL statements (idempotent)."""
    return [
        # Schema version tracking
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
        # Blackboard (Flat Memory) — UUID record API matching Postgres
        """
        CREATE TABLE IF NOT EXISTS blackboard (
            id          TEXT PRIMARY KEY,
            tenant_id   TEXT NOT NULL,
            scope_path  TEXT NOT NULL,
            content     TEXT NOT NULL,
            metadata    TEXT,
            ttl_until   TEXT,
            created_by  TEXT,
            created_at  TEXT NOT NULL
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_bb_tenant ON blackboard (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_bb_scope ON blackboard (scope_path)",
        # BrainCells
        """
        CREATE TABLE IF NOT EXISTS cells (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            user_id         TEXT,
            cell_kind       TEXT DEFAULT 'concept',
            source_type     TEXT,
            source_id       TEXT,
            source_ref      TEXT,
            title           TEXT,
            content         TEXT NOT NULL,
            struct_data     TEXT,
            keywords_text   TEXT,
            scope_path      TEXT,
            content_hash    TEXT,
            confidence      REAL NOT NULL DEFAULT 0.5,
            visibility      TEXT NOT NULL DEFAULT 'tenant',
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_cells_tenant ON cells (tenant_id)",
        """
        CREATE TABLE IF NOT EXISTS cell_embedding_jobs (
            job_id          TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            cell_id         TEXT NOT NULL,
            content_hash    TEXT NOT NULL,
            profile         TEXT NOT NULL,
            status          TEXT NOT NULL CHECK (status IN ('pending','processing','ready','failed','skipped')),
            attempt         INTEGER NOT NULL DEFAULT 0,
            lease_id        TEXT,
            lease_until     TEXT,
            error_code      TEXT,
            idempotency_key TEXT NOT NULL UNIQUE,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (cell_id) REFERENCES cells(id) ON DELETE CASCADE
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_embedding_jobs_claim ON cell_embedding_jobs (tenant_id, status, lease_until)",
        # CellEdges
        """
        CREATE TABLE IF NOT EXISTS cell_edges (
            tenant_id   TEXT NOT NULL,
            source_id   TEXT NOT NULL,
            target_id   TEXT NOT NULL,
            relation    TEXT NOT NULL,
            weight      REAL DEFAULT 1.0,
            metadata    TEXT,
            PRIMARY KEY (tenant_id, source_id, target_id, relation)
        )
        """,
        # Execution Traces
        """
        CREATE TABLE IF NOT EXISTS execution_traces (
            id              TEXT PRIMARY KEY,
            tenant_id       TEXT NOT NULL,
            agent_id        TEXT NOT NULL,
            session_id      TEXT,
            user_id         TEXT,
            graph_name      TEXT,
            tool_calls      TEXT NOT NULL DEFAULT '[]',
            token_usage     TEXT NOT NULL DEFAULT '{}',
            timing_ms       INTEGER,
            security_flags  TEXT NOT NULL DEFAULT '{}',
            metadata        TEXT NOT NULL DEFAULT '{}',
            provenance      TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            graph_run_id    TEXT,
            payload_digest  TEXT,
            terminal_status TEXT CHECK (terminal_status IN ('succeeded','failed','cancelled')),
            terminal_reason TEXT,
            trace_schema_version TEXT NOT NULL DEFAULT 'legacy_v0',
            prompt_evidence TEXT NOT NULL DEFAULT '[]',
            steps           TEXT NOT NULL DEFAULT '[]',
            control_evidence TEXT NOT NULL DEFAULT '{}',
            final_verdict TEXT NOT NULL DEFAULT '{}'
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_execution_traces_tenant ON execution_traces (tenant_id)",
        "CREATE INDEX IF NOT EXISTS idx_execution_traces_session ON execution_traces (session_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_traces_run ON execution_traces (tenant_id, graph_run_id) WHERE graph_run_id IS NOT NULL",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_execution_traces_tenant_id_uq ON execution_traces (tenant_id, id)",
        """
        CREATE TABLE IF NOT EXISTS outcome_observations (
            observation_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            trace_id TEXT NOT NULL,
            graph_run_id TEXT NOT NULL,
            verdict_digest TEXT NOT NULL,
            observation_kind TEXT NOT NULL CHECK (observation_kind IN ('verified_success','verified_failure','neutral')),
            source_authority TEXT NOT NULL CHECK (source_authority = 'operator_review/v1'),
            source_ref TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            canonical_digest TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            resolution_receipt TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            CONSTRAINT outcome_observations_trace_scope_fk
                FOREIGN KEY (tenant_id, trace_id)
                REFERENCES execution_traces(tenant_id, id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, source_authority, idempotency_key)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_outcome_observations_trace ON outcome_observations (tenant_id, trace_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_outcome_observations_tenant_id_uq ON outcome_observations (tenant_id, observation_id)",
        """
        CREATE TABLE IF NOT EXISTS outcome_synapse_effects (
            effect_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            observation_id TEXT NOT NULL,
            synapse_id TEXT NOT NULL,
            source_authority TEXT NOT NULL CHECK (source_authority = 'operator_review/v1'),
            idempotency_key TEXT NOT NULL,
            policy_version TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            CONSTRAINT outcome_synapse_effects_observation_scope_fk
                FOREIGN KEY (tenant_id, observation_id)
                REFERENCES outcome_observations(tenant_id, observation_id)
                ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT outcome_synapse_effects_synapse_scope_fk
                FOREIGN KEY (tenant_id, synapse_id)
                REFERENCES synapses(tenant_id, id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, source_authority, idempotency_key, synapse_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_outcome_synapse_effects_observation ON outcome_synapse_effects (tenant_id, observation_id)",
        """
        CREATE TABLE IF NOT EXISTS execution_trace_artifacts (
            artifact_id         TEXT PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            project_id          TEXT NOT NULL,
            trace_id            TEXT NOT NULL,
            graph_run_id        TEXT NOT NULL,
            invocation_id       TEXT NOT NULL,
            provider_attempt_id TEXT NOT NULL,
            artifact_kind       TEXT NOT NULL CHECK (artifact_kind = 'model_io'),
            content_schema      TEXT NOT NULL CHECK (content_schema = 'contextunity.model-io-content/v1'),
            capture_state       TEXT NOT NULL CHECK (capture_state IN ('captured','disabled','redacted','rejected','unavailable')),
            storage_state       TEXT NOT NULL CHECK (storage_state IN ('hot','archiving','cold','restoring','purging','purged')),
            lifecycle_profile_id TEXT NOT NULL,
            content_digest      TEXT NOT NULL,
            reservation_digest  TEXT NOT NULL,
            protected_envelope  TEXT,
            archive_receipt     TEXT,
            request_bytes       INTEGER NOT NULL CHECK (request_bytes >= 0),
            response_bytes      INTEGER NOT NULL CHECK (response_bytes >= 0),
            revision            INTEGER NOT NULL CHECK (revision >= 1),
            created_at          TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at          TEXT NOT NULL DEFAULT (datetime('now')),
            purged_at           TEXT,
            UNIQUE (tenant_id, project_id, graph_run_id, provider_attempt_id, artifact_kind)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_trace_artifacts_trace ON execution_trace_artifacts (tenant_id, project_id, trace_id)",
        # UniversalDebugBus — negative-experience authority, separate from traces.
        """
        CREATE TABLE IF NOT EXISTS debug_cases (
            case_id             TEXT PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            fingerprint_version TEXT NOT NULL CHECK (fingerprint_version = 'contextunity.udb-fingerprint/v1'),
            fingerprint         TEXT NOT NULL CHECK (length(fingerprint) = 64 AND fingerprint NOT GLOB '*[^0-9a-f]*'),
            fault_class         TEXT NOT NULL CHECK (fault_class IN ('agent_fault','infra_fault','upstream_fault','policy_fault','reference_fault')),
            operation_kind      TEXT NOT NULL CHECK (operation_kind IN ('brain_search','auto_extract','secure_node','synapse_record','memory_synthesis','embedding_enrichment')),
            policy_version      TEXT NOT NULL CHECK (policy_version = 'contextunity.error-evidence/v1'),
            comparison_key      TEXT NOT NULL,
            state               TEXT NOT NULL CHECK (state IN ('open','resolved')),
            fault_count         INTEGER NOT NULL CHECK (fault_count >= 1),
            success_count       INTEGER NOT NULL CHECK (success_count >= 0),
            q_error             REAL NOT NULL CHECK (q_error >= 0.0 AND q_error <= 1.0),
            case_revision       INTEGER NOT NULL CHECK (case_revision >= 1),
            first_occurred_at   TEXT NOT NULL,
            last_occurred_at    TEXT NOT NULL,
            resolved_at         TEXT,
            UNIQUE (tenant_id, case_id),
            UNIQUE (tenant_id, fingerprint_version, fingerprint)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_debug_cases_tenant_state ON debug_cases (tenant_id, state, last_occurred_at DESC)",
        """
        CREATE TABLE IF NOT EXISTS debug_case_occurrences (
            occurrence_id       TEXT PRIMARY KEY,
            case_id             TEXT NOT NULL,
            tenant_id           TEXT NOT NULL,
            producer_id         TEXT NOT NULL,
            idempotency_key     TEXT NOT NULL,
            fingerprint_version TEXT NOT NULL,
            fingerprint         TEXT NOT NULL,
            fault_class         TEXT NOT NULL,
            operation_kind      TEXT NOT NULL,
            fault_code          TEXT NOT NULL,
            policy_version      TEXT NOT NULL,
            comparison_key      TEXT NOT NULL,
            trace_id            TEXT,
            graph_run_id        TEXT,
            node_id             TEXT,
            step_id             TEXT,
            occurred_at         TEXT NOT NULL,
            canonical_digest    TEXT NOT NULL,
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (tenant_id, case_id, occurrence_id),
            UNIQUE (tenant_id, producer_id, idempotency_key)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_debug_occurrences_case ON debug_case_occurrences (tenant_id, case_id, occurred_at DESC)",
        """
        CREATE TABLE IF NOT EXISTS debug_case_recoveries (
            recovery_id            TEXT PRIMARY KEY,
            case_id                TEXT NOT NULL,
            tenant_id              TEXT NOT NULL,
            policy_version         TEXT NOT NULL,
            comparison_key         TEXT NOT NULL,
            expected_case_revision INTEGER NOT NULL,
            exposure_id            TEXT NOT NULL,
            kind                   TEXT NOT NULL CHECK (kind IN ('verified_recovery_probe','comparable_success')),
            verified_at            TEXT NOT NULL,
            canonical_digest       TEXT NOT NULL,
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (case_id, exposure_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_debug_recoveries_tenant_case ON debug_case_recoveries (tenant_id, case_id)",
        """
        CREATE TABLE IF NOT EXISTS debug_case_transitions (
            transition_id         TEXT PRIMARY KEY,
            case_id               TEXT NOT NULL,
            tenant_id             TEXT NOT NULL,
            transition_kind       TEXT NOT NULL CHECK (transition_kind IN ('resolved','reopened')),
            expected_case_revision INTEGER NOT NULL,
            trigger_occurrence_id TEXT,
            transitioned_at       TEXT NOT NULL,
            canonical_digest      TEXT NOT NULL,
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            FOREIGN KEY (tenant_id, case_id, trigger_occurrence_id)
                REFERENCES debug_case_occurrences(tenant_id, case_id, occurrence_id) ON DELETE RESTRICT,
            UNIQUE (case_id, transition_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_debug_transitions_tenant_case ON debug_case_transitions (tenant_id, case_id)",
        """
        CREATE TABLE IF NOT EXISTS debug_case_mitigations (
            attempt_id             TEXT PRIMARY KEY,
            case_id                TEXT NOT NULL,
            tenant_id              TEXT NOT NULL,
            expected_case_revision INTEGER NOT NULL,
            kind                   TEXT NOT NULL CHECK (kind IN ('retry','mitigation','manual_probe')),
            idempotency_key        TEXT NOT NULL,
            attempted_at           TEXT NOT NULL,
            canonical_digest        TEXT NOT NULL,
            FOREIGN KEY (tenant_id, case_id) REFERENCES debug_cases(tenant_id, case_id) ON DELETE RESTRICT,
            UNIQUE (case_id, idempotency_key)
        )
        """,
        # Conversation History
        """
        CREATE TABLE IF NOT EXISTS conversation_records (
            record_id        TEXT PRIMARY KEY,
            tenant_id        TEXT NOT NULL,
            user_id          TEXT NOT NULL,
            session_id       TEXT,
            role             TEXT NOT NULL CHECK (role IN ('user','assistant','system','tool','legacy')),
            kind             TEXT NOT NULL CHECK (kind IN ('message','turn_summary','conversation_note','legacy_import')),
            content          TEXT NOT NULL,
            content_hash     TEXT NOT NULL CHECK (substr(content_hash, 1, 7) = 'sha256:' AND length(content_hash) = 71 AND substr(content_hash, 8) NOT GLOB '*[^0-9a-f]*'),
            source_hash      TEXT NOT NULL CHECK (substr(source_hash, 1, 7) = 'sha256:' AND length(source_hash) = 71 AND substr(source_hash, 8) NOT GLOB '*[^0-9a-f]*'),
            graph_run_id     TEXT,
            metadata_version INTEGER NOT NULL CHECK (metadata_version = 1),
            idempotency_key  TEXT NOT NULL,
            metadata         TEXT NOT NULL DEFAULT '{}',
            created_at       TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (tenant_id, idempotency_key)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_conversation_tenant_user_time ON conversation_records (tenant_id, user_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_conversation_tenant_session_time ON conversation_records (tenant_id, session_id, created_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_conversation_tenant_run ON conversation_records (tenant_id, graph_run_id)",
        """
        CREATE TABLE IF NOT EXISTS conversation_migration_receipts (
            migration_id TEXT NOT NULL,
            tenant_id TEXT NOT NULL,
            source_count INTEGER NOT NULL,
            target_count INTEGER NOT NULL,
            source_digest TEXT NOT NULL,
            target_digest TEXT NOT NULL,
            completed_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (migration_id, tenant_id),
            CHECK (source_count = target_count),
            CHECK (source_digest = target_digest)
        )
        """,
        # BrainSynapse (Flat Memory Phase B) — mirrors the canonical Postgres
        # `synapses` table (postgres/schema.py `_synapses_schema`). SQLite
        # has no generated-column support in this SQLite build path, so
        # `q_composite` is stored (not generated) and kept in sync by the
        # storage mixin at write/update time using the same formula.
        """
        CREATE TABLE IF NOT EXISTS synapses (
            id                  TEXT PRIMARY KEY,
            tenant_id           TEXT NOT NULL,
            agent_id            TEXT NOT NULL,
            graph_name          TEXT,
            graph_run_id        TEXT,
            node_id             TEXT,
            node_name           TEXT,
            action_type         TEXT NOT NULL,
            action_data         TEXT NOT NULL,
            action_data_ref     TEXT,
            context_summary     TEXT,
            thought_trace_ref   TEXT,
            content_hash        TEXT,
            client_id           TEXT,
            node_role           TEXT NOT NULL DEFAULT 'worker',
            fault_class         TEXT,
            status              TEXT NOT NULL DEFAULT 'active',
            q_action            REAL NOT NULL DEFAULT 0.5,
            q_hypothesis        REAL NOT NULL DEFAULT 0.5,
            q_relevance         REAL NOT NULL DEFAULT 0.5,
            q_composite         REAL NOT NULL DEFAULT 0.5,
            scope_path          TEXT,
            metadata            TEXT NOT NULL DEFAULT '{}',
            created_at          TEXT DEFAULT (datetime('now')),
            updated_at          TEXT DEFAULT (datetime('now'))
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_synapses_tenant ON synapses (tenant_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_synapses_tenant_id_uq ON synapses (tenant_id, id)",
        "CREATE INDEX IF NOT EXISTS idx_synapses_agent ON synapses (tenant_id, agent_id)",
        "CREATE INDEX IF NOT EXISTS idx_synapses_run ON synapses (graph_run_id)",
        "CREATE INDEX IF NOT EXISTS idx_synapses_q_composite ON synapses (q_composite DESC)",
        # Ranked-lookup index: query_synapses scopes by tenant then orders by
        # q_composite DESC — tenant-leading composite avoids scanning other
        # tenants' rows (parity with Postgres synapses_tenant_q_composite_idx).
        "CREATE INDEX IF NOT EXISTS idx_synapses_tenant_q_composite ON synapses (tenant_id, q_composite DESC)",
    ]


def build_vector_ddl(vector_dim: int) -> list[str]:
    """Virtual table DDL for sqlite-vec (requires extension)."""
    return [
        f"""
        CREATE VIRTUAL TABLE IF NOT EXISTS vec_cells USING vec0(
            node_id TEXT PRIMARY KEY,
            embedding float[{vector_dim}]
        )
        """,
    ]


__all__ = [
    "SCHEMA_VERSION",
    "apply_preflight_renames",
    "apply_udb_digest_upgrade",
    "build_core_ddl",
    "build_vector_ddl",
]
