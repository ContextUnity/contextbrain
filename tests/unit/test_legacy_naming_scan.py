"""Code-scan guard: legacy physical table names must not reappear outside the
recorded preflight-rename / migration / quarantine locations. Public docs, SDK
additions, storage code, migrations, and new tests must use the canonical
BrainCell / cells naming — legacy names may appear only inside the one
canonical migration and its reverse-local recovery note.

This scans ``services/*/src`` and ``packages/*/src`` for the five *unambiguous*
legacy table names — ones with no legitimate unrelated meaning, unlike
``agent_traces`` (collides with the still-current "Trace" RPC family, e.g.
``AdminSearchTraces``) or ``taxonomy_path`` (collides with an unrelated
Router/RAG filesystem-path parameter for loading ``taxonomy.json``). Those two are excluded from the automated regex: every
hit under either name is unrelated to the renamed DB column/table, so
including them would only produce permanent noise.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Table names with no legitimate unrelated meaning in this codebase — any
# occurrence outside ALLOWED_FILES is either new legacy-naming debt or an
# accidental regression of the preflight rename.
LEGACY_TABLE_NAMES = (
    "knowledge_nodes",
    "knowledge_edges",
    "knowledge_aliases",
    "blackboard_records",
    "agent_experiences",
)

# Python storage contract/facade names retired by the backend rename:
# `BrainStorageProtocol` / `BrainStorageInterface` / `PostgresBrainStore` /
# `SqliteBrainStore` replace them with zero remaining references anywhere in
# the tree — unlike the SQL table names above, no preflight function needs
# to literally spell these out, so there is no allowlist for this scan.
LEGACY_PYTHON_CONTRACT_NAMES = (
    "KnowledgeStoreProtocol",
    "KnowledgeStoreInterface",
    "PostgresKnowledgeStore",
    "SqliteVecStorageBackend",
)

# Relative-to-repo-root paths where legacy names are expected and allowed:
# the preflight rename functions themselves (which must literally spell out
# the old names to rename them), the frozen historical Alembic migration, and
# generated gRPC stubs are excluded from the source naming scan.
ALLOWED_RELATIVE_PATHS = (
    "services/brain/src/contextunity/brain/storage/postgres/schema.py",
    "services/brain/src/contextunity/brain/storage/sqlite/schema.py",
)
ALLOWED_PATH_PREFIXES = ("services/brain/migrations/versions/",)

# Documentation files/dirs whose entire purpose is recording the legacy <->
# canonical mapping (naming history, not live examples) — the documented
# exception, not debt. Everything else under docs/ and website/src/content/
# docs/ must be 100% clean, so both trees are scanned alongside
# services/*/src and packages/*/src, not just the source trees.
ALLOWED_DOC_RELATIVE_PATHS = (
    "docs/architecture/source_authority.md",
    "docs/architecture/legacy_to_canonical_bridge.md",
    "docs/naming/rejected_terms.md",
    "docs/phase1_storage_reset_handoff.md",
)
# Derived doc bundle: compiled by scripts/collect-llm-context.sh into
# docs/contextunity-llm-context/ (gitignored). Not a source of truth to lint.
# If source docs are clean, regenerating the bundle makes it clean too.
ALLOWED_DOC_PATH_PREFIXES = ("docs/contextunity-llm-context/",)


def _repo_root() -> Path:
    # services/brain/tests/unit/test_legacy_naming_scan.py -> repo root
    return Path(__file__).resolve().parents[4]


def _is_allowed(rel_path: str) -> bool:
    if rel_path in ALLOWED_RELATIVE_PATHS:
        return True
    return any(rel_path.startswith(prefix) for prefix in ALLOWED_PATH_PREFIXES)


def _is_doc_allowed(rel_path: str) -> bool:
    if rel_path in ALLOWED_DOC_RELATIVE_PATHS:
        return True
    return any(rel_path.startswith(prefix) for prefix in ALLOWED_DOC_PATH_PREFIXES)


def _find_violations(
    files: list[Path], root: Path, allowed: object, names: tuple[str, ...] = LEGACY_TABLE_NAMES
) -> list[str]:
    violations: list[str] = []
    for file_path in files:
        rel_path = str(file_path.relative_to(root))
        if allowed(rel_path):
            continue
        text = file_path.read_text(encoding="utf-8")
        for legacy in names:
            if legacy in text:
                for lineno, line in enumerate(text.splitlines(), start=1):
                    if legacy in line:
                        violations.append(f"{rel_path}:{lineno}: {legacy!r} — {line.strip()}")
    return violations


def _scan_source_tree() -> list[str]:
    root = _repo_root()
    files = [
        py_file
        for base in ("services", "packages")
        for py_file in (root / base).glob("*/src/**/*.py")
    ]
    return _find_violations(files, root, _is_allowed)


def _scan_docs_tree() -> list[str]:
    root = _repo_root()
    files = list((root / "docs").glob("**/*.md"))
    files += list((root / "website" / "src" / "content" / "docs").glob("**/*.md"))
    files += list((root / "website" / "src" / "content" / "docs").glob("**/*.mdx"))
    return _find_violations(files, root, _is_doc_allowed)


PYTHON_CONTRACT_SCAN_SELF_PATH = "services/brain/tests/unit/test_legacy_naming_scan.py"


def _scan_python_contract_names() -> list[str]:
    """Scan src *and* tests for the retired storage contract names.

    No allowlist except this scan file itself (which must literally spell out
    the retired names to check for them): every other reference was
    mechanically renamed in one pass, so any other hit here is a real
    regression, not expected debt.
    """
    root = _repo_root()
    files = [
        py_file
        for base in ("services", "packages", "tests")
        for py_file in (root / base).glob("**/*.py")
    ]
    return _find_violations(
        files,
        root,
        lambda rel_path: rel_path == PYTHON_CONTRACT_SCAN_SELF_PATH,
        LEGACY_PYTHON_CONTRACT_NAMES,
    )


class TestLegacyNamingScan:
    def test_no_legacy_table_names_outside_preflight_and_migrations(self):
        violations = _scan_source_tree()
        assert not violations, (
            "Found legacy physical table name(s) outside the preflight rename "
            "functions and the frozen migration — legacy names may appear only "
            "there:\n" + "\n".join(violations)
        )

    def test_no_legacy_python_contract_names_anywhere(self):
        violations = _scan_python_contract_names()
        assert not violations, (
            "Found retired storage contract name(s) — "
            "`KnowledgeStoreProtocol`/`KnowledgeStoreInterface`/`PostgresKnowledgeStore`/"
            "`SqliteVecStorageBackend` were fully replaced by `BrainStorageProtocol`/"
            "`BrainStorageInterface`/`PostgresBrainStore`/`SqliteBrainStore` with no "
            "allowlist:\n" + "\n".join(violations)
        )

    def test_no_legacy_table_names_in_docs_or_website(self):
        violations = _scan_docs_tree()
        assert not violations, (
            "Found legacy physical table name(s) in docs/ or website/src/content/"
            "docs/ outside the recorded naming-history documents — public docs "
            "must use canonical names:\n" + "\n".join(violations)
        )

    def test_scan_actually_covers_files(self):
        """Guard the guard: if the glob pattern ever stops matching anything
        (e.g. a src-layout change), the main test above would pass vacuously
        and silently stop protecting anything."""
        root = _repo_root()
        count = sum(
            1 for base in ("services", "packages") for _ in (root / base).glob("*/src/**/*.py")
        )
        assert count > 100, (
            f"Expected to scan >100 source files, found {count} — check the glob pattern"
        )

    def test_docs_scan_actually_covers_files(self):
        root = _repo_root()
        count = len(list((root / "docs").glob("**/*.md")))
        count += len(list((root / "website" / "src" / "content" / "docs").glob("**/*.md")))
        assert count > 50, f"Expected to scan >50 doc files, found {count} — check the glob pattern"

    def test_allowed_paths_actually_exist(self):
        """Guard against the allowlist silently going stale (e.g. after a
        future file move) and hiding a real regression."""
        root = _repo_root()
        for rel_path in ALLOWED_RELATIVE_PATHS + ALLOWED_DOC_RELATIVE_PATHS:
            assert (root / rel_path).is_file(), f"Allowlisted path no longer exists: {rel_path}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
