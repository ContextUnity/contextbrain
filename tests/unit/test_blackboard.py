"""Tests for Blackboard schema DDL and payload rejection.

DDL column/index name assertions deleted — the DDL is the source of truth.
Scope path regex tests deleted — tested a regex defined in the test, not production code.
Payload constructor echo tests deleted — `Model(x=y); assert model.x == y`.
"""

import pytest

from contextunity.brain.payloads import WriteBlackboardPayload


class TestBlackboardSchema:
    """Test critical schema properties (not column name echo)."""

    def test_table_is_unlogged(self):
        from contextunity.brain.storage.postgres.schema import _blackboard_schema

        stmts = _blackboard_schema(1536)
        create_stmt = stmts[0]
        assert "UNLOGGED" in create_stmt

    def test_table_has_embedding_column(self):
        from contextunity.brain.storage.postgres.schema import _blackboard_schema

        stmts = _blackboard_schema(768)
        create_stmt = stmts[0]
        assert "VECTOR(768)" in create_stmt

    def test_included_in_build_schema_sql(self):
        from contextunity.brain.storage.postgres.schema import build_schema_sql

        all_stmts = build_schema_sql(vector_dim=1536)
        all_sql = " ".join(all_stmts)
        assert "blackboard_records" in all_sql


class TestBlackboardRLS:
    """Test that blackboard_records is included in RLS policies."""

    def test_blackboard_in_rls_tenant_tables(self):
        from contextunity.brain.storage.postgres.schema import build_rls_sql

        rls_stmts = build_rls_sql()
        rls_sql = " ".join(rls_stmts)
        assert "blackboard_records" in rls_sql


class TestBlackboardPayloads:
    """Test payload rejection behavior (not constructor echo)."""

    def test_write_payload_rejects_missing_fields(self):
        with pytest.raises(Exception):
            WriteBlackboardPayload.model_validate(
                {"content": {"data": 42}}
            )  # missing tenant_id, scope_path
