"""Tests for agent_experiences schema presence and reward constants.

Schema DDL column/index name assertions are deleted — the DDL is the source
of truth; tests that echo it provide no behavioral coverage.
"""

from __future__ import annotations


class TestExperiencesSchema:
    """Verify experiences table exists in the combined schema DDL."""

    def test_table_exists_in_schema(self):
        from contextunity.brain.storage.postgres.schema import _experiences_schema

        stmts = _experiences_schema(1536)
        all_sql = " ".join(stmts)
        assert "agent_experiences" in all_sql

    def test_table_has_embedding_column(self):
        from contextunity.brain.storage.postgres.schema import _experiences_schema

        stmts = _experiences_schema(768)
        create_stmt = stmts[0]
        assert "VECTOR(768)" in create_stmt

    def test_included_in_build_schema_sql(self):
        from contextunity.brain.storage.postgres.schema import build_schema_sql

        all_stmts = build_schema_sql(vector_dim=1536)
        all_sql = " ".join(all_stmts)
        assert "agent_experiences" in all_sql


class TestExperiencesRLS:
    """Test that agent_experiences is included in RLS policies."""

    def test_experiences_in_rls_tenant_tables(self):
        from contextunity.brain.storage.postgres.schema import build_rls_sql

        rls_stmts = build_rls_sql()
        rls_sql = " ".join(rls_stmts)
        assert "agent_experiences" in rls_sql


class TestRewardConstants:
    """Test that reward constants are defined and within valid ranges."""

    def test_constants_exist(self):
        from contextunity.brain.reward_constants import (
            DISCOUNT_FACTOR,
            LEARNING_RATE,
            PENALTY_AGENT_FAULT,
            REWARD_NODE_SUCCESS,
        )

        assert REWARD_NODE_SUCCESS > 0
        assert PENALTY_AGENT_FAULT < 0
        assert 0 < DISCOUNT_FACTOR < 1
        assert 0 < LEARNING_RATE <= 1

    def test_review_constants_are_absolute(self):
        from contextunity.brain.reward_constants import (
            REVIEW_REJECTED_SET_Q,
            REVIEW_VERIFIED_SET_Q,
        )

        assert 0 <= REVIEW_VERIFIED_SET_Q <= 1
        assert 0 <= REVIEW_REJECTED_SET_Q <= 1
        assert REVIEW_VERIFIED_SET_Q > REVIEW_REJECTED_SET_Q

    def test_lifecycle_weights(self):
        from contextunity.brain.reward_constants import LIFECYCLE_WEIGHTS

        assert LIFECYCLE_WEIGHTS["confirmed"] > LIFECYCLE_WEIGHTS["active"]
        assert LIFECYCLE_WEIGHTS["active"] > LIFECYCLE_WEIGHTS["outdated"]
        assert LIFECYCLE_WEIGHTS["outdated"] > LIFECYCLE_WEIGHTS["archived"]
