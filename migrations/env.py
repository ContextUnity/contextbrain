from __future__ import annotations

import re
from logging.config import fileConfig
from pathlib import Path

from alembic import context

# Load .env file
from dotenv import load_dotenv
from sqlalchemy import Connection, engine_from_config, pool, text

env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    load_dotenv(env_file)

from contextunity.brain.core.config import get_env  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)


def _get_url() -> str:
    # Check multiple possible environment variables
    url = (
        get_env("POSTGRES_DSN")
        or get_env("BRAIN_DATABASE_URL")
        or config.get_main_option("sqlalchemy.url")
    )
    if not url:
        raise ValueError("Database URL not configured. Set POSTGRES_DSN or BRAIN_DATABASE_URL")
    # Ensure proper driver prefix for SQLAlchemy with psycopg (v3)
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql+psycopg://", 1)
    elif url.startswith("postgresql://") and "+psycopg" not in url:
        url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def _brain_schema() -> str:
    """Return the configured PostgreSQL schema after identifier validation."""
    schema = get_env("BRAIN_SCHEMA") or "brain"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema) is None:
        raise ValueError("BRAIN_SCHEMA must be a PostgreSQL identifier")
    return schema


def _prepare_schema(connection: Connection, schema: str) -> None:
    """Set the schema before *any* revision emits unqualified Brain DDL."""
    quoted_schema = f'"{schema}"'
    connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quoted_schema}"))
    connection.execute(text(f"SET search_path TO {quoted_schema}, public"))


def _prepare_version_table(connection: Connection) -> None:
    """Keep shared Alembic bookkeeping in public with room for revision IDs."""
    connection.execute(
        text(
            "CREATE TABLE IF NOT EXISTS public.alembic_version "
            "(version_num VARCHAR(128) NOT NULL PRIMARY KEY)"
        )
    )
    connection.execute(
        text("ALTER TABLE public.alembic_version ALTER COLUMN version_num TYPE VARCHAR(128)")
    )


def run_migrations_offline() -> None:
    url = _get_url()
    schema = _brain_schema()
    context.configure(
        url=url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        version_table_schema="public",
    )
    context.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    context.execute(f'SET search_path TO "{schema}", public')
    context.execute(
        "CREATE TABLE IF NOT EXISTS public.alembic_version "
        "(version_num VARCHAR(128) NOT NULL PRIMARY KEY)"
    )
    context.execute("ALTER TABLE public.alembic_version ALTER COLUMN version_num TYPE VARCHAR(128)")

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = _get_url()
    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )
    schema = _brain_schema()

    with connectable.connect() as connection:
        with connection.begin():
            _prepare_schema(connection, schema)
            _prepare_version_table(connection)
        context.configure(connection=connection, version_table_schema="public")

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
