from __future__ import annotations

from logging.config import fileConfig
from pathlib import Path

from alembic import context

# Load .env file
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool

env_file = Path(__file__).parent.parent / ".env"
if env_file.exists():
    load_dotenv(env_file)

from contextbrain.core.config import get_env  # noqa: E402

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


def run_migrations_offline() -> None:
    url = _get_url()
    context.configure(
        url=url,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

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

    with connectable.connect() as connection:
        context.configure(connection=connection)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
