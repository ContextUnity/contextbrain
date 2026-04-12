"""contextunity.brain CLI entry point."""

from __future__ import annotations

import asyncio

import typer
from contextunity.core import get_contextunit_logger
from rich.console import Console

app = typer.Typer(
    name="contextbrain",
    help="ContextBrain — Vector Storage, Episodic Memory, & Knowledge Graph",
    add_completion=False,
    invoke_without_command=True,
)
console = Console()
logger = get_contextunit_logger(__name__)


@app.callback()
def main(ctx: typer.Context):
    """CLI entry point. Backwards-compatible argument routing."""
    if ctx.invoked_subcommand is not None:
        return
    _run_serve()


@app.command("serve")
def serve():
    """Start the gRPC service."""
    _run_serve()


def _run_serve():
    from .service import serve as grpc_serve

    asyncio.run(grpc_serve())


if __name__ == "__main__":
    app()
