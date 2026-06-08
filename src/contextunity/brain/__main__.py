"""Module providing Module docstring is missing capabilities."""

from __future__ import annotations

import sys

from .cli import app


def main():
    """CLI entry point."""
    app(sys.argv[1:])


if __name__ == "__main__":
    main()
