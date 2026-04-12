from __future__ import annotations

import sys

from .cli import app


def main():
    app(sys.argv[1:])


if __name__ == "__main__":
    main()
