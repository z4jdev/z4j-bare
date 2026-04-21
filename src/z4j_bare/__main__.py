"""Entry point for ``python -m z4j_bare``."""

from __future__ import annotations

import sys

from z4j_bare.cli import main

if __name__ == "__main__":
    sys.exit(main())
