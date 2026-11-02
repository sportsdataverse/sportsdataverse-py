"""Console-script entry point for ``sdv``.

The cyclopts CLI needs Python 3.11 and the ``cyclopts`` / ``rich`` dependencies, which are declared
for ``python_version >= '3.11'``. When they cannot be imported (Python 3.10, or an environment where
they were removed), ``sdv`` runs the frozen argparse CLI in :mod:`sportsdataverse.cli` instead, so 3.10
users keep a working command.
"""

from __future__ import annotations

import sys
from typing import Callable, List, Optional

_NOTICE = "sdv: running the classic CLI; Python 3.11+ with cyclopts installed unlocks the new one."


def _load_commands() -> Callable[[Optional[List[str]]], int]:
    from sdv_cli.commands import run

    return run


def main(argv: Optional[List[str]] = None) -> int:
    """Run the ``sdv`` command line and return its exit code.

    Exit codes: 0 the command succeeded (for ``find-*``, a match was found); 1 a ``find-*``
    command found no match; 2 an error, printed as ``sdv: <message>`` on stderr, or a usage error.

    Args:
        argv: Arguments after the program name; ``None`` reads ``sys.argv[1:]``.

    Returns:
        int: The exit code (the console script passes it to ``SystemExit``).
    """
    try:
        run = _load_commands()
    except ModuleNotFoundError as e:
        if e.name not in ("cyclopts", "rich"):
            raise
        from sportsdataverse.cli import main as classic

        if sys.stderr.isatty():
            print(_NOTICE, file=sys.stderr)
        return classic(argv)
    return run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
