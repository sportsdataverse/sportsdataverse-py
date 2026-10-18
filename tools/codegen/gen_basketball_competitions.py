"""Write ``sportsdataverse/basketball/_competitions_registry.py`` from ``basketball_competitions.yaml``.

The YAML is the source of truth (one entry per competition, see its header); the wheel ships only the
package, so the runtime reads this generated copy. ``tests/basketball/test_competition_factories.py``
fails when the committed copy drifts from the YAML.

Run: ``python tools/codegen/gen_basketball_competitions.py [--check]``
"""

from __future__ import annotations

import pprint
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "tools/codegen/basketball_competitions.yaml"
TARGET = ROOT / "sportsdataverse/basketball/_competitions_registry.py"
_HEAD = '''"""Competition registry of the per-competition basketball families (package data).

``REGISTRY[platform]`` is the list of competition entries of ``fiba`` / ``genius`` / ``atrium``, in
registry order. Written by ``tools/codegen/gen_basketball_competitions.py`` from
``tools/codegen/basketball_competitions.yaml``; do not edit.
"""

from typing import Any, Dict, List

__all__: List[str] = []

# fmt: off
REGISTRY: Dict[str, List[Dict[str, Any]]] = '''


def render() -> str:
    data: Dict[str, Any] = yaml.safe_load(SOURCE.read_text(encoding="utf-8"))
    return f"{_HEAD}{pprint.pformat(data, indent=4, width=110, sort_dicts=False)}\n# fmt: on\n"


def main(argv: List[str]) -> int:
    text = render()
    if "--check" in argv:
        ok = TARGET.exists() and TARGET.read_text(encoding="utf-8") == text
        print("registry is current" if ok else "registry is STALE: rerun gen_basketball_competitions.py")
        return 0 if ok else 1
    with TARGET.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
