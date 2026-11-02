"""No tracked file names the maintainers' private reference material.

The repo is public and its docstrings render on the docs site, so a pointer to a private repo or to one of
its files tells a reader nothing. Provenance is the fact itself or a public source. The pattern is the docs
leak gate's (``generate._INTERNAL_LEAK_PATTERNS["private repository"]``) plus the private endpoint
notes, matched over every tracked file, so a mention fails here before a regeneration can publish it.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

_PRIVATE = re.compile(rb"(?i)sdv[-_]internal[-_]refs|claude[-_ ]?cowork|endpoints\.md")

# The one file that must carry a variant of the name: the leak gate's own test samples. (This file and the gate
# spell the pattern with character classes, which the pattern does not match.)
_ALLOWED = {"tests/codegen/test_docs_polish.py"}


def _tracked() -> list[str]:
    if shutil.which("git") is None or not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    out = subprocess.run(["git", "ls-files", "-z"], cwd=ROOT, capture_output=True, check=True).stdout
    return [p for p in out.decode("utf-8").split("\0") if p]


def test_no_tracked_file_names_private_reference_material() -> None:
    hits = []
    for rel in _tracked():
        if rel in _ALLOWED:
            continue
        path = ROOT / rel
        if not path.is_file():  # deleted in the working tree, or a submodule
            continue
        data = path.read_bytes()
        if b"\0" in data[:8192]:  # binary
            continue
        for m in _PRIVATE.finditer(data):
            line = data.count(b"\n", 0, m.start()) + 1
            hits.append(f"{rel}:{line}: {m.group().decode('utf-8', 'replace')}")
    assert hits == [], "state the fact or a public source instead:\n" + "\n".join(hits)
