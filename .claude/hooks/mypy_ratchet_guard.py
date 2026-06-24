#!/usr/bin/env python3
"""Stop-hook guard: type-check the mypy ratchet when a ratcheted module changed.

Wired as a Claude Code ``Stop`` hook (see ``.claude/settings.local.json``). It is
diff-aware and **non-blocking**: it only runs mypy when a file listed in the
``[tool.mypy] files`` ratchet in ``pyproject.toml`` shows up as changed in
``git status``, and on failure it surfaces a ``systemMessage`` rather than
blocking the turn. Silent on success.

Rationale: the recurring int-vs-str / ``id -> Utf8`` ID bugs only surfaced at
test time. mypy on the curated ratchet (``follow_imports = "skip"`` → sub-second)
catches type regressions the moment Claude stops, not 300 tests later.

Cross-platform + version-portable (no ``tomllib`` dependency, prefers the venv
mypy on either ``Scripts`` or ``bin``), so it is safe to commit for the team.
"""

from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]  # .claude/hooks/ -> repo root


def _ratchet() -> set[str] | None:
    """Ratchet file list from ``[tool.mypy].files``. ``None`` = could not parse,
    so the caller fails closed (runs mypy on any source change) instead of
    silently going inert."""
    try:
        raw = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    except OSError:
        return None
    try:  # prefer a real TOML parse (3.11+) — robust to quoting / inline arrays
        import tomllib

        files = tomllib.loads(raw).get("tool", {}).get("mypy", {}).get("files")
        if isinstance(files, list):
            return {str(f) for f in files}
    except Exception:
        pass
    m = re.search(r"\[tool\.mypy\].*?\bfiles\s*=\s*\[(.*?)\]", raw, re.S)
    return set(re.findall(r'"([^"]+)"', m.group(1))) if m else None


def _changed() -> set[str]:
    """Changed paths (staged + unstaged + untracked) vs the index, slash-normalized."""
    try:
        out = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout
    except Exception:
        return set()
    paths: set[str] = set()
    for line in out.splitlines():
        p = line[3:].strip()  # porcelain: 2 status chars + space, then path
        if " -> " in p:  # rename: "old -> new"
            p = p.split(" -> ", 1)[1]
        paths.add(p.strip('"').replace("\\", "/"))
    return paths


def _mypy_cmd() -> list[str]:
    for cand in (ROOT / ".venv/Scripts/mypy.exe", ROOT / ".venv/bin/mypy"):
        if cand.exists():
            return [str(cand)]
    return ["uv", "run", "mypy"]  # fallback: let uv resolve the env


def main() -> int:
    ratchet = _ratchet()
    changed = _changed()
    if ratchet is None:  # parse failed -> fail closed: run if any source py changed
        relevant = any(p.startswith("sportsdataverse/") and p.endswith(".py") for p in changed)
    else:
        relevant = bool(changed & ratchet)
    if not relevant:
        return 0  # nothing ratcheted changed — stay silent & fast

    res = subprocess.run(_mypy_cmd(), cwd=ROOT, capture_output=True, text=True)
    if res.returncode == 0:
        return 0

    out = (res.stdout or res.stderr or "mypy failed").strip()
    keep = [ln for ln in out.splitlines() if ": error:" in ln or ln.startswith("Found ")]
    summary = "\n".join(keep[:20]) or out[:1500]
    print(json.dumps({"systemMessage": "mypy ratchet guard — type errors in a changed ratchet module:\n" + summary}))
    return 0  # non-blocking: inform, don't trap the turn


if __name__ == "__main__":
    sys.exit(main())
