#!/usr/bin/env python3
"""PreToolUse(git push) gate: block a push that would ship stale codegen output.

Wired as a Claude Code ``PreToolUse`` hook scoped to ``git push`` (see
``.claude/settings.local.json``). ``generate.py --check`` takes ~2 min, so this
gate is **diff-aware**: it only runs the check when the commits being pushed
touch codegen INPUTS (``tools/codegen/**``) or the generated tree
(``docs/docs/**``). Everyday pushes that touch neither return instantly.

Docstring-only edits (which *can* affect generated docs) deliberately fall
through to the CI drift gate and the ``/ship`` skill — the trade keeps ordinary
pushes fast. Returns exit code 2 to block the push when drift is detected.

Committable + cross-platform: uses ``sys.executable`` (the venv python the hook
is launched with) and only git plumbing.
"""

from __future__ import annotations

import json
import pathlib
import re
import shlex
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]  # .claude/hooks/ -> repo root
TRIGGERS = ("tools/codegen/", "docs/docs/")


def _git(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)


def _pushed_paths() -> list[str] | None:
    """Files in commits ahead of the upstream branch. None = range unknown."""
    up = _git(["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"])
    if up.returncode != 0 or not up.stdout.strip():
        return None  # no upstream (first push of a new branch) -> can't scope
    diff = _git(["diff", "--name-only", "@{u}..HEAD"])
    if diff.returncode != 0:
        return None
    return [ln.strip().replace("\\", "/") for ln in diff.stdout.splitlines() if ln.strip()]


def _command_is_push(cmd: str) -> bool:
    """True only if some segment of `cmd` is a real ``git push`` invocation.

    Guards against the PreToolUse ``if:`` matcher firing on a "push" *substring* (e.g. a
    commit message like ``regen-before-push``): split on shell sequencing operators,
    strip leading env-assignments, and require ``git`` immediately followed by ``push``."""
    for seg in re.split(r"&&|\|\||;|\||\n", cmd):
        try:
            toks = shlex.split(seg, posix=True)
        except ValueError:
            toks = seg.split()
        i = 0
        while i < len(toks) and re.match(r"^[A-Za-z_][A-Za-z0-9_]*=", toks[i]):
            i += 1
        rest = toks[i:]
        if len(rest) >= 2 and rest[0].rsplit("/", 1)[-1] in ("git", "git.exe") and rest[1] == "push":
            return True
    return False


def _stdin_command() -> str | None:
    """The Bash command from a PreToolUse payload on stdin, or None when run standalone."""
    try:
        if sys.stdin.isatty():
            return None
        raw = sys.stdin.read()
    except Exception:
        return None
    if not raw.strip():
        return None
    try:
        return (json.loads(raw).get("tool_input") or {}).get("command") or ""
    except Exception:
        return None


def main() -> int:
    # PreToolUse stdin guard: when invoked with a command that is NOT a real `git push`
    # (e.g. a "push" substring in another command), no-op so the hook can't misfire.
    # No stdin (manual run) -> fall through and run the check.
    cmd = _stdin_command()
    if cmd is not None and not _command_is_push(cmd):
        return 0

    pushed = _pushed_paths()
    if pushed is not None and not any(p.startswith(TRIGGERS) for p in pushed):
        return 0  # nothing codegen-relevant in this push -> allow, instantly

    # Range unknown (new branch) OR codegen inputs touched -> run the real gate.
    res = subprocess.run(
        [sys.executable, "tools/codegen/generate.py", "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if res.returncode == 0:
        return 0

    sys.stderr.write(
        "BLOCKED: codegen drift — generated docs/wrappers are stale.\n"
        "Regenerate, then re-stage & push:\n"
        "  uv run python tools/codegen/generate.py\n\n" + (res.stdout or res.stderr or "")[-1500:]
    )
    return 2  # PreToolUse: a non-zero exit blocks the tool call


if __name__ == "__main__":
    sys.exit(main())
