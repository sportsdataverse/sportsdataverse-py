#!/usr/bin/env python3
"""Claude Code statusline: branch | model | context-budget gauge.

Reads the statusline JSON on stdin, derives the git branch and the current
context-window occupancy (from the transcript's most recent ``usage`` block),
and prints a single ASCII line with an ANSI-colored context gauge:

    git:main | Opus 4.8 | ctx 47% (106k/200k, 94k left)

The gauge is the point: 5+ hour sessions can silently approach the output/context
ceiling and truncate a turn. A visible "% used / left" lets you see the wall
coming. Output is ASCII-only to avoid the Windows cp1252 encode crash; color
is plain ANSI (which statuslines render).

Token extraction regexes the tail rather than parsing whole JSON lines: a single
transcript line (a full message with cache) can be hundreds of KB, larger than
any sane tail window, so ``json.loads(line)`` would never see a complete line.
The last match of each usage field belongs to the most recent assistant turn.

Context limit defaults to 200k; override with ``CLAUDE_STATUSLINE_CONTEXT``.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

try:  # be safe even though output is ASCII
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass


def _k(n: int) -> str:
    return f"{n / 1000:.0f}k"


def main() -> None:
    try:
        data = json.load(sys.stdin)
    except Exception:
        data = {}

    model = (data.get("model") or {}).get("display_name") or (data.get("model") or {}).get("id") or "?"
    cwd = (data.get("workspace") or {}).get("current_dir") or data.get("cwd") or "."

    branch = ""
    try:
        r = subprocess.run(
            ["git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
        )
        if r.returncode == 0:
            branch = r.stdout.strip()
    except Exception:
        pass

    # Current context occupancy = the most recent request's input + cache tokens,
    # regexed from the transcript tail (whole-line JSON parse is unreliable: lines
    # can exceed any tail window).
    used = 0
    tp = data.get("transcript_path")
    if tp and os.path.exists(tp):
        try:
            with open(tp, "rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 1_000_000))  # 1 MB tail
                tail = f.read().decode("utf-8", "ignore")

            def _last(pat: str) -> int:
                ms = re.findall(pat, tail)
                return int(ms[-1]) if ms else 0

            used = (
                _last(r'"input_tokens":\s*(\d+)')
                + _last(r'"cache_read_input_tokens":\s*(\d+)')
                + _last(r'"cache_creation_input_tokens":\s*(\d+)')
            )
        except Exception:
            pass

    # Context limit: env override wins; else auto-detect 200k vs 1M-window sessions
    # (Claude windows are effectively one or the other; a fixed 200k denominator
    # reads as >100% on a large-context session).
    env_lim = os.environ.get("CLAUDE_STATUSLINE_CONTEXT")
    if env_lim:
        limit = int(env_lim)
    elif data.get("exceeds_200k_tokens") or used > 200_000:
        limit = 1_000_000
    else:
        limit = 200_000
    pct = (used / limit * 100) if limit else 0.0
    left = max(0, limit - used)

    if pct >= 80:
        col = "\033[31m"  # red
    elif pct >= 60:
        col = "\033[33m"  # yellow
    else:
        col = "\033[32m"  # green
    rst, dim = "\033[0m", "\033[2m"

    ctx = f"{col}ctx {pct:.0f}%{rst} {dim}({_k(used)}/{_k(limit)}, {_k(left)} left){rst}"
    parts = [p for p in (f"git:{branch}" if branch else "", str(model), ctx) if p]
    print(" | ".join(parts))


if __name__ == "__main__":
    main()
