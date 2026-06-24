# `.claude/` — repo dev-tooling for Claude Code

This directory holds the Claude Code customizations for sdv-py. The **committed**
pieces (skills, hook scripts, statusline) are shared with the team; the
**personal** wiring lives in `settings.local.json` (gitignored).

## Skills (`.claude/skills/<name>/SKILL.md`)

Invoke with `/<name>`; Claude also auto-uses them when the description matches.

| Skill | Purpose |
|---|---|
| `/ship` | Gated PR flow: regenerate codegen docs → lint → full pytest → push → CI green → confirm merge → **then** clean up the branch. |
| `/release` | Cut a PyPI release: bump version → CHANGELOG entry → `yarn version:docs` snapshot → tag a GitHub Release (triggers `python-publish.yml`). |
| `/preflight` | Fast scoped sweep on changed files (ruff + mypy ratchet + targeted tests) before a commit/PR. |
| `/address-bot-reviews` | Triage + resolve CodeRabbit / Copilot review threads on a PR (fix valid, decline convention-conflicts with a citation, reply + resolve). Used by `/ship` post-CI. |
| `/reprocess` | OOM-safe bounded/resumable sweep methodology for the `-raw`/`-data` repos. **(user-level: `~/.claude/skills/reprocess/`)** |
| `/port-r-to-python` | Parity-test-first port of R logic (nflfastR / cfbfastR / `0.36-live`) into sdv-py polars: golden fixture → failing parity test → port → green, with the polars-1x + ID-dtype + no-lookaround conventions. |
| `/port-python-to-r` | The mirror direction — port sdv-py Python/polars logic into a SDV R package (cfbfastR, hoopR, …): golden fixture → failing testthat test → port → green, with the polars→tidyverse map + roxygen/pkgdown conventions. |

## Hook scripts (`.claude/hooks/*.py`, committed)

Cross-platform (no machine paths); the **wiring** that invokes them lives in
`settings.local.json`.

| Script | Hook event | What it does |
|---|---|---|
| `mypy_ratchet_guard.py` | `Stop` | If a changed file is in the `[tool.mypy] files` ratchet, runs mypy and surfaces type errors as a non-blocking `systemMessage`. Silent otherwise. |
| `codegen_drift_gate.py` | `PreToolUse` (git push) | Runs `generate.py --check` **only** when the push touches `tools/codegen/**` or `docs/docs/**`; blocks the push on drift. Fast path is ~0.1s. |

## Statusline (`.claude/statusline.py`, committed)

`git:<branch> | <model> | ctx <pct>% (<used>/<limit>, <left> left)` with a
green/yellow/red gauge. Auto-detects a 200k vs 1M context window; override the
denominator with `CLAUDE_STATUSLINE_CONTEXT`.

## Personal wiring (`settings.local.json`, gitignored)

Not committed because it references machine-local venv paths
(`.venv/Scripts/*.exe`). It wires:

- **PostToolUse (Edit|Write)** → ruff `format` + `check --fix` on edited `.py`.
- **PreToolUse (git push)** → `codegen_drift_gate.py`.
- **Stop** → `mypy_ratchet_guard.py`.
- **SessionStart** → warns if venv `python`/`ruff`/`mypy` are missing (so the
  hooks above can't silently no-op).
- **statusLine** → `statusline.py`.
- **permissions.allow** → read-only `uv`/`git`/`gh` commands + a few MCP read
  tools, to cut prompt friction.

To replicate on another machine, copy the structure and fix the absolute paths.

## pre-commit hooks (`.pre-commit-config.yaml`, committed)

Beyond ruff/doctoc/codegen, two local hooks enforce repo rules:

- **`commit-msg`** (`tools/hooks/check_commit_msg.py`) — Conventional-Commit
  subject + **rejects AI `Co-Authored-By` trailers** (CLAUDE.md rule).
- **`pre-push`** — mypy ratchet + codegen drift + the `/preflight` **ID/name-matching
  contract** (`tests/test_id_conventions.py`), each scoped to the pushed files, so
  manual terminal pushes get the same CI-parity the Claude hooks give in-session. The
  ID contract is the sub-second offline guard for the recurring int-vs-str / `id→Utf8`
  bug class; the heavier targeted-test sweep stays the manual `/preflight` inner loop.

`default_install_hook_types` installs all three stages, so a plain
`uv run pre-commit install` wires up `pre-commit` + `commit-msg` + `pre-push`.
