---
name: preflight
description: Use for a fast local sanity sweep on the current changes in sportsdataverse-py (sdv-py) before committing or opening a PR — runs ruff, the mypy ratchet, and the relevant tests scoped to changed files only (not the whole suite). Invoke for "preflight", "quick check before commit", "lint+type+test my changes", or "is this ready to push".
---

# Preflight — fast checks on changed files

A quick, scoped sweep that mirrors what CI will check, but only over what you
changed — seconds, not a full `pytest`. Use it before a commit/PR for fast
feedback; use `/ship` for the full release-gate flow.

## Steps

1. **Find changed Python files** (staged + unstaged + untracked):

   ```sh
   # all changed .py (handles renames "old -> new" and space-containing paths)
   git status --porcelain | sed 's/^...//; s/.* -> //' | grep -E '\.py$'
   ```

2. **Ruff** — format + lint the changed files (the PostToolUse hook already
   formats on edit; this catches anything edited outside Claude):

   ```sh
   uv run ruff format <changed.py ...>
   uv run ruff check <changed.py ...>
   ```

3. **mypy ratchet** — only if a changed file is in the `[tool.mypy] files`
   ratchet in `pyproject.toml`. mypy with no args checks the curated list
   (`follow_imports = "skip"` → sub-second):

   ```sh
   uv run mypy
   ```

4. **Targeted tests** — map each changed module to its test dir and run only
   those, rather than the whole suite. For a changed
   `sportsdataverse/<sport>/<mod>.py`, run `tests/<sport>/`:

   ```sh
   uv run pytest tests/<sport>/ -q
   ```

   If the change is cross-cutting (e.g. `dl_utils.py`, `_common_espn*.py`,
   `config.py`) or the mapping is unclear, fall back to the full suite
   (`uv run pytest -q`) and say so. Live-API tests stay skipped unless
   `SDV_PY_LIVE_TESTS=1`.

   **Always also run the ID / name-matching contract** — a sub-second offline guard
   for the recurring int-vs-str / `id→Utf8` / case-sensitive-regex bug class:

   ```sh
   uv run pytest tests/test_id_conventions.py -q
   ```

5. **Report** a one-line verdict per stage (ruff / mypy / tests: pass|fail) and,
   on any failure, the specific file:line. This is a sanity sweep — surface
   problems, don't auto-fix beyond ruff's own `--fix`.

## When to escalate

If preflight is green and you're about to release or merge, switch to `/ship`
(full pytest + codegen drift + CI-green + merge-confirm). Preflight is the fast
inner-loop check; `/ship` is the gate.
