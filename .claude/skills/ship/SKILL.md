---
name: ship
description: Use when shipping a change in sportsdataverse-py (sdv-py) — opening, pushing, or merging a PR. Runs the steps in the correct order (regenerate codegen docs, lint, full pytest, commit, push, wait for CI green, confirm merge) and never cleans up a branch before the merge is confirmed. Invoke for "ship this", "open/merge the PR", "land this change", or any end-of-change release flow.
---

# Ship a change (sdv-py)

A gated checklist for landing a change in `sportsdataverse-py`. The ordering is
load-bearing: each gate caught a real failure in past sessions (stale generated
docs failing CI, a silently-aborted ruff commit, a branch deleted before merge
was confirmed). **Do not reorder, and do not skip a gate because a step "looks
clean."** Create one todo per numbered step and check them off as you go.

## Preconditions

- Run from the repo root (any checkout; do not hard-code a path — use
  `git rev-parse --show-toplevel` if you need it).
- This repo uses **uv**. Prefix Python tooling with `uv run`.
- **Never branch-ship from `main`.** If `git branch --show-current` is `main`,
  create a feature branch first (`git switch -c <type>/<slug>`) before committing.
- Commit convention: **Conventional Commits** (`feat(nfl): ...`, `fix(cfb): ...`).
  **Never add an AI `Co-Authored-By` trailer** (Claude/Copilot/etc.) — the human
  author is the sole attributable contributor. See CLAUDE.md.

## Steps

1. **Regenerate generated docs (codegen drift gate).** If the change touched any
   endpoint YAML (`tools/codegen/endpoints/*`), schemas, docstrings, loaders, or
   wrappers, the generated reference subtree is probably stale and **CI will fail
   on it**. Regenerate, then check:

   ```sh
   uv run python tools/codegen/generate.py          # regenerate wrappers + docs
   uv run python tools/codegen/generate.py --check   # must exit 0 (drift gate)
   ```

   If `--check` is non-zero, the generated tree drifted — regenerate and stage
   the result. The same gate runs in CI and the `sdv-codegen` pre-commit hook.

2. **Lint + format.** The PostToolUse ruff hook formats files as they're edited,
   but run the suite-wide pass before committing so nothing slips through:

   ```sh
   uv run ruff format sportsdataverse/ tools/ .claude/
   uv run ruff check sportsdataverse/ tools/ .claude/
   ```

   A ruff-format hook can **silently abort a commit** — if `git commit` reports
   nothing committed, suspect the pre-commit ruff hook rewrote files; re-`git add`
   and re-commit. Confirm the commit actually landed (`git log -1 --oneline`).

3. **Run the full test suite.** Not a subset — the whole thing must be green
   before declaring done.

   ```sh
   uv run pytest
   ```

   Live-API tests are gated behind `SDV_PY_LIVE_TESTS=1` and skip by default;
   that's expected. A red suite is a stop-ship — fix before continuing.

4. **Commit.** Conventional Commits subject, scoped where useful. No AI co-author
   trailer. Split unrelated work into separate commits.

5. **Push** the feature branch and **open the PR** (or update it):

   ```sh
   git push -u origin HEAD
   gh pr create --fill        # or: gh pr view --web  to edit
   ```

6. **Wait for CI to go green.** Do not merge on a yellow/pending or red run.

   ```sh
   gh pr checks --watch
   ```

   If CI fails, read the failing job, fix, and loop back to the relevant step
   (often step 1 for docs drift or step 3 for tests). Report the failure — do
   not silently retry.

7. **Address automated reviews (CodeRabbit / Copilot).** They post a few minutes
   after CI. Triage + resolve the unresolved bot threads before merging — run
   `/address-bot-reviews` (fix the valid ones, decline convention-conflicts with
   a CLAUDE.md citation, then reply + resolve each thread). Skip only if no bot
   review landed.

8. **Merge — only after CI is green and bot threads are resolved.**

   ```sh
   gh pr merge --squash        # or the project's preferred strategy
   ```

9. **Confirm the merge landed, THEN clean up.** Verify before deleting anything:

   ```sh
   gh pr view --json state,mergedAt   # state must be MERGED
   ```

   Only once `state == MERGED`: delete the branch and `git switch main && git pull`.
   **Never delete the branch before this confirmation** — a premature cleanup
   stranded work in a past session.

## Stop conditions (report, don't push through)

- Drift gate (`generate.py --check`) non-zero after regeneration.
- Any test failure in the full suite.
- CI red or still pending.
- PR `state` not `MERGED` at cleanup time.

In each case: stop, surface the exact output, and wait — do not delete branches,
force-push, or skip hooks (`--no-verify`) to get unstuck.
