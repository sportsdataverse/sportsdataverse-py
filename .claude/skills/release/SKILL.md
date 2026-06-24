---
name: release
description: Use when cutting a new sportsdataverse-py (sdv-py) PyPI release — bump the version, write the CHANGELOG entry, freeze the docs snapshot, and tag a GitHub Release (which triggers the PyPI publish workflow). Invoke for "cut a release", "release 0.0.x", "ship a new version", "publish to PyPI", or "bump the version and release".
---

# Cut an sdv-py release

End-to-end pre-tag sequence for a PyPI release. The actual PyPI publish is
automated by `.github/workflows/python-publish.yml` on the **GitHub Release
`published`** event — so this skill's job is everything *up to and including*
creating that release. Create one todo per numbered step; the ordering matters.

## Preconditions

- Run from the repo root; this repo uses **uv** (`uv run …`).
- Work on a branch, not `main` (`git switch -c release/0.0.x`). The version +
  CHANGELOG + docs-snapshot land in one PR, then the tag is cut after merge.
- Decide the new version by semver bump from the current
  `pyproject.toml` `[project] version` (today: check it — releases have been
  rapid, e.g. 0.0.66 → 0.0.69).

## Steps

1. **Run the `/ship` gates first.** A release must be green: regenerate codegen
   docs (`uv run python tools/codegen/generate.py`), `uv run ruff format/check`,
   and the **full** `uv run pytest`. Do not proceed on a red suite.

2. **Bump the version** in `pyproject.toml`:

   ```toml
   [project]
   version = "0.0.X"
   ```

   (There is no `setup.py` / `__version__` duplicate — `pyproject.toml` is the
   single source of truth.)

3. **Write the CHANGELOG entry.** Add a new section at the **top** of
   `CHANGELOG.md` (immediately below the doctoc TOC comment block), matching the
   existing shape exactly:

   ```markdown
   ## 0.0.X Release: <Month DD, YYYY>

   ### <SPORT/AREA> — <short title>

   <prose summary of the change, mirroring prior entries>
   ```

   Derive the subsections from the Conventional-Commit subjects since the last
   tag:

   ```sh
   git describe --tags --abbrev=0          # last tag (note the v-prefix convention)
   git log <last-tag>..HEAD --pretty=format:'%s'
   ```

   Do **not** hand-edit the TOC — the `doctoc` pre-commit hook regenerates it,
   and the `sync-docs-changelog` local hook copies `CHANGELOG.md` →
   `docs/src/pages/CHANGELOG.md` automatically on commit.

4. **Freeze the docs snapshot.** Snapshot the live `docs/docs/` tree into a
   frozen per-release archive:

   ```sh
   (cd docs && yarn version:docs 0.0.X)    # subshell: stays at repo root after
   ```

   This writes `docs/versioned_docs/version-0.0.X/`. **Keep `lastVersion:
   'current'` in `docusaurus.config` — never bump it away from `current`**, so
   the live docs always track `main`. (Gotcha: a docs snapshot has triggered a
   **Vercel build heap-OOM** before; if the Vercel deploy fails after this,
   that's the suspect — drop/trim the snapshot rather than loosening anything.)

5. **Commit** everything in one release commit:

   ```sh
   git add pyproject.toml CHANGELOG.md docs/
   git commit -m "chore(release): 0.0.X"
   ```

   (The `commit-msg` hook enforces the Conventional-Commit subject + no AI
   co-author trailer.)

6. **Push, open the PR, wait for CI green, merge** — i.e. finish via `/ship`
   (regenerate-if-needed → CI green → confirm `state == MERGED` → cleanup).

7. **Tag the GitHub Release** (this is what triggers PyPI). Match the existing
   tag convention from step 3:

   ```sh
   gh release create v0.0.X --title "0.0.X" \
     --notes "<paste the new CHANGELOG section>"
   ```

   The `publish` workflow builds the sdist+wheel with `uv build` and publishes
   via **PyPI Trusted Publishing (OIDC)** — no token needed.

8. **Verify the publish landed.**

   ```sh
   gh run watch                            # follow the `publish` workflow
   ```

   Confirm the new version appears on <https://pypi.org/project/sportsdataverse/>.
   `workflow_dispatch` is a build-only dry-run; only the Release event publishes.

9. **Downstream pins (if relevant).** If a consumer repo pins a minimum
   sportsdataverse version (e.g. the cfb-raw / nfl-data pipelines), bump the
   pin to `>=0.0.X` in a follow-up.

## Stop conditions (report, don't push through)

- Red `pytest` or codegen drift at step 1.
- Vercel docs build OOM after the snapshot (step 4) — investigate the snapshot.
- `publish` workflow failure at step 8 — read the job; do not re-cut a tag over
  a partially-published version (the action's `skip-existing: true` protects
  against dup-file errors, but a wrong version is a wrong version).
