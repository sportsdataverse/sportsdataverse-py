<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Contributing to sportsdataverse-py](#contributing-to-sportsdataverse-py)
  - [Development setup](#development-setup)
  - [Python version support](#python-version-support)
  - [Code standards for new modules](#code-standards-for-new-modules)
  - [Deprecating a public API](#deprecating-a-public-api)
  - [Documentation & the docs site](#documentation--the-docs-site)
    - [Updating docs (everyday — including a commit after a release)](#updating-docs-everyday--including-a-commit-after-a-release)
    - [At release time](#at-release-time)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Contributing to sportsdataverse-py

`sportsdataverse-py` is the Python sister to the SportsDataverse R packages
(`wehoop`, `hoopR`, `cfbfastR`, etc.), providing tidy access to play-by-play,
box score, schedule, and roster data across multiple sports. See the
[README](README.md) for an overview of the package and supported data sources.

This document captures the conventions contributors should follow when
adding or modifying code in this repository.

## Development setup

This project uses [uv](https://docs.astral.sh/uv/) as its canonical package
manager. uv reads PEP 621 `[project]` and PEP 735 `[dependency-groups]`
metadata directly from `pyproject.toml` — there is no `requirements.txt`
or `setup.py` source-of-truth anymore.

Install uv:

```sh
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

See the [uv install docs](https://docs.astral.sh/uv/getting-started/installation/)
for other options.

Common workflow from the repo root:

```sh
# Create the venv and install runtime + all extras + the dev group
uv sync --all-extras --dev

# Run the test suite
uv run pytest

# Type-check
uv run mypy sportsdataverse/

# Lint
uv run ruff check sportsdataverse/

# Add a new runtime dep
uv add some-package

# Add a new dev-only dep
uv add --dev some-package
```

`uv.lock` is committed so every contributor gets a reproducible install. If
you bump a dep, commit the regenerated lockfile alongside the `pyproject.toml`
change.

**pip fallback.** Contributors who cannot use uv can still install from the
`[project.optional-dependencies]` table:

```sh
pip install -e ".[tests]"
```

**Conda fallback.** Contributors who prefer conda can build the package
locally from the recipe in `recipe/`:

```sh
conda install -n base conda-build
conda build recipe/
conda install --use-local sportsdataverse
```

See [`recipe/README.md`](recipe/README.md) for the full conda workflow,
including the conda-forge feedstock submission flow.

## Python version support

Supported: 3.9, 3.10, 3.11, 3.12, 3.13, 3.14. CI runs against the floor (3.9)
and ceiling (3.14) at minimum. New modules use modern type syntax
(`dict[str, X]`, `X | None`) without `from __future__ import annotations`.

## Code standards for new modules

The package is in the middle of a tooling refresh. New modules adopt a
stricter typed baseline; legacy modules stay un-typed for the time being.
The rules below apply to **new modules** (and to legacy modules that you
are intentionally migrating).

- **Python target:** 3.9–3.14. `pyproject.toml` `[tool.ruff]` pins
  `line-length = 120` and `fix = true`; the `[project]` classifiers
  cover through py314. Linting + formatting run via Ruff
  (`ruff check --fix` and `ruff format`) — see `.pre-commit-config.yaml`
  for the full pre-commit chain (Ruff + isort future-import injector +
  pygrep-hooks + add-trailing-comma + actionlint + yamlfmt + doctoc +
  markdownlint-cli2).
- **Type hints required** on all new public functions and helpers
  (parameters and return types). Internal one-line lambdas and trivial
  callbacks may be omitted at author discretion.
- **Lint + typecheck before committing.** From the repo root:

  ```sh
  uv run mypy sportsdataverse/<your_module>.py
  uv run ruff check sportsdataverse/<your_module>.py
  ```

  The strict-typing gate is a **ratchet** keyed on `[tool.mypy] files`
  in `pyproject.toml` — append your module's path to that `files` list
  once it type-checks cleanly. The gate checks *only* the listed modules
  (with `follow_imports = "skip"`), so it stays green while the
  not-yet-typed legacy surface is left untouched. We intentionally do
  **not** use `[[tool.mypy.overrides]]` for this: that mechanism tunes
  per-module strictness but can't scope the gate, so it would pull the
  whole package into checking (~95 pre-existing legacy errors) and the
  gate would never be green.
- **Polars 1.x.** Runtime is pinned to `polars>=1.0,<2.0`. New code uses
  the modern API surface (`group_by`, `with_row_index`, `map_elements`,
  varargs `pl.struct(*cols)`, etc.). The 0.18 → 1.x migration of the
  legacy `*_pbp.py` modules landed in May 2026; if you see lingering
  0.18-style call sites, treat them as bugs.
- **Tests for new live-API modules** go under `tests/wbb/` or
  `tests/wnba/` (mirroring the source layout) and use the
  `skip_if_no_live` decorator from `tests/conftest.py`. Live tests are
  gated by the `SDV_PY_LIVE_TESTS=1` environment variable so CI and
  routine local runs do not hit upstream APIs.
- **New `load_*` dataset modules** should mirror the existing
  `sportsdataverse/<sport>/<sport>_loaders.py` pattern: a thin parquet /
  CSV reader keyed by season, returning a `pl.DataFrame` (with
  `return_as_pandas=True` opt-in), pulling from the corresponding
  `sportsdataverse-data` GitHub release. Match the column shape of the
  R-side loader the dataset is mirroring so downstream users can swap
  engines without changing call sites.
- **New codegen native-API families** (a `tools/codegen/endpoints/<stem>.yaml`)
  default to the shared JSON `_get`. If the host returns CSV / HTML (not JSON),
  point `getter_module:` at a small module exposing a content-type-aware `_get`
  (see `sportsdataverse/mlb/mlb_statcast_runtime.py` for the Baseball Savant
  one) and give each endpoint a `parser:` that consumes that shape — otherwise
  the JSON-only getter silently returns `{}`. Add a `returns_schema:` per
  endpoint (`tools/codegen/schemas/native/<stem>/*.yaml`) so the docs render a
  `col_name | type | description` table; column names must match the parser's
  snake-cased output. Validate the parser against a **real captured payload**,
  not a hand-written fixture.

## Deprecating a public API

Public APIs are **never removed without warning**. The policy lives in
`sportsdataverse/_deprecation.py` and is enforced through one helper so the
message format and `stacklevel` stay consistent package-wide.

- **Window:** a deprecated API keeps working, unchanged, for **at least two
  minor releases** (deprecated in `0.0.57` → removable no earlier than `0.1.0`).
- **Every call warns:** for the whole window the API emits a
  `DeprecationWarning` that names *both* the replacement *and* the target
  removal version, so downstream users get an actionable, time-boxed migration
  path. Removal happens only in the named release and is called out in the
  changelog.
- **Emit via the helper, never hand-rolled `warnings.warn`:**

  ```python
  # In-body — when the function preserves custom legacy behavior:
  from sportsdataverse._deprecation import warn_deprecated

  def load_nfl_ngs_passing(...):
      warn_deprecated(
          "load_nfl_ngs_passing",
          replacement="load_nfl_nextgen_stats(stat_type='passing')",
          removed_in="0.1.0",
      )
      ...

  # Decorator — for a plain forwarding alias:
  from sportsdataverse._deprecation import deprecated

  @deprecated(replacement="new_fn", removed_in="0.1.0")
  def old_fn(*args, **kwargs):
      return new_fn(*args, **kwargs)
  ```

  Generated `sportsdataverse.parsed.*` alias modules carry their own
  codegen-emitted deprecation banner (template-driven, since `0.0.54`) — don't
  hand-edit those; change the codegen template instead.

## Documentation & the docs site

The docs live under `docs/` (Docusaurus) and publish to
<https://py.sportsdataverse.org> via **Vercel**, which auto-builds and
redeploys on every push to `main` — there is no in-repo deploy workflow.

**The default docs always track the code.** The unversioned `docs/docs/`
tree is the live `current` version, served at the root `/docs/`
(`lastVersion: 'current'`, labelled `main`). So any docs change you push to
`main` appears at the default URL on the next Vercel build — **no
re-versioning needed**. Frozen per-release archives live at `/docs/<x.y.z>/`
and are never touched by edits to `current`.

### Updating docs (everyday — including a commit after a release)

1. Edit the **source of truth**, not the built output:
   - **Reference pages** (`docs/docs/<league>/reference/*`, the per-league
     `index.md`, `reference/parameters.md`) are *generated*. Edit the endpoint
     metadata under `tools/codegen/endpoints/*.yaml` (or the templates /
     schemas), then regenerate with `python tools/codegen/generate.py --docs`.
     Never hand-edit the generated files — the `--check` drift gate (CI + the
     `sdv-codegen` pre-commit hook) will fail.
   - **Conceptual pages** (`docs/docs/intro.md`, `ecosystem.md`,
     `architecture/*`, `parsers/*`) and the notebook intros are hand-authored
     — edit them directly.
   - **Home page**: `docs/src/pages/index.tsx`.
   - **Changelog**: edit the repo-root `CHANGELOG.md`; the
     `sync-docs-changelog` pre-commit hook mirrors it to
     `docs/src/pages/CHANGELOG.md` (served at `/CHANGELOG`).
2. Commit (pre-commit runs the drift gate, doctoc, markdownlint, and the
   changelog sync). Preview locally with `cd docs && yarn build` if you like.
3. Push to `main`. Vercel rebuilds and the change is live at the default
   `/docs/` — because `current` **is** the default. **That's the whole
   workflow; a post-release docs commit is no different from any other.**

### At release time

Freeze a permanent snapshot of the docs for that release, then keep going:

```sh
cd docs && yarn version:docs <x.y.z>   # snapshots docs/docs/ -> versioned_docs/version-<x.y.z>/
git add docs/versioned_docs docs/versioned_sidebars docs/versions.json
git commit -m "docs: snapshot <x.y.z>"
```

Do **not** bump `lastVersion` — leave it `'current'` so the live `main`
docs stay the default and keep tracking the code. The snapshot becomes a
read-only archive at `/docs/<x.y.z>/` in the version dropdown. (To backport a
fix into an already-released archive — rare — edit
`docs/versioned_docs/version-<x.y.z>/...` directly.)
