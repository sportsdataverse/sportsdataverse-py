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
conda install -n base conda-build conda-verify
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

- **Python target:** 3.9–3.14. The `pyproject.toml` `[tool.black]`,
  `[tool.mypy]`, and `[tool.ruff]` blocks pin to py39 as the floor, with
  classifiers and black `target-version` covering through py314.
- **Type hints required** on all new public functions and helpers
  (parameters and return types). Internal one-line lambdas and trivial
  callbacks may be omitted at author discretion.
- **Lint + typecheck before committing.** From the repo root:

  ```sh
  uv run mypy sportsdataverse/<your_module>.py
  uv run ruff check sportsdataverse/<your_module>.py
  ```

  Per-module strict overrides live in a single `[[tool.mypy.overrides]]`
  block in `pyproject.toml` — append your module's dotted path to the
  `module` list there rather than creating a new override block.
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
