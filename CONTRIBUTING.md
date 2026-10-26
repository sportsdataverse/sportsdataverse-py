<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Contributing to sportsdataverse-py](#contributing-to-sportsdataverse-py)
  - [Development setup](#development-setup)
    - [Coverage numbers](#coverage-numbers)
    - [Worktrees](#worktrees)
  - [Python version support](#python-version-support)
  - [Code standards for new modules](#code-standards-for-new-modules)
  - [Quality gates (`quality.yml`)](#quality-gates-qualityyml)
    - [The scoreboard and its baselines](#the-scoreboard-and-its-baselines)
    - [Mutation registry](#mutation-registry)
  - [Deprecating a public API](#deprecating-a-public-api)
  - [Codegen and the docs site](#codegen-and-the-docs-site)
  - [Adding a league](#adding-a-league)
  - [Test fixtures](#test-fixtures)
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
uv run mypy   # the [tool.mypy] files list scopes it; a path argument breaks the ratchet

# Lint
uv run ruff check sportsdataverse/

# Add a new runtime dep
uv add some-package

# Add a new dev-only dep
uv add --dev some-package
```

`uv.lock` is committed so every contributor gets a reproducible install. If
you bump a dep or the package `version`, run `uv lock` and commit the
regenerated lockfile alongside the `pyproject.toml` change; the `quality`
workflow fails on a stale lock.

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

### Coverage numbers

The README shows two coverage badges. Both come from the `coverage delta (offline suite)`
job in `.github/workflows/quality.yml` on the latest push to `main`; its
`publish coverage badge (main only)` job writes them to the orphan `badges` branch as
shields.io endpoint JSON. Neither number is measured anywhere else.

- **coverage** is line coverage of hand-written code, `coverage_handwritten_percent` from
  `tools/quality_scoreboard.py`. Generated modules (`*_espn_ext.py`, `sportsdataverse/parsed/`
  and any file carrying the codegen marker) are left out. This is the number the scoreboard
  gates and ratchets against `baseline/coverage-devel.json`, so it is the one to cite.
- **coverage (all, incl. generated)** is the plain pytest-cov total over the whole package. It
  is lower because the offline suite skips `tests/codegen`, which together with the codegen
  drift gate is what covers the generated wrappers. It is published for transparency and
  is not gated.

The badges follow every finished `main` run, including one whose coverage gate or a test
failed: that run still measured coverage, so the badge shows the drop rather than an older,
higher number. A badge keeps its last published value only when a run produced no coverage
report at all (it was cancelled, or failed before the suite ran); it never shows a number
no run measured.

### Worktrees

Every `git worktree` needs its own venv. Run `uv sync --all-extras --dev` (or
`uv sync --all-extras --frozen` to keep `uv.lock` untouched) inside the worktree, which creates
`<worktree>/.venv` with the package installed editable from that worktree.

Never point `UV_PROJECT_ENVIRONMENT` at another checkout's venv. `uv sync` reinstalls the
editable `sportsdataverse` into whatever environment it is given, so a worktree synced into the
main checkout's `.venv` repoints that checkout's import at the worktree's source, and every test
run there silently exercises the wrong code. The same trap applies to a script run with the main
checkout's interpreter from inside a worktree: check `sportsdataverse.__file__` when in doubt.

## Python version support

Supported: 3.10, 3.11, 3.12, 3.13, 3.14. The suite runs on **3.13.2 only**; a separate
job installs every extra and imports every module on the **3.10 floor** (`tests.yml`
`floor-import`). No other version runs the full suite. New modules use modern type syntax
(`dict[str, X]`, `X | None`). DO add `from __future__ import annotations` — 931 of the
991 `.py` files under `sportsdataverse/` already do (October 2026), and it keeps `X | None`
valid on the 3.10 floor.

## Code standards for new modules

The package is in the middle of a tooling refresh. New modules adopt a
stricter typed baseline; legacy modules stay un-typed for the time being.
The rules below apply to **new modules** (and to legacy modules that you
are intentionally migrating).

- **Python target:** 3.10–3.14. `pyproject.toml` `[tool.ruff]` pins
  `line-length = 120` and `fix = true` (so a bare `ruff check` rewrites files; pass
  `--no-fix` to only report); the `[project]` classifiers cover through py314. Ruff runs
  its default rules plus `BLE001` (`[tool.ruff.lint] extend-select`); the import-sorting
  `I` rules are **not** enabled and the standalone isort hook was removed, so import order
  is not enforced. Keep the load-bearing `# isort: off` block in
  `sportsdataverse/__init__.py` as it is.
- **Pre-commit hooks** (`.pre-commit-config.yaml`). A plain `pre-commit install` wires all
  three stages, because the config sets
  `default_install_hook_types: [pre-commit, commit-msg, pre-push]`. On an older clone, run
  `pre-commit install --hook-type pre-commit --hook-type commit-msg --hook-type pre-push`.
  - **At commit:** `ruff-check` and `ruff-format`; the pre-commit-hooks file checks
    (`trailing-whitespace`, `check-merge-conflict`, `check-ast`, `check-toml` / `-json` /
    `-xml` / `-yaml`, `check-symlinks`, `end-of-file-fixer`, `requirements-txt-fixer`,
    `check-added-large-files`, `debug-statements`); the pygrep-hooks
    (`python-use-type-annotations`, `python-no-eval`, `python-no-log-warn`, the three `rst-*`
    checks, `text-unicode-replacement-char`, `python-check-mock-methods`,
    `python-check-blanket-noqa`, `python-check-blanket-type-ignore`); `sync-pre-commit-deps`;
    `check-github-workflows` (check-jsonschema); `actionlint`; `doctoc`; `markdownlint-cli2`;
    and the local `sync-docs-changelog` (on `CHANGELOG.md` / `changelog.d/`) and `sdv-codegen`
    (`generate.py --check` on codegen inputs and `CHANGELOG.md`).
  - **At commit-msg:** `conventional-commit-no-ai` (Conventional Commits, no AI
    `Co-Authored-By` trailer).
  - **At push:** `mypy-ratchet-prepush` (`uv run mypy`, when `sportsdataverse/**.py` is
    pushed), `codegen-drift-prepush` (`generate.py --check`, also on `docs/docs/`),
    `preflight-id-contract-prepush` (`tests/test_id_conventions.py`) and
    `validation-contracts-prepush` (`tests/contracts/`, when `tools/validation/` or
    `tests/contracts/` is pushed).

  When doctoc rewrites a staged file's table of contents, pre-commit aborts the commit:
  `git add` the file again and re-commit, then check `git log -1`.
- **Type hints required** on all new public functions and helpers
  (parameters and return types). Internal one-line lambdas and trivial
  callbacks may be omitted at author discretion.
- **Lint + typecheck before committing.** From the repo root:

  ```sh
  uv run mypy   # the [tool.mypy] files ratchet scopes it; a path argument breaks the gate
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
- **Polars 2.x.** Runtime is `polars>=2.0,<3`; polars 1.x is not supported, so new code may use
  2.x-only API (see `CLAUDE.md`, "Polars version"). New code uses
  the modern API surface (`group_by`, `with_row_index`, `map_elements`,
  varargs `pl.struct(*cols)`, etc.). The 0.18 → 1.x migration of the
  legacy `*_pbp.py` modules landed in May 2026; if you see lingering
  0.18-style call sites, treat them as bugs.
- **Tests for new live-API modules** go under `tests/wbb/` or
  `tests/wnba/` (mirroring the source layout) and use the
  `skip_if_no_live` decorator from `tests/conftest.py`. Live tests are
  gated by the `SDV_PY_LIVE_TESTS=1` environment variable, which `live-tests.yml`
sets on pushes to main and manual runs (not on PRs; opt out with a `workflow_dispatch`
`live_tests=false`) and `live-tests-cron.yml` always sets; `nba_stats` / `wnba_stats` live tests use
the separate `SDV_PY_NBA_STATS_LIVE=1` gate that no workflow sets. The gates keep PR CI and
  routine local runs off upstream APIs.
- **Warnings are errors** (`pytest.ini`). A test that exercises a warning path asserts
  it with `pytest.warns(..., match=...)`; one that trips a warning incidentally filters
  exactly that message, with a comment naming the test that asserts it. Fix a warning
  from package code at the source.
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

## Quality gates (`quality.yml`)

`.github/workflows/quality.yml` runs on every pull request into `main` or a `devel-*` branch,
on pushes to those branches, and on manual dispatch. Its gating jobs:

- **`ruff + mypy`**: the quality scoreboard (below), `ruff check` and `ruff format --check` over
  `sportsdataverse/ tests/ tools/`, `uv run --frozen mypy` (the `[tool.mypy] files` ratchet) and
  `pytest tests/contracts/`. The install step is `uv sync --locked`, so a stale `uv.lock` fails here.
- **`coverage delta (offline suite)`**: the offline suite once under pytest-cov
  (`pytest -n auto --dist loadgroup --ignore=tests/codegen --cov=sportsdataverse`), then the
  scoreboard with `--coverage-json`. It uploads `coverage.json` + `scoreboard-ci.json` as the
  `coverage-scoreboard` artifact; on `main` the `publish coverage badge` job turns that into the
  README badges (see [Coverage numbers](#coverage-numbers)).
- **`mutation registry (I/N)`**: see [Mutation registry](#mutation-registry).

### The scoreboard and its baselines

`tools/quality_scoreboard.py` measures hand-written code only (generated modules are excluded):
counts of ruff rule families (`UP`, `B`, `SIM`, `C901`, stale `noqa` comments, ...), size metrics
and, when given a pytest-cov JSON report, hand-written coverage overall and per hotspot module.
`--check` exits 1 when any metric is worse than the baseline and otherwise ends with
`SCOREBOARD OK: no metric regressed`. Counts are strict; a coverage percentage may drop by
0.1 point (`COVERAGE_TOLERANCE`) before it counts. Run it locally (about 15 s without coverage):

```sh
uv run --frozen python tools/quality_scoreboard.py --check --baseline baseline/scoreboard-devel.json
```

Which baseline gates a pull request:

- **Read from the base branch, never the PR.** CI fetches `baseline/` from the PR's base branch,
  so a PR cannot lower its own bar by editing it. A PR that does edit
  `baseline/scoreboard-devel.json` to loosen it gets a warning annotation for reviewers.
- **Scoreboard file.** A PR into `devel-<topic>` is held to `baseline/scoreboard-<topic>-base.json`
  (the topic's branch-point snapshot) when the base branch has one, otherwise to
  `baseline/scoreboard-devel.json`. A PR into `main` uses `scoreboard-devel.json`, and the coverage
  job always uses `baseline/coverage-devel.json`. A push gates against the checkout's own files.
- **BOOTSTRAP.** When the base branch has no such baseline file yet, CI warns `BOOTSTRAP` and uses
  the PR's own copy. This is the only case where the PR's file is trusted; once it merges, every
  later PR reads the base branch.
- **Worse than both baselines.** A sync PR (an upstream branch merged into its topic, such as
  `devel-test-harness` into `devel-<topic>`) carries metrics the upstream already accepted, so
  `tools/upstream_baseline.sh` adds the upstream's baseline as a second `--baseline`. A metric then
  fails only when it is worse than **both**. The allowance applies only when the PR head contains
  the upstream tip and the base does not; an ordinary task PR gets none, so never merge the upstream
  into a task PR.

**Refreshing a baseline** is a deliberate, reviewed commit to `baseline/`, made after the code
it measures has merged. The two files differ: `scoreboard-devel.json` holds lint and size
metrics only (the lint job passes no coverage report), while `coverage-devel.json` adds the
coverage metrics. Re-seed them with `--write`:

```sh
# lint + size only; never give this file coverage metrics, or the lint job fails them as "not measured"
uv run --frozen python tools/quality_scoreboard.py --write baseline/scoreboard-devel.json
# coverage: copy scoreboard-ci.json from the coverage-scoreboard artifact of a green CI run of the
# branch head (CI numbers, not a local run), or measure from a pytest-cov JSON report:
uv run --frozen python tools/quality_scoreboard.py --write baseline/coverage-devel.json --coverage-json coverage.json
```

A metric a baseline tracks can only disappear by re-seeding that baseline, never by omission.

### Mutation registry

`tests/mutants/registry.yaml` lists mutations that some test must catch, and
`tools/run_mutants.py` applies each one. A row names the source `file`, a `snippet` that occurs
**exactly once** in it (never a line number), the `mutant` that replaces it, the pytest `test`
(a file or node id, `-k` allowed) that must go red, the audit `finding` it guards, an optional
`group`, and `expected`:

- **`expected: kill`**: the run fails if `test` stays green under the mutant. The test must also
  be green without it.
- **`expected: {survive_until: <task>}`**: documents a measured test gap. It is reported, never
  fatal, until the named task lands its killing test; then flip the row to `kill` and point `test`
  at that test. An early kill is reported as `killed early`.

Whatever `expected` says, a row that cannot be applied fails the run: the snippet drifted or is
duplicated, the test file is gone, the mutant does not compile, or pytest selects nothing,
crashes or hangs. Fix the row in the PR that moved the code. The mutation is a text replacement
made in place and restored in a `finally`; if the runner is killed hard, `git checkout -- <file>`
restores the source.

```sh
uv run --frozen python tools/run_mutants.py --list                      # print the rows, run nothing
uv run --frozen python tools/run_mutants.py --dry-run                   # validate every snippet + test path
uv run --frozen python tools/run_mutants.py --only <id|finding|group>   # one row, finding or group
uv run --frozen python tools/run_mutants.py --shard 2/12                # one round-robin CI shard
uv run --frozen python tools/run_mutants.py                             # every row (slow: ~20 s a row)
```

In CI the `mutants` job is a 12-shard matrix; each shard runs `--shard I/N` with I/N taken from
`strategy.job-index` / `strategy.job-total`, so editing the matrix can never leave a row unrun.
When the slowest shard nears the job's `timeout-minutes`, add a shard or raise the timeout.

**A new guard gets a row.** A test written to stop a specific bug lands with a registry row in
the same PR: the mutant re-introduces the bug and `test` names the new test, so the row proves
the test can fail. Run `--only <id>` before pushing.

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
- **`removed_in` is a commitment, not a hint.** An alias whose `removed_in`
  version has shipped must be REMOVED in that release, not carried forward. The
  eleven NFL per-type loader aliases marked `removed_in="0.1.0"` were carried
  through 0.1.0-0.1.4 and are removed in 0.1.5; do not let that happen again.
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

## Codegen and the docs site

Both the API wrappers and the docs site are generated by
`tools/codegen/generate.py`; the pipeline, its inputs, the drift gate and the
`sources.yaml` provider registry are documented in
[`docs/docs/architecture/codegen.md`](docs/docs/architecture/codegen.md). Read that
before touching anything under `tools/codegen/`, and regenerate (`generate.py` then
`--check`) after editing endpoint YAML, schemas, templates, `tools/codegen/*.py`, a
public docstring, an `__all__`, or `CHANGELOG.md`.

The `sdv-docs` MCP server (`sdv_docs/`, extra `sportsdataverse[mcp]`, Python >=
3.10) answers questions about this surface from a prebuilt SQLite index. It must
never import `sportsdataverse` — a test guards that.

## Adding a league

Which steps a new league takes depends on its source. In every case, regenerate
(`uv run python tools/codegen/generate.py`, then `--check`) and make sure each new public
function resolves in `tools/codegen/sources.yaml`, or `--check` fails naming it.
[`docs/docs/architecture/codegen.md`](docs/docs/architecture/codegen.md) has the details.

- **An ESPN league.** Add one line to `tools/codegen/endpoints/leagues.yaml`:
  `{prefix: <lg>, sport: <espn sport>, league: <espn league slug>, scopes: [...]}`, plus an
  optional `group:` that nests the module under a container (`hockey`, `baseball`, `football`,
  `soccer`). An endpoint in the five ESPN YAMLs (`generate.ESPN_APIS`) is emitted for every league
  whose `scopes` include the endpoint's `scope:` (`universal`, `ncaa`, `football`, `mlb`), minus
  its `exclude_leagues` (the `espn_cdn` endpoints also take an `include_prefixes` allowlist,
  so a new league reaches them only when added there). The regen writes `sportsdataverse/<lg>/<lg>_espn_ext.py` and wires
  the league's `__init__.py`. For a top-level league to reach the flat `sportsdataverse.<fn>`
  namespace, add its `from sportsdataverse.<lg> import *` line to the hand-written
  `sportsdataverse/__init__.py`.
- **A flat-API family** (a non-ESPN host). Write `tools/codegen/endpoints/<stem>.yaml` and add a
  `("<stem>", "<league prefix>")` tuple to `FLAT_APIS` in `tools/codegen/generate.py`; a family
  that nests under `hockey/` or `basketball/` also needs its home in `_NESTED_HOMES` (via
  `_EURO_HOCKEY_HOMES` / `_BASKETBALL_HOMES`). Give each endpoint a parser and a `returns_schema:`
  validated against a real capture (see "New codegen native-API families" above). Some endpoint
  YAMLs are written by a `tools/codegen/gen_*.py` generator from a vendor spec; edit the generator's
  input, not its output.
- **A HockeyTech league.** No parser code: add a `LeagueConfig` row to `LEAGUES` (and the code to
  the `LeagueCode` literal) in `sportsdataverse/hockeytech/_leagues.py`; create
  `sportsdataverse/hockey/<lg>/__init__.py` calling `build_family("<lg>")` (copy
  `hockey/echl/__init__.py`); add the league to `_HOCKEYTECH_MODULE_LEAGUES` in `generate.py`;
  add its wildcard import and its `_MOVED` back-compat entry to `sportsdataverse/__init__.py`. See
  `CLAUDE.md`, "Hockey — HockeyTech multi-league", for the canvas, `league_id` and key caveats.

## Test fixtures

Committed fixtures live under `tests/fixtures/<surface>/` (about 150 directories). Each directory
carries a `README.md` recording provenance (source URL and capture date); give a new one its README. A
parser test asserts against a **real captured payload**, never a hand-written one; adding a parser
means dropping a fixture in the right directory (with its README) and a test in the matching
`test_*_parsers.py`.

Capture tools, all run once with network access and their output committed:

- `uv run python -m tools.capture_espn_fixtures`: per-league ESPN scoreboard / teams / standings /
  leaders payloads into `tests/fixtures/espn/`, which `generate.py --schemas` introspects offline.
- `uv run python -m tools.capture_cfb_fixtures`: ESPN CFB summary payloads into
  `tests/cfb/fixtures/summary_<game_id>.json`.
- `tools/fixtures/gen_*_fixtures.py`: the NBA, WNBA and G League engine fixtures and the NBA tracking
  fixtures (`uv run python tools/fixtures/gen_wnba_engine_fixtures.py --game-ids ...`; each script's
  docstring gives its arguments). They call stats.nba.com / stats.wnba.com, so run them from a
  residential IP.
- `SDV_INTERNAL_REFS_REPO=<path> uv run python tools/codegen/vendor_captures.py`: trimmed copies of
  the private `sdv-internal-refs` real captures (nba_stats, wnba_stats, On3) into `tests/fixtures/`,
  for the returns-schema generators. It refuses to write a trimmed file that parses to different
  columns or dtypes than the full capture.

## Documentation & the docs site

The docs live under `docs/` (Docusaurus) and publish to
<https://py.sportsdataverse.org> via **Vercel**, which auto-builds and
redeploys on every push to `main`. `.github/workflows/docs-deploy.yml` builds the
site and publishes it to the `gh-pages` branch, which Vercel serves at
py.sportsdataverse.org.

**The default docs always track the code.** The unversioned `docs/docs/`
tree is the live `current` version, served at the root `/docs/`
(`lastVersion: 'current'`, labelled **main (latest)**). So any docs change you push to
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
   - **Changelog**: add a fragment file, `changelog.d/<slug>.<group>.md`; do
     not edit `CHANGELOG.md`, which holds released sections only. The group is
     one of `breaking`, `added`, `changed`, `deprecated`, `removed`, `fixed`,
     `security`, `data`; the slug is lowercase letters, digits, `.`, `_` and
     `-`; to use a branch name, lowercase it and turn `/` and any other
     character into `-` (branch `Fix/ESPN-Rankings` becomes
     `fix-espn-rankings`), so
     you need no PR number to name it. Write each change as one bullet,
     `- **<Area>:** <what changed for a user>. (#<PR>)`, at most three lines;
     the full write-up belongs in the PR description. A change that spans two
     groups is two files. [`changelog.d/README.md`](changelog.d/README.md) has
     an example. Because every PR adds its own file, two open PRs never conflict
     on the changelog. `tests/test_sync_docs_changelog.py` rejects a misnamed
     fragment or one that is not a bullet list. The docs site's
     `/changelog-unreleased` page is built from the fragments when the site is
     deployed (it is gitignored); `/CHANGELOG` and `/changelog-archive` are
     rendered from `CHANGELOG.md` by the `sync-docs-changelog` pre-commit hook,
     or by `uv run python tools/codegen/generate.py` if no hook ran.
2. Commit (pre-commit runs the drift gate, doctoc, markdownlint, and the
   changelog sync). Preview locally with `cd docs && yarn build` if you like.
3. Push to `main`. `docs-deploy.yml` builds the site (rendering
   `/changelog-unreleased` from `changelog.d/` first) and publishes it to the
   `gh-pages` branch, which Vercel serves as-is; the change is live at the default
   `/docs/` — because `current` **is** the default. **That's the whole
   workflow; a post-release docs commit is no different from any other.**

### At release time

Fold the changelog fragments into a release section first. The script writes
`## <x.y.z> Release: <Month D, YYYY>` above the newest release, with the groups
in order and each group's bullets sorted, deletes the fragments and stages both:

```sh
uv run python tools/release_changelog.py <x.y.z>   # --date YYYY-MM-DD for a date other than today
# then: write the **Highlights** paragraph under the new heading,
npx --yes doctoc@2 --github CHANGELOG.md
uv run python tools/codegen/generate.py
```

Then freeze a permanent snapshot of the docs for that release, and keep going.
**This had not been done since 0.0.75** — 0.1.0 through 0.1.4 have no snapshot,
which 0.1.5 fixes. Only the three newest snapshots are built
(`VERSIONS_TO_KEEP=3` in `docusaurus.config.ts`), so `/docs/0.0.50/` is no longer
served:

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
