<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [sportsdataverse-py Copilot Instructions](#sportsdataverse-py-copilot-instructions)
  - [Project Context](#project-context)
  - [Repository Workflow](#repository-workflow)
  - [Commit Convention](#commit-convention)
  - [Code Style](#code-style)
  - [DataFrame Engine — Polars 1.x](#dataframe-engine--polars-1x)
  - [HTTP Layer](#http-layer)
  - [Module Naming](#module-naming)
  - [NFL — nflreadpy Parity](#nfl--nflreadpy-parity)
    - [NFL Cache + Config](#nfl-cache--config)
    - [NFL — `ep_wp` model application + EPA/WPA](#nfl--ep_wp-model-application--epawpa)
  - [CFB — `cfb_play_participants`](#cfb--cfb_play_participants)
  - [CFB — offline reprocess (`CFBPlayProcess`, 0.0.52+)](#cfb--offline-reprocess-cfbplayprocess-0052)
  - [Module Pattern (NEW modules)](#module-pattern-new-modules)
  - [Test Conventions](#test-conventions)
  - [Build & Development Commands](#build--development-commands)
  - [Common Pitfalls](#common-pitfalls)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# sportsdataverse-py Copilot Instructions

## Project Context

`sportsdataverse-py` is the Python sister to the SportsDataverse R packages
(`wehoop`, `hoopR`, `cfbfastR`, etc.). It provides tidy access to play-by-play,
box score, schedule, roster, and other sports data across the NBA, WNBA,
NFL, MLB, NHL, MBB (men's college basketball), WBB (women's college
basketball), CFB (college football), and odds endpoints.

When there is any conflict between this file and repository contributor
docs, follow `CONTRIBUTING.md`, `CLAUDE.md`, and the current test suite
under `tests/` as the source of truth.

## Repository Workflow

- Use feature branches for changes.
- `main` is the default branch and release branch. It uses **polars 1.x**
  end-to-end, all packaging metadata lives in `pyproject.toml` (PEP 621),
  and **uv** is the canonical day-to-day package manager.
- A separate `0.36-live` branch carries pandas-based development with a
  set of CFB PBP bug fixes that are gradually being translated into the
  polars `main` branch. Do NOT merge `0.36-live` wholesale — it would
  undo the polars migration. Translate semantic fixes individually.
- For any change to exported functions, update tests and documentation in
  the same PR.

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(wbb): add espn_wbb_team_roster() season-level scraper
fix(cfb): correct kneel-down classification in cfb_pbp parser
docs(contributing): document uv workflow and skip_if_no_live gate
chore(deps): bump polars to >=1.0,<2.0 + re-lock
```

**Important: Never include AI agents or assistants (e.g., Claude, Copilot,
Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By`
trailers referencing AI tools. This applies whether the change was
generated, refactored, or reviewed with AI assistance — the human author
is the sole attributable contributor.

## Code Style

- Follow PEP 8 with Ruff formatting (line-length 120, configured in
  `pyproject.toml [tool.ruff]`). Ruff also handles import sorting,
  pyupgrade, and unused-import removal — black, isort, pycln, and
  flake8 are NOT used directly. The standalone `isort` hook in
  `.pre-commit-config.yaml` runs only to inject `from __future__ import
  annotations` at the top of every Python file via `--add-import`.
- Lint, format, and type-check before committing:

  ```sh
  uv run ruff check --fix sportsdataverse/<your_module>.py
  uv run ruff format sportsdataverse/<your_module>.py
  uv run mypy sportsdataverse/<your_module>.py
  ```

- New modules: full type hints required (params + returns); legacy modules
  remain un-typed for now. Per-module strict mypy overrides live in a
  single `[[tool.mypy.overrides]] module = [...]` list in `pyproject.toml`
  — append your module's dotted path to that list rather than creating a
  new override block.
- Use `from __future__ import annotations` only when targeting py3.8 (not
  applicable here; floor is py3.9). Modern type syntax (`dict[str, X]`,
  `X | None`, `list[int]`) is allowed everywhere.

## DataFrame Engine — Polars 1.x

Runtime is pinned to `polars>=1.0,<2.0`. Use the modern API surface:

| Use this | Don't use this (0.18-era) |
|---|---|
| `df.group_by("col")` | `df.groupby("col")` |
| `df.with_row_index("name")` | `df.with_row_count("name")` |
| `expr.map_elements(f, return_dtype=...)` | `expr.apply(f)` |
| `pl.struct(*cols)` | `pl.struct([cols])` |
| `pl.read_csv(schema_overrides=)` | `pl.read_csv(dtypes=)` |
| `Series.scatter()` | `Series.set_at_idx()` |
| `pl.len()` | `pl.count()` |
| `df.join(..., how="full", coalesce=True)` | `df.join(..., how="outer")` |
| `s.cum_sum()` | `s.cumsum()` |
| `s.shift(n=k, fill_value=v)` | `s.shift_and_fill(periods=k, fill_value=v)` |

If you find yourself reaching for a 0.18-style API, treat it as a bug —
all legacy modules were migrated in May 2026.

## HTTP Layer

All HTTP goes through `sportsdataverse.dl_utils.download()`. It is
type-hinted, iterative (no recursion), initializes `response = None`
defensively, and re-raises the most recent exception when the retry budget
is exhausted (instead of returning an unbound variable).

Wrappers do NOT wrap `download()` calls in try/except — they trust it to
either return a usable `requests.Response` or raise
`sportsdataverse.errors.NoESPNDataError` / `requests.exceptions.*`.

## Module Naming

| Sport | Prefix | Example |
|---|---|---|
| Women's college basketball | `wbb_` | `espn_wbb_team_roster()` |
| WNBA | `wnba_` | `espn_wnba_player_stats()` |
| Men's college basketball | `mbb_` | `espn_mbb_pbp()` |
| NBA | `nba_` | `espn_nba_pbp()` |
| College football | `cfb_` | `espn_cfb_pbp()`, `espn_cfb_play_participants()` |
| NFL | `nfl_` | `espn_nfl_pbp()` |
| NHL | `nhl_` | `espn_nhl_pbp()` |
| MLB | `mlb_` / `mlbam_` | `mlbam_games()` |
| Bulk loaders | `load_<sport>_<dataset>` | `load_wbb_pbp()` |
| NFL nflreadpy aliases | bare `load_*` inside `sportsdataverse.nfl` only | `nfl.load_pbp([2024])` |

**Source families** (orthogonal to the sport prefix above): `espn_<sport>_*` is
the default; `fox_<sport>_*` wraps Fox Sports Bifrost (cfb/nba/mbb/nhl/mlb),
`yahoo_cfb_*` wraps Yahoo Sports, and native-site APIs use their own prefixes
(`nfl_*` → `api.nfl.com`, `nhl_*` / `mlb_api_*` → the league sites,
`mlb_statcast_*` → Baseball Savant). Native API families are
**codegen-generated** from `tools/codegen/endpoints/<stem>.yaml` (authenticated
ones like NFL.com set `auth: true` + `getter_module:`); see `CLAUDE.md` →
"Reference-docs build toolchain (codegen)".

**MLB Statcast (`mlb_statcast_*`, 0.0.64+):** the full ~43-endpoint Baseball
Savant surface, named `mlb_statcast_<family>_<name>` (search / leaderboard /
gamefeed / player), all parsed to a tidy frame by default. Savant mixes CSV /
JSON / HTML, so the family's codegen YAML overrides `getter_module:` to a smart
`_get` (dict for JSON, str for CSV/HTML). The pre-0.0.64 `statcast_*` names were
renamed with no aliases — don't reintroduce them. Validate Savant parsers
against real captured payloads (the JSON/CSV/HTML shapes are easy to guess
wrong); see `CLAUDE.md` → "MLB — Statcast".

**NBA / WNBA stats API (`nba_stats_*` / `wnba_stats_*`, 0.0.72+):** two
codegen-generated flat-API stems — `nba_stats` (112 wrappers, `stats.nba.com`,
`league_id="00"` NBA / `"20"` G-League / `"15"` Summer League) and `wnba_stats`
(95 wrappers, `stats.wnba.com`). Key gotcha: **`stats.nba.com` TLS/JA3-
fingerprint-blocks plain `requests`** — the runtime uses `curl_cffi` with
`impersonate="chrome"`, which is a **lazy optional import** in the `tests`/`all`
extras (not a hard dep). One generic parser `parse_nba_stats_result_sets`
handles the `{resultSets:[{name,headers,rowSet}]}` envelope for both families
(polars default, pandas via `return_as_pandas=True`, zero-row frame on
empty/malformed). Generated via `tools/codegen/gen_nba_stats.py` → `FLAT_APIS`.
See `CLAUDE.md` → "NBA / WNBA — stats.nba.com".

## NFL — nflreadpy Parity

`sportsdataverse.nfl` is a near drop-in replacement for nflreadpy.

- 25 nflreadpy-style aliases live INSIDE `sportsdataverse.nfl` (e.g.
  `load_pbp`, `load_schedules`, `load_nextgen_stats`, `load_pfr_advstats`,
  `load_ff_rankings`, `clear_cache`, `get_current_season`,
  `get_current_week`). Do NOT re-export them at the top-level
  `sportsdataverse` namespace — the prefix-based names handle
  cross-sport disambiguation there.
- Use unified `load_nfl_nextgen_stats(stat_type=...)` and
  `load_nfl_pfr_advstats(stat_type=, summary_level=)`. The per-type
  variants (`load_nfl_ngs_passing`, etc.) emit `DeprecationWarning` and
  dispatch to the unified function. Extend the unified function;
  do NOT add new per-type wrappers.
- `load_nfl_ff_rankings` accepts `kind=` (preferred — `type=` shadows
  the builtin) AND `type=` (kept for nflreadpy parity). Pass exactly one.
- Three module-level static datasets ship at import time from
  `sportsdataverse/nfl/datasets.py`: `team_abbr_mapping` (relocations
  folded), `team_abbr_mapping_norelocate` (history preserved), and
  `player_name_mapping`. They're inline-bundled so wheels ship them
  without `package_data` config.

### NFL Cache + Config

`sportsdataverse/nfl/cache.py` + `config.py` provide a shared caching
layer. All 23 canonical loaders + 11 deprecated aliases are wrapped with
`@cached_loader`. Cache key hashes `(qualified_name, args, sorted_kwargs)`
and excludes `return_as_pandas` (one stored polars frame serves both
engines).

Three modes — `memory` (default, per-process dict), `filesystem`
(parquet under `cache_dir`), `off`. Set via `update_config()` or env vars:

| Env var | Effect |
|---|---|
| `SDV_PY_NFL_CACHE` | `memory` \| `filesystem` \| `off` |
| `SDV_PY_NFL_CACHE_DIR` | filesystem cache directory |
| `SDV_PY_NFL_CACHE_DURATION` | TTL (seconds) |
| `SDV_PY_NFL_VERBOSE` | progress chatter on/off |
| `SDV_PY_NFL_TIMEOUT` | HTTP timeout (seconds) |
| `SDV_PY_NFL_USER_AGENT` | custom UA |

Programmatic:

```python
from sportsdataverse.nfl import update_config, get_config, reset_config, clear_cache
update_config(cache_mode="filesystem", cache_duration=3600)
clear_cache()  # wipes both memory + filesystem
```

Call `clear_cache()` after modifying a loader's underlying URL — the
key does NOT hash the URL or function body, so a renamed-URL/same-name
change yields stale data until invalidated.

### NFL — `ep_wp` model application + EPA/WPA

`sportsdataverse/nfl/ep_wp.py` is the **single owner of NFL model
application and EPA/WPA derivation**. Construction modules (`NFLPlayProcess`,
`native_pbp`, `load_nfl_pbp`) must never add EPA/WPA inline — they emit a
frame and `ep_wp.enrich_nfl_pbp` / `calculate_epa` / `calculate_wpa` apply
it. `NFLPlayProcess.__process_epa` / `__process_wpa` delegate to those shared
functions (byte-identical output verified — one engine across both the
ESPN and nflverse `lead_diff` paths). `build_nfl_season(game_ids, *, source=...)`
compiles a full season: per-game construct→enrich→`diagonal_relaxed` concat,
with a per-game parquet cache keyed `(game_id, PIPELINE_VERSION)` via
`nfl/cache.py`. `method="snapshot"` on `enrich_nfl_pbp` is intentionally
`NotImplementedError` — the cross-era comparison was validated without it.

## CFB — `cfb_play_participants`

`sportsdataverse/cfb/cfb_play_participants.py` is the authoritative
source for per-play participant names/IDs. `cfb_pbp.__add_player_cols`
delegates to it (was 471 lines of regex; now 130 lines + a narrow
fallback for known sidecar gaps: `sack_player_name2`,
`fg_block_player_name`, `punt_block_player_name`,
`interception_player_name`).

Three-tier resolution: ESPN per-play `participants[]` -> sidecar
`playerHash` lookup -> `$ref` URL fetch for athletes the sidecar omits
(`resolve_missing=True` by default, capped at 50 fetches/game via
`resolve_missing_max=50`).

Output is hybrid scalar + list: `{type}_player_name` (primary) AND
`{type}_player_names` (full list) so multi-entry types like split sacks
aren't silently collapsed. Don't add new regex extraction — extend the
participants module instead.

## CFB — offline reprocess (`CFBPlayProcess`, 0.0.52+)

For rebuilding games from on-disk raw JSON without re-hitting ESPN:

- `espn_cfb_pbp(raw=True)` keeps `injuries` + `gameNotes` in the allowlist
  (`incoming_keys_expected`); default `[]` when absent.
- `self.odds_source` (`summary_pickcenter` | `core_odds_api` | `default` |
  `injected`) records spread provenance and is also written to the returned
  payload (`pbp_txt["odds_source"]`).
- `CFBPlayProcess(odds_override={gameSpread, overUnder, homeFavorite,
  gameSpreadAvailable})` short-circuits odds resolution (no live core-odds call,
  no default fallback) — the spread is an EPA/WPA *input*, so offline rebuilds
  must inject the persisted odds. Validated + coerced in `__init__` (bad payload
  → `ValueError`). Default `None` = unchanged. Pattern:
  `CFBPlayProcess(gameId, path_to_json=raw_dir, odds_override=...).cfb_pbp_disk()`
  then `.run_processing_pipeline()`.

## Module Pattern (NEW modules)

Worked-example references:

- `sportsdataverse/wbb/wbb_team_roster.py` — single-table `pl.DataFrame` return.
- `sportsdataverse/wbb/wbb_player_stats.py` — multi-table
  `dict[str, pl.DataFrame]` return keyed by canonical category names.
- `sportsdataverse/wnba/wnba_team_roster.py` and `wnba_player_stats.py`
  — thin shims over the shared `_espn_basketball_*` helper with the
  league slug fixed to `"wnba"`.

Mirror their structure for any new ESPN-endpoint module:

1. Public function `espn_<league>_<dataset>(primary_id, ..., *, raw=False, return_as_pandas=False, **kwargs)`.
2. `@overload` chain to type-narrow the return based on `raw` and
   `return_as_pandas` flags.
3. Shared private helper `_espn_basketball_<dataset>(league, ...)` keeps the
   wbb/wnba pair DRY — the wnba module is a thin wrapper that imports the
   helper and fixes the league slug.
4. Returns `pl.DataFrame` for single-table endpoints,
   `dict[str, pl.DataFrame]` for multi-table endpoints, or raw `dict` if
   `raw=True`.
5. Multi-table returns key on canonical category names (`Averages`,
   `Totals`, `Misc` for player stats), with an `Other` bucket added only
   when ESPN ships a non-canonical category.
6. Snake-case columns via `sportsdataverse.dl_utils.underscore`.
7. Append the new module to the consolidated
   `[[tool.mypy.overrides]] module = [...]` list in `pyproject.toml`.

## Test Conventions

- Test files mirror the source layout: `tests/<sport>/test_<sport>_<module>.py`.
- Live-API tests use `@skip_if_no_live` from `tests/conftest.py` and run
  only when `SDV_PY_LIVE_TESTS=1` is set in the environment. CI does NOT
  set this var by default — live runs are opt-in by contributor.
- Assertion style: prefer **subset** column checks (`expected_cols.issubset(set(df.columns))`)
  rather than exact equality, so upstream column additions don't fail tests.
- Smoke tests for live endpoints assert shape (row count > 0, expected
  key columns present, expected dtype) rather than exact values (which
  drift season-to-season).

## Build & Development Commands

```sh
uv sync --all-extras --dev          # install runtime + extras + dev group
uv run pytest                       # run gated suite (live tests skip)
SDV_PY_LIVE_TESTS=1 uv run pytest   # include live API tests
uv run mypy sportsdataverse/<mod>.py
uv run ruff check sportsdataverse/<mod>.py
uv build                            # build wheel + sdist
uv add some-package                 # add runtime dep
uv add --dev some-package           # add dev-only dep
```

`uv.lock` is committed — bump and commit it together with any
`pyproject.toml` dependency change.

## Common Pitfalls

- **`pkg_resources` is removed in setuptools 81+.** `cfb_pbp.py` and
  `nfl_pbp.py` already migrated to `importlib.resources.files()`. Don't
  reintroduce `from pkg_resources import resource_filename`. The
  transitive-dep `UserWarning` is filtered in `pytest.ini`.
- **`psutil` is optional** — imported lazily in `decorators.py`. Don't
  promote it to a hard runtime dep.
- **Polars/Rust regex has no lookaround** (`(?=...)` etc. raise
  `ComputeError`). Use the inline case-flag toggle
  `(?i)prefix(?-i: NAMES)` to stop a capture at a stopword without
  lookahead.
- **All seven `*_pbp.py` modules migrated to polars 1.x in May 2026**
  (~165 sites). 0.18-style API in this codebase = bug, not style.
- **NFL cache invalidation**: `@cached_loader` hashes
  `(qualified_name, args, sorted_kwargs)` only — NOT the URL. After
  changing a loader's underlying URL, call `clear_cache()` or set
  `cache_mode="off"` during development. The cache key excludes
  `return_as_pandas`.
- **Don't add new per-type NFL loaders.** `load_nfl_ngs_*` and
  `load_nfl_pfr_advstats_*` per-type variants emit `DeprecationWarning`
  and dispatch to the unified `load_nfl_nextgen_stats(stat_type=)` /
  `load_nfl_pfr_advstats(stat_type=, summary_level=)`. Extend the
  unified function instead.
- **`cfb_play_participants` sidecar gaps** are mostly backfilled by the
  default-on `$ref` resolution pass (capped at 50 fetches/game).
  Don't replace it with new regex extraction — the four narrow
  fallback fields in `cfb_pbp.__add_player_cols` are documented sidecar
  blind spots.
- **`docs/` is the Docusaurus site.** Internal planning / spec / scratch
  docs live in the gitignored `dev/` directory — NOT under `docs/` and NOT
  at the repo root. `dev/` is in `.gitignore` precisely because those
  files are local-only working notes; promote a doc to the repo root only
  if it becomes contributor-visible reference material.
- **`requirements.txt`, `requirements-dev.txt`, and `setup.py` are all
  deleted as of May 2026.** All packaging metadata lives in
  `pyproject.toml` under PEP 621 `[project]`. Build via `python -m build`
  (PEP 517). Don't reintroduce `setup.py` or a `requirements*.txt`.
- **Conda installability** — the `recipe/meta.yaml` recipe ships a
  `noarch: python` build that mirrors `[project.dependencies]`. Local
  builds via `conda build recipe/`; conda-forge feedstock submission is
  documented in `recipe/README.md`. Don't drift the runtime deps in
  `recipe/meta.yaml` from the ones in `pyproject.toml` — keep them in
  lockstep.
- **Reference docs are generated, not Sphinx'd.** The legacy Sphinx
  pipeline (`Sphinx-docs/` + `create_docs.sh`) is retired; the per-league
  reference subtree under `docs/docs/<sport>/` is generated from endpoint
  metadata by `python tools/codegen/generate.py --docs` and gated by
  `--check` (CI + the `sdv-codegen` pre-commit hook). Never hand-edit the
  generated league/`reference/` dirs; conceptual pages outside them
  (`intro`, `quality-of-life`, `architecture/`, `parsers/`) are
  hand-authored. Verify the site with `cd docs && yarn build`.
- **Every new public function ships a runnable `Example:` block.** Use
  the napoleon literal-block format (heading + `::` + 4-space indented
  code), 2-4 sub-blocks max (quick-start, useful parameters, optional
  pipeline next-step). NEVER use raw `>>> ...` doctest prompts (they
  drift for live-API loaders, so they read as noise). Include a
  `See Also:` block linking to
  the relevant companion R package (`wehoop` / `hoopR` / `cfbfastR` /
  `baseballr` / `fastRhockey`) and any reasonable Python alternative
  (`nflreadpy`, `nba_api`, `nhl-api-py`). Existing one-line backtick
  examples on legacy functions should be REPLACED, not appended to.
- **Example notebooks** under `examples/notebooks/` are intro/intermediate
  walkthroughs, one per sport. New sport submodules should add a parallel
  `0X_<sport>_intro.ipynb`. Outputs are intentionally cleared — users
  execute them locally against live APIs.
- **Polars literal-from-numpy no longer auto-broadcasts in 1.x.** Use
  `pl.lit(np_array).first()` to extract a scalar, or pass a Python value
  directly.
- **`pyjanitor 0.32.18+` silently switched to pandas 3.x.** Keep the
  defensive `pyjanitor<0.32.18` upper bound in `pyproject.toml` until
  pandas 3 is the project floor.
