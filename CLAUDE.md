<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CLAUDE.md — sportsdataverse-py Development Guide](#claudemd--sportsdataverse-py-development-guide)
  - [Package Overview](#package-overview)
  - [Commit Convention](#commit-convention)
  - [Branches](#branches)
  - [Packaging](#packaging)
  - [Build & Development Commands](#build--development-commands)
  - [Project Structure](#project-structure)
  - [ESPN Cross-League Architecture (0.0.51+)](#espn-cross-league-architecture-0051)
  - [Parser Layer (0.0.51+)](#parser-layer-0051)
    - [`return_parsed=True` dispatch shim](#return_parsedtrue-dispatch-shim)
    - [Summary dispatcher (21 sub-frames)](#summary-dispatcher-21-sub-frames)
    - [Test fixtures (89 captures across 6 directories)](#test-fixtures-89-captures-across-6-directories)
    - [Test infrastructure summary](#test-infrastructure-summary)
  - [Key Coding Conventions](#key-coding-conventions)
    - [Module pattern (NEW modules)](#module-pattern-new-modules)
    - [NFL — nflreadpy parity](#nfl--nflreadpy-parity)
    - [CFB — `cfb_play_participants` and the 0.36-live reconciliation](#cfb--cfb_play_participants-and-the-036-live-reconciliation)
    - [CFB — offline reprocess (`odds_override`, raw allowlist, `odds_source`) (0.0.52+)](#cfb--offline-reprocess-odds_override-raw-allowlist-odds_source-0052)
    - [HTTP / retry layer](#http--retry-layer)
    - [Polars version](#polars-version)
    - [Type hints](#type-hints)
    - [Test gating](#test-gating)
  - [Common Pitfalls](#common-pitfalls)
  - [Documentation Maintenance](#documentation-maintenance)
  - [Docstring conventions for new functions](#docstring-conventions-for-new-functions)
  - [Example notebooks](#example-notebooks)
  - [Reference-docs build toolchain (codegen)](#reference-docs-build-toolchain-codegen)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CLAUDE.md — sportsdataverse-py Development Guide

## Package Overview

`sportsdataverse-py` is the Python sister to the SportsDataverse R packages
(`wehoop`, `hoopR`, `cfbfastR`, `cfbfastR-py`, etc.) and provides tidy access
to play-by-play, box score, schedule, roster, and other sports data across
multiple leagues (NBA, WNBA, NFL, MLB, NHL, MBB, WBB, CFB, plus odds).

When this guide differs from current repository docs, treat
`CONTRIBUTING.md` and the current test suite under `tests/` as authoritative.

- **License:** MIT
- **Branch:** `main` is the default branch and release branch.
- **Python target:** 3.9, 3.10, 3.11, 3.12, 3.13, 3.14
- **Packaging:** uv (PEP 621 `[project]` + PEP 735 `[dependency-groups]`)
- **DataFrame engine:** polars 1.x (the `0.36-live` branch is a parallel
  pandas-based line of development; see "Branches" below)

## Commit Convention

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat(wbb): add espn_wbb_team_roster() season-level scraper
fix(cfb): correct kneel-down classification in cfb_pbp parser
docs(contributing): document uv workflow and skip_if_no_live gate
test(wnba): add live smoke tests for player_stats canonical categories
refactor(dl_utils): rewrite download() retry as iterative + raise on exhaustion
chore(deps): bump polars to >=1.0,<2.0 + re-lock
ci(actions): add py3.14 to the test matrix
```

Prefer scoped commit subjects when useful (e.g., `feat(wbb): ...`,
`fix(cfb): ...`). Use `type!:` or a `BREAKING CHANGE:` footer for breaking
changes. Split unrelated work into separate commits for reviewability.

**Important: Never include AI agents or assistants (e.g., Claude, Copilot,
Cursor, GPT, Gemini) as co-authors on commits.** Omit all `Co-Authored-By`
trailers referencing AI tools. This applies whether the change was
generated, refactored, or reviewed with AI assistance — the human author
is the sole attributable contributor.

## Branches

- **`main`** — default; uses **polars 1.x** end-to-end. Recently migrated
  from polars 0.18 → 1.x and converted to uv-based packaging (May 2026).
- **`0.36-live`** — parallel pandas-based line of development. Carries CFB
  PBP bug fixes (kneel-down handling, half-edge cases, turnover detection,
  WP cases, punt/yardage parsing, etc.) that have not yet been ported into
  the polars `main` branch. When porting fixes from `0.36-live`, translate
  pandas idioms (`np.select`, `df.loc[...]`, `df.assign(...)`) into polars
  (`pl.when().then().otherwise()`, `with_columns(...)`). The
  function-by-function reconciliation notes live in `dev/` (untracked).

## Packaging

All packaging metadata lives in `pyproject.toml` (PEP 621 `[project]` table)
— there is no `setup.py`. The build path is PEP 517:

```sh
python -m build          # produces sdist + wheel into dist/
```

setuptools is the build backend (`build-system.build-backend =
"setuptools.build_meta"`). Runtime deps live under `[project.dependencies]`,
extras under `[project.optional-dependencies]` (`tests`, `docs`, `models`,
`all`). Package data ships via `[tool.setuptools.package-data]` (currently
`cfb/models/*` + `nfl/models/*`). The `[tool.setuptools.packages.find]`
block excludes `tests*`, `Sphinx-docs*`, `docs*`, `examples*`, `archive*`,
`recipe*`, `dev*` from the wheel.

`recipe/meta.yaml` provides a `noarch: python` conda-build recipe that
mirrors `[project.dependencies]`. Local `conda build recipe/` works today;
conda-forge feedstock submission uses the PyPI-pinned `url:` + `sha256:`
mode documented in `recipe/README.md`. CI verifies the recipe on every PR
that touches `recipe/` or `pyproject.toml` via
`.github/workflows/conda-build.yml`.

## Build & Development Commands

This project uses [uv](https://docs.astral.sh/uv/) — see `CONTRIBUTING.md`
for installation. Common commands from the repo root:

```sh
# Sync deps (creates .venv on first run)
uv sync --all-extras --dev

# Run the test suite (gated tests skip without SDV_PY_LIVE_TESTS=1)
uv run pytest

# Run live tests (hits real APIs)
SDV_PY_LIVE_TESTS=1 uv run pytest

# Type-check the strict-listed modules
uv run mypy sportsdataverse/<your_module>.py

# Lint
uv run ruff check sportsdataverse/

# Build wheel + sdist
uv build

# Bump a dependency
uv add some-package          # runtime
uv add --dev some-package    # dev-only
```

`uv.lock` is committed for reproducible installs across contributors.

## Project Structure

```
sportsdataverse/
  cfb/        # College football (heaviest PBP module)
    cfb_pbp.py                # CFBPlayProcess; __add_player_cols delegates to cfb_play_participants
    cfb_play_participants.py  # ESPN per-play participants -> {type}_player_name/_id pivot
    cfb_loaders.py, cfb_schedule.py, cfb_teams.py, cfb_game_rosters.py, models/
  mbb/        # Men's college basketball
  mlb/        # MLB (mlbam endpoints + retrosheet/retrosplits)
  nba/        # NBA
  nfl/        # NFL — nflreadpy-parity surface
    nfl_loaders.py    # 23 canonical load_nfl_* + 11 deprecated per-type aliases
    nfl_pbp.py, nfl_schedule.py, nfl_teams.py, nfl_games.py, nfl_game_rosters.py
    cache.py          # @cached_loader, memory/filesystem/off, clear_cache()
    config.py         # NflConfig dataclass + get_config / update_config / reset_config
    datasets.py       # team_abbr_mapping, team_abbr_mapping_norelocate, player_name_mapping
    utils_date.py     # get_current_nfl_season(), get_current_nfl_week()
  nhl/        # NHL
  wbb/        # Women's college basketball
    wbb_pbp.py, wbb_game_rosters.py, wbb_schedule.py, wbb_loaders.py, wbb_teams.py
    wbb_team_roster.py        # single-table espn_wbb_team_roster()
    wbb_player_stats.py       # multi-table dict[str, pl.DataFrame]
  wnba/       # WNBA
    wnba_team_roster.py       # thin shim over wbb helper, league="wnba"
    wnba_player_stats.py      # same shape as wbb_player_stats
  odds/       # Odds & betting lines
  dl_utils.py # download() retry + janitor + (under|kebab|camel)ize helpers
  errors.py   # NoESPNDataError, SeasonNotFoundError
  config.py   # Per-sport URL constants pointing at sportsdataverse-data releases
  __init__.py
tests/
  conftest.py # skip_if_no_live decorator
  cfb/, mbb/, mlb/, nba/, nfl/, nhl/, wbb/, wnba/  # one subdir per source pkg
docs/                       # Docusaurus site (don't pollute with internal docs)
docs_instructions.md        # Reference for the docs build workflow
dev/                        # Local-only working notes (gitignored)
recipe/                     # Conda-build recipe (meta.yaml + README)
pyproject.toml              # PEP 621 metadata + tooling (ruff lint+format, mypy)
pytest.ini                  # filterwarnings for env-level pkg_resources / nspkg.pth noise
uv.lock                     # Committed
CONTRIBUTING.md             # uv workflow + new-module standards
```

## ESPN Cross-League Architecture (0.0.51+)

The cross-league ESPN wrapper surface lives in
`sportsdataverse/_common_espn.py`. The pattern is **one core +
N thin extensions**:

- `_common_espn.py` — ~80 core functions parameterized on
  `(sport, league)` slugs. Every ESPN URL family is wrapped once
  (Site v2 / Site v2 alt / Web v3 / Core v2 / Core v3 / CDN).
- `_UNIVERSAL_WRAPPERS` — list of `(short_name, core_fn)` tuples
  that map to wrapper functions on every league.
- `_NCAA_WRAPPERS` / `_FOOTBALL_WRAPPERS` / `_MLB_WRAPPERS` —
  opt-in extras gated by `include_ncaa=` / `include_football=` /
  `include_mlb=` flags on `make_league_module()`.
- `_bind(core_fn, sport, league, full_name, parser=None)` — wraps
  each core function with a `functools.partial` (when no parser
  registered) or a closure (when a parser is registered) that adds
  `__name__` / `__qualname__` / `__doc__` for IDE introspection
  and optionally accepts `return_parsed=True` / `return_as_pandas=True`
  kwargs.
- `make_league_module(sport, league, prefix, namespace, ...)` —
  iterates the wrapper tables and registers each one in `namespace`
  with the canonical `espn_{prefix}_{short}` name.

Per-league extension modules (`sportsdataverse/{league}/{league}_espn_ext.py`)
are 4-line files: import `make_league_module`, call it with the
appropriate (sport, league, prefix) tuple + extras flags, assign
the return value to `__all__`. Examples:

```python
# sportsdataverse/nba/nba_espn_ext.py
__all__ = make_league_module("basketball", "nba", "nba", globals())

# sportsdataverse/cfb/cfb_espn_ext.py — both NCAA + football extras
__all__ = make_league_module(
    "football", "college-football", "cfb", globals(),
    include_ncaa=True, include_football=True,
)
```

Total cross-league surface: **121 short names** registered across 8
leagues = **819 wrappers**.

## Parser Layer (0.0.51+)

Every wrapper returns raw `Dict` by default. The parser layer turns
those payloads into tidy polars / pandas DataFrames. **Five parser
modules**, one per data surface:

| Module | Surface | Parsers |
|---|---|---|
| `_common_espn_parsers.py` | ESPN cross-league (Site v2 + Core v2 + Web v3) | 30+ dedicated + 3 generic + 21-section summary dispatcher |
| `nhl/nhl_api_web_parsers.py` | `api-web.nhle.com/v1/` modern game-feed | 16 dedicated + 2 dispatchers (right_rail, club_stats) |
| `nhl/nhl_edge_parsers.py` | `api-web.nhle.com/v1/edge/*` player tracking | 4 family + 3 sub-frame + 1 fallback |
| `nhl/nhl_stats_rest_parsers.py` + `nhl_records_parsers.py` | `api.nhle.com/stats/rest` + `records.nhl.com` | 1 generic each (shared `{data: [...]}` shape) |
| `mlb/mlb_api_parsers.py` | `statsapi.mlb.com` Stats API | 5 dedicated + 1 generic |

**Parser contract (universal across all 5 modules):**

- Return `polars.DataFrame` by default; pandas via
  `return_as_pandas=True`.
- Empty / malformed payloads return a zero-row frame instead of
  raising — callers can chain without null-checks.
- Output columns are snake-cased via
  `sportsdataverse.dl_utils.underscore`.
- Use `pandas.json_normalize` for nested flattening, then convert
  to polars at the end. List-valued cells are stringified so polars
  accepts the frame.

### `return_parsed=True` dispatch shim

ESPN cross-league wrappers whose `short` name is registered in
`ENDPOINT_PARSERS` accept an optional `return_parsed=True` kwarg
that routes the raw payload through the registered parser:

```python
from sportsdataverse.nba import espn_nba_team_roster

raw = espn_nba_team_roster(team_id=13)                          # → Dict
df  = espn_nba_team_roster(team_id=13, return_parsed=True)      # → polars
pdf = espn_nba_team_roster(team_id=13, return_parsed=True,
                            return_as_pandas=True)              # → pandas
```

The shim is **strictly additive** — every existing caller continues
to get raw `Dict` when the kwarg is omitted. NHL / MLB sibling-API
wrappers compose with their parser explicitly:

```python
from sportsdataverse.nhl import nhl_web_pbp, parse_nhl_web_pbp
df = parse_nhl_web_pbp(nhl_web_pbp(2023030417))                 # 331 plays
```

**`ENDPOINT_PARSERS` invariant**: every wrapper short name across
`_UNIVERSAL_WRAPPERS` + `_NCAA_WRAPPERS` + `_FOOTBALL_WRAPPERS` +
`_MLB_WRAPPERS` is in the registry. Three generic fall-throughs
cover the long tail:

- `parse_single_entity` — Core v2 single-resource payloads
  (`team`, `venue`, `franchise`, `coach`, etc.).
- `parse_items` — Core v2 paginated `{items: [...]}` and the
  Core v2 `{entries: [...]}` variant (athlete_statisticslog).
- `parse_summary` — Site v2 `summary` dispatcher (21 sub-frames
  per game).

Three regression tests in `tests/test_espn_universal_parsers.py`
lock in the 121/121 coverage invariant + the shim invariant. Any
new wrapper short name added without a matching `ENDPOINT_PARSERS`
entry fails CI.

### Summary dispatcher (21 sub-frames)

`parse_summary(payload, section=None)` is the dispatcher for the
rich Site v2 summary payload (~700KB–1.8MB per game). With
`section=None` returns a dict of all 21 sub-frames keyed by section
name; with `section="<name>"` returns just that one frame.

Section list (current 21): `boxscore_player`, `boxscore_team`,
`plays`, `winprobability`, `leaders`, `game_info`, `officials`,
`header`, `season_series`, `against_the_spread`, `standings`,
`broadcasts`, `format`, `pickcenter`, `odds`, `article`, `injuries`,
`news`, `drives`, `drive_plays`, `scoring_plays`.

Cross-league shape divergences captured by tests:

- NFL + CFB ship `drives.previous[]` + `scoringPlays` instead of
  top-level `plays[]`. `parse_summary_drive_plays` unrolls drive
  plays into a long-form frame with `drive_id` + `drive_sequence`
  join keys for football PBP parity.
- NHL doesn't publish per-play `winprobability`.
- `pickcenter` / `odds` / `against_the_spread` are sparse in
  past-game captures (live games typically populate them).
- NCAA W basketball `officials` sometimes ships < 3 rows; CFB
  national championship shipped 0 officials.

### Test fixtures (89 captures across 6 directories)

Captured fixtures live under `tests/fixtures/{espn,mlb_api,nhl_api_web,
nhl_edge,nhl_stats_rest,nhl_records}/`. Each directory has a
`README.md` documenting provenance (URL + capture date). See
`docs/docs/parsers/fixtures.md` for the full inventory.

Adding a new parser → drop a fixture in the right directory + add
a test in the matching `test_*_parsers.py` file. The parser tests
are payload-agnostic so re-captured fixtures continue to work as
long as the schema doesn't drift; when it does, the weekly cron
drift detector (`.github/workflows/live-tests-cron.yml`) catches it
and opens a tracking issue labeled `live-tests:drift`.

### Test infrastructure summary

| Test file | Count | Surface |
|---|---:|---|
| `tests/test_espn_universal_parsers.py` | 128 | ESPN cross-league + summary dispatcher |
| `tests/test_nhl_api_web_parsers.py` | 37 | NHL api-web modern game-feed |
| `tests/test_nhl_edge_parsers.py` | 32 | NHL EDGE player-tracking |
| `tests/test_nhl_aux_parsers.py` | 21 | NHL Stats REST + Records |
| `tests/test_mlb_api_parsers.py` | 17 | MLB Stats API |
| **Offline parser tests total** | **235** | |
| `tests/test_espn_live.py` | 41 | Live API integration (gated by `SDV_PY_LIVE_TESTS=1`) |

## Key Coding Conventions

### Module pattern (NEW modules)

Each new ESPN scrape module follows the worked-example shape established by
`sportsdataverse/wbb/wbb_team_roster.py` (single-table return) and
`wbb_player_stats.py` (multi-table `dict[str, pl.DataFrame]` return). The
`wnba_team_roster.py` / `wnba_player_stats.py` pair are thin shims over the
shared basketball helper.

1. Public function `espn_<league>_<dataset>(primary_id, ..., *, raw=False, return_as_pandas=False, **kwargs)`.
2. `@overload` chain to type-narrow return based on `raw` / `return_as_pandas` flags.
3. Shared private helper `_espn_basketball_<dataset>(league, ...)` keeps the
   wbb/wnba pair DRY — wnba module is a thin wrapper that imports the helper
   and fixes the league slug.
4. Returns `pl.DataFrame` for single-table endpoints, `dict[str, pl.DataFrame]`
   for multi-table endpoints, or `dict` if `raw=True`.
5. Multi-table returns key on canonical category names (`Averages`, `Totals`,
   `Misc` for player stats), with an `Other` fallback bucket added only when
   ESPN ships a non-canonical category name. Empty frames carry the
   documented schema so callers always see a stable column set.
6. Snake-case columns via `sportsdataverse.dl_utils.underscore`.
7. Append the new module to the consolidated `[[tool.mypy.overrides]] module = [...]`
   list in `pyproject.toml`. Do NOT create a new override block per module.

### NFL — nflreadpy parity

The NFL submodule is a near drop-in replacement for [nflreadpy](https://github.com/nflverse/nflreadpy).
The canonical sdv-py names use the `load_nfl_*` prefix (cross-sport
disambiguation under the umbrella `sportsdataverse` package); inside
`sportsdataverse.nfl` itself we additionally export 25 nflreadpy-style
aliases without the prefix:

```python
import sportsdataverse.nfl as nfl
pbp        = nfl.load_pbp([2024])           # alias -> load_nfl_pbp
schedules  = nfl.load_schedules([2024])     # alias -> load_nfl_schedule
ngs        = nfl.load_nextgen_stats(stat_type="passing")
adv        = nfl.load_pfr_advstats(stat_type="pass", summary_level="season")
nfl.clear_cache()
```

The aliases are NOT re-exported at the top-level `sportsdataverse` package
on purpose — only inside `sportsdataverse.nfl`. New nflreadpy-parity
loaders should follow that same scoping rule.

**Unified loaders**: `load_nfl_nextgen_stats(stat_type=)` replaces three
per-type variants (`load_nfl_ngs_passing` / `_rushing` / `_receiving`) and
`load_nfl_pfr_advstats(stat_type=, summary_level=)` replaces eight
per-type/per-summary variants. The legacy per-type wrappers still exist as
thin shims that emit `DeprecationWarning` and dispatch to the unified
function. Don't add new per-type wrappers; extend the unified function.

**`load_nfl_ff_rankings`**: accepts both `kind=` (preferred) and `type=`
(nflreadpy's name; kept for parity). `type` shadows the builtin so the
codebase prefers `kind` internally.

**Caching layer** — `sportsdataverse/nfl/cache.py` +
`config.py`. Three modes selected via `NflConfig.cache_mode`:

| Mode | Storage | TTL |
|---|---|---|
| `memory` (default) | per-process dict | `cache_duration` seconds |
| `filesystem` | parquet under `cache_dir` | `cache_duration` seconds |
| `off` | no caching | n/a |

All 23 canonical loaders + 11 deprecated aliases are wrapped with
`@cached_loader`. The cache key hashes `(qualified_name, args, sorted_kwargs)`
and **excludes** `return_as_pandas` so a single stored polars frame serves
both polars and pandas callers (the conversion happens on read).

Env-var initialization (precedence: explicit `update_config()` > env > default):

| Env var | Effect |
|---|---|
| `SDV_PY_NFL_CACHE` | `memory` \| `filesystem` \| `off` |
| `SDV_PY_NFL_CACHE_DIR` | filesystem cache directory |
| `SDV_PY_NFL_CACHE_DURATION` | TTL in seconds |
| `SDV_PY_NFL_VERBOSE` | progress chatter on/off |
| `SDV_PY_NFL_TIMEOUT` | HTTP timeout in seconds |
| `SDV_PY_NFL_USER_AGENT` | custom UA string |

Programmatic access:

```python
from sportsdataverse.nfl import get_config, update_config, reset_config, clear_cache
update_config(cache_mode="filesystem", cache_duration=3600)
clear_cache()  # also wipes both memory + filesystem
```

**Static datasets** — `sportsdataverse/nfl/datasets.py` exports
three module-level dicts: `team_abbr_mapping` (relocations folded;
`OAK -> LV`, `SD -> LAC`, `STL -> LA`), `team_abbr_mapping_norelocate`
(historical identity preserved), and `player_name_mapping`. They're
inline-bundled (not separate JSON files) because the
`[tool.setuptools.package-data]` block in `pyproject.toml` only ships
`cfb/models/*` and `nfl/models/*`. Refresh procedure is documented in the
`datasets.py` module docstring.

**Date helpers** — `utils_date.get_current_nfl_season()` and
`get_current_nfl_week()` (also aliased as `get_current_season` /
`get_current_week` inside `sportsdataverse.nfl`).

When in doubt about the upstream API surface, check
`gh repo view nflverse/nflreadpy` — sdv-py mirrors nflreadpy's signatures
where practical.

### CFB — `cfb_play_participants` and the 0.36-live reconciliation

`sportsdataverse/cfb/cfb_play_participants.py` replaced 471 lines of regex
inside `cfb_pbp.CFBPlayProcess.__add_player_cols` with a 130-line
endpoint-delegated extractor. `__add_player_cols` now delegates to the
participants module and only runs a narrow regex fallback for ESPN
sidecar gaps (`sack_player_name2`, `fg_block_player_name`,
`punt_block_player_name`, `interception_player_name`).

**Three-tier resolution chain**, in order:

1. ESPN's per-play `participants[]` array (the authoritative source).
2. `cdn.espn.com/.../playbyplay` sidecar `playerHash` for display names
   (one round trip per game).
3. **`$ref` resolution** for athletes the sidecar omits (~6 per game on
   average — split sacks where the second sacker isn't on the leaders
   list, returners on lateral plays, etc.). Default `resolve_missing=True`,
   capped at 50 fetches/game (`resolve_missing_max=50`) so a pathological
   game can't run away. Set `resolve_missing=False` to disable.

**Hybrid scalar + list output**: per-play columns are emitted as both
`{type}_player_name` (scalar — the first / primary participant of that
type) and `{type}_player_names` (list — all participants of that type
on the play). Multi-entry types like split sacks are no longer silently
collapsed to a single name.

**The "0.36-live → main reconciliation"** landed in May 2026 and ported
~17 commits' worth of pandas-side CFB pbp bug fixes into the polars
`main` branch. Coverage includes yardage parsing, kneel-down handling,
half-edge cases, end-of-game WP, penalty-assessed-on-kickoff, plus the
full participants-module extraction described above.

### CFB — offline reprocess (`odds_override`, raw allowlist, `odds_source`) (0.0.52+)

`CFBPlayProcess` supports rebuilding a game's enriched output from on-disk raw JSON
without re-hitting ESPN — the contract the `cfbfastR-cfb-raw` scraper's reprocess
pipeline relies on. Three additive pieces:

- **Raw allowlist keeps `injuries` + `gameNotes`.** `espn_cfb_pbp(raw=True)` filters the
  summary to an allowlist (`incoming_keys_expected` in `cfb_pbp.py`); `injuries` and
  `gameNotes` are retained (default `[]` when absent). When adding a summary key the
  pipeline should preserve, add it to that list.
- **`odds_source` provenance.** `__helper_cfb_pickcenter` tags `self.odds_source` as
  `"summary_pickcenter"` | `"core_odds_api"` | `"default"` | `"injected"`, and the value
  is written into the returned payload (`pbp_txt["odds_source"]`) — not just the instance
  attribute — so dict consumers retain provenance.
- **`odds_override` constructor arg.** The spread/OU/homeFavorite are EPA/WPA *inputs*, not
  passthroughs. For 2024+ games the summary `pickcenter` is empty and the helper otherwise
  cascades to the live `sports.core.api.espn.com` odds endpoint (defaulting to
  `(2.5, 55.5, True, False)` on failure). Passing `CFBPlayProcess(odds_override={...})`
  with keys `gameSpread`/`overUnder`/`homeFavorite`/`gameSpreadAvailable` short-circuits
  resolution to those values (sets `odds_source="injected"`), so offline rebuilds never
  touch the network or inherit defaults. The override is **validated + type-coerced in
  `__init__`** (missing key / non-dict → `ValueError`). Default `None` = unchanged behavior.

Offline-rebuild pattern: `CFBPlayProcess(gameId, path_to_json=raw_dir, odds_override=<persisted>).cfb_pbp_disk()` then `.run_processing_pipeline()`.

### HTTP / retry layer

All HTTP goes through `sportsdataverse.dl_utils.download()`. As of May 2026
it's type-hinted, iterative (no recursion), initializes `response = None`
defensively, and re-raises the most recent exception when the retry budget
is exhausted (instead of returning an unbound variable). Wrappers do NOT
wrap the call in try/except — they trust `download()` to either return a
usable `requests.Response` or raise.

### Polars version

Pinned to `polars>=1.0,<2.0`. All seven `*_pbp.py` modules (cfb, nfl, nba,
nhl, mbb, wbb, wnba) were migrated wholesale from the 0.18 surface to 1.x
in May 2026 — roughly 165 call sites. If you find a 0.18-style API in
this codebase, treat it as a bug, not a style preference.

Use the modern API surface:

| Use this | Don't use this (0.18 era) |
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
| `s.str.strip_chars()` | `s.str.strip()` |
| `s.str.len_chars()` | `s.str.n_chars()` |

- **Boolean masks on polars expressions** use `pl.col("col") == True` /
  `pl.col("col") == False` explicitly (NOT `pl.col("col")` / `~pl.col("col")`).
  Ruff's `E712` is suppressed in `pyproject.toml` for this reason. The
  explicit form is more readable when the column itself is also a polars
  expression and avoids surprises around null handling.

- **Polars/Rust regex has no lookaround support.** `(?=...)`, `(?!...)`,
  `(?<=...)`, `(?<!...)` raise `ComputeError`. To stop a capture at a
  stopword without lookahead, use the **inline case-flag toggle**:
  `(?i)prefix(?-i: NAMES)`. The `(?-i:...)` group disables case-insensitivity
  for the captured names so lowercase narrative tails (`for`, `at`,
  `return`, `and`, etc.) cannot be folded into a captured proper noun.
  Example (extract a player name after "sacked by"):

  ```python
  pl.col("cleaned_text").str.extract(
      r"(?i)sacked by(?-i: ([A-Z][\w'\.\-]+(?:\s+[A-Z][\w'\.\-]+)?))", 1
  )
  ```

### Type hints

New modules MUST be fully typed (params + returns). Append to the strict
mypy override list in `pyproject.toml`. Legacy modules remain un-typed.

### Test gating

Live-API tests use `@skip_if_no_live` from `tests/conftest.py` and run only
when `SDV_PY_LIVE_TESTS=1` is set. CI does NOT set the var; live runs are
opt-in by contributor.

## Common Pitfalls

- **`cfb_play_participants` sidecar gaps are now (mostly) backfilled.**
  ESPN's `cdn.espn.com/.../playbyplay` sidecar omits ~6 athletes per game
  on average (split-sack secondary participants, lateral returners, etc.).
  The module's default `resolve_missing=True` fetches each missing
  athlete's `$ref` URL one-by-one (capped at 50/game via
  `resolve_missing_max`) before the per-play pivot. A narrow regex
  fallback against `cleaned_text` is still retained inside
  `cfb_pbp.__add_player_cols` for `sack_player_name2`,
  `fg_block_player_name`, `punt_block_player_name`, and
  `interception_player_name` — those four are documented sidecar
  blind spots. Don't add new regex extraction; extend the participants
  module instead.

- **Cache invalidation when modifying loaders.** The `@cached_loader`
  decorator hashes `(qualified_name, args, sorted_kwargs)` — it does NOT
  hash the URL or function body. If you change a loader's URL or
  resource path without renaming the function, callers will see stale
  data until they call `clear_cache()`. During development against a
  cached loader, prefer `update_config(cache_mode="off")` or
  `clear_cache()` between runs to avoid debugging phantom data.
- **Don't add new per-type NFL loaders.** `load_nfl_ngs_passing` /
  `_rushing` / `_receiving` and the eight per-type/per-summary
  `load_nfl_pfr_advstats_*` wrappers all emit `DeprecationWarning` and
  dispatch to the unified `load_nfl_nextgen_stats(stat_type=)` /
  `load_nfl_pfr_advstats(stat_type=, summary_level=)` functions.
  Extend the unified function; do not introduce new per-type wrappers.
- **`load_nfl_ff_rankings`: `kind=` vs `type=`.** Both work and resolve
  to the same parameter — `kind` is preferred internally because `type`
  shadows the builtin. nflreadpy uses `type=`, so we accept both for
  parity. Pass exactly one.
- **Polars/Rust regex has no lookaround support** (`(?=...)`, `(?!...)`,
  `(?<=...)`, `(?<!...)` raise `ComputeError`). Use the inline
  case-flag toggle `(?i)prefix(?-i: NAMES)` to stop a capture at a
  stopword without lookahead. See the example in the "Polars version"
  table above.
- **`docs/` is the Docusaurus site.** Internal working notes (specs,
  reconciliation maps, scratchpads, etc.) live in the gitignored `dev/`
  directory — NOT under `docs/` and NOT at the repo root. `dev/` is in
  `.gitignore` precisely because those files are working notes; if a doc
  graduates to contributor-visible, move it to the repo root and add it
  to git.
- **`requirements.txt`, `requirements-dev.txt`, and `setup.py` are all
  deleted as of May 2026.** All packaging metadata lives in
  `pyproject.toml` under PEP 621 `[project]`. The build path is
  `python -m build` (PEP 517); don't reintroduce `setup.py`, and don't
  add a `requirements*.txt`.
- **The 0.36-live branch is intentionally divergent.** Don't merge it
  wholesale into main — it's pandas-flavored and would undo the polars
  migration. Cherry-pick semantic fixes via translation; the
  reconciliation notes live in `dev/` (untracked).
  The May 2026 reconciliation already ported the bulk of the CFB pbp
  fixes (yardage, kneel-downs, half edges, WP, penalty-on-kickoff, etc.).
- **`pkg_resources` is removed in setuptools 81+.** `cfb_pbp.py` and
  `nfl_pbp.py` already migrated to `importlib.resources.files()`. Don't
  reintroduce `from pkg_resources import resource_filename`. If the
  `pkg_resources` API-deprecation `UserWarning` surfaces from a
  transitive dep, the noise is filtered in `pytest.ini`'s
  `filterwarnings`; investigate before suppressing further.
- **`psutil` is optional in `decorators.py`.** It's imported lazily so
  the package still imports cleanly when `psutil` isn't installed.
  Don't promote it to a hard runtime dep without a deliberate decision.
- **Polars literal-from-numpy no longer auto-broadcasts in 1.x.** Use
  `pl.lit(np_array).first()` to extract a scalar, or pass a Python value
  directly.
- **`pyjanitor 0.32.18+` silently switched to pandas 3.x.** Keep the
  defensive `pyjanitor<0.32.18` upper bound in `pyproject.toml` until
  pandas 3 is the project floor.

## Documentation Maintenance

- The Docusaurus site lives under `docs/`. The per-league reference subtree
  (`docs/docs/<sport>/index.md`, `<sport>/reference/*.md`, `_category_.json`,
  and `reference/parameters.md`) is **generated** from endpoint metadata by
  `python tools/codegen/generate.py --docs` — never hand-edit those. Conceptual
  pages OUTSIDE the generated league/`reference/` dirs (`intro.md`,
  `quality-of-life.md`, `architecture/`, `parsers/`) ARE hand-authored and are
  preserved across regeneration.
- Internal working notes (specs, reconciliation maps, scratchpads) live
  in `dev/`, which is gitignored. Promote a doc to the repo root only if
  it becomes contributor-visible reference material.
- `CONTRIBUTING.md` is the canonical contributor onboarding file (covers
  uv, conda, lint/typecheck, dep-bumping flow).
- `README.md` has Standard pip / Modern uv / Conda / Development install
  paths plus the runtime notes (Python 3.9-3.14, polars 1.x, NFL cache).
- `recipe/meta.yaml` + `recipe/README.md` ship the conda-build recipe and
  document the conda-forge feedstock submission flow. The local-source
  build (`conda build recipe/`) reads metadata from `pyproject.toml`
  through the PEP 517 path; the conda-forge variant pins to a PyPI sdist
  via `url:` + `sha256:`.

## Docstring conventions for new functions

Every public callable (function / class / method that doesn't start with
underscore) ships a Google-style docstring with `Args:` / `Returns:` /
`Raises:` blocks AND an `Example:` block in the napoleon literal-block
format. The canonical shape:

```
Example:
    Quick start::

        from sportsdataverse.<sport> import <fn>
        df = <fn>(<minimal args>)
        print(df.shape)

    Useful parameter combination::

        df_pd = <fn>(..., return_as_pandas=True)

    Pipeline next step (one line)::

        df.filter(pl.col("...") == ...).head()

    See Also:
        * `<companion package>`_ -- short rationale
        * `<alternative source>`_ -- short rationale

    .. _<companion package>: https://...
    .. _<alternative source>: https://...
```

Rules:

- Use the napoleon `Example:` heading (singular), blank line, then a
  literal block introduced by `::`. Indent the code block 4 spaces.
- Do NOT use raw `>>> ...` doctest prompts. `sphinx.ext.doctest` is
  enabled and would try to verify them — for live-API loaders the
  values drift, so doctest noise is guaranteed.
- Each example should be runnable as-is (copy-paste into a REPL).
- Keep examples short — 2-4 sub-blocks max per function. The pipeline
  next-step is ONE line, not a notebook.
- Cross-link to companion packages in the `See Also:` block. Canonical
  URLs are listed below; pick the relevant ones for the sport / domain.

Companion-package cross-link URLs:

| Package | URL | Domain |
|---|---|---|
| wehoop | <https://wehoop.sportsdataverse.org> | Women's basketball (R) |
| hoopR | <https://hoopR.sportsdataverse.org> | Men's basketball (R) |
| cfbfastR | <https://cfbfastR.sportsdataverse.org> | College football (R) |
| baseballr | <https://baseballr.sportsdataverse.org> | Baseball (R) |
| fastRhockey | <https://fastRhockey.sportsdataverse.org> | Hockey (R) |
| nflfastR | <https://www.nflfastr.com> | NFL (R) |
| nflverse | <https://nflverse.nflverse.com> | NFL ecosystem |
| nflreadpy | <https://github.com/nflverse/nflreadpy> | NFL (Python) |
| nba_api | <https://github.com/swar/nba_api> | NBA/WNBA (Python) |
| nhl-api-py | <https://github.com/coreyjs/nhl-api-py> | NHL (Python) |
| recruitR | <https://github.com/sportsdataverse/recruitR> | CFB recruiting (R) |

## Example notebooks

Intro/intermediate Jupyter notebooks live under `examples/notebooks/`,
one per sport plus a top-level cross-sport quickstart. Each demonstrates
the canonical surface for that sport — schedule, PBP, teams, season
stats, plus the package-wide cache + config layer where relevant. New
sport submodules should add a corresponding `0X_<sport>_intro.ipynb` so
the introductory walkthrough stays parallel across sports.

## Reference-docs build toolchain (codegen)

The legacy Sphinx pipeline (`Sphinx-docs/` + `create_docs.sh`) is **retired**.
Reference docs are now generated from the same YAML endpoint metadata that drives
the wrappers, via the codegen CLI:

- **Generate:** `python tools/codegen/generate.py --docs` rewrites the per-league
  reference subtree under `docs/docs/<sport>/` (full-clobbers each league dir +
  the shared `docs/docs/reference/` dir; conceptual pages outside them survive).
  The no-arg `python tools/codegen/generate.py` also regenerates docs alongside
  the wrappers/loaders/parsed modules.
- **Templates:** `tools/codegen/templates/_reference_block.jinja` (the 8-section
  per-function block) + `reference_page`/`league_index`/`loaders_page`/
  `parameter_reference`/`category_json` templates. `@return` tables come from
  `tools/codegen/schemas/*.yaml` (ESPN) and `schemas/loader_schemas.yaml` (loaders).
- **Drift gate:** `python tools/codegen/generate.py --check` fails on stale
  generated docs (orphan-checked only within the generated league/`reference/`
  dirs). Same gate runs in CI + the `sdv-codegen` pre-commit hook. Offline tests
  live in `tests/codegen/test_docs.py` + `test_doc_parity.py`.
- **Docusaurus:** `docs/sidebars.ts` drives each league as a clickable category
  (link → generated `index`) expanding to an autogenerated reference subtree, so
  new endpoints surface with no sidebar edit. Verify with
  `cd docs && yarn build` (broken-link warnings are confined to the frozen
  `0.0.50` version + CHANGELOG doctoc fragments).
- The `--docs` output is markdown the prose linters skip (`docs/docs/**` is
  excluded from doctoc + markdownlint), so generated tables/fences don't fight the
  hooks. Docstrings still use Google-style sections (`Args:`/`Returns:`/`Raises:`/
  `Example:`) — those feed the wrappers' runtime help, not a Sphinx build.
