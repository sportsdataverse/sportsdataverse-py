<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [0.0.50 Release: May 7, 2026](#0050-release-may-7-2026)
  - [Packaging modernization](#packaging-modernization)
  - [Documentation toolchain](#documentation-toolchain)
  - [Runnable docstring examples (~190 functions)](#runnable-docstring-examples-190-functions)
  - [Example notebooks](#example-notebooks)
  - [NFL -- nflreadpy parity](#nfl----nflreadpy-parity)
  - [NFL -- caching and configuration](#nfl----caching-and-configuration)
  - [NFL -- static datasets](#nfl----static-datasets)
  - [NFL -- pickcenter / odds modern path](#nfl----pickcenter--odds-modern-path)
  - [NFL -- `load_nfl_schedule` parquet port](#nfl----load_nfl_schedule-parquet-port)
  - [WBB / WNBA -- new ESPN scrape modules](#wbb--wnba----new-espn-scrape-modules)
  - [CFB -- `__add_player_cols` collapse and reconciliation](#cfb----__add_player_cols-collapse-and-reconciliation)
  - [CFB -- pandas → polars 1.x bug-fix reconciliation (`0.36-live` → `main`)](#cfb----pandas-%E2%86%92-polars-1x-bug-fix-reconciliation-036-live-%E2%86%92-main)
  - [Infrastructure and tooling](#infrastructure-and-tooling)
  - [Bug fixes](#bug-fixes)
  - [Deprecations](#deprecations)
- [0.0.40 Release: December 6, 2025](#0040-release-december-6-2025)
- [0.0.38-39 Release: August 28, 2023](#0038-39-release-august-28-2023)
- [0.0.36-37 Release: July 9, 2023](#0036-37-release-july-9-2023)
- [0.0.34-35 Release: May 7-9, 2023](#0034-35-release-may-7-9-2023)
- [0.0.18 Release: July 25, 2022](#0018-release-july-25-2022)
- [0.0.17 Release: July 9, 2022](#0017-release-july-9-2022)
- [0.0.15 Release: May 8, 2022](#0015-release-may-8-2022)
- [0.0.14 Release: March 16, 2022](#0014-release-march-16-2022)
- [0.0.12 Release: February 24, 2022](#0012-release-february-24-2022)
- [0.0.5 Release: October 20, 2021](#005-release-october-20-2021)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## 0.0.50 Release: May 7, 2026

This release is a big one: a near drop-in nflreadpy-parity surface inside `sportsdataverse.nfl` (six new loaders, two unified per-type loaders, a caching layer, runtime config, and three static datasets), four new ESPN modules across `wbb` and `wnba`, a new `cfb_play_participants` module and a corresponding ~340-line collapse inside `cfb_pbp.__add_player_cols`, and the long-running `0.36-live` polars-1.x reconciliation finally merging into `main`. Round bump to `0.0.50` (rather than `0.0.41`) to signal scope; we are still alpha.

### Packaging modernization

- Migrated all packaging metadata from `setup.py` to PEP 621 `[project]` in `pyproject.toml`. `setup.py` is removed; `python -m build` is the only supported build path.
- License switched from classifier (`License :: OSI Approved :: MIT License`) to SPDX expression (`license = "MIT"` + `license-files = ["LICENSE"]`) for Metadata 2.4 compliance.
- New `recipe/meta.yaml` ships a conda-build recipe (`noarch: python`) that consumes `pyproject.toml` directly. Local builds work today via `conda build recipe/`; `recipe/README.md` documents the conda-forge feedstock submission flow.
- New `.github/workflows/conda-build.yml` verifies the recipe on every PR that touches `recipe/` or `pyproject.toml`, plus on every release.
- `python-publish.yml` switched from `python setup.py sdist bdist_wheel` to `python -m build`. `tests.yml` switched from `pip install setuptools pytest wheel` + `pip install .[all]` to a single `pip install ".[tests]"` install step.

### Documentation toolchain

- Added `sphinx.ext.napoleon` to `Sphinx-docs/conf.py` with explicit Google-style settings -- the new `wbb` / `wnba` / `nfl` / `cfb` modules use Google-style docstrings (Args / Returns / Raises) and these were producing 22 docutils warnings on build before napoleon was wired up.
- Added a no-op `visit_abbreviation` shim to the markdown translator in `Sphinx-docs/conf.py`. Sphinx 9 emits `abbreviation` nodes for the keyword-only `*` separator in rendered function signatures, and `sphinx-markdown-builder` 0.6.10 has no visitor for that node type. The shim emits the inner text and skips the node, so the build is now warning-free under `sphinx-build -W`.
- Module docstrings in `cfb_play_participants.py` and `nfl/utils_date.py` had bullet lists immediately following a `Caveats:` / `NFL season convention:` paragraph header. Added the required blank line + asterisk markers so docutils parses them as proper RST bullet lists.

### Runnable docstring examples (~190 functions)

- Every public callable across `cfb`, `nfl`, `nba`, `nhl`, `mbb`, `wbb`, `wnba`, `dl_utils`, `decorators`, `errors`, `nfl/cache`, `nfl/config`, `nfl/datasets`, `nfl/utils_date`, and the top-level package now ships a multi-block `Example:` section: a quick-start invocation, one or two useful parameter combinations, a one-line pipeline next-step, and a `See Also:` block with cross-links to companion R packages (`wehoop`, `hoopR`, `cfbfastR`, `baseballr`, `fastRhockey`), `nflverse`, `nflreadpy`, `nba_api`, and `nhl-api-py` where applicable.
- Examples use the napoleon literal-block format (heading + `::` + 4-space indented code) so they render as proper code blocks in the markdown docs without triggering `sphinx.ext.doctest`. Users can copy-paste any block and run it as-is.
- Existing one-line backtick-wrapped examples (the legacy `Example: <inline call>` shape) were replaced (not appended) so each function has exactly one `Example:` section.

### Example notebooks

- Seven new Jupyter notebooks under `examples/notebooks/`: `01_quickstart.ipynb`, `02_cfb_intro.ipynb`, `03_nfl_intro.ipynb`, `04_nba_intro.ipynb`, `05_wbb_wnba_intro.ipynb`, `06_mbb_intro.ipynb`, `07_nhl_intro.ipynb`. Intro/intermediate level — schedule, pbp, team / player / season-stats endpoints, the `nfl.update_config` / `clear_cache` / `get_current_*` runtime surface, and a small pipeline example per sport. Outputs cleared so the user runs them locally; cross-references link to companion R packages and alternative Python libraries.

### NFL -- nflreadpy parity

- Six new loaders: `load_nfl_team_stats`, `load_nfl_ftn_charting`, `load_nfl_trades`, `load_nfl_ff_playerids`, `load_nfl_ff_rankings`, `load_nfl_ff_opportunity`.
- Two new utility helpers in `nfl/utils_date.py`: `get_current_nfl_season()`, `get_current_nfl_week()`.
- Unified `load_nfl_nextgen_stats(stat_type=...)` consolidating the per-type variants. The per-type functions are kept as aliases that emit `DeprecationWarning` and forward to the unified entry point.
- Unified `load_nfl_pfr_advstats(stat_type=, summary_level=)` consolidating eight per-type/per-summary functions, with the same deprecation alias pattern.
- 25 nflreadpy-parity aliases inside `sportsdataverse.nfl` (`load_pbp` ↔ `load_nfl_pbp`, etc.). Identity-equivalent -- no perf overhead, just a friendlier import surface for nflreadpy users.
- `kind=` parameter added to `load_nfl_ff_rankings` as the preferred name; `type=` retained for nflreadpy parity.

### NFL -- caching and configuration

- New caching layer in `sportsdataverse.nfl.cache` with both memory and filesystem backends and TTL support.
- `clear_cache()` for explicit invalidation.
- New `NflConfig` plus `update_config()`, with env-var initialization: `SDV_PY_NFL_CACHE`, `SDV_PY_NFL_CACHE_DIR`, `SDV_PY_NFL_CACHE_DURATION`, `SDV_PY_NFL_VERBOSE`, `SDV_PY_NFL_TIMEOUT`, `SDV_PY_NFL_USER_AGENT`.
- All 23 canonical loaders plus the 11 deprecated aliases are decorated with `@cached_loader`.
- `return_as_pandas=True` round-trips correctly through the cache: a single polars frame is stored, and conversion happens on read.

### NFL -- static datasets

- `team_abbr_mapping` (143 entries, relocations folded into the modern abbreviation).
- `team_abbr_mapping_norelocate` (143 entries, history preserved).
- `player_name_mapping` (136 entries, common-variant → canonical).
- All three are eagerly loaded at import time and inline-bundled in the package -- no separate JSON files to ship.

### NFL -- pickcenter / odds modern path

- `__helper__espn_nfl_odds_information__` now hits the modern `sports.core.api.espn.com/v2/.../events/{gid}/competitions/{gid}/odds` endpoint when the legacy `summary?event=` `pickcenter` array is empty (true for all 2024+ games).
- Cascades to defaults `(2.5, 55.5, True, False)` only if both modern and legacy paths fail.
- For example, the 2024 CFP semifinal previously returned `(2.5, 55.5, True, False)` and now correctly returns `(-3.5, 67.5, True, True)`.

### NFL -- `load_nfl_schedule` parquet port

- Switched from the stale `nflverse-pbp/master/schedules/sched_{season}.rds` (which was 404'ing on every season) to the modern `nflverse-data/releases/download/schedules/games.parquet`. One combined file, 1999--2025, 7,276 rows × 46 cols.

### WBB / WNBA -- new ESPN scrape modules

- `wbb_team_roster` / `wnba_team_roster`: per (team, season) roster.
- `wbb_player_stats` / `wnba_player_stats`: per (athlete, season) stats. Returns a multi-table dict with canonical keys `Averages`, `Totals`, `Misc`, plus an `Other` fallback when ESPN exposes a non-canonical category name.
- All four ship with full `@overload` typing (mypy-strict), polars 1.x APIs, and `snake_case` columns via `dl_utils.underscore`.
- All four are namespace-exported from `sportsdataverse.wbb.__init__` and `sportsdataverse.wnba.__init__`.

### CFB -- `__add_player_cols` collapse and reconciliation

- New `cfb_play_participants` module hits the ESPN `events/{gid}/competitions/{gid}/plays` participants endpoint, with `$ref` resolution (default-on, `resolve_missing=True`) for athletes missing from the sidecar.
- `cfb_pbp.__add_player_cols` shrunk from 471 lines of regex extraction to ~130 lines that delegate to the participants module.
- All 19 legacy `_player_name` columns preserved via an alias mapping.
- Hybrid scalar + list-column output: `{type}_player_name` plus `{type}_player_names`, so multi-entry types like split sacks aren't silently collapsed to a single name.
- Targeted regex fallbacks retained as a tertiary safety net for `sack_player_name2`, `fg_block_player_name`, `punt_block_player_name`, and `interception_player_name` -- ESPN's sidecar has documented gaps for those.

### CFB -- pandas → polars 1.x bug-fix reconciliation (`0.36-live` → `main`)

- Foundation: new `cleaned_text` column normalizes ESPN play descriptions and is the single source of truth for downstream feature extraction.
- Behavioral: kneel-down semantics flag plus `scrimmage_play` exclusion.
- Yardage: structural rewrite of `__add_yardage_cols` (~150-line `np.select` chain → `pl.when().then()` chain), pass-yards regex tightened from `(?<=for)` to `(?<=[\s,]for)`, full punt rewrite, fair-catch fix.
- Helper-features: end-state edge cases, NCG 2025 GW play hardcode, `lead_half` end-of-half fix, OOB punts block, FG classification correction, `end.TimeSecsRem` shift direction flipped from lag to lead -- which is what WPA inputs expected all along.
- WPA: `__process_wpa` end-of-game branch rewrite plus onside-kick rewrite, plus `penalty_assessed_on_kickoff` plumbing across `__setup_penalty_data` + `__process_epa` + `__process_wpa`.
- Player names: extraction migrated to `cleaned_text` everywhere.

### Infrastructure and tooling

- Polars 1.x migration across `cfb/cfb_pbp.py`, `nfl/nfl_pbp.py`, `mbb/mbb_pbp.py`, `nba/nba_pbp.py`, `nhl/nhl_pbp.py`, `wbb/wbb_pbp.py`, `wnba/wnba_pbp.py`. Roughly 165 API translation sites: `groupby` → `group_by`, `with_row_count` → `with_row_index`, `apply` → `map_elements` (with explicit `return_dtype`), struct list-arg → varargs, `shift_and_fill` → `shift`, `cumsum` → `cum_sum`, `str.strip` → `str.strip_chars`, `str.n_chars` → `str.len_chars`, outer-join → `full` + `coalesce`, `write_json` kwargs.
- Polars 1.x `is_in` same-datatype deprecation: switched to `.implode()` for the global-containment idiom.
- `pkg_resources.resource_filename` → `importlib.resources.files()` in `cfb_pbp.py` and `nfl_pbp.py`. Setuptools 81+ removes `pkg_resources`.
- Ruff `E712` suppression in `pyproject.toml`: the codebase intentionally uses `pl.col(...) == True/False` for polars boolean masks because the polars expression engine requires it (`is True/False` doesn't work on `pl.Expr`).
- `download()` retry rewrite: iterative loop instead of recursion, defensive `response = None` init, re-raises the last captured exception when the retry budget is exhausted.
- Sphinx docstring and toctree cleanup -- the docs build is now warning-free.
- `psutil` made optional in `decorators.py` (previously an undeclared transitive dep that broke autodoc).

### Bug fixes

- `test_havoc_rate` corrected for both `cfb` and `nfl`: `def_int` field name fix, bounded `<=` assertion, `def_box.sort()` for deterministic group_by emit order, and `turnover_box` now produces a cli warning instead of silently padding an empty dict.
- `yds_punted` duplicate definition removed.
- `drive.id` NCG 2025 GW play hardcode.
- `is_in(col)` → `is_in(col.implode())` for global containment, applied across `cfb_pbp` and `nfl_pbp`.

### Deprecations

- Four NFL loader families now consolidate per-type variants into a single unified function: `load_nfl_nextgen_stats(stat_type=...)` and `load_nfl_pfr_advstats(stat_type=, summary_level=)`. The per-type names continue to work but emit a `DeprecationWarning` pointing at the unified function. No removal yet.

## 0.0.40 Release: December 6, 2025

- Minor changes to mbb_calendar and wbb_calendar functions to include all games, even when top 25 teams are not competing

## 0.0.38-39 Release: August 28, 2023

- Minor changes to cfb_pbp functions to improve WP calculation and player parsing.

## 0.0.36-37 Release: July 9, 2023

- Switched most under the hood dataframe operations to use the python `polars` library and many functions now have a parameter `return_as_pandas` which defaults to `False` but can be set to `True` to return a pandas dataframe instead of a polars dataframe. This is a **breaking change.**
- Added `**kwargs` which pass arguments to the `dl_utils.download()` function, including `headers`, `proxy`, `timeout` (default 30s), `num_retries` (default = 15), `logger` (default = None)
- Function `espn_cfb_game_rosters()` added.
- Function `espn_nba_game_rosters()` added.
- Function `espn_nfl_game_rosters()` added.
- Function `espn_nhl_game_rosters()` added.
- Function `espn_wbb_game_rosters()` added.
- Function `espn_wnba_game_rosters()` added.
- Function `load_cfb_betting_lines()` added (only 2006 through 2019).

## 0.0.34-35 Release: May 7-9, 2023

- Reconfigured some imports
- Improved compliance with pandas upgrades
- Updated loader locations to use sportsdataverse-data releases and nflverse releases
- Flattened the returned results somewhat for "sportsdataverse.cfb.espn_cfb_schedule()" functions, but also now including some nested data frame and list columns

## 0.0.18 Release: July 25, 2022

- Added ondays parameter to ESPN calendar functions
- Renamed "sportsdataverse.cfb.cfb_teams()" to "sportsdataverse.cfb.espn_cfb_teams()" to avoid an edge case issue when running the function.

## 0.0.17 Release: July 9, 2022

- Added MLBAM API functionality to the sportsdataverse-py package. For more information on how to use these new functions, refer to the docs.
- Fixed a bug where the "sportsdataverse.nfl.load_nfl_schedule()" function would cause a 404 error when run.
- For functions where multiple files are loaded in, progress bars have been added to indicate how far along the sportsdataverse-py package is in completing its task(s).
- Renamed "sportsdataverse.cfb.cfb_teams()" to "sportsdataverse.cfb.get_cfb_teams()" to avoid an edge case issue when running the function.

## 0.0.15 Release: May 8, 2022

- Refactor schedule and teams functions for all existing leagues.
- Created more robust home/away mappings to simplify assignment.

## 0.0.14 Release: March 16, 2022

- Refactor schedule and teams functions for all existing leagues.
- Created more robust home/away mappings to simplify assignment.

## 0.0.12 Release: February 24, 2022

- Minor refactor to all the pbp functions, attempting to normalize behavior.
- Adding raw parameter to same functions to return object as it comes in without any transformation
- Adding some config file corrections.

## 0.0.5 Release: October 20, 2021

- f'in round
- findin' out
