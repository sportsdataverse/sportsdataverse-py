---
title: "NFL — additional Python functions — Build"
sidebar_label: "Build"
sidebar_position: 1
description: "NFL — additional Python functions — Build — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Build

### build_nfl_player_stats {#build_nfl_player_stats}

`build_nfl_player_stats(seasons: 'List[int]', *, summary_level: 'str' = 'week', season_type: 'str' = 'REG', source: 'str' = 'sdv', return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Build nflverse **player_stats** by aggregating SDV-native play-by-play.

A faithful polars port of nflfastR's `calculate_player_stats`
(`aggregate_game_stats.R`): per-player passing / rushing / receiving frames
are full-outer-joined on the group keys, special-teams touchdowns and fantasy
points are added, and player metadata is joined from
`sportsdataverse.nfl.load_nfl_players`. See the module docstring for the
SDV-PBP column-gap handling (`passing_epa` uses the exact `qb_epa`;
`rushing_epa` / `receiving_epa` use plain `epa` per nflfastR).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Four-digit NFL seasons to aggregate (e.g. `[2023]`). |
| `summary_level` | `str` | `'week'` | `"week"` (group on season + week + player_id, with `opponent_team`) or `"season"` (group on season + player_id, with `recent_team` = last team and `games` = distinct game count). |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"REG+POST"`. Pre-filters the play-by-play before aggregation. |
| `source` | `str` | `'sdv'` | Play-by-play release passed to `load_nfl_pbp`. Defaults to `"sdv"` (the SDV-native enriched release). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame in the published `load_nfl_player_stats` schema. At `summary_level="season"` the `week` / `season_type` / `opponent_team` columns are replaced by a `games` column.

**Example**

```python
from sportsdataverse.nfl import build_nfl_player_stats
wk = build_nfl_player_stats([2023], summary_level="week")
print(wk.shape)

# Season totals as pandas

df_pd = build_nfl_player_stats([2023], summary_level="season",
                               return_as_pandas=True)

# Pipeline next step (one line)

wk.filter(pl.col("attempts") >= 5).sort("passing_epa", descending=True).head()
```

### build_nfl_player_stats_def {#build_nfl_player_stats_def}

`build_nfl_player_stats_def(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Build player-level defensive stats from play-by-play (nflfastR parity).

A faithful polars port of nflfastR's deprecated
`calculate_player_stats_def()` (`aggregate_game_stats_def.R`). Tackle,
sack (half-sack = 0.5 weighting), pass-defense, interception, safety,
fumble (own/opponent recovery), penalty, and touchdown sub-frames are each
aggregated on `(season, week, team=defteam, player_id)` and full-outer
joined together, then player metadata is joined from
`sportsdataverse.nfl.load_nfl_players`.

Unlike `build_nfl_player_stats`, this function takes a
caller-supplied `pbp` frame directly rather than loading one -- matching
the R function's own signature.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying the wide nflverse defensive columns (`solo_tackle_1_player_id`, `sack_player_id`, `half_sack_{1,2}_player_id`, `interception_player_id`, `pass_defense_{1,2}_player_id`, `fumbled_{1,2}_team` / `fumble_recovery_{1,2}_team`, etc. -- the same columns `sportsdataverse.nfl.load_nfl_pbp` serves). |
| `weekly` | `bool` | `False` | If `True` return one row per (season, week, player); if `False` collapse to one row per `(player_id, team)` -- note this does NOT retain a `season` column even if `pbp` spans multiple seasons (see the module-level note above), matching the R source's own `group_by(player_id, team)` (no `season`). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with the `def_*` column set documented in the nflfastR-parity reference (weekly grain carries `season`/`week`/`season_type`; the season collapse replaces those with `games`).

**Example**

```python
from sportsdataverse.nfl import build_nfl_player_stats_def, load_nfl_pbp
pbp = load_nfl_pbp([2023])
wk = build_nfl_player_stats_def(pbp, weekly=True)
print(wk.shape)

# Season totals (one season's worth of ``pbp`` at a time)

season = build_nfl_player_stats_def(pbp, weekly=False)

# Pipeline next step (one line)

wk.sort("def_sacks", descending=True).head()
```

### build_nfl_player_stats_kicking {#build_nfl_player_stats_kicking}

`build_nfl_player_stats_kicking(pbp: 'pl.DataFrame', *, weekly: 'bool' = False, return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Build player-level kicking stats from play-by-play (nflfastR parity).

A faithful polars port of nflfastR's deprecated
`calculate_player_stats_kicking()` (`aggregate_game_stats_kicking.R`).
Field goals (made-distance buckets, `fg_long`, `fg_pct`, `;`-joined
distance lists), extra points, and game-winning-FG attempts (last drive of
the game, trailing by 2 or fewer points) are each aggregated on the kicker
and full-outer joined together, then player metadata is joined from
`sportsdataverse.nfl.load_nfl_players`.

Unlike `build_nfl_team_stats`, this function takes a caller-supplied
`pbp` frame directly rather than loading one -- matching the R
function's own signature.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play frame carrying `kicker_player_id` / `kicker_player_name`, `field_goal_attempt` / `field_goal_result` / `kick_distance`, `extra_point_attempt` / `extra_point_result`, `fixed_drive`, and `score_differential` (the same columns `sportsdataverse.nfl.load_nfl_pbp` serves). |
| `weekly` | `bool` | `False` | If `True` return one row per (season, week, player) with a `gwfg_distance` list column; if `False` collapse to one row per `(player_id, team)` with a `games` column and a `;`-joined `gwfg_distance_list` string column in place of `gwfg_distance` (the R source's own deliberate column-name change based on the `weekly` flag). Note this does NOT retain a `season` column even if `pbp` spans multiple seasons (see the module-level note above `build_nfl_player_stats_def`). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame with the `fg_*`/`pat_*`/`gwfg_*` column set documented in the nflfastR-parity reference.

**Example**

```python
from sportsdataverse.nfl import build_nfl_player_stats_kicking, load_nfl_pbp
pbp = load_nfl_pbp([2023])
wk = build_nfl_player_stats_kicking(pbp, weekly=True)
print(wk.shape)

# Season totals (one season's worth of ``pbp`` at a time)

season = build_nfl_player_stats_kicking(pbp, weekly=False)

# Pipeline next step (one line)

wk.filter(pl.col("fg_att") >= 1).sort("fg_pct", descending=True).head()
```

### build_nfl_players {#build_nfl_players}

`build_nfl_players(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build an SDV-native NFL players frame from ESPN's public athletes endpoint.

Walks ESPN's public NFL athletes index
(`sports.core.api.espn.com/v2/sports/football/leagues/nfl/athletes`),
resolves each athlete's detail resource, flattens it onto the SDV-native
players schema, **dedups to the highest numeric `espn_id` per
`(full_name, birth_date)`** (the ESPN ~2007 4-digit -> 7-digit id
migration left some players with two ids), and enriches `gsis_id` +
other cross-IDs by a best-effort join against
`sportsdataverse.nfl.load_nfl_players`.

This is the **public ESPN-athletes tier only** — a partial mirror of
nflverse's full seven-source `players.parquet` (three of those sources,
PFR / OTC / PFF, require private credentials). ESPN-native rows with no
nflverse match keep only their ESPN fields (cross-IDs left null). For the
full identity master prefer `sportsdataverse.nfl.load_nfl_players`;
use `build_nfl_players` when you need an SDV-native frame that depends
only on the live public ESPN API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-player `DataFrame` with the documented schema (`espn_id`, `full_name`, `first_name`, `last_name`, `position`, `team`, `jersey`, `height`, `weight`, `birth_date`, `status`, `headshot_url`, `gsis_id`, `esb_id`, `pfr_id`, `pff_id`, `smart_id`, `college`). An empty fetch yields a zero-row frame carrying the same column set.

**Example**

```python
from sportsdataverse.nfl import build_nfl_players
players = build_nfl_players()
print(players.shape)

# Pandas output

df = build_nfl_players(return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
build_nfl_players().filter(pl.col("position") == "QB").head()
```

### build_nfl_rosters {#build_nfl_rosters}

`build_nfl_rosters(seasons: 'List[int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build SDV-native NFL season rosters from the public Shield API.

For each `(season, team)` the public NFL Shield endpoint
`/football/v2/rosters` returns (reached through
`sportsdataverse.nfl.nfl_rosters`), every player in the `persons[]`
array is flattened onto the SDV-native season-roster schema, team
abbreviations are folded to the nflverse standard (season-aware
relocations), and cross-system IDs + college are enriched by a best-effort
left join against `sportsdataverse.nfl.load_nfl_players` on
`gsis_id`.

This is the **public Shield tier only** — a partial mirror of nflverse's
full three-tier roster product. Shield supplies `gsis_id` densely across
all seasons, but the cross-system IDs (`espn_id`, `sportradar_id`,
`yahoo_id`, `rotowire_id`, `pff_id`, `pfr_id`, `fantasy_data_id`,
`sleeper_id`) and `college` are only as dense as the players-table
cross-walk, which is **sparse for pre-2016 seasons**. For the richest roster
data prefer `sportsdataverse.nfl.load_nfl_rosters` (reads nflverse's
published parquet); use `build_nfl_rosters` when you need an
SDV-native frame that depends only on the live NFL Shield API.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Seasons to build (e.g. `[2023]` or `range(2020, 2025)`). A single `int` is accepted and wrapped. A season Shield returns no data for contributes no rows rather than raising. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-player season-roster `DataFrame` with the documented schema. An empty / missing season yields a zero-row frame carrying the same column set (never a raise).

**Example**

```python
from sportsdataverse.nfl import build_nfl_rosters
rosters = build_nfl_rosters([2023])
print(rosters.shape)

# Multi-season build, pandas output

df = build_nfl_rosters(range(2021, 2024), return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
build_nfl_rosters([2023]).filter(pl.col("team") == "KC").head()
```

### build_nfl_season {#build_nfl_season}

`build_nfl_season(game_ids: 'list[int] | None' = None, *, seasons: 'list[int] | None' = None, source: 'str' = 'espn', return_as_pandas: 'bool' = False, raw_dir: "'str | Path | None'" = None, schedule_lookup: "'dict[str, dict[str, Any]] | None'" = None) -> "'pl.DataFrame | pd.DataFrame'"`

Compile play-by-play for multiple NFL games into one tidy frame.

The `source` parameter determines which input parameter is required:

- `source="espn"` — requires *game_ids*; *seasons* must be `None`.
- `source="nflverse"` — requires *seasons*; *game_ids* must be `None`.
- `source="shield"` — requires *seasons* and *raw_dir*; *game_ids* must be `None`.

For ESPN games the function either loads a previously cached plays frame or
processes the game fresh via `NFLPlayProcess`.  Individual game failures
are logged and skipped so a single bad game does not abort the whole season
build.  The per-game frames are concatenated with `how="diagonal_relaxed"`
(schema union, missing columns filled with `null`) so games with slightly
different column sets merge cleanly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `list[int] \| None` | `None` | ESPN event IDs to compile (e.g. `[401671801, 401671802]`). Required when `source="espn"`; must be `None` for other sources. |
| `seasons` | `list[int] \| None` | `None` | Season years to compile (e.g. `[2023, 2024]`). Required when `source="nflverse"`; must be `None` for other sources. |
| `source` | `str` | `'espn'` | Data source. - `"espn"` *(default)*: each game is processed via `NFLPlayProcess(gameId=gid).espn_nfl_pbp()` + `run_processing_pipeline()`. Pass *game_ids*. - `"nflverse"`: delegates to `sportsdataverse.nfl.load_nfl_pbp` for the requested seasons. Pass *seasons*. Returns the full pre-enriched season frame as-is. - `"shield"`: reconstructs nflverse-shape play-by-play from a committed library of Shield (api.nfl.com) per-game JSON files via `sportsdataverse.nfl.shield_pbp.build_season` (the nflfastR parser port graduated from nfl-data's `native_pbp`). Pass *seasons* and *raw_dir*. Preseason games are skipped and TIMEOUT rows dropped, matching nflverse's row set. The frame is NOT EP/WP-enriched; feed it to `sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` for the `nfl_model_pbp` columns. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame` instead of polars. |
| `raw_dir` | `str \| Path \| None` | `None` | `source="shield"` only. Root of the per-game Shield JSON library laid out as `{raw_dir}/{season}/{game_id}.json` (the `nfl-raw` repo's `nfl/raw`). Required for the shield source; must be `None` otherwise. |
| `schedule_lookup` | `dict[str, dict[str, Any]] \| None` | `None` | `source="shield"` only. `{game_id: {"roof": ..., "spread_line": ..., "total_line": ...}}` supplying the game-level fields the Shield feed omits. `None` *(default)* builds it from `sportsdataverse.nfl.load_nfl_schedule` for each season, degrading to nulls with a `RuntimeWarning` if the schedule cannot be loaded. Pass `{}` to skip the lookup (hermetic; the three columns stay null). |

**Returns**

All plays from the requested games/seasons, concatenated with schema-union semantics (missing columns are `null`). Returns a zero-row frame if every game failed (ESPN source only). When *return_as_pandas* is `True`, returns a `pandas.DataFrame` instead. For `source="shield"` the frame carries the nflverse base columns (233; a superset of the EP/WP/CP training contract) with the same names, types and meanings as `sportsdataverse.nfl.load_nfl_model_pbp` minus the EP/WP/CP enrichment columns: identifiers (`game_id`, `play_id`, `posteam`, `defteam`), game state (`down`, `ydstogo`, `yardline_100`, `qtr`, `half_seconds_remaining`, `game_seconds_remaining`, `score_differential`, `posteam_timeouts_remaining`), play classification (`play_type`, `pass`, `rush`, `desc`, `yards_gained`, `touchdown`, `field_goal_result`), drive/series (`fixed_drive`, `fixed_drive_result`, `series`, `series_result`), schedule fields (`roof`, `spread_line`, `total_line`) and game outcome (`home_score`, `away_score`, `result`).

**Example**

```python
from sportsdataverse.nfl import build_nfl_season
df = build_nfl_season(game_ids=[401671801, 401671802])
print(df.shape)

# nflverse season compile (pass season years)

from sportsdataverse.nfl import build_nfl_season
df = build_nfl_season(seasons=[2023], source="nflverse")
print(df.shape)

# Shield season compile from a committed raw library (nfl-raw checkout)

from sportsdataverse.nfl import build_nfl_season
df = build_nfl_season(seasons=[2024], source="shield", raw_dir="nfl-raw/nfl/raw")
print(df.shape)

# With filesystem cache enabled (ESPN)

from sportsdataverse.nfl import build_nfl_season, update_config
update_config(cache_mode="filesystem")
df = build_nfl_season(game_ids=[401671801, 401671802])  # processes + caches
df2 = build_nfl_season(game_ids=[401671801, 401671802]) # served from cache

# Pandas output

from sportsdataverse.nfl import build_nfl_season
df_pd = build_nfl_season(game_ids=[401671801], return_as_pandas=True)
print(df_pd.shape)
```

### build_nfl_team_stats {#build_nfl_team_stats}

`build_nfl_team_stats(seasons: 'List[int]', *, summary_level: 'str' = 'week', season_type: 'str' = 'REG', source: 'str' = 'sdv', return_as_pandas: 'bool' = False) -> "pl.DataFrame | 'pd.DataFrame'"`

Build nflverse **team_stats** by aggregating SDV-native play-by-play.

A faithful polars port of nflfastR's `calculate_stats(stat_type = "team")`
(the `aggregate_game_stats*` family). Offense is keyed on `posteam`,
defense on the tackler's team (per-play `*_team` slot tags -- NOT
`defteam`, which double-counts on return plays), kicking on `posteam`,
and returns / penalties / timeouts on the relevant play team tag. See the
module docstring for the full grouping + SDV-PBP gap notes (`passing_epa`
uses the exact `qb_epa`; `gwfg_*` derive from `fixed_drive`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | Four-digit NFL seasons to aggregate (e.g. `[2023]`). |
| `summary_level` | `str` | `'week'` | `"week"` (group on season + week + team, with `opponent_team`) or `"season"` (group on season + team, with a `games` distinct-game count replacing week / season_type / opponent_team). |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"REG+POST"`. Pre-filters the play-by-play before aggregation. |
| `source` | `str` | `'sdv'` | Play-by-play release passed to `load_nfl_pbp`. Defaults to `"sdv"` (the SDV-native enriched release). |
| `return_as_pandas` | `bool` | `False` | If `True` return a pandas DataFrame; else polars. |

**Returns**

A polars (or pandas) DataFrame in the published `load_nfl_team_stats` schema (~102 columns). At `summary_level="season"` the `week` / `season_type` / `opponent_team` columns are replaced by a `games` column.

**Example**

```python
from sportsdataverse.nfl import build_nfl_team_stats
wk = build_nfl_team_stats([2023], summary_level="week")
print(wk.shape)

# Season totals as pandas

df_pd = build_nfl_team_stats([2023], summary_level="season",
                             return_as_pandas=True)

# Pipeline next step (one line)

wk.sort("def_sacks", descending=True).head()
```
