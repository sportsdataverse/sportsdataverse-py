---
title: "NFL — additional Python functions — Play-by-play processing"
sidebar_label: "Play-by-play processing"
sidebar_position: 10
description: "NFL — additional Python functions — Play-by-play processing — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Play-by-play processing

### NFLPlayProcess {#NFLPlayProcess}

`NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)`

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN `summary` endpoint (or a local JSON dump) and pipes the
result through a chain of feature-engineering steps -- down/distance,
play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced
box score. Use `run_processing_pipeline()` for the full feature set
or `run_cleaning_pipeline()` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` | `int` | `0` | ESPN `event` id (e.g. `401671801`). |
| `raw` | `bool` | `False` | If `True`, `espn_nfl_pbp()` returns the ESPN payload untouched. If `False` (default), it normalizes keys. |
| `path_to_json` | `str` | `'/'` | Directory containing `{gameId}.json` for the `nfl_pbp_disk()` flow (offline replay). |
| `return_keys` | `list[str] \| None` | `None` | If supplied, `run_processing_pipeline` returns only the listed keys from the result dict. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

# Offline replay from a JSON dump

proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()

# Subset the return payload

proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

**Methods**

#### NFLPlayProcess.corrupt_pbp_check

`NFLPlayProcess.corrupt_pbp_check()`

Detect ESPN payloads that look corrupt or partial.

Returns `True` when one of three guard conditions trips:

* No plays at all.
* Fewer than 50 plays for a game ESPN reports as completed.
* More than 500 plays for a game ESPN reports as completed.

`run_processing_pipeline()` and `run_cleaning_pipeline()` use
this to skip feature engineering on obviously broken payloads.

**Returns**

`True` if the payload looks corrupt; `False` otherwise.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
if not proc.corrupt_pbp_check():
    result = proc.run_processing_pipeline()
```

#### NFLPlayProcess.create_box_score

`NFLPlayProcess.create_box_score(play_df)`

Build the advanced box score (passer / rusher / receiver / team / situational / defensive / turnover / drives)

from a feature-engineered plays DataFrame.

This is normally called by `run_processing_pipeline()` -- it
auto-runs the pipeline first if it hasn't been triggered yet, so
callers can also invoke it directly on a freshly-instantiated
processor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `play_df` | `pl.DataFrame` |  | The plays frame produced after the full feature-engineering chain (downs, play-type flags, EPA, WPA, drive aggregation). |

**Returns**

Box score keyed by `"pass"`, `"rush"`, `"receiver"`, `"team"`, `"situational"`, `"defensive"`, `"turnover"`, `"drives"` -- each value a list of dicts ready to be serialized.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
box = result["advBoxScore"]
sorted(box.keys())
```

#### NFLPlayProcess.espn_nfl_pbp

`NFLPlayProcess.espn_nfl_pbp(summary=None, **kwargs)`

espn_nfl_pbp() - Pull the game by id. Data from API endpoints: `nfl/playbyplay`, `nfl/summary`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `summary` | `dict` | `None` | A previously fetched ESPN summary payload. When given, no request is made -- the offline path for committed raw libraries -- and the pipeline joins participants only if `participants=` was passed at construction (it never fetches them, nor a roster, for a supplied summary). |

**Returns**

Dictionary of game data with keys - "gameId", "plays", "boxscore", "header", "broadcasts", "videos", "playByPlaySource", "standings", "leaders", "timeouts", "homeTeamSpread", "overUnder", "pickcenter", "againstTheSpread", "odds", "predictor", "winprobability", "espnWP", "gameInfo", "season"

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
payload = proc.espn_nfl_pbp()
sorted(payload.keys())[:5]

# Raw ESPN passthrough (no key normalization)

proc_raw = NFLPlayProcess(gameId=401220403, raw=True)
espn_dump = proc_raw.espn_nfl_pbp()

# Chain into the full processing pipeline

proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
```

#### NFLPlayProcess.nfl_pbp_disk

`NFLPlayProcess.nfl_pbp_disk()`

Load a previously-saved ESPN payload from `{path_to_json}/{gameId}.json`.

Use this to replay an old game offline without hitting the ESPN
endpoint -- handy for snapshot-driven tests and reproducible
feature engineering.

**Returns**

The parsed JSON content; also stored on `self.json`.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
result = proc.run_processing_pipeline()
```

#### NFLPlayProcess.nfl_pbp_json

`NFLPlayProcess.nfl_pbp_json(**kwargs)`

Return the JSON payload currently attached to this `NFLPlayProcess` instance.

`espn_nfl_pbp()` (live, or `summary=` offline) and `nfl_pbp_disk()`
attach the payload; this returns it unchanged.

**Returns**

dict | None: The attached payload (`self.json`); `None` before one is attached.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401220403)
proc.espn_nfl_pbp()
payload = proc.nfl_pbp_json()
```

#### NFLPlayProcess.run_cleaning_pipeline

`NFLPlayProcess.run_cleaning_pipeline()`

Run the lighter cleaning pipeline against `self.json`.

Identical to `run_processing_pipeline()` up through the
add_spread_time` step but stops short of EPA / WPA / QBR /
drive aggregation and the advanced box score. Use this when you
want clean play structure without the modeled features.

**Returns**

The cleaned game dict (or the subset specified by `return_keys` at construction).

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
cleaned = proc.run_cleaning_pipeline()
"plays" in cleaned and "advBoxScore" not in cleaned
```

#### NFLPlayProcess.run_processing_pipeline

`NFLPlayProcess.run_processing_pipeline(validate: 'bool' = False)`

Run the full feature-engineering pipeline against `self.json`.

Pipes the plays frame through the chain of helpers: downs,
play-type flags, rush/pass flags, team-score variables, new play
types, penalties, play-category flags, yardage cols, player cols,
post-play cols, spread time, EPA, WPA, drive data, and QBR --
followed by the advanced box score build.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `validate` | `bool` | `False` | when True, score the processed frame with the packaged per-game gate (`sportsdataverse.validation`) and attach its report dict under the `"validation"` key of the processed game (`{}` when the pipeline produced no plays). Name `"validation"` in `return_keys` to get it back when a subset was requested. Off by default -- the gate costs a few milliseconds and most callers do not read it. |

**Returns**

Dict | None: The full processed game dict (or the subset specified by `return_keys` at construction). Returns the partial result when `corrupt_pbp_check()` short-circuits.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"]), len(result["drives"])

# Subset returned keys for downstream serialization

proc = NFLPlayProcess(
    gameId=401671801,
    return_keys=["plays", "advBoxScore", "winprobability"],
)
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())
```

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

### clean_nfl_pbp {#clean_nfl_pbp}

`clean_nfl_pbp(df: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Canonicalize names/ids/teams on a play-by-play frame (nflfastR `clean_pbp` port).

See the module docstring for the full column set added, the
compute-if-absent scope note on `pass`/`rush`, and the lookaround ->
capture-group regex rewrites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | An nflverse-shape (or ESPN/native) play-by-play `polars.DataFrame`. Required columns: `desc`, `epa`, `game_id`, `play_id`, `season`, `posteam`. See the module docstring for the full optional-column-with-default list. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

The input frame with every §6 column added/overwritten (idempotent -- pre-existing values of those columns, except `pass`/`rush`, are dropped and recomputed). A zero-row input yields a zero-row frame carrying the full documented schema rather than raising.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_clean import clean_nfl_pbp

pbp = load_nfl_pbp([2023])
cleaned = clean_nfl_pbp(pbp)
print(cleaned.select("name", "id", "fantasy").head())

# Pandas output

cleaned_pd = clean_nfl_pbp(pbp, return_as_pandas=True)

# Pipeline next step (one line)

import polars as pl
cleaned.filter(pl.col("play") == 1).group_by("passer").len()
```

### shield_nfl_pbp {#shield_nfl_pbp}

`shield_nfl_pbp(game_detail: 'Optional[Dict[str, Any]]' = None, shield_game_id: 'Optional[str]' = None, *, enrich: 'bool' = True, context: 'Optional[Dict[str, Any]]' = None, game_id: 'Optional[str]' = None) -> 'pl.DataFrame'`

Build one NFL game's nflverse-shape play-by-play from Shield, at ANY game phase.

The live entry point: the same parser `build_pbp` runs on the archived
`nfl/raw` finals, plus the four things a game still being played needs — the
in-progress drive's possession, game-outcome columns held null until the feed says
FINAL, a next-snap row from `summary`, and provisional rows flagged (see
`sportsdataverse.nfl.shield_pbp.live`). Safe to poll: pass the payload you
already have via *game_detail* (no network), or a *shield_game_id* to fetch it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_detail` | `Optional[Dict[str, Any]]` | `None` | A Shield `experience/v2/gamedetails` payload (the raw body, or a `{"data": ...}` envelope). Takes precedence over *shield_game_id*, so tests and pollers that already hold a payload never touch the network. |
| `shield_game_id` | `Optional[str]` | `None` | Shield game uuid, fetched via `sportsdataverse.nfl.nfl_game_details_v2` with `include_drive_chart=True, return_parsed=False` when *game_detail* is None. |
| `enrich` | `bool` | `True` | Run `sportsdataverse.nfl.ep_wp.enrich_nfl_pbp` on the result (default True) for the `nfl_model_pbp` EP/EPA/WP/WPA/CP/CPOE columns. Pass False for the base frame only (no model loads). |
| `context` | `Optional[Dict[str, Any]]` | `None` | Game context `{"roof": ..., "spread_line": ..., "total_line": ...}` the Shield feed omits. Unset fields fall back to the nflverse schedule row for this game, then to `live.DEFAULT_CONTEXT` (`outdoors` / 2.5 / 55.5, the same default the ESPN processor uses). |
| `game_id` | `Optional[str]` | `None` | Override the nflverse game_id (computed from the payload when None). |

**Returns**

A polars DataFrame, one row per play (plus, while `summary.phase` is `INGAME`, one current-situation row), carrying the `nfl_model_pbp` columns — the `build_pbp` base frame, the EP/WP enrichment when *enrich* is True, and: | col_name | type | description | |----------|------|-------------| | `live_phase` | `str` | The payload's `summary.phase`: `PREGAME`, `INGAME`, `HALFTIME`, `FINAL` or `FINAL_OVERTIME`. | | `is_play` | `int` | `1` for a real play; `0` for the feed's `GAME_START` / `END_QUARTER` / `END_GAME` markers and the current-situation row. | | `provisional` | `int` | `1` when the feed has not closed the play (`playEndTime` null) and it is in the trailing run of such plays of a non-final game — its text, yardage and stats may still change. Always `0` on a final game. | `home_score` / `away_score` / `result` are null until the game is final. The current-situation row is not inert once *enrich* is True: it is the next state, so it also completes the **previous** play's lead-diff columns (`epa`, `qb_epa`, `wpa`, `vegas_wpa`, the `total_*` running sums). That play is usually still `provisional`, so those values can move on the next poll. A payload Shield has not populated a drive chart for (every scheduled game before kickoff) returns a zero-row frame carrying only the three live columns — check `df.is_empty()` before selecting anything else.

**Example**

```python
import polars as pl
from sportsdataverse.nfl import shield_nfl_pbp

df = shield_nfl_pbp(shield_game_id="a9a8944e-4feb-11f1-abca-2c54536568a9")
df.filter(pl.col("is_play") == 0).select("posteam", "down", "ydstogo", "wp")
```

### shield_to_espn_summary {#shield_to_espn_summary}

`shield_to_espn_summary(game_detail: 'Mapping[str, Any]', idmap_row: 'Mapping[str, Any]', *, parsed: 'Optional[pl.DataFrame]' = None, odds: 'Optional[Mapping[str, Any]]' = None, player_stats: 'Optional[Mapping[str, Any]]' = None, team_stats: 'Optional[Mapping[str, Any]]' = None) -> 'Tuple[Dict[str, Any], List[str]]'`

Project one Shield game (any phase) onto an ESPN-summary-shaped dict.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_detail` | `Mapping[str, Any]` |  | A Shield `experience/v2/gamedetails` payload (raw body or a `{"data": ...}` envelope) -- the same object `sportsdataverse.nfl.shield_pbp.build.shield_nfl_pbp` consumes. |
| `idmap_row` | `Mapping[str, Any]` |  | The game's pre-kickoff id-map row (`sportsdataverse.football.sources.idmap.GAME_SCHEMA`): `espn_event_id`, `home_espn_team_id` and `away_espn_team_id` are required; the optional `home_team` / `away_team` sub-dicts supply the era-correct `espn_abbr`. |
| `parsed` | `Optional[DataFrame]` | `None` | The frame `shield_nfl_pbp(game_detail, enrich=False)` already produced. Built here when None -- pass it to parse the payload once for both projections. |
| `odds` | `Optional[Mapping[str, Any]]` | `None` | `{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}` (the stored closing line, `sportsdataverse.football.sources.idmap._odds_override_from_row`). Becomes the summary's one-provider `pickcenter`. |
| `player_stats` | `Optional[Mapping[str, Any]]` | `None` | A Shield `/football/v2/stats/live/player-statistics/{gameId}` body. Becomes `boxscore.players` in ESPN's exact shape (ten categories, athletes carrying ESPN ids from the players crosswalk). Omitted -> the box stays empty and no ESPN athlete id is attached to any play. |
| `team_stats` | `Optional[Mapping[str, Any]]` | `None` | A Shield `/football/v2/stats/live/team-statistics/{gameId}` body. Becomes `boxscore.teams` -- the authoritative countable team totals `NFLPlayProcess.create_box_score` prefers over its play-by-play derivation. |

**Returns**

`(summary, notes)`. | item | type | description | |---|---|---| | summary | dict | An ESPN-summary-shaped payload: `header` (season/week/competitions/competitors/status), `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter`, `boxscore` (filled when `player_stats`/`team_stats` are given) and passthrough arrays. Feed it to `espn_nfl_pbp(summary=)`. | | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a missing `summary.timeouts` block, a missing `summary.homeTeam`/`awayTeam` team id, a PAT with no touchdown to fold into, plays outside the drive chart, and (pre-2014) play ids that do not join ESPN's own. |

**Example**

```python
import json
from sportsdataverse.nfl import NFLPlayProcess, shield_to_espn_summary

# any Shield gamedetails body -- here the copy nfl-raw keeps
with open("nfl/raw/2025/2025_07_LA_JAX.json") as fh:
    game = json.load(fh)
row = {"espn_event_id": "401772635", "home_espn_team_id": "30", "away_espn_team_id": "14"}
summary, notes = shield_to_espn_summary(game, row)
proc = NFLPlayProcess(gameId=401772635, join_participants=False)
proc.espn_nfl_pbp(summary=summary)
result = proc.run_processing_pipeline()
```

### team_name_fn {#team_name_fn}

`team_name_fn(expr: 'pl.Expr') -> 'pl.Expr'`

Fold historical/relocated team codes onto their current abbreviation.

Verbatim port of nflfastR's `team_name_fn` (a plain
`stringr::str_replace_all` over a 10-entry named vector). Operates as a
**substring** replace (not a full-value lookup) so it also fixes
embedded codes like `"SD 49" -> "LAC 49"` on yard-line columns. The
10 from-codes are disjoint from all of their to-values, so the order of
the 10 sequential replacements does not matter (verified in
`tests.nfl.test_nfl_clean`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `expr` | `Expr` |  | A `polars.Expr` over a Utf8 column (e.g. `pl.col("posteam")`). |

**Returns**

The same expression with every occurrence of the 10 historical codes replaced by their current-franchise code.
