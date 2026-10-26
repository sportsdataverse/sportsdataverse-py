---
title: "NFL — additional Python functions — Play-by-play processing: NFLPlayProcess–build_nfl"
sidebar_label: "Play-by-play processing: NFLPlayProcess–build_nfl"
sidebar_position: 11
description: "NFL — additional Python functions — Play-by-play processing: NFLPlayProcess–build_nfl — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Play-by-play processing: NFLPlayProcess–build_nfl

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
`__add_spread_time` step but stops short of EPA / WPA / QBR /
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

| col_name | type | description |
|---|---|---|
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | String | Full name of player |
| `player_display_name` | String | Full name of the player |
| `position` | String | Primary position as reported by NFL.com |
| `position_group` | String | Postion group of player as listed by NFL |
| `headshot_url` | String | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | String | Most recent team player appears in `pbp` with. |
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | Int64 | Season week. |
| `season_type` | String | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | String |  |
| `completions` | Int64 | The number of completed passes. |
| `attempts` | Int64 | The number of pass attempts as defined by the NFL. |
| `passing_yards` | Float64 | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | Int64 | The number of passing touchdowns. |
| `interceptions` | Int64 | The number of interceptions thrown. |
| `sacks` | Int64 | The Number of times sacked. |
| `sack_yards` | Float64 | Yards lost on sack plays. |
| `sack_fumbles` | Int64 | The number of sacks with a fumble. |
| `sack_fumbles_lost` | Int64 | The number of sacks with a lost fumble. |
| `passing_air_yards` | Float64 | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | Float64 | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | Int64 | First downs on pass attempts. |
| `passing_epa` | Float64 | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | Int64 | Two-point conversion passes. |
| `pacr` | Float64 | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | Float64 | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | Int64 | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | Float64 | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | Int64 | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | Int64 | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | Int64 | The number of rushes with a lost fumble. |
| `rushing_first_downs` | Int64 | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | Float64 | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | Int64 | Two-point conversion rushes |
| `receptions` | Int64 | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | Int64 | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | Float64 | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | Int64 | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | Int64 | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | Int64 | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | Float64 | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | Float64 | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | Int64 | Total number of first downs gained on receptions |
| `receiving_epa` | Float64 | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | Int64 | Two-point conversion receptions |
| `racr` | Float64 | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | Float64 | "Player's share of team receiving targets in this game" |
| `air_yards_share` | Float64 | Player's share of the team's air yards in this game |
| `wopr` | Float64 | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | Int64 | Total number of kick/punt return touchdowns |
| `fantasy_points` | Float64 | Standard fantasy points. |
| `fantasy_points_ppr` | Float64 | PPR fantasy points. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `summary_level` is not `"week"`/`"season"` or `season_type` is not `"REG"`/`"POST"`/`"REG+POST"`. |

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

| col_name | type | description |
|---|---|---|
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | String | Full name of player |
| `player_display_name` | String | Full name of the player |
| `games` | Int64 | Games played in career |
| `position` | String | Primary position as reported by NFL.com |
| `position_group` | String | Postion group of player as listed by NFL |
| `headshot_url` | String | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `def_tackles` | Float64 | Total number of tackles for this player |
| `def_tackles_solo` | Float64 | Total number of solo tackles for this player |
| `def_tackles_with_assist` | Float64 | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | Float64 | Number of assisted tackles for this player |
| `def_tackles_for_loss` | Float64 | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | Float64 | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | Float64 | Number of times a fumble was forced from this player |
| `def_sacks` | Float64 | Number of sacks form this player |
| `def_sack_yards` | Float64 | Yards lost from sacks forced by this player |
| `def_qb_hits` | Float64 | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | Float64 | Number of interceptions forced by this player |
| `def_interception_yards` | Float64 | yards gained/lost by interception returns from this player |
| `def_pass_defended` | Float64 | Number of passes defended/broken up by this player |
| `def_tds` | Float64 | Number of defensive touchdowns scored by this player |
| `def_fumbles` | Float64 | Number of fumbles by this player |
| `def_fumble_recovery_own` | Float64 | Number of times a player's team fumbled the ball and this player recovered |
| `def_fumble_recovery_yards_own` | Float64 | Number of yards gained/lost from fumble recoveries that happened because the player's team fumbled the ball and this player recovered the fumble on that same play |
| `def_fumble_recovery_opp` | Float64 | Number of times a player's opponent fumbled the ball and this player recovered |
| `def_fumble_recovery_yards_opp` | Float64 | Number of yards gained/lost from fumble recoveries that happened because the player's opponent fumbled the ball and this player recovered the fumble on that same play |
| `def_safety` | Float64 | Number of times this player forced a defensive safety |
| `def_penalty` | Float64 | Number of times this player was penalized defensively |
| `def_penalty_yards` | Float64 | Number of penalty yards for this player defensively |

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

| col_name | type | description |
|---|---|---|
| `player_id` | String | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `player_name` | String | Full name of player |
| `player_display_name` | String | Full name of the player |
| `games` | Int64 | Games played in career |
| `position` | String | Primary position as reported by NFL.com |
| `position_group` | String | Postion group of player as listed by NFL |
| `headshot_url` | String | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `fg_made` | Int64 |  |
| `fg_att` | Int64 |  |
| `fg_missed` | Int64 |  |
| `fg_blocked` | Int64 |  |
| `fg_long` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg_made_0_19` | Int64 |  |
| `fg_made_20_29` | Int64 |  |
| `fg_made_30_39` | Int64 |  |
| `fg_made_40_49` | Int64 |  |
| `fg_made_50_59` | Int64 |  |
| `fg_made_60_` | Int64 |  |
| `fg_missed_0_19` | Int64 |  |
| `fg_missed_20_29` | Int64 |  |
| `fg_missed_30_39` | Int64 |  |
| `fg_missed_40_49` | Int64 |  |
| `fg_missed_50_59` | Int64 |  |
| `fg_missed_60_` | Int64 |  |
| `fg_made_list` | String |  |
| `fg_missed_list` | String |  |
| `fg_blocked_list` | String |  |
| `fg_made_distance` | Int64 |  |
| `fg_missed_distance` | Int64 |  |
| `fg_blocked_distance` | Int64 |  |
| `pat_made` | Int64 |  |
| `pat_att` | Int64 |  |
| `pat_missed` | Int64 |  |
| `pat_blocked` | Int64 |  |
| `pat_pct` | Float64 |  |
| `gwfg_att` | Int64 |  |
| `gwfg_distance_list` | String |  |
| `gwfg_made` | Int64 |  |
| `gwfg_missed` | Int64 |  |
| `gwfg_blocked` | Int64 |  |

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

| col_name | type | description |
|---|---|---|
| `season` | Int64 | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | String | NFL team. Uses official abbreviations as per NFL.com |
| `position` | String | Primary position as reported by NFL.com |
| `depth_chart_position` | String | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | Int64 | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | String |  |
| `full_name` | String | Full name as per NFL.com |
| `first_name` | String | First name of player |
| `last_name` | String | Last name of player |
| `birth_date` | String | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | Float64 | Official height, in inches |
| `weight` | Int64 | Official weight, in pounds |
| `college` | String | Official college (usually the last one attended) |
| `gsis_id` | String | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | String | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | String | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | String | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | String | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | String | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | String | Pro-Football-Reference ID for player |
| `fantasy_data_id` | String | FantasyData ID - usual format five digit integer |
| `sleeper_id` | String | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | Int64 | Years played in league |
| `headshot_url` | String | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `esb_id` | String | Player ID for Elias Sports Bureau |
| `smart_id` | String | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `football_name` | String | Common player name (i.e. in most cases common_first_name last_name) |
| `ngs_position` | String | Primary position as reported by the NextGen stats API. |
| `entry_year` | Int64 | The year a player first became eligible to play in the NFL. |
| `rookie_year` | Int64 | The year a player lost their rookie eligibility. |

**Raises**

| Exception | Description |
|---|---|
| `TypeError` | If `seasons` is not an `int` or an iterable of `int`. |

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
