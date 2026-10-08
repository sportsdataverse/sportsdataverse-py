---
title: "NFL — additional Python functions — Play-by-play processing: NFLPlayProcess–build_nfl"
sidebar_label: "Play-by-play processing: NFLPlayProcess–build_nfl"
sidebar_position: 10
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

| col_name | type | description |
|---|---|---|
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `recent_team` | character | Most recent team player appears in `pbp` with. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character |  |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `interceptions` | integer | The number of interceptions thrown. |
| `sacks` | integer | The Number of times sacked. |
| `sack_yards` | double | Yards lost on sack plays. |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `pacr` | double | Passing (yards) Air (yards) Conversion Ratio - the number of passing yards per air yards thrown per game |
| `dakota` | double | Adjusted EPA + CPOE composite based on coefficients which best predict adjusted EPA/play in the following year. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `racr` | double | Receiving (yards) Air (yards) Conversion Ratio - the number of receiving yards per air yards targeted per game |
| `target_share` | double | "Player's share of team receiving targets in this game" |
| `air_yards_share` | double | Player's share of the team's air yards in this game |
| `wopr` | double | Weighted OPportunity Rating - 1.5 x target_share + 0.7 x air_yards_share - a weighted average that contextualizes total fantasy usage. |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `fantasy_points` | double | Standard fantasy points. |
| `fantasy_points_ppr` | double | PPR fantasy points. |

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
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `games` | integer | Games played in career |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `def_tackles` | double | Total number of tackles for this player |
| `def_tackles_solo` | double | Total number of solo tackles for this player |
| `def_tackles_with_assist` | double | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | double | Number of assisted tackles for this player |
| `def_tackles_for_loss` | double | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | double | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | double | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | double | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | double | Number of interceptions forced by this player |
| `def_interception_yards` | double | yards gained/lost by interception returns from this player |
| `def_pass_defended` | double | Number of passes defended/broken up by this player |
| `def_tds` | double | Number of defensive touchdowns scored by this player |
| `def_fumbles` | double | Number of fumbles by this player |
| `def_fumble_recovery_own` | double | Number of times a player's team fumbled the ball and this player recovered |
| `def_fumble_recovery_yards_own` | double | Number of yards gained/lost from fumble recoveries that happened because the player's team fumbled the ball and this player recovered the fumble on that same play |
| `def_fumble_recovery_opp` | double | Number of times a player's opponent fumbled the ball and this player recovered |
| `def_fumble_recovery_yards_opp` | double | Number of yards gained/lost from fumble recoveries that happened because the player's opponent fumbled the ball and this player recovered the fumble on that same play |
| `def_safety` | double | Number of times this player forced a defensive safety |
| `def_penalty` | double | Number of times this player was penalized defensively |
| `def_penalty_yards` | double | Number of penalty yards for this player defensively |

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
| `player_id` | character | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `player_name` | character | Full name of player |
| `player_display_name` | character | Full name of the player |
| `games` | integer | Games played in career |
| `position` | character | Primary position as reported by NFL.com |
| `position_group` | character | Postion group of player as listed by NFL |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `fg_made` | integer |  |
| `fg_att` | integer |  |
| `fg_missed` | integer |  |
| `fg_blocked` | integer |  |
| `fg_long` | double |  |
| `fg_pct` | double |  |
| `fg_made_0_19` | integer |  |
| `fg_made_20_29` | integer |  |
| `fg_made_30_39` | integer |  |
| `fg_made_40_49` | integer |  |
| `fg_made_50_59` | integer |  |
| `fg_made_60_` | integer |  |
| `fg_missed_0_19` | integer |  |
| `fg_missed_20_29` | integer |  |
| `fg_missed_30_39` | integer |  |
| `fg_missed_40_49` | integer |  |
| `fg_missed_50_59` | integer |  |
| `fg_missed_60_` | integer |  |
| `fg_made_list` | character |  |
| `fg_missed_list` | character |  |
| `fg_blocked_list` | character |  |
| `fg_made_distance` | integer |  |
| `fg_missed_distance` | integer |  |
| `fg_blocked_distance` | integer |  |
| `pat_made` | integer |  |
| `pat_att` | integer |  |
| `pat_missed` | integer |  |
| `pat_blocked` | integer |  |
| `pat_pct` | double |  |
| `gwfg_att` | integer |  |
| `gwfg_distance_list` | character |  |
| `gwfg_made` | integer |  |
| `gwfg_missed` | integer |  |
| `gwfg_blocked` | integer |  |

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
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `position` | character | Primary position as reported by NFL.com |
| `depth_chart_position` | character | Position assigned on depth chart. Not always accurate! |
| `jersey_number` | integer | Jersey number. Often useful for joins by name/team/jersey. |
| `status` | character |  |
| `full_name` | character | Full name as per NFL.com |
| `first_name` | character | First name of player |
| `last_name` | character | Last name of player |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `height` | double | Official height, in inches |
| `weight` | integer | Official weight, in pounds |
| `college` | character | Official college (usually the last one attended) |
| `gsis_id` | character | Game Stats and Info Service ID: the primary ID for play-by-play data. |
| `espn_id` | character | ESPN ID - usual format is an integer with ~5 digits |
| `sportradar_id` | character | SportRadar ID - often also called sportsdata_id by other services. A UUID. |
| `yahoo_id` | character | Yahoo ID - usual format is an integer with ~5 digits |
| `rotowire_id` | character | Rotowire ID - usual format is an integer with ~four digits. Not to be confused with rotowire_id. |
| `pff_id` | character | Pro Football Focus ID - usually an integer with between 3 and 6 digits. |
| `pfr_id` | character | Pro-Football-Reference ID for player |
| `fantasy_data_id` | character | FantasyData ID - usual format five digit integer |
| `sleeper_id` | character | Sleeper ID - usually an integer with ~4 digits. |
| `years_exp` | integer | Years played in league |
| `headshot_url` | character | A URL string that points to player photos used by NFL.com (or sometimes ESPN) |
| `esb_id` | character | Player ID for Elias Sports Bureau |
| `smart_id` | character | SMART ID for player (that's in raw pbp. It includes a hashed ESB_ID) |
| `football_name` | character | Common player name (i.e. in most cases common_first_name last_name) |
| `ngs_position` | character | Primary position as reported by the NextGen stats API. |
| `entry_year` | integer | The year a player first became eligible to play in the NFL. |
| `rookie_year` | integer | The year a player lost their rookie eligibility. |

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

| col_name | type | description |
|---|---|---|
| `game_play_number` | integer |  |
| `id` | integer | ID of the player in the 'name' column. |
| `sequenceNumber` | integer |  |
| `text` | character |  |
| `awayScore` | integer |  |
| `homeScore` | integer |  |
| `scoringPlay` | logical |  |
| `priority` | logical |  |
| `modified` | character |  |
| `wallclock` | character |  |
| `teamParticipants` | integer |  |
| `isPenalty` | logical |  |
| `statYardage` | integer |  |
| `isTurnover` | logical |  |
| `type.id` | character |  |
| `type.text` | character |  |
| `period.number` | integer |  |
| `clock.displayValue` | character |  |
| `start.down` | integer |  |
| `start.distance` | integer |  |
| `start.yardLine` | integer |  |
| `start.yardsToEndzone` | integer |  |
| `start.team.id` | integer |  |
| `end.down` | integer |  |
| `end.distance` | integer |  |
| `end.yardLine` | integer |  |
| `end.yardsToEndzone` | integer |  |
| `end.team.id` | integer |  |
| `type.abbreviation` | character |  |
| `start.downDistanceText` | character |  |
| `start.shortDownDistanceText` | character |  |
| `start.possessionText` | character |  |
| `end.downDistanceText` | character |  |
| `end.shortDownDistanceText` | character |  |
| `end.possessionText` | character |  |
| `scoringType.name` | character |  |
| `scoringType.displayName` | character |  |
| `scoringType.abbreviation` | character |  |
| `pointAfterAttempt.id` | double |  |
| `pointAfterAttempt.text` | character |  |
| `pointAfterAttempt.abbreviation` | character |  |
| `pointAfterAttempt.value` | double |  |
| `drive.id` | character |  |
| `drive.displayResult` | character |  |
| `drive.isScore` | logical |  |
| `drive.team.shortDisplayName` | character |  |
| `drive.team.displayName` | character |  |
| `drive.team.name` | character |  |
| `drive.team.abbreviation` | character |  |
| `drive.yards` | integer |  |
| `drive.offensivePlays` | integer |  |
| `drive.result` | character |  |
| `drive.description` | character |  |
| `drive.shortDisplayResult` | character |  |
| `drive.timeElapsed.displayValue` | character |  |
| `drive.start.period.number` | integer |  |
| `drive.start.period.type` | character |  |
| `drive.start.yardLine` | integer |  |
| `drive.start.clock.displayValue` | character |  |
| `drive.start.text` | character |  |
| `drive.end.period.number` | integer |  |
| `drive.end.period.type` | character |  |
| `drive.end.yardLine` | integer |  |
| `drive.end.clock.displayValue` | character |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | integer |  |
| `week` | integer | Season week. |
| `status_type_completed` | logical |  |
| `homeTeamId` | integer |  |
| `awayTeamId` | integer |  |
| `homeTeamName` | character |  |
| `awayTeamName` | character |  |
| `homeTeamMascot` | character |  |
| `awayTeamMascot` | character |  |
| `homeTeamAbbrev` | character |  |
| `awayTeamAbbrev` | character |  |
| `homeTeamNameAlt` | character |  |
| `awayTeamNameAlt` | character |  |
| `gameSpread` | double |  |
| `homeFavorite` | logical |  |
| `gameSpreadAvailable` | logical |  |
| `overUnder` | double |  |
| `roof` | character | One of 'dome', 'outdoors', 'closed', 'open' indicating indicating the roof status of the stadium the game was played in. (Source: Pro-Football-Reference) |
| `homeTeamSpread` | double |  |
| `clock.minutes` | integer |  |
| `clock.seconds` | integer |  |
| `half` | integer |  |
| `lag_half` | integer |  |
| `lead_half` | integer |  |
| `start.TimeSecsRem` | integer |  |
| `start.adj_TimeSecsRem` | integer |  |
| `orig_play_type` | character |  |
| `lead_text` | character |  |
| `lead_start_team` | character |  |
| `lead_start_yardsToEndzone` | integer |  |
| `lead_start_down` | integer |  |
| `lead_start_distance` | integer |  |
| `lead_scoringPlay` | logical |  |
| `text_dupe` | logical |  |
| `start.pos_team.id` | integer |  |
| `start.def_pos_team.id` | integer |  |
| `end.def_pos_team.id` | integer |  |
| `end.pos_team.id` | integer |  |
| `start.pos_team.name` | character |  |
| `start.def_pos_team.name` | character |  |
| `end.pos_team.name` | character |  |
| `end.def_pos_team.name` | character |  |
| `start.is_home` | logical |  |
| `end.is_home` | logical |  |
| `homeTimeoutCalled` | logical |  |
| `awayTimeoutCalled` | logical |  |
| `end.homeTeamTimeouts` | integer |  |
| `end.awayTeamTimeouts` | integer |  |
| `start.homeTeamTimeouts` | integer |  |
| `start.awayTeamTimeouts` | integer |  |
| `end.TimeSecsRem` | integer |  |
| `end.adj_TimeSecsRem` | integer |  |
| `start.posTeamTimeouts` | integer |  |
| `start.defPosTeamTimeouts` | integer |  |
| `end.posTeamTimeouts` | integer |  |
| `end.defPosTeamTimeouts` | integer |  |
| `firstHalfKickoffTeamId` | integer |  |
| `period` | integer |  |
| `start.yard` | integer |  |
| `end.yard` | integer |  |
| `lag_scoringPlay` | logical |  |
| `end_of_half` | logical |  |
| `down_1` | logical |  |
| `down_2` | logical |  |
| `down_3` | logical |  |
| `down_4` | logical |  |
| `down_1_end` | logical |  |
| `down_2_end` | logical |  |
| `down_3_end` | logical |  |
| `down_4_end` | logical |  |
| `scoring_play` | logical |  |
| `td_play` | logical |  |
| `touchdown` | logical | Binary indicator for if the play resulted in a TD. |
| `td_check` | logical |  |
| `safety` | logical | Binary indicator for whether or not a safety occurred. |
| `fumble_vec` | logical |  |
| `forced_fumble` | logical |  |
| `kickoff_play` | logical |  |
| `kickoff_tb` | logical |  |
| `kickoff_onside` | logical |  |
| `kickoff_oob` | logical |  |
| `kickoff_fair_catch` | logical | Binary indicator for if the kickoff was caught with a fair catch. |
| `kickoff_downed` | logical | Binary indicator for if the kickoff was downed. |
| `kick_play` | logical |  |
| `kickoff_safety` | logical |  |
| `punt` | logical |  |
| `punt_play` | logical |  |
| `punt_tb` | logical |  |
| `punt_oob` | logical |  |
| `punt_fair_catch` | logical | Binary indicator for if the punt was caught with a fair catch. |
| `punt_downed` | logical | Binary indicator for if the punt was downed. |
| `punt_safety` | logical |  |
| `punt_blocked` | logical | Binary indicator for if the punt was blocked. |
| `penalty_safety` | logical |  |
| `rush` | logical | Binary indicator if the play was a rushing play. |
| `pass` | logical | Binary indicator if the play was a pass play (sacks and scrambles included). |
| `sack_vec` | logical |  |
| `pos_team` | integer |  |
| `def_pos_team` | integer |  |
| `is_home` | logical |  |
| `lag_HA_score_diff` | integer |  |
| `HA_score_diff` | integer |  |
| `net_HA_score_pts` | integer |  |
| `H_score_diff` | integer |  |
| `A_score_diff` | integer |  |
| `lag_homeScore` | integer |  |
| `lag_awayScore` | integer |  |
| `start.homeScore` | integer |  |
| `start.awayScore` | integer |  |
| `end.homeScore` | integer |  |
| `end.awayScore` | integer |  |
| `pos_team_score` | integer |  |
| `def_pos_team_score` | integer |  |
| `start.pos_team_score` | integer |  |
| `start.def_pos_team_score` | integer |  |
| `start.pos_score_diff` | integer |  |
| `end.pos_team_score` | integer |  |
| `end.def_pos_team_score` | integer |  |
| `end.pos_score_diff` | integer |  |
| `lag_pos_team` | integer |  |
| `lead_pos_team` | integer |  |
| `lead_pos_team2` | integer |  |
| `pos_score_diff` | integer |  |
| `lag_pos_score_diff` | integer |  |
| `pos_score_pts` | integer |  |
| `pos_score_diff_start` | integer |  |
| `start.pos_team_receives_2H_kickoff` | logical |  |
| `end.pos_team_receives_2H_kickoff` | logical |  |
| `change_of_poss` | logical |  |
| `penalty_flag` | logical |  |
| `penalty_declined` | logical |  |
| `penalty_no_play` | logical |  |
| `penalty_offset` | logical |  |
| `penalty_1st_conv` | logical |  |
| `penalty_in_text` | logical |  |
| `penalty_detail` | character |  |
| `penalty_text` | character |  |
| `yds_penalty` | character |  |
| `penalty_count` | integer |  |
| `penalty_declined_count` | integer |  |
| `penalty_all_declined` | logical |  |
| `penalty_enforcement` | character |  |
| `penalty_negated_play` | logical |  |
| `sack` | logical | Binary indicator for if the play ended in a sack. |
| `int` | logical |  |
| `int_td` | logical |  |
| `completion` | logical |  |
| `pass_attempt` | logical | Binary indicator for if the play was a pass attempt (includes sacks). |
| `target` | logical |  |
| `pass_breakup` | logical |  |
| `pass_td` | logical |  |
| `rush_td` | logical |  |
| `pass_depth` | character |  |
| `pass_direction` | character |  |
| `rush_direction` | character |  |
| `turnover_vec` | logical |  |
| `offense_score_play` | logical |  |
| `defense_score_play` | logical |  |
| `downs_turnover` | logical |  |
| `yds_punted` | integer |  |
| `yds_punt_gained` | integer |  |
| `fg_attempt` | logical |  |
| `fg_made` | logical |  |
| `yds_fg` | integer |  |
| `pos_unit` | character |  |
| `def_pos_unit` | character |  |
| `lead_play_type` | character |  |
| `sp` | logical | Binary indicator for whether or not a score occurred on the play. |
| `play` | logical | Binary indicator: 1 if the play was a 'normal' play (including penalties), 0 otherwise. |
| `scrimmage_play` | logical |  |
| `change_of_pos_team` | logical |  |
| `pos_score_diff_end` | integer |  |
| `fumble_lost` | logical | Binary indicator for if the fumble was lost. |
| `fumble_recovered` | logical |  |
| `field_goal_result` | character | String indicator for result of field goal attempt: made, missed, or blocked. |
| `extra_point_result` | character | String indicator for the result of the extra point attempt: good, failed, blocked, safety (touchback in defensive endzone is 1 point apparently), or aborted. |
| `two_point_conv_result` | character | String indicator for result of two point conversion attempt: success, failure, safety (touchback in defensive endzone is 1 point apparently), or return. |
| `kneel_down` | logical |  |
| `qb_hurry` | logical |  |
| `xp_attempt` | logical |  |
| `xp_made` | logical |  |
| `two_point_attempt` | logical | Binary indicator for two point conversion attempt. |
| `defensive_two_point_attempt` | logical | Binary indicator whether or not the defense was able to have an attempt on a two point conversion, this results following a turnover. |
| `defensive_two_point_conv` | logical | Binary indicator whether or not the defense successfully scored on the two point conversion. |
| `two_point_pass` | logical |  |
| `two_point_rush` | logical |  |
| `yds_rushed` | integer |  |
| `yds_receiving` | integer |  |
| `yds_int_return` | character |  |
| `yds_kickoff` | integer |  |
| `yds_kickoff_return` | integer |  |
| `yds_punt_return` | integer |  |
| `yds_fumble_return` | character |  |
| `yds_sacked` | integer |  |
| `sack_players` | character |  |
| `xp_kicker_player_name` | character |  |
| `passer_player_name` | character | String name for the player that attempted the pass. |
| `rusher_player_name` | character | String name for the player that attempted the run. |
| `receiver_player_name` | character | String name for the targeted receiver. |
| `sack_player_name` | character | String name of the player who recorded a solo sack. |
| `sack_player_name2` | character |  |
| `pass_breakup_player_name` | character |  |
| `interception_player_name` | character | String name for the player that intercepted the pass. |
| `fg_kicker_player_name` | character |  |
| `fg_block_player_name` | character |  |
| `fg_return_player_name` | character |  |
| `kickoff_player_name` | character |  |
| `kickoff_return_player_name` | character |  |
| `punter_player_name` | character | String name for the punter. |
| `punt_block_player_name` | character |  |
| `punt_return_player_name` | character |  |
| `punt_block_return_player_name` | character |  |
| `fumble_player_name` | character |  |
| `fumble_forced_player_name` | character |  |
| `fumble_recovered_player_name` | character |  |
| `kicking_team` | integer |  |
| `return_team` | integer | String abbreviation of the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `fumble_or_muff` | logical |  |
| `recovery_team` | character |  |
| `recovery_team_2` | character |  |
| `penalty_spot_yardline` | integer |  |
| `penalty_spot_side` | character |  |
| `penalty_spot_yardsToEndzone` | integer |  |
| `fumbling_team` | character |  |
| `int_turnover` | logical |  |
| `pos_fumble_lost` | logical |  |
| `def_fumble_lost` | logical |  |
| `is_pos_team_turnover` | logical |  |
| `is_def_pos_team_turnover` | logical |  |
| `is_turnover` | logical |  |
| `turnover_team` | character |  |
| `is_st_turnover` | logical |  |
| `is_blocked_punt_turnover` | logical |  |
| `is_blocked_fg_turnover` | logical |  |
| `sack_team` | integer |  |
| `interception_team` | integer |  |
| `pass_breakup_team` | integer |  |
| `forced_fumble_team` | integer |  |
| `fumble_recovery_team` | character |  |
| `punt_return_team` | integer |  |
| `kick_return_team` | integer |  |
| `fg_team` | integer |  |
| `punt_team` | integer |  |
| `penalized_team` | integer |  |
| `penalty_yards_signed` | integer |  |
| `penalty_side` | character |  |
| `penalty_yards_net` | integer |  |
| `penalty_team_id` | integer |  |
| `lateral_player_name` | character |  |
| `yds_lateral` | character |  |
| `yards_after_catch` | character | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `air_yards` | character | Numeric value for distance in yards perpendicular to the line of scrimmage at where the targeted receiver either caught or didn't catch the ball. |
| `air_yardsToEndzone` | character |  |
| `new_down` | integer |  |
| `new_distance` | integer |  |
| `middle_8` | logical |  |
| `rz_play` | logical |  |
| `under_2` | logical |  |
| `goal_to_go` | logical | Binary indicator for whether or not the posteam is in a goal down situation. |
| `scoring_opp` | logical |  |
| `stuffed_run` | logical |  |
| `stopped_run` | logical |  |
| `opportunity_run` | logical |  |
| `highlight_run` | logical |  |
| `adj_rush_yardage` | integer |  |
| `line_yards` | double |  |
| `second_level_yards` | double |  |
| `open_field_yards` | integer |  |
| `highlight_yards` | double |  |
| `opp_highlight_yards` | double |  |
| `short_rush_success` | logical |  |
| `short_rush_attempt` | logical |  |
| `power_rush_success` | logical |  |
| `power_rush_attempt` | logical |  |
| `early_down` | logical |  |
| `late_down` | logical |  |
| `early_down_pass` | logical |  |
| `early_down_rush` | logical |  |
| `late_down_pass` | logical |  |
| `late_down_rush` | logical |  |
| `standard_down` | logical |  |
| `passing_down` | logical |  |
| `TFL` | logical |  |
| `TFL_pass` | logical |  |
| `TFL_rush` | logical |  |
| `havoc` | logical |  |
| `first_down_yards` | logical |  |
| `first_down_penalty` | logical | Binary indicator for if a penalty converted the first down. |
| `first_down_earned` | logical |  |
| `new_series` | logical |  |
| `firstD_by_kickoff` | logical |  |
| `firstD_by_poss` | logical |  |
| `firstD_by_penalty` | logical |  |
| `firstD_by_yards` | logical |  |
| `start.pos_team_spread` | double |  |
| `start.elapsed_share` | double |  |
| `start.spread_time` | double |  |
| `end.pos_team_spread` | double |  |
| `end.elapsed_share` | double |  |
| `end.spread_time` | double |  |
| `pass_length` | character | String indicator for pass length: short or deep. |
| `pass_location` | character | String indicator for pass location: left, middle, or right. |
| `shotgun` | integer | Binary indicator for whether or not the play was in shotgun formation. |
| `no_huddle` | integer | Binary indicator for whether or not the play was in no_huddle formation. |
| `pass_middle` | integer |  |
| `down` | integer | The down for the given play. |
| `distance` | integer |  |
| `start.yardsToEndzone.touchback` | integer |  |
| `penalty_assessed_on_kickoff` | logical |  |
| `EP_start_touchback` | double |  |
| `EP_start` | double |  |
| `EP_end` | double |  |
| `EP_penalty_cf` | character |  |
| `penalty_cf_yardsToEndzone` | character |  |
| `lag_EP_end` | double |  |
| `lag_change_of_pos_team` | logical |  |
| `EP_between` | double |  |
| `EPA` | double |  |
| `def_EPA` | double |  |
| `EPA_scrimmage` | double |  |
| `EPA_rush` | double |  |
| `EPA_pass` | double |  |
| `EPA_explosive` | logical |  |
| `EPA_non_explosive` | double |  |
| `EPA_explosive_pass` | logical |  |
| `EPA_explosive_rush` | logical |  |
| `first_down_created` | logical |  |
| `EPA_success` | logical |  |
| `EPA_success_early_down` | logical |  |
| `EPA_success_early_down_pass` | logical |  |
| `EPA_success_early_down_rush` | logical |  |
| `EPA_success_late_down` | logical |  |
| `EPA_success_late_down_pass` | logical |  |
| `EPA_success_late_down_rush` | logical |  |
| `EPA_success_standard_down` | logical |  |
| `EPA_success_passing_down` | logical |  |
| `EPA_success_pass` | logical |  |
| `EPA_success_rush` | logical |  |
| `EPA_success_EPA` | double |  |
| `EPA_success_standard_down_EPA` | double |  |
| `EPA_success_passing_down_EPA` | double |  |
| `EPA_success_pass_EPA` | double |  |
| `EPA_success_rush_EPA` | double |  |
| `EPA_middle_8_success` | logical |  |
| `EPA_middle_8_success_pass` | logical |  |
| `EPA_middle_8_success_rush` | logical |  |
| `EPA_penalty` | double |  |
| `EPA_penalty_direct` | double |  |
| `EPA_sp` | double |  |
| `EPA_fg` | double |  |
| `EPA_punt` | double |  |
| `EPA_kickoff` | double |  |
| `qb_epa` | double | Gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `start.ExpScoreDiff_touchback` | double |  |
| `start.ExpScoreDiff` | double |  |
| `start.ExpScoreDiff_Time_Ratio_touchback` | double |  |
| `start.ExpScoreDiff_Time_Ratio` | double |  |
| `end.ExpScoreDiff` | double |  |
| `end.ExpScoreDiff_Time_Ratio` | double |  |
| `wp_before` | double |  |
| `wp_touchback` | double |  |
| `wp_after` | double |  |
| `def_wp_before` | double |  |
| `home_wp_before` | double |  |
| `away_wp_before` | double |  |
| `lead_wp_before` | double |  |
| `lead_wp_before2` | double |  |
| `def_wp_after` | double |  |
| `home_wp_after` | double |  |
| `away_wp_after` | double |  |
| `wpa` | double | Win probability added (WPA) for the posteam. |
| `wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play. |
| `vegas_wp` | double | Estimated win probability for the posteam given the current situation at the start of the given play, incorporating pre-game Vegas line. |
| `def_wp` | double | Estimated win probability for the defteam. |
| `home_wp` | double | Estimated win probability for the home team. |
| `away_wp` | double | Estimated win probability for the away team. |
| `wp_before_naive` | double |  |
| `wp_after_naive` | double |  |
| `wpa_naive` | double |  |
| `def_wp_before_naive` | double |  |
| `def_wp_after_naive` | double |  |
| `home_wp_before_naive` | double |  |
| `home_wp_after_naive` | double |  |
| `lead_wp_before_naive` | double |  |
| `lead_wp_before2_naive` | double |  |
| `wp_touchback_naive` | double |  |
| `away_wp_before_naive` | double |  |
| `away_wp_after_naive` | double |  |
| `cp` | character | Numeric value indicating the probability for a complete pass based on comparable game situations. |
| `cpoe` | character | For a single pass play this is 1 - cp when the pass was completed or 0 - cp when the pass was incomplete. Analyzed for a whole game or season an indicator for the passer how much over or under expectation his completion percentage was. |
| `xpass` | double | Probability of dropback scaled from 0 to 1. |
| `pass_oe` | double | Dropback percent over expected on a given play scaled from 0 to 100. |
| `xyac_epa` | character | Expected value of EPA gained after the catch, starting from where the catch was made. Zero yards after the catch would be listed as zero EPA. |
| `xyac_mean_yardage` | character | Average expected yards after the catch based on where the ball was caught. |
| `xyac_median_yardage` | character | Median expected yards after the catch based on where the ball was caught. |
| `xyac_success` | character | Probability play earns positive EPA (relative to where play started) based on where ball was caught. |
| `xyac_fd` | character | Probability play earns a first down based on where the ball was caught. |
| `drive_start` | double |  |
| `drive_stopped` | logical |  |
| `drive_play_index` | integer |  |
| `drive_offense_plays` | integer |  |
| `prog_drive_EPA` | double |  |
| `prog_drive_WPA` | double |  |
| `drive_offense_yards` | integer |  |
| `drive_total_yards` | integer |  |
| `fixed_drive` | integer | Manually created drive number in a game. |
| `fixed_drive_result` | character | Manually created drive result. |
| `series` | integer | Starts at 1, each new first down increments, numbers shared across both teams NA: kickoffs, extra point/two point conversion attempts, non-plays, no posteam |
| `series_result` | character | Possible values: First down, Touchdown, Opp touchdown, Field goal, Missed field goal, Safety, Turnover, Punt, Turnover on downs, QB kneel, End of half |
| `series_success` | integer | 1: scored touchdown, gained enough yards for first down. |
| `go_wp` | double |  |
| `first_down_prob` | double |  |
| `wp_succeed` | double |  |
| `wp_fail` | double |  |
| `fg_make_prob` | double |  |
| `make_fg_wp` | double |  |
| `miss_fg_wp` | double |  |
| `fg_wp` | double |  |
| `punt_wp` | double |  |
| `go_boost` | double |  |
| `go_wp_diff` | double |  |
| `punt_wp_diff` | double |  |
| `fg_wp_diff` | double |  |
| `fourth_down_recommendation` | character |  |
| `two_pt_wp` | double |  |
| `xp_wp` | double |  |
| `prob_2pt` | double |  |
| `two_pt_wp_diff` | double |  |
| `two_pt_recommendation` | character |  |
| `qbr_epa` | double |  |
| `weight` | double | Official weight, in pounds |
| `non_fumble_sack` | logical |  |
| `sack_epa` | double |  |
| `pass_epa` | double |  |
| `rush_epa` | double |  |
| `pen_epa` | double |  |
| `sack_weight` | double |  |
| `pass_weight` | double |  |
| `rush_weight` | double |  |
| `pen_weight` | double |  |
| `action_play` | logical |  |
| `athlete_name` | character |  |
| `sack_player_id2` | character |  |
| `passer_player_id` | character | Unique identifier for the player that attempted the pass. |
| `rusher_player_id` | character | Unique identifier for the player that attempted the run. |
| `receiver_player_id` | character | Unique identifier for the receiver that was targeted on the pass. |
| `punter_player_id` | character | Unique identifier for the punter. |
| `fg_kicker_player_id` | character |  |
| `sack_player_id` | character | Unique identifier of the player who recorded a solo sack. |
| `punt_return_player_id` | character |  |
| `kickoff_return_player_id` | character |  |
| `interception_player_id` | character | Unique identifier for the player that intercepted the pass. |
| `pass_breakup_player_id` | character |  |
| `fumble_forced_player_id` | character |  |
| `fumble_recovered_player_id` | character |  |
| `fumble_player_id` | character |  |
| `punt_block_player_id` | character |  |
| `punt_block_return_player_id` | character |  |
| `kickoff_player_id` | character |  |
| `fg_block_player_id` | character |  |
| `fg_return_player_id` | character |  |
| `xp_kicker_player_id` | character |  |

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

| col_name | type | description |
|---|---|---|
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `week` | integer | Season week. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `season_type` | character | REG or POST indicating if the timeframe belongs to regular or post season. |
| `opponent_team` | character |  |
| `completions` | integer | The number of completed passes. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `passing_yards` | double | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_tds` | integer | The number of passing touchdowns. |
| `passing_interceptions` | integer |  |
| `sacks_suffered` | integer |  |
| `sack_yards_lost` | double |  |
| `sack_fumbles` | integer | The number of sacks with a fumble. |
| `sack_fumbles_lost` | integer | The number of sacks with a lost fumble. |
| `passing_air_yards` | double | Passing air yards (includes incomplete passes). |
| `passing_yards_after_catch` | double | Yards after the catch gained on plays in which player was the passer (this is an unofficial stat and may differ slightly between different sources). |
| `passing_first_downs` | integer | First downs on pass attempts. |
| `passing_epa` | double | Total expected points added on pass attempts and sacks. NOTE: this uses the variable `qb_epa`, which gives QB credit for EPA for up to the point where a receiver lost a fumble after a completed catch and makes EPA work more like passing yards on plays with fumbles. |
| `passing_cpoe` | double |  |
| `passing_2pt_conversions` | integer | Two-point conversion passes. |
| `carries` | integer | The number of official rush attempts (incl. scrambles and kneel downs). Rushes after a lateral reception don't count as carry. |
| `rushing_yards` | double | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_tds` | integer | The number of rushing touchdowns (incl. scrambles). Also includes touchdowns after obtaining a lateral on a play that started with a rushing attempt. |
| `rushing_fumbles` | integer | The number of rushes with a fumble. |
| `rushing_fumbles_lost` | integer | The number of rushes with a lost fumble. |
| `rushing_first_downs` | integer | First downs on rush attempts (incl. scrambles). |
| `rushing_epa` | double | Expected points added on rush attempts (incl. scrambles and kneel downs). |
| `rushing_2pt_conversions` | integer | Two-point conversion rushes |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `receiving_yards` | double | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_tds` | integer | The number of touchdowns following a pass reception. Also includes touchdowns after receiving a lateral on a play that started as a pass play. |
| `receiving_fumbles` | integer | The number of fumbles after a pass reception. |
| `receiving_fumbles_lost` | integer | The number of fumbles lost after a pass reception. |
| `receiving_air_yards` | double | Receiving air yards (incl. incomplete passes). |
| `receiving_yards_after_catch` | double | Yards after the catch gained on plays in which player was receiver (this is an unofficial stat and may differ slightly between different sources). |
| `receiving_first_downs` | integer | Total number of first downs gained on receptions |
| `receiving_epa` | double | Total EPA on plays where this receiver was targeted |
| `receiving_2pt_conversions` | integer | Two-point conversion receptions |
| `special_teams_tds` | integer | Total number of kick/punt return touchdowns |
| `def_tackles_solo` | integer | Total number of solo tackles for this player |
| `def_tackles_with_assist` | integer | Number of tackles this player had with an assisted tackle |
| `def_tackle_assists` | integer | Number of assisted tackles for this player |
| `def_tackles_for_loss` | integer | Number of tackles for loss (TFL) for this player |
| `def_tackles_for_loss_yards` | integer | Yards lost from TFLs involving this player |
| `def_fumbles_forced` | integer | Number of times a fumble was forced from this player |
| `def_sacks` | double | Number of sacks form this player |
| `def_sack_yards` | double | Yards lost from sacks forced by this player |
| `def_qb_hits` | integer | Number of QB hits from this player (should not include plays where the QB was sacked) |
| `def_interceptions` | integer | Number of interceptions forced by this player |
| `def_interception_yards` | double | yards gained/lost by interception returns from this player |
| `def_pass_defended` | integer | Number of passes defended/broken up by this player |
| `def_tds` | integer | Number of defensive touchdowns scored by this player |
| `def_fumbles` | integer | Number of fumbles by this player |
| `def_safeties` | integer |  |
| `misc_yards` | integer |  |
| `fumble_recovery_own` | integer |  |
| `fumble_recovery_yards_own` | integer |  |
| `fumble_recovery_opp` | integer |  |
| `fumble_recovery_yards_opp` | integer |  |
| `fumble_recovery_tds` | integer |  |
| `penalties` | integer |  |
| `penalty_yards` | integer | Yards gained (or lost) by the posteam from the penalty. |
| `timeouts` | integer |  |
| `punt_returns` | integer |  |
| `punt_return_yards` | integer |  |
| `kickoff_returns` | integer |  |
| `kickoff_return_yards` | integer |  |
| `fg_made` | integer |  |
| `fg_att` | integer |  |
| `fg_missed` | integer |  |
| `fg_blocked` | integer |  |
| `fg_long` | double |  |
| `fg_pct` | double |  |
| `fg_made_0_19` | integer |  |
| `fg_made_20_29` | integer |  |
| `fg_made_30_39` | integer |  |
| `fg_made_40_49` | integer |  |
| `fg_made_50_59` | integer |  |
| `fg_made_60_` | integer |  |
| `fg_missed_0_19` | integer |  |
| `fg_missed_20_29` | integer |  |
| `fg_missed_30_39` | integer |  |
| `fg_missed_40_49` | integer |  |
| `fg_missed_50_59` | integer |  |
| `fg_missed_60_` | integer |  |
| `fg_made_list` | character |  |
| `fg_missed_list` | character |  |
| `fg_blocked_list` | character |  |
| `fg_made_distance` | integer |  |
| `fg_missed_distance` | integer |  |
| `fg_blocked_distance` | integer |  |
| `pat_made` | integer |  |
| `pat_att` | integer |  |
| `pat_missed` | integer |  |
| `pat_blocked` | integer |  |
| `pat_pct` | double |  |
| `gwfg_made` | integer |  |
| `gwfg_att` | integer |  |
| `gwfg_missed` | integer |  |
| `gwfg_blocked` | integer |  |
| `gwfg_distance` | integer |  |

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
