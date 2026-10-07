---
title: "NFL — additional Python functions — NFL Pro: nfl_ngs–scrape_ngs"
sidebar_label: "NFL Pro: nfl_ngs–scrape_ngs"
sidebar_position: 4
description: "NFL — additional Python functions — NFL Pro: nfl_ngs–scrape_ngs — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — NFL Pro: nfl_ngs–scrape_ngs

### nfl_ngs_statboard_leaders {#nfl_ngs_statboard_leaders}

`nfl_ngs_statboard_leaders(season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)`

NGS cross-stat "leaders" board, stacked long with a `category` column.

Wraps `/api/statboard/leaders`, which bundles several short top-N lists of
mixed shape (`fastestBallCarriers`, `fastestSacks`, `longestCompletions`,
`highestSeparation`, `rushYardsOverExpected`, `completionPctAboveExpected`,
`avgYACAboveExpected`). Each list is normalized separately and concatenated
diagonally (union of columns; missing cells become null), with a `category`
column recording which board each row came from.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2024` | season year. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | optional single-week filter. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` stacking every leader list, with a `category` column. Empty frame if no lists are present.

| col_name | type | description |
|---|---|---|
| `category` | character | Broader category of player positions |
| `leader_esbId` | character | ESB (NFL's Enterprise Subscriber Base) identifier for the player featured in the leader record. |
| `leader_firstName` | character | First name of the player featured in the leader record. |
| `leader_gsisId` | character | GSIS (Game Statistics and Information System) identifier for the player featured in the leader record. |
| `leader_jerseyNumber` | integer | Jersey number of the player featured in the leader record. |
| `leader_lastName` | character | Last name of the player featured in the leader record. |
| `leader_playerName` | character | Full display name of the player featured in the leader record. |
| `leader_position` | character | Position designation of the player featured in the leader record (e.g., 'QB', 'WR'). |
| `leader_positionGroup` | character | Broad position group of the player featured in the leader record (e.g., 'OFFENSE', 'DEFENSE'). |
| `leader_shortName` | character | Abbreviated display name of the player featured in the leader record (e.g., 'P.Mahomes'). |
| `leader_teamAbbr` | character | Team abbreviation of the player featured in the leader record (e.g., 'KC'). |
| `leader_teamId` | character | NFL Shield team identifier for the team of the player featured in the leader record. |
| `leader_week` | integer | Week number associated with the weekly leader record. |
| `leader_yards` | integer | Yardage total associated with the featured leader statistic for this player. |
| `leader_inPlayDist` | double | Distance (in yards) the player traveled while the ball was in play on the featured leader statistic. |
| `leader_maxSpeed` | double | Maximum speed (in yards per second or mph) recorded by the player on the featured leader statistic play. |
| `leader_headshot` | character | URL to the headshot image of the player featured in the leader record. |
| `play_gameId` | integer | NFL Shield API game identifier for the game this play belongs to. |
| `play_playId` | integer | NFL Shield API unique integer identifier for this play within the game. |
| `play_sequence` | integer | Sequential order integer of this play within the game as used by the NFL Shield API. |
| `play_down` | integer | Down number (1–4) at the time of this play. |
| `play_gameClock` | character | Game clock time remaining at the start of this play in MM:SS format. |
| `play_gameKey` | integer | NFL Shield internal game key integer identifying the game in internal API routing. |
| `play_health_playerTracking` | character | Data quality status string for player-tracking data on this play (e.g., 'GOOD', 'PARTIAL'). |
| `play_health_ballTracking` | character | Data quality status string for ball-tracking data on this play (e.g., 'GOOD', 'MISSING'). |
| `play_homeScore` | integer | Home team's score at the end of this play. |
| `play_isBigPlay` | logical | Boolean flag indicating whether this play was classified as a 'big play' by the NFL Shield API. |
| `play_isEndQuarter` | logical | Boolean flag indicating whether this play was the final play of a quarter. |
| `play_isGoalToGo` | logical | Boolean flag indicating whether this play occurred in a goal-to-go situation. |
| `play_isPenalty` | logical | Boolean flag indicating whether a penalty was assessed on this play. |
| `play_isSTPlay` | logical | Boolean flag indicating whether this play was a special teams play. |
| `play_isScoring` | logical | Boolean flag indicating whether this play resulted in a score. |
| `play_playDescription` | character | Official NFL text description of the play (e.g., '(2:11) J.Allen scrambles right end for 9 yards'). |
| `play_playState` | character | Status of the play record (e.g., 'FINAL', 'LIVE') in the NFL Shield API at time of capture. |
| `play_playStats` | integer | Serialized or encoded statistical detail for the play outcomes (e.g., yards, player IDs). |
| `play_playType` | character | Text label classifying the play type (e.g., 'PASS', 'RUSH', 'PUNT') from the NFL Shield API. |
| `play_playTypeCode` | integer | Integer code corresponding to the play type classification in the NFL Shield API. |
| `play_possessionTeamId` | character | NFL Shield team identifier for the team that had possession during this play. |
| `play_preSnapHomeScore` | integer | Home team's score immediately before the snap of this play. |
| `play_preSnapVisitorScore` | integer | Visiting team's score immediately before the snap of this play. |
| `play_quarter` | integer | Quarter number (1–4, or 5 for overtime) in which this play occurred. |
| `play_timeOfDayUTC` | character | Wall-clock timestamp in UTC at which this play occurred during the broadcast. |
| `play_visitorScore` | integer | Visiting team's score at the end of this play. |
| `play_yardline` | character | Human-readable yardline string (e.g., 'DAL 22') indicating where this play began. |
| `play_yardlineNumber` | integer | Numeric yard line (1–50 from nearest end zone) of the line of scrimmage at the snap. |
| `play_yardlineSide` | character | Team abbreviation indicating which team's side of the field the yardline is on. |
| `play_yardsToGo` | integer | Integer yards needed for a first down on this play. |
| `play_absoluteYardlineNumber` | integer | Absolute yard line number (1–100 from one end zone) of the line of scrimmage at the snap for this play. |
| `play_actualYardlineForFirstDown` | double | Actual yard line marker needed for a first down on this play. |
| `play_actualYardsToGo` | double | Exact decimal yards required for a first down on this play. |
| `play_endGameClock` | character | Game clock time remaining at the end of this play in MM:SS format. |
| `play_isChangeOfPossession` | logical | Boolean flag indicating whether this play resulted in a change of possession. |
| `play_playDirection` | character | Direction of the play (left, right, middle) as recorded in NFL Shield tracking data. |
| `play_startGameClock` | character | Game clock time remaining at the moment this play began, in MM:SS format. |
| `leader_time` | double | Elapsed time value associated with the leader record, such as time of possession or time-to-throw. |
| `leader_seasonAvg` | double | Season-to-date average of the featured leader statistic for this player. |
| `leader_teamAvg` | double | Team-level average of the featured statistic across all players on the team. |
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `avgAirDistance` | double | Average air distance (in yards) the ball traveled on all pass attempts, from NFL Next Gen Stats. |
| `avgAirYardsDifferential` | double | Average difference between intended air yards and completed air yards per attempt, indicating whether the passer throws short or long relative to the targeted depth. |
| `avgAirYardsToSticks` | double | Average air yards relative to the first-down marker on pass attempts, from NFL Next Gen Stats (negative = short of sticks, positive = past sticks). |
| `avgCompletedAirYards` | double | Average air yards on completed passes only, from NFL Next Gen Stats. |
| `avgIntendedAirYards` | double | Average intended air yards per pass attempt (including incompletions), from NFL Next Gen Stats. |
| `avgTimeToThrow` | double | Average time in seconds from snap to throw for the passer, from NFL Next Gen Stats. |
| `completionPercentage` | double | Percentage of pass attempts completed by the passer, from NFL Next Gen Stats statboard leaders. |
| `completionPercentageAboveExpectation` | double | Passer's actual completion percentage minus their expected completion percentage based on target depth, coverage, and game context (CPOE), from NFL Next Gen Stats. |
| `completions` | integer | The number of completed passes. |
| `expectedCompletionPercentage` | double | Model-predicted completion percentage for the passer based on depth of target, receiver separation, and coverage, from NFL Next Gen Stats. |
| `gamesPlayed` | integer | Number of games played by the player in the given season or time period. |
| `interceptions` | integer | The number of interceptions thrown. |
| `maxAirDistance` | double | Maximum air distance (in yards) recorded on a single pass attempt by the passer, from NFL Next Gen Stats. |
| `maxCompletedAirDistance` | double | Maximum air distance on a completed pass for the passer, from NFL Next Gen Stats. |
| `passTouchdowns` | integer | Total passing touchdowns recorded by the player in the given period. |
| `passYards` | integer | Total passing yards recorded by the player in the given period. |
| `passerRating` | double | NFL passer rating (0–158.3 scale) for the quarterback in the given period. |
| `playerName` | character | Full display name of the player associated with this NGS statboard leader record. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Phase of the season for this record (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `position` | character | Primary position as reported by NFL.com |
| `teamId` | character | NFL Shield team identifier for the team associated with this leader statistic. |
| `player_season` | integer | NFL season year in which this player record is valid. |
| `player_currentTeamId` | character | NFL Shield team identifier for the player's current team. |
| `player_displayName` | character | Player's full display name as used by NFL Next Gen Stats (e.g., 'Patrick Mahomes'). |
| `player_esbId` | character | ESB (Enterprise Subscriber Base) identifier assigned to this player by the NFL. |
| `player_firstName` | character | Player's first name as recorded in the NFL Next Gen Stats player roster. |
| `player_footballName` | character | Player's preferred football name (may differ from legal first name) used on the field and in broadcasts. |
| `player_gsisId` | character | GSIS (Game Statistics and Information System) identifier, the primary NFL player identifier used across nflverse datasets. |
| `player_gsisItId` | integer | GSIS integrated tracking identifier, an alternate integer-form player ID used in the NFL Shield tracking system. |
| `player_jerseyNumber` | integer | Player's jersey number as worn on the field. |
| `player_lastName` | character | Player's last name as recorded in the NFL Next Gen Stats player roster. |
| `player_position` | character | Position of the player accordinng to NGS |
| `player_positionGroup` | character | Official NFL position group for the player (e.g., 'OFFENSE', 'DEFENSE', 'SPECIAL_TEAMS'). |
| `player_shortName` | character | Abbreviated player name used in display contexts (e.g., 'P.Mahomes'). |
| `player_status` | character | Player's current roster status (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_uniformNumber` | character | Player's uniform number as a string, matching what appears on the jersey. |
| `player_headshot` | character |  |
| `player_smartId` | character | NFL Smart ID — a system-agnostic unique identifier for the player used across NFL Shield systems. |
| `player_ngsPosition` | character | Player's position as classified by NFL Next Gen Stats (may differ from official NFL position; e.g., NGS uses 'ILB' vs 'LB'). |
| `player_ngsPositionGroup` | character | Broad position group assigned by NFL Next Gen Stats (e.g., 'QB', 'WR', 'DB', 'DL'). |
| `avgCushion` | double | Average distance (in yards) between the receiver and the nearest defender at the snap, from NFL Next Gen Stats receiver tracking. |
| `avgExpectedYAC` | double | Average yards after catch expected based on game situation and receiver location, from NFL Next Gen Stats models. |
| `avgSeparation` | double | Average separation (in yards) between the receiver and the nearest defender at the moment of catch or incompletion, from NFL Next Gen Stats. |
| `avgYAC` | double | Average yards after catch per reception, from NFL Next Gen Stats receiver tracking. |
| `avgYACAboveExpectation` | double | Average yards after catch above statistical expectation (YAC - expected YAC), from NFL Next Gen Stats models. |
| `catchPercentage` | double | Percentage of targets caught by the receiver, from NFL Next Gen Stats statboard leaders. |
| `percentShareOfIntendedAirYards` | double | Receiver's share (as a percentage) of the team's total intended air yards, from NFL Next Gen Stats. |
| `recTouchdowns` | integer | Total receiving touchdowns recorded by the player in the given period. |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `yards` | integer | The number of receiving yards |
| `avgTimeToLos` | double | Average time (in seconds) for the ball carrier to reach the line of scrimmage on rush attempts, from NFL Next Gen Stats. |
| `expectedRushYards` | double | Model-predicted rushing yards based on field position, personnel, and pre-snap alignment, from NFL Next Gen Stats. |
| `rushAttempts` | integer | Total rushing attempts by the player in the given period. |
| `rushPctOverExpected` | double | Percentage of rush attempts on which the player gained more yards than the model-predicted expectation, from NFL Next Gen Stats. |
| `rushTouchdowns` | integer | Total rushing touchdowns recorded by the player in the given period. |
| `rushYards` | integer | Total rushing yards recorded by the player in the given period. |
| `rushYardsOverExpected` | double | Total rushing yards gained above statistical expectation (RYOE) in the given period, from NFL Next Gen Stats. |
| `rushYardsOverExpectedPerAtt` | double | Average rushing yards over expected per attempt (RYOE/attempt), from NFL Next Gen Stats. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_statboard_leaders
bd = nfl_ngs_statboard_leaders(season=2024, season_type="REG")
bd["category"].unique().to_list()
```

### nfl_ngs_yac_oe {#nfl_ngs_yac_oe}

`nfl_ngs_yac_oe(seasons: 'Union[int, Sequence[int]]', *, min_receptions: 'int' = 10, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

YAC over expected per receiver-season, stabilised with EB shrinkage.

`yac_oe_raw` is the NGS-shipped `avg_yac_above_expectation` passed
through unchanged (per-reception yards after catch minus the NGS
tracking-model expectation). `yac_oe_shrunk` applies per-season
Efron-Morris empirical-Bayes shrinkage toward the reception-weighted
league mean, weighted by `receptions`, so small-sample extremes are
pulled in. The shrinkage prior is fit at call time on rows with
`receptions >= min_receptions` — no bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s) to compute, 2016+. |
| `min_receptions` | `int` | `10` | Qualification threshold for the prior fit and for receiving a `yac_oe_rank`. Defaults to `sportsdataverse.nfl.nfl_ngs_constants.MIN_RECEPTIONS`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, player_gsis_id)` with raw + shrunk YAC-OE, `reliability` in [0, 1], and a dense descending `yac_oe_rank` over qualified rows (null for unqualified rows). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY). |
| `player_gsis_id` | character | Player GSIS identifier (nflverse id, e.g. "00-0036223"), pinned Utf8. |
| `player_display_name` | character | Player display name as shipped by NGS. |
| `team_abbr` | character | Team abbreviation. |
| `position` | character | Player position (from NGS player_position). |
| `receptions` | double | Season reception count — the shrinkage weight. |
| `avg_yac` | double | Average yards after catch per reception. |
| `avg_expected_yac` | double | NGS tracking-model expected YAC per reception. |
| `yac_oe_raw` | double | NGS avg_yac_above_expectation passed through unchanged — per-reception YAC minus the tracking-model expectation. |
| `yac_oe_shrunk` | double | yac_oe_raw after per-season empirical-Bayes shrinkage toward the reception-weighted league mean (sampling variance identified from weekly rows). |
| `reliability` | double | Shrinkage reliability tau2 / (tau2 + sigma2 / receptions) in [0, 1]; the fraction of the raw deviation retained. |
| `yac_oe_rank` | integer | Dense descending rank of yac_oe_shrunk within season over qualified rows; null when receptions < min_receptions. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_yac_oe
df = nfl_ngs_yac_oe([2023])
print(df.sort("yac_oe_rank").head())

# Pandas output

df_pd = nfl_ngs_yac_oe(2023, return_as_pandas=True)
```

### scrape_ngs_season {#scrape_ngs_season}

`scrape_ngs_season(stat_type: 'str', season: 'int', *, include_season_totals: 'bool' = True, return_as_pandas: 'bool' = False)`

Scrape a full season of NGS statboard data, shaped like the nflverse parquet.

Port of nflverse ngs-data's R `save_ngs_type`: loop the regular-season weeks
(`1..max_reg` where `max_reg = 18` for `season >= 2021` else `17`) plus
the playoff weeks (`max_reg+1 .. max_reg+5`, fetched with
`season_type="POST"`), stack them diagonally, and -- when
`include_season_totals` -- prepend the season-aggregate rows (NGS `week=0`,
a `REG` call with no `week` param) tagged `week=0`. Duplicate rows (NGS
returned dupes for some 2022 weeks) are de-duplicated on
`(season, week, player_gsis_id)`.

Output columns match the published nflverse NGS parquet read by
`sportsdataverse.nfl.load_nfl_nextgen_stats` (snake_case, `team_abbr`
resolved). It will not be byte-identical -- nflverse post-processes (column
pruning / ordering) -- but the core metric columns and the
player/team/week keys align.

NGS statboard rows are **player-week aggregates**, NOT per-play rows.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` |  | one of `"passing"`, `"rushing"`, `"receiving"`. |
| `season` | `int` |  | season year (NGS coverage starts in 2016). |
| `include_season_totals` | `bool` | `True` | also fetch the season-aggregate (`week=0`) rows. Defaults to `True` (matches ngs-data, whose week loop starts at 0). |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` stacking every week (and, by default, the season totals) for the requested `stat_type` and `season`. An EMPTY frame carrying the documented key schema if nothing is returned.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Percentage of pass attempts thrown into tight windows (defender within one yard of the receiver at completion or incompletion), from NFL Next Gen Stats. Passing only. |
| `attempts` | integer | Pass attempts (including incompletions) recorded for the passer during the slice. Passing only. |
| `avg_air_distance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. Passing only. |
| `avg_air_yards_differential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. Passing only. |
| `avg_air_yards_to_sticks` | double | Average air yards relative to the first-down marker on pass attempts (positive = beyond the sticks). Passing only. |
| `avg_completed_air_yards` | double | Average air yards on completed passes only, measuring depth of actual completions. Passing only. |
| `avg_intended_air_yards` | double | Average depth of target on all pass attempts, regardless of completion. Passing/receiving. |
| `avg_time_to_throw` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. Passing only. |
| `completion_percentage` | double | Actual completion percentage for the passer during the slice. Passing only. |
| `completion_percentage_above_expectation` | double | Completion percentage above the model-expected completion rate (CPOE/CPAE). Passing only. |
| `completions` | integer | Completed passes for the passer during the slice. Passing only. |
| `expected_completion_percentage` | double | Model-expected completion percentage based on target depth, separation, and coverage, from NFL Next Gen Stats. Passing only. |
| `games_played` | integer | Number of games played by the player during the slice covered by the row. |
| `interceptions` | integer | Interceptions thrown by the passer during the slice. Passing only. |
| `max_air_distance` | double | Maximum air distance in yards recorded on any single pass attempt during the slice. Passing only. |
| `max_completed_air_distance` | double | Maximum air distance in yards recorded on any single completed pass during the slice. Passing only. |
| `pass_touchdowns` | integer | Passing touchdowns thrown by the passer during the slice. Passing only. |
| `pass_yards` | integer | Total passing yards accumulated by the passer during the slice. Passing only. |
| `passer_rating` | double | NFL passer rating (0–158.3 scale) for the passer during the slice. Passing only. |
| `player_name` | character | Display name of the player as returned at the top-level statboard row. |
| `season` | integer | NFL season year for the row. |
| `season_type` | character | Season segment for the row ('REG' for regular-season weeks and the week-0 aggregate, 'POST' for playoff weeks). |
| `week` | integer | NFL Next Gen Stats week tag; 0 is the season aggregate, 1..max_reg are regular-season weeks, and higher values are continuous playoff weeks. |
| `position` | character | Player position as returned at the top-level statboard row (e.g., 'QB', 'WR', 'RB'). |
| `team_id` | character | NFL Next Gen Stats team identifier for the player's team on this row; the key joined to resolve team_abbr. |
| `player_season` | integer | NFL season year recorded in the nested player record. |
| `player_season_type` | character | Season segment recorded in the nested player record. |
| `player_week` | integer | Week recorded in the nested player record (the loop week tag overrides this on the top-level week column). |
| `player_jersey_number` | integer | Jersey number worn by the player. |
| `player_last_name` | character | Last name of the player. |
| `player_football_name` | character | Football name used by the player, which may differ from the legal first name. |
| `player_first_name` | character | First name of the player. |
| `player_position` | character | Player position from the nested player record. |
| `player_gsis_it_id` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_gsis_id` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_esb_id` | character | Elias Sports Bureau (ESB) identifier for the player. |
| `player_display_name` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_short_name` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_uniform_number` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_current_team_id` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_smart_id` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_headshot` | character | URL template for the player's headshot image. |
| `player_position_group` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_ngs_position` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngs_position_group` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |
| `team_abbr` | character | Team abbreviation resolved from team_id via the NGS team directory (relocated franchise abbreviations dropped to keep the mapping one-to-one). |

**Example**

```python
from sportsdataverse.nfl import scrape_ngs_season
pas = scrape_ngs_season("passing", 2023)
pas.select(["season", "week", "player_display_name", "team_abbr"]).head()

# Regular-season weeks only (skip the week-0 totals)

wk = scrape_ngs_season("receiving", 2023, include_season_totals=False)

# Column-compatible with the published parquet

from sportsdataverse.nfl import load_nfl_nextgen_stats
published = load_nfl_nextgen_stats(seasons=[2023], stat_type="passing")
shared = set(pas.columns) & set(published.columns)
```

### scrape_ngs_week {#scrape_ngs_week}

`scrape_ngs_week(stat_type: 'str', season: 'int', week: 'int', season_type: 'str' = 'REG', *, return_as_pandas: 'bool' = False)`

Scrape one (season, week) NGS statboard slice, shaped like the nflverse parquet.

Port of nflverse ngs-data's R `load_week_ngs`: fetch a single statboard
slice via `nfl_ngs_statboard`, resolve `team_abbr` from the team
directory, snake-case every column, and tag the row with the loop `week`.
`week=0` is the season-aggregate row (a `season_type="REG"` call with no
`week` query param); weeks `1..max_reg` are regular-season, and the
playoff weeks (`max_reg+1` upward) are fetched with `season_type="POST"`.

NGS statboard rows are **player-week aggregates** (`avg_intended_air_yards`,
`completion_percentage_above_expectation`, `avg_time_to_throw`, ...), NOT
per-play rows -- this is a season-stats source, not a per-play air-yards /
completion-probability source.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` |  | one of `"passing"`, `"rushing"`, `"receiving"`. |
| `season` | `int` |  | season year (NGS coverage starts in 2016). |
| `week` | `int` |  | NGS week. `0` -> season aggregate; `1..max_reg` -> REG; higher -> POST. The supplied value is what tags the returned rows. |
| `season_type` | `str` | `'REG'` | `"REG"` or `"POST"`; the caller (or `scrape_ngs_season`) selects this per week. Defaults to `"REG"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame` of player-week NGS rows with snake-cased columns + a resolved `team_abbr`. An EMPTY frame carrying the documented key schema (not an exception) when the API yields no stats.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Percentage of pass attempts thrown into tight windows (defender within one yard of the receiver at completion or incompletion), from NFL Next Gen Stats. Passing only. |
| `attempts` | integer | Pass attempts (including incompletions) recorded for the passer during the week. Passing only. |
| `avg_air_distance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. Passing only. |
| `avg_air_yards_differential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. Passing only. |
| `avg_air_yards_to_sticks` | double | Average air yards relative to the first-down marker on pass attempts (positive = beyond the sticks). Passing only. |
| `avg_completed_air_yards` | double | Average air yards on completed passes only, measuring depth of actual completions. Passing only. |
| `avg_intended_air_yards` | double | Average depth of target on all pass attempts, regardless of completion. Passing/receiving. |
| `avg_time_to_throw` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. Passing only. |
| `completion_percentage` | double | Actual completion percentage for the passer during the week. Passing only. |
| `completion_percentage_above_expectation` | double | Completion percentage above the model-expected completion rate (CPOE/CPAE). Passing only. |
| `completions` | integer | Completed passes for the passer during the week. Passing only. |
| `expected_completion_percentage` | double | Model-expected completion percentage based on target depth, separation, and coverage, from NFL Next Gen Stats. Passing only. |
| `games_played` | integer | Number of games played by the player during the slice covered by the row. |
| `interceptions` | integer | Interceptions thrown by the passer during the week. Passing only. |
| `max_air_distance` | double | Maximum air distance in yards recorded on any single pass attempt during the week. Passing only. |
| `max_completed_air_distance` | double | Maximum air distance in yards recorded on any single completed pass during the week. Passing only. |
| `pass_touchdowns` | integer | Passing touchdowns thrown by the passer during the week. Passing only. |
| `pass_yards` | integer | Total passing yards accumulated by the passer during the week. Passing only. |
| `passer_rating` | double | NFL passer rating (0–158.3 scale) for the passer during the week. Passing only. |
| `player_name` | character | Display name of the player as returned at the top-level statboard row. |
| `season` | integer | NFL season year for the row. |
| `season_type` | character | Season segment for the row ('REG' or 'POST') as supplied by the caller. |
| `week` | integer | NFL Next Gen Stats week tag supplied by the caller; 0 is the season aggregate, 1..max_reg are regular-season weeks, and higher values are continuous playoff weeks. |
| `position` | character | Player position as returned at the top-level statboard row (e.g., 'QB', 'WR', 'RB'). |
| `team_id` | character | NFL Next Gen Stats team identifier for the player's team on this row; the key joined to resolve team_abbr. |
| `player_season` | integer | NFL season year recorded in the nested player record. |
| `player_season_type` | character | Season segment recorded in the nested player record. |
| `player_week` | integer | Week recorded in the nested player record (the loop week tag overrides this on the top-level week column). |
| `player_jersey_number` | integer | Jersey number worn by the player. |
| `player_last_name` | character | Last name of the player. |
| `player_football_name` | character | Football name used by the player, which may differ from the legal first name. |
| `player_first_name` | character | First name of the player. |
| `player_position` | character | Player position from the nested player record. |
| `player_gsis_it_id` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_gsis_id` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_esb_id` | character | Elias Sports Bureau (ESB) identifier for the player. |
| `player_display_name` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_short_name` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_uniform_number` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_current_team_id` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_smart_id` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_headshot` | character | URL template for the player's headshot image. |
| `player_position_group` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_ngs_position` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngs_position_group` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |
| `team_abbr` | character | Team abbreviation resolved from team_id via the NGS team directory (relocated franchise abbreviations dropped to keep the mapping one-to-one). |

**Example**

```python
from sportsdataverse.nfl import scrape_ngs_week
wk1 = scrape_ngs_week("passing", 2023, week=1)
wk1.select(["season", "week", "player_display_name", "team_abbr"]).head()

# Season-aggregate row (week 0)

tot = scrape_ngs_week("rushing", 2023, week=0)
```
