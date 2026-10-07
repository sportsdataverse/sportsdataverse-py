---
title: "NFL — additional Python functions — Nfl: ngs–season"
sidebar_label: "Nfl: ngs–season"
sidebar_position: 9
description: "NFL — additional Python functions — Nfl: ngs–season — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Nfl: ngs–season

### nfl_ngs_play_is_highlight {#nfl_ngs_play_is_highlight}

`nfl_ngs_play_is_highlight(game_id, play_id, return_as_pandas: 'bool' = False)`

Look up whether a single play is an NGS highlight -- one-row frame.

Wraps `/api/plays/isHighlight` (keyed by NGS `gameId` + `playId`). When
the play is a highlight, the response's nested `highlight` block (the play
metadata, the `players` involved, season/week/team) is flattened onto the
row alongside the top-level `gameId`/`playId`/`isHighlight` flag. Pull a
real `(gameId, playId)` pair from `nfl_ngs_leaders` -- each leader
entry's `play_gameId` / `play_playId` is a known highlight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  | NGS `gameId` (e.g. `"2024111800"`). |
| `play_id` |  |  | the play id within that game (e.g. `1214`). |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A one-row polars (or pandas) `DataFrame` with `gameId`, `playId`, `isHighlight` and (when true) flattened `highlight_*` columns.

| col_name | type | description |
|---|---|---|
| `gameId` | integer | NFL Shield API game identifier for the game this highlight record belongs to. |
| `playId` | integer |  |
| `isHighlight` | logical | Boolean flag indicating whether this play record has been designated as a highlight by the NFL Shield API. |
| `highlight_gameId` | integer | NFL Shield API game identifier associated with the specific highlight clip. |
| `highlight_playId` | integer | NFL Shield API play identifier for the specific play associated with the highlight clip. |
| `highlight_play_playType` | character | Text label for the type of play (e.g., 'PASS', 'RUSH', 'PUNT') as classified by the NFL Shield API. |
| `highlight_play_gameId` | integer | NFL Shield API game identifier embedded in the play detail record for the highlight. |
| `highlight_play_gameKey` | integer | NFL Shield internal game key integer used to identify the game in internal API routing. |
| `highlight_play_yardlineSide` | character | Team abbreviation indicating which team's side of the field the yardline is on. |
| `highlight_play_absoluteYardlineNumber` | integer | Absolute yard line number (1–100 from one end zone) of the line of scrimmage at the snap for the highlighted play. |
| `highlight_play_yardlineNumber` | integer | Numeric yard line (1–50 from nearest end zone) of the line of scrimmage at the snap. |
| `highlight_play_timeOfDayUTC` | character | Wall-clock timestamp in UTC at which the highlighted play occurred during the broadcast. |
| `highlight_play_isPenalty` | logical | Boolean flag indicating whether a penalty was assessed on the highlighted play. |
| `highlight_play_homeScore` | integer | Home team's score at the end of the highlighted play. |
| `highlight_play_visitorScore` | integer | Visiting team's score at the end of the highlighted play. |
| `highlight_play_playStats` | integer | Serialized or encoded statistical detail associated with the play outcomes (e.g., yards, player ids). |
| `highlight_play_playId` | integer | NFL Shield API unique integer identifier for the specific play within the game. |
| `highlight_play_playDescription` | character | Official NFL text description of the highlighted play (e.g., '(12:34) T.Brady pass short right to J.Edelman for 8 yards'). |
| `highlight_play_playTypeCode` | integer | Integer code corresponding to the play type classification in the NFL Shield API. |
| `highlight_play_quarter` | integer | Quarter number (1–4, or 5 for overtime) during which the highlighted play occurred. |
| `highlight_play_down` | integer | Down number (1–4) at the time of the highlighted play. |
| `highlight_play_yardsToGo` | integer | Integer yards needed for a first down on the highlighted play. |
| `highlight_play_actualYardsToGo` | double | Exact decimal yards required for a first down on the highlighted play. |
| `highlight_play_actualYardlineForFirstDown` | double | Actual yard line marker needed for a first down on the highlighted play. |
| `highlight_play_possessionTeamId` | character | NFL Shield team identifier for the team that had possession during the highlighted play. |
| `highlight_play_isGoalToGo` | logical | Boolean flag indicating whether the highlighted play occurred in a goal-to-go situation. |
| `highlight_play_health` | character | Data quality or tracking health status string indicating the reliability of player/ball tracking data for this play. |
| `highlight_play_endGameClock` | character | Game clock time remaining at the end of the highlighted play in MM:SS format. |
| `highlight_play_startGameClock` | character | Game clock time remaining at the start of the highlighted play in MM:SS format. |
| `highlight_play_playState` | character | State or status of the play record (e.g., 'FINAL', 'LIVE') in the NFL Shield API at time of capture. |
| `highlight_play_preSnapHomeScore` | integer | Home team's score immediately before the snap of the highlighted play. |
| `highlight_play_preSnapVisitorScore` | integer | Visitor team's score immediately before the snap of the highlighted play. |
| `highlight_play_sequence` | integer | Sequential order integer of this play within the game, as used by the NFL Shield API. |
| `highlight_play_gameClock` | character | Game clock time remaining at the start of the highlighted play in MM:SS format. |
| `highlight_play_yardline` | character | Human-readable yardline string (e.g., 'KC 35') indicating where the play began. |
| `highlight_play_isScoring` | logical | Boolean flag indicating whether the highlighted play resulted in a score. |
| `highlight_play_isEndQuarter` | logical | Boolean flag indicating whether the highlighted play was the final play of a quarter. |
| `highlight_play_isSTPlay` | logical | Boolean flag indicating whether the highlighted play was a special teams play. |
| `highlight_play_playDirection` | character | Direction of the play (left, right, middle) as recorded in the NFL Shield tracking data. |
| `highlight_play_isBigPlay` | logical | Boolean flag indicating whether the play was classified as a 'big play' (e.g., long gain or turnover) by the NFL Shield API. |
| `highlight_play_isChangeOfPossession` | logical | Boolean flag indicating whether the highlighted play resulted in a change of possession. |
| `highlight_players` | integer | Serialized list or count of player tracking records associated with the highlight clip. |
| `highlight_season` | integer | NFL season year (e.g., 2024) in which the highlighted play occurred. |
| `highlight_seasonType` | character | Season phase of the highlighted play (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `highlight_teamAbbr` | character | Team abbreviation of the team featured in or associated with the highlight clip. |
| `highlight_teamId` | character | NFL Shield team identifier for the team featured in the highlight clip. |
| `highlight_week` | integer | Week number within the season during which the highlighted play occurred. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_leaders, nfl_ngs_play_is_highlight
lead = nfl_ngs_leaders(category="speed", season=2024, season_type="REG")
gid, pid = lead["play_gameId"][0], lead["play_playId"][0]
hl = nfl_ngs_play_is_highlight(game_id=gid, play_id=pid)
hl.select(["gameId", "playId", "isHighlight"]).head()
```

### nfl_ngs_ryoe {#nfl_ngs_ryoe}

`nfl_ngs_ryoe(seasons: 'Union[int, Sequence[int]]', *, min_attempts: 'int' = 20, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Rush yards over expected per rusher-season, stabilised with EB shrinkage.

`ryoe_per_att_raw` is the NGS-shipped
`rush_yards_over_expected_per_att` passed through unchanged (the NGS
tracking-model residual); `ryoe_total` is the season total
`rush_yards_over_expected`. `ryoe_per_att_shrunk` applies per-season
Efron-Morris empirical-Bayes shrinkage toward the attempt-weighted league
mean, weighted by `rush_attempts`. `pct_stacked_box`
(`percent_attempts_gte_eight_defenders`) is reported as a context
covariate — v1 does not adjust on it. The prior is fit at call time on
rows with `rush_attempts >= min_attempts` — no bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s) to compute, 2016+. |
| `min_attempts` | `int` | `20` | Qualification threshold for the prior fit and for receiving a `ryoe_rank`. Defaults to `sportsdataverse.nfl.nfl_ngs_constants.MIN_ATTEMPTS`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, player_gsis_id)` with raw + shrunk RYOE/attempt, `reliability` in [0, 1], and a dense descending `ryoe_rank` over qualified rows (null for unqualified rows). Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY). |
| `player_gsis_id` | character | Player GSIS identifier (nflverse id, e.g. "00-0036223"), pinned Utf8. |
| `player_display_name` | character | Player display name as shipped by NGS. |
| `team_abbr` | character | Team abbreviation. |
| `position` | character | Player position (from NGS player_position). |
| `rush_attempts` | double | Season rush attempts — the shrinkage weight. |
| `rush_yards` | double | Season rushing yards. |
| `expected_rush_yards` | double | NGS tracking-model expected rushing yards for the season. |
| `ryoe_total` | double | Season rush yards over expected (NGS rush_yards_over_expected, passed through). |
| `ryoe_per_att_raw` | double | NGS rush_yards_over_expected_per_att passed through unchanged — the tracking-model residual per attempt. |
| `ryoe_per_att_shrunk` | double | ryoe_per_att_raw after per-season empirical-Bayes shrinkage toward the attempt-weighted league mean (sampling variance identified from weekly rows). |
| `pct_stacked_box` | double | Percent of attempts against 8+ defenders in the box (NGS percent_attempts_gte_eight_defenders) — reported as a context covariate, not adjusted on. |
| `reliability` | double | Shrinkage reliability tau2 / (tau2 + sigma2 / rush_attempts) in [0, 1]; the fraction of the raw deviation retained. |
| `ryoe_rank` | integer | Dense descending rank of ryoe_per_att_shrunk within season over qualified rows; null when rush_attempts < min_attempts. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_ryoe
df = nfl_ngs_ryoe([2023])
print(df.sort("ryoe_rank").head())

# Pandas output

df_pd = nfl_ngs_ryoe(2023, return_as_pandas=True)
```

### nfl_ngs_separation_oe {#nfl_ngs_separation_oe}

`nfl_ngs_separation_oe(seasons: 'Union[int, Sequence[int]]', *, min_targets: 'int' = 20, return_as_pandas: 'bool' = False, _loader: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Separation over a built context expectation, per receiver-season.

Unlike YAC-OE and RYOE, NGS ships no expected-separation field, so this
model BUILDS one: a per-season weighted ridge
(`sportsdataverse.nfl.nfl_ngs_constants.expected_separation_ridge`)
of `avg_separation` on `avg_cushion`, `avg_intended_air_yards` and
a position one-hot, weighted by `targets`. `sep_oe_raw` is the
residual — a CONTEXT residual (role/scheme proxies), not a
tracking-model expectation; treat it as descriptive, not causal.
`sep_oe_shrunk` applies the same per-season empirical-Bayes shrinkage
as the sibling models, weighted by `targets`. All parameters are fit
from the requested seasons at call time — no bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, Sequence[int]]` |  | Season(s) to compute, 2016+. |
| `min_targets` | `int` | `20` | Qualification threshold for the shrinkage prior and for receiving a `sep_oe_rank`. Defaults to `sportsdataverse.nfl.nfl_ngs_constants.MIN_TARGETS`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |
| `_loader` | `Optional[Callable]` | `None` | Injectable loader for offline tests. |

**Returns**

One row per `(season, player_gsis_id)` with the built `expected_separation`, raw + shrunk separation-over-expected, `reliability` in [0, 1], and a dense descending `sep_oe_rank` over qualified rows. Rows with null separation/cushion/air-yards inputs are dropped before the fit. Empty input returns a zero-row frame with the documented schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | NFL season (YYYY). |
| `player_gsis_id` | character | Player GSIS identifier (nflverse id, e.g. "00-0036223"), pinned Utf8. |
| `player_display_name` | character | Player display name as shipped by NGS. |
| `team_abbr` | character | Team abbreviation. |
| `position` | character | Player position (from NGS player_position); one-hot feature in the expectation ridge. |
| `targets` | double | Season target count — the ridge weight and the shrinkage weight. |
| `avg_cushion` | double | Average defender cushion at snap, yards (ridge feature). |
| `avg_separation` | double | Average separation from the nearest defender at catch/incompletion, yards. |
| `avg_intended_air_yards` | double | Average intended air yards on targets (ridge feature). |
| `expected_separation` | double | Built per-season weighted-ridge expectation of avg_separation from cushion, intended air yards and position — a CONTEXT expectation, not a tracking model. |
| `sep_oe_raw` | double | avg_separation minus expected_separation (context residual). |
| `sep_oe_shrunk` | double | sep_oe_raw after per-season empirical-Bayes shrinkage toward the target-weighted league mean (sampling variance identified from weekly rows). |
| `reliability` | double | Shrinkage reliability tau2 / (tau2 + sigma2 / targets) in [0, 1]; the fraction of the raw deviation retained. |
| `sep_oe_rank` | integer | Dense descending rank of sep_oe_shrunk within season over qualified rows; null when targets < min_targets. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_separation_oe
df = nfl_ngs_separation_oe([2023])
print(df.sort("sep_oe_rank").head())

# Pandas output

df_pd = nfl_ngs_separation_oe(2023, return_as_pandas=True)
```

### nfl_ngs_statboard {#nfl_ngs_statboard}

`nfl_ngs_statboard(stat_type: 'str' = 'passing', season: 'int' = 2024, season_type: 'str' = 'REG', week: 'Optional[int]' = None, return_as_pandas: 'bool' = False)`

NGS season/week statboard leaderboard for a stat family (one row per player).

Wraps `/api/statboard/{passing,receiving,rushing}`. Each record is a flat
per-player stat line (e.g. for passing: `completionPercentageAboveExpectation`,
`avgTimeToThrow`, `aggressiveness`, `passerRating` ...). The player's bio
is nested under a `player` object and is flattened to `player_*` columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stat_type` | `str` | `'passing'` | one of `"passing"`, `"receiving"`, `"rushing"`. (For the cross-stat highlight board use `nfl_ngs_statboard_leaders`.) |
| `season` | `int` | `2024` | season year, e.g. `2024`. |
| `season_type` | `str` | `'REG'` | `"REG"`, `"POST"`, or `"PRE"`. |
| `week` | `int \| None` | `None` | single week to filter to; `None` returns the full-season board. |
| `return_as_pandas` | `bool` | `False` | return a pandas frame instead of polars. |

**Returns**

A polars (or pandas) `DataFrame`, one row per qualifying player.

| col_name | type | description |
|---|---|---|
| `aggressiveness` | double | Aggressiveness tracks the amount of passing attempts a quarterback makes that are into tight coverage, where there is a defender within 1 yard or less of the receiver at the time of completion or incompletion. AGG is shown as a % of attempts into tight windows over all passing attempts. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `avgAirDistance` | double | Average air distance in yards on all pass attempts, measuring how far the ball travels through the air regardless of direction. |
| `avgAirYardsDifferential` | double | Average difference between intended air yards and completed air yards, measuring accuracy relative to target depth. |
| `avgAirYardsToSticks` | double | Average air yards relative to the first-down marker on pass attempts, where positive values indicate throws beyond the sticks. |
| `avgCompletedAirYards` | double | Average air yards on completed passes only, measuring depth of actual completions. |
| `avgIntendedAirYards` | double | Average depth of target on all pass attempts, regardless of completion. |
| `avgTimeToThrow` | double | Average time in seconds from snap to release for the passer, as tracked by NFL Next Gen Stats. |
| `completionPercentage` | double | Actual completion percentage for the passer during the period covered. |
| `completionPercentageAboveExpectation` | double | Completion percentage above the model-expected completion rate (CPAE), measuring accuracy relative to difficulty of throws attempted. |
| `completions` | integer | The number of completed passes. |
| `expectedCompletionPercentage` | double | Model-expected completion percentage based on factors such as target depth, separation, and defensive coverage as calculated by NFL Next Gen Stats. |
| `gamesPlayed` | integer | Number of games played by the passer during the period covered. |
| `interceptions` | integer | The number of interceptions thrown. |
| `maxAirDistance` | double | Maximum air distance in yards recorded on any single pass attempt during the period covered. |
| `maxCompletedAirDistance` | double | Maximum air distance in yards recorded on any single completed pass during the period covered. |
| `passTouchdowns` | integer | Total passing touchdowns thrown by the passer during the period covered. |
| `passYards` | integer | Total passing yards accumulated by the passer during the period covered. |
| `passerRating` | double | NFL passer rating (0–158.3 scale) for the passer during the period covered. |
| `playerName` | character | Display name of the passer as returned at the top-level statboard row. |
| `season` | integer | 4 digit number indicating to which season(s) the specified timeframe belongs to. |
| `seasonType` | character | Season segment covered by this statboard record (e.g., 'REG' for regular season, 'POST' for playoffs). |
| `position` | character | Primary position as reported by NFL.com |
| `teamId` | character | NFL Next Gen Stats team identifier for the passer's team at the time of this record. |
| `player_season` | integer | NFL season year associated with this player's statboard record. |
| `player_currentTeamId` | character | NFL Next Gen Stats team identifier for the team the player is currently rostered on. |
| `player_displayName` | character | Full display name of the player as used in NFL Next Gen Stats records. |
| `player_esbId` | character | ESPN-Elias Sports Bureau (ESB) identifier for the player. |
| `player_firstName` | character | First name of the player. |
| `player_footballName` | character | Football name used by the player, which may differ from the legal first name. |
| `player_gsisId` | character | NFL GSIS (Game Statistics and Information System) identifier, the primary nflverse player key. |
| `player_gsisItId` | integer | NFL GSIS internal tracking integer identifier for the player. |
| `player_headshot` | character |  |
| `player_jerseyNumber` | integer | Jersey number worn by the player. |
| `player_lastName` | character | Last name of the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `player_positionGroup` | character | Broad position group for the player (e.g., 'QB', 'WR') in the NGS player record. |
| `player_shortName` | character | Shortened display name for the player (e.g., 'P.Mahomes'). |
| `player_smartId` | character | NFL Next Gen Stats smart (UUID-style) identifier for the player. |
| `player_status` | character | Current roster status of the player (e.g., 'ACT' for active, 'IR' for injured reserve). |
| `player_uniformNumber` | character | Uniform number worn by the player, stored as a string to preserve leading zeros if applicable. |
| `player_ngsPosition` | character | Player's position as classified by the NFL Next Gen Stats system. |
| `player_ngsPositionGroup` | character | Broader position group the player belongs to as classified by the NFL Next Gen Stats system. |

**Example**

```python
from sportsdataverse.nfl import nfl_ngs_statboard
qb = nfl_ngs_statboard(stat_type="passing", season=2024, season_type="REG")
qb.select(["playerName", "passerRating", "completionPercentageAboveExpectation"]).head()
```

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

### nfl_play_call_probabilities {#nfl_play_call_probabilities}

`nfl_play_call_probabilities(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None, *, models_dir: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Score the bundled play-call classifier over offensive plays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp (must carry `xpass`; run `sportsdataverse.nfl.ep_wp.calculate_xpass` first if not). |
| `participation` | `Optional[DataFrame]` | `None` | Optional participation frame for personnel features. |
| `models_dir` | `Optional[str]` | `None` | Optional directory holding `nfl_playcall.ubj` (defaults to the bundled package artifact; no first-use download). |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Keys + per-family probabilities `p_inside_run` / `p_outside_run` / `p_short_pass` / `p_deep_pass` / `p_scramble`, `p_pass` (pass-family sum), `pred_family` (argmax) and `pass_oe_model` (`100 * (is_pass - p_pass)`). Empty input yields a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `game_id` | character | nflverse game identifier (Utf8 join key). |
| `play_id` | integer | nflverse play identifier within the game (Int64 join key). |
| `season` | integer | Season of the play. |
| `week` | integer | Week of the play. |
| `posteam` | character | Offense (possession) team abbreviation. |
| `p_inside_run` | double | Predicted probability of an inside run (guard/center gap or middle). |
| `p_outside_run` | double | Predicted probability of an outside run (end/tackle or off-middle). |
| `p_short_pass` | double | Predicted probability of a short pass. |
| `p_deep_pass` | double | Predicted probability of a deep pass. |
| `p_scramble` | double | Predicted probability of a QB scramble. |
| `p_pass` | double | Predicted pass probability (short + deep + scramble family sum). |
| `pred_family` | character | Argmax family among the five class probabilities. |
| `pass_oe_model` | double | Pass-rate over model expectation for the play, 100 * (is_pass - p_pass). |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import nfl_play_call_probabilities
out = nfl_play_call_probabilities(calculate_xpass(load_nfl_pbp([2023])))
print(out.select("p_pass", "pred_family").head())

# Pipeline next step

out.group_by("posteam").agg(pl.col("p_pass").mean()).sort("p_pass")
```

### nfl_play_call_tendencies {#nfl_play_call_tendencies}

`nfl_play_call_tendencies(pbp: 'pl.DataFrame', participation: 'Optional[pl.DataFrame]' = None, *, models_dir: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Aggregate scored play-call probabilities to team-season tendencies.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp (with `xpass`). |
| `participation` | `Optional[DataFrame]` | `None` | Optional participation frame. |
| `models_dir` | `Optional[str]` | `None` | Optional directory holding `nfl_playcall.ubj`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, posteam)`: `plays`, `mean_p_pass`, `pass_rate`, `proe` (`100 * (pass_rate - mean_p_pass)`) and the family mix shares `share_<family>`. Empty input yields a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `posteam` | character | Offense team abbreviation. |
| `plays` | integer | Offensive run/pass plays scored. |
| `mean_p_pass` | double | Mean model pass probability across the team's plays. |
| `pass_rate` | double | Actual pass rate (scrambles count as passes). |
| `proe` | double | Pass rate over expected, 100 * (pass_rate - mean_p_pass). |
| `share_inside_run` | double | Share of plays labeled inside_run. |
| `share_outside_run` | double | Share of plays labeled outside_run. |
| `share_short_pass` | double | Share of plays labeled short_pass. |
| `share_deep_pass` | double | Share of plays labeled deep_pass. |
| `share_scramble` | double | Share of plays labeled scramble. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.ep_wp import calculate_xpass
from sportsdataverse.nfl.nfl_playcall import nfl_play_call_tendencies
t = nfl_play_call_tendencies(calculate_xpass(load_nfl_pbp([2023])))
print(t.sort("proe", descending=True).head())
```

### nfl_player_projection {#nfl_player_projection}

`nfl_player_projection(seasons: 'List[int]', target_season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Marcel-style next-season player projection with delta-method aging.

Loads weekly player stats + rosters, aggregates to season rates, and for
every player visible in seasons **strictly before** `target_season`
(the as-of-date leakage boundary) produces a recency-weighted rate blend
regressed toward the volume-weighted position mean by
`k / (k + reliability)`, scaled by the position aging-curve ratio
`aging_mult(proj_age) / aging_mult(current_age)`. The aging curve is fit
only on the same pre-target history.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  | History seasons to load (seasons `>= target_season` are discarded by the leakage split). |
| `target_season` | `int` |  | The season being projected. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. |

**Returns**

One row per projected player: `player_id:Utf8, target_season:Int64, position_group:Utf8, proj_age:Float64, proj_ppg:Float64, proj_volume:Float64, proj_games:Float64, aging_mult:Float64, reliability:Float64` plus `proj_<stat>_rate` component-rate columns. Empty history returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | character | nflverse gsis player id (character join key). |
| `target_season` | integer | The season being projected (features use strictly earlier seasons only - the as-of-date leakage boundary). |
| `position_group` | character | nflverse offensive position group (QB/RB/WR/TE plus fringe groups). |
| `proj_age` | double | Projected age at the target season (age at last visible season + season gap). |
| `proj_ppg` | double | Projected PPR fantasy points per game - recency-weighted rate blend regressed toward the volume-weighted position mean by k/(k + reliability), scaled by the damped aging-curve ratio. |
| `proj_volume` | double | Projected position-specific opportunity volume (QB = pass attempts, RB = carries + targets, WR/TE = targets). |
| `proj_games` | double | Recency-weighted mean of historical games played. |
| `aging_mult` | double | Applied aging multiplier - the damped, clamped ratio aging_curve(proj_age) / aging_curve(current_age). |
| `reliability` | double | Recency-weighted volume sum - the shrinkage evidence weight. |
| `proj_completions_rate` | double | Projected per-game pass completions (Marcel blend x aging ratio). |
| `proj_attempts_rate` | double | Projected per-game pass attempts (Marcel blend x aging ratio). |
| `proj_passing_yards_rate` | double | Projected per-game passing yards (Marcel blend x aging ratio). |
| `proj_passing_tds_rate` | double | Projected per-game passing touchdowns (Marcel blend x aging ratio). |
| `proj_interceptions_rate` | double | Projected per-game interceptions thrown (Marcel blend x aging ratio). |
| `proj_carries_rate` | double | Projected per-game rush attempts (Marcel blend x aging ratio). |
| `proj_rushing_yards_rate` | double | Projected per-game rushing yards (Marcel blend x aging ratio). |
| `proj_rushing_tds_rate` | double | Projected per-game rushing touchdowns (Marcel blend x aging ratio). |
| `proj_receptions_rate` | double | Projected per-game receptions (Marcel blend x aging ratio). |
| `proj_targets_rate` | double | Projected per-game targets (Marcel blend x aging ratio). |
| `proj_receiving_yards_rate` | double | Projected per-game receiving yards (Marcel blend x aging ratio). |
| `proj_receiving_tds_rate` | double | Projected per-game receiving touchdowns (Marcel blend x aging ratio). |
| `proj_receiving_air_yards_rate` | double | Projected per-game receiving air yards (Marcel blend x aging ratio). |
| `proj_fumbles_lost_rate` | double | Projected per-game fumbles lost (Marcel blend x aging ratio). |

**Example**

```python
from sportsdataverse.nfl.nfl_projection import nfl_player_projection
proj = nfl_player_projection([2021, 2022, 2023], 2024)
proj.sort("proj_ppg", descending=True).head()

# Pandas round-trip

proj_pd = nfl_player_projection([2021, 2022, 2023], 2024, return_as_pandas=True)
```

### nfl_player_props {#nfl_player_props}

`nfl_player_props(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, era: 'str' = 'modern', lines: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Empirical-Bayes player-prop projections, leakage-safe per week.

For every game in the requested season(s) (or, with `as_of_date`, every
game on/after that date), projects each rostered QB/RB/WR/TE's stat-family
mean as `usage x efficiency x matchup x game-script`:

- usage + efficiency from `player_usage_efficiency` built **as-of
  that game's week** (weeks strictly before it),
- the matchup multiplier from the opponent's `adj_def_epa` in
  `sportsdataverse.nfl.nfl_ratings.nfl_ratings` (as-of the week's
  first game date),
- game script from the **native** expected margin
  (`sportsdataverse.nfl.nfl_market.nfl_predict_games`) -- the
  market line is never read (binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Season (e.g. `2023`) or list of seasons. |
| `as_of_date` | `date \| None` | `None` | When given, only games with `gameday >= as_of_date` are projected (history before each game's week still feeds the projections). `None` projects every week of the season(s). |
| `era` | `str` | `'modern'` | Constants era key. |
| `lines` | `DataFrame \| None` | `None` | Optional market lines to score `p_over` against -- columns `game_id` / `player_id` / `stat` (Utf8) + `line` (Float64), e.g. built from `espn_nfl_game_propbets` (ESPN only serves propbets for upcoming games). `None` leaves `line` / `p_over` null. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per (player-game, stat): `season` / `week` (Int64), `game_id` / `player_id` / `position` / `team_id` / `opp_team_id` / `stat` (Utf8), `proj_mean` / `proj_sd` / `line` / `p_over` (Float64; `p_over = 1 - Phi((line - proj_mean) / proj_sd)` when a line is joined, else null). Stats are `passing_yards` (QB), `rushing_yards` (RB), `receiving_yards` (WR/TE). Zero-row, correctly-typed when there is nothing to project.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the projection belongs to. |
| `week` | integer | Week of the projected game; only weeks strictly before it feed the projection. |
| `game_id` | character | Game identifier from the schedule (nflverse id, e.g. "2023_06_DET_TB"). |
| `player_id` | character | nflverse GSIS player identifier (character join key). |
| `position` | character | Player position (QB, RB, WR, TE) selecting the projected stat family. |
| `team_id` | character | Player's team nflverse abbreviation as of the projection week. |
| `opp_team_id` | character | Opponent team nflverse abbreviation (drives the matchup multiplier). |
| `stat` | character | Projected stat name (passing_yards for QB, rushing_yards for RB, receiving_yards for WR/TE). |
| `proj_mean` | double | Projected stat mean - EB-shrunk usage x efficiency x opponent matchup x game-script. |
| `proj_sd` | double | Residual standard deviation for the stat family (fitted on the 2023 as-of backtest). |
| `line` | double | Market prop line joined from the caller-supplied lines frame (e.g. espn_nfl_game_propbets); null when no line is available. |
| `p_over` | double | Probability the player exceeds `line`, 1 - Phi((line - proj_mean) / proj_sd); null without a line. |

**Example**

```python
from sportsdataverse.nfl import nfl_player_props
props = nfl_player_props(2023)
props.filter(props["stat"] == "passing_yards").head()

# Upcoming-only, as-of a date

import datetime as dt
props = nfl_player_props(2024, as_of_date=dt.date(2024, 11, 1))
```

### nfl_players_crosswalk {#nfl_players_crosswalk}

`nfl_players_crosswalk(*, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Pure-consumer ID crosswalk sliced from `load_nfl_players`.

Reads nflverse's published players master and projects it down to just the
cross-system identifier columns it carries (`gsis_id`, `esb_id`,
`espn_id`, `pfr_id`, `pff_id`, `otc_id`, `nfl_id`, `smart_id` —
whichever the parquet exposes) plus `full_name` and `position`, deduped
on `gsis_id`. The players master has no Yahoo or CBS ids, so `yahoo_id`
and `cbs_id` are joined on `gsis_id` from
`load_nfl_ff_playerids` (DynastyProcess). A `gsis_id` that
DynastyProcess lists twice is ambiguous upstream and gets null provider ids.
It is a convenience for joining identity IDs onto PBP / rosters / stats
frames without carrying the full ~40-column master.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If `True`, return a `pandas.DataFrame`; otherwise a `polars.DataFrame` (default). |

**Returns**

A one-row-per-`gsis_id` `DataFrame` of cross-system IDs (all `Utf8`) + `full_name` / `position`, with `yahoo_id` / `cbs_id` null where DynastyProcess has no unambiguous match (or its load fails). A failed / empty players load yields a zero-row frame carrying the same column set (never a raise).

**Example**

```python
from sportsdataverse.nfl import nfl_players_crosswalk
xwalk = nfl_players_crosswalk()
print(xwalk.columns)

# Join nflverse IDs onto a PBP frame (one line)

pbp.join(nfl_players_crosswalk(), left_on="passer_player_id", right_on="gsis_id", how="left")
```

### nfl_predict_games {#nfl_predict_games}

`nfl_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', odds: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Vectorized pregame predictions (+ display-only market edge) per game.

Joins `ratings` twice (home/away) onto the schedule and computes the
three closed-form predictions. `odds` is **display-only**: it feeds
`market_edge = exp_margin - close_spread_home` and never the
predictions themselves (the binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game: `game_id` (Utf8), `home_team_id` / `away_team_id` (Utf8 team abbreviations), `neutral_site` (Boolean). |
| `ratings` | `DataFrame` |  | The `sportsdataverse.nfl.nfl_ratings.nfl_ratings` output (needs `team_id`, `adj_off_epa`, `adj_def_epa`, `adj_net`). |
| `era` | `str` | `'modern'` | Constants era key. |
| `odds` | `DataFrame \| None` | `None` | Optional market frame (`game_id`, `close_spread_home` -- the market's expected home margin, positive = home favored). Games absent from `odds` get a null `market_edge`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

One row per input game: `game_id` / `home_team_id` / `away_team_id` (Utf8), `neutral_site` (Boolean), `exp_margin` / `home_win_prob` / `exp_total` / `market_edge` (Float64; `market_edge` null without odds). Zero-row, correctly-typed on empty input.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Game identifier carried through from the input schedule. |
| `home_team_id` | character | Home team nflverse abbreviation (character; the ratings `team_id` join key). |
| `away_team_id` | character | Away team nflverse abbreviation (character; the ratings `team_id` join key). |
| `neutral_site` | logical | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | double | Expected home scoring margin in points (points_per_net * net rating differential + the fitted home-field advantage on non-neutral fields). |
| `home_win_prob` | double | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | double | Expected combined point total (avg_total + total_scale * the four-way efficiency matchup sum). |
| `market_edge` | double | Display-only native-minus-market spread edge (exp_margin - close_spread_home); null when no odds frame is supplied. |

**Example**

```python
from sportsdataverse.nfl import nfl_ratings
from sportsdataverse.nfl.nfl_market import nfl_predict_games
ratings = nfl_ratings(2023)
preds = nfl_predict_games(games, ratings)
preds.sort("home_win_prob", descending=True).head()

# With a market edge (display only)

preds = nfl_predict_games(games, ratings, odds=odds)
```

### nfl_punter_value {#nfl_punter_value}

`nfl_punter_value(seasons: 'Union[int, List[int]]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Punter net-field-position value over expected.

Expected net comes from the shipped punt landing distribution
(`nfl_fourth_down._load_punt_data`) evaluated at each punt's line of
scrimmage; realized net is `kick_distance - return_yards - 20*touchback`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int]]` |  | Season or list of seasons. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

Per `(season, punter_player_id)`: `punts`, `gross_avg`, `net_avg`, `exp_net_avg`, `net_over_expected`, `epa`. Empty seasons yield a zero-row frame with this schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `punter_player_id` | character | nflverse punter GSIS id (Utf8 join key). |
| `punts` | integer | Punts with a recorded kick distance. |
| `gross_avg` | double | Mean gross punt distance (yards). |
| `net_avg` | double | Mean net distance, kick_distance - return_yards - 20 * touchback. |
| `exp_net_avg` | double | Mean expected net from the shipped punt landing distribution at each punt's line of scrimmage. |
| `net_over_expected` | double | net_avg minus exp_net_avg (yards of field position per punt over expectation). |
| `epa` | double | Total EPA on the punter's punt plays (kicking-team perspective). |

**Example**

```python
from sportsdataverse.nfl.nfl_special_teams import nfl_punter_value
pv = nfl_punter_value([2023])
print(pv.head())
```

### nfl_ratings {#nfl_ratings}

`nfl_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the native NFL ratings spine (off/def/ST EPA).

Public orchestrator over `efficiency_ratings` +
`special_teams_ratings`. Loads play-by-play + schedule via
`load_nfl_pbp` / `load_nfl_schedule`, joins each game's `gameday`
onto the plays, optionally applies the as-of-date leakage boundary
(only plays from games with `gameday < as_of_date` are used), then
fits both components and reshapes into one wide per-team table with
dense ranks and a net z-score.

The loaded pbp is down-selected to the ridge columns *before* any fit so
no market column (`spread_line` / `vegas_wp`) can leak into the
ratings (the binding non-market boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons pooled into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, only plays from games strictly before this date are used (mirrors what was knowable heading into that date). `None` (default) uses the full season(s). |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs forwarded to both component fits; defaults to `RatingsConfig`. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame. |

**Returns**

A DataFrame with one row per `team_id`: `season` (Int64 -- the single passed season, `null` for a pooled multi-season call), `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_st_epa` / `adj_net` (Float64; `adj_net` is offense minus defense -- special teams stays a separate column), `games` (Int64), `off_rank` / `def_rank` / `net_rank` (Int64; `def_rank` ascends -- fewer EPA allowed ranks better), `net_z` (Float64). Zero-row, correctly-typed when the seasons have no data or `as_of_date` filters out every play.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the ratings cover (null for a pooled multi-season fit). |
| `team_id` | character | nflverse team abbreviation (character join key, e.g. "KC"). |
| `adj_off_epa` | double | Opponent-adjusted offensive EPA per play (higher is better); competitive-play ridge fit. |
| `adj_def_epa` | double | Opponent-adjusted defensive EPA allowed per play (lower is better); competitive-play ridge fit. |
| `adj_st_epa` | double | Opponent-adjusted special-teams EPA per play (ridge on special==1 plays; 0.0 for teams with no special-teams plays in the window). |
| `adj_net` | double | Opponent-adjusted net efficiency (adj_off_epa minus adj_def_epa; special teams not folded in). |
| `games` | integer | Number of games the team played in the fitted window. |
| `off_rank` | integer | Dense rank on adj_off_epa descending (best offense = 1). |
| `def_rank` | integer | Dense rank on adj_def_epa ascending (fewer EPA allowed ranks better). |
| `net_rank` | integer | Dense rank on adj_net descending (best net rating = 1). |
| `net_z` | double | Z-score of adj_net across the 32 teams. |

**Example**

```python
from sportsdataverse.nfl import nfl_ratings
ratings = nfl_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week6 = nfl_ratings(2023, as_of_date=dt.date(2023, 10, 12))
```

### nfl_season_standings {#nfl_season_standings}

`nfl_season_standings(games: 'pl.DataFrame', *, ranks: 'str' = 'CONF', tiebreaker_depth: 'str' = 'SOS', playoff_seeds: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Compute NFL standings with the real NFL tiebreaking procedures.

Faithful polars port of `nflseedR::nfl_standings()` (v2 engine,
`R/standings.R` L82-155): initializes records, points, win
percentages, SOV and SOS from a games frame, then resolves division
ranks, conference ranks (playoff seeds) and draft order through the
full NFL tiebreaker cascades.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Games frame with one row per game. Required columns: `sim` or `season` (identifier), `game_type` (`'REG'`, `'WC'`, `'DIV'`, `'CON'`, `'SB'`), `week`, `away_team`, `home_team`, and `result` (home score minus away score; no missing values allowed). `away_score` / `home_score` are additionally required for `tiebreaker_depth='POINTS'` and enable the `pf`/`pa`/`pd` output columns. |
| `ranks` | `str` | `'CONF'` | One of `'DIV'`, `'CONF'` (default), `'DRAFT'`, or `'NONE'` — which rank columns (and thus tiebreakers) to compute. `'DRAFT'` implies `'CONF'` implies `'DIV'`. |
| `tiebreaker_depth` | `str` | `'SOS'` | One of `'SOS'` (default), `'PRE-SOV'`, `'POINTS'`, or `'RANDOM'`. Controls how deep the tiebreaker cascade goes before falling back to a coin toss. |
| `playoff_seeds` | `Optional[int]` | `None` | If not `None`, only conference ranks up to this value are resolved with tiebreakers; deeper ranks are returned as null. Must be in 1-16. |
| `return_as_pandas` | `bool` | `False` | If `True`, return a pandas DataFrame. |

**Returns**

A standings frame with one row per (sim/season, team) including records, `win_pct`/`div_pct`/`conf_pct`, `sov`, `sos`, and the requested `div_rank`/`conf_rank`/`draft_rank` columns plus `*_tie_broken_by` bookkeeping. `conf_rank` is the playoff seed.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season identifier from the input games frame (named `sim` instead when the input used a `sim` column). |
| `conf` | character | Conference of the team (AFC or NFC). |
| `division` | character | Division of the team (e.g. "AFC East"). |
| `team` | character | Team abbreviation. |
| `games` | integer | Number of regular season games played. |
| `wins` | double | Regular season wins with ties counted as half a win. |
| `true_wins` | integer | Regular season wins excluding ties (outright wins only). |
| `losses` | integer | Regular season losses. |
| `ties` | integer | Regular season ties. |
| `pf` | integer | Points scored across regular season games (points for); present only when the input carries home_score and away_score. |
| `pa` | integer | Points allowed across regular season games (points against); present only when the input carries scores. |
| `pd` | integer | Regular season point differential (pf minus pa); present only when the input carries scores. |
| `win_pct` | double | Regular season win percentage with ties counted as half a win. |
| `div_pct` | double | Win percentage in games against division opponents (0 when the team played no division games). |
| `conf_pct` | double | Win percentage in games against conference opponents (0 when the team played no conference games). |
| `sov` | double | Strength of victory - combined win percentage of all opponents the team defeated (0 for winless teams). |
| `sos` | double | Strength of schedule - combined win percentage of all opponents the team faced. |
| `div_rank` | integer | Rank within the division (1-4) after applying the NFL division tiebreaking procedures. |
| `div_tie_broken_by` | character | Tiebreaker step that resolved the team's division rank (e.g. "Head-To-Head Win PCT (2)" or "Coin Toss"); null when the rank needed no tiebreaker. |
| `conf_rank` | integer | Conference rank, i.e. the playoff seed, after applying the NFL conference tiebreaking procedures; null beyond `playoff_seeds` when that argument is set. |
| `conf_tie_broken_by` | character | Tiebreaker step that resolved the team's conference rank; null when the rank needed no tiebreaker. |
| `exit` | character | Round of the team's final game - REG, WC, DIV, CON, SB, or SB_WIN for the Super Bowl winner (returned with ranks="DRAFT"). |
| `draft_rank` | integer | Draft pick position (1 = first overall pick) derived from postseason exit, win percentage, SOS and the draft tiebreaking procedures (returned with ranks="DRAFT"). |
| `draft_tie_broken_by` | character | Tiebreaker step that resolved the team's draft rank; null when the rank needed no tiebreaker. |

**Example**

```python
import sportsdataverse.nfl as nfl
games = nfl.load_schedules([2024])
standings = nfl.nfl_season_standings(games, ranks="DRAFT")
print(standings.shape)

# Playoff seeds only, pandas output

df = nfl.nfl_season_standings(
    games, ranks="CONF", playoff_seeds=7, return_as_pandas=True
)

# Pipeline next step (one line)

standings.filter(pl.col("conf_rank") <= 7).sort("conf", "conf_rank")
```
