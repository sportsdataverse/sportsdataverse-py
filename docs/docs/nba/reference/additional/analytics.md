---
title: "NBA — additional Python functions — Analytics: add_ctg–nba_box"
sidebar_label: "Analytics: add_ctg–nba_box"
sidebar_position: 15
description: "NBA — additional Python functions — Analytics: add_ctg–nba_box — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: add_ctg–nba_box

### add_ctg_shot_zones {#add_ctg_shot_zones}

`add_ctg_shot_zones(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Append CTG's shot-location zone (`ctg_shot_zone`) to an enhanced PBP frame.

CTG's zones differ from the official NBA zones emitted by
`~sportsdataverse.nba.nba_shot_zones.add_shot_zones`: CTG splits the
midrange at the free-throw-line distance rather than at the paint boundary.

* `at_rim` — shot distance < 4 ft ("Shots within 4 feet of the basket").
* `short_mid` — 4 ft <= distance < 14 ft ("outside of 4 feet, but inside of
  ~14 feet (the free throw line distance)").
* `long_mid` — >= 14 ft, inside the arc.
* `corner_3` — a three "below the break" (`|x_legacy| >= 220` and
  `y_legacy <= 87.5`).
* `arc_3` — any other three (CTG's "non-corner three").

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. |

**Returns**

The input frame with a `ctg_shot_zone` Utf8 column appended (null on non-field-goal rows). Empty input returns a zero-row frame carrying the column — never raises.

| col_name | type | description |
|---|---|---|
| `order_index` | Int64 |  |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `clock` | String | Game clock value. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `player_name_i` | String | Player name i. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int64 | 1 if the action was a field goal; 0 otherwise. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `points_total` | Int64 | Running total of points scored. |
| `location` | String | Location. |
| `description` | String | Long-form description text. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `video_available` | Int64 | Video available. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `action_id` | Int64 | Unique action identifier within a game (V3 PBP). |
| `game_id` | String | Unique game identifier. |
| `seconds_remaining` | Float64 | Seconds remaining in the period. |
| `event_type` | String | Event / play type code (V2 PBP). |
| `is_made_shot` | Boolean |  |
| `is_missed_shot` | Boolean |  |
| `is_free_throw` | Boolean |  |
| `is_rebound` | Boolean |  |
| `is_turnover` | Boolean |  |
| `is_foul` | Boolean |  |
| `is_substitution` | Boolean |  |
| `is_jump_ball` | Boolean |  |
| `is_timeout` | Boolean |  |
| `is_period` | Boolean |  |
| `ctg_shot_zone` | String |  |

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_ctg_shot_zones
pbp = add_ctg_shot_zones(enhanced_pbp_from_payload(payload))
print(pbp.filter(pl.col("ctg_shot_zone").is_not_null())["ctg_shot_zone"].value_counts())
```

### add_play_context {#add_play_context}

`add_play_context(enhanced_pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', starters_on_court: 'Optional[dict[int, int]]' = None) -> 'pl.DataFrame'`

Build possessions and enrich them with the full CTG play-context surface.

One call: `~sportsdataverse.nba.nba_possessions.build_possessions` ->
`add_start_type_detail` -> `add_transition` ->
`flag_heave_possessions` -> `flag_garbage_time`.

The CTG filter columns are **flags, not filters** — nothing is dropped. Apply

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`. |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 6.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional starters-on-floor counts; see `flag_garbage_time`. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus every column in `PLAY_CONTEXT_POSSESSIONS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `possession_start_type_detail` | String |  |
| `possession_start_type_ctg` | String |  |
| `seconds_to_first_play` | Float64 |  |
| `is_transition_ctg` | Boolean | Cleaning the Glass-style transition (renamed from is_transition), true when the possession's initial play (first shot, free-throw trip or turnover) lands within transition_seconds (default 6) of its start, under the chosen variant; never on a period's first possession or after a timeout. Not the possessions table's is_transition (add_possession_flags), which uses an 8-second first-shot window. |
| `is_transition` | Boolean | Deprecated copy of is_transition_ctg under its old name; read is_transition_ctg (removed in 0.4.0). Not the possessions table's is_transition (add_possession_flags). |
| `transition_source` | String |  |
| `possession_context` | String |  |
| `is_heave_possession` | Boolean |  |
| `is_garbage_time_ctg` | Boolean | Cleaning the Glass garbage time (renamed from is_garbage_time), true in the 4th quarter with a margin at the possession's start of 25+ in minutes 12-9, 20+ in 9-6 or 10+ after, and, when starters_on_court is given, two or fewer starters on the floor (garbage_time_basis says which). Not the possessions table's is_garbage_time (add_possession_flags, bigballR's rule). |
| `is_garbage_time` | Boolean | Deprecated copy of is_garbage_time_ctg under its old name; read is_garbage_time_ctg (removed in 0.4.0). Not the possessions table's is_garbage_time (add_possession_flags). |
| `garbage_time_basis` | String |  |

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_play_context
poss = add_play_context(enhanced_pbp_from_payload(payload))
print(poss["possession_start_type_ctg"].value_counts())
```

### add_possession_flags {#add_possession_flags}

`add_possession_flags(possessions: 'pl.DataFrame', pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 8.0) -> 'pl.DataFrame'`

Append `seconds_to_first_shot`, `is_transition` and `is_garbage_time` to possessions.

Works for the NBA and the WNBA, on any number of games at once. `possessions` is the
`nba_stats_possessions` / `wnba_stats_possessions` frame (or the producer's
`attach_possession_lineups` output) and `pbp` the play-by-play it was built from:
the `nba_stats_pbp` release or the enhanced pbp frame. The input's columns, values,
row count and row order are unchanged; only the three columns are appended.

**seconds_to_first_shot** (Float64): seconds from the change of possession to the
possession's first field-goal attempt. The change of possession is the clock at the
previous possession's last event (`start_seconds_remaining` is the clock at this
possession's first event, which after a rebound is often the shot itself). A period's
first possession counts from its own first event. Null when the possession has no field
goal attempt (turnovers, free-throw-only trips). It can pass 24 when the shot clock
reset inside the possession (a kicked ball, a foul).

**is_transition** (Boolean, never null): `possession_start_type` is `OffMadeShot`,
`OffMissedShot` or `OffLiveBallTurnover`, the possession is not its period's first,
and `seconds_to_first_shot <= transition_seconds` (8 by default, the window that
tracks Synergy's `Transition` play type; college's is 10).

**is_garbage_time** (Boolean, never null): bigballR's rule, the one behind the college
flag. A possession triggers it when any of its events has an absolute score margin of
at least 25 with 10:00 or less left in regulation, at least 20 with 5:00 or less, or at
least 15 with 2:00 or less (or at least 15 in overtime), with at most 3 of the game's 10
starters on the floor (the first possession's 10). Once a game triggers, it stays
garbage time to the end. The module docstring lists where this differs from bigballR
and why the triggering possession can qualify on its own basket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Every possession of each game it holds (the previous possession's clock and the first possession's starters are read from it, so do not filter it to `count_as_possession` or to one team first). Needs `game_id`, `period`, `possession_number`, `number_in_period`, `start_order_index`, `end_order_index`, `start_seconds_remaining`, `end_seconds_remaining`, `possession_start_type` and `off_player_1..5` / `def_player_1..5`. |
| `pbp` | `DataFrame` |  | The play-by-play of those games. Needs `game_id`, `order_index`, `event_type`, `seconds_remaining`, `score_home` and `score_away` (Utf8 or integer; the margin is the running score after each event). |
| `transition_seconds` | `float` | `8.0` | The transition window in seconds. Default 8.0 (`TRANSITION_SECONDS`). |

**Returns**

`possessions` with `POSSESSION_FLAGS_SCHEMA` appended. An empty frame returns zero rows with those columns.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 | NBA Stats team id of the defending team for the possession. |
| `start_order_index` | Int64 | order_index of the play-by-play event that starts the possession. |
| `end_order_index` | Int64 | order_index of the play-by-play event that ends the possession. |
| `start_seconds_remaining` | Float64 | Seconds remaining in the period when the possession started. |
| `end_seconds_remaining` | Float64 | Seconds remaining in the period when the possession ended. |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean | Whether the row is a second-chance continuation following an offensive rebound. |
| `number_in_period` | Int64 | Possession number within the period, both teams counted, resetting to 1 at each period start. |
| `possession_start_type` | String | How the possession began (e.g. off a made shot, defensive rebound, turnover, or period start). |
| `count_as_possession` | Boolean | Whether the row counts as a true possession for per-possession rate stats. |
| `fg2a` | Int64 | Two-point field goal attempts during the possession. |
| `fg2m` | Int64 | Two-point field goals made during the possession. |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 | NBA Stats player id of offensive on-court player 1 of 5 for the possession. |
| `off_player_2` | Int64 | NBA Stats player id of offensive on-court player 2 of 5 for the possession. |
| `off_player_3` | Int64 | NBA Stats player id of offensive on-court player 3 of 5 for the possession. |
| `off_player_4` | Int64 | NBA Stats player id of offensive on-court player 4 of 5 for the possession. |
| `off_player_5` | Int64 | NBA Stats player id of offensive on-court player 5 of 5 for the possession. |
| `def_player_1` | Int64 | NBA Stats player id of defensive on-court player 1 of 5 for the possession. |
| `def_player_2` | Int64 | NBA Stats player id of defensive on-court player 2 of 5 for the possession. |
| `def_player_3` | Int64 | NBA Stats player id of defensive on-court player 3 of 5 for the possession. |
| `def_player_4` | Int64 | NBA Stats player id of defensive on-court player 4 of 5 for the possession. |
| `def_player_5` | Int64 | NBA Stats player id of defensive on-court player 5 of 5 for the possession. |
| `lineup_source` | String | Provenance of the on-court lineup identification for the row (how the five-man units were resolved). |
| `season` | Int64 | Season year. |
| `seconds_to_first_shot` | Float64 | Seconds from the change of possession (the clock at the previous possession's last event) to the possession's first field-goal attempt; null when it has no field-goal attempt. |
| `is_transition` | Boolean | True when possession_start_type is OffMadeShot, OffMissedShot or OffLiveBallTurnover, the possession is not its period's first, and seconds_to_first_shot is at most transition_seconds (default 8, the window that tracks Synergy's Transition play type; college's is 10). Never null. |
| `is_garbage_time` | Boolean | True from the possession that first meets bigballR's garbage-time rule (the college ncaa_*_possessions rule) to the end of the game. At some event of the possession the absolute margin is 25+ with 10:00 or less left in regulation, 20+ with 5:00 or less, 15+ with 2:00 or less, or 15+ in overtime, and at most 3 of the game's 10 starters (the first possession's players) are on the floor. The triggering possession can qualify on its own basket. Never null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a needed column is missing, a column the flags would write already exists, `game_id` differs in dtype between the two frames, or `pbp` has no event for some possession. |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_pbp, load_nba_stats_possessions
from sportsdataverse.nba.nba_possession_flags import POSSESSION_FLAGS_SCHEMA, add_possession_flags

# a release that already carries the flags: drop them to re-flag
raw = load_nba_stats_possessions(2023).drop(*POSSESSION_FLAGS_SCHEMA, strict=False)
poss = add_possession_flags(raw, load_nba_stats_pbp(2023))
print(poss.group_by("is_transition").len())
```

### add_shot_make_prob {#add_shot_make_prob}

`add_shot_make_prob(shots: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Add each field-goal attempt's make chance from the bundled location model.

The model reads only what the play-by-play records for a shot: its court location
(`x_legacy` / `y_legacy`, and its distance from the hoop computed from them), its value
and its shot type (`sub_type`, e.g. `"Driving Layup Shot"`). It was fitted on three
regular-season play-by-play release seasons and gated against the zone table walk-forward by
season (`tools/models/nba_shot_value/train_shot_make.py`; the fit report sits beside it).

The make chance sits at the level of the booster's training seasons (2022-23 to 2024-25); the
league's make rate moves from season to season, by more than a point in some (see
`score_shot_xpoints`). The shot type is the scorer's label, written after the play, so a
make chance that reads it already credits some of what the shooter did (a "Running Dunk Shot"
goes in about 95% of the time).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Play-by-play rows (`nba_stats_pbp` release shape) with `x_legacy`, `y_legacy`, `shot_value` and `sub_type`. Other rows may be present. |
| `league_id` | `str` | `'00'` | `"00"` NBA (the only league with a booster). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The input plus `xfg:Float64`: the make chance of every row with `shot_value` 2 or 3 and a location; null on every other row (free throws, non-shots, an unlocated attempt). Empty input returns the input with the null column.

| col_name | type | description |
|---|---|---|
| `order_index` | Int64 | Stable within-game ordering index for events, after the feed's events are put in play order. |
| `action_number` | Int64 | Sequential action number within a game (V3 PBP). |
| `clock` | String | Game clock value. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int64 | Unique team identifier. |
| `team_tricode` | String | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `player_name_i` | String | Player name i. |
| `x_legacy` | Int64 | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | Int64 | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | Int64 | Shot distance from the basket, in feet. |
| `shot_result` | String | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | Int64 | 1 if the action was a field goal; 0 otherwise. |
| `score_home` | String | Score home. |
| `score_away` | String | Score away. |
| `points_total` | Int64 | Running total of points scored. |
| `location` | String | Location. |
| `description` | String | Long-form description text. |
| `action_type` | String | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | String | Action sub-type label. |
| `video_available` | Int64 | Video available. |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `action_id` | Int64 | Unique action identifier within a game (V3 PBP). |
| `game_id` | String | Unique game identifier. |
| `seconds_remaining` | Float64 | Seconds remaining in the period. |
| `event_type` | String | Event / play type code (V2 PBP). |
| `is_made_shot` | Boolean | Whether the event is a made field goal. |
| `is_missed_shot` | Boolean | Whether the event is a missed field goal. |
| `is_free_throw` | Boolean | Whether the event is a free throw attempt. |
| `is_rebound` | Boolean | Whether the event is a rebound. |
| `is_turnover` | Boolean | Whether the event is a turnover. |
| `is_foul` | Boolean | Whether the event is a foul. |
| `is_substitution` | Boolean | Whether the event is a substitution. |
| `is_jump_ball` | Boolean | Whether the event is a jump ball. |
| `is_timeout` | Boolean | Whether the event is a timeout. |
| `is_period` | Boolean | Whether the event is a period start or end marker. |
| `possession_number` | Int64 | Possession number. |
| `off_player_1` | Int64 | NBA Stats player id of offensive on-court player 1 of 5 during the event. |
| `off_player_2` | Int64 | NBA Stats player id of offensive on-court player 2 of 5 during the event. |
| `off_player_3` | Int64 | NBA Stats player id of offensive on-court player 3 of 5 during the event. |
| `off_player_4` | Int64 | NBA Stats player id of offensive on-court player 4 of 5 during the event. |
| `off_player_5` | Int64 | NBA Stats player id of offensive on-court player 5 of 5 during the event. |
| `def_player_1` | Int64 | NBA Stats player id of defensive on-court player 1 of 5 during the event. |
| `def_player_2` | Int64 | NBA Stats player id of defensive on-court player 2 of 5 during the event. |
| `def_player_3` | Int64 | NBA Stats player id of defensive on-court player 3 of 5 during the event. |
| `def_player_4` | Int64 | NBA Stats player id of defensive on-court player 4 of 5 during the event. |
| `def_player_5` | Int64 | NBA Stats player id of defensive on-court player 5 of 5 during the event. |
| `season` | Int64 | Season year. |
| `xfg` | Float64 | Make chance of a located two- or three-point attempt from the bundled location model; null on every other row (free throws, non-shots, an attempt without a location). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | No booster is bundled for `league_id`, or an input column is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import load_nba_stats_pbp
from sportsdataverse.nba.nba_shot_value import add_shot_make_prob
pbp = add_shot_make_prob(load_nba_stats_pbp([2025]))
pbp.filter(pl.col("xfg").is_not_null()).select("sub_type", "x_legacy", "y_legacy", "xfg").head()

# Pipeline next step (one line)

pbp.group_by("team_id").agg((pl.col("xfg") * pl.col("shot_value")).sum().alias("xfg_points"))
```

### add_start_type_detail {#add_start_type_detail}

`add_start_type_detail(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Append the full pbpstats start-type taxonomy to a possession frame.

Upgrades the engine's coarse 5-value `possession_start_type` into:

* `possession_start_type_detail` — the zone-split pbpstats vocabulary:
  `Off{AtRim|ShortMidRange|LongMidRange|Corner3|Arc3}{Make|Miss|Block}`,
  `OffFTMake` / `OffFTMiss`, `OffLiveBallTurnover`, `OffTimeout`,
  `OffDeadball`.
* `possession_start_type_ctg` — the coarse bucket CTG reports on:
  `off_made` / `off_live_rebound` / `off_steal` / `off_deadball` /
  `off_timeout` (see `~nba_play_context_constants.CTG_START_BUCKETS`).

Precedence (pbpstats): period start > timeout > previous boundary event. A
**team rebound** (`person_id == 0`) is a dead-ball start even though a
rebound row exists; a **timeout** beats a made basket (an after-timeout
possession is `OffTimeout`, not `OffMadeShot`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `~sportsdataverse.nba.nba_possessions.build_possessions`. |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame those possessions were built from. |

**Returns**

`possessions` with the two columns appended. Empty input returns a zero-row frame carrying them — never raises.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `possession_start_type_detail` | String |  |
| `possession_start_type_ctg` | String |  |

**Example**

```python
from sportsdataverse.nba.nba_possessions import build_possessions
from sportsdataverse.nba.nba_play_context import add_start_type_detail
poss = add_start_type_detail(build_possessions(pbp), pbp)
print(poss["possession_start_type_ctg"].value_counts())
```

### add_transition {#add_transition}

`add_transition(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, transition_seconds: 'float' = 6.0, variant: 'str' = 'hoop_math') -> 'pl.DataFrame'`

Flag possessions that started in transition, and time their initial play.

CTG defines transition as beginning at the possession start and ending "once
the defense is set", **without publishing a seconds threshold**. We therefore
time the possession's *initial play* — its first shot attempt, trip to the
line, or turnover (CTG's own definition of a "play") — and call the
possession transition when that play lands within `transition_seconds`.

Variants (`~nba_play_context_constants.TRANSITION_VARIANTS`):

* `hoop_math` (default) — any non-timeout start type qualifies.
* `haslametrics` — steal starts only (conservative).
* `bigballr` — the previous possession must have ended live
  (`off_made` / `off_live_rebound` / `off_steal`); a dead-ball start can
  never be transition.

The first possession of a period is never transition. After a timeout the
defense is set by construction, so `off_timeout` never qualifies under any
variant.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame carrying `possession_start_type_ctg` (i.e. the output of `add_start_type_detail`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `transition_seconds` | `float` | `6.0` | Initial-play cutoff. Default 6.0 (`~nba_play_context_constants.DEFAULT_TRANSITION_SECONDS`, calibrated to CTG's published rates; hoop-math's college rule is 10.0). |
| `variant` | `str` | `'hoop_math'` | One of `~nba_play_context_constants.TRANSITION_VARIANTS`. |

**Returns**

`possessions` with `seconds_to_first_play` (Float64, null when the possession had no play), `is_transition_ctg` (Boolean; also as the deprecated `is_transition`), `transition_source` (Utf8: `steal` / `live_rebound` / `made` / `deadball`; null when not transition) and `possession_context` (Utf8: `transition` / `halfcourt` / `misc`) appended.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `possession_start_type_detail` | String |  |
| `possession_start_type_ctg` | String |  |
| `seconds_to_first_play` | Float64 |  |
| `is_transition_ctg` | Boolean | Cleaning the Glass-style transition (renamed from is_transition), true when the possession's initial play (first shot, free-throw trip or turnover) lands within transition_seconds (default 6) of its start, under the chosen variant; never on a period's first possession or after a timeout. Not the possessions table's is_transition (add_possession_flags), which uses an 8-second first-shot window. |
| `is_transition` | Boolean | Deprecated copy of is_transition_ctg under its old name; read is_transition_ctg (removed in 0.4.0). Not the possessions table's is_transition (add_possession_flags). |
| `transition_source` | String |  |
| `possession_context` | String |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `variant` is not a known transition variant. |

**Example**

```python
poss = add_transition(add_start_type_detail(poss, pbp), pbp)
print(poss["is_transition_ctg"].mean())          # transition frequency

# Tune the knob against the Synergy oracle

poss8 = add_transition(poss, pbp, transition_seconds=8.0)
```

### add_two_for_one_flags {#add_two_for_one_flags}

`add_two_for_one_flags(possessions: 'pl.DataFrame', pbp: 'pl.DataFrame', *, window: 'tuple[float, float]' = (33.0, 46.0), go_floor: 'float' = 27.0, rules: 'str' = 'sdv', shot_clock: 'float' = 24.0) -> 'pl.DataFrame'`

Append the 2-for-1 choice and its end-of-period outcome to possessions (BB-D44).

Near a period's end, a team that gets the ball with about half a minute left can shoot
early, leaving more than a shot clock so it gets the ball back for the last shot (two
possessions to one), or run a normal possession and let the opponent hold for the last
shot. This marks every possession that faces that choice, which one the team made, and
what the rest of the period brought: NBA, G League and WNBA from 2006 (24-second shot clock,
quarters). A game whose first period runs over 15 minutes (WNBA halves and a 30-second shot
clock through 2005; All-Star-weekend exhibitions) gets no `two_for_one`, so no outcomes.
Every clock is seconds left in the period.

**change_clock** (Float64): the clock at the change of possession, the same moment
`seconds_to_first_shot` counts from (the previous possession's last event; a period's
first possession uses its own first event). **first_shot_clock** (Float64): the clock at the
possession's first field-goal attempt, null without one. **margin_at_change** (Int64): the
offense's score minus the defense's at the change of possession, from the pbp running score;
null for a game whose home side the score cannot tell. Set on every possession.

**two_for_one** (Utf8, null outside the window): with `rules="sdv"` the window is
`window[0] < change_clock <= window[1]`; `"went"` when `first_shot_clock >= go_floor`,
`"held"` when not went and the possession ends with 24 or fewer seconds left (the
opponent can hold for the last shot), `"other"` otherwise (an early turnover or foul
trip, or an end between 24 and `go_floor`). `rules="mekonen"` approximates ACC-2026-0075:
the change clock rounded half-to-even (as numpy does) in 34-40 s, `"went"` for a first
shot within 7 s, `"held"` for a later first shot and a rounded end clock of 24 or less,
and in the 4th quarter only when `|margin_at_change| <= 10`. The study's "start clock" is
its possession's first event, and its possessions open with the defensive rebound or steal
(625 of its 778 rows) that ours give to the previous possession, so it is our change clock
(`tools/models/nba_two_for_one_parity.py`: 713 of its 778 rows match one of ours and 604 of
those agree; the study times some first shots by possession length, which this does not).

In the window: **extra_possession** (Boolean), the offense has the ball again after the
defense has had it, in the same period, on a possession whose change clock is above 0 (a
0.4-second heave counts; a team that keeps the ball does not); **sequence_points**
(Int64), the offense's points on this and its later possessions in the period;
**sequence_points_allowed** (Int64), the defense's points on its possessions after this one
in the period; **sequence_net** (Int64), the difference. All four are null outside it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Every possession of each game (the previous possession's clock and the rest of the period are read from it; do not filter it first). Needs `game_id`, `period`, `possession_number`, `number_in_period`, `start_order_index`, `end_order_index`, `start_seconds_remaining`, `end_seconds_remaining`, `offense_team_id`, `defense_team_id` and `points`. |
| `pbp` | `DataFrame` |  | The play-by-play of those games. Needs `game_id`, `order_index`, `event_type`, `seconds_remaining`, `score_home` and `score_away`. |
| `window` | `tuple[float, float]` | `(33.0, 46.0)` | `(lo, hi]` on the change clock for `rules="sdv"`. Default `TWO_FOR_ONE_WINDOW`. |
| `go_floor` | `float` | `27.0` | The first-shot clock at or above which a possession went 2-for-1. Default `TWO_FOR_ONE_GO_FLOOR`. |
| `rules` | `str` | `'sdv'` | `"sdv"` (default) or `"mekonen"` (ACC-2026-0075's definition, approximated, for parity). |
| `shot_clock` | `float` | `24.0` | Must be 24.0: the window and floor are calibrated for that shot clock only. |

**Returns**

`possessions` with `TWO_FOR_ONE_SCHEMA` appended; every input column, value, row and row order is unchanged. An empty frame returns zero rows with those columns.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 | NBA Stats team id of the defending team for the possession. |
| `start_order_index` | Int64 | order_index of the play-by-play event that starts the possession. |
| `end_order_index` | Int64 | order_index of the play-by-play event that ends the possession. |
| `start_seconds_remaining` | Float64 | Seconds remaining in the period when the possession started. |
| `end_seconds_remaining` | Float64 | Seconds remaining in the period when the possession ended. |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean | Whether the row is a second-chance continuation following an offensive rebound. |
| `number_in_period` | Int64 | Possession number within the period, both teams counted, resetting to 1 at each period start. |
| `possession_start_type` | String | How the possession began (e.g. off a made shot, defensive rebound, turnover, or period start). |
| `count_as_possession` | Boolean | Whether the row counts as a true possession for per-possession rate stats. |
| `fg2a` | Int64 | Two-point field goal attempts during the possession. |
| `fg2m` | Int64 | Two-point field goals made during the possession. |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 | NBA Stats player id of offensive on-court player 1 of 5 for the possession. |
| `off_player_2` | Int64 | NBA Stats player id of offensive on-court player 2 of 5 for the possession. |
| `off_player_3` | Int64 | NBA Stats player id of offensive on-court player 3 of 5 for the possession. |
| `off_player_4` | Int64 | NBA Stats player id of offensive on-court player 4 of 5 for the possession. |
| `off_player_5` | Int64 | NBA Stats player id of offensive on-court player 5 of 5 for the possession. |
| `def_player_1` | Int64 | NBA Stats player id of defensive on-court player 1 of 5 for the possession. |
| `def_player_2` | Int64 | NBA Stats player id of defensive on-court player 2 of 5 for the possession. |
| `def_player_3` | Int64 | NBA Stats player id of defensive on-court player 3 of 5 for the possession. |
| `def_player_4` | Int64 | NBA Stats player id of defensive on-court player 4 of 5 for the possession. |
| `def_player_5` | Int64 | NBA Stats player id of defensive on-court player 5 of 5 for the possession. |
| `lineup_source` | String | Provenance of the on-court lineup identification for the row (how the five-man units were resolved). |
| `season` | Int64 | Season year. |
| `seconds_to_first_shot` | Float64 | Seconds from the change of possession to the possession's first field-goal attempt (add_possession_flags), passed through unchanged; null when it has no field-goal attempt. |
| `is_transition` | Boolean | The possessions table's transition flag (add_possession_flags), passed through unchanged. |
| `is_garbage_time` | Boolean | The possessions table's garbage-time flag (add_possession_flags), passed through unchanged. |
| `change_clock` | Float64 | Seconds left in the period at the change of possession (the previous possession's last event; a period's first possession uses its own first event), the moment seconds_to_first_shot counts from. Set on every possession. |
| `first_shot_clock` | Float64 | Seconds left in the period at the possession's first field-goal attempt (change_clock minus seconds_to_first_shot); null when it has no field-goal attempt. |
| `two_for_one` | String | The 2-for-1 choice, null outside the window (change_clock in (33, 46] by default). went when the first shot leaves at least go_floor (27) seconds, held when not and the possession ends with 24 or fewer left (the opponent can hold for the last shot), other otherwise (an early turnover or foul trip, or an end between 24 and 27). |
| `extra_possession` | Boolean | In the window, whether the offense has the ball again after the defense has had it, in the same period, on a possession whose change clock is above 0 (a last-second heave counts; keeping the ball does not); null outside it. |
| `sequence_points` | Int64 | In the window, the offense's points on this possession and its later ones in the period; null outside it. |
| `sequence_points_allowed` | Int64 | In the window, the defense's points on its possessions after this one in the period; null outside it. |
| `sequence_net` | Int64 | sequence_points minus sequence_points_allowed; null outside the window. |
| `margin_at_change` | Int64 | The offense's score minus the defense's at the change of possession, from the play-by-play running score. Set on every possession, except null for a game whose home side the score cannot tell. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a needed column is missing, a column it would write already exists, `game_id` differs in dtype between the frames, `offense_team_id` and `defense_team_id` differ in dtype, `rules` is unknown, `shot_clock` is not 24, or a possession has no event in `pbp` (the wrong season, a filtered frame). |

**Example**

```python
from sportsdataverse.nba import load_nba_stats_pbp, load_nba_stats_possessions
from sportsdataverse.nba.nba_possession_flags import TWO_FOR_ONE_SCHEMA, add_two_for_one_flags

raw = load_nba_stats_possessions(2026).drop(*TWO_FOR_ONE_SCHEMA, strict=False)
poss = add_two_for_one_flags(raw, load_nba_stats_pbp(2026))
print(poss.group_by("two_for_one").len())

# The study's definition, for comparison

mek = add_two_for_one_flags(raw, load_nba_stats_pbp(2026), rules="mekonen")
```

### box_features {#box_features}

`box_features(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', *, game_ids: 'Optional[List[str]]' = None) -> 'pl.DataFrame'`

Aggregate per-player per-100-possession box features over a set of games.

Restricting `game_ids` to a fold's games is the harness leakage guard.

Per-100 possessions are computed per game (so mid-window trades use each
game's own team pace), then summed — the result is fully deterministic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines (`game_id`, `team_id`, `player_id`, `min`, and the counting stats in `_STATS`). |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines (`game_id`, `team_id`, `min`, `fga`, `oreb`, `tov`, `fta`) for the possession estimate. |
| `game_ids` | `Optional[List[str]]` | `None` | Optional subset of `game_id` to include (default: all). |

**Returns**

One row per player: `player_id`, the `_STATS` per-100 rates, `min` (total), `gp` (games). Empty frame with that schema on empty input.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `pts` | Float64 | Points scored. |
| `fg3m` | Float64 | Three-point field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fta` | Float64 | Free throw attempts. |
| `ast` | Float64 | Assists. |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |

### build_play_context_shots {#build_play_context_shots}

`build_play_context_shots(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, putback_seconds: 'float' = 2.0) -> 'pl.DataFrame'`

Build the per-shot frame carrying CTG's play context.

CTG assigns context **per play**, not per possession: one possession can
contain a transition miss, a halfcourt reset and a putback. This frame is the
play-level view — one row per field-goal attempt.

* `is_putback` — pbpstats `field_goal.py:112-144`: an **unassisted 2-point**
  attempt whose preceding event is a **real offensive rebound by the same
  player**, within `putback_seconds`. A three is never a putback.
* `is_second_chance_shot` — the shot follows an offensive rebound earlier in
  the same possession.
* `shot_context` — `transition` / `putback` / `halfcourt`. **Transition
  wins over putback**, reproducing CTG exactly: "if a team comes down in
  transition and misses a shot but gets a putback, that putback is classified
  as part of the overall transition event."

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_transition` (needs `is_transition_ctg`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `putback_seconds` | `float` | `2.0` | Rebound-to-shot window. Default 2.0 (pbpstats). |

**Returns**

Polars DataFrame with schema `PLAY_CONTEXT_SHOTS_SCHEMA` — one row per field-goal attempt. Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `possession_number` | Int64 | Possession number. |
| `order_index` | Int64 |  |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | Int64 | Unique team identifier. |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `shot_value` | Int64 | Point value of the shot (2 or 3). |
| `shot_made` | Boolean |  |
| `ctg_shot_zone` | String |  |
| `is_assisted` | Boolean |  |
| `is_putback` | Boolean |  |
| `is_second_chance_shot` | Boolean |  |
| `shot_context` | String |  |

**Example**

```python
shots = build_play_context_shots(poss, pbp)
print(shots.group_by("shot_context").len())
print(shots.filter(pl.col("is_putback") == True).height)
```

### build_possession_shooting {#build_possession_shooting}

`build_possession_shooting(enhanced_pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Build the per-shooter companion frame from an enhanced play-by-play DataFrame.

Companion to `build_possessions`: instead of one team-level row per
possession, emits one row per distinct shooter (`player_id`) per
possession, with their own `fg2a/fg2m/fg3a/fg3m/fta/ftm` counts. Shares
the same possession-group traversal as `build_possessions` via
`_assemble` — the two frames are always built from a single
consistent pass over the play-by-play. Consumed by WP2's luck-adjusted
shooting response.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `enhanced_pbp` | `DataFrame` |  | Polars DataFrame with schema `ENHANCED_PBP_SCHEMA` (from `~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`). An empty or malformed frame returns a zero-row frame with `POSSESSION_SHOOTING_SCHEMA` — never raises. |

**Returns**

Polars DataFrame with schema `POSSESSION_SHOOTING_SCHEMA`. One row per `(possession_number, player_id)` pair. Events with `person_id == 0` are skipped (unattributable to a shooter — they still count toward `build_possessions`' team-level totals). Per-possession sums of the six shooting columns match the corresponding `build_possessions` columns exactly.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `possession_number` | Int64 | Possession number. |
| `player_id` | Int64 | Unique player identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |

**Example**

```python
import json, pathlib
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possessions import build_possession_shooting

payload = json.loads(pathlib.Path("playbyplayv3.json").read_text())
pbp = enhanced_pbp_from_payload(payload)
sh = build_possession_shooting(pbp)
print(sh.shape, sh.schema["player_id"])

# Per-player shooting totals

import polars as pl
totals = sh.group_by("player_id").agg(
    pl.col("fg3m").sum(), pl.col("ftm").sum()
)
print(totals.head())
```

### clutch_delta {#clutch_delta}

`clutch_delta(clutch: 'pl.DataFrame', ratings: 'pl.DataFrame') -> 'pl.DataFrame'`

Clutch net-rating delta vs a full-game baseline, per (season, team_id).

`clutch_delta = clutch_net_rating - adj_net_rtg`. Joins `clutch` to the
baseline `ratings` frame on `(season, team_id)` (asserting dtype
agreement first).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `clutch` | `DataFrame` |  | Frame with `season, team_id, clutch_net_rating, clutch_poss`. |
| `ratings` | `DataFrame` |  | Full-game baseline with `season, team_id, adj_net_rtg` (the stats full-game net, or any per-team baseline). |

**Returns**

One row per matched (season, team_id): `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_poss`. Empty input returns that schema with zero rows.

No returns table is published for this function: no capture: its clutch frame is built from stats.nba.com leaguedashteamclutch, which answers HTTP 403 to the datacenter IP the docs are built on.

**Example**

```python
from sportsdataverse.nba.nba_clutch import clutch_delta
d = clutch_delta(clutch_frame, baseline_frame)
```

### flag_garbage_time {#flag_garbage_time}

`flag_garbage_time(possessions: 'pl.DataFrame', enhanced_pbp: 'pl.DataFrame', *, starters_on_court: 'Optional[dict[int, int]]' = None) -> 'pl.DataFrame'`

Flag CTG garbage time (excluded from CTG stats by default).

CTG (exact): "the game has to be in the **4th quarter**, the score
differential has to be **>= 25 for minutes 12-9, >= 20 for minutes 9-6, and
>= 10 for the remainder of the quarter**. Additionally, there have to be **two
or fewer starters on the floor combined between the two teams**. Importantly,
the game can never go back to being non-garbage time, or this clock resets."

The margin x minutes bands are reproduced exactly, evaluated on the score at
each possession's start. The reset semantics fall out of that per-possession
evaluation: if the trailing team claws back inside the band's threshold, the
condition stops holding and those possessions are NOT garbage time (CTG's own
"comeback is not counted as garbage time" example); if the lead re-expands,
the flag turns back on.

**The starters clause is applied only when `starters_on_court` is supplied**
— it needs lineup + box `START_POSITION` data this frame does not carry.
Without it the flag is the **margin-only superset** of CTG's definition (it can
flag a blowout stretch in which the starters are still on the floor), and
`garbage_time_basis` records which rule was actually used. This is a
deliberate, documented divergence — do not read a `margin_only` flag as
CTG-exact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame with `period`, `start_seconds_remaining` and `start_order_index`. |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame (supplies the running score). |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional map `possession_number -> number of starters on the floor across BOTH teams`. When given, a possession is garbage time only if that count is <= `~nba_play_context_constants.GARBAGE_TIME_MAX_STARTERS`. |

**Returns**

`possessions` with Boolean `is_garbage_time_ctg` (also as the deprecated `is_garbage_time`) and Utf8 `garbage_time_basis` (`"margin_and_starters"` or `"margin_only"`) appended.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `is_garbage_time_ctg` | Boolean | Cleaning the Glass garbage time (renamed from is_garbage_time), true in the 4th quarter with a margin at the possession's start of 25+ in minutes 12-9, 20+ in 9-6 or 10+ after, and, when starters_on_court is given, two or fewer starters on the floor (garbage_time_basis says which). Not the possessions table's is_garbage_time (add_possession_flags, bigballR's rule). |
| `is_garbage_time` | Boolean | Deprecated copy of is_garbage_time_ctg under its old name; read is_garbage_time_ctg (removed in 0.4.0). Not the possessions table's is_garbage_time (add_possession_flags). |
| `garbage_time_basis` | String |  |

**Example**

```python
poss = flag_garbage_time(poss, pbp)
print(poss.filter(pl.col("is_garbage_time_ctg") == True).height)

# CTG-exact, with the starters clause

poss = flag_garbage_time(poss, pbp, starters_on_court=starters_by_possession)
```

### flag_heave_possessions {#flag_heave_possessions}

`flag_heave_possessions(possessions: 'pl.DataFrame') -> 'pl.DataFrame'`

Flag CTG's "projected heave possessions" (excluded from CTG stats by default).

CTG (exact): "possessions that start with **4 or fewer seconds on the game
clock at the end of one of the first three quarters**." Q4/OT are exempt — a
late Q4 possession is a real possession.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Any frame with `period` and `start_seconds_remaining`. |

**Returns**

`possessions` with a Boolean `is_heave_possession` column appended.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 |  |
| `off_player_2` | Int64 |  |
| `off_player_3` | Int64 |  |
| `off_player_4` | Int64 |  |
| `off_player_5` | Int64 |  |
| `def_player_1` | Int64 |  |
| `def_player_2` | Int64 |  |
| `def_player_3` | Int64 |  |
| `def_player_4` | Int64 |  |
| `def_player_5` | Int64 |  |
| `lineup_source` | String |  |
| `is_heave_possession` | Boolean |  |

**Example**

```python
poss = flag_heave_possessions(poss)
clean = poss.filter(pl.col("is_heave_possession") == False)
```

### hoopshype_salaries {#hoopshype_salaries}

`hoopshype_salaries(*, proxy: 'Any' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

League-wide NBA player salaries from HoopsHype.

One row per player per contract season (current plus the future seasons
HoopsHype lists), for the whole league (~600 players).

HoopsHype is a Next.js app: the single `/salaries/players/` page paginates
client-side and only ~20 rows survive a static fetch, but each team page
embeds that team's complete roster in `<script id="__NEXT_DATA__">`. This
walks the 30 slugs in `HOOPSHYPE_TEAMS` **serially** -- ~30 requests per
call -- and parses that JSON. A team page that fails is warned about and
skipped rather than aborting the league.

Pacing is environment-tunable, never hardcoded in the fetch path:

============================ ==================================================
`SDV_PY_HOOPSHYPE_DELAY`   seconds slept between team pages (default `0.5`)
============================ ==================================================

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `proxy` | `Any` | `None` | Proxy configuration forwarded to `~sportsdataverse.dl_utils.download` (`requests` `proxies=` shape). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player-season with `player_id`, `player`, `first_name`, `last_name`, `team_id`, `team`, `season`, `salary`, `cap_allocation`, `team_option`, `player_option`, `two_way` and `qualifying_offer`. Ids are `Utf8`, money is `Float64`, options are `Boolean`. All 30 pages failing yields a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `player` | String | Player name. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `team_id` | String | Unique team identifier. |
| `team` | String | Team-side label or team identifier. |
| `season` | Int64 | Season year. |
| `salary` | Float64 | Total cap-counting salary for the season ($). |
| `cap_allocation` | Float64 | Cap allocation for the season (USD). |
| `team_option` | Boolean | Whether the season is a team option. |
| `player_option` | Boolean | Whether the season is a player option. |
| `two_way` | Boolean | Whether it is a two-way contract. |
| `qualifying_offer` | Boolean | Whether it is a qualifying offer. |

**Raises**

| Exception | Description |
|---|---|
| `UserWarning` | Emitted (not raised) per team page that cannot be fetched or parsed; the remaining teams still load. |

**Example**

```python
from sportsdataverse.nba import hoopshype_salaries

salaries = hoopshype_salaries()
print(salaries.shape)

# As pandas

salaries_pd = hoopshype_salaries(return_as_pandas=True)

# Pipeline next step (this season's top-paid)

salaries.filter(pl.col("season") == 2026).sort("salary", descending=True).head()
```

### lineup_play_context {#lineup_play_context}

`lineup_play_context(possessions: 'pl.DataFrame', *, min_poss: 'int' = 0, league_non_transition_ppp: 'Optional[float]' = None, apply_ctg_filters: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Roll possessions up into a per-5-man-lineup Play-Context table.

The lineup analogue of `team_play_context`: same metric columns, grouped
by the five players on the floor **for the offense**. Lineups are identified by
`lineup_id` — the five player ids sorted ascending and hyphen-joined — so the
same five players always land in the same bucket regardless of slot order.

Requires the `off_player_1..5` columns from
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`
(which passes the play-context columns through, so the two compose in either
order).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Frame from `add_play_context` **with lineups attached**. |
| `min_poss` | `int` | `0` | Drop lineups below this possession count (CTG's tables carry a minimum; 0 keeps everything, which is what the partition identity needs). |
| `league_non_transition_ppp` | `Optional[float]` | `None` | Pts+/Poss baseline; see `team_play_context`. |
| `apply_ctg_filters` | `bool` | `True` | Drop garbage-time / heave / non-counting possessions first. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (team, lineup) with `LINEUP_PLAY_CONTEXT_SCHEMA`. Empty input returns a zero-row frame with that schema.

| col_name | type | description |
|---|---|---|
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `lineup_id` | String |  |
| `off_player_1` | Int64 |  |
| `off_player_2` | Int64 |  |
| `off_player_3` | Int64 |  |
| `off_player_4` | Int64 |  |
| `off_player_5` | Int64 |  |
| `poss` | Int64 | Poss. |
| `points` | Int64 | Points scored. |
| `pts_per_100` | Float64 |  |
| `transition_poss` | Int64 |  |
| `transition_points` | Int64 |  |
| `transition_freq` | Float64 |  |
| `transition_pts_per_100` | Float64 |  |
| `non_transition_pts_per_100` | Float64 |  |
| `transition_pts_added_per_100` | Float64 |  |
| `halfcourt_poss` | Int64 |  |
| `halfcourt_pts_per_100` | Float64 |  |
| `freq_off_steal` | Float64 |  |
| `freq_off_live_rebound` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | When the `off_player_*` columns are missing. |

**Example**

```python
poss = attach_possession_lineups(add_play_context(enh), oncourt, enh, home_team_id=home)
lu = lineup_play_context(poss, min_poss=25)
print(lu.sort("pts_per_100", descending=True).head())
```

### load_shot_make_model {#load_shot_make_model}

`load_shot_make_model(league_id: 'str' = '00') -> 'xgb.Booster'`

Load the bundled shot-make booster for `league_id` (only `"00"`, the NBA, has one).

Its attributes carry the shot-type levels it was fitted with (`shot_types`, a JSON list),
the train / holdout seasons and the fitting script.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league_id` | `str` | `'00'` | `"00"` NBA. |

**Returns**

The `xgboost.Booster`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | No booster is bundled for `league_id`, or the booster's feature names are not `SHOT_MAKE_NUMERIC` plus its shot-type levels. |

**Example**

```python
from sportsdataverse.nba.nba_shot_value import load_shot_make_model
booster = load_shot_make_model("00")
booster.attr("train_seasons")
```

### make_prob_by_context {#make_prob_by_context}

`make_prob_by_context(ptshots: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

Marginal FG% tables by defender distance and by shot clock.

The public API exposes defender-distance and shot-clock only as aggregate
bucket tables (`playerdashptshots`), not per-shot fields, so this
aggregates `Σfgm/Σfga` across players within each bucket.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ptshots` | `DataFrame` |  | The stacked `playerdashptshots` fixture — one frame with a `result_set` tag (`ClosestDefenderShooting` / `ShotClockShooting`) plus `bucket, fga, fgm`. |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

`{"defender": frame, "shot_clock": frame}` each with rows per `bucket` (`bucket, fga, fgm, fg_pct`). Missing result sets return the zero-row schema.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context
tables = make_prob_by_context(ptshots)
tables["defender"].sort("fg_pct")
```

### make_prob_joint {#make_prob_joint}

`make_prob_joint(defender: 'pl.DataFrame', shot_clock: 'pl.DataFrame', overall_fg_pct: 'float', *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Independence-combined defender x shot-clock make probability.

Combines the two marginal FG% tables under a conditional-independence
assumption via odds multipliers: `odds(p) = p/(1-p)`;
`odds_joint = odds_overall * (odds_def/odds_overall) *
(odds_clock/odds_overall)`; `joint = odds_joint/(1+odds_joint)`. This
assumes defender distance and shot-clock effects are independent given the
league baseline — a simplification (a late clock correlates with tighter
defense), documented here so callers weigh it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `defender` | `DataFrame` |  | The `"defender"` marginal table from `make_prob_by_context` (`bucket, fg_pct`). |
| `shot_clock` | `DataFrame` |  | The `"shot_clock"` marginal table (`bucket, fg_pct`). |
| `overall_fg_pct` | `float` |  | The league overall FG% baseline. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(close_def_dist_range, shot_clock_range)`: `close_def_dist_range:Utf8, shot_clock_range:Utf8, joint_fg_pct:Float64`. Empty inputs return the zero-row schema.

No returns table is published for this function: no capture: its inputs come from make_prob_by_context on stats.nba.com tracking data, which answers HTTP 403 to the datacenter IP the docs are built on.

**Example**

```python
from sportsdataverse.nba.nba_shot_value import make_prob_by_context, make_prob_joint
t = make_prob_by_context(ptshots)
joint = make_prob_joint(t["defender"], t["shot_clock"], 0.47)
```

### nba_availability {#nba_availability}

`nba_availability(seasons: "'int | list[int]'", *, league: 'str' = 'nba', gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project games-available % for a season (or seasons) from career GP history.

`avail_pct` is **availability, not skill** -- it is the only output of
this function and is never combined into a value/rating column by this
module (`sportsdataverse.nba.nba_rookie_projection.nba_rookie_projection`
reports it as a separate column too).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (end year, e.g. `2020`) or list of seasons. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `gleague_bridge` | `bool` | `False` | When `True` (and `league != "gleague"`), also pulls each season's G-League (`league_id="20"`) bulk GP as a development-outcome bridge feature before scoring. Best-effort: gracefully absent (never raises) when the G-League bulk call returns no rows for a season; the returned schema is unaffected either way since the bridge column isn't part of the bundled artifact's scored features. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, season:Int64, avail_pct:Float64` (clipped to `[0, 1]`). Empty `seasons` -> zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_availability
proj = nba_availability(2019)
print(proj.sort("avail_pct").head())
```

### nba_box_events {#nba_box_events}

`nba_box_events(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame') -> 'pl.DataFrame'`

Each player's box counts per stats.nba.com action, the input of the stint and split box.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_nba_stats_pbp` / `load_wnba_stats_pbp` rows (`season`, `game_id`, `order_index`, `team_id`, `person_id`, `player_name`, `player_name_i`, `action_type`, `sub_type`, `description`, `shot_value`). |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `home_team_id`, `away_team_id`). |

**Returns**

Rows in `sportsdataverse._common.game_stints.box_event_schema` (Int64 player ids), `order` the action's `order_index`: one per player action with a count, and one per assist on the made shot's action. An assist whose passer matches no single teammate's name in the game has no row. Games not in `schedule` are dropped.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | String | stats.nba.com game id (e.g. 0022500101). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
| `order` | Int64 | The action's place in the game: the stint rows' order (stats.nba.com order_index; for college the event's row in the pbp release). |
| `points` | Int64 | Points the player scored: 2 or 3 per made field goal and 1 per made free throw. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goals attempted. |
| `fg3m` | Int64 | Three-pointers made. |
| `fg3a` | Int64 | Three-pointers attempted. |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throws attempted. |
| `oreb` | Int64 | Offensive rebounds (a team rebound is no player's). |
| `dreb` | Int64 | Defensive rebounds (a team rebound is no player's). |
| `ast` | Int64 | Assists: made field goals the play-by-play credits him as the passer on. |
| `stl` | Int64 | Steals he is credited with. |
| `blk` | Int64 | Shots he blocked. |
| `tov` | Int64 | Turnovers (a team turnover is no player's). |
| `pf` | Int64 | Personal fouls (technicals are not counted). |

**Example**

```python
from sportsdataverse.nba import nba_loaders as L
from sportsdataverse.nba.nba_game_stints import nba_box_events
nba_box_events(L.load_nba_stats_pbp(2025), L.load_nba_stats_schedules(2025)).head()
```

### nba_box_logs {#nba_box_logs}

`nba_box_logs(season: 'str', *, league_id: 'str' = '00', season_type: 'str' = 'Regular Season', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Dict[str, pl.DataFrame]'`

Fetch per-player and per-team game logs for a season (bulk, one call each).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season in `"2023-24"` form. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `season_type` | `str` | `'Regular Season'` | SeasonType (`"Regular Season"`). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguegamelog` replacement for offline tests. |

**Returns**

`{"player": <per-player-game logs>, "team": <per-team-game logs>}` as snake-cased polars frames.

**Example**

```python
from sportsdataverse.nba.nba_box_logs import nba_box_logs
logs = nba_box_logs("2023-24")
print(logs["player"].shape)
```
