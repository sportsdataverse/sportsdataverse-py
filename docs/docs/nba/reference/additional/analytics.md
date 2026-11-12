---
title: "NBA — additional Python functions — Analytics: add_ctg–nba_coach"
sidebar_label: "Analytics: add_ctg–nba_coach"
sidebar_position: 15
description: "NBA — additional Python functions — Analytics: add_ctg–nba_coach — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: add_ctg–nba_coach

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
| `number_in_period` | Int64 | Sequential possession number for the offense within the period. |
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

### nba_coach_careers {#nba_coach_careers}

`nba_coach_careers(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', espn: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Head-coach careers over `seasons`: `nba_coach_seasons` summed per coach, rates recomputed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | The seasons the careers cover (a career counts only these). |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | As `nba_coach_seasons`. |
| `espn` | `bool` | `True` | As `nba_coach_seasons`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `coach_id`, most games first: `teams` (abbreviations, first to last), `seasons`, `first_season`, `last_season`, the summed record and counts and the recomputed rates, with ranks among coaches with at least a full season of games. To build careers from coach seasons already in hand, call `~sportsdataverse._common.coach_seasons.coach_careers` on them.

| col_name | type | description |
|---|---|---|
| `coach_id` | Int64 | stats.nba.com coach id of the head coach (nba_stats_coaches / wnba_stats_coaches). |
| `coach` | String | Head coach's name (stats.nba.com's staff list). |
| `role` | String | HC: head coach (the only role the table carries). |
| `teams` | String | Teams coached, first to last, comma-joined (schools in college, abbreviations in the pros). |
| `seasons` | Int64 | Distinct seasons in the career (only the seasons passed in). |
| `first_season` | Int64 | First season in the career. |
| `last_season` | Int64 | Last season in the career. |
| `games` | Int64 | Games credited to the coach over the seasons passed in. |
| `wins` | Int64 | Wins in the coach's games (final scores). |
| `losses` | Int64 | Losses in the coach's games (final scores). |
| `win_pct` | Float64 | Win fraction: wins divided by games, from the final scores. |
| `pts` | Int64 | Points scored in the coach's games (final scores). |
| `opp_pts` | Int64 | Points allowed in the coach's games (final scores). |
| `pbp_games` | Int64 | The coach's games with published possessions; the ratings and four factors cover these. |
| `poss` | Int64 | Offensive possessions in the coach's games. |
| `opp_poss` | Int64 | Defensive possessions in the coach's games. |
| `poss_pts` | Int64 | Points scored on those possessions (the off_rtg numerator; can differ from pts by a few points a game). |
| `opp_poss_pts` | Int64 | Points allowed on those possessions (the def_rtg numerator). |
| `seconds` | Float64 | Game clock, in seconds, that the coach's possessions used (the pace denominator). |
| `o_fga` | Float64 | The team's field-goal attempts in the coach's games (a four-factor count; null when the source has none). |
| `d_fga` | Float64 | Opponents' field-goal attempts in the coach's games (a four-factor count). |
| `o_fgm` | Float64 | The team's field goals made in the coach's games (a four-factor count; null when the source has none). |
| `d_fgm` | Float64 | Opponents' field goals made in the coach's games (a four-factor count). |
| `o_fg3m` | Float64 | The team's three-pointers made in the coach's games (a four-factor count; null when the source has none). |
| `d_fg3m` | Float64 | Opponents' three-pointers made in the coach's games (a four-factor count). |
| `o_fta` | Float64 | The team's free-throw attempts in the coach's games (a four-factor count; null when the source has none). |
| `d_fta` | Float64 | Opponents' free-throw attempts in the coach's games (a four-factor count). |
| `o_oreb` | Float64 | The team's offensive rebounds in the coach's games (a four-factor count; null when the source has none). |
| `d_oreb` | Float64 | Opponents' offensive rebounds in the coach's games (a four-factor count). |
| `o_dreb` | Float64 | The team's defensive rebounds in the coach's games (a four-factor count; null when the source has none). |
| `d_dreb` | Float64 | Opponents' defensive rebounds in the coach's games (a four-factor count). |
| `o_tov` | Float64 | The team's turnovers in the coach's games (a four-factor count; null when the source has none). |
| `d_tov` | Float64 | Opponents' turnovers in the coach's games (a four-factor count). |
| `o_ff_poss` | Float64 | The team's possessions counted for the turnover rate (the box possessions in college) in the coach's games (a four-factor count; null when the source has none). |
| `d_ff_poss` | Float64 | Opponents' possessions counted for the turnover rate (the box possessions in college) in the coach's games (a four-factor count). |
| `off_rtg` | Float64 | Offensive rating: points scored per 100 offensive possessions, over the coach's games. |
| `def_rtg` | Float64 | Defensive rating: points allowed per 100 defensive possessions, over the coach's games. |
| `net_rtg` | Float64 | Net rating: off_rtg minus def_rtg. |
| `pace` | Float64 | Pace: possessions per 48 minutes (40 with league_id="10", the WNBA), the mean of poss and opp_poss over the game clock those possessions used. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 * 3PM) / FGA, as a fraction. |
| `tov_pct` | Float64 | Turnovers per possession, as a fraction. |
| `orb_pct` | Float64 | Offensive rebound rate: offensive rebounds over offensive-rebound chances (own OREB plus the defense's DREB), as a fraction. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA. |
| `opp_efg_pct` | Float64 | Opponents' effective field-goal percentage, as a fraction. |
| `opp_tov_pct` | Float64 | Opponents' turnovers per possession (turnovers forced), as a fraction. |
| `opp_orb_pct` | Float64 | Opponents' offensive rebound rate, as a fraction. |
| `opp_ft_rate` | Float64 | Opponents' free-throw rate, FTA / FGA. |
| `win_pct_rank` | Float64 | Rank of win_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `off_rtg_rank` | Float64 | Rank of off_rtg among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `def_rtg_rank` | Float64 | Rank of def_rtg among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `net_rtg_rank` | Float64 | Rank of net_rtg among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `pace_rank` | Float64 | Rank of pace among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `efg_pct_rank` | Float64 | Rank of efg_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `tov_pct_rank` | Float64 | Rank of tov_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `orb_pct_rank` | Float64 | Rank of orb_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `ft_rate_rank` | Float64 | Rank of ft_rate among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `opp_efg_pct_rank` | Float64 | Rank of opp_efg_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `opp_tov_pct_rank` | Float64 | Rank of opp_tov_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the highest value ranks 1; ties average). |
| `opp_orb_pct_rank` | Float64 | Rank of opp_orb_pct among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `opp_ft_rate_rank` | Float64 | Rank of opp_ft_rate among coaches with at least one full season of games (the most games any team played in a season of the input; null under five) (the lowest value ranks 1; ties average). |
| `win_pct_pct` | Float64 | Percentile of win_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `off_rtg_pct` | Float64 | Percentile of off_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `def_rtg_pct` | Float64 | Percentile of def_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `net_rtg_pct` | Float64 | Percentile of net_rtg among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `pace_pct` | Float64 | Percentile of pace among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `efg_pct_pct` | Float64 | Percentile of efg_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `tov_pct_pct` | Float64 | Percentile of tov_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `orb_pct_pct` | Float64 | Percentile of orb_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `ft_rate_pct` | Float64 | Percentile of ft_rate among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_efg_pct_pct` | Float64 | Percentile of opp_efg_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_tov_pct_pct` | Float64 | Percentile of opp_tov_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_orb_pct_pct` | Float64 | Percentile of opp_orb_pct among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `opp_ft_rate_pct` | Float64 | Percentile of opp_ft_rate among the same qualified coaches: 100 * (n + 1 - rank) / (n + 1), n the qualifiers with a value. |
| `win_pct_n` | Int64 | Sample size of win_pct: games. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: offensive possessions. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: defensive possessions. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: offensive plus defensive possessions. |
| `pace_n` | Int64 | Sample size of pace: the games with possessions (pbp_games). |
| `efg_pct_n` | Int64 | Sample size of efg_pct: field-goal attempts. |
| `tov_pct_n` | Int64 | Sample size of tov_pct: turnover-rate possessions. |
| `orb_pct_n` | Int64 | Sample size of orb_pct: offensive-rebound chances. |
| `ft_rate_n` | Int64 | Sample size of ft_rate: field-goal attempts. |
| `opp_efg_pct_n` | Int64 | Sample size of opp_efg_pct: opponents' field-goal attempts. |
| `opp_tov_pct_n` | Int64 | Sample size of opp_tov_pct: opponents' turnover-rate possessions. |
| `opp_orb_pct_n` | Int64 | Sample size of opp_orb_pct: opponents' offensive-rebound chances. |
| `opp_ft_rate_n` | Int64 | Sample size of opp_ft_rate: opponents' field-goal attempts. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | As `nba_coach_seasons`. |

**Example**

```python
from sportsdataverse.nba.nba_coach_seasons import nba_coach_careers
nba_coach_careers(list(range(2016, 2027))).select("coach", "teams", "games", "win_pct").head()
```
