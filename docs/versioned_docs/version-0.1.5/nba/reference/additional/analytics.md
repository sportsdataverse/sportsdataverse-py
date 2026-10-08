---
title: "NBA — additional Python functions — Analytics: add_ctg–nba_referee"
sidebar_label: "Analytics: add_ctg–nba_referee"
sidebar_position: 12
description: "NBA — additional Python functions — Analytics: add_ctg–nba_referee — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: add_ctg–nba_referee

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
| `order_index` | integer |  |
| `action_number` | integer | Sequential action number within a game (V3 PBP). |
| `clock` | character | Game clock value. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | integer | Unique team identifier. |
| `team_tricode` | character | Three-letter team code (e.g. 'LAS' / 'NYL'). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_name` | character | Player name. |
| `player_name_i` | character | Player name i. |
| `x_legacy` | integer | V2-format X coordinate (preserved for V3-to-V2 compatibility). |
| `y_legacy` | integer | V2-format Y coordinate (preserved for V3-to-V2 compatibility). |
| `shot_distance` | integer | Shot distance from the basket, in feet. |
| `shot_result` | character | Shot result ('Made' / 'Missed'). |
| `is_field_goal` | integer | 1 if the action was a field goal; 0 otherwise. |
| `score_home` | character | Score home. |
| `score_away` | character | Score away. |
| `points_total` | integer | Running total of points scored. |
| `location` | character | Location. |
| `description` | character | Long-form description text. |
| `action_type` | character | Action type label (e.g. 'Made Shot', 'Substitution'). |
| `sub_type` | character | Action sub-type label. |
| `video_available` | integer | Video available. |
| `shot_value` | integer | Point value of the shot (2 or 3). |
| `action_id` | integer | Unique action identifier within a game (V3 PBP). |
| `game_id` | character | Unique game identifier. |
| `seconds_remaining` | double | Seconds remaining in the period. |
| `event_type` | character | Event / play type code (V2 PBP). |
| `is_made_shot` | logical |  |
| `is_missed_shot` | logical |  |
| `is_free_throw` | logical |  |
| `is_rebound` | logical |  |
| `is_turnover` | logical |  |
| `is_foul` | logical |  |
| `is_substitution` | logical |  |
| `is_jump_ball` | logical |  |
| `is_timeout` | logical |  |
| `is_period` | logical |  |
| `ctg_shot_zone` | character |  |

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
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `starters_on_court` | `Optional[dict[int, int]]` | `None` | Optional starters-on-floor counts; see `flag_garbage_time`. |

**Returns**

The possession frame (`POSSESSIONS_SCHEMA`) plus every column in `PLAY_CONTEXT_POSSESSIONS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | integer | Possession number. |
| `offense_team_id` | integer | Unique identifier for offense team. |
| `defense_team_id` | integer |  |
| `start_order_index` | integer |  |
| `end_order_index` | integer |  |
| `start_seconds_remaining` | double |  |
| `end_seconds_remaining` | double |  |
| `points` | integer | Points scored. |
| `is_second_chance` | logical |  |
| `number_in_period` | integer |  |
| `possession_start_type` | character |  |
| `count_as_possession` | logical |  |
| `fg2a` | integer |  |
| `fg2m` | integer |  |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3m` | integer | Three-point field goals made. |
| `fta` | integer | Free throw attempts. |
| `ftm` | integer | Free throws made. |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `tov` | integer | Turnovers. |
| `possession_start_type_detail` | character |  |
| `possession_start_type_ctg` | character |  |
| `seconds_to_first_play` | double |  |
| `is_transition` | logical |  |
| `transition_source` | character |  |
| `possession_context` | character |  |
| `is_heave_possession` | logical |  |
| `is_garbage_time` | logical |  |
| `garbage_time_basis` | character |  |

**Example**

```python
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import add_play_context
poss = add_play_context(enhanced_pbp_from_payload(payload))
print(poss["possession_start_type_ctg"].value_counts())
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
| `game_id` | character | Unique game identifier. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | integer | Possession number. |
| `offense_team_id` | integer | Unique identifier for offense team. |
| `defense_team_id` | integer |  |
| `start_order_index` | integer |  |
| `end_order_index` | integer |  |
| `start_seconds_remaining` | double |  |
| `end_seconds_remaining` | double |  |
| `points` | integer | Points scored. |
| `is_second_chance` | logical |  |
| `number_in_period` | integer |  |
| `possession_start_type` | character |  |
| `count_as_possession` | logical |  |
| `fg2a` | integer |  |
| `fg2m` | integer |  |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3m` | integer | Three-point field goals made. |
| `fta` | integer | Free throw attempts. |
| `ftm` | integer | Free throws made. |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `tov` | integer | Turnovers. |
| `possession_start_type_detail` | character |  |
| `possession_start_type_ctg` | character |  |

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
| `transition_seconds` | `float` | `6.0` | Initial-play cutoff. Default 10.0 (hoop-math). Calibrate against Synergy transition frequency (league mean ~15-16%). |
| `variant` | `str` | `'hoop_math'` | One of `~nba_play_context_constants.TRANSITION_VARIANTS`. |

**Returns**

`possessions` with `seconds_to_first_play` (Float64, null when the possession had no play), `is_transition` (Boolean), `transition_source` (Utf8: `steal` / `live_rebound` / `made` / `deadball`; null when not transition) and `possession_context` (Utf8: `transition` / `halfcourt` / `misc`) appended.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | integer | Possession number. |
| `offense_team_id` | integer | Unique identifier for offense team. |
| `defense_team_id` | integer |  |
| `start_order_index` | integer |  |
| `end_order_index` | integer |  |
| `start_seconds_remaining` | double |  |
| `end_seconds_remaining` | double |  |
| `points` | integer | Points scored. |
| `is_second_chance` | logical |  |
| `number_in_period` | integer |  |
| `possession_start_type` | character |  |
| `count_as_possession` | logical |  |
| `fg2a` | integer |  |
| `fg2m` | integer |  |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3m` | integer | Three-point field goals made. |
| `fta` | integer | Free throw attempts. |
| `ftm` | integer | Free throws made. |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `tov` | integer | Turnovers. |
| `possession_start_type_detail` | character |  |
| `possession_start_type_ctg` | character |  |
| `seconds_to_first_play` | double |  |
| `is_transition` | logical |  |
| `transition_source` | character |  |
| `possession_context` | character |  |

**Example**

```python
poss = add_transition(add_start_type_detail(poss, pbp), pbp)
print(poss["is_transition"].mean())          # transition frequency

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
| `player_logs` | `DataFrame` |  | Per-player-per-game box lines (`game_id`, `team_id`, `player_id`, `min`, and the counting stats in STATS`). |
| `team_logs` | `DataFrame` |  | Per-team-per-game lines (`game_id`, `team_id`, `min`, `fga`, `oreb`, `tov`, `fta`) for the possession estimate. |
| `game_ids` | `Optional[List[str]]` | `None` | Optional subset of `game_id` to include (default: all). |

**Returns**

One row per player: `player_id`, the STATS` per-100 rates, `min` (total), `gp` (games). Empty frame with that schema on empty input.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `pts` | double | Points scored. |
| `fg3m` | double | Three-point field goals made. |
| `fga` | double | Field goal attempts. |
| `fta` | double | Free throw attempts. |
| `ast` | double | Assists. |
| `oreb` | double | Offensive rebounds. |
| `dreb` | double | Defensive rebounds. |
| `stl` | double | Steals. |
| `blk` | double | Blocks. |
| `tov` | double | Turnovers. |
| `pf` | double | Personal fouls. |
| `min` | double | Minutes played. |
| `gp` | integer | Games played. |

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
| `possessions` | `DataFrame` |  | Frame from `add_transition` (needs `is_transition`). |
| `enhanced_pbp` | `DataFrame` |  | The enhanced PBP frame the possessions were built from. |
| `putback_seconds` | `float` | `2.0` | Rebound-to-shot window. Default 2.0 (pbpstats). |

**Returns**

Polars DataFrame with schema `PLAY_CONTEXT_SHOTS_SCHEMA` — one row per field-goal attempt. Empty input returns the zero-row schema.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `possession_number` | integer | Possession number. |
| `order_index` | integer |  |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | integer | Unique team identifier. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `shot_value` | integer | Point value of the shot (2 or 3). |
| `shot_made` | logical |  |
| `ctg_shot_zone` | character |  |
| `is_assisted` | logical |  |
| `is_putback` | logical |  |
| `is_second_chance_shot` | logical |  |
| `shot_context` | character |  |

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
assemble` — the two frames are always built from a single
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
| `game_id` | character | Unique game identifier. |
| `possession_number` | integer | Possession number. |
| `player_id` | integer | Unique player identifier. |
| `team_id` | integer | Unique team identifier. |
| `fg2a` | integer |  |
| `fg2m` | integer |  |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3m` | integer | Three-point field goals made. |
| `fta` | integer | Free throw attempts. |
| `ftm` | integer | Free throws made. |

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

`possessions` with Boolean `is_garbage_time` and Utf8 `garbage_time_basis` (`"margin_and_starters"` or `"margin_only"`) appended.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | integer | Possession number. |
| `offense_team_id` | integer | Unique identifier for offense team. |
| `defense_team_id` | integer |  |
| `start_order_index` | integer |  |
| `end_order_index` | integer |  |
| `start_seconds_remaining` | double |  |
| `end_seconds_remaining` | double |  |
| `points` | integer | Points scored. |
| `is_second_chance` | logical |  |
| `number_in_period` | integer |  |
| `possession_start_type` | character |  |
| `count_as_possession` | logical |  |
| `fg2a` | integer |  |
| `fg2m` | integer |  |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3m` | integer | Three-point field goals made. |
| `fta` | integer | Free throw attempts. |
| `ftm` | integer | Free throws made. |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `tov` | integer | Turnovers. |
| `is_garbage_time` | logical |  |
| `garbage_time_basis` | character |  |

**Example**

```python
poss = flag_garbage_time(poss, pbp)
print(poss.filter(pl.col("is_garbage_time") == True).height)

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
| `game_id` | character | Unique game identifier. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | integer | Possession number. |
| `offense_team_id` | integer | Unique identifier for offense team. |
| `defense_team_id` | integer |  |
| `start_order_index` | integer |  |
| `end_order_index` | integer |  |
| `start_seconds_remaining` | double |  |
| `end_seconds_remaining` | double |  |
| `points` | integer | Points scored. |
| `is_second_chance` | logical |  |
| `number_in_period` | integer |  |
| `possession_start_type` | character |  |
| `count_as_possession` | logical |  |
| `fg2a` | integer |  |
| `fg2m` | integer |  |
| `fg3a` | integer | Three-point field goal attempts. |
| `fg3m` | integer | Three-point field goals made. |
| `fta` | integer | Free throw attempts. |
| `ftm` | integer | Free throws made. |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `tov` | integer | Turnovers. |
| `off_player_1` | integer |  |
| `off_player_2` | integer |  |
| `off_player_3` | integer |  |
| `off_player_4` | integer |  |
| `off_player_5` | integer |  |
| `def_player_1` | integer |  |
| `def_player_2` | integer |  |
| `def_player_3` | integer |  |
| `def_player_4` | integer |  |
| `def_player_5` | integer |  |
| `lineup_source` | character |  |
| `is_heave_possession` | logical |  |

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
| `player_id` | character | Unique player identifier. |
| `player` | character | Player name. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `team_id` | character | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `season` | integer | Season year. |
| `salary` | double | Total cap-counting salary for the season ($). |
| `cap_allocation` | double | Cap allocation for the season (USD). |
| `team_option` | logical | Whether the season is a team option. |
| `player_option` | logical | Whether the season is a player option. |
| `two_way` | logical | Whether it is a two-way contract. |
| `qualifying_offer` | logical | Whether it is a qualifying offer. |

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
| `offense_team_id` | integer | Unique identifier for offense team. |
| `lineup_id` | character |  |
| `off_player_1` | integer |  |
| `off_player_2` | integer |  |
| `off_player_3` | integer |  |
| `off_player_4` | integer |  |
| `off_player_5` | integer |  |
| `poss` | integer | Poss. |
| `points` | integer | Points scored. |
| `pts_per_100` | double |  |
| `transition_poss` | integer |  |
| `transition_points` | integer |  |
| `transition_freq` | double |  |
| `transition_pts_per_100` | double |  |
| `non_transition_pts_per_100` | double |  |
| `transition_pts_added_per_100` | double |  |
| `halfcourt_poss` | integer |  |
| `halfcourt_pts_per_100` | double |  |
| `freq_off_steal` | double |  |
| `freq_off_live_rebound` | double |  |

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

### nba_foul_drawing {#nba_foul_drawing}

`nba_foul_drawing(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, advanced: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Expected FTA + residual foul-drawing skill from Synergy play-type mix.

`lg_ft_rate_t` = poss-weighted league mean of `ft_freq` for play type
`t` (`Σ_players(ft_freq_t·poss_t) / Σ_players(poss_t)`).
`expected_fta = scale · Σ_t poss_t · lg_ft_rate_t`;
`foul_draw_skill = 100·(fta − expected_fta)/poss`.

`ft_freq` (Synergy's `ft_poss_pct`) counts FT-drawing *trips*, not
individual free-throw attempts (a shooting foul is typically a 2-shot
trip) -- `scale = Σ actual fta / Σ raw trip-based estimate` is derived
from the fetched season itself (never hard-coded) so the trip-to-attempt
conversion self-normalizes and `Σ expected_fta ≡ Σ fta` holds exactly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `fta`, `poss` (bypasses the live fetch). |
| `advanced` | `Optional[DataFrame]` | `None` | Injected `Advanced`-measure frame with `player_id`, `pfd` (personal fouls drawn); optional -- `pfd` is `null` when omitted (`fta` is the always-present proxy). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `ft_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`fta`/ `expected_fta`/`foul_draw_skill` (Float64), `pfd` (Float64, null when *advanced* has no data for that player or is omitted). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_foul_drawing
f = nba_foul_drawing("2023-24")
print(f.sort("foul_draw_skill", descending=True).head(10))

# Injected offline (oracle / test) path

f = nba_foul_drawing("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

f.filter(pl.col("poss") >= 200).sort("foul_draw_skill", descending=True)
```

### nba_l2m {#nba_l2m}

`nba_l2m(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse an NBA Last Two Minute report from official.nba.com.

Retrieves the L2M report for a given game and returns parsed tables of calls,
game metadata, and error statistics. The game_id is zero-padded to 10 digits
(e.g., 42500405 becomes "0042500405"). A report is published for any game that
is within 3 points (5 points before the 2017-18 season) at any point during the
last two minutes of the fourth quarter or overtime -- not only playoff games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | NBA game ID (can be int or str). Automatically zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a dict with keys `"calls"`, `"game"`, `"stats"` mapping to DataFrames as documented in `parse_nba_l2m`.

| col_name | type | description |
|---|---|---|
| `calls.game_id` | character | 10-digit NBA game id (zero-padded), the join key back to the L2M game and stats tables. |
| `calls.period` | integer | Period number extracted from period_name's digits: 4 for the fourth quarter, 5+ for overtime periods (Q5 = OT1 through Q8 = OT4). |
| `calls.period_name` | character | Raw NBA.com period label for the graded play, e.g. Q4 for the fourth quarter or Q5 for the first overtime. |
| `calls.pc_time` | character | Raw game clock string from the L2M report before normalization, in MM:SS or MM:SS.t tenths-of-a-second format. |
| `calls.seconds_remaining` | double | Seconds remaining in the period at the graded play, parsed out of pc_time. |
| `calls.call_type` | character | Raw "Call: Type" label from the report, e.g. "Foul: Shooting" or "Turnover:  24 Second Violation"; whitespace (including doubled spaces) is NOT cleaned here -- the cleaned, upper-cased parts are call and type. |
| `calls.call` | character | Upper-cased category before the colon in call_type, e.g. FOUL, TURNOVER, or STOPPAGE. |
| `calls.type` | character | Upper-cased detail after the colon in call_type, e.g. SHOOTING or 24 SECOND VIOLATION. |
| `calls.committing` | character | Player, team nickname, or coach responsible for the graded action. |
| `calls.disadvantaged` | character | Player or team nickname disadvantaged by the graded action, blank when not applicable. |
| `calls.decision` | character | Normalized grading decision: CC (correct call), CNC (correct non-call), IC (incorrect call), or INC (incorrect non-call); null when the report marks the play undetectable at game speed. |
| `calls.decision_raw` | character | Raw grading code from the report before normalization, including the rare NCC/NCI aliases folded into decision. |
| `calls.comment` | character | Grader's free-text explanation of the ruling, sometimes naming a player as "Last (TEAM)". |
| `calls.difficulty` | character | Grader's difficulty rating for the play, e.g. Observable, Difficult, Blatant/Hard, or Undetectable. |
| `calls.video_event_id` | character | L2M video event id from the report (source field VideolLink, sic); not a stats.nba.com play-by-play EVENTNUM, so it cannot be joined to pbp data. |
| `calls.pos_id` | integer | Possession id assigned by the report, rising monotonically across the game; rows sharing pos_start/pos_end belong to the same possession. |
| `calls.pos_start` | character | Game clock at the start of the possession containing this graded play. |
| `calls.pos_end` | character | Game clock at the end of the possession containing this graded play. |
| `calls.pos_team_id` | integer | 10-digit NBA team id of the team in possession during the graded play. |
| `game.game_id` | character | 10-digit NBA game id (zero-padded), the join key to the calls and stats tables. |
| `game.game_date` | date | Game date parsed from the report's local tip-off timestamp (no timezone offset is applied). |
| `game.season_type` | character | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `game.home_team_id` | integer | 10-digit NBA team id (1610612xxx) of the home team. |
| `game.away_team_id` | integer | 10-digit NBA team id (1610612xxx) of the away team. |
| `game.home_team_abbr` | character | Three-letter abbreviation of the home team. |
| `game.away_team_abbr` | character | Three-letter abbreviation of the away team. |
| `game.home_team_name` | character | Home team nickname as published in the report, e.g. Thunder rather than Oklahoma City Thunder. |
| `game.away_team_name` | character | Away team nickname as published in the report. |
| `game.home_score` | integer | Home team's final score for the game. |
| `game.away_score` | integer | Away team's final score for the game. |
| `game.l2m_comments` | character | Report-level note from the league, e.g. a technical-issue disclaimer about missing video; null for almost every game. |
| `stats.game_id` | character | 10-digit NBA game id (zero-padded), the join key to the calls and game tables. |
| `stats.stat_name` | character | Name of the report-level error-rate statistic: Calls, Errors in Favor, or Possessions in Favor. |
| `stats.home` | integer | Value of stat_name attributed to the home team. |
| `stats.away` | integer | Value of stat_name attributed to the away team. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m
result = nba_l2m("0042500405")
calls = result["calls"]
print(f"Game had {calls.height} tracked plays in the L2M window")

# Parse as pandas instead

result = nba_l2m("0042500405", return_as_pandas=True)
calls_pd = result["calls"]

# Access raw JSON

payload = nba_l2m("0042500405", raw=True)
print(payload["game"])
```

### nba_l2m_games {#nba_l2m_games}

`nba_l2m_games(season: 'int | str', *, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch the list of games with Last Two Minute reports for an NBA season.

Retrieves and parses the L2M season index page from official.nba.com,
returning a table of all games for which L2M reports exist. JSON reports
exist only from 2019-01-01 onward; earlier seasons' index pages list PDFs,
which this function ignores. A release loader for historical (PDF-era)
reports is planned but does not exist yet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int \| str` |  | The NBA season (end year) as a 4-digit int or string, e.g. 2026 for the 2025-26 season. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

A `polars.DataFrame` (or `pandas.DataFrame` when `return_as_pandas=True`) with schema `{"game_id": Utf8, "season": Int32, "season_type": Utf8, "label": Utf8}`, one row per unique game ID in page order.

| col_name | type | description |
|---|---|---|
| `game_id` | character | 10-digit NBA game id (zero-padded), parsed from the season listing page's report link. |
| `season` | integer | NBA season end year passed to the function (e.g. 2026 for the 2025-26 season), stamped onto every row. |
| `season_type` | character | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `label` | character | Matchup label text scraped from the listing page link, e.g. "Thunder 125, Rockets 124 (2OT)". |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m_games
df = nba_l2m_games(2026)
print(f"Season had {df.height} games with L2M reports")

# Get playoff games only

df = nba_l2m_games(2026)
playoffs = df.filter(df["season_type"] == "playoffs")

# Convert to pandas

df = nba_l2m_games(2026, return_as_pandas=True)
```

### nba_play_context {#nba_play_context}

`nba_play_context(game_id: 'str', league_id: 'str' = '00', *, transition_seconds: 'float' = 6.0, transition_variant: 'str' = 'hoop_math', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch one game and return its possessions with the full CTG play-context surface.

Single live call to `nba_stats_playbyplayv3`, then
`add_play_context`. Works for NBA (`league_id="00"`), WNBA and the
G-League — `stats.wnba.com` ships the same play-by-play shapes, and every
threshold here is league-agnostic.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | Ten-character game identifier (e.g. `"0022200001"`). |
| `league_id` | `str` | `'00'` | League identifier (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `transition_seconds` | `float` | `6.0` | Transition initial-play cutoff (default 10.0). |
| `transition_variant` | `str` | `'hoop_math'` | See `add_transition`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Possession frame with the play-context columns. Empty/malformed payloads return a zero-row frame — never raises.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_play_context import nba_play_context
poss = nba_play_context("0022200001")
print(poss["possession_start_type_ctg"].value_counts())

# Transition rate for the game

import polars as pl
clean = poss.filter(
    (pl.col("is_garbage_time") == False) & (pl.col("is_heave_possession") == False)
)
print(clean["is_transition"].mean())
```

### nba_player_ages {#nba_player_ages}

`nba_player_ages(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Per-player age for a season (bulk), for the DARKO aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguedashplayerbiostats` replacement for offline tests. |

**Returns**

Frame `player_id:Int64, age:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_player_ages
ages = nba_player_ages("2023-24")
print(ages.head())
```

### nba_player_identity {#nba_player_identity}

`nba_player_identity(player_logs: 'pl.DataFrame') -> 'pl.DataFrame'`

Human-readable identity for every player in a season's box logs.

Model outputs key on `player_id` alone, which makes them unusable without a
second lookup -- a leaderboard reads `1628983` instead of
`Shai Gilgeous-Alexander`. This derives the display columns from the season's
own game logs, so they are **season-accurate**: a player's team is what he
actually played for that year, not his current one (which is what a player
directory would give and would silently mislabel every historical season).

A traded player has rows for several teams. `team_*` is his **primary** team
by minutes -- the one a reader means when they say "his team that season" --
and `teams` lists every abbreviation he appeared for, in descending minutes,
so a trade is visible rather than silently collapsed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | Per-player-per-game rows from `leaguegamelog` (the `player_or_team="P"` variant), carrying `player_id`, `player_name`, `team_id`, `team_abbreviation`, `team_name` and `min`. |

**Returns**

One row per `player_id` with `PLAYER_IDENTITY_SCHEMA`. An empty input -- or one missing any required column, `min` included -- gives the zero-row frame with that schema, so callers can join unconditionally. `min` is required rather than optional: without it every team totals zero minutes and "primary team" quietly degrades to whichever `team_id` sorts first, which looks like an answer but is not one.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | character | Nested list of member-team membership spans. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_player_identity

logs = pl.DataFrame({
    "player_id": [1628983],
    "player_name": ["Shai Gilgeous-Alexander"],
    "team_id": [1610612760],
    "team_abbreviation": ["OKC"],
    "team_name": ["Oklahoma City Thunder"],
    "min": [34.0],
})
ratings = pl.DataFrame({"player_id": [1628983], "war": [21.9]})
named = ratings.join(nba_player_identity(logs), on="player_id", how="left")
print(named.select("player_name", "team_name", "war"))
```

### nba_player_positions {#nba_player_positions}

`nba_player_positions(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Fetch league-wide listed positions for a season as numeric 1-5.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_playerindex` replacement for offline tests. |

**Returns**

Frame with columns `player_id:Int64, position_num:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_player_positions
pos = nba_player_positions("2023-24")
print(pos.head())

# Offline / injectable fetch for testing

import polars as pl
stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
pos = nba_player_positions("2023-24", fetch=stub)
```

### nba_player_props {#nba_player_props}

`nba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player expected prop lines + team pace projection for a matchup.

Loads the season's player box logs + team ratings, computes per-minute
`player_rates`, and projects each player's line onto their mean
minutes and the matchup's pace factor (`exp_poss / avg_pace`). Only the
two teams in the matchup are returned.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024`). |
| `game_id` | `str` |  | The game id (passed through for the caller's join; not used to filter historical rates). |
| `home_team_id` | `str` |  | Home team id. |
| `away_team_id` | `str` |  | Away team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player on either team: `player_id, team_id, stat_pts_exp, stat_reb_exp, stat_ast_exp, stat_fg3m_exp, pace_proj`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `team_id` | character | Unique team identifier. |
| `stat_pts_exp` | double |  |
| `stat_reb_exp` | double |  |
| `stat_ast_exp` | double |  |
| `stat_fg3m_exp` | double |  |
| `pace_proj` | double |  |

**Example**

```python
from sportsdataverse.nba.nba_player_props import nba_player_props
props = nba_player_props(2024, "401585828", "2", "6")
```

### nba_playtype_ratings {#nba_playtype_ratings}

`nba_playtype_ratings(season: 'str', *, league_id: 'str' = '00', off_team: 'Optional[pl.DataFrame]' = None, def_team: 'Optional[pl.DataFrame]' = None, schedule: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season play-type-adjusted offensive/defensive team ratings.

Fetches (or uses injected) Synergy offensive/defensive team frames plus the
league schedule, computes raw per-type efficiency
(`raw_playtype_efficiency`), opponent-adjusts it
(`adjust_playtype_efficiency`), then rolls up to one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id` (Int64), `adj_off`/`adj_def`/`adj_net` (Float64) roll-ups, plus per-type wide columns `adj_off_ppp_<playtype>`/`adj_def_ppp_<playtype>`/`off_freq_<playtype>` (Float64) for each play type present in the data. `adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100` (symmetric for `adj_def` off `def_freq_t`); `adj_net = adj_off - adj_def`. Returns a zero-row frame with the base roll-up schema when the upstream fetch is empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_playtype_ratings
r = nba_playtype_ratings("2023-24")
print(r.sort("adj_off", descending=True).head(10))

# Injected offline (oracle / test) path

r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

# Pipeline next step

r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)
```

### nba_ratings_panel {#nba_ratings_panel}

`nba_ratings_panel(model: 'AnyModel', possessions: 'pl.DataFrame', dates: 'Optional[Sequence[datetime.date]]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Player-ratings-through-date long panel: one row per (player_id, date).

Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
calls `ratings_as_of` independently, so the panel is leakage-free by
construction: a possession dated after a given checkpoint can never affect
that checkpoint's row, no matter what other dates are also being computed
or what future rows exist in `possessions`. Cost is a full refit per
checkpoint date; for a season's sparse RAPM-family design this is seconds
per date, not minutes — acceptable for a nightly/daily cadence but not for
live in-game updating (out of scope; see spec non-goals).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel`. |
| `possessions` | `DataFrame` |  | A possession+lineup frame with a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `dates` | `Optional[Sequence[date]]` | `None` | Checkpoint dates to compute. `None` (default) uses every distinct `game_date` present in `possessions`, sorted ascending — a rating for every game day, matching what EPM/LEBRON publish nightly. Duplicates are deduped; input order does not matter (the output is always sorted by date). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Long frame with `RATINGS_PANEL_SCHEMA` columns (`player_id`, `date`, `o_rating`, `d_rating`, `rating`). Zero-row (that schema) when `possessions` is empty or no date yields any players.

No returns table is published for this function: no capture: its possessions input needs a game_date column, and neither nba_possessions nor the released possessions carry one.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
print(panel.filter(pl.col("player_id") == 201939).sort("date"))

# Every game day, no explicit grid

panel = nba_ratings_panel(RidgeRapmModel(), season_poss)
```

### nba_raw_store_season_frame {#nba_raw_store_season_frame}

`nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"`

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **END** year (2024 = 2023-24) -- the key both halves of the store are filed under. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | integer | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | double | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | double | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | double | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Rebounds per game. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |
| `video_available` | integer | Video available. |

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```

### nba_referee_assignments {#nba_referee_assignments}

`nba_referee_assignments(date: 'str | _dt.date', *, league: 'str' = 'nba', raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse NBA referee assignments for a given date from official.nba.com.

Retrieves one league's referee crew assignments and replay-center officials for a
date: NBA, G-League or WNBA, picked by `league` (`raw=True` returns all three
leagues' blocks). The `crew_position` column (1–4) represents the feed's slot
order; slot 1 is inferred to be the crew chief. The `season` column converts
from the feed's format to an END year: START+1 for NBA/G-League
(two-calendar-year seasons) and START unchanged for WNBA.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `str \| date` |  | The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date). |
| `league` | `str` | `'nba'` | The league to extract ("nba", "gl", or "wnba"). Defaults to "nba". |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict with keys "nba", "gl", "wnba". Otherwise, a dict with keys `"officials"` and `"replay_center"` mapping to DataFrames as documented in `parse_nba_referee_assignments`.

| col_name | type | description |
|---|---|---|
| `officials.league` | character | League the assignment belongs to: nba, gl (G League), or wnba. |
| `officials.game_id` | character | 10-digit game id (zero-padded) for the assigned game. |
| `officials.game_date` | date | Game date parsed from the feed's MM/DD/YYYY format. |
| `officials.season` | integer | Season end year, converted from the feed's <season-type digit><start year> code: start year + 1 for NBA/G League's two-calendar-year seasons, start year unchanged for WNBA's single-year seasons. |
| `officials.season_type` | character | Season type decoded from the feed's season code first digit: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `officials.game_code` | character | League game code in YYYYMMDD/AWYHOM format, matching the away and home team abbreviations. |
| `officials.home_team_id` | integer | 10-digit team id of the home team. |
| `officials.home_team_abbr` | character | Three-letter abbreviation of the home team. |
| `officials.away_team_id` | integer | 10-digit team id of the away team. |
| `officials.away_team_abbr` | character | Three-letter abbreviation of the away team. |
| `officials.crew_position` | integer | Feed's official slot order (1-4); slot 1 is inferred to be the crew chief since the API does not label roles. |
| `officials.official_id` | integer | Numeric official id from the feed (source field official{n}_code); expected to match stats.nba.com's OFFICIAL_ID. |
| `officials.official_name` | character | Official's display name for this crew slot. |
| `officials.jersey_num` | character | Official's jersey number as a string, from the feed's official{n}_JNum field. |
| `replay_center.league` | character | League the replay-center staffing belongs to: nba, gl, or wnba. |
| `replay_center.game_date` | date | Date the replay-center official worked; a date-level staffing record, not tied to one game. |
| `replay_center.official_id` | integer | Numeric replay-center official id from the feed. |
| `replay_center.official_name` | character | Replay-center official's display name for that date. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_referee_assignments
result = nba_referee_assignments("2026-06-13")
officials = result["officials"]
print(f"Found {officials.height} official slots")

# Fetch WNBA assignments for the same date

result = nba_referee_assignments("2026-06-13", league="wnba")
wnba_officials = result["officials"]
```
