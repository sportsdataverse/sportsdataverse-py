---
title: "Package — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 10
description: "Package — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Analytics

### defense_vs_position {#defense_vs_position}

`defense_vs_position(pbp: 'pl.DataFrame', rosters: 'pl.DataFrame', league: 'str') -> 'pl.DataFrame'`

EPA/play, success and explosive rate each defense allowed to QBs, RBs, WRs and TEs.

Population: plays from scrimmage on a numbered down (1-4) with an EPA, in the
regular season or postseason -- CFB `EPA_scrimmage` not null and `seasonType`
2/3 (the `sportsdataverse.rolling_windows` population), NFL `play_type`
pass/run and `season_type` REG/POST. Filter season types first to narrow it.

A dropback (`pass`: attempts and sacks, plus NFL scrambles) is the QB's; a
carry (`rush`) goes to the rusher's roster group and a target to the
receiver's, so one completion counts for QB and for its receiver's group. The
roster is matched on `(season, player id)`: CFB `athlete_id` with
`position_abbreviation` (or `position` when that is the abbreviation, the
older shape), NFL `gsis_id` with `position`. A carrier or receiver with no
roster row, an `other` position or two different groups that season counts
in no group, and so does a target with no receiver id. CFB roster positions
are usable from 2014; the 2004-2013 releases list nearly every player as
unknown (`-`), so those seasons get QB dropbacks and little else.

Rates: `success` is EPA > 0; `explosive` is a dropback with EPA >= 2.4 or
a carry with EPA >= 1.8 (cfb_pbp's `EPA_explosive`). `sack_rate_allowed`
is sacks per dropback, `rush_yards_per_carry_allowed` the carries' rushing
yards (CFB `yds_rushed`, `statYardage` where it is null), `yards_per_target_allowed` receiving yards per target (0 on an
incompletion or interception). Extras are null on groups whose plays do not
make them real. `games` counts the games with at least one of the cell's
plays, and `qualified` is `games >= MIN_GAMES`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `pl.DataFrame` |  | released plays, any number of seasons -- CFB `load_cfb_pbp` or NFL `load_nfl_model_pbp` (project to `PBP_COLUMNS[league]`). |
| `rosters` | `pl.DataFrame` |  | the same seasons' rosters -- CFB `load_cfb_rosters` (`season`, `athlete_id`, `position_abbreviation` / `position`), NFL `load_nfl_rosters` (`season`, `gsis_id`, `position`). |
| `league` | `str` |  | `"cfb"` or `"nfl"`. |

**Returns**

one row per `(season, team_id, position_group)` the defense faced, `OUTPUT_SCHEMA`, sorted by those keys. `team_id` is the defense's ESPN team id as text (CFB `def_pos_team_id`) or its nflverse abbreviation (NFL `defteam`). Empty pbp gives an empty frame with the schema.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the plays, keyed as the source pbp keys it (CFB and NFL starting year). |
| `team_id` | character | The DEFENSE, as text: its ESPN team id (CFB def_pos_team_id) or its nflverse abbreviation (NFL defteam). |
| `position_group` | character | Group of the offensive player the plays went to: QB (every dropback, plus carries and targets of roster QBs), RB (roster RB or FB), WR or TE, from the season roster position. |
| `plays` | integer | Plays in the cell, each counted once per group; a completion counts for QB and for its receiver's group. |
| `games` | integer | Games with at least one of the cell's plays; the qualified floor counts these. |
| `epa_per_play_allowed` | double | Mean offensive EPA of the cell's plays (higher is worse for the defense). |
| `success_rate_allowed` | double | Share of the cell's plays with EPA > 0. |
| `explosive_rate_allowed` | double | Share of the cell's plays that were explosive (a dropback with EPA >= 2.4 or a carry with EPA >= 1.8, as cfb_pbp's EPA_explosive). |
| `dropbacks` | integer | QB rows only, null for other groups; dropbacks faced (pass attempts and sacks, plus NFL scrambles). |
| `sack_rate_allowed` | double | QB rows only, null for other groups; sacks per dropback. |
| `carries` | integer | RB rows only, null for other groups; carries by roster RBs and FBs. |
| `rush_yards_per_carry_allowed` | double | RB rows only, null for other groups; rushing yards per carry (CFB yds_rushed, statYardage where ESPN left it null). |
| `targets` | integer | WR and TE rows only, null for other groups; targets to the group's players that name a receiver. |
| `yards_per_target_allowed` | double | WR and TE rows only, null for other groups; receiving yards per target, 0 on an incompletion or interception. ESPN CFB names no receiver on most incompletions and every interception, so CFB targets are mostly completions and run high. |
| `qualified` | logical | True when games >= 3 (MIN_GAMES), the floor below which the producer gives no percentile. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp, load_cfb_rosters
from sportsdataverse.defense_vs_position import PBP_COLUMNS, defense_vs_position

pbp = load_cfb_pbp(2024).select(PBP_COLUMNS["cfb"])
dvp = defense_vs_position(pbp, load_cfb_rosters(2024), "cfb")

# The NFL twin (gsis ids, nflverse team abbreviations)

from sportsdataverse.nfl import load_nfl_model_pbp, load_nfl_rosters

pbp = load_nfl_model_pbp([2024]).select(PBP_COLUMNS["nfl"])
dvp_nfl = defense_vs_position(pbp, load_nfl_rosters([2024]), "nfl")

# Pipeline next step (one line)

dvp.filter((pl.col("position_group") == "TE") & (pl.col("qualified") == True)).sort("epa_per_play_allowed")
```

### football_attempts {#football_attempts}

`football_attempts(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Field-goal, air-yards, fourth-down and down-x-distance attempts from released `espn_{cfb,nfl}_pbp`.

Populations (regular season + postseason):

* `fg_pct_by_distance`: `fg_attempt` with a `yds_fg`; success = `fg_made`;
  player = the kicker.
* `cmp_pct_by_air_yards` / `epa_by_air_yards`: `pass_attempt` with
  `air_yards`; success = `completion` / `EPA_success`; player = the passer.
  CFB carries air yards from 2025 only (41% of attempts), so earlier seasons
  yield no rows.
* `fourth_conv_by_ytg`: fourth-down rushes and passes that stood (no nullifying
  penalty); success = a first down or the offense's own touchdown -- the
  `usage_box._standing_scrimmage` semantics. No player rows.
* `success_by_down_distance`: every standing scrimmage play on downs 1-4 with
  a distance; success = `EPA_success`; `down` is the second axis. No player
  rows. Distance follows standing_scrimmage` (clipped to 1-25, so ESPN's rare
  `distance == 0` lands in the 1-yard bucket).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | released pbp plays, any number of seasons (project to `FOOTBALL_ATTEMPT_COLUMNS`). |

**Returns**

one row per attempt x metric, `ATTEMPT_SCHEMA`; ids are text.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the play or shot, keyed as the source asset keys it. |
| `metric` | character | Curve the attempt feeds (a key of metric_curves.BUCKET_EDGES). |
| `player_id` | character | ESPN athlete id (text) of the kicker or passer credited with the attempt; null for fourth-down and down x distance plays. |
| `player_name` | character | Name of the credited player as the source carries it; null when no player is credited. |
| `team_id` | character | ESPN id (text) of the team on offense, from pos_team_id. |
| `team_name` | character | Offense's team label from the released pbp (pos_team). |
| `down` | integer | Down of the play (1-4) for success_by_down_distance; null for every other metric. |
| `x` | double | Position on the metric's axis in yards: yds_fg, air_yards or the standing-scrimmage distance to go. |
| `success` | logical | Whether the attempt succeeded: a make, a completion, an EPA success, a fourth-down conversion or a made shot. |
| `epa` | double | EPA of the play from the released pbp. |
| `id_source` | character | Always "espn": the ids are ESPN athlete and team ids. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp
from sportsdataverse.metric_curves import FOOTBALL_ATTEMPT_COLUMNS, football_attempts

pbp = load_cfb_pbp(2024).select(FOOTBALL_ATTEMPT_COLUMNS)
att = football_attempts(pbp)
att.filter(pl.col("metric") == "fg_pct_by_distance").head()

# Pipeline next step (one line)

att.group_by("metric").agg(pl.len(), pl.col("success").mean())
```

### football_events {#football_events}

`football_events(pbp: 'pl.DataFrame', game_dates: 'pl.DataFrame') -> 'pl.DataFrame'`

Dropback / target / carry / team-play events from released `espn_{cfb,nfl}_pbp` plays.

Population: plays from scrimmage on a numbered down (`EPA_scrimmage` not null,
`down` 1-4) in the regular season or postseason -- the population sdv-db's
player routes aggregate. `pass` includes sacks (a sack is a dropback); CFB has no
scramble flag, so a scramble is a carry.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | released pbp plays, any number of seasons (project to `FOOTBALL_PBP_COLUMNS`). |
| `game_dates` | `DataFrame` |  | `game_id` (int) and `game_date` (date) for every game in `pbp`. |

**Returns**

one row per event x metric (`epa`, `success_rate`), `EVENT_SCHEMA`.

No returns table is published for this function: no capture: its game_dates input needs game_id with game_date, and no package function returns that pair (load_cfb_schedule carries start_date).

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp, load_cfb_schedule
from sportsdataverse.rolling_windows import FOOTBALL_PBP_COLUMNS, football_events

pbp = load_cfb_pbp(2024).select(FOOTBALL_PBP_COLUMNS)
sched = load_cfb_schedule(2024)
game_dates = sched.select(
    pl.col("game_id").cast(pl.Int64),
    game_date=pl.col("start_date")
    .str.to_datetime(time_zone="UTC")
    .dt.convert_time_zone("America/New_York")
    .dt.date(),
)
ev = football_events(pbp, game_dates)
ev.filter(pl.col("window_unit") == "dropback").head()

# Pipeline next step (one line)

ev.group_by("entity_id", "season").agg(pl.col("value").mean())
```

### metric_curves {#metric_curves}

`metric_curves(attempts: 'pl.DataFrame', league: 'str') -> 'pl.DataFrame'`

League, team and player rate curves from an `ATTEMPT_SCHEMA` frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `attempts` | `DataFrame` |  | one row per attempt x metric (from an adapter), any number of seasons. |
| `league` | `str` |  | `"cfb"`, `"nfl"`, `"nba"` or `"wnba"`. The curves are league-agnostic; `league` only supplies the default `id_source` (`ID_SOURCE`) when the attempts frame carries no `id_source` column. An adapter's column wins, so the ESPN adapter on NFL pbp keeps `espn` whatever `league` says. |

**Returns**

one row per (season, entity, metric, down, bucket), `OUTPUT_SCHEMA`: * `entity_type` `league` (`entity_id` null), `team` and `player` (only attempts credited to a player; `team_id` is the team of most of them). * `x_lo` / `x_hi`: the attempt's bucket, inclusive / exclusive. * `attempts`, `successes`, `rate = successes / attempts` (exact), `epa_per_att` (mean EPA of the attempts that have one, else null). * `down`: only set for `success_by_down_distance`. A bucket with no attempt has no row.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the curve covers, keyed as the source asset keys it (CFB/NFL starting year, nba_stats ENDING year, WNBA calendar year). |
| `entity_type` | character | Aggregation level of the row: "league" (every attempt), "team" or "player". |
| `entity_id` | character | Text id of the entity the row describes, per entity_type: team rows carry the source's team id (ESPN pos_team_id, nflfastR posteam, stats.nba team_id) and player rows the source's player id (ESPN athlete id, nflfastR gsis id until the NFL producer re-keys it to ESPN, stats.nba person_id); null on league rows. |
| `entity_name` | character | Display name of the player or team as the source pbp/shots carry it; null on league rows. |
| `team_id` | character | On player rows, the team of most of the player's attempts that season (text id); null on league and team rows. |
| `id_source` | character | Id system of the row's ids, stamped by the adapter that built the attempts (espn, gsis, nba_stats or wnba_stats); the league argument only supplies the default when the attempts frame carries no id_source column. |
| `metric` | character | Curve name: fg_pct_by_distance, cmp_pct_by_air_yards, epa_by_air_yards, fourth_conv_by_ytg, success_by_down_distance or fg_pct_by_shot_distance. |
| `down` | integer | Down (1-4), the second axis of success_by_down_distance; null for every other metric. |
| `x_lo` | double | Inclusive lower edge of the bucket on the metric's axis, in yards (kick distance, air yards, yards to go) or feet (shot distance); the edges are fixed per metric in metric_curves.BUCKET_EDGES. |
| `x_hi` | double | Exclusive upper edge of the bucket on the same axis as x_lo; an attempt at exactly x_hi belongs to the next bucket up. |
| `attempts` | integer | Attempts in the bucket (field goals, pass attempts, fourth-down plays, scrimmage plays or shots); always positive, since an empty bucket has no row. |
| `successes` | integer | Successful attempts in the bucket: makes, completions, EPA successes (EPA > 0), fourth-down conversions or made shots. |
| `rate` | double | successes divided by attempts, computed exactly (no smoothing). |
| `epa_per_att` | double | Mean EPA of the bucket's attempts that carry an EPA; null for shots and for kicks without EPA. |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp
from sportsdataverse.metric_curves import FOOTBALL_ATTEMPT_COLUMNS, football_attempts, metric_curves

pbp = load_cfb_pbp(2024).select(FOOTBALL_ATTEMPT_COLUMNS)
curves = metric_curves(football_attempts(pbp), "cfb")
curves.filter((pl.col("metric") == "fg_pct_by_distance") & (pl.col("entity_type") == "league"))

# Pipeline next step (one line)

curves.filter((pl.col("entity_type") == "player") & (pl.col("attempts") >= 10)).sort("rate", descending=True)
```

### nflfastr_attempts {#nflfastr_attempts}

`nflfastr_attempts(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

The same attempts from `nfl_model_pbp` (the nflfastR shape), which carries air yards.

Populations (`REG` + `POST`):

* `fg_pct_by_distance`: `field_goal_attempt` with a `kick_distance`; success =
  `field_goal_result == "made"`; player = the kicker.
* `cmp_pct_by_air_yards` / `epa_by_air_yards`: `pass_attempt == 1 & sack == 0`
  with `air_yards`; success = `complete_pass` / `epa > 0` (nflfastR's success);
  player = the passer.
* `fourth_conv_by_ytg`: fourth-down `play_type` `pass` / `run` (a nullified
  play is `no_play`); success = a rushing or passing first down or the
  offense's own touchdown. No player rows.
* `success_by_down_distance`: every `pass` / `run` play on downs 1-4 with a
  `ydstogo`; success = `epa > 0`. No player rows.

Player ids are nflfastR gsis ids; the producer re-keys them to ESPN through the
players master and keeps `gsis_id` beside `entity_id`. Teams are nflfastR
abbreviations.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_nfl_model_pbp` plays, any number of seasons (project to `NFLFASTR_ATTEMPT_COLUMNS`). |

**Returns**

one row per attempt x metric, `ATTEMPT_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the play or shot, keyed as the source asset keys it. |
| `metric` | character | Curve the attempt feeds (a key of metric_curves.BUCKET_EDGES). |
| `player_id` | character | nflfastR gsis id (text) of the kicker or passer credited with the attempt; null for fourth-down and down x distance plays. |
| `player_name` | character | Name of the credited player as the source carries it; null when no player is credited. |
| `team_id` | character | nflfastR abbreviation of the team on offense (posteam). |
| `team_name` | character | nflfastR abbreviation of the team on offense (posteam), repeated as the label. |
| `down` | integer | Down of the play (1-4) for success_by_down_distance; null for every other metric. |
| `x` | double | Position on the metric's axis in yards: kick_distance, air_yards or ydstogo. |
| `success` | logical | Whether the attempt succeeded: a make, a completion, an EPA success, a fourth-down conversion or a made shot. |
| `epa` | double | EPA of the play from nfl_model_pbp. |
| `id_source` | character | Always "gsis": player ids are nflfastR gsis ids and teams are nflfastR abbreviations. |

**Example**

```python
import polars as pl
from sportsdataverse.nfl import load_nfl_model_pbp
from sportsdataverse.metric_curves import NFLFASTR_ATTEMPT_COLUMNS, metric_curves, nflfastr_attempts

pbp = load_nfl_model_pbp([2024]).select(NFLFASTR_ATTEMPT_COLUMNS)
curves = metric_curves(nflfastr_attempts(pbp), "nfl")
curves.filter((pl.col("metric") == "cmp_pct_by_air_yards") & (pl.col("entity_type") == "league"))
```

### rolling_windows {#rolling_windows}

`rolling_windows(events: 'pl.DataFrame', season: 'int', windows: 'dict[str, tuple[int, ...]] | None' = None) -> 'pl.DataFrame'`

Rolling-window form for every entity with an event in `season`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `DataFrame` |  | an `EVENT_SCHEMA` frame covering every season up to `season` (the career history the baselines read). |
| `season` | `int` |  | the season the rows describe; later seasons in `events` are ignored. |
| `windows` | `dict[str, tuple[int, ...]] \| None` | `None` | `{window_unit: (sizes...)}`; defaults to `WINDOWS`. |

**Returns**

one row per (entity, unit, metric, window size), `OUTPUT_SCHEMA`. Null / NaN event values are dropped before any window is computed. Columns: * `cur`: the mean of the entity's last `window_n` events through `season`. * `prev`: the mean of the `window_n` events immediately before `cur`'s window; null unless a full window of earlier history exists. * `season_start`: the mean of the `window_n` events immediately before season `season` started -- i.e. the entity's form entering the season, not counting any event actually played in `season`. * `career_baseline`: the mean of every event before `cur`'s window, including earlier events within `season` itself; null unless at least one full window of history precedes it. * `qualified`: `True` iff `n == window_n` -- the window is fully populated (not padded by a short career). Consumers building a "hottest" list should filter on this first. * `team_id` / `entity_name`: taken from the entity's single latest event through `season`, so a player who changed teams mid-season is labelled with their current team. * `delta_prev_rank`: 1 = biggest riser, ties share the lowest rank; null unless `qualified` and `prev` exists.

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `entity_type` | character | Kind of entity the alias record points at (e.g., "team", "player", "league"). |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `entity_name` | character |  |
| `team_id` | character |  |
| `metric` | character |  |
| `window_unit` | character |  |
| `window_n` | integer |  |
| `cur` | double |  |
| `prev` | double |  |
| `season_start` | double |  |
| `career_baseline` | double |  |
| `delta_prev` | double |  |
| `delta_season` | double |  |
| `delta_career` | double |  |
| `delta_prev_rank` | integer |  |
| `n` | integer |  |
| `qualified` | logical |  |
| `last_event_date` | character |  |
| `as_of_date` | character |  |

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp, load_cfb_schedule
from sportsdataverse.rolling_windows import FOOTBALL_PBP_COLUMNS, football_events, rolling_windows

pbp = load_cfb_pbp(2024).select(FOOTBALL_PBP_COLUMNS)
sched = load_cfb_schedule(2024)
game_dates = sched.select(
    pl.col("game_id").cast(pl.Int64),
    game_date=pl.col("start_date")
    .str.to_datetime(time_zone="UTC")
    .dt.convert_time_zone("America/New_York")
    .dt.date(),
)
ev = football_events(pbp, game_dates)
rw = rolling_windows(ev, 2024)
rw.filter(pl.col("window_unit") == "dropback").head()

# Pipeline next step (one line)

rw.filter(pl.col("qualified") & (pl.col("delta_prev_rank") == 1)).select(
    "entity_name", "window_unit", "window_n"
)
```

### shot_attempts {#shot_attempts}

`shot_attempts(shots: 'pl.DataFrame', league: 'str' = 'nba') -> 'pl.DataFrame'`

`fg_pct_by_shot_distance` attempts from released `{nba,wnba}_stats_shots`.

Regular-season (`season_type_id` `"2"`) and playoff (`"4"`) shots; success =
`shot_result == "Made"`; player = the shooter (stats.nba `person_id`); no EPA.

The distance binned is the exact release distance from the legacy coordinates,
`sqrt(x_legacy^2 + y_legacy^2) / 10` feet (tenths of a foot, rim at the origin, the
same on the NBA and WNBA feeds), not the feed's `shot_distance`: stats.nba
`playbyplayv3` reports `shot_distance` 0 for every three released under 23.5 ft
(15,378 NBA corner threes in 2025-26; most WNBA threes before the 2013 line move),
which put them in the 0-1 ft bucket and emptied 22-24 ft, and its whole-foot
rounding shifts every other bucket by half a foot. `shot_distance` is used only
when a coordinate is null.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | `load_{nba,wnba}_stats_shots` rows, any number of seasons (project to `SHOT_ATTEMPT_COLUMNS`). |
| `league` | `str` | `'nba'` | `"nba"` (default) or `"wnba"` -- which stats site the ids come from; stamps `id_source` `nba_stats` / `wnba_stats`. |

**Returns**

one row per shot, `ATTEMPT_SCHEMA`; `season` is the asset's key (END year for the NBA).

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the play or shot, keyed as the source asset keys it. |
| `metric` | character | Curve the attempt feeds (a key of metric_curves.BUCKET_EDGES). |
| `player_id` | character | stats.nba person_id (text) of the shooter. |
| `player_name` | character | Name of the credited player as the source carries it; null when no player is credited. |
| `team_id` | character | stats.nba team_id (text) of the shooting team. |
| `team_name` | character | Shooting team's tricode from the shot row (team_tricode). |
| `down` | integer | Down of the play (1-4) for success_by_down_distance; null for every other metric. |
| `x` | double | Shot distance in feet, as stats.nba records it. |
| `success` | logical | Whether the attempt succeeded: a make, a completion, an EPA success, a fourth-down conversion or a made shot. |
| `epa` | double | Always null: shots carry no EPA. |
| `id_source` | character | "nba_stats" or "wnba_stats" per the league argument: the stats site whose person_id and team_id the row carries. |

**Example**

```python
import polars as pl
from sportsdataverse.nba import load_nba_stats_shots
from sportsdataverse.metric_curves import SHOT_ATTEMPT_COLUMNS, metric_curves, shot_attempts

shots = load_nba_stats_shots([2024]).select(SHOT_ATTEMPT_COLUMNS)
curves = metric_curves(shot_attempts(shots), "nba")
curves.filter((pl.col("entity_type") == "player") & (pl.col("entity_id") == "201939"))
```

### shot_events {#shot_events}

`shot_events(shots: 'pl.DataFrame', game_dates: 'pl.DataFrame') -> 'pl.DataFrame'`

Field-goal-attempt events from released `{nba,wnba}_stats_shots`.

Population: regular-season (`season_type_id` `"2"`) and playoff (`"4"`) shots,
the population `sportsdataverse.metric_curves.shot_attempts` keeps; play-in
and NBA Cup final games do not count. Every attempt is an `fga` event (metric
`fg_pct`); a three-point attempt is also an `fg3a` event (metric `fg3_pct`).
`value` is 1.0 for a make, 0.0 for a miss. `seq` orders a game's shots by
period, then game clock running down, then the provider's row order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | released shots, any number of seasons (project to `SHOT_COLUMNS`). `season` is the asset's key: the ENDING year for the NBA, the calendar year for the WNBA. |
| `game_dates` | `DataFrame` |  | `game_id` (text, `"0022400007"`) and `game_date` (date) for every game in `shots`. |

**Returns**

one row per attempt x unit, `EVENT_SCHEMA`. `entity_id` is the stats.nba / stats.wnba `person_id`, not an ESPN id; `entity_name` is the provider's name, which is the family name only (`"Curry"`).

| col_name | type | description |
|---|---|---|
| `season` | integer |  |
| `entity_type` | character | Kind of entity the alias record points at (e.g., "team", "player", "league"). |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `entity_name` | character |  |
| `team_id` | character |  |
| `window_unit` | character |  |
| `metric` | character |  |
| `game_id` | character |  |
| `event_date` | character |  |
| `seq` | integer |  |
| `value` | double |  |

**Example**

```python
import polars as pl
from sportsdataverse.nba import load_nba_stats_schedules, load_nba_stats_shots
from sportsdataverse.rolling_windows import SHOT_COLUMNS, rolling_windows, shot_events

shots = load_nba_stats_shots([2024, 2025]).select(SHOT_COLUMNS)
sched = load_nba_stats_schedules([2024, 2025])
game_dates = sched.select("game_id", pl.col("game_date").str.slice(0, 10).str.to_date())
ev = shot_events(shots, game_dates.unique("game_id"))
rw = rolling_windows(ev, 2025)

# Pipeline next step (one line)

rw.filter((pl.col("window_unit") == "fg3a") & pl.col("qualified")).sort("delta_prev_rank")
```
