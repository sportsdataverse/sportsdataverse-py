---
title: Package — additional Python functions
sidebar_label: Additional functions
description: "Package — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# Package — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse`
not covered by the generated API-endpoint reference above.

## Other

### `cache_stats() -> 'Dict[str, Any]'` {#cache_stats}

Return a snapshot of the cache for debugging / inspection.

Returns a dict with `mode`, `entries`, and `disk_bytes` (only
populated when mode=filesystem). Cheap — doesn't read the cached
bodies, just counts + sizes.

### `college_baseball_re24(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#college_baseball_re24}

`sportsdataverse.baseball.college_run_expectancy.college_baseball_re24` fixed to `league="college_baseball"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | See the core function. |
| `state` | `Optional[DataFrame]` | `None` | See the core function. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

see the core function's Returns table.

| col_name | type | description |
|---|---|---|
| `base_state` | character | 3-char base occupancy code ("_" = empty, "1"/"2"/"3" = occupied), e.g. "1_3" for runners on first and third. |
| `outs` | integer | Outs at the start of the base-out state (0-2). |
| `run_expectancy` | double | Empirical mean runs scored from this state through the end of the half-inning (RE24). |
| `n` | integer | Number of plate appearances observed starting in this base-out state. |

**Example**

```python
from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_state, college_baseball_re24
state = college_baseball_state(raw)
matrix = college_baseball_re24(state=state)
```

### `college_baseball_state(plays: 'Dict[str, Any]') -> 'pl.DataFrame'` {#college_baseball_state}

`sportsdataverse.baseball.college_run_expectancy.college_baseball_state` fixed to `league="college_baseball"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `Dict[str, Any]` |  | Raw payload from `espn_college_baseball_game_plays(event_id, return_parsed=False)`. |

**Returns**

see the core function's Returns table.

**Example**

```python
from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_state
state = college_baseball_state(raw)
```

### `college_baseball_wpa(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#college_baseball_wpa}

`sportsdataverse.baseball.college_run_expectancy.college_baseball_wpa` fixed to `league="college_baseball"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | See the core function. |
| `state` | `Optional[DataFrame]` | `None` | See the core function. |
| `results` | `Optional[DataFrame]` | `None` | See the core function. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

see the core function's Returns table.

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id for the game (join key to the schedule). |
| `play_seq` | integer | Game-global sequential plate-appearance order. |
| `re_before` | double | RE24 of the base-out state before the PA. |
| `re_after` | double | RE24 of the base-out state after the PA. |
| `run_value` | double | re_after minus re_before, plus runs scored on the play. |
| `wpa` | double | Home-perspective win-probability added. |

**Example**

```python
from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_wpa
wpa = college_baseball_wpa(state=state, results=results)
```

### `college_softball_re24(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#college_softball_re24}

`sportsdataverse.baseball.college_run_expectancy.college_baseball_re24` fixed to `league="college_softball"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | See the core function. |
| `state` | `Optional[DataFrame]` | `None` | See the core function. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

see the core function's Returns table.

| col_name | type | description |
|---|---|---|
| `base_state` | character | 3-char base occupancy code ("_" = empty, "1"/"2"/"3" = occupied), e.g. "1_3" for runners on first and third. |
| `outs` | integer | Outs in the inning after the play. |
| `run_expectancy` | double | Empirical mean runs scored from this state through the end of the half-inning (RE24), fit on plate appearances outside the bottom of the 7th inning and later. |
| `n` | integer | Number of plate appearances observed starting in this base-out state, excluding the bottom of the 7th inning and later. |

**Example**

```python
from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_state, college_softball_re24
state = college_softball_state(raw)
matrix = college_softball_re24(state=state)
```

### `college_softball_state(plays: 'Dict[str, Any]') -> 'pl.DataFrame'` {#college_softball_state}

`sportsdataverse.baseball.college_run_expectancy.college_baseball_state` fixed to `league="college_softball"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `Dict[str, Any]` |  | Raw payload from `espn_college_softball_game_plays(event_id, return_parsed=False)`. |

**Returns**

see the core function's Returns table.

**Example**

```python
from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_state
state = college_softball_state(raw)
```

### `college_softball_wpa(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#college_softball_wpa}

`sportsdataverse.baseball.college_run_expectancy.college_baseball_wpa` fixed to `league="college_softball"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, List[int], None]` | `None` | See the core function. |
| `state` | `Optional[DataFrame]` | `None` | See the core function. |
| `results` | `Optional[DataFrame]` | `None` | See the core function. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` instead of polars. |

**Returns**

see the core function's Returns table.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `play_seq` | integer | 0-based game-global plate-appearance order (sorted by ESPN atBatId); joins back to college_softball_state. |
| `re_before` | double | RE24 of the base-out state before the PA, looked up in the matrix fit on the same state frame; 0.0 when that state is absent from the matrix. |
| `re_after` | double | RE24 of the base-out state after the PA (the next PA's before-state in the same half-inning); 0.0 for the last PA of a half-inning. |
| `run_value` | double | re_after minus re_before, plus runs scored on the play (change in the combined cumulative score). |
| `wpa` | double | Win probability added (WPA) for the posteam. |

**Example**

```python
from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_wpa
wpa = college_softball_wpa(state=state, results=results)
```

### `cricket_expected_runs(state_wp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cricket_expected_runs}

Expected remaining runs + run rate from a win-probability-scored state frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state_wp` | `DataFrame` |  | Output of `~sportsdataverse.cricket.cricket_win_prob.cricket_win_probability` (must carry `proj_final`, `runs`, `overs_left`). |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

The input rows plus `exp_runs_remaining:Float64` (`proj_final - runs`, floored at 0) and `exp_run_rate:Float64` (per remaining over; null when no overs remain). A zero-row input returns the schema with both columns appended (all null).

**Example**

```python
import polars as pl
from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
from sportsdataverse.cricket.cricket_wpa import cricket_expected_runs
scored = cricket_win_probability(state)
er = cricket_expected_runs(scored)
er.select("exp_runs_remaining", "exp_run_rate").head()
```

### `cricket_match_state(summary: 'dict', *, fmt: 'str', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cricket_match_state}

Extract over-level match state from an ESPN cricket summary/scoreboard payload.

One row per innings with a parseable competitor score string. The batting
side that carries a `target` in its score is the second innings (chasing);
the other is the first innings (setting).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `summary` | `dict` |  | Raw ESPN cricket `summary` or `scoreboard` payload (dict). |
| `fmt` | `str` |  | Format slug (`"t20"` / `"odi"`); validated via `~sportsdataverse.cricket.cricket_model_constants.get_format`. |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

A `polars.DataFrame` (or pandas) with the documented state schema; a zero-row frame when the payload is empty/malformed.

**Example**

```python
from sportsdataverse.cricket import espn_cricket_summary
from sportsdataverse.cricket.cricket_win_prob import cricket_match_state
state = cricket_match_state(espn_cricket_summary(event="1385691", return_parsed=False), fmt="t20")
print(state.shape)
```

### `cricket_win_probability(state: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cricket_win_probability}

In-play win probability for the batting/chasing team from match state.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state` | `DataFrame` |  | Over-level match state carrying the documented state schema (`event_id, innings_number, batting_team_id, runs, wickets, balls_bowled, balls_total, target, fmt`) — e.g. the output of `cricket_match_state`. |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

The input rows plus `overs_left:Int64`, `wickets_left:Int64`, `resources_left:Float64`, `proj_final:Float64`, `win_prob_raw:Float64` (parametric core) and `win_prob:Float64` (calibrated, the shipped estimate). A zero-row input returns the schema with these columns appended (all null).

**Example**

```python
import polars as pl
from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
st = pl.DataFrame([{ "event_id": "M1", "innings_number": 2,
    "batting_team_id": "A", "runs": 120, "wickets": 3,
    "balls_bowled": 90, "balls_total": 120, "target": 160, "fmt": "t20"}])
cricket_win_probability(st).select("win_prob").item()
```

### `cricket_wpa(state_wp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#cricket_wpa}

Batting/bowling win-probability added per over/wicket transition.

`wpa_batting` is the change in the batting team's win probability since the
previous state within the same innings; `wpa_bowling` is its negation (the
bowling side gains exactly what the batting side loses). The lead is taken
`.over(["event_id", "innings_number"])` so no change leaks across matches or
innings, and the first state of each innings has `wpa_batting = 0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `state_wp` | `DataFrame` |  | Output of `~sportsdataverse.cricket.cricket_win_prob.cricket_win_probability` (must carry `event_id`, `innings_number`, `balls_bowled`, `win_prob`). |
| `return_as_pandas` | `bool` | `False` | When True, return a `pandas.DataFrame`. |

**Returns**

The input rows (sorted by `event_id, innings_number, balls_bowled`) plus `win_prob_before:Float64`, `wpa_batting:Float64` and `wpa_bowling:Float64`. A zero-row input returns the schema with those columns appended (all null).

**Example**

```python
from sportsdataverse.cricket.cricket_win_prob import cricket_win_probability
from sportsdataverse.cricket.cricket_wpa import cricket_wpa
wpa = cricket_wpa(cricket_win_probability(state))
wpa.select("wpa_batting", "wpa_bowling").head()
```

### `decompose_college_baseball_plays(rows: "'list[dict]'", *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"` {#decompose_college_baseball_plays}

Decompose pre-extracted play rows into the full `PBP_SCHEMA` frame.

The row-level half of `parse_college_baseball_ncaa_pbp` -- the play-text
decomposition engine without the HTML extraction. This is the entry point
for sources that already hold the base play fields, e.g. the legacy R-era
`baseballr-data` trees (2012-2023: `description`/`inning`/
`inning_top_bot`/`batting`/`fielding`/`score`), so legacy and
freshly captured games resolve into IDENTICAL pbp columns.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rows` | `list[dict]` |  | One dict per play. Recognized keys (all optional except `description`): `contest_id`, `inning` (int), `inning_top_bot` (`"top"`/`"bot"`), `batting`, `fielding`, `play_number`, `score_away`/`score_home` (ints) or a combined `score` string (`"3-2"`, away-home), and `description`. Unrecognized keys are ignored; `play_number` defaults to the 1-based position in *rows*. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of `polars`. |

**Returns**

One row per input play with every text-derivable `PBP_SCHEMA` column populated (`play_type`, hit/out flags, `rbi`, `pitch_sequence`, runner movement, ...). Empty input returns a zero-row frame with the documented schema.

**Example**

```python
from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays
df = decompose_college_baseball_plays(
    [{"inning": 1, "inning_top_bot": "top", "score": "0-0",
      "description": "Jack Moss singled to left field (1-2 KBFX)."}]
)
print(df.select("play_type", "is_hit", "pitch_sequence").row(0))
```

### `football_attempts(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#football_attempts}

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

### `football_events(pbp: 'pl.DataFrame', game_dates: 'pl.DataFrame') -> 'pl.DataFrame'` {#football_events}

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

### `get_cache_mode() -> 'str'` {#get_cache_mode}

Return the current cache mode.

### `mch_ratings(dates: 'list[str]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#mch_ratings}

MCH opponent-adjusted goal-margin ratings over a set of scoreboard dates.

Fetches `espn_mch_scoreboard` for each date in `dates`, concatenates
the completed games, and adjusts with
`sportsdataverse.hockey.college_hockey_ratings.college_hockey_ratings`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `list[str]` |  | `YYYYMMDD` date strings to fetch (ESPN has no single "whole season" scoreboard endpoint; the caller supplies the date sweep -- see `dev/league_ports/capture_wch_and_scoreboards.py` for the sweep used to build the committed oracle fixture). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

One row per team: `team_id, adj_off, adj_def, adj_net, raw_off, raw_def, games`.

**Example**

```python
from sportsdataverse.hockey.mch import mch_ratings
ratings = mch_ratings(["20250118", "20250201"])
ratings.sort("adj_net", descending=True).head()
```

### `metric_curves(attempts: 'pl.DataFrame', league: 'str') -> 'pl.DataFrame'` {#metric_curves}

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

### `nflfastr_attempts(pbp: 'pl.DataFrame') -> 'pl.DataFrame'` {#nflfastr_attempts}

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

### `pff_aaf_facet_blocking_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_blocking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_blocking_summary()
```

### `pff_aaf_facet_coverage_scheme(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_coverage_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_scheme()
```

### `pff_aaf_facet_coverage_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_coverage_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_summary()
```

### `pff_aaf_facet_defense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_defense_summary()
```

### `pff_aaf_facet_field_goal_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_field_goal_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_field_goal_summary()
```

### `pff_aaf_facet_kicking_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_kicking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_kicking_summary()
```

### `pff_aaf_facet_offense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_offense_summary()
```

### `pff_aaf_facet_pass_blocking(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_pass_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_blocking()
```

### `pff_aaf_facet_pass_rush_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_pass_rush_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_rush_summary()
```

### `pff_aaf_facet_passing_allowed_pressure(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_passing_allowed_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_allowed_pressure()
```

### `pff_aaf_facet_passing_concept(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_passing_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_concept()
```

### `pff_aaf_facet_passing_depth(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_passing_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_depth()
```

### `pff_aaf_facet_passing_detail_stats(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_passing_detail_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_detail_stats()
```

### `pff_aaf_facet_passing_pressure(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_passing_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_pressure()
```

### `pff_aaf_facet_passing_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_summary()
```

### `pff_aaf_facet_pbes(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_pbes}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pbes()
```

### `pff_aaf_facet_prps(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_prps}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_prps()
```

### `pff_aaf_facet_punting_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_punting_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_punting_summary()
```

### `pff_aaf_facet_receiving_concept(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_receiving_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_concept()
```

### `pff_aaf_facet_receiving_coverage(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_receiving_coverage}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage()
```

### `pff_aaf_facet_receiving_coverage_stats(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_receiving_coverage_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage_stats()
```

### `pff_aaf_facet_receiving_depth(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_receiving_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_depth()
```

### `pff_aaf_facet_receiving_scheme(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_receiving_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_scheme()
```

### `pff_aaf_facet_receiving_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_summary()
```

### `pff_aaf_facet_return_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_return_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_return_summary()
```

### `pff_aaf_facet_run_blocking(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_run_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_blocking()
```

### `pff_aaf_facet_run_defense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_run_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_defense_summary()
```

### `pff_aaf_facet_rushing_direction_stats(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_rushing_direction_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_direction_stats()
```

### `pff_aaf_facet_rushing_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_summary()
```

### `pff_aaf_facet_slot_coverages(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_slot_coverages}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_slot_coverages()
```

### `pff_aaf_facet_special_teams_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_special_teams_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_special_teams_summary()
```

### `pff_aaf_facet_time_in_pockets(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_time_in_pockets}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_time_in_pockets()
```

### `pff_aaf_games(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_games}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[int]` | `None` | Single week number. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_games()
```

### `pff_aaf_leagues(headers: 'Optional[Dict[str, str]]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_leagues}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Leagues + seasons + week groups (bootstrap)

Endpoint: `GET https://premium.pff.com/api/v1/leagues`
Example URL: https://premium.pff.com/api/v1/leagues

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_leagues()
```

### `pff_aaf_player_defense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_defense_summary()
```

### `pff_aaf_player_offense_blocking(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_offense_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_blocking()
```

### `pff_aaf_player_offense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_summary()
```

### `pff_aaf_player_passing_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_passing_summary()
```

### `pff_aaf_player_position_pivot(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_position_pivot}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_position_pivot()
```

### `pff_aaf_player_receiving_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_receiving_summary()
```

### `pff_aaf_player_rushing_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_rushing_summary()
```

### `pff_aaf_player_seasons(*, league: 'Optional[str]' = 'aaf', player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_seasons}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_seasons()
```

### `pff_aaf_player_snaps_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_player_snaps_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_snaps_summary()
```

### `pff_aaf_players(*, league: 'Optional[str]' = 'aaf', name: 'Optional[str]' = None, id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_players}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `Optional[str]` | `None` | Player-name search prefix. |
| `id` | `Optional[int]` | `None` | Entity id (player lookup). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_players()
```

### `pff_aaf_teams(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_teams}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams()
```

### `pff_aaf_teams_overview(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_teams_overview}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams_overview()
```

### `pff_ncaa_facet_blocking_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_blocking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_blocking_summary()
```

### `pff_ncaa_facet_coverage_scheme(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_coverage_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_scheme()
```

### `pff_ncaa_facet_coverage_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_coverage_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_summary()
```

### `pff_ncaa_facet_defense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_defense_summary()
```

### `pff_ncaa_facet_field_goal_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_field_goal_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_field_goal_summary()
```

### `pff_ncaa_facet_kicking_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_kicking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_kicking_summary()
```

### `pff_ncaa_facet_offense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_offense_summary()
```

### `pff_ncaa_facet_pass_blocking(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_pass_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_blocking()
```

### `pff_ncaa_facet_pass_rush_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_pass_rush_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_rush_summary()
```

### `pff_ncaa_facet_passing_allowed_pressure(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_passing_allowed_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_allowed_pressure()
```

### `pff_ncaa_facet_passing_concept(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_passing_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_concept()
```

### `pff_ncaa_facet_passing_depth(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_passing_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_depth()
```

### `pff_ncaa_facet_passing_detail_stats(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_passing_detail_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_detail_stats()
```

### `pff_ncaa_facet_passing_pressure(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_passing_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_pressure()
```

### `pff_ncaa_facet_passing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_summary()
```

### `pff_ncaa_facet_pbes(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_pbes}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pbes()
```

### `pff_ncaa_facet_prps(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_prps}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_prps()
```

### `pff_ncaa_facet_punting_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_punting_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_punting_summary()
```

### `pff_ncaa_facet_receiving_concept(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_receiving_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_concept()
```

### `pff_ncaa_facet_receiving_coverage(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_receiving_coverage}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage()
```

### `pff_ncaa_facet_receiving_coverage_stats(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_receiving_coverage_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage_stats()
```

### `pff_ncaa_facet_receiving_depth(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_receiving_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_depth()
```

### `pff_ncaa_facet_receiving_scheme(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_receiving_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_scheme()
```

### `pff_ncaa_facet_receiving_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_summary()
```

### `pff_ncaa_facet_return_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_return_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_return_summary()
```

### `pff_ncaa_facet_run_blocking(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_run_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_blocking()
```

### `pff_ncaa_facet_run_defense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_run_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_defense_summary()
```

### `pff_ncaa_facet_rushing_direction_stats(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_rushing_direction_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_direction_stats()
```

### `pff_ncaa_facet_rushing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_summary()
```

### `pff_ncaa_facet_slot_coverages(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_slot_coverages}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_slot_coverages()
```

### `pff_ncaa_facet_special_teams_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_special_teams_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_special_teams_summary()
```

### `pff_ncaa_facet_time_in_pockets(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_facet_time_in_pockets}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_time_in_pockets()
```

### `pff_ncaa_games(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_games}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[int]` | `None` | Single week number. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_games()
```

### `pff_ncaa_leagues(headers: 'Optional[Dict[str, str]]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_leagues}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Leagues + seasons + week groups (bootstrap)

Endpoint: `GET https://premium.pff.com/api/v1/leagues`
Example URL: https://premium.pff.com/api/v1/leagues

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_leagues()
```

### `pff_ncaa_player_defense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_defense_summary()
```

### `pff_ncaa_player_offense_blocking(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_offense_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_blocking()
```

### `pff_ncaa_player_offense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_summary()
```

### `pff_ncaa_player_passing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_passing_summary()
```

### `pff_ncaa_player_position_pivot(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_position_pivot}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_position_pivot()
```

### `pff_ncaa_player_receiving_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_receiving_summary()
```

### `pff_ncaa_player_rushing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_rushing_summary()
```

### `pff_ncaa_player_seasons(*, league: 'Optional[str]' = 'ncaa', player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_seasons}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_seasons()
```

### `pff_ncaa_player_snaps_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_player_snaps_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_snaps_summary()
```

### `pff_ncaa_players(*, league: 'Optional[str]' = 'ncaa', name: 'Optional[str]' = None, id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_players}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `Optional[str]` | `None` | Player-name search prefix. |
| `id` | `Optional[int]` | `None` | Entity id (player lookup). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_players()
```

### `pff_ncaa_teams(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_teams}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams()
```

### `pff_ncaa_teams_overview(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ncaa_teams_overview}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams_overview()
```

### `pff_nfl_facet_blocking_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_blocking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_blocking_summary()
```

### `pff_nfl_facet_coverage_scheme(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_coverage_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_scheme()
```

### `pff_nfl_facet_coverage_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_coverage_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_summary()
```

### `pff_nfl_facet_defense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_defense_summary()
```

### `pff_nfl_facet_field_goal_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_field_goal_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_field_goal_summary()
```

### `pff_nfl_facet_kicking_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_kicking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_kicking_summary()
```

### `pff_nfl_facet_offense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_offense_summary()
```

### `pff_nfl_facet_pass_blocking(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_pass_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_blocking()
```

### `pff_nfl_facet_pass_rush_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_pass_rush_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_rush_summary()
```

### `pff_nfl_facet_passing_allowed_pressure(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_passing_allowed_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_allowed_pressure()
```

### `pff_nfl_facet_passing_concept(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_passing_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_concept()
```

### `pff_nfl_facet_passing_depth(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_passing_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_depth()
```

### `pff_nfl_facet_passing_detail_stats(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_passing_detail_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_detail_stats()
```

### `pff_nfl_facet_passing_pressure(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_passing_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_pressure()
```

### `pff_nfl_facet_passing_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_summary()
```

### `pff_nfl_facet_pbes(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_pbes}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pbes()
```

### `pff_nfl_facet_prps(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_prps}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_prps()
```

### `pff_nfl_facet_punting_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_punting_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_punting_summary()
```

### `pff_nfl_facet_receiving_concept(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_receiving_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_concept()
```

### `pff_nfl_facet_receiving_coverage(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_receiving_coverage}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage()
```

### `pff_nfl_facet_receiving_coverage_stats(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_receiving_coverage_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage_stats()
```

### `pff_nfl_facet_receiving_depth(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_receiving_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_depth()
```

### `pff_nfl_facet_receiving_scheme(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_receiving_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_scheme()
```

### `pff_nfl_facet_receiving_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_summary()
```

### `pff_nfl_facet_return_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_return_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_return_summary()
```

### `pff_nfl_facet_run_blocking(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_run_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_blocking()
```

### `pff_nfl_facet_run_defense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_run_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_defense_summary()
```

### `pff_nfl_facet_rushing_direction_stats(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_rushing_direction_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_direction_stats()
```

### `pff_nfl_facet_rushing_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_summary()
```

### `pff_nfl_facet_slot_coverages(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_slot_coverages}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_slot_coverages()
```

### `pff_nfl_facet_special_teams_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_special_teams_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_special_teams_summary()
```

### `pff_nfl_facet_time_in_pockets(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_facet_time_in_pockets}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_time_in_pockets()
```

### `pff_nfl_games(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_games}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[int]` | `None` | Single week number. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_games()
```

### `pff_nfl_leagues(headers: 'Optional[Dict[str, str]]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_leagues}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Leagues + seasons + week groups (bootstrap)

Endpoint: `GET https://premium.pff.com/api/v1/leagues`
Example URL: https://premium.pff.com/api/v1/leagues

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_leagues()
```

### `pff_nfl_player_defense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_defense_summary()
```

### `pff_nfl_player_offense_blocking(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_offense_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_blocking()
```

### `pff_nfl_player_offense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_summary()
```

### `pff_nfl_player_passing_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_passing_summary()
```

### `pff_nfl_player_position_pivot(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_position_pivot}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_position_pivot()
```

### `pff_nfl_player_receiving_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_receiving_summary()
```

### `pff_nfl_player_rushing_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_rushing_summary()
```

### `pff_nfl_player_seasons(*, league: 'Optional[str]' = 'nfl', player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_seasons}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_seasons()
```

### `pff_nfl_player_snaps_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_player_snaps_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_snaps_summary()
```

### `pff_nfl_players(*, league: 'Optional[str]' = 'nfl', name: 'Optional[str]' = None, id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_players}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `Optional[str]` | `None` | Player-name search prefix. |
| `id` | `Optional[int]` | `None` | Entity id (player lookup). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_players()
```

### `pff_nfl_teams(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_teams}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams()
```

### `pff_nfl_teams_overview(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_nfl_teams_overview}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams_overview()
```

### `pff_ufl_facet_blocking_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_blocking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_blocking_summary()
```

### `pff_ufl_facet_coverage_scheme(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_coverage_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_scheme()
```

### `pff_ufl_facet_coverage_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_coverage_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_summary()
```

### `pff_ufl_facet_defense_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_defense_summary()
```

### `pff_ufl_facet_field_goal_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_field_goal_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_field_goal_summary()
```

### `pff_ufl_facet_kicking_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_kicking_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_kicking_summary()
```

### `pff_ufl_facet_offense_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_offense_summary()
```

### `pff_ufl_facet_pass_blocking(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_pass_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_blocking()
```

### `pff_ufl_facet_pass_rush_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_pass_rush_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_rush_summary()
```

### `pff_ufl_facet_passing_allowed_pressure(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_passing_allowed_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_allowed_pressure()
```

### `pff_ufl_facet_passing_concept(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_passing_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_concept()
```

### `pff_ufl_facet_passing_depth(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_passing_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_depth()
```

### `pff_ufl_facet_passing_detail_stats(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_passing_detail_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_detail_stats()
```

### `pff_ufl_facet_passing_pressure(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_passing_pressure}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_pressure()
```

### `pff_ufl_facet_passing_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_summary()
```

### `pff_ufl_facet_pbes(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_pbes}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pbes()
```

### `pff_ufl_facet_prps(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_prps}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_prps()
```

### `pff_ufl_facet_punting_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_punting_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_punting_summary()
```

### `pff_ufl_facet_receiving_concept(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_receiving_concept}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_concept()
```

### `pff_ufl_facet_receiving_coverage(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_receiving_coverage}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage()
```

### `pff_ufl_facet_receiving_coverage_stats(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_receiving_coverage_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_coverage_stats()
```

### `pff_ufl_facet_receiving_depth(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_receiving_depth}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_depth()
```

### `pff_ufl_facet_receiving_scheme(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_receiving_scheme}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_scheme()
```

### `pff_ufl_facet_receiving_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_summary()
```

### `pff_ufl_facet_return_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_return_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_return_summary()
```

### `pff_ufl_facet_run_blocking(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_run_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_blocking()
```

### `pff_ufl_facet_run_defense_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_run_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_defense_summary()
```

### `pff_ufl_facet_rushing_direction_stats(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_rushing_direction_stats}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_direction_stats()
```

### `pff_ufl_facet_rushing_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_summary()
```

### `pff_ufl_facet_slot_coverages(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_slot_coverages}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_slot_coverages()
```

### `pff_ufl_facet_special_teams_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_special_teams_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_special_teams_summary()
```

### `pff_ufl_facet_time_in_pockets(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_facet_time_in_pockets}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_time_in_pockets()
```

### `pff_ufl_games(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_games}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[int]` | `None` | Single week number. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_games()
```

### `pff_ufl_leagues(headers: 'Optional[Dict[str, str]]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_leagues}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Leagues + seasons + week groups (bootstrap)

Endpoint: `GET https://premium.pff.com/api/v1/leagues`
Example URL: https://premium.pff.com/api/v1/leagues

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_leagues()
```

### `pff_ufl_player_defense_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_defense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_defense_summary()
```

### `pff_ufl_player_offense_blocking(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_offense_blocking}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_blocking()
```

### `pff_ufl_player_offense_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_offense_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_offense_summary()
```

### `pff_ufl_player_passing_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_passing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_passing_summary()
```

### `pff_ufl_player_position_pivot(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_position_pivot}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_position_pivot()
```

### `pff_ufl_player_receiving_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_receiving_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_receiving_summary()
```

### `pff_ufl_player_rushing_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_rushing_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_rushing_summary()
```

### `pff_ufl_player_seasons(*, league: 'Optional[str]' = 'ufl', player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_seasons}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_seasons()
```

### `pff_ufl_player_snaps_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_player_snaps_summary}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_snaps_summary()
```

### `pff_ufl_players(*, league: 'Optional[str]' = 'ufl', name: 'Optional[str]' = None, id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_players}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `Optional[str]` | `None` | Player-name search prefix. |
| `id` | `Optional[int]` | `None` | Entity id (player lookup). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_players()
```

### `pff_ufl_teams(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_teams}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams()
```

### `pff_ufl_teams_overview(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_ufl_teams_overview}

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams_overview()
```

### `rolling_windows(events: 'pl.DataFrame', season: 'int', windows: 'dict[str, tuple[int, ...]] | None' = None) -> 'pl.DataFrame'` {#rolling_windows}

Rolling-window form for every entity with an event in `season`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `events` | `DataFrame` |  | an `EVENT_SCHEMA` frame covering every season up to `season` (the career history the baselines read). |
| `season` | `int` |  | the season the rows describe; later seasons in `events` are ignored. |
| `windows` | `dict[str, tuple[int, ...]] \| None` | `None` | `{window_unit: (sizes...)}`; defaults to `WINDOWS`. |

**Returns**

one row per (entity, unit, metric, window size), `OUTPUT_SCHEMA`. Null / NaN event values are dropped before any window is computed. Columns: * `cur`: the mean of the entity's last `window_n` events through `season`. * `prev`: the mean of the `window_n` events immediately before `cur`'s window; null unless a full window of earlier history exists. * `season_start`: the mean of the `window_n` events immediately before season `season` started -- i.e. the entity's form entering the season, not counting any event actually played in `season`. * `career_baseline`: the mean of every event before `cur`'s window, including earlier events within `season` itself; null unless at least one full window of history precedes it. * `qualified`: `True` iff `n == window_n` -- the window is fully populated (not padded by a short career). Consumers building a "hottest" list should filter on this first. * `team_id` / `entity_name`: taken from the entity's single latest event through `season`, so a player who changed teams mid-season is labelled with their current team. * `delta_prev_rank`: 1 = biggest riser, ties share the lowest rank; null unless `qualified` and `prev` exists.

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

### `set_cache_mode(mode: 'str') -> 'None'` {#set_cache_mode}

Switch the global cache mode.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mode` | `str` |  | One of `"off"`, `"memory"`, `"filesystem"`. |

### `set_default_ttl(ttl: 'Optional[Union[timedelta, int]]') -> 'None'` {#set_default_ttl}

Override the default TTL for endpoints not matched by the tier rules.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ttl` | `Optional[Union[timedelta, int]]` |  | A `timedelta`, an integer (interpreted as seconds), or `None` to reset to the built-in `DEFAULT_TTL` (`MODERATE` = 1 hour). |

### `shot_attempts(shots: 'pl.DataFrame', league: 'str' = 'nba') -> 'pl.DataFrame'` {#shot_attempts}

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

### `ufl_pbp(game_id: 'Union[str, int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#ufl_pbp}

Enriched UFL play-by-play (EP/EPA/WP/WPA/CP/CPOE).

Same shared spring-football core as `~sportsdataverse.football.xfl.xfl_pbp`
(see `sportsdataverse.football.spring_football_ep_wp`).

**Capture finding:** ESPN publishes no play-by-play for UFL games as of
this port -- verified empty (`summary.drives` AND the Core v2
`.../plays` endpoint) across every completed 2024 + 2025 UFL game. This
function returns a zero-row (contract-shaped) frame on today's real data
-- not a stub -- and will pick up real rows automatically once ESPN
backfills UFL play-by-play. See
`tests/fixtures/league_ports/FEASIBILITY.md`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[str, int]` |  | ESPN UFL event id. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per play with `ep`/`epa`/`wp`/`wpa`/`cp`/`cpoe` and the other `enrich_nfl_pbp` output columns. Zero rows today for every UFL game (see capture finding above).

**Example**

```python
from sportsdataverse.football.ufl import ufl_pbp

df = ufl_pbp("401638299")
print(df.height)  # 0 today -- see the capture-finding note above
```

### `validate_game(frame: 'pl.DataFrame', league: 'str', *, header: 'dict | None' = None, source: 'str' = 'espn', summary: 'dict | None' = None, box: 'dict | None' = None) -> 'GameReport'` {#validate_game}

Validate one processed game against the packaged invariant rules.

Pure and offline: nothing is fetched, nothing is written, the frame is not
mutated. A rule whose columns the frame lacks is skipped rather than failed,
so a slim frame validates the rules it can support.

For a source whose producer names the same quantities differently,
`SOURCE_COLUMNS` supplies the ESPN-shaped aliases on a view of the
frame, and `NOT_APPLICABLE` names the rules that source cannot
support at all -- those are reported in `not_applicable` rather than
skipped silently.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `frame` | `DataFrame` |  | one game's processed plays, in processor row order -- the `plays_frame` attribute of `NFLPlayProcess` / `CFBPlayProcess`. |
| `league` | `str` |  | `"nfl"` or `"cfb"`. |
| `header` | `dict \| None` | `None` | the game's ESPN-shaped `header` dict. Only used to resolve `game_id` / `season` when the frame carries neither. |
| `source` | `str` | `'espn'` | the source the game came from (`"espn"`, `"shield"`, `"cbs"`, `"yahoo"`, `"fox"`, `"ncaa"`). Rules that only judge ESPN's own feed are skipped for an adapted source. |
| `summary` | `dict \| None` | `None` | the full ESPN-shaped summary, when available. Enables the header-score, final-WP, dropped-play, drive-count and ESPN box rules. |
| `box` | `dict \| None` | `None` | the processor's `advBoxScore` dict. Enables the team box and team EPA aggregations. |

**Returns**

`ok` is True when no rule fired at `error` severity.

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
from sportsdataverse.validation import validate_game

proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
game = proc.run_processing_pipeline()
report = validate_game(proc.plays_frame, "nfl", summary=proc.json, box=game.get("advBoxScore"))
report.ok, sorted(report.counts_by_rule)
```

### `wch_ratings(dates: 'list[str]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'` {#wch_ratings}

WCH opponent-adjusted goal-margin ratings over a set of scoreboard dates.

See the module docstring's coverage caveat -- ESPN's WCH scoreboard
coverage observed during this port was tournament-only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `list[str]` |  | `YYYYMMDD` date strings to fetch. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

One row per team: `team_id, adj_off, adj_def, adj_net, raw_off, raw_def, games`.

**Example**

```python
from sportsdataverse.hockey.wch import wch_ratings
ratings = wch_ratings(["20250315", "20250321", "20250322", "20250323"])
ratings.sort("adj_net", descending=True).head()
```

### `xfl_pbp(game_id: 'Union[str, int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"` {#xfl_pbp}

Enriched XFL play-by-play (EP/EPA/WP/WPA/CP/CPOE).

Fetches the ESPN game summary, unrolls its drives into an nflverse-shape
frame, and scores it with the same parity-validated NFL EP/WP pipeline
used league-wide (see
`sportsdataverse.football.spring_football_ep_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[str, int]` |  | ESPN XFL event id. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per play with `ep`/`epa`/`wp`/`wpa`/`cp`/`cpoe` and the other `enrich_nfl_pbp` output columns. Zero rows for a game ESPN has no play-by-play for.

**Example**

```python
from sportsdataverse.football.xfl import xfl_pbp

df = xfl_pbp("401517780")
print(df.select("play_id", "epa", "wp").head())
```
