---
title: "Package — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 7
description: "Package — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Other

### cache_stats {#cache_stats}

`cache_stats() -> 'Dict[str, Any]'`

Return a snapshot of the cache for debugging / inspection.

Returns a dict with `mode`, `entries`, and `disk_bytes` (only
populated when mode=filesystem). Cheap — doesn't read the cached
bodies, just counts + sizes.

### college_baseball_re24 {#college_baseball_re24}

`college_baseball_re24(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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

### college_baseball_state {#college_baseball_state}

`college_baseball_state(plays: 'Dict[str, Any]') -> 'pl.DataFrame'`

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

### college_baseball_wpa {#college_baseball_wpa}

`college_baseball_wpa(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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

### college_softball_re24 {#college_softball_re24}

`college_softball_re24(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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

### college_softball_state {#college_softball_state}

`college_softball_state(plays: 'Dict[str, Any]') -> 'pl.DataFrame'`

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

### college_softball_wpa {#college_softball_wpa}

`college_softball_wpa(seasons: 'Union[int, List[int], None]' = None, *, state: 'Optional[pl.DataFrame]' = None, results: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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

### cricket_expected_runs {#cricket_expected_runs}

`cricket_expected_runs(state_wp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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

### cricket_match_state {#cricket_match_state}

`cricket_match_state(summary: 'dict', *, fmt: 'str', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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

### cricket_win_probability {#cricket_win_probability}

`cricket_win_probability(state: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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

### cricket_wpa {#cricket_wpa}

`cricket_wpa(state_wp: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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

### decompose_college_baseball_plays {#decompose_college_baseball_plays}

`decompose_college_baseball_plays(rows: "'list[dict]'", *, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

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

### deserved_wins {#deserved_wins}

`deserved_wins(games: 'pl.DataFrame') -> 'pl.DataFrame'`

Season deserved wins and luck per team from `paper_index_games` rows.

With `p_i` the team's `paper_share` in game `i` of the season and `W` its
real wins: `deserved_wins = sum(p_i)`, `luck_wins = W - deserved_wins` and
`luck_z = luck_wins / sqrt(sum(p_i * (1 - p_i)))`. Under the model a season's
wins are a sum of independent Bernoulli(`p_i`) draws, so `luck_z` is luck in
standard deviations; it is null when that variance is 0 (every share exactly 0
or 1).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | `paper_index_games` output (needs `season`, `team_id`, `won`, `paper_share`), any number of seasons of ONE league: ESPN team ids overlap across leagues (2 is Auburn in college and Buffalo in the NFL). |

**Returns**

one row per `(season, team_id)`, sorted by them: `games`, `wins`, `deserved_wins`, `luck_wins`, `luck_z` (`DESERVED_WINS_SCHEMA`; `team_id` keeps its dtype). An empty frame with the same columns when `games` is empty. Read with care: * `games` / `wins` count only the games `paper_index_games` scored, so they can differ from the official record: ties and snap-floor games are out (NFL ties: 1 in 2021, 2 in 2022), postseason games are in. * Luck includes home field: the share has no intercept, and home teams beat their shares in every 2022-2025 season (+0.013 to +0.047 wins per home game; fit holdouts: college 1,204 home wins vs 1,171 deserved, NFL 628 vs 596). A team's `luck_wins` is biased by about that rate x (home - away games), and the nominal home side at a neutral site gets it too. * Small samples: college FCS opponents appear with 1-2 games; filter on a minimum `games` before ranking. Over 2022-2025 the spread of `luck_z` for teams with 8+ games was 1.03-1.11 (college) and 0.85-1.28 (NFL).

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the games rolled up, as the pbp keys it (CFB and NFL starting year). |
| `team_id` | integer | Team id as paper_index_games carries it (the released pbp's pos_team_id dtype, ESPN team id). |
| `games` | integer | Games the team played that season that paper_index_games scored (completed, not tied, both sides at least 20 scrimmage snaps; NFL Pro Bowl excluded). |
| `wins` | integer | Games the team won among those scored games. |
| `deserved_wins` | double | Sum of the team's paper_share over the season: the wins the Paper Index says its play earned. |
| `luck_wins` | double | wins minus deserved_wins; positive means the team won more games than its play deserved. |
| `luck_z` | double | luck_wins / sqrt(sum(p * (1 - p))) over the team's game shares p: luck in standard deviations of a sum of independent Bernoulli(p) wins; null when that variance is 0. |

**Example**

```python
from sportsdataverse.cfb import load_cfb_pbp
from sportsdataverse.paper_index import PBP_COLUMNS, deserved_wins, paper_index_games

luck = deserved_wins(paper_index_games(load_cfb_pbp(2024).select(PBP_COLUMNS), "cfb"))
luck.sort("luck_z", descending=True).head(10)  # the season's luckiest teams
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

### get_cache_mode {#get_cache_mode}

`get_cache_mode() -> 'str'`

Return the current cache mode.

### mch_ratings {#mch_ratings}

`mch_ratings(dates: 'list[str]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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

### nbagl_enhanced_pbp {#nbagl_enhanced_pbp}

`nbagl_enhanced_pbp(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return a normalised enhanced play-by-play frame for a G-League game.

Fetches the raw `playbyplayv3` payload from `stats.nba.com` via
`~sportsdataverse.nba.nba_stats.nba_stats_playbyplayv3` then
delegates all transformation to the league-agnostic
`~sportsdataverse.nba.nba_enhanced_pbp.enhanced_pbp_from_payload`
core with `league_id="20"`.  Never raises on malformed or empty
payloads — returns a zero-row frame instead.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | G-League game identifier string (e.g. `"2022400003"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema `sportsdataverse.nba.nba_enhanced_pbp.ENHANCED_PBP_SCHEMA`. Key columns include `game_id` (Utf8), `action_number` (Int64), `period` (Int64), `seconds_remaining` (Float64), `team_id` (Int64), `person_id` (Int64), `is_substitution` (Boolean), and one Boolean flag per event type.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_enhanced_pbp
df = nbagl_enhanced_pbp("2022400003")
print(df.shape)

# Pandas output

df_pd = nbagl_enhanced_pbp("2022400003", return_as_pandas=True)
print(type(df_pd))

# Filter substitution events

subs = df.filter(df["is_substitution"] == True)  # noqa: E712
print(subs.select(["period", "seconds_remaining", "person_id"]))
```

### nbagl_on_court {#nbagl_on_court}

`nbagl_on_court(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the rotation-keyed on-court player frame for a G-League game.

Makes three network calls (play-by-play v3, game rotation,
box-score traditional v3), infers on-court rosters from the rotation
stints via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
and returns one row per PBP action with ten Int64 player-ID columns
(`home_player_1..5` / `away_player_1..5`).  All transformation is
performed by the shared `nba/` core with `league_id="20"` forwarded
to the rotation endpoint.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | G-League game identifier string (e.g. `"2022400003"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per PBP action and columns `home_player_1` … `home_player_5`, `away_player_1` … `away_player_5` (all Int64), plus the `action_number` join key.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_on_court
oc = nbagl_on_court("2022400003")
print(oc.select(["action_number", "home_player_1"]).head())

# Pandas output

oc_pd = nbagl_on_court("2022400003", return_as_pandas=True)
print(type(oc_pd))

# Join on enhanced PBP

from sportsdataverse.nbagl.nbagl_engine import nbagl_enhanced_pbp
enh = nbagl_enhanced_pbp("2022400003")
joined = enh.join(oc, on="action_number", how="left")
```

### nbagl_possessions {#nbagl_possessions}

`nbagl_possessions(game_id: 'str', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Return the possession-level lineup stint matrix for a G-League game.

Builds possessions from the enhanced PBP via
`~sportsdataverse.nba.nba_possessions.build_possessions`, resolves
on-court rosters via
`~sportsdataverse.nba.nba_lineups.players_on_court_from_rotation`,
then attaches the 5v5 lineups via
`~sportsdataverse.nba.nba_possessions.attach_possession_lineups`.
All transformation is performed by the shared `nba/` cores — no
G-League-specific logic.  Never raises on malformed payloads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` |  | G-League game identifier string (e.g. `"2022400003"`). |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with schema combining `POSSESSIONS_SCHEMA` and ten lineup columns: `off_player_1` … `off_player_5`, `def_player_1` … `def_player_5` (all Int64). One row per possession. Empty or malformed inputs return a zero-row frame.

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_possessions
poss = nbagl_possessions("2022400003")
print(poss.shape)

# Pandas output

poss_pd = nbagl_possessions("2022400003", return_as_pandas=True)
print(type(poss_pd))

# Total points check

total = int(poss["points"].sum())
print(f"Total points scored: {total}")
```

### nbagl_rapm_from_games {#nbagl_rapm_from_games}

`nbagl_rapm_from_games(game_ids: 'Sequence[str]', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Compute per-player RAPM estimates over a sequence of G-League games.

Iterates *game_ids*, builds the possession-level stint matrix for each
via `nbagl_possessions`, concatenates the results, and fits a
ridge-regression RAPM model via
`~sportsdataverse.nba.nba_rapm.nba_rapm`.  Games whose possession
frame is empty (e.g. a malformed payload) are silently skipped.  Returns
a zero-row frame when no valid possessions are found.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_ids` | `Sequence[str]` |  | Sequence of G-League game identifier strings. |
| `return_as_pandas` | `bool` | `False` | If `True`, convert the result to a `pandas.DataFrame` before returning. |

**Returns**

Polars (or pandas) DataFrame with one row per player and columns `player_id` (Int64), `o_rapm` (Float64), `d_rapm` (Float64), `rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

**Example**

```python
from sportsdataverse.nbagl.nbagl_engine import nbagl_rapm_from_games
rapm = nbagl_rapm_from_games(["2022400003", "2022400009"])
print(rapm.sort("rapm", descending=True).head())

# Pandas output

rapm_pd = nbagl_rapm_from_games(["2022400003"], return_as_pandas=True)
print(type(rapm_pd))

# Multi-season aggregation

import polars as pl
game_ids = pl.read_parquet("nbagl_schedule.parquet")["game_id"].to_list()
rapm = nbagl_rapm_from_games(game_ids)
print(rapm.sort("rapm", descending=True).head(10))
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

### paper_index_game {#paper_index_game}

`paper_index_game(pbp: 'pl.DataFrame', home_id: 'Union[int, str]', away_id: 'Union[int, str]', league: 'str') -> 'Optional[dict[str, Any]]'`

Paper Index of one game: each side's deserved-win share and the eight margins.

The served path, as Game on Paper's `paper_index.compute`: no snap floor and
no tie rule, just the model applied to the plays given.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | one game's released `espn_{cfb,nfl}_pbp` plays: `game_play_number` and the `PBP_COLUMNS` from `pos_team_id` on (`fg_made` only for the NFL; `game_id` optional). |
| `home_id` | `Union[int, str]` |  | the home team's id as `pos_team_id` carries it (int or str). |
| `away_id` | `Union[int, str]` |  | the away team's id. |
| `league` | `str` |  | `"cfb"` or `"nfl"` -- picks the fitted weights, the field-position curve and the field-goal rule. |

**Returns**

dict | None: `{"home_share": float, "away_share": float, "margins": {name: float}}` with the eight `MARGINS` from the home side (home minus away; havoc and turnovers signed so positive favors home). `away_share` is `1 - home_share`. None when either side has no usable inputs (no scrimmage snap, no drive id, or no successful play).

**Example**

```python
import polars as pl
from sportsdataverse.cfb import load_cfb_pbp
from sportsdataverse.paper_index import paper_index_game

pbp = load_cfb_pbp(2024).filter(pl.col("game_id") == 401628337)
out = paper_index_game(pbp, 2, 25, "cfb")  # Auburn (home) vs California
out["home_share"], out["margins"]["success"]
```

### paper_index_games {#paper_index_games}

`paper_index_games(pbp: 'pl.DataFrame', league: 'str') -> 'pl.DataFrame'`

Paper Index of every completed game in a pbp frame, one row per team per game.

A game counts when it passes the trainer's filters (fit_paper_index.py
`season_game_rows`): it has a winner (ties dropped), both sides ran at least
`MIN_PLAYS_PER_TEAM` scrimmage snaps, and (NFL) both teams are franchises
(the Pro Bowl dropped). The port adds two checks of its own: the game's last
play is marked completed (`status_type_completed`), and both sides' eight
inputs are computable and finite (where the trainer required a finite mean EPA).
Final scores and the home/away ids are the last play's (by `game_play_number`).
On the NFL seasons the fit read, this selects exactly the fit's games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | released `espn_{cfb,nfl}_pbp` plays, any number of games and seasons; project to `PBP_COLUMNS` first (`load_cfb_pbp(s).select(PBP_COLUMNS)`). |
| `league` | `str` |  | `"cfb"` or `"nfl"`. |

**Returns**

keyed by `(game_id, team_id)`, sorted by them; `game_id` and `team_id` keep the pbp's `game_id` / `pos_team_id` dtype (never through float), the rest is `GAMES_SCHEMA`: * `season`, `season_type` (ESPN `seasonType`: 2 regular, 3 post), `week`. * `won`: the team outscored its opponent. * `paper_share`: the team's deserved-win probability; `opp_share` the opponent's (they sum to 1). * eight `{margin}_margin` columns, team minus opponent, positive favors the team. An empty frame with the same columns when no game qualifies.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game id, typed as the released pbp's game_id (Int64 in espn_cfb_pbp and espn_nfl_pbp). |
| `team_id` | integer | ESPN id of the team the row describes, typed as the released pbp's pos_team_id (Int64 in espn_cfb_pbp and espn_nfl_pbp). |
| `season` | integer | Season of the game, as the pbp keys it (CFB and NFL starting year). |
| `season_type` | integer | ESPN season type of the game (seasonType): 2 regular season, 3 postseason. |
| `week` | integer | ESPN week number of the game within its season type. |
| `won` | logical | Whether the team outscored its opponent (final score from the game's last play). |
| `paper_share` | double | The team's Paper Index share in [0, 1]: its deserved-win probability from the eight margins through the league's fitted intercept-free logistic. |
| `opp_share` | double | The opponent's Paper Index share; paper_share + opp_share = 1. |
| `success_margin` | double | Team EPA success rate minus the opponent's, over scrimmage snaps. |
| `explosive_margin` | double | Team explosive-play rate (EPA_explosive) minus the opponent's, over scrimmage snaps. |
| `explosive_epa_margin` | double | Team explosiveness (mean EPA per successful snap) minus the opponent's. |
| `opp_conversion_margin` | double | Team scoring-opportunity conversion (share of opportunity drives that scored; 0.5 with no opportunities) minus the opponent's. |
| `pts_per_opp_margin` | double | Team points per scoring opportunity minus the opponent's (NFL counts made field goals; no opportunities takes the league's train-season average). |
| `field_position_margin` | double | Expected points of the team's average drive start on the league's bundled EP-by-yardline curve minus the opponent's. |
| `havoc_margin` | double | Havoc rate the team's defense created (havoc allowed on opponent snaps) minus the havoc rate its offense allowed. |
| `turnovers_margin` | double | Turnovers the opponent's offense committed minus the team's. |

**Example**

```python
import polars as pl
from sportsdataverse.paper_index import PBP_COLUMNS, paper_index_games

url = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/"
    "espn_nfl_pbp/play_by_play_2024.parquet"
)
games = paper_index_games(pl.read_parquet(url, columns=list(PBP_COLUMNS)), "nfl")

# Pipeline next step (one line)

games.filter(~pl.col("won") & (pl.col("paper_share") > 0.8))  # deserved to win, lost
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

### set_cache_mode {#set_cache_mode}

`set_cache_mode(mode: 'str') -> 'None'`

Switch the global cache mode.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mode` | `str` |  | One of `"off"`, `"memory"`, `"filesystem"`. |

### set_default_ttl {#set_default_ttl}

`set_default_ttl(ttl: 'Optional[Union[timedelta, int]]') -> 'None'`

Override the default TTL for endpoints not matched by the tier rules.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ttl` | `Optional[Union[timedelta, int]]` |  | A `timedelta`, an integer (interpreted as seconds), or `None` to reset to the built-in `DEFAULT_TTL` (`MODERATE` = 1 hour). |

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

### soccer_events_to_frame {#soccer_events_to_frame}

`soccer_events_to_frame(dataset: 'Any', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Turn a kloppy dataset (any provider, any file) into a tidy frame.

The one place the package's column convention is applied to kloppy output: one row per
event, columns snake-cased (kloppy's own names -- `event_id`, `event_type`,
`period_id`, `timestamp`, `team_id`, `player_id`, `coordinates_x`,
`coordinates_y`, ... -- already are, so this is a no-op guard for any extra column).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dataset` | `Any` |  | A kloppy `EventDataset` (or any dataset with `to_df`), e.g. from `kloppy.statsbomb.load(event_data=..., lineup_data=...)` or `kloppy.opta.load(...)`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A polars DataFrame (pandas with `return_as_pandas=True`), one row per event. Coordinates are in the dataset's coordinate system -- kloppy's default is a 0-1 normalised pitch; pass `coordinates="statsbomb"` (etc.) to kloppy's loader to keep the provider's units.

**Example**

```python
from kloppy import statsbomb
from sportsdataverse.soccer import soccer_events_to_frame
ds = statsbomb.load(event_data="8658.json", lineup_data="lineups_8658.json")
df = soccer_events_to_frame(ds)
print(df.shape)

# Useful parameter combination

df_pd = soccer_events_to_frame(ds, return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "SHOT").select("player_id", "coordinates_x", "coordinates_y")
```

### soccer_open_events {#soccer_open_events}

`soccer_open_events(provider: 'str', match_id: 'Union[int, str]', *, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame]'`

Load one match of a provider's free open event data as a tidy frame.

`provider="statsbomb"` reads StatsBomb open data (https://github.com/statsbomb/open-data)
through `kloppy.statsbomb.load_open_data(match_id=...)`. That data is free for research and
non-commercial use only, under StatsBomb's open-data licence -- read it before publishing
anything built on it. Other kloppy open samples (Metrica, SkillCorner) follow the same
shape and are added on request.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `provider` | `str` |  | Open-data provider key; currently `"statsbomb"`. |
| `match_id` | `Union[int, str]` |  | The provider's match id (StatsBomb: e.g. `8658` -- France v Croatia, 2018 World Cup final). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

A polars DataFrame (pandas with `return_as_pandas=True`), one row per event; see `soccer_events_to_frame` for the columns.

**Example**

```python
from sportsdataverse.soccer import soccer_open_events
df = soccer_open_events("statsbomb", 8658)
print(df.shape)

# Useful parameter combination

df_pd = soccer_open_events("statsbomb", 8658, coordinates="statsbomb", return_as_pandas=True)

# Pipeline next step (one line)

df.filter(pl.col("event_type") == "SHOT").group_by("team_id").len()
```

### ufl_pbp {#ufl_pbp}

`ufl_pbp(game_id: 'Union[str, int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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

### validate_game {#validate_game}

`validate_game(frame: 'pl.DataFrame', league: 'str', *, header: 'dict | None' = None, source: 'str' = 'espn', summary: 'dict | None' = None, box: 'dict | None' = None) -> 'GameReport'`

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

### wch_ratings {#wch_ratings}

`wch_ratings(dates: 'list[str]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

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

### xfl_pbp {#xfl_pbp}

`xfl_pbp(game_id: 'Union[str, int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

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
