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

### `pff_aaf_facet_blocking_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'` {#pff_aaf_facet_blocking_summary}

Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[int]` | `None` | week query parameter. |
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

Leagues + seasons + week groups (bootstrap)

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

Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `name` | `Optional[str]` | `None` | name query parameter. |
| `id` | `Optional[int]` | `None` | id query parameter. |
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

Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
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

Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
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

Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[int]` | `None` | week query parameter. |
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

Leagues + seasons + week groups (bootstrap)

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

Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `name` | `Optional[str]` | `None` | name query parameter. |
| `id` | `Optional[int]` | `None` | id query parameter. |
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

Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
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

Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
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

Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[int]` | `None` | week query parameter. |
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

Leagues + seasons + week groups (bootstrap)

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

Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `name` | `Optional[str]` | `None` | name query parameter. |
| `id` | `Optional[int]` | `None` | id query parameter. |
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

Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
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

Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
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

Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `franchise_id` | `Optional[int]` | `None` | franchiseId query parameter. |
| `game_id` | `Optional[int]` | `None` | gameId query parameter. |
| `division` | `Optional[str]` | `None` | division query parameter. |
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

Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[int]` | `None` | week query parameter. |
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

Leagues + seasons + week groups (bootstrap)

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

Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
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

Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `player_id` | `Optional[int]` | `None` | player_id query parameter. |
| `career` | `Optional[str]` | `None` | career query parameter. |
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

Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `name` | `Optional[str]` | `None` | name query parameter. |
| `id` | `Optional[int]` | `None` | id query parameter. |
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

Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
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

Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | league query parameter. |
| `season` | `Optional[int]` | `None` | season query parameter. |
| `week` | `Optional[str]` | `None` | week query parameter. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams_overview()
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
