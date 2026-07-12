---
title: Package — additional Python functions
sidebar_label: Additional functions
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

### `get_cache_mode() -> 'str'` {#get_cache_mode}

Return the current cache mode.

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
