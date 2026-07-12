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
