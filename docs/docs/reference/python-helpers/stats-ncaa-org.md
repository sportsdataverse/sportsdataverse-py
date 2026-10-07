---
title: "Package — additional Python functions — stats.ncaa.org"
sidebar_label: "stats.ncaa.org"
sidebar_position: 7
description: "Package — additional Python functions — stats.ncaa.org — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — stats.ncaa.org

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
| `outs` | integer |  |
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
| `game_id` | character |  |
| `play_seq` | integer | 0-based game-global plate-appearance order (sorted by ESPN atBatId); joins back to college_softball_state. |
| `re_before` | double | RE24 of the base-out state before the PA, looked up in the matrix fit on the same state frame; 0.0 when that state is absent from the matrix. |
| `re_after` | double | RE24 of the base-out state after the PA (the next PA's before-state in the same half-inning); 0.0 for the last PA of a half-inning. |
| `run_value` | double | re_after minus re_before, plus runs scored on the play (change in the combined cumulative score). |
| `wpa` | double |  |

**Example**

```python
from sportsdataverse.baseball.college_softball.college_softball_re import college_softball_wpa
wpa = college_softball_wpa(state=state, results=results)
```
