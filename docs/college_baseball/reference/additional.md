# College baseball — additional Python functions

> College baseball — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package.

Hand-written wrappers, loaders, and helpers in `sportsdataverse.baseball.college_baseball` that are not
generated API-endpoint wrappers (those have their own reference pages).

## stats.ncaa.org

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
| `base_state` | String | 3-char base occupancy code ("_" = empty, "1"/"2"/"3" = occupied), e.g. "1_3" for runners on first and third. |
| `outs` | Int64 | Outs at the start of the base-out state (0-2). |
| `run_expectancy` | Float64 | Empirical mean runs scored from this state through the end of the half-inning (RE24). |
| `n` | Int64 | Number of plate appearances observed starting in this base-out state. |

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

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `inning` | Int64 |  |
| `half` | String |  |
| `base_state` | String |  |
| `outs` | Int64 |  |
| `runs_before` | Int64 |  |
| `runs_after` | Int64 |  |
| `batting_team_id` | String |  |
| `play_seq` | Int64 |  |
| `score_diff` | Int64 |  |

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
| `game_id` | String | ESPN event id for the game (join key to the schedule). |
| `play_seq` | Int64 | Game-global sequential plate-appearance order. |
| `re_before` | Float64 | RE24 of the base-out state before the PA. |
| `re_after` | Float64 | RE24 of the base-out state after the PA. |
| `run_value` | Float64 | re_after minus re_before, plus runs scored on the play. |
| `wpa` | Float64 | Home-perspective win-probability added. |

**Example**

```python
from sportsdataverse.baseball.college_baseball.college_baseball_re import college_baseball_wpa
wpa = college_baseball_wpa(state=state, results=results)
```

## Play-by-play processing

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

| col_name | type | description |
|---|---|---|
| `contest_id` | String |  |
| `inning` | Int64 |  |
| `inning_top_bot` | String |  |
| `batting` | String |  |
| `fielding` | String |  |
| `play_number` | Int64 |  |
| `score_away` | Int64 |  |
| `score_home` | Int64 |  |
| `batter` | String | MLBAM player id of the batter. |
| `play_type` | String |  |
| `hit_trajectory` | String |  |
| `fielded_position` | String |  |
| `is_hit` | Boolean |  |
| `is_out` | Boolean |  |
| `strikeout_type` | String |  |
| `is_sacrifice` | Boolean |  |
| `sac_type` | String |  |
| `is_double_play` | Boolean |  |
| `rbi` | Int64 |  |
| `count_balls` | Int64 |  |
| `count_strikes` | Int64 |  |
| `pitch_sequence` | String |  |
| `error_position` | String |  |
| `unearned` | Boolean |  |
| `runs_scored` | Int64 |  |
| `scoring_runners` | String |  |
| `runners_advanced` | String |  |
| `outs_on_play` | Int64 |  |
| `is_scoring_play` | Boolean | Flag indicating that the play put points on the board (1 = scoring play, 0 = not). |
| `description` | String |  |

**Example**

```python
from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays
df = decompose_college_baseball_plays(
    [{"inning": 1, "inning_top_bot": "top", "score": "0-0",
      "description": "Jack Moss singled to left field (1-2 KBFX)."}]
)
print(df.select("play_type", "is_hit", "pitch_sequence").row(0))
```
