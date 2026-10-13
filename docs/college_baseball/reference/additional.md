# COLLEGE_BASEBALL — additional Python functions

> COLLEGE_BASEBALL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package.

Hand-written wrappers, loaders, and helpers in `sportsdataverse.college_baseball`
not covered by the generated API-endpoint reference above.

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

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `inning` | integer |  |
| `half` | character |  |
| `base_state` | character |  |
| `outs` | integer |  |
| `runs_before` | integer |  |
| `runs_after` | integer |  |
| `batting_team_id` | character |  |
| `play_seq` | integer |  |
| `score_diff` | integer |  |

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
| `contest_id` | character |  |
| `inning` | integer |  |
| `inning_top_bot` | character |  |
| `batting` | character |  |
| `fielding` | character |  |
| `play_number` | integer |  |
| `score_away` | integer |  |
| `score_home` | integer |  |
| `batter` | character | MLBAM player id of the batter. |
| `play_type` | character |  |
| `hit_trajectory` | character |  |
| `fielded_position` | character |  |
| `is_hit` | logical |  |
| `is_out` | logical |  |
| `strikeout_type` | character |  |
| `is_sacrifice` | logical |  |
| `sac_type` | character |  |
| `is_double_play` | logical |  |
| `rbi` | integer |  |
| `count_balls` | integer |  |
| `count_strikes` | integer |  |
| `pitch_sequence` | character |  |
| `error_position` | character |  |
| `unearned` | logical |  |
| `runs_scored` | integer |  |
| `scoring_runners` | character |  |
| `runners_advanced` | character |  |
| `outs_on_play` | integer |  |
| `is_scoring_play` | logical | Flag indicating that the play put points on the board (1 = scoring play, 0 = not). |
| `description` | character |  |

**Example**

```python
from sportsdataverse.baseball.college_baseball import decompose_college_baseball_plays
df = decompose_college_baseball_plays(
    [{"inning": 1, "inning_top_bot": "top", "score": "0-0",
      "description": "Jack Moss singled to left field (1-2 KBFX)."}]
)
print(df.select("play_type", "is_hit", "pitch_sequence").row(0))
```
