---
title: "Package — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 9
description: "Package — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Other

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

### cache_stats {#cache_stats}

`cache_stats() -> 'Dict[str, Any]'`

Return a snapshot of the cache for debugging / inspection.

Returns a dict with `mode`, `entries`, and `disk_bytes` (only
populated when mode=filesystem). Cheap — doesn't read the cached
bodies, just counts + sizes.

### get_cache_mode {#get_cache_mode}

`get_cache_mode() -> 'str'`

Return the current cache mode.

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
