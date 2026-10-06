---
title: "WNBA — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 6
description: "WNBA — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Other

### load_wnba_stats_leaguedash {#load_wnba_stats_leaguedash}

`load_wnba_stats_leaguedash(family: 'str', seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load one asset family of the `wnba_stats_leaguedash` release.

`wnba_stats_leaguedash` is a parameter cube: one asset per
(family, season) pair rather than one per season, so a family must be named.
The valid families are exported as `WNBA_STATS_LEAGUEDASH_FAMILIES` --
import that tuple to discover them rather than passing a bare string; an
unknown family raises `ValueError` listing every valid value. This is the
non-deprecated way to reach the cube; the four `load_wnba_stats_*` shims
below only reconstruct retired tags' stacked shapes from it.

Column sets are family-specific (a `lineups_*` frame keys on `group_id`,
a `player_*` frame on `player_id`), so this loader documents no fixed
returns table. `player_id` / `team_id` are `Int64` in every family and
season, so cross-family joins need no dtype reconciliation.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `family` | `str` |  | Asset family, e.g. `"player_stats_advanced"`. Must be one of `WNBA_STATS_LEAGUEDASH_FAMILIES`. |
| `seasons` | `int \| Iterable[int]` |  | Season, or iterable of seasons, to load. WNBA seasons are single calendar years. 1997 is the earliest season on the tag. A requested season the family does not publish is warned about and skipped, not an error. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe with one row per player / team / lineup per requested season for the requested family; an empty frame when no requested season is published.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_leaguedash
adv = load_wnba_stats_leaguedash("player_stats_advanced", seasons=2025)
print(adv.shape)

# Discover the valid families

from sportsdataverse.wnba import WNBA_STATS_LEAGUEDASH_FAMILIES
print(WNBA_STATS_LEAGUEDASH_FAMILIES)

# Multi-season, pandas round-trip

team_pd = load_wnba_stats_leaguedash(
    "team_stats_base", seasons=range(2020, 2026), return_as_pandas=True
)

# Pipeline next step (best net rating in 2025)

import polars as pl
load_wnba_stats_leaguedash("team_stats_advanced", seasons=2025).sort(
    "net_rating", descending=True
).head()
```

### load_wnba_stats_lineups {#load_wnba_stats_lineups}

`load_wnba_stats_lineups(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA 5-man lineup statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per lineup-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `lineups_{base, advanced}` assets filtered to `group_quantity == 5` — matching the old `wnba_stats_lineups` tag's 5-man-only, Base+Advanced-only coverage. Call the cube's `lineups_*` assets directly (unfiltered) for 2/3/4-man lineups or the other 4 measure types.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_lineups
df = load_wnba_stats_lineups(seasons=2026)
print(df.shape)
```

### load_wnba_stats_player_season_stats {#load_wnba_stats_player_season_stats}

`load_wnba_stats_player_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA player statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per player-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `player_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Usage`/`Defense` — matches the old `wnba_stats_player_season_stats` tag's coverage; player-level `Opponent`/`Four Factors` are empty upstream and were never populated by either version).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_player_season_stats
df = load_wnba_stats_player_season_stats(seasons=2026)
print(df.shape)

# Pipeline next step (Advanced-only rows)

import polars as pl
adv = df.filter(pl.col("measure_type") == "Advanced")
```

### load_wnba_stats_standings {#load_wnba_stats_standings}

`load_wnba_stats_standings(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA standings (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season, read from the `wnba_stats_leaguedash` cube's `standings` asset -- the same underlying `leaguestandingsv3` endpoint/params as the old `wnba_stats_standings` tag, so this is close to a pure passthrough.

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_standings
df = load_wnba_stats_standings(seasons=2026)
print(df.shape)
```

### load_wnba_stats_team_season_stats {#load_wnba_stats_team_season_stats}

`load_wnba_stats_team_season_stats(seasons, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load season-level WNBA team statistics (deprecated).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  | an int or iterable of seasons. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per team-season-measure_type, stacked from the `wnba_stats_leaguedash` cube's `team_stats_*` assets (`Base`/`Advanced`/`Misc`/`Scoring`/`Defense`/ `Opponent` — matches the old `wnba_stats_team_season_stats` tag's coverage; team-level `Usage`/`Four Factors` are empty upstream).

**Example**

```python
from sportsdataverse.wnba import load_wnba_stats_team_season_stats
df = load_wnba_stats_team_season_stats(seasons=2026)
print(df.shape)
```

### wnba_aging_curve {#wnba_aging_curve}

`wnba_aging_curve(*, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

WNBA aging curve -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_aging_curve.nba_aging_curve` for the
full contract; this is a by-reference re-export, same algorithm, women's
bundled artifact.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64`.

| col_name | type | description |
|---|---|---|
| `age` | integer | Player age (in years). |
| `rel_value` | double | Value multiplier for this age relative to the peak age: a delta-method curve chaining minutes-weighted within-player consecutive-age changes in per-100-possession box-score value, quadratic-smoothed and min-max scaled to [0.4, 1.0], so the peak age is exactly 1.0 and the lowest-valued age 0.4. |
| `peak_age` | double | Age at which rel_value reaches its maximum of 1.0, repeated on every row for filtering and joining (29.0 in the bundled curve). |

**Example**

```python
from sportsdataverse.wnba import wnba_aging_curve
curve = wnba_aging_curve()
```

### wnba_career_trajectory {#wnba_career_trajectory}

`wnba_career_trajectory(player_values: 'pl.DataFrame', *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

WNBA career trajectory -- the NBA core bound to `league="wnba"`.

See `sportsdataverse.nba.nba_aging_curve.nba_career_trajectory` for
the full contract.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` and `proj_next_value`.

**Example**

```python
import polars as pl
from sportsdataverse.wnba import wnba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [26], "value": [10.0]})
wnba_career_trajectory(player_values)
```

### wnba_draft_model {#wnba_draft_model}

`wnba_draft_model(draft_year: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project WNBA prospect career value + draft probability from draft slot.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year (e.g. `2023`) or list of years. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8`. Empty input returns the zero-row schema, never raises.

**Example**

```python
from sportsdataverse.wnba import wnba_draft_model
board = wnba_draft_model(2023)
```

### wnba_in_game_win_prob {#wnba_in_game_win_prob}

`wnba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA in-game win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  |  |
| `pregame_home_prob` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### wnba_predict_games {#wnba_predict_games}

`wnba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

WNBA vectorized pregame predictions (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_predict_games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  |  |
| `ratings` | `DataFrame` |  |  |
| `league_id` | `str` | `'00'` |  |
| `return_as_pandas` | `bool` | `False` |  |

### wnba_predict_margin {#wnba_predict_margin}

`wnba_predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'`

WNBA expected margin (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  |  |
| `away_net` | `float` |  |  |
| `home_pace` | `float` |  |  |
| `away_pace` | `float` |  |  |
| `neutral` | `bool` | `False` |  |
| `league_id` | `str` | `'00'` |  |

### wnba_predict_total {#wnba_predict_total}

`wnba_predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'`

WNBA expected total (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_total.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  |  |
| `home_def` | `float` |  |  |
| `away_off` | `float` |  |  |
| `away_def` | `float` |  |  |
| `home_pace` | `float` |  |  |
| `away_pace` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |

### wnba_rookie_projection {#wnba_rookie_projection}

`wnba_rookie_projection(draft_year: "'int | list[int]'", *, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

WNBA rookie/sophomore projection -- composes the WNBA draft/aging/availability pieces.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year or list of years. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64, proj_soph_value:Float64, proj_rookie_min:Float64, proj_avail_pct:Float64, pro_tier:Utf8`. Empty input -> zero-row schema.

**Example**

```python
from sportsdataverse.wnba import wnba_rookie_projection
board = wnba_rookie_projection(2023)
```

### wnba_team_ratings {#wnba_team_ratings}

`wnba_team_ratings(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

WNBA team ratings (league_id='10'). See sportsdataverse.nba.nba_team_ratings.nba_team_ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  |  |
| `league_id` | `str` | `'00'` |  |
| `as_of_date` | `Union[date, None]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

### wnba_win_prob_from_margin {#wnba_win_prob_from_margin}

`wnba_win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'`

WNBA home win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.win_prob_from_margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |

### most_recent_wnba_season {#most_recent_wnba_season}

`most_recent_wnba_season()`

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it's May or later (the WNBA regular
season has tipped off), otherwise the previous calendar year.

**Returns**

Year (e.g. `2024`) suitable for passing as a `season` argument to schedule / loader functions.

**Example**

```python
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```

### wnba_player_crosswalk {#wnba_player_crosswalk}

`wnba_player_crosswalk(season: 'Optional[int]' = None, min_confidence: 'float' = 0.92, *, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source player crosswalk (ESPN / WNBA Stats / Fox).

One row per ESPN athlete per team. `match_method` / `match_confidence`
describe the **Stats API** match (normalized exact name, then
Jaro-Winkler with jersey and DOB tiebreaks); Fox contributes
`fox_athlete_id` only.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `min_confidence` | `float` | `0.92` | Jaro-Winkler floor for fuzzy matches (R default 0.92). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-team ESPN or Fox roster fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN athlete, 21 columns.

**Example**

```python
from sportsdataverse.wnba import wnba_player_crosswalk
df = wnba_player_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Tighten the fuzzy floor

strict = wnba_player_crosswalk(season=2026, min_confidence=0.97)

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "fuzzy_jw").head()
```

### wnba_schedule_crosswalk {#wnba_schedule_crosswalk}

`wnba_schedule_crosswalk(season: 'Optional[int]' = None, *, stats_games: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, strict: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source schedule crosswalk (ESPN / WNBA Stats).

One row per game, joined on `(game_date, home_espn_team_id,
away_espn_team_id)` after both sides reduce to the Eastern-Time date. The
Stats CDN serves the current season only, so the live builder is
effectively current-season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `stats_games` | `Optional[DataFrame]` | `None` | Pre-fetched Stats schedule frame; `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |
| `strict` | `bool` | `False` | Raise on the first failed per-date ESPN scoreboard fetch (a 404 is still skipped) instead of skipping isolated failures. Default `False` matches the R producers; a provider whose every item failed raises either way. An item the host *answered* -- including a 404 -- counts as answered. |

**Returns**

`pl.DataFrame` (or pandas) with `SCHEDULE_COLUMNS`.

**Example**

```python
from sportsdataverse.wnba import wnba_schedule_crosswalk
df = wnba_schedule_crosswalk(season=2026)
print(df["match_method"].value_counts())

# Pipeline next step (one line)

df.filter(pl.col("match_method") == "both").select("espn_game_id", "wnba_game_id").head()
```

### wnba_team_crosswalk {#wnba_team_crosswalk}

`wnba_team_crosswalk(season: 'Optional[int]' = None, *, stats: 'Optional[pl.DataFrame]' = None, fox: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Build the WNBA cross-source team crosswalk (ESPN / WNBA Stats / Fox).

One row per ESPN team, keyed on `espn_team_id`. The Stats side is
derived from the season schedule's home/away team fields (as in wehoop)
and joined on the normalized `city + name`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season year (e.g. `2026`). Defaults to the most recent WNBA season. |
| `stats` | `Optional[DataFrame]` | `None` | Pre-fetched Stats team directory. `None` derives it from the Stats schedule. |
| `fox` | `Optional[DataFrame]` | `None` | Pre-fetched `fox_wnba_teams()` frame. `None` fetches live. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

`pl.DataFrame` (or pandas), one row per ESPN team, with `TEAM_COLUMNS`.

**Example**

```python
from sportsdataverse.wnba import wnba_team_crosswalk
df = wnba_team_crosswalk(season=2026)
print(df.shape)

# Offline with pre-fetched provider frames

df = wnba_team_crosswalk(season=2026, stats=my_stats, fox=my_fox)

# Pipeline next step (one line)

df.select("espn_team_id", "wnba_team_id", "match_method").head()
```
