---
title: "WNBA — additional Python functions — Models and calculators"
sidebar_label: "Models and calculators"
sidebar_position: 6
description: "WNBA — additional Python functions — Models and calculators — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Models and calculators

### build_wnba_season_wp {#build_wnba_season_wp}

`build_wnba_season_wp(season: 'int', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

A WNBA season's play-by-play with win-probability columns joined in.

Loads the season's play-by-play, schedule, and team boxscores, builds a
leakage-free weekly as-of pregame anchor per game from the WNBA ratings
engine (`league_id="10"`), scores every play through the bundled
in-game win-probability artifact, and returns the full `load_wnba_pbp`
frame with `pregame_home_prob` + `home_win_prob` appended -- the
enrich-in-place shape that overwrites the season's
`play_by_play_<season>.parquet` release asset.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season year (e.g. `2024`); bounded by `load_wnba_pbp` release availability. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The season's `load_wnba_pbp` frame (every column preserved) with the two WP columns `pregame_home_prob` + `home_win_prob` appended (both `Float64`), sorted by `game_id` then `game_play_number`.

| col_name | type | description |
|---|---|---|
| `game_play_number` | integer | Game play number |
| `id` | integer | Unique play identification number |
| `sequence_number` | integer | Sequence number representing a shot-possession (V3 PBP). |
| `type_id` | integer | Type identifier (numeric). |
| `type_text` | character | Display text for the type field. |
| `text` | character | Text description of the play / record. |
| `away_score` | integer | Away team score at the time of the play. |
| `home_score` | integer | Home team score at the time of the play. |
| `period_number` | integer | Numeric period (1-4 for quarters; 5+ for OT). |
| `period_display_value` | character | Period display label (e.g. '1st Quarter', 'OT'). |
| `clock_display_value` | character | Game clock display string (e.g. '8:32'). |
| `scoring_play` | logical | TRUE if the play resulted in points scored. |
| `score_value` | integer | Point value of the play (2 / 3 / 1). |
| `team_id` | integer | Unique team identifier. |
| `athlete_id_1` | integer | Primary athlete identifier (e.g. shooter). |
| `athlete_id_2` | integer | Secondary athlete identifier (e.g. assister / fouler). |
| `athlete_id_3` | integer | Athlete id 3. |
| `wallclock` | character | Wallclock. |
| `shooting_play` | logical | TRUE if the play was a shooting attempt. |
| `coordinate_x_raw` | double | X coordinate as returned by the API before any adjustment. |
| `coordinate_y_raw` | double | Y coordinate as returned by the API before any adjustment. |
| `points_attempted` | integer |  |
| `short_description` | character |  |
| `game_id` | integer | Unique game identifier. |
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `season_type` | integer | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_name` | character | Home team name. |
| `home_team_mascot` | character | Home team mascot. |
| `home_team_abbrev` | character | Home team three-letter abbreviation. |
| `home_team_name_alt` | character | Alternate versions of the home team abbreviation |
| `away_team_id` | integer | Unique identifier for the away team. |
| `away_team_name` | character | Away team name. |
| `away_team_mascot` | character | Away team mascot. |
| `away_team_abbrev` | character | Away team three-letter abbreviation. |
| `away_team_name_alt` | character | Alternate versions of the away team abbreviation |
| `game_spread` | double | Game spread in (-X Team) format. There are almost none, I would recommend not trusting any of these three columns |
| `home_favorite` | logical | Logical (TRUE/FALSE) indicating whether the home team is favored |
| `game_spread_available` | logical | Logical (TRUE/FALSE) indicating whether the spread was available from ESPN. Basically, I would just not recommend using any of the spread information, I think I defaulted a lot of them to -2.5 for the home team. Most games probably do not have spread information. This column should really be listed first |
| `home_team_spread` | double | The game spread with respect to the home team |
| `qtr` | integer | Quarter of the game |
| `time` | character | Time left within the period |
| `clock_minutes` | integer | Clock minutes split from seconds for developer convenience |
| `clock_seconds` | double | Clock seconds split from minutes for developer convenience |
| `home_timeout_called` | logical |  |
| `away_timeout_called` | logical |  |
| `half` | integer | Half of the game |
| `game_half` | integer | Half of the game |
| `lag_qtr` | integer | A lag column on the quarter |
| `lead_qtr` | integer | A lead column on the quarter |
| `lag_half` | integer | A lag column on the half |
| `lead_half` | integer | A lead column on the half |
| `start_quarter_seconds_remaining` | double | Quarter seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_half_seconds_remaining` | double | Game half seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_game_seconds_remaining` | double | Game seconds remaining at the start of the play (''') |
| `end_quarter_seconds_remaining` | double | Quarter seconds remaining at the end of the play (''') |
| `end_half_seconds_remaining` | double | Game half seconds remaining at the end of the play (''') |
| `end_game_seconds_remaining` | double | Game seconds remaining at the end of the play (''') |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `coordinate_x` | double | X coordinate on the court (half-court layout). |
| `coordinate_y` | double | Y coordinate on the court (half-court layout). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_date_time` | character | Game start date/time (ISO 8601). |
| `athlete_name_1` | character |  |
| `athlete_name_2` | character |  |
| `athlete_name_3` | character |  |
| `type_abbreviation` | character | Play type abbreviation |
| `pregame_home_prob` | double |  |
| `home_win_prob` | double |  |

**Example**

```python
from sportsdataverse.wnba import build_wnba_season_wp
wp = build_wnba_season_wp(2024)
wp.select("game_id", "game_play_number", "home_win_prob").head()

# Pandas output

wp_pd = build_wnba_season_wp(2024, return_as_pandas=True)
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

No returns table is published for this function: no capture: its input is a user-built player-age panel (player_id, age, value) that no package function produces.

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

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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

**Returns**

One row per play: the five feature columns plus `home_win_prob`.

| col_name | type | description |
|---|---|---|
| `score_diff` | double |  |
| `sec_left` | double |  |
| `sqrt_sec_left` | double |  |
| `pregame_logit` | double |  |
| `home_has_ball` | integer |  |
| `home_win_prob` | double |  |

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

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

No returns table is published for this function: no capture: it needs one row per game with string home_team_id / away_team_id matching wnba_team_ratings.team_id, and every package schedule carries integer ids.

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

**Returns**

Expected margin in points (positive favors the home team).

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

**Returns**

Expected combined points scored by both teams.

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

No returns table is published for this function: no capture: it reads stats.wnba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg, raw_pace, games, rank, adj_net_z`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season identifier (4-digit year or 'YYYY-YY' string). |
| `team_id` | character | Unique team identifier. |
| `adj_off_rtg` | double |  |
| `adj_def_rtg` | double |  |
| `adj_net_rtg` | double |  |
| `adj_pace` | double |  |
| `raw_off_rtg` | double |  |
| `raw_def_rtg` | double |  |
| `raw_pace` | double |  |
| `games` | integer | Games played. |
| `rank` | integer | Rank. |
| `adj_net_z` | double |  |

### wnba_win_prob_from_margin {#wnba_win_prob_from_margin}

`wnba_win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'`

WNBA home win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.win_prob_from_margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  |  |
| `league_id` | `str` | `'00'` |  |

**Returns**

Probability the home team wins, in `(0, 1)`.
