---
title: "NHL — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "NHL — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Other

### load_nhl_games {#load_nhl_games}

`load_nhl_games(return_as_pandas: 'bool' = False)`

Load the NHL games-in-data-repo manifest (no `seasons` argument).

Mirrors fastRhockey (R) `load_nhl_games()` which reads a manifest of every
NHL game that has processed data in the data repository.

Tries the sportsdataverse-data release asset first; falls back to the raw
fastRhockey-data GitHub path.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame of all games in the data repository.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Unique game identifier. |
| `season_full` | character | Full season label (e.g. 20212022). |
| `game_type` | character | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_time` | character | Scheduled start time of the game. |
| `home_team_abbr` | character | Home team abbreviation. |
| `away_team_abbr` | character | Away team abbreviation. |
| `home_team_name` | character | Home team name. |
| `away_team_name` | character | Away team name. |
| `home_score` | integer | Home team final score. |
| `away_score` | integer | Away team final score. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `venue` | character | Venue where the game was played. |
| `series_letter` | character | Single-letter identifier for the playoff series to which this game belongs, used to group games within the same bracket matchup in the NHL games dataset. |
| `playoff_round` | integer | Playoff round identifier. |
| `series_game_number` | integer | Series game number. |
| `season` | integer | Season year (echoed from arg). |
| `game_json` | logical | Whether processed game JSON is available. |
| `game_json_url` | character | URL to the processed game JSON. |
| `PBP` | logical | Whether play-by-play data is available. |
| `team_box` | logical | Whether team box score data is available. |
| `player_box` | logical | Whether player box score data is available. |
| `skater_box` | logical | Whether skater box data is available. |
| `goalie_box` | logical | Whether goalie box data is available. |
| `game_info` | logical | Whether game info data is available. |
| `game_rosters` | logical | Whether game rosters data is available. |
| `scoring` | logical | TRUE when the play results in a score (TD, FG, safety, two-point conversion). |
| `penalties` | logical | Penalty count. |
| `scratches` | logical | Logical flag indicating whether a scratches list (players healthy-scratched and not dressing) is available for this game in the NHL games loader output. |
| `linescore` | logical | Logical flag indicating whether linescore data (period-by-period scoring breakdown) is available for this game in the NHL games loader output. |
| `three_stars` | logical | Whether three stars data is available. |
| `shifts` | logical | Number of shifts. |
| `officials` | logical | Whether officials data is available. |
| `shots_by_period` | logical | Whether shots-by-period data is available. |
| `shootout` | logical | Whether shootout data is available. |

**Example**

```python
load_nhl_games()
```

### load_nhl_goalie_box {#load_nhl_goalie_box}

`load_nhl_goalie_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_goalie_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_nhl_player_box {#load_nhl_player_box}

`load_nhl_player_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_player_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_nhl_skater_box {#load_nhl_skater_box}

`load_nhl_skater_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_skater_boxscores() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_nhl_team_box {#load_nhl_team_box}

`load_nhl_team_box(seasons, return_as_pandas: 'bool' = False)`

Alias of load_nhl_team_boxscore() for naming parity with fastRhockey (R).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` |  |  |  |
| `return_as_pandas` | `bool` | `False` |  |

### load_xg_models {#load_xg_models}

`load_xg_models(model_dir: 'str | Path | None' = None) -> 'dict'`

Load the two published boosters (+ embedded feature names) and the penalty-shot constant.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_dir` | `str \| Path \| None` | `None` | `None` downloads the canonical `nhl_xg_models` release on first use and caches under `booster_cache_dir()`; pass a dir to use local models (the offline test suite always passes the committed fixture dir). |

**Returns**

dict with keys `m5v5`/`mst` (`xgboost.Booster`), `feats_5v5`/`feats_st` (embedded feature-name lists), and `ps` (penalty-shot constant probability).

**Example**

```python
from sportsdataverse.nhl.nhl_xg import load_xg_models
models = load_xg_models("tests/fixtures/nhl_player_impact/xg_models")
```

### most_recent_nhl_season {#most_recent_nhl_season}

`most_recent_nhl_season()`

most_recent_nhl_season - return the season year for "today".

NHL seasons are labeled by the year they end in. October flips the
label to next calendar year (the new season just started), otherwise
the current calendar year is returned.

**Returns**

A season year suitable for season-aware loaders / schedule helpers.

**Example**

```python
from sportsdataverse.nhl import most_recent_nhl_season, espn_nhl_calendar
season = most_recent_nhl_season()
cal = espn_nhl_calendar(season=season)
print(season, cal.height)
```

### year_to_season {#year_to_season}

`year_to_season(year)`

year_to_season - format a starting year as the canonical `YYYY-YY` season string.

NHL season strings (used by `statsapi` / `api-web.nhle.com`) are of the form
`"2023-24"`. This helper converts a starting year (`2023`) into that string.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` |  |  | Starting calendar year of the season (e.g. `2023`). |

**Returns**

Season string formatted as `"YYYY-YY"`.

**Example**

```python
from sportsdataverse.nhl import year_to_season
year_to_season(2023)  # '2023-24'
year_to_season(2009)  # '2009-10'
year_to_season(1999)  # '1999-00'
```

### ImpactConfig {#ImpactConfig}

`ImpactConfig(goals_per_win: 'float', replacement_ev_off: 'float', replacement_ev_def: 'float', league_xg_rate_ev: 'float', league_xg_rate_pp: 'float', league_xg_rate_pk: 'float', rapm_lambda_grid: 'list[float]' = <factory>, penalty_goal_weight: 'float' = 0.18, faceoff_goal_weight: 'float' = 0.02, rink_x_goal_line: 'float' = 89.0, danger_high: 'dict' = <factory>, danger_medium: 'dict' = <factory>, xg_booster_league: 'str' = 'nhl') -> None`

League-specific constants consumed by every player-impact engine function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `goals_per_win` | `float` |  | goals-per-win denominator for GAR->WAR (Task 6.2 fits the NHL value from team wins vs goal differential; seeded here until fit). |
| `replacement_ev_off` | `float` |  | EV offense replacement-level rate (xG/60), subtracted before summing GAR. |
| `replacement_ev_def` | `float` |  | EV defense replacement-level rate (xGA/60 suppressed). |
| `league_xg_rate_ev` | `float` |  | league-average even-strength xG rate (per 60), used as the RAPM intercept sanity check. |
| `league_xg_rate_pp` | `float` |  | league-average power-play xGF rate (per 60). |
| `league_xg_rate_pk` | `float` |  | league-average penalty-kill xGA rate (per 60). |
| `rapm_lambda_grid` | `list[float]` | `<factory>` | candidate ridge penalties for the skater RAPM CV. |
| `penalty_goal_weight` | `float` | `0.18` | goals-per-(penalty drawn - taken) conversion. |
| `faceoff_goal_weight` | `float` | `0.02` | goals-per-(faceoff win - 0.5) conversion. |
| `rink_x_goal_line` | `float` | `89.0` | absolute rink x-coordinate of the goal line (feet), used by the shot-geometry expansion. |
| `danger_high` | `dict` | `<factory>` | `{"max_distance": float, "max_angle": float}` band for "high" danger. |
| `danger_medium` | `dict` | `<factory>` | same shape, wider band for "medium" danger; outside both -> "low". |
| `xg_booster_league` | `str` | `'nhl'` | which league's published boosters back this league's `nhl_xg` scoring (the PWHL borrows the NHL boosters -- a documented approximation). |

### LeagueConstants {#LeagueConstants}

`LeagueConstants(hfa: 'float', margin_sd: 'float', avg_xgf: 'float', avg_total_goals: 'float', total_scale: 'float', shrink_k: 'float', prop_kappa: 'dict', pos_priors: 'dict', prop_team_volume_slope: 'float', in_game_wp_artifact: 'str', min_season: 'int') -> None`

Fitted, league-specific constants for the NHL/PWHL prediction spine.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `hfa` | `float` |  | home-ice edge, expected-goals units. |
| `margin_sd` | `float` |  | standard deviation of the final goal margin (deliberately WIDE for hockey). |
| `avg_xgf` | `float` |  | league mean even-strength xG-for, per game. |
| `avg_total_goals` | `float` |  | league mean total goals per game. |
| `total_scale` | `float` |  | multiplier converting rating differential to total-goals deviation. |
| `shrink_k` | `float` |  | games-played prior strength for rating shrinkage. |
| `prop_kappa` | `dict` |  | empirical-Bayes shrinkage strength per player-prop stat family. |
| `pos_priors` | `dict` |  | per-position (F/D) per-stat-family prior rates. |
| `prop_team_volume_slope` | `float` |  | game-script tilt on a player-prop projection (favored team -> fewer late shots-for). SEEDED PLACEHOLDER (~0.04), not yet fitted -- a future prop-fit task should estimate it from the realized shots-vs-exp_margin slope, mirroring how fit_props.py fits prop_kappa/pos_priors. |
| `in_game_wp_artifact` | `str` |  | filename of the bundled in-game win-probability model under `sportsdataverse/nhl/models/`. |
| `min_season` | `int` |  | earliest season this league's prediction spine supports. |

### add_shot_geometry {#add_shot_geometry}

`add_shot_geometry(df: 'pl.DataFrame', *, league: 'str' = 'nhl') -> 'pl.DataFrame'`

Attach `distance_to_net` / `shot_angle` / `shot_danger` (descriptive output only).

Distance/angle are computed off `x_fixed`/`y` against the rink goal-line
x-coordinate in `LEAGUE_CONSTANTS[league].rink_x_goal_line`; `shot_danger` buckets
into `high`/`medium`/`low` using the `danger_high`/`danger_medium`
distance+angle bands from the same config. These are output columns only -- never
fed back into the boosters (Decision D2; a new feature would force a retrain).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | any frame carrying `x_fixed` and `y` columns. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects the danger-zone bands. |

**Returns**

`df` with `distance_to_net:Float64`, `shot_angle:Float64`, `shot_danger:Utf8` appended.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import add_shot_geometry
out = add_shot_geometry(pl.DataFrame({"x_fixed": [80], "y": [0]}))
```

### adjust_rate_opponent {#adjust_rate_opponent}

`adjust_rate_opponent(game_rates: 'pl.DataFrame', *, for_col: 'str', against_col: 'str', hfa: 'float', avg: 'float', shrink_k: 'float', max_iter: 'int' = 100, tol: 'float' = 0.0001) -> 'pl.DataFrame'`

Opponent-adjust a per-game for/against rate by iterative fixed-point, then shrink.

League-agnostic: every constant (`hfa`, `avg`, `shrink_k`) is passed
in -- no NHL/PWHL number is hard-coded here. This is the flagged T7.2
"rate-iterative + shrinkage" shared-solver candidate (the hockey
counterpart of the NFL/CFB per-play ridge); `for_col`/`against_col`
are symmetric (offense sees opponent defense).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_rates` | `DataFrame` |  | one row per (team, opponent, game) with columns `season`, `team`, `opp_team`, `is_home`, `neutral_site`, and the two numeric rate columns named by `for_col`/`against_col`. |
| `for_col` | `str` |  | name of the team's own-side rate column (e.g. `"xgf"`). |
| `against_col` | `str` |  | name of the team's against-side rate column (e.g. `"xga"`). |
| `hfa` | `float` |  | home-ice edge added to the home side / subtracted from the away side. |
| `avg` | `float` |  | league mean rate to adjust and shrink toward. |
| `shrink_k` | `float` |  | games-played prior strength for the post-convergence shrink. |
| `max_iter` | `int` | `100` | maximum fixed-point iterations. |
| `tol` | `float` | `0.0001` | convergence tolerance on the max absolute update. |

**Returns**

A polars DataFrame, one row per (season, team). |col_name |type | |:------------|:------| |season |Int64 | |team |String | |adj_for |Float64| |adj_against |Float64| |adj_net |Float64| |raw_for |Float64| |raw_against |Float64| |games |Int64 |

**Example**

```python
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
adjust_rate_opponent(
    game_rates, for_col="xgf", against_col="xga",
    hfa=0.2, avg=2.55, shrink_k=15.0,
)
```

### as_of_ratings_split {#as_of_ratings_split}

`as_of_ratings_split(df: 'pl.DataFrame', cutoff_date: '_dt.date', *, date_col: 'str' = 'date') -> 'pl.DataFrame'`

Filter a frame to rows strictly before `cutoff_date` (the leakage boundary).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | a polars DataFrame with a date column. |
| `cutoff_date` | `date` |  | the game date being predicted; only strictly-earlier rows are kept. |
| `date_col` | `str` | `'date'` | name of the date column (default `"date"`). |

**Returns**

The subset of `df` with `df[date_col] < cutoff_date`.

**Example**

```python
import datetime as dt
import polars as pl
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split
df = pl.DataFrame({"date": [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]})
as_of_ratings_split(df, dt.date(2023, 1, 2))
```

### booster_cache_dir {#booster_cache_dir}

`booster_cache_dir(override: 'str | Path | None' = None) -> 'Path'`

Resolve the local cache directory for the downloaded `nhl_xg_models` boosters.

Precedence: explicit `override` argument > `NHL_XG_MODEL_DIR` env var >
`~/.cache/nhl_xg_models`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `override` | `str \| Path \| None` | `None` | an explicit directory (e.g. a committed test-fixture dir); wins over the env var when given. |

**Returns**

The resolved `pathlib.Path` (not created here -- `ensure_xg_models` creates it on first download).

**Example**

```python
from sportsdataverse.nhl.nhl_player_impact_constants import booster_cache_dir
d = booster_cache_dir()
```

### brier_score {#brier_score}

`brier_score(y_true: 'np.ndarray', p_pred: 'np.ndarray') -> 'float'`

Mean squared error between predicted probabilities and binary outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |

**Returns**

The Brier score (0.0 is a perfect forecast).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import brier_score
brier_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### build_design {#build_design}

`build_design(stints: 'pl.DataFrame') -> "tuple['sp.csr_matrix', np.ndarray, np.ndarray, list[int]]"`

Build the sparse RAPM design matrix -- two rows per stint (one per attacking team).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `stints` | `DataFrame` |  | a `build_stints`-shaped frame. |

**Returns**

`(X, y, w, player_index)` where `X` is a `scipy.sparse.csr_matrix` with columns `off_<player>` (all on-ice attackers), `def_<player>` (all on-ice defenders), then a trailing home-ice indicator and intercept column; `y` is the attacking team's xGF per 60; `w` is stint duration (seconds); `player_index` maps each `off_`/`def_` column pair's position to a `player_id` (so column `j` is `off_<player_index[j]>` and column `j + n_players` is `def_<player_index[j]>`).

**Example**

```python
from sportsdataverse.nhl.nhl_rapm import build_design
X, y, w, player_index = build_design(stints)
```

### build_stints {#build_stints}

`build_stints(shifts: 'pl.DataFrame', scored: 'pl.DataFrame', *, as_of: 'int | None' = None) -> 'pl.DataFrame'`

Fold `load_nhl_shifts` CHANGE events into contiguous constant-personnel intervals.

Per game: resolves each shift row's full team name (`event_team`) to home/away via
`team_fullname_to_abbr` + the game's `home_abbr`/`away_abbr` (from `scored`),
then folds `ids_on`/`ids_off` deltas chronologically into a running on-ice set per
side. A new interval begins at every distinct `game_seconds` boundary; the final
interval is closed at the last `scored` event's `game_seconds` + 1 for that game
(there is no explicit "end of game" CHANGE row in the shift-chart feed).

Known simplification: shift-chart id lists do not distinguish position, so
`home_ids`/`away_ids` may include the on-ice goalie's id alongside skaters;
`home_goalie`/`away_goalie` are instead sourced from the overlapping `scored`
events' `home_goalie_id`/`away_goalie_id` (the modal value in the interval).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `scored` | `DataFrame` |  | an `nhl_xg`-scored frame (for the game's `home_abbr`/`away_abbr` and each interval's on-ice xG-for and goalie). |
| `as_of` | `int \| None` | `None` | an optional per-game `game_seconds` cutoff -- intervals starting at or after `as_of` are dropped. This is the leakage boundary for any forward-looking use: features for a game/date must use only stints strictly before that game's cutoff. |

**Returns**

one row per interval -- `game_id:Int64, period:Int64, start_s:Int64, end_s:Int64, duration:Int64, home_ids:List(Int64), away_ids:List(Int64), home_goalie:Int64, away_goalie:Int64, strength_state:Utf8, xgf_home:Float64, xgf_away:Float64`. Empty/malformed `shifts` returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import nhl_xg
from sportsdataverse.nhl.nhl_rapm import build_stints
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
stints = build_stints(shifts, scored)
```

### calibration_table {#calibration_table}

`calibration_table(y_true: 'np.ndarray', p_pred: 'np.ndarray', n_bins: 'int' = 10) -> 'pl.DataFrame'`

Bucket predicted probabilities into bins and compare to actual outcome rates.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `n_bins` | `int` | `10` | Number of equal-width probability bins. |

**Returns**

A `polars.DataFrame` with columns `bin_mid`, `mean_pred`, `mean_actual`, `n` (one row per non-empty bin).

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import calibration_table
calibration_table(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
```

### ensure_xg_models {#ensure_xg_models}

`ensure_xg_models(model_dir: 'str | Path | None' = None) -> 'Path'`

Return a dir holding the 3 published booster files, downloading any missing ones.

Mirrors the fastRhockey/nflverse download-on-demand + cache pattern -- the documented
exception to "no first-use download" (the boosters are a large, already-published,
already-validated artifact; see Decision D1 in the design spec). An explicit
`model_dir` whose files already exist (e.g. the committed offline test fixtures)
never touches the network.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_dir` | `str \| Path \| None` | `None` | directory to check/populate; `None` resolves via `booster_cache_dir()` (env `NHL_XG_MODEL_DIR` override, else `~/.cache/nhl_xg_models`). |

**Returns**

The resolved directory containing all 3 booster files.

**Example**

```python
from sportsdataverse.nhl.nhl_xg import ensure_xg_models
d = ensure_xg_models()  # downloads on first use, cached after
```

### espn_nhl_teams {#espn_nhl_teams}

`espn_nhl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nhl_teams - look up NHL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nhl.espn_nhl_teams.clear_cache().

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Team abbreviation. |
| `team_alternate_color` | character | Team alternate color hex. |
| `team_color` | character | Team primary color hex. |
| `team_display_name` | character | Team display name. |
| `team_id` | character | Unique team identifier. |
| `team_is_active` | logical | TRUE if the team is currently active. |
| `team_is_all_star` | logical | TRUE if the row represents an All-Star team. |
| `team_location` | character | Team city/location. |
| `team_logos` | integer | Team logo metadata. |
| `team_name` | character | Team name. |
| `team_nickname` | character | Team nickname. |
| `team_short_display_name` | character | Team short display name. |
| `team_slug` | character | Team URL slug. |
| `team_uid` | character | ESPN team uid. |

**Example**

```python
from sportsdataverse.nhl import espn_nhl_teams
teams = espn_nhl_teams()
print(teams.shape)
teams.select(["team_id", "team_abbreviation", "team_display_name"]).head()

# Find Tampa Bay Lightning (team_id 14)

import polars as pl
teams.filter(pl.col("team_id") == "14").to_dicts()

# Refresh the cache (the call is ``lru_cache``'d) and round-trip to pandas

espn_nhl_teams.cache_clear()
teams_pd = espn_nhl_teams(return_as_pandas=True)
teams_pd[["team_id", "team_abbreviation", "team_display_name"]].head()
```

### expected_goals {#expected_goals}

`expected_goals(adj_xgf_home: 'float', adj_xga_home: 'float', adj_xgf_away: 'float', adj_xga_away: 'float', neutral: 'bool', *, league: 'str' = 'nhl') -> 'tuple[float, float]'`

Per-team expected goals, blending own offense with opponent defense.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_xgf_home` | `float` |  | home team's opponent-adjusted xG-for rate. |
| `adj_xga_home` | `float` |  | home team's opponent-adjusted xG-against rate. |
| `adj_xgf_away` | `float` |  | away team's opponent-adjusted xG-for rate. |
| `adj_xga_away` | `float` |  | away team's opponent-adjusted xG-against rate. |
| `neutral` | `bool` |  | whether the game is at a neutral site (drops HFA). |
| `league` | `str` | `'nhl'` | resolves `hfa` via `get_constants`. |

**Returns**

A `(eg_home, eg_away)` tuple of expected goals.

**Example**

```python
from sportsdataverse.nhl.nhl_market import expected_goals
expected_goals(2.8, 2.2, 2.5, 2.4, False)
```

### get_constants {#get_constants}

`get_constants(league: 'str') -> 'LeagueConstants'`

Resolve the fitted-constants row for a league.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` |  | `"nhl"` or `"pwhl"`. |

**Returns**

The `LeagueConstants` row for `league`.

**Example**

```python
from sportsdataverse.nhl.nhl_prediction_constants import get_constants
get_constants("nhl").margin_sd
```

### in_game_features {#in_game_features}

`in_game_features(pbp: 'pl.DataFrame', pregame_home_prob: 'float') -> 'pl.DataFrame'`

Per-play in-game win-probability features from game state.

Reads the `load_nhl_pbp_full` schema (`home_score`/`away_score`,
`game_seconds_remaining`, `home_skaters`/`away_skaters`,
`home_goalie_in`/`away_goalie_in`). A live feed can populate the same
five features via a documented column map from
`sportsdataverse.nhl.nhl_api_web_parsers.parse_nhl_web_pbp`
(`homeScore`/`awayScore`/`timeRemaining`/`situationCode`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a play-by-play frame shaped like `load_nhl_pbp_full`. |
| `pregame_home_prob` | `float` |  | the model (2) pregame home win probability (e.g. from `win_prob_from_margin`), converted to a logit and carried as a constant per-play anchor feature. |

**Returns**

A polars DataFrame, one row per play. |col_name |type | |:-------------------|:------| |score_diff |Int32 | |sec_remaining |Float64| |sqrt_sec_remaining |Float64| |strength_diff |Int32 | |home_goalie_pulled |Int8 | |away_goalie_pulled |Int8 | |pregame_logit |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_market import in_game_features, win_prob_from_margin
pregame_p = win_prob_from_margin(0.3)
feats = in_game_features(pbp, pregame_home_prob=pregame_p)
```

### log_loss_score {#log_loss_score}

`log_loss_score(y_true: 'np.ndarray', p_pred: 'np.ndarray', eps: 'float' = 1e-15) -> 'float'`

Binary cross-entropy loss between predicted probabilities and outcomes.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Array of binary outcomes (0/1). |
| `p_pred` | `ndarray` |  | Array of predicted probabilities in [0, 1]. |
| `eps` | `float` | `1e-15` | Clipping bound to avoid `log(0)`. |

**Returns**

The mean log loss.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import log_loss_score
log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
```

### mae {#mae}

`mae(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

Mean absolute error between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The mean absolute error.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import mae
mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
```

### predict_margin {#predict_margin}

`predict_margin(adj_xgf_home: 'float', adj_xga_home: 'float', adj_xgf_away: 'float', adj_xga_away: 'float', neutral: 'bool', *, league: 'str' = 'nhl') -> 'float'`

Expected home-minus-away goal margin.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_xgf_home` | `float` |  | home team's opponent-adjusted xG-for rate. |
| `adj_xga_home` | `float` |  | home team's opponent-adjusted xG-against rate. |
| `adj_xgf_away` | `float` |  | away team's opponent-adjusted xG-for rate. |
| `adj_xga_away` | `float` |  | away team's opponent-adjusted xG-against rate. |
| `neutral` | `bool` |  | whether the game is at a neutral site. |
| `league` | `str` | `'nhl'` | resolves `hfa` via `get_constants`. |

**Returns**

`eg_home - eg_away`.

**Example**

```python
from sportsdataverse.nhl.nhl_market import predict_margin
predict_margin(2.8, 2.2, 2.5, 2.4, False)
```

### predict_total {#predict_total}

`predict_total(adj_xgf_home: 'float', adj_xga_home: 'float', adj_xgf_away: 'float', adj_xga_away: 'float', neutral: 'bool', *, league: 'str' = 'nhl') -> 'float'`

Expected total goals, variance-corrected by the fitted `total_scale`.

The raw `eg_home + eg_away` sum is built from opponent-adjusted,
**shrunk** ratings, which systematically compress the total's spread
below the real-world variance (confirmed at fitting time: the OLS slope
of realized total on the raw sum is ~1.91, not 1.0). `total_scale`
corrects for that: the raw total's deviation from the league-average
total is stretched by `total_scale` before adding back the league mean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `adj_xgf_home` | `float` |  | home team's opponent-adjusted xG-for rate. |
| `adj_xga_home` | `float` |  | home team's opponent-adjusted xG-against rate. |
| `adj_xgf_away` | `float` |  | away team's opponent-adjusted xG-for rate. |
| `adj_xga_away` | `float` |  | away team's opponent-adjusted xG-against rate. |
| `neutral` | `bool` |  | whether the game is at a neutral site. |
| `league` | `str` | `'nhl'` | resolves `hfa`/`avg_total_goals`/`total_scale` via `get_constants`. |

**Returns**

The variance-corrected expected combined goal total for the game.

**Example**

```python
from sportsdataverse.nhl.nhl_market import predict_total
predict_total(2.8, 2.2, 2.5, 2.4, False)
```

### prepare_xg_features {#prepare_xg_features}

`prepare_xg_features(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Port of `helper_nhl_prepare_xg_data` -- one row per unblocked shot, model features.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame (`x`, `x_fixed`, `strength_state`, `home_skaters`/`away_skaters`, `game_seconds`, `event_id`, `secondary_type`, `event_team_abbr`, `home_abbr`/`away_abbr`, `season`, `empty_net` -- see `load_nhl_pbp_full`'s returns table). |

**Returns**

one row per unblocked shot (`SHOT`/`MISSED_SHOT`/`GOAL`) carrying every era one-hot, shot-type one-hot, last-event one-hot, and the derived `rebound`/`rush`/`cross_ice_event`/`total_skaters_on`/ `event_team_advantage`/`empty_net` columns the boosters expect. Empty/ malformed input returns a zero-row frame (never raises).

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import prepare_xg_features
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
feat = prepare_xg_features(pbp)
print(feat.shape)
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  |  |

### spearman_corr {#spearman_corr}

`spearman_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

Spearman rank correlation between two arrays.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `ndarray` |  | First array of values. |
| `b` | `ndarray` |  | Second array of values (same length as `a`). |

**Returns**

The Spearman rank correlation coefficient.

**Example**

```python
import numpy as np
from sportsdataverse._common.metrics import spearman_corr
spearman_corr(np.array([1, 2, 3]), np.array([3, 1, 2]))
```

### team_fullname_to_abbr {#team_fullname_to_abbr}

`team_fullname_to_abbr(name: 'str') -> 'str | None'`

Map an NHL full team display name to its abbreviation, or `None` if unknown.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | a full team display name as it appears in `load_nhl_shifts`'s `event_team` column (e.g. `"Buffalo Sabres"`). |

**Returns**

The team abbreviation matching `load_nhl_pbp_full`'s `event_team_abbr` / `home_abbr` / `away_abbr` convention, or `None` for an unmapped name.

**Example**

```python
from sportsdataverse.nhl.nhl_player_impact_constants import team_fullname_to_abbr
team_fullname_to_abbr("Buffalo Sabres")  # "BUF"
```

### team_game_xg_rates {#team_game_xg_rates}

`team_game_xg_rates(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', *, even_strength_only: 'bool' = True) -> 'pl.DataFrame'`

Per-(game, team) even-strength xG-for/against + realized goals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a play-by-play frame shaped like `load_nhl_pbp_full`/`load_nhl_pbp_lite` (needs `game_id`, `event_team_abbr`, `home_abbr`, `away_abbr`, `home_skaters`, `away_skaters`, `home_goalie_in`, `away_goalie_in`, `xg`). |
| `schedule` | `DataFrame` |  | a schedule frame with `game_id`, `season`, `date`, `home_abbr`, `away_abbr`, `neutral_site` (`home_goals`/`away_goals` are accepted but ignored -- realized `gf`/`ga` are derived from the pbp's own GOAL events, never from schedule scores; see the module note on the `load_nhl_schedule(s)` placeholder-score bug for seasons <= 2023). |
| `even_strength_only` | `bool` | `True` | restrict to `home_skaters == away_skaters == 5` with both goalies in net (filters out PP/PK/empty-net distortion). |

**Returns**

A polars DataFrame, one row per (game_id, team), both home and away. |col_name |type | |:------------|:------| |game_id |String | |season |Int64 | |date |Date | |team |String | |opp_team |String | |is_home |Boolean| |neutral_site |Boolean| |xgf |Float64| |xga |Float64| |gf |Int64 | |ga |Int64 |

**Example**

```python
from sportsdataverse.nhl.nhl_team_ratings import team_game_xg_rates
from sportsdataverse.nhl import load_nhl_pbp_full, load_nhl_schedules

pbp = load_nhl_pbp_full([2023])
sched = load_nhl_schedules([2023])
rates = team_game_xg_rates(pbp, sched)
print(rates.filter(pl.col("team") == "TOR").head())
```

### weighted_ridge {#weighted_ridge}

`weighted_ridge(X: 'Any', y: 'np.ndarray', w: 'np.ndarray', lam: 'float') -> 'np.ndarray'`

Solve the weighted ridge normal equations `(X'WX + lam*I)^-1 X'Wy`.

Dense path (`numpy.linalg.solve`) for small/dense `X`; conjugate-gradient
(`scipy.sparse.linalg.cg`) for `scipy.sparse` `X` (the skater-RAPM design
matrix, ~thousands of columns).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `Any` |  | design matrix, dense `numpy.ndarray` or any `scipy.sparse` matrix. |
| `y` | `ndarray` |  | response vector. |
| `w` | `ndarray` |  | nonnegative observation weights (e.g. stint duration in seconds). |
| `lam` | `float` |  | ridge penalty. |

**Returns**

The fitted coefficient vector.

**Example**

```python
import numpy as np
from sportsdataverse.nhl.nhl_player_impact_constants import weighted_ridge
X = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
y = np.array([2.0, -1.0, 1.0])
beta = weighted_ridge(X, y, np.ones(3), lam=1e-6)
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, league: 'str' = 'nhl') -> 'float'`

Convert an expected goal margin to a home win probability via Phi(margin/sigma).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | expected home-minus-away goal margin. |
| `league` | `str` | `'nhl'` | resolves `margin_sd` via `get_constants`. |

**Returns**

`P(home win)` in (0, 1).

**Example**

```python
from sportsdataverse.nhl.nhl_market import win_prob_from_margin
win_prob_from_margin(0.35)
```
