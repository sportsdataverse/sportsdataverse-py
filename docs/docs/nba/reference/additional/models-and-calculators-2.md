---
title: "NBA — additional Python functions — Models and calculators: nba_four–win_prob"
sidebar_label: "Models and calculators: nba_four–win_prob"
sidebar_position: 11
description: "NBA — additional Python functions — Models and calculators: nba_four–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: nba_four–win_prob

### nba_four_factor_rapm {#nba_four_factor_rapm}

`nba_four_factor_rapm(possessions: 'pl.DataFrame', *, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design.

Each factor is regressed on the identical offense/defense design matrix,
differing only in the per-possession response (`_FACTOR_RESPONSES`).
Output mirrors the oracle's `RA_*__Off/__Def` layout. **DECISION 5/6/7**
govern the response definitions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m, fg3m, ftm, oreb, tov` and the ten lineup columns. Must also carry a `points` column -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally) requires it unconditionally even though none of the four factor responses use it. `offense_team_id` is NOT required here (unlike `nba_la_rapm`): none of the four factor responses need the offense-only shooter join. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override, shared by all four factor fits. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `FOUR_FACTOR_SCHEMA` — `{factor}__off` / `{factor}__def` columns per factor, plus possession counts. Empty input → zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `efg__off` | Float64 |  |
| `efg__def` | Float64 |  |
| `ftr__off` | Float64 |  |
| `ftr__def` | Float64 |  |
| `orbd__off` | Float64 |  |
| `orbd__def` | Float64 |  |
| `tov__off` | Float64 |  |
| `tov__def` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_four_factor_rapm
ff = nba_four_factor_rapm(season_poss)
print(ff.sort("efg__off", descending=True).head())
```

### nba_in_game_win_prob {#nba_in_game_win_prob}

`nba_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play home win probability from the bundled in-game model.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/nba/models/nba_in_game_wp.ubj` for NBA -- a shallow
xgboost booster, trained on 2022-23 so the 2023-24 calibration backtest
stays out-of-sample; escalated from a plain logistic that failed the
per-bucket calibration gate).

Gate note: the plan's concurrent oracle (stats.nba.com
`winprobabilitypbp` HOME_PCT) is a dead endpoint, so this model is
validated ONLY on realized-outcome calibration, not against a native WP
feed. See the fixtures README + SDD ledger.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_nba_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob`.

| col_name | type | description |
|---|---|---|
| `score_diff` | Float64 |  |
| `sec_left` | Float64 |  |
| `sqrt_sec_left` | Float64 |  |
| `pregame_logit` | Float64 |  |
| `home_has_ball` | Int64 |  |
| `home_win_prob` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | If the bundled artifact is an xgboost `.ubj` and `xgboost` is not installed. |

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = nba_in_game_win_prob(pbp, 0.62)
```

### nba_la_rapm {#nba_la_rapm}

`nba_la_rapm(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, alphas: 'Optional[np.ndarray]' = None, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Luck-adjusted RAPM: ridge on an expected-points response (high-variance shooting regressed).

Replaces realized 3-point and free-throw outcomes with the shooter's shrunk
expected value (`luck_adjusted_response`); 2-pt makes stay realized.
**DECISION 2/3/4** govern the response recipe and shrinkage constants.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame with team-level `fg2m` and the ten lineup columns; join keys `game_id` + `possession_number` + `offense_team_id` (the last is required by `luck_adjusted_response`'s defense-shooter leak filter). Must also carry a `points` column even though the LA response (`la_points`) supersedes it for fitting -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally via `_prepare`) requires it unconditionally. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame from `build_possession_shooting`. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override; `None` → shrink from `shooting`. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule (`cv=` `ORACLE_RAPM_CV` always; there is no plain-schedule mode). |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `LA_RAPM_SCHEMA`. Empty input → zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `o_la_rapm` | Float64 |  |
| `d_la_rapm` | Float64 |  |
| `la_rapm` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_la_rapm
df = nba_la_rapm(season_poss, season_shooting)
print(df.sort("la_rapm", descending=True).head())

# Planted-truth shooter rates (e.g. for testing)

df = nba_la_rapm(season_poss, season_shooting, {7: (0.4, 0.8)})
```

### nba_matchup_drapm {#nba_matchup_drapm}

`nba_matchup_drapm(season: 'str', *, league_id: 'str' = '00', matchups: 'Optional[pl.DataFrame]' = None, config: 'Optional[PlaytypeConfig]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Matchup-based defensive RAPM (offense-quality-controlled).

Fits `points_allowed_per_100 ~ defender_FE + offense_FE` via
`~sklearn.linear_model.RidgeCV` (weighted by matchup possessions),
reusing the shipped RAPM ridge machinery on the
`build_matchup_drapm_design` two-way-FE design.

**Sign + scale:** the design target `y` is already points-allowed *per 100*
matchup possessions (`100 * player_pts / partial_poss`), so the defender
coefficient is already on the per-100 scale -- `matchup_drapm =
-(beta_defender - mean_beta_defender)` (centered, NO extra ×100, unlike
`~sportsdataverse.nba.nba_rapm.nba_rapm` whose `y` is per-*possession*
and needs the ×100). Sign is negated so higher = better defense (fewer points
allowed), matching the `d_rapm` convention. Typical magnitudes are a few to
low-double-digit points per 100 vs the league defender average.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `matchups` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leagueseasonmatchups`-shaped frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `config` | `Optional[PlaytypeConfig]` | `None` | `~sportsdataverse.nba.nba_playtype_constants.PlaytypeConfig`; defaults to a fresh instance (`ridge_alphas` = the shared RAPM grid, `min_matchup_poss` = 25.0 inclusion floor). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per defender: `player_id` (Int64), `matchup_drapm` (Float64, points-allowed-per-100 estimate, higher = better defense), `matchup_poss` (Float64, total matchup possessions guarded). Returns a zero-row frame with this schema when the upstream fetch/injection is empty or no row survives the `min_matchup_poss` floor (sparse-coverage leagues never raise).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `matchup_drapm` | Float64 |  |
| `matchup_poss` | Float64 |  |

**Example**

```python
from sportsdataverse.nba import nba_matchup_drapm
d = nba_matchup_drapm("2023-24")
print(d.sort("matchup_drapm", descending=True).head(10))

# Injected offline (oracle / test) path

d = nba_matchup_drapm("2023-24", matchups=matchups_df)

# Pipeline next step

d.filter(pl.col("matchup_poss") >= 200).sort("matchup_drapm", descending=True)
```

### nba_predict_games {#nba_predict_games}

`nba_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home/away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` / `predict_total`
math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace` (the `~sportsdataverse.nba.nba_team_ratings.nba_team_ratings` output for one season/as-of date). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

No returns table is published for this function: no capture: it raises a join-key dtype mismatch on a real schedule (the schedule's home_team_id is Int32, nba_team_ratings' team_id is String).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a join-key dtype mismatches `ratings['team_id']`, or if `ratings` has duplicate `team_id` rows (e.g. multiple seasons passed at once). |

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
preds = nba_predict_games(games, nba_team_ratings(2024))
```

### nba_rookie_projection {#nba_rookie_projection}

`nba_rookie_projection(draft_year: "'int | list[int]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project rookie/sophomore value by composing draft x aging x availability.

Composition (no re-derived features -- each term is the verbatim public
output of ①②③):

- `base = nba_draft_model(...).proj_career_value * rookie_fraction`
  (`rookie_fraction` from the bundled residual artifact -- the share of
  career value realized in a single rookie season).
- `proj_rookie_value = base * rel_value(rookie_age) / rel_value(peak_age)
  + residual[pro_tier]`; `proj_soph_value` uses `rookie_age + 1`.
- `proj_avail_pct` from `sportsdataverse.nba.nba_availability.nba_availability`
  at rookie age -- reported separately, **never** multiplied into the
  value columns (availability is availability, not skill).
- `proj_rookie_min = games_full_season * proj_avail_pct * expected_mpg(pro_tier)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `int \| list[int]` |  | A draft year or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional college-side prior frame, forwarded verbatim to `sportsdataverse.nba.nba_draft_model.nba_draft_model`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64, proj_soph_value:Float64, proj_rookie_min:Float64, proj_avail_pct:Float64, pro_tier:Utf8`. Empty input -> zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_rookie_projection
board = nba_rookie_projection(2019)
print(board.sort("proj_rookie_value", descending=True).head())
```

### nba_spm {#nba_spm}

`nba_spm(box_features: 'pl.DataFrame', coefficients: 'SpmCoefficients', *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Apply fitted SPM coefficients to per-100 box features -> OSPM/DSPM/SPM.

Applies a linear scoring rule:

.. code-block:: text

    ospm = X @ o_coef + o_intercept
    dspm = X @ d_coef + d_intercept
    spm  = ospm + dspm

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id`, every column in `coefficients.feature_names`, `min`, and `gp`. |
| `coefficients` | `SpmCoefficients` |  | A `SpmCoefficients` instance from `train_spm`. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame` instead of a `polars.DataFrame`. |

**Returns**

Per-player frame with columns `player_id` (Int64), `ospm` (Float64), `dspm` (Float64), `spm` (Float64), `min` (Float64), `gp` (Int64).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `ospm` | Float64 |  |
| `dspm` | Float64 |  |
| `spm` | Float64 |  |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |

**Example**

```python
from sportsdataverse.nba import nba_spm
ratings = nba_spm(box_feats, coef)
print(ratings.sort("spm", descending=True).head())

# Pipeline next step

ratings.filter(pl.col("min") >= 500).sort("spm", descending=True)
```

### nba_team_ratings {#nba_team_ratings}

`nba_team_ratings(seasons: 'Union[int, list[int]]', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Opponent-adjusted team ratings (AdjOffRtg/AdjDefRtg/AdjNet/AdjPace), as-of-date aware.

Loads schedule + team box score for `seasons`, optionally filters to
games strictly before `as_of_date` (the leakage boundary, via
`~sportsdataverse.nba.nba_prediction_constants.as_of_ratings_split`),
computes per-game efficiency, runs the opponent-adjustment fixed points,
and adds a per-season dense `rank` (on `adj_net_rtg` descending) and
`adj_net_z` (z-score of `adj_net_rtg`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2024`) or list of seasons. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | If given, only games with `date < as_of_date` are used (predictive/backtest usage); `None` computes full-season descriptive ratings. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg, raw_pace, games, rank, adj_net_z`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year. |
| `team_id` | String | Unique team identifier. |
| `adj_off_rtg` | Float64 |  |
| `adj_def_rtg` | Float64 |  |
| `adj_net_rtg` | Float64 |  |
| `adj_pace` | Float64 |  |
| `raw_off_rtg` | Float64 |  |
| `raw_def_rtg` | Float64 |  |
| `raw_pace` | Float64 |  |
| `games` | Int64 | Games played. |
| `rank` | Int64 | Rank. |
| `adj_net_z` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
ratings = nba_team_ratings(2024)
ratings.sort("rank").head()

# As-of-date (leakage-safe) ratings for a backtest

import datetime as dt
ratings = nba_team_ratings(2024, as_of_date=dt.date(2024, 1, 15))

# WNBA / G-League via ``league_id``

wnba_ratings = nba_team_ratings(2024, league_id="10")
```

### nba_war {#nba_war}

`nba_war(ratings: 'pl.DataFrame', poss: 'pl.DataFrame', *, replacement_level: 'float', pts_per_win: 'float', rating_col: 'str' = 'rating', poss_col: 'str' = 'poss', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Points-above-replacement -> wins for each player.

`war_i = (rating_i - replacement_level) * poss_i / 100 / pts_per_win`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Per-player rating frame with `player_id` and `rating_col` (e.g. `nba_rapm`'s `rapm` column renamed, a `nba_ratings_panel` row filtered to one date, or `nba_bpm`'s `bpm` column). |
| `poss` | `DataFrame` |  | Per-player possession-count frame with `player_id` and `poss_col` (e.g. `off_poss + def_poss` from `nba_rapm`). |
| `replacement_level` | `float` |  | Per-100-possession rating of a replacement-level player. No built-in default — calibrate via `calibrate_replacement_level`. |
| `pts_per_win` | `float` |  | Points of season point-margin per marginal win. No built-in default — calibrate via `calibrate_pts_per_win`. |
| `rating_col` | `str` | `'rating'` | Column in `ratings` to score. |
| `poss_col` | `str` | `'poss'` | Column in `poss` giving total possessions played. |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Frame with `WAR_SCHEMA` columns (`player_id`, `war`). Empty (that schema) when either input is empty.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `war` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If both inputs are non-empty but share no `player_id` (a probable id-source or dtype mismatch, not a normal empty case). |

**Example**

```python
from sportsdataverse.nba.nba_war import nba_war
war = nba_war(rapm_df.rename({"rapm": "rating"}), poss_df,
               replacement_level=-2.0, pts_per_win=250.0)
print(war.sort("war", descending=True).head())

# Derive both required kwargs from real data first

from sportsdataverse.nba.nba_war import (
    calibrate_pts_per_win, calibrate_replacement_level, nba_war,
)
pts_per_win = calibrate_pts_per_win(team_standings)
repl = calibrate_replacement_level(
    ratings, poss, pts_per_win=pts_per_win, target_total_war=300.0,
)
war = nba_war(ratings, poss, replacement_level=repl, pts_per_win=pts_per_win)
```

### ot_adjusted_seconds {#ot_adjusted_seconds}

`ot_adjusted_seconds(pbp: 'pl.DataFrame', *, regulation_periods: 'int' = 4) -> 'pl.DataFrame'`

Tag overtime rows and give them the overtime period clock as their seconds remaining.

The in-game models clip `start_game_seconds_remaining` at zero, so every overtime play
(negative in the NBA frame) would score as "game over at the current margin". Each overtime is
scored as its own five-minute endgame instead: on rows with `period_number > regulation_periods`
`start_game_seconds_remaining` becomes the period clock, `start_quarter_seconds_remaining` or
(men's college frames) `start_period_seconds_remaining`, whichever is present and not null.
In a halves game (`regulation_periods=2`) period 3 is the first overtime.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the basketball pbp schema. |
| `regulation_periods` | `int` | `4` | The game's regulation periods (4 quarters, 2 halves); see `game_regulation_periods`. |

**Returns**

The frame with `wp_regime` (`"reg"` / `"ot"`) and the adjusted seconds column.

| col_name | type | description |
|---|---|---|
| `wp_regime` | String | "reg" for regulation plays; "ot" for plays after the last regulation period (period 3 on in a halves game, 5 on in a quarters game), scored on the overtime period clock as a five-minute endgame. |
| `start_game_seconds_remaining` | Float64 | Seconds remaining the model sees: the game clock in regulation, the overtime period clock (the quarter clock, or the period clock in men's college frames) on overtime plays. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If `period_number` or `start_game_seconds_remaining` is missing, or neither period clock column is present. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import ot_adjusted_seconds
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
tagged = ot_adjusted_seconds(pbp)
tagged.filter(pl.col("wp_regime") == "ot").select("start_game_seconds_remaining").head()

# A men's college game (two halves)

from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
ot_adjusted_seconds(one, regulation_periods=2)
```

### player_game_rates {#player_game_rates}

`player_game_rates(player_box: 'pl.DataFrame', team_box: 'pl.DataFrame', *, periods: 'Optional[int]' = None, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'pl.DataFrame'`

True-shooting and usage for every boxed player of one game.

TS% = PTS / (2 (FGA + 0.44 FTA)); USG% = 100 ((FGA + 0.44 FTA + TOV) (team minutes / 5)) /
(MIN (team FGA + 0.44 team FTA + team TOV)) with team TOV as in `team_game_rates`
(`total_turnovers`, repaired where ESPN doubled or omitted it) and team
minutes = 5 x game minutes (regulation 48 NBA / 40 WNBA, MBB and WBB, 5 per overtime). For MBB and
WBB team TOV follows the college rule of `team_game_rates` (null in WBB before 2009 and in a
WBB game whose two teams together show fewer than 5, so usage is null there). A player with no shots, no minutes,
or `did_not_play` gets `null`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | One game in the `load_nba_player_boxscore` schema. |
| `team_box` | `DataFrame` |  | The game's two `load_nba_team_boxscore` rows. |
| `periods` | `Optional[int]` | `None` | Highest period played (default: the regulation periods). |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 or 4); default `game_regulation_periods` for the league and the team box's `season`. |

**Returns**

One row per `(game_id, athlete_id)` with `team_id, ts_pct, usg_pct`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `athlete_id` | Int64 | ESPN athlete id, the join key to the player box score. |
| `team_id` | Int64 | ESPN team id, the join key to the team box score. |
| `ts_pct` | Float64 | True-shooting percentage, PTS / (2 x (FGA + 0.44 x FTA)); null for a player with no shots, no minutes or a DNP. |
| `usg_pct` | Float64 | Usage rate, 100 x (FGA + 0.44 x FTA + TOV) x (team minutes / 5) / (MIN x team (FGA + 0.44 x FTA + total turnovers)); null for no minutes, a DNP, or a team whose turnovers were not recorded (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `team_box` does not hold exactly two different teams of one `game_id` (a duplicated team row would double the player rows), or the league is unknown, or `league` contradicts `league_id`. |
| `polars.exceptions.ColumnNotFoundError` | If a required box-score column is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import player_game_rates
from sportsdataverse.nba.nba_loaders import load_nba_player_boxscore, load_nba_team_boxscore
team_box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
player_box = load_nba_player_boxscore([2025]).filter(pl.col("game_id") == 401766458)
player_game_rates(player_box, team_box).sort("usg_pct", descending=True).head()
```

### predict_margin {#predict_margin}

`predict_margin(home_net: 'float', away_net: 'float', *, home_pace: 'float', away_pace: 'float', neutral: 'bool' = False, league_id: 'str' = '00') -> 'float'`

Expected home-minus-away margin from two adjusted net ratings.

The AdjNet difference (points/100 possessions) is scaled by the
matchup's `expected_possessions` before the home-court advantage
is added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_net` | `float` |  | Home team's adjusted net rating (`adj_net_rtg`). |
| `away_net` | `float` |  | Away team's adjusted net rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted HFA. |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_margin
predict_margin(10.0, -2.0, home_pace=100.0, away_pace=98.0, neutral=False)
```

### predict_total {#predict_total}

`predict_total(home_off: 'float', home_def: 'float', away_off: 'float', away_def: 'float', home_pace: 'float', away_pace: 'float', *, league_id: 'str' = '00') -> 'float'`

Expected total points from adjusted ratings and paces.

Expected possessions come from `expected_possessions`; each side's
expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_off` | `float` |  | Home adjusted offensive rating (points/100 poss). |
| `home_def` | `float` |  | Home adjusted defensive rating. |
| `away_off` | `float` |  | Away adjusted offensive rating. |
| `away_def` | `float` |  | Away adjusted defensive rating. |
| `home_pace` | `float` |  | Home team's adjusted pace. |
| `away_pace` | `float` |  | Away team's adjusted pace. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the pace anchor. |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import predict_total
predict_total(118.0, 108.0, 110.0, 112.0, 100.0, 98.0)
```

### raw_game_efficiency {#raw_game_efficiency}

`raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-team, per-game possessions + raw offensive/defensive rating.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team box score with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_rtg, def_rtg`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `season` | Int64 | Season year. |
| `date` | String | Date in YYYY-MM-DD format. |
| `team_id` | String | Unique team identifier. |
| `opp_team_id` | String |  |
| `is_home` | Boolean | Whether the team was home. |
| `neutral_site` | Boolean | Neutral site. |
| `poss` | Float64 | Poss. |
| `off_rtg` | Float64 |  |
| `def_rtg` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_schedule, load_nba_team_boxscore
from sportsdataverse.nba.nba_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_nba_schedule([2024]), load_nba_team_boxscore([2024]))
```

### render_report {#render_report}

`render_report(report: 'ValidationReport') -> 'str'`

Render a `ValidationReport` as a human-readable markdown validation card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `report` | `ValidationReport` |  | A populated `ValidationReport` from `validate_model`. |

**Returns**

A multi-section markdown string with one `##` heading per oracle. Sections whose oracle result is `None` (either skipped or not applicable for a point-estimate model) are rendered as `- n/a`.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model, render_report,
)

rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
md = render_report(rep)
print(md)

# Capture the markdown string for downstream use

with open("validation_card.md", "w") as f:
    f.write(render_report(rep))
```

### score_win_prob {#score_win_prob}

`score_win_prob(pbp: 'pl.DataFrame', *, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None, neutral_site: 'Optional[bool]' = False) -> 'pl.DataFrame'`

Per-play home win probability, its play-to-play delta, and the prior, for one game.

NBA and WNBA games score with the NBA / WNBA in-game booster
(`~sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob`), men's and women's college
games with the men's / women's college booster
(`~sportsdataverse.mbb.mbb_game_predict.mbb_in_game_win_prob`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the basketball pbp schema (needs `home_team_spread`, `game_spread_available`, `period_number`, `start_game_seconds_remaining`, the period clock (`start_quarter_seconds_remaining` or `start_period_seconds_remaining`), `home_score`, `away_score`, `team_id`, `home_team_id`), in play order. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA / `"10"` WNBA. |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA). |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 halves, 4 quarters); default from the frame's `season` via `game_regulation_periods`. |
| `neutral_site` | `Optional[bool]` | `False` | Whether the game is at a neutral site: no home-court advantage in a lineless prior; see `home_prior`. |

**Returns**

The input plus `pregame_home_prob`, `prior_source` (`"spread"`, `"neutral"` or `"hfa"`), `wp_regime`, `home_win_prob` and `wp_delta` (the first play's delta is measured from the prior). `home_win_prob` is settled to 1.0 / 0.0 on "End Game" / "End Period" rows (`type_text`) from the last regulation period on (period 2 in a halves game, 4 in a quarters game) where the score is not tied, before `wp_delta` is taken; tied rows go to overtime and keep the model value. A frame without `type_text` is not settled. An empty input returns an empty frame with those columns. A null `home_score` / `away_score` means "no change" and is carried forward within the game for the model (a null before the first score is 0); the returned `home_score` / `away_score` are left as given.

| col_name | type | description |
|---|---|---|
| `pregame_home_prob` | Float64 | Pregame home win probability fed to the in-game model: Phi(home_team_spread / margin_sd) when a line existed, else home-court advantage alone (0.5 at a neutral site without a line). |
| `prior_source` | String | Where the prior came from: "spread" (a real line), "hfa" (no line; home-court advantage only, so the default +2.5 never reaches the model) or "neutral" (no line at a neutral site; prior 0.5). |
| `wp_regime` | String | "reg" for regulation plays; "ot" for plays after the last regulation period (period 3 on in a halves game, 5 on in a quarters game), scored on the overtime period clock as a five-minute endgame. |
| `home_win_prob` | Float64 | Home win probability before the play from the bundled in-game model (score margin, time left, the prior, possession). |
| `wp_delta` | Float64 | Change in home win probability from the previous play; the first play's delta is measured from the pregame prior. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing (`home_team_spread`, `game_spread_available`, `period_number`, `start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`, or neither period clock column). |
| `ValueError` | If the league is unknown, `league` contradicts `league_id`, or `pbp` holds more than one `game_id`. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import score_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = score_win_prob(one)

# Biggest swings

wp.sort(pl.col("wp_delta").abs(), descending=True).select("text", "wp_delta").head(10)

# A men's college tournament game

from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
one = load_mbb_pbp([2026]).filter(pl.col("game_id") == 401830342)
score_win_prob(one, league="mbb", neutral_site=True)
```

### shot_coordinates {#shot_coordinates}

`shot_coordinates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Shot locations in the court frame the Game on Paper hex chart uses.

ESPN's play coordinates are feet across the court with the hoop at `(25, 0)` and `y`
growing toward half court (measured on 401766458: dunks at `y` about 2, a 22-foot corner
three at `(3, 0)`). The chart wants tenths of a foot with the hoop at the origin, so
`x_legacy = (x - 25) * 10` and `y_legacy = y * 10`. Free throws carry no real location
(the NBA frame pins them to `(25, 13.75)`, the college frames to the rim, `(25, 0)`;
`(?i)free ?throw` matches `Free Throw - 1 of 2` and college `MadeFreeThrow`) and ESPN's
missing-coordinate sentinel is a large negative number; both become `null`, as does every
non-shooting play.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Plays in the `load_nba_pbp` schema (`shooting_play`, `type_text`, `coordinate_x_raw`, `coordinate_y_raw`). |

**Returns**

The input plus `x_legacy` and `y_legacy` (Float64).

| col_name | type | description |
|---|---|---|
| `x_legacy` | Float64 | Shot location in tenths of a foot with the hoop at the origin, x across the court; null for free throws, unlocated shots and non-shooting plays. |
| `y_legacy` | Float64 | Shot location in tenths of a foot with the hoop at the origin, y toward half court; null for free throws, unlocated shots and non-shooting plays. |

**Raises**

| Exception | Description |
|---|---|
| `polars.exceptions.ColumnNotFoundError` | If a required column is missing (`shooting_play`, `type_text`, `coordinate_x_raw`, `coordinate_y_raw`). |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import shot_coordinates
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2025]).filter(pl.col("game_id") == 401766458)
shots = shot_coordinates(pbp).filter(pl.col("x_legacy").is_not_null())
shots.select("text", "x_legacy", "y_legacy").head()
```

### team_game_rates {#team_game_rates}

`team_game_rates(team_box: 'pl.DataFrame', *, periods: 'Optional[int]' = None, league_id: 'Optional[str]' = None, league: 'Optional[str]' = None, regulation_periods: 'Optional[int]' = None) -> 'pl.DataFrame'`

Four factors, possessions, ratings and pace for both teams of one game.

Dean Oliver's estimates on the `load_nba_team_boxscore` columns: possessions =
FGA - ORB + TOV + 0.44 FTA, averaged over the two teams; eFG% = (FGM + 0.5 3PM) / FGA;
TOV% = TOV / (FGA + 0.44 FTA + TOV); ORB% = ORB / (ORB + opponent DRB); FT rate = FTA / FGA;
ORtg / DRtg per 100 possessions; pace per regulation length (48 minutes NBA; 40 WNBA, MBB and WBB). `TOV` is the team's player plus team turnovers:
`total_turnovers`, except that when it is null it is `turnovers + team_turnovers`, and when ESPN
has duplicated the team turnover (`team_turnovers == turnovers` and `total_turnovers == 2 *
turnovers`, measured on every 2002-2012 game, e.g. 17/17/34) it is `turnovers`. Without the
`turnovers` / `team_turnovers` columns it is `total_turnovers` as given. For MBB and WBB
`TOV` is `turnovers` (`team_turnovers` in WBB 2009-2012, where the other two are 0), and a 0
is a real 0, except where the WBB box recorded none: in WBB before 2009 (and for a WBB box with
no `season`) and in a WBB game of any season whose two teams together show fewer than 5,
TOV%, possessions, ratings and pace are null, never computed from unrecorded turnovers. A
team's 0 in a game whose total is 5 or more stays 0. A zero denominator gives `null`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_box` | `DataFrame` |  | Exactly two rows (one per team) of one game in the `load_nba_team_boxscore` schema. |
| `periods` | `Optional[int]` | `None` | Highest period played (default: the regulation periods), for the pace denominator. |
| `league_id` | `Optional[str]` | `None` | Legacy alias: `"00"` NBA (48-minute regulation) / `"10"` WNBA (40). |
| `league` | `Optional[str]` | `None` | `"nba"`, `"wnba"`, `"mbb"` or `"wbb"` (default NBA); overtime is 5 minutes in all four. |
| `regulation_periods` | `Optional[int]` | `None` | The game's regulation periods (2 or 4); default `game_regulation_periods` for the league and the box's `season`. |

**Returns**

One row per `(game_id, team_id)` with `efg_pct, tov_pct, orb_pct, ft_rate, possessions, off_rtg, def_rtg, pace, total_turnovers, game_minutes`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN game id, the join key to the play-by-play and box-score frames. |
| `team_id` | Int64 | ESPN team id, the join key to the team box score. |
| `efg_pct` | Float64 | Effective field-goal percentage, (FGM + 0.5 x 3PM) / FGA; null with no attempts. |
| `tov_pct` | Float64 | Turnover rate, total turnovers / (FGA + 0.44 x FTA + total turnovers); null when the denominator is not positive or the box records no turnovers (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |
| `orb_pct` | Float64 | Offensive rebound rate, own ORB / (own ORB + opponent DRB); null when the denominator is not positive. |
| `ft_rate` | Float64 | Free-throw rate, FTA / FGA; null with no attempts. |
| `possessions` | Float64 | Oliver's possession estimate (FGA - ORB + total turnovers + 0.44 x FTA) averaged over the two teams; null when the box records no turnovers (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |
| `off_rtg` | Float64 | Points scored per 100 possessions; null when possessions are not positive. |
| `def_rtg` | Float64 | Points allowed per 100 possessions; null when possessions are not positive. |
| `pace` | Float64 | Possessions per regulation length (48 minutes NBA; 40 WNBA, MBB and WBB), overtime minutes included in the denominator; null when the box records no turnovers. |
| `total_turnovers` | Int64 | The team turnover count every rate above uses, player plus team turnovers; ESPN's doubled pre-2013 NBA total is repaired to the player count; college: the box turnovers (team_turnovers in WBB 2009-2012), null when not recorded (WBB before 2009, or a WBB game whose two teams together show fewer than 5 turnovers). |
| `game_minutes` | Int64 | Minutes played in the game: regulation (48 NBA, 40 WNBA and college) plus five per overtime. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `team_box` does not hold exactly two rows, or the rows are not two different `team_id` values of one `game_id`, or the league is unknown, or `league` contradicts `league_id`. |
| `polars.exceptions.ColumnNotFoundError` | If a required box-score column is missing. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import team_game_rates
from sportsdataverse.nba.nba_loaders import load_nba_team_boxscore
box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
team_game_rates(box).select("team_id", "efg_pct", "off_rtg", "pace")

# WNBA box (40-minute regulation)

team_game_rates(box, league_id="10")

# A women's college game from the halves era

from sportsdataverse.wbb.wbb_loaders import load_wbb_team_boxscore
box = load_wbb_team_boxscore([2010]).filter(pl.col("game_id") == 293170052)
team_game_rates(box, league="wbb", regulation_periods=2)
```

### train_spm {#train_spm}

`train_spm(box_features: 'pl.DataFrame', rapm_target: 'pl.DataFrame', *, feature_names: 'Optional[List[str]]' = None, alpha: 'float' = 100.0) -> 'SpmCoefficients'`

Ridge-fit box features onto `o_rapm` and `d_rapm` (two regressions).

The two models share the same feature matrix but separate target vectors,
producing independent offense and defense coefficient vectors.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `box_features` | `DataFrame` |  | Per-player per-100 features. Must contain `player_id` and every column in *feature_names*. |
| `rapm_target` | `DataFrame` |  | Per-player RAPM target frame with columns `player_id`, `o_rapm`, and `d_rapm`. Only the rows whose `player_id` appears in *box_features* are used (inner join). |
| `feature_names` | `Optional[List[str]]` | `None` | Ordered list of feature columns to regress on. Defaults to `SPM_FEATURES` (= `_STATS` from `nba_box_logs`). |
| `alpha` | `float` | `100.0` | Ridge regularization strength (`sklearn.linear_model.Ridge`). Lower values approach OLS; higher values shrink toward zero. |

**Returns**

`SpmCoefficients` with offense and defense coefficient vectors, intercepts, and the ordered `feature_names`.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If *feature_names* lists a column absent from *box_features* or *rapm_target* is missing required columns. |

**Example**

```python
from sportsdataverse.nba import train_spm
coef = train_spm(box_feats, rapm_ratings)

# With custom regularization

coef = train_spm(box_feats, rapm_ratings, alpha=50.0)
```

### validate_model {#validate_model}

`validate_model(model: 'AnyModel', season_frames: 'List[pl.DataFrame]', *, model_name: 'str' = 'model', oracles: 'Tuple[str, ...]' = ('retrodiction', 'reliability', 'cross_season', 'calibration'), seed: 'int' = 0, external_ratings: 'Optional[pl.DataFrame]' = None, external_oracle: 'Optional[pl.DataFrame]' = None, external_rating_col: 'str' = 'rating', external_oracle_col: 'str' = 'oracle_value', external_join: 'str' = 'id', walk_forward_horizon_days: 'int' = 14, walk_forward_min_games: 'int' = 15) -> 'ValidationReport'`

Run the selected oracles and assemble a `ValidationReport`.

`retrodiction`/`reliability`/`calibration` run on the pooled possessions
(all seasons concatenated); `cross_season` runs on the ordered per-season
frames. Any oracle not selected is left `None`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A fitted or unfitted RAPM-family estimator (`fit(X, y)` protocol). |
| `season_frames` | `List[DataFrame]` |  | Ordered list of per-season possession frames. All frames are concatenated into a single pooled frame for Oracles 1, 2, and 4. |
| `model_name` | `str` | `'model'` | Label written into the returned report and markdown card. |
| `oracles` | `Tuple[str, ...]` | `('retrodiction', 'reliability', 'cross_season', 'calibration')` | Tuple of oracle names to run. Omit a name to skip that oracle and leave its result field `None`. Accepts `"external"` and `"walk_forward"` in addition to the four original names; the default tuple is unchanged, so existing callers are unaffected. |
| `seed` | `int` | `0` | RNG seed forwarded to each oracle for determinism. |
| `external_ratings` | `Optional[DataFrame]` | `None` | The model's own ratings frame -- required when `"external"` is in `oracles`. |
| `external_oracle` | `Optional[DataFrame]` | `None` | A loaded oracle frame (from `nba_oracle_data`) -- required when `"external"` is in `oracles`. |
| `external_rating_col` | `str` | `'rating'` | Rating column name in `external_ratings`. |
| `external_oracle_col` | `str` | `'oracle_value'` | Value column name in `external_oracle`. |
| `external_join` | `str` | `'id'` | `"id"` or `"name"`, forwarded to `external_validity`. |
| `walk_forward_horizon_days` | `int` | `14` | Forwarded to `walk_forward`. |
| `walk_forward_min_games` | `int` | `15` | Forwarded to `walk_forward` as `min_games_before_first_checkpoint`. |

**Returns**

A `ValidationReport` whose fields are populated for every selected oracle and `None` for every skipped oracle.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `"external"` is in `oracles` but `external_ratings` or `external_oracle` is `None`. |

**Example**

```python
from sportsdataverse.nba.nba_model_validation import (
    RidgeRapmModel, validate_model,
)

# season_frames is a list[pl.DataFrame] of possession stints
rep = validate_model(RidgeRapmModel(), season_frames, model_name="plain_rapm")
print(rep.retrodiction.game_margin_rmse)   # out-of-sample margin RMSE
print(rep.reliability.spearman_brown)      # split-half Spearman-Brown
print(rep.calibration)                     # None — RidgeRapmModel has no posterior

# Skip slow oracles when iterating quickly

rep = validate_model(
    RidgeRapmModel(), season_frames,
    oracles=("retrodiction", "reliability"),
)
print(rep.cross_season)   # None — not selected
```

### walk_forward {#walk_forward}

`walk_forward(model: 'AnyModel', possessions: 'pl.DataFrame', *, checkpoint_dates: 'Optional[List[datetime.date]]' = None, horizon_days: 'int' = 14, min_games_before_first_checkpoint: 'int' = 15) -> 'WalkForwardResult'`

Oracle 6: time-ordered "predict tomorrow" retrodiction.

For each checkpoint date D: fit on games with `game_date <= D`, predict
games with `D < game_date <= D + horizon_days`, aggregate to
per-(game, team) margins -- reusing `_fit_on` / `_design_with_ids`
/ `predict_points` / `_team_game_margins` (the same machinery
`retrodiction` uses). `carry_forward_rmse` reapplies the PREVIOUS
checkpoint's fit (no refit) to the current window. `random_fold_rmse` is
`retrodiction`'s pooled game-margin RMSE on the same possessions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model (`RapmModel`/`RatingsModel`/`PriorModel`). |
| `possessions` | `DataFrame` |  | A season possession+lineup frame with `game_date` (from `compile_nba_season`), `game_id`, `offense_team_id`, `points`, and the ten lineup columns. |
| `checkpoint_dates` | `Optional[List[date]]` | `None` | Explicit checkpoint grid; derived from `possessions` via `horizon_days`/`min_games_before_first_checkpoint` when `None` (default). |
| `horizon_days` | `int` | `14` | Days-ahead prediction window per checkpoint (default 14). |
| `min_games_before_first_checkpoint` | `int` | `15` | Distinct-game-date index of the first checkpoint when deriving the default grid (default 15, "~game 15 of the season"). |

**Returns**

`WalkForwardResult`. All metrics `nan` and counts `0` when `possessions` is empty, lacks a `game_date` column, or the derived/given grid produces zero non-degenerate checkpoints.

**Example**

```python
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel, walk_forward
res = walk_forward(RidgeRapmModel(), season_possessions)
print(res.game_margin_rmse, res.carry_forward_rmse, res.random_fold_rmse)
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, league_id: 'str' = '00') -> 'float'`

Home win probability from an expected margin (normal-CDF closed form).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |
| `league_id` | `str` | `'00'` | `"00"`/`"10"`/`"20"` -- selects the fitted margin sigma. |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.nba.nba_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```
