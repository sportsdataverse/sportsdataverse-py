---
title: "NBA — additional Python functions — Models and calculators: nba_predict–win_prob"
sidebar_label: "Models and calculators: nba_predict–win_prob"
sidebar_position: 11
description: "NBA — additional Python functions — Models and calculators: nba_predict–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: nba_predict–win_prob

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
| `player_id` | integer | Unique player identifier. |
| `ospm` | double |  |
| `dspm` | double |  |
| `spm` | double |  |
| `min` | double | Minutes played. |
| `gp` | integer | Games played. |

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
| `season` | integer | Season year. |
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
| `player_id` | integer | Unique player identifier. |
| `war` | double |  |

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

`ot_adjusted_seconds(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Tag overtime rows and give them the OT period clock as their seconds remaining.

The in-game model clips `start_game_seconds_remaining` at zero, so every overtime play
(negative in the hoopR frame) would score as "game over at the current margin". Each OT period
is scored as its own five-minute endgame instead: `start_game_seconds_remaining` becomes
`start_quarter_seconds_remaining` on rows with `period_number > 4`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the `load_nba_pbp` schema. |

**Returns**

The frame with `wp_regime` (`"reg"` / `"ot"`) and the adjusted seconds column.

| col_name | type | description |
|---|---|---|
| `wp_regime` | character | "reg" for regulation plays; "ot" for overtime plays, which are scored on the overtime period clock as a five-minute endgame. |
| `start_game_seconds_remaining` | double | Seconds remaining the model sees: the game clock in regulation, the overtime period clock on overtime plays. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import ot_adjusted_seconds
tagged = ot_adjusted_seconds(pbp)
tagged.filter(pl.col("wp_regime") == "ot").select("start_game_seconds_remaining").head()
```

### player_game_rates {#player_game_rates}

`player_game_rates(player_box: 'pl.DataFrame', team_box: 'pl.DataFrame', *, periods: 'int' = 4) -> 'pl.DataFrame'`

True-shooting and usage for every boxed player of one game.

TS% = PTS / (2 (FGA + 0.44 FTA)); USG% = 100 ((FGA + 0.44 FTA + TOV) (team minutes / 5)) /
(MIN (team FGA + 0.44 team FTA + team TOV)) with team TOV = `total_turnovers` and team
minutes = 5 x game minutes. A player with no shots, no minutes, or `did_not_play` gets
`null`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_box` | `DataFrame` |  | One game in the `load_nba_player_boxscore` schema. |
| `team_box` | `DataFrame` |  | The game's two `load_nba_team_boxscore` rows. |
| `periods` | `int` | `4` | Highest period played. |

**Returns**

One row per `(game_id, athlete_id)` with `team_id, ts_pct, usg_pct`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game id, the join key to the play-by-play and box-score frames. |
| `athlete_id` | integer | ESPN athlete id, the join key to the player box score. |
| `team_id` | integer | ESPN team id, the join key to the team box score. |
| `ts_pct` | double | True-shooting percentage, PTS / (2 x (FGA + 0.44 x FTA)); null for a player with no shots, no minutes or a DNP. |
| `usg_pct` | double | Usage rate, 100 x (FGA + 0.44 x FTA + TOV) x (team minutes / 5) / (MIN x team (FGA + 0.44 x FTA + total turnovers)); null for no minutes or a DNP. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import player_game_rates
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
| `game_id` | character | Unique game identifier. |
| `season` | integer | Season year. |
| `date` | character | Date in YYYY-MM-DD format. |
| `team_id` | character | Unique team identifier. |
| `opp_team_id` | character |  |
| `is_home` | logical | Whether the team was home. |
| `neutral_site` | logical | Neutral site. |
| `poss` | double | Poss. |
| `off_rtg` | double |  |
| `def_rtg` | double |  |

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

`score_win_prob(pbp: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'pl.DataFrame'`

Per-play home win probability, its play-to-play delta, and the prior, for one game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | One game in the `load_nba_pbp` schema (needs `home_team_spread`, `game_spread_available`, `period_number`, `start_game_seconds_remaining`, `start_quarter_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`), in play order. |
| `league_id` | `str` | `'00'` | `"00"` / `"10"` / `"20"`. |

**Returns**

The input plus `pregame_home_prob`, `prior_source`, `wp_regime`, `home_win_prob` and `wp_delta` (the first play's delta is measured from the prior). An empty input returns an empty frame with those columns.

| col_name | type | description |
|---|---|---|
| `pregame_home_prob` | double | Pregame home win probability fed to the in-game model: Phi(home_team_spread / margin_sd) when a line existed, else home-court advantage alone. |
| `prior_source` | character | Where the prior came from: "spread" (a real line) or "hfa" (no line; home-court advantage only, so the default +2.5 never reaches the model). |
| `wp_regime` | character | "reg" for regulation plays; "ot" for overtime plays, which are scored on the overtime period clock as a five-minute endgame. |
| `home_win_prob` | double | Home win probability before the play from the bundled in-game model (score margin, time left, the prior, possession). |
| `wp_delta` | double | Change in home win probability from the previous play; the first play's delta is measured from the pregame prior. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import score_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
one = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = score_win_prob(one)

# Biggest swings

wp.sort(pl.col("wp_delta").abs(), descending=True).select("text", "wp_delta").head(10)
```

### shot_coordinates {#shot_coordinates}

`shot_coordinates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Shot locations in the court frame the Game on Paper hex chart uses.

ESPN's play coordinates are feet across the court with the hoop at `(25, 0)` and `y`
growing toward half court (measured on 401766458: dunks at `y` about 2, a 22-foot corner
three at `(3, 0)`). The chart wants tenths of a foot with the hoop at the origin, so
`x_legacy = (x - 25) * 10` and `y_legacy = y * 10`. Free throws carry no real location
(`load_nba_pbp` pins them to `(25, 13.75)`) and ESPN's missing-coordinate sentinel is a
large negative number; both become `null`, as does every non-shooting play.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Plays in the `load_nba_pbp` schema (`shooting_play`, `type_text`, `coordinate_x_raw`, `coordinate_y_raw`). |

**Returns**

The input plus `x_legacy` and `y_legacy` (Float64).

| col_name | type | description |
|---|---|---|
| `x_legacy` | double | Shot location in tenths of a foot with the hoop at the origin, x across the court; null for free throws, unlocated shots and non-shooting plays. |
| `y_legacy` | double | Shot location in tenths of a foot with the hoop at the origin, y toward half court; null for free throws, unlocated shots and non-shooting plays. |

**Example**

```python
from sportsdataverse.nba.nba_game_page import shot_coordinates
shots = shot_coordinates(pbp).filter(pl.col("x_legacy").is_not_null())
shots.select("text", "x_legacy", "y_legacy").head()
```

### team_game_rates {#team_game_rates}

`team_game_rates(team_box: 'pl.DataFrame', *, periods: 'int' = 4) -> 'pl.DataFrame'`

Four factors, possessions, ratings and pace for both teams of one game.

Dean Oliver's estimates on the `load_nba_team_boxscore` columns: possessions =
FGA - ORB + TOV + 0.44 FTA, averaged over the two teams; eFG% = (FGM + 0.5 3PM) / FGA;
TOV% = TOV / (FGA + 0.44 FTA + TOV); ORB% = ORB / (ORB + opponent DRB); FT rate = FTA / FGA;
ORtg / DRtg per 100 possessions; pace per 48 minutes. `TOV` is `total_turnovers` (player
plus team turnovers). A zero denominator gives `null`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_box` | `DataFrame` |  | Exactly two rows (one per team) of one game in the `load_nba_team_boxscore` schema. |
| `periods` | `int` | `4` | Highest period played, for the pace denominator. |

**Returns**

One row per `(game_id, team_id)` with `efg_pct, tov_pct, orb_pct, ft_rate, possessions, off_rtg, def_rtg, pace, total_turnovers, game_minutes`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN game id, the join key to the play-by-play and box-score frames. |
| `team_id` | integer | ESPN team id, the join key to the team box score. |
| `efg_pct` | double | Effective field-goal percentage, (FGM + 0.5 x 3PM) / FGA; null with no attempts. |
| `tov_pct` | double | Turnover rate, total turnovers / (FGA + 0.44 x FTA + total turnovers). |
| `orb_pct` | double | Offensive rebound rate, own ORB / (own ORB + opponent DRB). |
| `ft_rate` | double | Free-throw rate, FTA / FGA; null with no attempts. |
| `possessions` | double | Oliver's possession estimate (FGA - ORB + total turnovers + 0.44 x FTA) averaged over the two teams. |
| `off_rtg` | double | Points scored per 100 possessions. |
| `def_rtg` | double | Points allowed per 100 possessions. |
| `pace` | double | Possessions per 48 minutes (overtime minutes included in the denominator). |
| `total_turnovers` | integer | Player plus team turnovers, the count every rate above uses. |
| `game_minutes` | integer | Minutes played in the game: 48 plus five per overtime. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_game_page import team_game_rates
from sportsdataverse.nba.nba_loaders import load_nba_team_boxscore
box = load_nba_team_boxscore([2025]).filter(pl.col("game_id") == 401766458)
team_game_rates(box).select("team_id", "efg_pct", "off_rtg", "pace")
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
| `feature_names` | `Optional[List[str]]` | `None` | Ordered list of feature columns to regress on. Defaults to `SPM_FEATURES` (= STATS` from `nba_box_logs`). |
| `alpha` | `float` | `100.0` | Ridge regularization strength (`sklearn.linear_model.Ridge`). Lower values approach OLS; higher values shrink toward zero. |

**Returns**

`SpmCoefficients` with offense and defense coefficient vectors, intercepts, and the ordered `feature_names`.

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
per-(game, team) margins -- reusing fit_on` / design_with_ids`
/ `predict_points` / team_game_margins` (the same machinery
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
