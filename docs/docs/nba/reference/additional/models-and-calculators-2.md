---
title: "NBA — additional Python functions — Models and calculators: predict_margin–win_prob"
sidebar_label: "Models and calculators: predict_margin–win_prob"
sidebar_position: 11
description: "NBA — additional Python functions — Models and calculators: predict_margin–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: predict_margin–win_prob

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
