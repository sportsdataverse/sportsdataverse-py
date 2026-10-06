---
title: "MBB — additional Python functions — Models and calculators: inject_rapm–win_prob"
sidebar_label: "Models and calculators: inject_rapm–win_prob"
sidebar_position: 7
description: "MBB — additional Python functions — Models and calculators: inject_rapm–win_prob — function reference in sdv-py, the SportsDataverse Python package."
---
# MBB — additional Python functions — Models and calculators: inject_rapm–win_prob

### inject_rapm_into_players {#inject_rapm_into_players}

`inject_rapm_into_players(players: 'list[PlayerOnOffStats]', off_rapm_input: 'RapmProcessingInputs', def_rapm_input: 'RapmProcessingInputs', stats_averages: 'PureStatSet', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', read_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value'), write_value_key: 'ValueKey' = 'value') -> 'None'`

Write `pick_ridge_regression`'s RAPM predictions back onto each player.

Faithful port of `RapmUtils.injectRapmIntoPlayers` (`RapmUtils.ts:781-916`).
For every `onOffReportReplacement` field (minus the possession/title/
separator/`adj_opp` housekeeping keys -- see landmine 11 for the exact,
faithfully-ported omit-key quirk), re-derives that field's off/def target
vectors via `calc_lineup_outputs`, applies each side's
`calculate_rapm` solver, blends in the strong prior (mirroring
`pick_ridge_regression`'s own blend, except for `adj_ppp` which
reuses `off_rapm_input["rapm_adj_ppp"]`/`def_rapm_input["rapm_adj_ppp"]`
directly rather than recomputing), then writes `{playerId}.rapm[field]
= {write_value_key: result, "override": ...}` onto every player not in
`ctx["removed_players"]`.

**NOTE (upstream comment, verbatim): when `write_value_key ==
"old_value"`, this must be called *after* an initial `write_value_key
== "value"` call on the same `players` list** -- the `old_value`
pass .merge`s (lodash_merge`) its results into each player's
*existing* `rapm` dict rather than replacing it, so a player's
`rapm["field"]` ends up carrying both a `value` (from the first
call) and an `old_value` (from the second) side by side.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `players` | `list[PlayerOnOffStats]` |  | The players to write RAPM results onto (mutated in place -- each qualifying player gets a `"rapm"` key set/merged). |
| `off_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s offensive output. |
| `def_rapm_input` | `RapmProcessingInputs` |  | `pick_ridge_regression`'s defensive output. |
| `stats_averages` | `PureStatSet` |  | League/context average stat set -- consulted for each field's off/def offset before `ctx["team_info"]`. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` (the same one `pick_ridge_regression` was called with). |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights, forwarded to `calc_lineup_outputs` / get_strong_weight` exactly as `pick_ridge_regression` does. |
| `read_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- which key (`"value"`/`"old_value"`) to prefer when reading `stats_averages`/`ctx["team_info"]` offsets and when calling `calc_lineup_outputs` (forwarded as its `use_old_val_if_possible` flag). |
| `write_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key each written field carries its result under. |

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import inject_rapm_into_players

inject_rapm_into_players(players, off_results, def_results, {}, ctx, None)
print(players[0]["rapm"]["off_adj_ppp"])  # {"value": ..., "override": None}

# Luck-adjusted two-call sequence (``"value"`` first, THEN ``"old_value"``)

inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("value", "old_value"), "value"
)
inject_rapm_into_players(
    players, off_results, def_results, {}, ctx, None, ("old_value", "old_value"), "old_value"
)
```

### kmeans_fit {#kmeans_fit}

`kmeans_fit(X: 'np.ndarray', k: 'int', seed: 'int', n_init: 'int' = 10, max_iter: 'int' = 100) -> "'tuple[np.ndarray, np.ndarray]'"`

Seeded Lloyd's KMeans, best-of-`n_init` by inertia.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)` (standardize first). |
| `k` | `int` |  | Number of clusters. |
| `seed` | `int` |  | RNG seed (deterministic output). |
| `n_init` | `int` | `10` | Independent restarts. |
| `max_iter` | `int` | `100` | Lloyd iterations per restart. |

**Returns**

`(centers[k, d], labels[n])`.

**Example**

```python
centers, labels = kmeans_fit(Z, k=8, seed=0)
```

### load_artifact {#load_artifact}

`load_artifact(name: 'str') -> 'dict'`

Read a bundled player-value artifact (`mbb/models/<name>.json`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |

**Returns**

The parsed JSON dict.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import load_artifact
art = load_artifact("mbb_box_bpm")
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

### logistic_fit {#logistic_fit}

`logistic_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float' = 1.0) -> 'np.ndarray'`

L2-penalized logistic regression via L-BFGS (intercept unpenalized).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Binary outcomes (0/1). |
| `lam` | `float` | `1.0` | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
coef = logistic_fit(X, drafted, lam=1.0)
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

### mbb_box_bpm {#mbb_box_bpm}

`mbb_box_bpm(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-player-season box Plus/Minus (offense, defense, total).

Aggregates the season's player boxscores, scores the per-100 features
through the bundled team-constrained coefficients, and applies the BPM
team adjustment so each team's minutes-weighted player scores sum to its
adjusted efficiency margin (points per 100 possessions above league
average; positive = good on both ends).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per (player_id, season, team_id): `player_id:Utf8, player, season, team_id:Utf8, min, box_obpm, box_dbpm, box_bpm`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_box_bpm
bpm = mbb_box_bpm(2025)

# Pipeline next step (one line)

bpm.filter(pl.col("min") >= 400).sort("box_bpm", descending=True).head(15)
```

### mbb_bracket_sim {#mbb_bracket_sim}

`mbb_bracket_sim(seeded_field: 'pl.DataFrame', ratings: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Single-elimination Monte Carlo over a bracket-ordered field.

Rows of `seeded_field` are bracket slots: adjacent rows meet in round 1
and winners of adjacent games meet next round (the standard fold). All
games are neutral-site. Round columns are named from the END of a 64-team
bracket (`champion` back to `reach_r32`); with a smaller field the
early columns are 1.0 for everyone (trivially reached).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seeded_field` | `DataFrame` |  | Bracket-ordered rows with `team_id` (and typically `seed` for reference). |
| `ratings` | `DataFrame` |  | One row per team: `team_id, adj_em`. |
| `n_sims` | `int` | `10000` | Number of simulated brackets. |
| `seed` | `int` | `0` | Seed for `numpy.random.default_rng`. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per field team: `team_id, seed?, reach_r32, reach_s16, reach_e8, reach_f4, reach_final, champion` (probabilities).

**Example**

```python
from sportsdataverse.mbb.mbb_season_sim import mbb_bracket_sim
odds = mbb_bracket_sim(field_64, ratings, n_sims=20000, seed=42)
```

### mbb_draft_projection {#mbb_draft_projection}

`mbb_draft_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Draft probability, projected pick, and pro tier per player-season.

`draft_prob` is the probability of being selected in the draft
immediately following the college season; `projected_pick` is the
expected overall pick conditional on being drafted (lower = better);
`pro_tier` buckets the pick through the bundled tier edges.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | A season (e.g. `2025`, feeding the June 2025 draft) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact; womens = WNBA draft). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per qualifying player-season: `player_id:Utf8, player, season, team_id:Utf8, draft_prob, projected_pick, pro_tier`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_draft_projection
board = mbb_draft_projection(2025)

# Pipeline next step (one line)

board.sort("draft_prob", descending=True).head(30)
```

### mbb_in_game_win_prob {#mbb_in_game_win_prob}

`mbb_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play home win probability from the bundled in-game logistic.

Scores `in_game_features` through the committed artifact
(`sportsdataverse/mbb/models`, trained on the season before the pregame
gate season so the calibration backtest stays out-of-sample).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Play-by-play for ONE game in the `load_mbb_pbp` schema (`start_game_seconds_remaining`, `home_score`, `away_score`, `team_id`, `home_team_id`). |
| `pregame_home_prob` | `float` |  | Pregame home win probability (e.g. from `win_prob_from_margin`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per play: the five feature columns plus `home_win_prob`.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob
from sportsdataverse.mbb.mbb_loaders import load_mbb_pbp
pbp = load_mbb_pbp([2024]).filter(pl.col("game_id") == 401638643)
wp = mbb_in_game_win_prob(pbp, 0.62)
```

### mbb_predict_games {#mbb_predict_games}

`mbb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame predictions for a schedule of games.

Joins the ratings frame twice (home / away) and applies the closed-form
`predict_margin` / `win_prob_from_margin` /
`predict_total` math column-wise.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `game_id`, `home_team_id`, `away_team_id` and optionally `neutral_site` (missing column means every game is a true home game). Team-id dtypes must match `ratings['team_id']` exactly. |
| `ratings` | `DataFrame` |  | One row per team with `team_id, adj_o, adj_d, adj_em, adj_tempo` (the `mbb_team_ratings` output for one season / as-of date). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per input game: `game_id, home_team_id, away_team_id, exp_margin, home_win_prob, exp_total`. Games whose teams are missing from `ratings` carry nulls.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import mbb_predict_games
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
preds = mbb_predict_games(games, mbb_team_ratings([2024]))
```

### mbb_recruiting_projection {#mbb_recruiting_projection}

`mbb_recruiting_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Expected freshman box-BPM per recruit + over/under-performance residual.

Scores each recruit of the season's incoming class through the bundled
recruiting ridge (composite grade + log national rank; missing values
imputed with the class median / the bubble rank). When the freshman
season is already observable, `resume_residual = realized box_bpm -
exp_box_bpm` (null otherwise, and `player_id` carries the matched
college athlete id).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | Freshman college season(s) (e.g. `2025` = the class arriving for 2024-25). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit: `recruit_id:Utf8, player_id:Utf8 (nullable), player, season, team_id:Utf8, composite, rank_nat, exp_box_bpm, resume_residual`. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_recruiting_projection
proj = mbb_recruiting_projection(2026)

# Pipeline next step (one line)

proj.sort("exp_box_bpm", descending=True).head(15)
```

### mbb_season_sim {#mbb_season_sim}

`mbb_season_sim(ratings: 'pl.DataFrame', remaining_schedule: 'pl.DataFrame', *, n_sims: 'int' = 10000, seed: 'int' = 0, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Monte Carlo the remaining schedule: expected wins + title odds.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | One row per team: `season, team_id, adj_em` and optionally `conference` (enables `conf_title_prob`) and `current_wins` (added to the simulated remaining wins). |
| `remaining_schedule` | `DataFrame` |  | Games to simulate: `home_team_id, away_team_id, neutral_site`. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `seed` | `int` | `0` | Seed for `numpy.random.default_rng` (deterministic output). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `season, team_id, exp_wins` (mean simulated total wins), `playoff_prob` (share of sims finishing in the top 68 win totals -- a field-size proxy, ties broken by `adj_em`) and `conf_title_prob` (share of sims with the most wins among conference members; ties count for every tied team; null without a `conference` column).

**Example**

```python
from sportsdataverse.mbb.mbb_season_sim import mbb_season_sim
odds = mbb_season_sim(ratings, remaining, n_sims=5000, seed=42)
```

### mbb_shooter_talent {#mbb_shooter_talent}

`mbb_shooter_talent(scored: 'pl.DataFrame', *, league: 'str' = 'mens', k: "'float | None'" = None, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Per-shooter EB-regressed make% over expected + points over expected.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output (needs `shooter_id, made, point_value, xmake, xpoints`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (default `k` source). |
| `k` | `float \| None` | `None` | Shrinkage pseudo-shots; `None` uses `get_constants(league).shrink_k_talent` (fitted split-half). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per shooter: `shooter_id:Utf8, n_shots, make_rate, xmake_mean, oe_pct, oe_pct_regressed, points_over_expected, poe_per_100`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality, mbb_shooter_talent
talent = mbb_shooter_talent(mbb_shot_quality(mbb_shot_data(2025)))

# Pipeline next step (one line)

talent.filter(pl.col("n_shots") >= 200).sort("oe_pct_regressed", descending=True).head(15)
```

### mbb_shot_quality {#mbb_shot_quality}

`mbb_shot_quality(shots: 'pl.DataFrame', *, model: "'pl.DataFrame | None'" = None, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Score each shot with `xmake` / `xpoints` from the cell table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Canonical shot frame. |
| `model` | `DataFrame \| None` | `None` | A `mbb_shot_quality_model` table. When `None` it is built from `shots` itself -- convenient, but leakage-safe evaluation should pass a model fit on PRIOR data. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`shots`'s columns plus `xmake:Float64, xpoints:Float64` (null for cells absent from the model). Empty input returns the input schema plus the two columns, zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality
scored = mbb_shot_quality(mbb_shot_data(2025))

# Pipeline next step (one line)

scored.group_by("team_id").agg(pl.col("xpoints").sum()).sort("xpoints", descending=True)
```

### mbb_shot_quality_model {#mbb_shot_quality_model}

`mbb_shot_quality_model(shots: 'pl.DataFrame', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Empirical-Bayes `zone x type` make-rate / xPoints table.

Each cell's raw make rate is shrunk toward its PARENT-ZONE mean by
`n / (n + k)` with `k = get_constants(league).shrink_k_zone`
pseudo-attempts, so sparse cells (e.g. tip-ins in the mid zone) borrow
strength from their zone; `xpoints = make_rate_shrunk * point_value`
(the cell's modal point value).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `shots` | `DataFrame` |  | Canonical shot frame (needs `shot_zone, shot_type, made, point_value`). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the shrinkage `k`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per `(shot_zone, shot_type)`: `shot_zone, shot_type, n, make_rate_raw, make_rate_shrunk, point_value, xpoints`. Empty input returns the zero-row schema.

**Example**

```python
from sportsdataverse.mbb import mbb_shot_data, mbb_shot_quality_model
model = mbb_shot_quality_model(mbb_shot_data(2025))

# Pipeline next step (one line)

model.sort("xpoints", descending=True).head(5)
```

### mbb_team_ratings {#mbb_team_ratings}

`mbb_team_ratings(seasons: 'int | list[int]', *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo) per team-season.

Loads schedule + team boxscore for `seasons`, computes per-game efficiency,
runs the opponent-adjustment fixed points, and adds a per-season dense
`rank` (on `adj_em` descending) and `adj_em_z` (z-score of `adj_em`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (e.g. `2024`) or list of seasons. |
| `league` | `str` | `'mens'` | `"mens"` / `"womens"` -- selects the constants. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per (season, team_id) with columns `season, team_id, adj_o, adj_d, adj_em, adj_tempo, raw_o, raw_d, games, rank, adj_em_z`. Empty input returns that schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings
ratings = mbb_team_ratings(2024)
ratings.sort("rank").head()
```

### mbb_transfer_projection {#mbb_transfer_projection}

`mbb_transfer_projection(seasons: "'Union[int, list[int]]'", *, league: 'str' = 'mens', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Projected post-transfer box-BPM for each transfer arriving in `seasons`.

Detects the transfer cohort from BOXSCORE discontinuity -- a player who
logged qualifying minutes for different teams in consecutive seasons
(the roster release under-reports moves ~70x, so production is the
cohort source of record; bench-riders pre-move are excluded, which is
fine because they carry no pre production to project from). Joins each
player's pre-transfer (from-season) `box_bpm` and scores the bundled
ridge. `proj_delta = proj_box_bpm - pre_box_bpm` (the expected
move-related change; typically shrinks stars toward the mean).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | Destination season(s), e.g. `2026` = arrived for 2025-26. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the bundled artifact). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per transfer: `player_id:Utf8, player, from_team_id:Utf8, to_team_id:Utf8, to_season:Int64, pre_box_bpm, proj_box_bpm, proj_delta`. Transfers without a qualifying pre-season sample are dropped. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb import mbb_transfer_projection
proj = mbb_transfer_projection(2026)

# Pipeline next step (one line)

proj.sort("proj_box_bpm", descending=True).head(15)
```

### pick_ridge_regression {#pick_ridge_regression}

`pick_ridge_regression(off_weights: 'NDArray[np.float64]', def_weights: 'NDArray[np.float64]', ctx: 'RapmPlayerContext', adaptive_correl_weights: 'list[float] | None', diag_mode: 'bool', agg_value_key: 'ValueKey' = 'value', lineup_value_keys: 'tuple[ValueKey, ValueKey]' = ('value', 'value')) -> 'tuple[RapmProcessingInputs, RapmProcessingInputs]'`

Adaptively pick a ridge-regression lambda and blend in the RAPM priors.

Faithful port of `RapmUtils.pickRidgeRegression` (`RapmUtils.ts:1001-1540`)
-- the top-level driver that, per off/def side: scales a dimensionless
`lambda_range` by the design matrix's mean singular value
(`avg_eigen_val`) into an actual ridge strength, solves via
`slow_regression`/`calculate_rapm`, blends in each player's
strong prior (get_strong_weight`), reconciles the possession
-weighted team total against the actual team efficiency
(`[IMPORTANT-EQUATION-01]`, see below), nudges the result back towards
the weak priors on any remaining error (`apply_weak_priors`), and
decides whether to keep sweeping `lambda` upward, roll back to the
previous step, or stop.

**`[IMPORTANT-EQUATION-01]`** (`RapmUtils.ts:1306-1314`/`:1325-1333`):
`combined_adj_eff = sum(pct_by_player[i] * rapm[i] for i) +
add_low_volume_adj_rtg`, compared against `actual_eff[off_or_def]`
(the team's actual, prior-basis-adjusted efficiency, including
bench/removed-player possessions) to derive `adj_eff_err` -- the error
signal both the weak-prior nudge and the stopping rule react to.

**Stopping rule** (checked once per `lambda` step, in order): (1) once a
*second* step has run (`not_first_step`) and, unless in `diag_mode`,
the current step is past `lambda_range_to_use[3]`, roll back to the
*previous* step's `soln_matrix`/`ridge_lambda` (but **not**
`rapm_adj_ppp`/`rapm_raw_adj_ppp`/`sd_rapm`, which stay at the
current, over-threshold step's values -- a faithful, non-obvious TS
asymmetry, `RapmUtils.ts:1443-1448` vs `:1483-1484`) when
`adj_eff_err >= error_exit_thresh` (`1.35` for the low-possession
-count offense special case, else `1.05`) **and** the error is still
increasing (`>= last_error`); else (2) stop in place once
`mean_diff` (the mean per-player RAPM change since the previous step)
drops below `pick_ridge_thresh` (`0.061` off / `0.091` def --
"more confident in offensive priors"); else (3) keep sweeping.

**Adaptive-weight / prior asymmetry** (the deep-equality oracle's load
-bearing behavior): the per-player strong-prior blend
(get_strong_weight(ctx["prior_info"], adaptive_correl_weights[i])`)
only consults `adaptive_correl_weights` when
`ctx["prior_info"]["strong_weight"] < 0` (adaptive mode) -- a fixed,
non-negative `strong_weight` always wins. A fixture whose
`players_strong` entries carry no `def_adj_ppp` key makes the blend's
`stat.get(f"{off_or_def}_adj_ppp") or 0.0` term (and, transitively,
`calc_lineup_outputs`'s own `strong_val` term) contribute exactly
`0` on the def side regardless of `strong_weight` or
`adaptive_correl_weights` -- see the oracle test's `def_results1`/
`def_results2` invariance assertions.

**`svd` is `numpy.linalg.svd(..., compute_uv=False)`, singular values
only.** Upstream's `SVD(weights[side].valueOf())` (`svd-js`) also
computes `u`/`v`, but only `svd.q` (the singular values, via
`mean(svd.off.q)`/`mean(svd.def.q)` at `avg_eigen_val`,
`RapmUtils.ts:1077`) is ever read -- `u`/`v` are dead. Skipping them
is an efficiency-only deviation with an identical result (singular
values are unique to a matrix regardless of the underlying SVD
implementation).

**Dead-debug computation promoted to a real output (Python-side
addition, not upstream's own shape):** upstream also computes
`residuals`/`errSq`/`paramErrs`/`sdRapm` at this point
(`RapmUtils.ts:1363-1394`) purely to feed a `console.log` gated
behind the same hardcoded-`False` `debugMode` as
`apply_weak_priors` -- none of the four is ever stored on
`acc.output` upstream (`RapmProcessingInputs` has no `sdRapm`
field there either). Since Task 3.4 built
`calculate_predicted_out`/`calculate_residual_error`/
`calc_slow_pseudo_inverse`/`calculate_sd_rapm` specifically
so this task could surface real standard errors, this port keeps
calling all four (matching TS's actual computation, which reuses the
exact same `XᵀX + ridge_lambda·I` inverse `slow_regression`
already computed -- so no *new* failure mode is introduced by keeping
this) and additionally stores the result on `sd_rapm` -- a superset
of, not a divergence from, the upstream return shape.

**`soln_matrix`/`sd_rapm` are nested Python `list`s, not
`NDArray`s.** Every field on the returned `RapmProcessingInputs`
is a plain (possibly nested) Python `list`/`float` specifically so
the whole dict stays comparable via plain `==` -- the oracle's deep
-equality assertions (e.g. `off_results1 == off_results`) would
otherwise raise `ValueError: truth value of an array with more than one
element is ambiguous` the moment Python's dict/list equality machinery
tried to `bool()` a multi-element `ndarray` comparison.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `off_weights` | `NDArray[float64]` |  | The offensive design matrix (e.g. `calc_player_weights`'s first return value). |
| `def_weights` | `NDArray[float64]` |  | The defensive design matrix. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext`. |
| `adaptive_correl_weights` | `list[float] \| None` |  | Optional per-player adaptive-correlation weights (index-aligned with `ctx["col_to_player"]`) -- see the "adaptive-weight / prior asymmetry" note above. |
| `diag_mode` | `bool` |  | If `True`, keeps sweeping every remaining `lambda` step (collecting `prev_attempts` diagnostics for all of them) even after a stopping condition has already fired, and relaxes the rollback/pick eligibility guards for the first few (`< lambda_range_to_use[3]`) diagnostic-only steps. **Not exercised by this task's oracle** (always called with `False`) -- ported faithfully from TS, uncovered by test. |
| `agg_value_key` | `ValueKey` | `'value'` | `"value"` or `"old_value"` -- which key team/aggregate-level reads (`actual_eff`, the low-volume player adjustment) prefer when present. |
| `lineup_value_keys` | `tuple[ValueKey, ValueKey]` | `('value', 'value')` | `(off_key, def_key)` -- forwarded to `calc_lineup_outputs` as its `use_old_val_if_possible` flag (translated: `key == "old_value"`). |

**Returns**

`(off_results, def_results)` -- two `RapmProcessingInputs`.

**Example**

```python
from sportsdataverse.mbb.mbb_rapm import pick_ridge_regression

off_results, def_results = pick_ridge_regression(
    off_weights, def_weights, ctx, None, False
)
print(off_results["ridge_lambda"], off_results["rapm_adj_ppp"][:3])
```

### player_per100_features {#player_per100_features}

`player_per100_features(season_stats: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-100 / rate features for every (player_id, season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season_stats` | `DataFrame` |  | One row per player-season with the canonical counting columns (`minutes, field_goals_made, field_goals_attempted, three_point_field_goals_made, free_throws_attempted, turnovers, points, fga_rim, fga_mid, fga_three, offensive_rebounds, defensive_rebounds, assists, blocks, steals`) -- built from the player-boxscore aggregation (see the Phase-0 fitters). |

**Returns**

One row per (player_id, season): ids as `Utf8` plus the 17 rate / per-100 features. Empty input returns the schema with zero rows.

**Example**

```python
from sportsdataverse.mbb.mbb_player_value_constants import player_per100_features
feats = player_per100_features(season_stats)
```

### poss_calc_fragment_sum {#poss_calc_fragment_sum}

`poss_calc_fragment_sum(a: 'PossCalcFragment', b: 'PossCalcFragment') -> 'PossCalcFragment'`

Field-wise add two `PossCalcFragment`\ s

(`PossCalcFragment.sum`, `PossessionUtils.scala:146-153`).

The Scala original uses `shapeless.Generic` to zip the two case
classes' fields and sum pairwise; since every field is a plain `Int`,
a plain `zip` over `dataclasses.astuple` reproduces the same
behavior without the generic-programming machinery.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `a` | `PossCalcFragment` |  | The left-hand fragment. |
| `b` | `PossCalcFragment` |  | The right-hand fragment. |

**Returns**

A new `PossCalcFragment` with each field summed.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import (
    PossCalcFragment,
    poss_calc_fragment_sum,
)

frag1 = PossCalcFragment(1, 2, 3, 4, 5, 6, 7, 8)
frag2 = PossCalcFragment(1, 3, 5, 7, 9, 11, 13, 15)
poss_calc_fragment_sum(frag1, frag2)
# PossCalcFragment(2, 5, 8, 11, 14, 17, 20, 23)
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_em: 'float', away_adj_em: 'float', neutral: 'bool' = False, *, league: 'str' = 'mens') -> 'float'`

Expected home-minus-away margin from two adjusted efficiency margins.

The AdjEM difference is scaled by the league's fitted `em_scale` (AdjEM
is per-100-possessions; a game margin scales by roughly tempo/100, further
attenuated for as-of estimation noise) before the home-court advantage is
added.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_em` | `float` |  | Home team's adjusted efficiency margin (points / 100 poss). |
| `away_adj_em` | `float` |  | Away team's adjusted efficiency margin. |
| `neutral` | `bool` | `False` | True for a neutral-site game (no home-court advantage). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the fitted em_scale / HFA). |

**Returns**

Expected margin in points (positive favors the home team).

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import predict_margin
predict_margin(20.0, 10.0)
```

### predict_total {#predict_total}

`predict_total(home_adj_o: 'float', home_adj_d: 'float', away_adj_o: 'float', away_adj_d: 'float', home_tempo: 'float', away_tempo: 'float', *, league: 'str' = 'mens') -> 'float'`

Expected total points from adjusted efficiencies and tempos.

Expected possessions are `home_tempo * away_tempo / avg_tempo`; each
side's expected points per 100 possessions blend its offense with the
opponent's defense (`0.5 * (off + opp_def)`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_o` | `float` |  | Home adjusted offensive efficiency (points / 100 poss). |
| `home_adj_d` | `float` |  | Home adjusted defensive efficiency. |
| `away_adj_o` | `float` |  | Away adjusted offensive efficiency. |
| `away_adj_d` | `float` |  | Away adjusted defensive efficiency. |
| `home_tempo` | `float` |  | Home adjusted tempo (possessions / game). |
| `away_tempo` | `float` |  | Away adjusted tempo. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the tempo anchor). |

**Returns**

Expected combined points scored by both teams.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import predict_total
predict_total(110.0, 95.0, 105.0, 100.0, 68.0, 66.0)
```

### rank_corr {#rank_corr}

`rank_corr(a: 'np.ndarray', b: 'np.ndarray') -> 'float'`

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

### raw_game_efficiency {#raw_game_efficiency}

`raw_game_efficiency(schedule: 'pl.DataFrame', team_box: 'pl.DataFrame') -> 'pl.DataFrame'`

Per-team, per-game possessions + raw offensive/defensive efficiency.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `schedule` | `DataFrame` |  | Frame with `game_id, season, date, home_team_id, away_team_id, neutral_site` (ids as strings or ints; cast to `Utf8` here). |
| `team_box` | `DataFrame` |  | Per-team boxscore with `game_id, team_id, field_goals_attempted, offensive_rebounds, turnovers, free_throws_attempted, team_score`. When it also carries `total_turnovers` / `team_turnovers` (the loaders do), a row whose `turnovers` and `total_turnovers` are both 0 takes its count from `team_turnovers` -- where ESPN's 2009-2012 women's box files it. |

**Returns**

One row per (game_id, team_id): `game_id, season, date, team_id, opp_team_id, is_home, neutral_site, poss, off_eff, def_eff`. Empty input returns that schema with zero rows. Team-game rows whose possession estimate is non-positive (an all-zero ESPN boxscore shell) are dropped with a `UserWarning` -- their efficiency is undefined, and one of them poisons the whole season's fixed point. Games in which either team still has 0 turnovers are dropped the same way: the possession estimate would miss its turnover term.

**Example**

```python
from sportsdataverse.mbb.mbb_loaders import load_mbb_schedule, load_mbb_team_boxscore
from sportsdataverse.mbb.mbb_team_ratings import raw_game_efficiency
eff = raw_game_efficiency(load_mbb_schedule([2024]), load_mbb_team_boxscore([2024]))
```

### ridge_cv_lambda {#ridge_cv_lambda}

`ridge_cv_lambda(X: 'np.ndarray', y: 'np.ndarray', groups: 'np.ndarray', lams: "'list[float]'") -> 'float'`

Pick lambda by leave-one-group-out CV (groups = seasons/classes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix. |
| `y` | `ndarray` |  | Targets. |
| `groups` | `ndarray` |  | Group label per row (e.g. season); each held out once. |
| `lams` | `list[float]` |  | Candidate penalties. |

**Returns**

The candidate with the lowest mean held-out MSE.

**Example**

```python
lam = ridge_cv_lambda(X, y, seasons, [0.1, 1, 10, 100])
```

### ridge_fit {#ridge_fit}

`ridge_fit(X: 'np.ndarray', y: 'np.ndarray', lam: 'float') -> 'np.ndarray'`

Closed-form ridge with an unpenalized intercept (coefficient 0).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `X` | `ndarray` |  | Feature matrix `(n, d)`. |
| `y` | `ndarray` |  | Targets `(n,)`. |
| `lam` | `float` |  | L2 penalty on the non-intercept coefficients. |

**Returns**

Coefficient vector of length `d + 1` (intercept first).

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_player_value_constants import ridge_fit
beta = ridge_fit(np.random.rand(50, 3), np.random.rand(50), lam=1.0)
```

### roc_auc {#roc_auc}

`roc_auc(y_true: 'np.ndarray', score: 'np.ndarray') -> 'float'`

Area under the ROC curve via the rank-sum (Mann-Whitney) identity.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `y_true` | `ndarray` |  | Binary outcomes (0/1). |
| `score` | `ndarray` |  | Predicted scores (any monotone scale). |

**Returns**

AUC in `[0, 1]`; `nan` when only one class is present.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_player_value_constants import roc_auc
roc_auc(np.array([0, 1]), np.array([0.2, 0.9]))
```

### save_artifact {#save_artifact}

`save_artifact(name: 'str', obj: 'dict') -> 'None'`

Write a bundled artifact (dev/fitter use -- writes into the source tree).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `str` |  | Artifact stem, e.g. `"mbb_box_bpm"`. |
| `obj` | `dict` |  | JSON-serializable artifact payload. |

**Example**

```python
save_artifact("mbb_box_bpm", {"league": "mens", "coef": [0.1]})
```

### score_to_tuple {#score_to_tuple}

`score_to_tuple(s: 'str') -> 'tuple[int, int]'`

Parse a `"scored-allowed"` score string (`ExtractorUtils.score_to_tuple`,

`ExtractorUtils.scala:107-113`).

Scala's `str match { case regex(s1, s2) => ... }` on a compiled
`Regex` requires the ENTIRE string to match (`Regex.unapplySeq` calls
`Matcher.matches()`, not `find()`) -- ported here as
`re.fullmatch`, not `re.match`/`re.search`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `s` | `str` |  | The raw score string, e.g. `"55-68"`. |

**Returns**

`(scored, allowed)` as a tuple of ints, or `(0, 0)` if `s` doesn't fully match `([0-9]+)-([0-9]+)`.

**Example**

```python
from sportsdataverse.mbb.mbb_ncaa_models import score_to_tuple

score_to_tuple("55-68")   # (55, 68)
score_to_tuple("garbage")  # (0, 0)
```

### simulate_game {#simulate_game}

`simulate_game(home_em: 'float', away_em: 'float', neutral: 'bool', rng: 'np.random.Generator', *, league: 'str' = 'mens') -> 'bool'`

Sample one game outcome: margin `~ Normal(exp_margin, margin_sd)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_em` | `float` |  | Home team's adjusted efficiency margin. |
| `away_em` | `float` |  | Away team's adjusted efficiency margin. |
| `neutral` | `bool` |  | True for a neutral-site game. |
| `rng` | `Generator` |  | A seeded `numpy.random.Generator` (caller owns determinism). |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"`. |

**Returns**

True if the home team wins the sampled game.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_season_sim import simulate_game
simulate_game(20.0, 5.0, False, np.random.default_rng(0))
```

### slow_regression {#slow_regression}

`slow_regression(player_weight_matrix: 'NDArray[np.float64]', ridge_lambda: 'float', ctx: 'RapmPlayerContext') -> 'NDArray[np.float64]'`

Build the Tikhonov (ridge) regression solver matrix.

Faithful port of the private `RapmUtils.slowRegression`
(`RapmUtils.ts:756-769`): `(XᵀX + ridge_lambda·I)⁻¹Xᵀ`, where `X`
is `player_weight_matrix` (one row per lineup, one column per player --
see `calc_player_weights`). See the section banner above for why
this is a plain matrix inverse (`numpy.linalg.inv`), not an SVD.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_weight_matrix` | `NDArray[float64]` |  | The off/def design matrix, shape `(num_lineups, ctx["num_players"])`. |
| `ridge_lambda` | `float` |  | The Tikhonov regularization strength. |
| `ctx` | `RapmPlayerContext` |  | A `RapmPlayerContext` -- only `ctx["num_players"]` is read (sizes the identity matrix). |

**Returns**

The `(num_players, num_lineups)` solver matrix; apply it to a target vector via `calculate_rapm`.

**Example**

```python
import numpy as np
from sportsdataverse.mbb.mbb_rapm import slow_regression, calculate_rapm

x = np.array([[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]])
solver = slow_regression(x, 1.0, ctx)  # ctx["num_players"] == 2
rapm = calculate_rapm(solver, [1.0, 2.0, 3.0])
```

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

### talent_split_mse {#talent_split_mse}

`talent_split_mse(scored: 'pl.DataFrame', *, k: 'float', seed: 'int' = 0) -> 'float'`

Weighted MSE of the k-regressed first half predicting the raw second half.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scored` | `DataFrame` |  | `mbb_shot_quality` output. |
| `k` | `float` |  | Shrinkage pseudo-shots to evaluate. |
| `seed` | `int` | `0` | Split seed. |

**Returns**

`sum(n_h2 * (oe_h1 * n_h1/(n_h1+k) - oe_h2)^2) / sum(n_h2)`.

**Example**

```python
from sportsdataverse.mbb.mbb_shooter_talent import talent_split_mse
talent_split_mse(scored, k=200.0)
```

### transfer_cohort {#transfer_cohort}

`transfer_cohort(rosters: 'pl.DataFrame') -> 'pl.DataFrame'`

One row per transfer: same `player_id`, different `team_id` in

consecutive seasons.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `rosters` | `DataFrame` |  | Frame with `player_id`, `team_id`, `season` (extra columns ignored; one row per player-season-team). |

**Returns**

`player_id: Utf8, from_team_id:Utf8, to_team_id:Utf8, from_season:Int64, to_season:Int64` -- a player transferring twice appears twice.

**Example**

```python
from sportsdataverse.mbb import mbb_box_bpm, transfer_cohort
bpm = mbb_box_bpm([2025, 2026]).filter(pl.col("min") >= 150)
moves = transfer_cohort(bpm.select("player_id", "team_id", "season"))
```

### win_prob_from_margin {#win_prob_from_margin}

`win_prob_from_margin(exp_margin: 'float', *, league: 'str' = 'mens') -> 'float'`

Home win probability from an expected margin (normal-CDF closed form).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `exp_margin` | `float` |  | Expected home-minus-away margin in points. |
| `league` | `str` | `'mens'` | `"mens"` or `"womens"` (selects the fitted margin sigma). |

**Returns**

Probability the home team wins, in `(0, 1)`.

**Example**

```python
from sportsdataverse.mbb.mbb_game_predict import win_prob_from_margin
win_prob_from_margin(5.0)
```
