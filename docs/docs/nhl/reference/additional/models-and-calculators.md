---
title: "NHL — additional Python functions — Models and calculators"
sidebar_label: "Models and calculators"
sidebar_position: 3
description: "NHL — additional Python functions — Models and calculators — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Models and calculators

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

### nhl_expected_assists {#nhl_expected_assists}

`nhl_expected_assists(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', xg_model: 'ShotXGModel | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per-player expected primary/secondary assists from xG-weighted goal credit.

Each goal credits its `assist1` player its **relative danger**
`goal_xg / mean_goal_xg` as x_primary (and `assist2` likewise as
x_secondary). Normalizing to the league-mean goal xG is what makes the
total credit **unbiased** -- `Sum(x_primary + x_secondary) ~= Sum(actual
assists)` -- while still rewarding a playmaker who sets up high-danger
goals (relative danger > 1) over one who feeds tap-ins (< 1). Crediting
raw `goal_xg` (~0.1-0.2) instead would put expected assists on the xG
scale, an order of magnitude below the assist count, and could never be
unbiased against actual assists.
`assists_above_expected = (primary + secondary) - (x_primary + x_secondary)`
(positive = the player's assisted goals were lower-danger than average, so
they out-assisted their shot quality); `primary_share = primary /
(primary + secondary)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key (unused today -- assist credit is league-agnostic; kept for signature parity with the other microstat models and the PWHL shim). |
| `xg_model` | `ShotXGModel \| None` | `None` | A fitted `~sportsdataverse.nhl.nhl_microstat_constants.ShotXGModel`; fit on `pbp` when `None`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `primary_assists`, `secondary_assists`, `x_primary_assists`, `x_secondary_assists`, `assists_above_expected`, `primary_share`. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_expected_assists import nhl_expected_assists

out = nhl_expected_assists(pbp)

# PWHL

out_pwhl = nhl_expected_assists(pwhl_pbp, league="pwhl")
```

### nhl_goalie_gsax {#nhl_goalie_gsax}

`nhl_goalie_gsax(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Per-goalie goals-saved-above-expected (GSAx) for the games in `pbp`.

Scores every unblocked shot via `nhl_xg`, attributes each shot to the defending
goalie (attribute_goalie`), and aggregates `xga = sum(xg)`, `ga =
count(goals)`, `gsax = xga - ga`. `gsax_per_60` uses an on-ice-seconds proxy
derived from the pbp event span each goalie is credited on (see
toi_seconds_by_goalie`) -- `shifts` is accepted for interface parity with the
rest of the player-impact spine but is not currently required for TOI.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame (or an already `nhl_xg`-scored one -- re-scoring is idempotent since the prior `xg` column is dropped first). |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame (currently unused; accepted for interface parity -- see the module docstring). |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg` (booster directory). |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

`player_id:Int64, goalie:Utf8, shots:Int64, xga:Float64, ga:Int64, gsax:Float64, gsax_per_60:Float64`. League-wide `sum(gsax) == sum(xga) - sum(goals)`, which is `~= 0` at large sample and exactly zero only under perfect league-wide xG calibration. Empty/malformed input returns a zero-row frame with this schema -- never raises.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_gsax import nhl_goalie_gsax
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
gsax = nhl_goalie_gsax(pbp, pl.DataFrame(), model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(gsax.sort("gsax", descending=True))

# Pipeline next step

gsax.filter(pl.col("shots") >= 10).sort("gsax_per_60", descending=True).head()
```

### nhl_skater_rapm {#nhl_skater_rapm}

`nhl_skater_rapm(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', lam: 'float | None' = None, as_of: 'int | None' = None, strength_states: 'list[str] | None' = None, return_as_pandas: 'bool' = False, _stints: 'pl.DataFrame | None' = None) -> "'pl.DataFrame | pd.DataFrame'"`

Per-skater xG-based Regularized Adjusted Plus-Minus (RAPM), per 60 minutes.

Builds shift stints (`build_stints`), the sparse off/def design matrix
(`build_design`), and solves the weighted ridge (`weighted_ridge`). Offensive
rating is the `off_<player>` coefficient; defensive rating is the **negated**
`def_<player>` coefficient (suppressing xG-against is positive value) --
`xg_rapm = xg_rapm_off + xg_rapm_def`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects the ridge lambda-grid via `LEAGUE_CONSTANTS` when `lam` is not given. |
| `lam` | `float \| None` | `None` | an explicit ridge penalty; `None` selects via k-fold CV over `LEAGUE_CONSTANTS[league].rapm_lambda_grid`. |
| `as_of` | `int \| None` | `None` | forwarded to `build_stints` -- the leakage-boundary cutoff. |
| `strength_states` | `list[str] \| None` | `None` | restrict the design matrix to these `strength_state` values (e.g. `["5v5"]` for an even-strength-only fit, as used by `nhl_skater_war`'s `ev_off`/`ev_def` components so they don't overlap with `nhl_special_teams_value`'s PP/PK components). `None` (default) uses every strength state, matching the general-purpose all-situations RAPM. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |
| `_stints` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built stints frame, bypassing `pbp`/`shifts`/scoring (not part of the public contract). |

**Returns**

`player_id:Int64, xg_rapm_off:Float64, xg_rapm_def:Float64, xg_rapm:Float64, toi_minutes:Float64`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
rapm = nhl_skater_rapm(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(rapm.sort("xg_rapm", descending=True).head(10))
```

### nhl_skater_war {#nhl_skater_war}

`nhl_skater_war(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Per-skater GAR/WAR composite -- EV + special-teams + faceoffs + penalties.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`/`nhl_skater_rapm`/ `nhl_special_teams_value`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

`player_id:Int64, ev_off:Float64, ev_def:Float64, pp:Float64, pk:Float64, pens:Float64, faceoffs:Float64, gar:Float64, war:Float64`. `ev_off`/`ev_def` are `(5v5-only RAPM rate - replacement level) * EV TOI/60`; `gar` sums every component; `war = gar / goals_per_win`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_war import nhl_skater_war
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
war = nhl_skater_war(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(war.sort("war", descending=True).head(10))
```

### nhl_team_ratings {#nhl_team_ratings}

`nhl_team_ratings(seasons: 'Union[int, list[int]]', *, league: 'str' = 'nhl', as_of_date: '_dt.date | None' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Opponent-adjusted, shrunk even-strength xG (+ goal) team ratings.

Loads pbp + schedule for `seasons`, restricts to even strength, applies
the as-of-date leakage split if requested, opponent-adjusts + shrinks both
the xG rate (primary) and the realized-goal rate (concurrent sanity
rating) via `adjust_rate_opponent`, and derives off/def/net ranks.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | an int or iterable of seasons. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- resolves HFA/avg/shrink_k via `sportsdataverse.nhl.nhl_prediction_constants.get_constants`. |
| `as_of_date` | `date \| None` | `None` | if given, only games strictly before this date are used (the leakage boundary for a predictive backtest). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per (season, team). Empty input seasons return a zero-row frame with the documented schema. |col_name |type | |:----------|:------| |season |Int64 | |team |String | |adj_xgf |Float64| |adj_xga |Float64| |adj_xg_net |Float64| |adj_gf |Float64| |adj_ga |Float64| |games |Int64 | |off_rank |Int64 | |def_rank |Int64 | |net_rank |Int64 | |net_z |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_team_ratings import nhl_team_ratings

ratings = nhl_team_ratings(2023)
print(ratings.sort("net_rank").head())

# As-of-date leakage-safe rating

import datetime as dt
ratings = nhl_team_ratings(2023, as_of_date=dt.date(2023, 1, 1))

# Pipeline next step (one line)

ratings.filter(pl.col("team") == "TOR")
```

### nhl_unit_ratings {#nhl_unit_ratings}

`nhl_unit_ratings(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', unit_type: 'str' = 'forward_line', min_toi: 'float' = 20.0, return_as_pandas: 'bool' = False, _stints: 'pl.DataFrame | None' = None, _rapm: 'pl.DataFrame | None' = None) -> "'pl.DataFrame | pd.DataFrame'"`

Per on-ice skater combination: observed xGF/xGA + shrinkage-blended summed RAPM.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`/`nhl_skater_rapm`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. |
| `unit_type` | `str` | `'forward_line'` | `"forward_line"` (3-skater combinations) or `"defense_pair"` (2-skater combinations) -- see the module's data-availability caveat. |
| `min_toi` | `float` | `20.0` | minimum minutes-together for a unit to be reported. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |
| `_stints` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built stints frame. |
| `_rapm` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built skater-RAPM frame (paired with stints`; both must be given together to bypass real computation). |

**Returns**

`team:Utf8, unit_ids:Utf8 (sorted "id-id-id"), unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64, on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
units = nhl_unit_ratings(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(units.sort("unit_value", descending=True).head(10))
```

### nhl_xg {#nhl_xg}

`nhl_xg(pbp: 'pl.DataFrame', *, model_dir: 'str | Path | None' = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Score every unblocked shot in `pbp` with the published `nhl_xg_models` boosters.

Ports fastRhockey's `helper_nhl_calculate_xg` -- routes 5v5 shots to the 5v5
booster and every other strength state to the special-teams booster, overrides
penalty shots with the constant `xg_model_ps`, then left-joins `xg` back onto
`pbp` by `event_id`. Attaches the danger/distance/angle expansion
(`add_shot_geometry`) after scoring.

**Known issue -- the published boosters over-predict for seasons through 2023-24.**
Measured 2026-09-02 against the 2026-04 boosters currently in the `nhl_xg_models`
release: observed goals / sum(`xg`) is **0.771** at 5v5 (n=1,724,290 shots) and
**0.768** on special teams (n=349,232), where a correctly-levelled model gives 1.0 --
i.e. `xg` is inflated by roughly 25-30% for every season from 2009-10 through
2023-24. At 5v5 the two most recent seasons are much closer (2024-25 **0.949**,
2025-26 **0.913**). The cause is **not identified**. It is not a defect in the
feature frame this function builds, and it is not the missing-`MISSED_SHOT`
training corpus recorded here previously: every season carries missed shots
(27.7-34.8% of Fenwick events), the trainer's Fenwick selector takes
`MISSED_SHOT` alongside `SHOT` and `GOAL`, and the published artifacts'
`base_score` (0.07368 / 0.10533) matches the Fenwick goal rate (0.0695) rather
than the shots-on-goal rate (0.0977). Leave-one-season-out refits land at
goals / sum(`xg`) of 0.95-1.05 per season, so the miscalibration is a property
of the published artifact rather than of the data it was trained on.
Shot RANKING is far less affected (rank AUC 0.778 / 0.760), so `xg` is still usable
for ordering chances -- but any SUM of `xg` (per game, per player, team totals,
goals-above-expected, and `nhl_gsax` downstream) is inflated for pre-2024-25
seasons. Tracking:
[sportsdataverse-py#444](https://github.com/sportsdataverse/sportsdataverse-py/issues/444);
evidence:
[fastRhockey-nhl-data#11](https://github.com/sportsdataverse/fastRhockey-nhl-data/pull/11).
To check whether this still applies to the boosters you have, restrict to the rows
this function actually scored -- `xg` non-null, i.e. unblocked shots only -- and
compare `sum(xg)` against the goals **on those same rows**, separately for
`strength_state == "5v5"` and for the rest, since the two come from different
boosters and are quoted separately above; a corrected booster gives a ratio near 1.0
for each. Comparing against a season's full goal total instead would fold in
shootout and penalty-shot goals and every unscored row, and would not validate the
numbers above. The same measurement is packaged as
`nhl_data_build.xg_parity.artifact_calibration(pbp, booster, variant=...)`
(`variant` is `"5v5"` or `"st"`) in `fastRhockey-nhl-data`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `model_dir` | `str \| Path \| None` | `None` | booster directory; `None` downloads-and-caches on first use (see `ensure_xg_models`). Offline callers should pass the committed fixture dir. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects the danger-zone geometry bands (the PWHL borrows the NHL boosters themselves; see `xg_booster_league`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

`pbp` with `xg:Float64`, `distance_to_net:Float64`, `shot_angle:Float64`, `shot_danger:Utf8` appended (null/absent for non-shot rows). Empty/malformed input returns the input frame with a null `xg` column -- never raises.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_xg import nhl_xg
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(scored.filter(pl.col("xg").is_not_null()).height)

# Pandas round-trip

scored_pd = nhl_xg(pbp, return_as_pandas=True)
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
