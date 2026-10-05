---
title: "NBA — additional Python functions — Nba: adj–la"
sidebar_label: "Nba: adj–la"
sidebar_position: 4
description: "NBA — additional Python functions — Nba: adj–la — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Nba: adj–la

### nba_adj_rapm {#nba_adj_rapm}

`nba_adj_rapm(possessions: 'pl.DataFrame', prior: 'Dict[int, Tuple[float, float]]', *, alphas: 'np.ndarray' = array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ]), n_samples: 'int' = 200, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

One-shot prior-informed RAPM over a possession frame -> per-player ratings.

Builds the sparse design matrix via
`~sportsdataverse.nba.nba_rapm.build_rapm_design`, constructs the
per-possession `prior_mean` vector from `prior`, fits a residualized
ridge with an RTO posterior via fit_prior_ridge`, and returns the
per-player offensive, defensive, and combined adj-RAPM ratings alongside
possession counts.

Sign convention (matches `~sportsdataverse.nba.nba_rapm.nba_rapm`):
`d_adj_rapm` is positive for a good defender (lowers opponent points);
`adj_rapm = o_adj_rapm + d_adj_rapm`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | A possession+lineup frame produced by the possession engine (`game_id`, `offense_team_id`, `points`, `off_player_1..5`, `def_player_1..5`). |
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. Players absent from `prior` receive a `(0.0, 0.0)` default. |
| `alphas` | `ndarray` | `array([   100.        ,    268.26957953,    719.685673  ,   1930.69772888,
         5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])` | RidgeCV alpha grid for the regularisation strength (default `DEFAULT_RAPM_ALPHAS`). |
| `n_samples` | `int` | `200` | Number of RTO posterior samples (default 200). |
| `seed` | `int` | `0` | RNG seed for the RTO sampler (default 0). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with columns `player_id` (Int64), `o_adj_rapm` (Float64), `d_adj_rapm` (Float64), `adj_rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

**Example**

```python
from sportsdataverse.nba import nba_adj_rapm
ratings = nba_adj_rapm(possessions, spm_prior_dict)
print(ratings.sort("adj_rapm", descending=True).head())
```

### nba_aging_curve {#nba_aging_curve}

`nba_aging_curve(*, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Load the bundled per-age value-multiplier curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `age:Int64, rel_value:Float64, peak_age:Float64` (`peak_age` repeated on every row for convenient filtering/joining).

| col_name | type | description |
|---|---|---|
| `age` | integer | Player age (in years). |
| `rel_value` | double | Value multiplier for this age relative to the peak age: a delta-method curve chaining minutes-weighted within-player consecutive-age changes in per-100-possession box-score value, quadratic-smoothed and min-max scaled to [0.4, 1.0], so the peak age is exactly 1.0 and the lowest-valued age 0.4. |
| `peak_age` | double | Age at which rel_value reaches its maximum of 1.0, repeated on every row for filtering and joining (29.0 in the bundled curve). |

**Example**

```python
from sportsdataverse.nba import nba_aging_curve
curve = nba_aging_curve()
print(curve.sort("rel_value", descending=True).head(1))

# Pipeline next step (one line)

curve.filter(pl.col("age").is_between(24, 30))
```

### nba_availability {#nba_availability}

`nba_availability(seasons: "'int | list[int]'", *, league: 'str' = 'nba', gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Project games-available % for a season (or seasons) from career GP history.

`avail_pct` is **availability, not skill** -- it is the only output of
this function and is never combined into a value/rating column by this
module (`sportsdataverse.nba.nba_rookie_projection.nba_rookie_projection`
reports it as a separate column too).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season (end year, e.g. `2020`) or list of seasons. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `gleague_bridge` | `bool` | `False` | When `True` (and `league != "gleague"`), also pulls each season's G-League (`league_id="20"`) bulk GP as a development-outcome bridge feature before scoring. Best-effort: gracefully absent (never raises) when the G-League bulk call returns no rows for a season; the returned schema is unaffected either way since the bridge column isn't part of the bundled artifact's scored features. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, season:Int64, avail_pct:Float64` (clipped to `[0, 1]`). Empty `seasons` -> zero-row schema.

**Example**

```python
from sportsdataverse.nba import nba_availability
proj = nba_availability(2019)
print(proj.sort("avail_pct").head())
```

### nba_box_logs {#nba_box_logs}

`nba_box_logs(season: 'str', *, league_id: 'str' = '00', season_type: 'str' = 'Regular Season', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'Dict[str, pl.DataFrame]'`

Fetch per-player and per-team game logs for a season (bulk, one call each).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season in `"2023-24"` form. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA). |
| `season_type` | `str` | `'Regular Season'` | SeasonType (`"Regular Season"`). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_leaguegamelog` replacement for offline tests. |

**Returns**

`{"player": <per-player-game logs>, "team": <per-team-game logs>}` as snake-cased polars frames.

**Example**

```python
from sportsdataverse.nba.nba_box_logs import nba_box_logs
logs = nba_box_logs("2023-24")
print(logs["player"].shape)
```

### nba_bpm {#nba_bpm}

`nba_bpm(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True, granularity: 'str' = 'season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Faithful BPM 2.0 per player, at season or single-game granularity.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | per-player-per-game box lines (`nba_box_logs`'s `player`); must carry `game_id` when `granularity="game"`. |
| `team_logs` | `DataFrame` |  | per-team-per-game lines incl. `plus_minus` (`nba_box_logs`'s `team`); must carry `game_id` when `granularity="game"`. |
| `positions` | `DataFrame` |  | listed positions (`nba_player_positions`): player_id, position_num. |
| `team_adjust` | `bool` | `True` | apply the team adjustment (True) or return raw box-BPM (False). |
| `granularity` | `str` | `'season'` | `"season"` (default) aggregates every row in `player_logs`/ `team_logs` into one row per player. `"game"` runs the exact same pipeline independently per `game_id` (position/role are estimated game-native, mirroring `NbaBpmModel`'s existing fold-native design) and returns one row per (game_id, player_id) with a leading `game_id` column; `gp` is always 1 in this mode. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`"season"`: frame with `player_id`, `obpm`, `dbpm`, `bpm`, `min`, `gp` (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min). `"game"`: the same columns prefixed with `game_id` (Utf8), one row per (game_id, player_id). Empty (that schema) input -> zero-row frame with the same schema; never raises on empty.

**Example**

```python
from sportsdataverse.nba import nba_bpm, nba_box_logs, nba_player_positions
logs = nba_box_logs("2023-24"); pos = nba_player_positions("2023-24")
bpm = nba_bpm(logs["player"], logs["team"], pos)
print(bpm.sort("bpm", descending=True).head())

# Per-game BPM

bpm_game = nba_bpm(logs["player"], logs["team"], pos, granularity="game")
print(bpm_game.filter(pl.col("game_id") == "0022300001").sort("bpm", descending=True))

# Raw (no team adjustment)

bpm_raw = nba_bpm(logs["player"], logs["team"], pos, team_adjust=False)

# Pandas output

bpm_pd = nba_bpm(logs["player"], logs["team"], pos, return_as_pandas=True)
```

### nba_career_trajectory {#nba_career_trajectory}

`nba_career_trajectory(player_values: 'pl.DataFrame', *, league: 'str' = 'nba', return_as_pandas: 'bool' = False) -> "'pl.DataFrame | pd.DataFrame'"`

Age-adjust player-season values with the bundled aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_values` | `DataFrame` |  | Frame `player_id, age:Int64, value:Float64`. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`player_values` plus `age_adjusted_value` (`value / rel_value(age)`, peak-centered) and `proj_next_value` (`value * rel_value(age+1) / rel_value(age)`). Ages outside the bundled curve's range fall back to `rel_value = 1.0` (no adjustment). Empty input returns the zero-row schema.

**Example**

```python
import polars as pl
from sportsdataverse.nba import nba_career_trajectory
player_values = pl.DataFrame({"player_id": ["1"], "age": [24], "value": [10.0]})
nba_career_trajectory(player_values)
```

### nba_darko {#nba_darko}

`nba_darko(panel: 'pl.DataFrame', ages: 'pl.DataFrame', *, aging_curve: "'AgingCurve | None'" = None, process_var: "'float | None'" = None, obs_base: "'float | None'" = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Project each player's next-season rating via a per-player Kalman filter + aging curve.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `panel` | `DataFrame` |  | `player_id, season, rating` (+ optional `weight`) — a multi-season rating panel. |
| `ages` | `DataFrame` |  | `player_id, season, age` (from `nba_player_ages`). |
| `aging_curve` | `AgingCurve \| None` | `None` | an `AgingCurve`; fitted from `panel` if None. |
| `process_var` | `float \| None` | `None` | Kalman process variance `q`; MLE-fit from `panel` if None. |
| `obs_base` | `float \| None` | `None` | Kalman base observation variance; MLE-fit from `panel` if None. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`player_id, last_season, forecast_season, filtered_skill, projected_rating, projected_sd`.

**Example**

```python
from sportsdataverse.nba import nba_darko, nba_player_ages
proj = nba_darko(rating_panel, ages_panel)
print(proj.sort("projected_rating", descending=True).head())
```

### nba_decay_rapm {#nba_decay_rapm}

`nba_decay_rapm(possessions: 'pl.DataFrame', *, asof: 'Optional[datetime.date]' = None, half_life_days: 'float' = 180.0, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Time-decay RAPM: ridge weighted by `0.5 ** (days_ago / half_life_days)`.

`asof=None` disables decay: every possession is weighted `1.0` and the
fit uses **exactly** plain `~sportsdataverse.nba.nba_rapm.nba_rapm`'s
own schedule (`alphas=DEFAULT_RAPM_ALPHAS`, sklearn's efficient default
LOOCV) so the two agree byte-for-byte (see
`test_decay_rapm_asof_none_equals_plain_rapm`). When `asof` is set,
possessions dated after `asof` are dropped, the remainder is
exponentially down-weighted by age, and the fit switches to the
**oracle** regularization schedule (`oracle_rapm_alphas` evaluated
at the post-filter possession count, `cv=` `ORACLE_RAPM_CV`) per
the binding WP2 ridge-schedule ruling documented in the module docstring.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Multi-season possession+lineup frame. Must carry a `game_date` (`pl.Date`) column when `asof` is not `None`. |
| `asof` | `Optional[date]` | `None` | Reference date; `None` -> unweighted, plain-RAPM-equivalent fit. |
| `half_life_days` | `float` | `180.0` | Weight half-life in days (default 180). |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `~sportsdataverse.nba.nba_rapm.DEFAULT_RAPM_ALPHAS` when `asof is None` or `oracle_rapm_alphas` (evaluated at the possession count) when `asof` is set. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `DECAY_RAPM_SCHEMA`. Empty input, or an `asof` that drops every possession, -> zero-row frame.

**Example**

```python
import datetime
from sportsdataverse.nba.nba_rapm_variants import nba_decay_rapm

df = nba_decay_rapm(season_poss, asof=datetime.date(2024, 3, 1), half_life_days=120.0)
print(df.sort("decay_rapm", descending=True).head())

# Plain-RAPM-equivalent (no decay)

df = nba_decay_rapm(season_poss)  # asof=None
```

### nba_draft_model {#nba_draft_model}

`nba_draft_model(draft_year: "'Union[int, list[int]]'", *, league: 'str' = 'nba', college_prior: "'Optional[pl.DataFrame]'" = None, gleague_bridge: 'bool' = False, return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Project prospect career value + draft probability from combine measurements.

Loads the draft-combine wrappers for `draft_year` (or each year in the
list), builds the shared combine-feature vector
(`sportsdataverse.nba.nba_draft_constants.build_combine_features`),
and applies the bundled ridge (`proj_career_value`) / logistic
(`draft_prob`) heads fit in `dev/nba_draft/fit_draft_model.py`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `draft_year` | `Union[int, list[int]]` |  | A draft year (e.g. `2019`) or list of years. |
| `league` | `str` | `'nba'` | `"nba"`, `"wnba"`, or `"gleague"` -- selects the bundled artifact and the combine-wrapper family. |
| `college_prior` | `Optional[DataFrame]` | `None` | Optional frame keyed on `player_id:Utf8` carrying the college-side MBB/WBB player-value spine's `projected_pick` / `box_bpm` / `archetype` (model ⑤, see design doc §3.5). When present and the bundled artifact has matching feature columns, it is left-joined as an extra feature block. This function **never** imports `sportsdataverse.mbb` -- callers pass the frame in. |
| `gleague_bridge` | `bool` | `False` | When `True`, left-joins G-League (`league_id="20"`) bulk production (`gleague_pts`/`gleague_gp`/`gleague_min`) for the draft year's season as extra, forward-looking feature columns. Not part of any bundled artifact's scored features today (joining it never changes `proj_career_value`/`draft_prob`); gracefully absent when the G-League bulk call returns no rows -- never raises. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Frame `player_id:Utf8, draft_year:Int64, proj_career_value:Float64, draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8` — one row per prospect with combine measurements for that class. `projected_pick` is a contiguous 1..N rank within each draft year. Empty/malformed input returns the zero-row schema, never raises.

**Example**

```python
from sportsdataverse.nba import nba_draft_model
board = nba_draft_model(2019)
print(board.sort("proj_career_value", descending=True).head())

# With a college-side prior

board = nba_draft_model(2019, college_prior=mbb_prior_df)

# Pipeline next step (one line)

board.filter(pl.col("pro_tier") == "lottery")
```

### nba_expected_turnovers {#nba_expected_turnovers}

`nba_expected_turnovers(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Expected TOV + residual ball-security skill from Synergy play-type mix.

`lg_to_rate_t` = poss-weighted league mean of `turnover_freq` for play
type `t`; `expected_tov = scale · Σ_t poss_t · lg_to_rate_t`;
`ball_security_skill = 100·(expected_tov − tov)/poss` (fewer turnovers
than expected ⇒ positive skill -- sign flipped vs. the foul-drawing model).

`scale = Σ actual tov / Σ raw type-mix estimate` is derived from the
fetched season itself (covers players below Synergy's per-type
classification threshold) so `Σ expected_tov ≡ Σ tov` holds exactly.
Swapping each player's own `turnover_freq` in for the league rate
reconstructs their real season TOV almost exactly (slope ~1.03, Spearman
~0.97 on the 2023-24 oracle corpus) -- confirming the column semantics are
correct. The *expected* (league-rate) version necessarily explains less
variance than *actual* (turnover-avoidance is a more individual,
less play-type-bound skill than foul-drawing), so its calibration slope
runs lower than model (3)'s -- see the oracle gate for the observed floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `tov`, `poss` (bypasses the live fetch). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `turnover_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`tov`/ `expected_tov`/`ball_security_skill` (Float64). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_expected_turnovers
t = nba_expected_turnovers("2023-24")
print(t.sort("ball_security_skill", descending=True).head(10))

# Injected offline (oracle / test) path

t = nba_expected_turnovers("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

t.filter(pl.col("poss") >= 200).sort("ball_security_skill", descending=True)
```

### nba_foul_drawing {#nba_foul_drawing}

`nba_foul_drawing(season: 'str', *, league_id: 'str' = '00', base: 'Optional[pl.DataFrame]' = None, advanced: 'Optional[pl.DataFrame]' = None, player_mix: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Expected FTA + residual foul-drawing skill from Synergy play-type mix.

`lg_ft_rate_t` = poss-weighted league mean of `ft_freq` for play type
`t` (`Σ_players(ft_freq_t·poss_t) / Σ_players(poss_t)`).
`expected_fta = scale · Σ_t poss_t · lg_ft_rate_t`;
`foul_draw_skill = 100·(fta − expected_fta)/poss`.

`ft_freq` (Synergy's `ft_poss_pct`) counts FT-drawing *trips*, not
individual free-throw attempts (a shooting foul is typically a 2-shot
trip) -- `scale = Σ actual fta / Σ raw trip-based estimate` is derived
from the fetched season itself (never hard-coded) so the trip-to-attempt
conversion self-normalizes and `Σ expected_fta ≡ Σ fta` holds exactly.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `base` | `Optional[DataFrame]` | `None` | Injected `nba_stats_leaguedashplayerstats` (`Base` measure) frame: `player_id`, `fta`, `poss` (bypasses the live fetch). |
| `advanced` | `Optional[DataFrame]` | `None` | Injected `Advanced`-measure frame with `player_id`, `pfd` (personal fouls drawn); optional -- `pfd` is `null` when omitted (`fta` is the always-present proxy). |
| `player_mix` | `Optional[DataFrame]` | `None` | Injected Synergy player-level offensive mix: `player_id`, `play_type`, `poss`, `ft_freq`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per player: `player_id` (Int64), `poss`/`fta`/ `expected_fta`/`foul_draw_skill` (Float64), `pfd` (Float64, null when *advanced* has no data for that player or is omitted). Zero-row frame with this schema when the inputs are empty (sparse-coverage leagues never raise).

**Example**

```python
from sportsdataverse.nba import nba_foul_drawing
f = nba_foul_drawing("2023-24")
print(f.sort("foul_draw_skill", descending=True).head(10))

# Injected offline (oracle / test) path

f = nba_foul_drawing("2023-24", base=base_df, player_mix=mix_df)

# Pipeline next step

f.filter(pl.col("poss") >= 200).sort("foul_draw_skill", descending=True)
```

### nba_four_factor_rapm {#nba_four_factor_rapm}

`nba_four_factor_rapm(possessions: 'pl.DataFrame', *, alphas: 'Optional[np.ndarray]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design.

Each factor is regressed on the identical offense/defense design matrix,
differing only in the per-possession response (FACTOR_RESPONSES`).
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

**Example**

```python
from sportsdataverse.nba.nba_game_predict import nba_in_game_win_prob
from sportsdataverse.nba.nba_loaders import load_nba_pbp
pbp = load_nba_pbp([2024]).filter(pl.col("game_id") == 401585828)
wp = nba_in_game_win_prob(pbp, 0.62)
```

### nba_l2m {#nba_l2m}

`nba_l2m(game_id: 'str | int', *, raw: 'bool' = False, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'dict[str, Any]'`

Fetch and parse an NBA Last Two Minute report from official.nba.com.

Retrieves the L2M report for a given game and returns parsed tables of calls,
game metadata, and error statistics. The game_id is zero-padded to 10 digits
(e.g., 42500405 becomes "0042500405"). A report is published for any game that
is within 3 points (5 points before the 2017-18 season) at any point during the
last two minutes of the fourth quarter or overtime -- not only playoff games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str \| int` |  | NBA game ID (can be int or str). Automatically zero-padded to 10 digits. |
| `raw` | `bool` | `False` | If True, return the raw JSON payload (dict) instead of parsed DataFrames. |
| `return_as_pandas` | `bool` | `False` | If True, return pandas DataFrames instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

If `raw=True`, the raw JSON dict. Otherwise, a dict with keys `"calls"`, `"game"`, `"stats"` mapping to DataFrames as documented in `parse_nba_l2m`.

| col_name | type | description |
|---|---|---|
| `calls.game_id` | character | 10-digit NBA game id (zero-padded), the join key back to the L2M game and stats tables. |
| `calls.period` | integer | Period number extracted from period_name's digits: 4 for the fourth quarter, 5+ for overtime periods (Q5 = OT1 through Q8 = OT4). |
| `calls.period_name` | character | Raw NBA.com period label for the graded play, e.g. Q4 for the fourth quarter or Q5 for the first overtime. |
| `calls.pc_time` | character | Raw game clock string from the L2M report before normalization, in MM:SS or MM:SS.t tenths-of-a-second format. |
| `calls.seconds_remaining` | double | Seconds remaining in the period at the graded play, parsed out of pc_time. |
| `calls.call_type` | character | Raw "Call: Type" label from the report, e.g. "Foul: Shooting" or "Turnover:  24 Second Violation"; whitespace (including doubled spaces) is NOT cleaned here -- the cleaned, upper-cased parts are call and type. |
| `calls.call` | character | Upper-cased category before the colon in call_type, e.g. FOUL, TURNOVER, or STOPPAGE. |
| `calls.type` | character | Upper-cased detail after the colon in call_type, e.g. SHOOTING or 24 SECOND VIOLATION. |
| `calls.committing` | character | Player, team nickname, or coach responsible for the graded action. |
| `calls.disadvantaged` | character | Player or team nickname disadvantaged by the graded action, blank when not applicable. |
| `calls.decision` | character | Normalized grading decision: CC (correct call), CNC (correct non-call), IC (incorrect call), or INC (incorrect non-call); null when the report marks the play undetectable at game speed. |
| `calls.decision_raw` | character | Raw grading code from the report before normalization, including the rare NCC/NCI aliases folded into decision. |
| `calls.comment` | character | Grader's free-text explanation of the ruling, sometimes naming a player as "Last (TEAM)". |
| `calls.difficulty` | character | Grader's difficulty rating for the play, e.g. Observable, Difficult, Blatant/Hard, or Undetectable. |
| `calls.video_event_id` | character | L2M video event id from the report (source field VideolLink, sic); not a stats.nba.com play-by-play EVENTNUM, so it cannot be joined to pbp data. |
| `calls.pos_id` | integer | Possession id assigned by the report, rising monotonically across the game; rows sharing pos_start/pos_end belong to the same possession. |
| `calls.pos_start` | character | Game clock at the start of the possession containing this graded play. |
| `calls.pos_end` | character | Game clock at the end of the possession containing this graded play. |
| `calls.pos_team_id` | integer | 10-digit NBA team id of the team in possession during the graded play. |
| `game.game_id` | character | 10-digit NBA game id (zero-padded), the join key to the calls and stats tables. |
| `game.game_date` | date | Game date parsed from the report's local tip-off timestamp (no timezone offset is applied). |
| `game.season_type` | character | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `game.home_team_id` | integer | 10-digit NBA team id (1610612xxx) of the home team. |
| `game.away_team_id` | integer | 10-digit NBA team id (1610612xxx) of the away team. |
| `game.home_team_abbr` | character | Three-letter abbreviation of the home team. |
| `game.away_team_abbr` | character | Three-letter abbreviation of the away team. |
| `game.home_team_name` | character | Home team nickname as published in the report, e.g. Thunder rather than Oklahoma City Thunder. |
| `game.away_team_name` | character | Away team nickname as published in the report. |
| `game.home_score` | integer | Home team's final score for the game. |
| `game.away_score` | integer | Away team's final score for the game. |
| `game.l2m_comments` | character | Report-level note from the league, e.g. a technical-issue disclaimer about missing video; null for almost every game. |
| `stats.game_id` | character | 10-digit NBA game id (zero-padded), the join key to the calls and game tables. |
| `stats.stat_name` | character | Name of the report-level error-rate statistic: Calls, Errors in Favor, or Possessions in Favor. |
| `stats.home` | integer | Value of stat_name attributed to the home team. |
| `stats.away` | integer | Value of stat_name attributed to the away team. |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m
result = nba_l2m("0042500405")
calls = result["calls"]
print(f"Game had {calls.height} tracked plays in the L2M window")

# Parse as pandas instead

result = nba_l2m("0042500405", return_as_pandas=True)
calls_pd = result["calls"]

# Access raw JSON

payload = nba_l2m("0042500405", raw=True)
print(payload["game"])
```

### nba_l2m_games {#nba_l2m_games}

`nba_l2m_games(season: 'int | str', *, return_as_pandas: 'bool' = False, proxy: 'dict | None' = None) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Fetch the list of games with Last Two Minute reports for an NBA season.

Retrieves and parses the L2M season index page from official.nba.com,
returning a table of all games for which L2M reports exist. JSON reports
exist only from 2019-01-01 onward; earlier seasons' index pages list PDFs,
which this function ignores. A release loader for historical (PDF-era)
reports is planned but does not exist yet.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int \| str` |  | The NBA season (end year) as a 4-digit int or string, e.g. 2026 for the 2025-26 season. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame instead of polars. |
| `proxy` | `dict \| None` | `None` | Optional proxy dict passed through to the HTTP layer. |

**Returns**

A `polars.DataFrame` (or `pandas.DataFrame` when `return_as_pandas=True`) with schema `{"game_id": Utf8, "season": Int32, "season_type": Utf8, "label": Utf8}`, one row per unique game ID in page order.

| col_name | type | description |
|---|---|---|
| `game_id` | character | 10-digit NBA game id (zero-padded), parsed from the season listing page's report link. |
| `season` | integer | NBA season end year passed to the function (e.g. 2026 for the 2025-26 season), stamped onto every row. |
| `season_type` | character | Season type inferred from the third digit of game_id: preseason, regular, all-star, playoffs, play-in, or nba-cup-final. |
| `label` | character | Matchup label text scraped from the listing page link, e.g. "Thunder 125, Rockets 124 (2OT)". |

**Example**

```python
from sportsdataverse.nba.nba_officiating import nba_l2m_games
df = nba_l2m_games(2026)
print(f"Season had {df.height} games with L2M reports")

# Get playoff games only

df = nba_l2m_games(2026)
playoffs = df.filter(df["season_type"] == "playoffs")

# Convert to pandas

df = nba_l2m_games(2026, return_as_pandas=True)
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
| `possessions` | `DataFrame` |  | Possession+lineup frame with team-level `fg2m` and the ten lineup columns; join keys `game_id` + `possession_number` + `offense_team_id` (the last is required by `luck_adjusted_response`'s defense-shooter leak filter). Must also carry a `points` column even though the LA response (`la_points`) supersedes it for fitting -- `~sportsdataverse.nba.nba_rapm.build_rapm_design` (invoked internally via prepare`) requires it unconditionally. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame from `build_possession_shooting`. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override; `None` → shrink from `shooting`. |
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects `oracle_rapm_alphas` evaluated at the possession count -- the operative WP2 oracle schedule (`cv=` `ORACLE_RAPM_CV` always; there is no plain-schedule mode). |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `luck_adjusted_response` when `player_rates` is `None`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `LA_RAPM_SCHEMA`. Empty input → zero-row frame.

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import nba_la_rapm
df = nba_la_rapm(season_poss, season_shooting)
print(df.sort("la_rapm", descending=True).head())

# Planted-truth shooter rates (e.g. for testing)

df = nba_la_rapm(season_poss, season_shooting, {7: (0.4, 0.8)})
```
