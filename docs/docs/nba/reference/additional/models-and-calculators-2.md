---
title: "NBA — additional Python functions — Models and calculators: luck_adjusted–nba_predict"
sidebar_label: "Models and calculators: luck_adjusted–nba_predict"
sidebar_position: 12
description: "NBA — additional Python functions — Models and calculators: luck_adjusted–nba_predict — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: luck_adjusted–nba_predict

### luck_adjusted_response {#luck_adjusted_response}

`luck_adjusted_response(possessions: 'pl.DataFrame', shooting: 'pl.DataFrame', player_rates: 'Optional[dict[int, tuple[float, float]]]' = None, *, fg3_k: 'float' = 100.0, ft_k: 'float' = 50.0) -> 'pl.DataFrame'`

Attach a per-possession `la_points` expected-points response.

**DECISION 2/4**: `la_points = 2*fg2m + 3*Σ_shooter fg3a·p̂3 + Σ_shooter fta·p̂ft`
(offense-only, "one_way"). 2-pt makes stay realized. `p̂` come from
`player_rates` when given, else `_shrunk_shooter_rates` on `shooting`.

**Defense-shooter exclusion (bugfix)**: `shooting`
(`~sportsdataverse.nba.nba_possessions.build_possession_shooting`)
deliberately retains defense-team shooters in a possession group — e.g. a
defensive technical free throw shooter — because it is a per-shooter
companion frame, not a team-attributed one (that's why it carries its own
`team_id` column). The expected-points sum is offense-only by
definition (**DECISION 2**), so *before* aggregating `exp_extra` this
function joins `possessions[["game_id", "possession_number",
"offense_team_id"]]` onto `shooting` and filters to
`team_id == offense_team_id`, dropping any defense-team shooter row.
Without this filter a defense tech-FT's `fta·p̂ft` term leaks into the
offense's `la_points`, inflating it (reproduced: 2.9 vs. the
offense-only-correct 2.0 for a single offense 2-pt make plus one defense
tech FT).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | Possession+lineup frame carrying team-level `fg2m` and the join keys `game_id` + `possession_number` + `offense_team_id`. |
| `shooting` | `DataFrame` |  | Per-(possession, shooter) frame (`build_possession_shooting`), which may include defense-team shooter rows (e.g. technical FTs) — filtered out here before aggregation. |
| `player_rates` | `Optional[dict[int, tuple[float, float]]]` | `None` | Optional `{player_id: (p3, pft)}` override (e.g. planted truth in tests); `None` → shrink from `shooting`. |
| `fg3_k` | `float` | `100.0` | 3-point shrinkage pseudo-count, forwarded to `_shrunk_shooter_rates` when `player_rates` is `None`. |
| `ft_k` | `float` | `50.0` | Free-throw shrinkage pseudo-count, forwarded to `_shrunk_shooter_rates` when `player_rates` is `None`. |

**Returns**

`possessions` with an added `la_points: Float64` column (same rows, same order). Empty `possessions` → returned unchanged with an empty `la_points` column.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `period` | Int64 | Period of the game (1-4 quarters; 5+ for OT). |
| `possession_number` | Int64 | Possession number. |
| `offense_team_id` | Int64 | Unique identifier for offense team. |
| `defense_team_id` | Int64 |  |
| `start_order_index` | Int64 |  |
| `end_order_index` | Int64 |  |
| `start_seconds_remaining` | Float64 |  |
| `end_seconds_remaining` | Float64 |  |
| `points` | Int64 | Points scored. |
| `is_second_chance` | Boolean |  |
| `number_in_period` | Int64 |  |
| `possession_start_type` | String |  |
| `count_as_possession` | Boolean |  |
| `fg2a` | Int64 |  |
| `fg2m` | Int64 |  |
| `fg3a` | Int64 | Three-point field goal attempts. |
| `fg3m` | Int64 | Three-point field goals made. |
| `fta` | Int64 | Free throw attempts. |
| `ftm` | Int64 | Free throws made. |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `tov` | Int64 | Turnovers. |
| `off_player_1` | Int64 |  |
| `off_player_2` | Int64 |  |
| `off_player_3` | Int64 |  |
| `off_player_4` | Int64 |  |
| `off_player_5` | Int64 |  |
| `def_player_1` | Int64 |  |
| `def_player_2` | Int64 |  |
| `def_player_3` | Int64 |  |
| `def_player_4` | Int64 |  |
| `def_player_5` | Int64 |  |
| `lineup_source` | String |  |
| `la_points` | Float64 |  |

**Example**

```python
from sportsdataverse.nba.nba_rapm_variants import luck_adjusted_response
out = luck_adjusted_response(possessions_df, shooting_df)
print(out["la_points"].mean())

# Planted-truth override for testing

out = luck_adjusted_response(possessions_df, shooting_df, {7: (0.4, 0.8)})
```

### nba_adj_rapm {#nba_adj_rapm}

`nba_adj_rapm(possessions: 'pl.DataFrame', prior: 'Dict[int, Tuple[float, float]]', *, league_id: 'str | None' = None, alpha: 'float | None' = None, alphas: 'np.ndarray | None' = None, prior_scale: 'float | None' = None, n_samples: 'int' = 200, seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

One-shot prior-informed RAPM over a possession frame -> per-player ratings.

Builds the sparse design matrix via
`~sportsdataverse.nba.nba_rapm.build_rapm_design`, constructs the
per-possession `prior_mean` vector from `prior`, fits a residualized
ridge with an RTO posterior via `_fit_prior_ridge`, and returns the
per-player offensive, defensive, and combined adj-RAPM ratings alongside
possession counts.

Sign convention (matches `~sportsdataverse.nba.nba_rapm.nba_rapm`):
`d_adj_rapm` is positive for a good defender (lowers opponent points);
`adj_rapm = o_adj_rapm + d_adj_rapm` exactly.

Centring: the ridge fits an unpenalized intercept (the league's points per possession), as
`~sportsdataverse.nba.nba_rapm.nba_rapm` does, so `o_adj_rapm` and `d_adj_rapm` are
each relative to an average player: their possession-weighted means sit near 0 (2025-26 NBA
+0.32 / +0.06, 2025 WNBA +0.45 / +0.11, against o_rapm's +0.41 / +0.11). Fit without one,
as every release fit before #1137 was, the penalty split that level between every offensive and defensive
coefficient: o / d read about +12 / -11 (2025-26 NBA +12.03 / -10.84), a thinly-used
player kept less of the offset (-8.9 on offense under 200 possessions), and so `adj_rapm`
itself leaned toward heavy-minute players.

Penalty and prior scale: one per league, `ADJ_RAPM_PENALTY` and
`ADJ_RAPM_PRIOR_SCALE` (fitted by `tools/fit_adj_rapm_penalty.py`; the constants'
comment carries the evidence), as `~sportsdataverse.nba.nba_rapm.nba_rapm` takes
`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY`. Before #1150 each fit took RidgeCV's
per-season pick at the unscaled prior, which collapsed the spread in seven published WNBA
seasons (2004: sd 0.64 against plain RAPM's 1.47) and held NBA 2025-26 at 0.77 of plain RAPM's.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | A possession+lineup frame produced by the possession engine (`game_id`, `offense_team_id`, `points`, `off_player_1..5`, `def_player_1..5`). |
| `prior` | `Dict[int, Tuple[float, float]]` |  | Per-player `{player_id: (o_prior, d_prior)}` in per-100 units. Players absent from `prior` receive a `(0.0, 0.0)` default. |
| `league_id` | `str \| None` | `None` | `"00"`, `"10"` or `"20"`: the league whose penalty and prior scale are used. `None` reads it from the `game_id` prefix (NBA without one). |
| `alpha` | `float \| None` | `None` | A penalty instead of the league's (positive; `ValueError` otherwise). |
| `alphas` | `ndarray \| None` | `None` | Deprecated: pick the penalty per fit by RidgeCV over this grid (the old rule, whose scale is not comparable across seasons). |
| `prior_scale` | `float \| None` | `None` | A multiplier on `prior` instead of the league's. |
| `n_samples` | `int` | `200` | Number of RTO posterior samples (default 200). |
| `seed` | `int` | `0` | RNG seed for the RTO sampler (default 0). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with columns `player_id` (Int64), `o_adj_rapm` (Float64), `d_adj_rapm` (Float64), `adj_rapm` (Float64), `off_poss` (Int64), `def_poss` (Int64).

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `o_adj_rapm` | Float64 |  |
| `d_adj_rapm` | Float64 |  |
| `adj_rapm` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

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
| `age` | Int64 | Player age (in years). |
| `rel_value` | Float64 | Value multiplier for this age relative to the peak age: a delta-method curve chaining minutes-weighted within-player consecutive-age changes in per-100-possession box-score value, quadratic-smoothed and min-max scaled to [0.4, 1.0], so the peak age is exactly 1.0 and the lowest-valued age 0.4. |
| `peak_age` | Float64 | Age at which rel_value reaches its maximum of 1.0, repeated on every row for filtering and joining (29.0 in the bundled curve). |

**Example**

```python
from sportsdataverse.nba import nba_aging_curve
curve = nba_aging_curve()
print(curve.sort("rel_value", descending=True).head(1))

# Pipeline next step (one line)

curve.filter(pl.col("age").is_between(24, 30))
```

### nba_bpm {#nba_bpm}

`nba_bpm(player_logs: 'pl.DataFrame', team_logs: 'pl.DataFrame', positions: 'pl.DataFrame', *, team_adjust: 'bool' = True, granularity: 'str' = 'season', league_ortg: 'float | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Faithful BPM 2.0 per player, at season or single-game granularity.

Team adjustment (Myers, "About Box Plus/Minus (BPM)", BPM 2.0, Basketball-Reference): a
team's %Min-weighted ratings (five slots) sum to its rating, so its minute-weighted mean
`bpm` is its net rating per 100 / 5, and its minute-weighted `obpm` is (its offensive
rating - the league's) / 5, a separate constant. `dbpm = bpm - obpm` then averages (the
league's offensive rating - the team's defensive rating) / 5, positive for a good defense, so
league minute-weighted obpm and dbpm each sit near 0 (2025-26
NBA -0.001 / -0.006, 2025 WNBA +0.005 / +0.013). Team ratings are raw per-100 box margins, not
the schedule-adjusted ones Basketball-Reference uses. A traded player's minutes count with
the first team he appears for in the logs. Before #1148 every player took `net - team mean`
on both bpm and obpm: a team-mean target of the full net rating, five times BPM 2.0's, and
all of the adjustment on offense, so dbpm stayed raw (2025-26 regular season, minute-weighted:
league obpm -2.94 / dbpm +2.90, 97% of players with dbpm > 0; Michael Porter
Jr. -8.97 / +2.26, now +2.22 / -0.95). On 2024-25 the fix lifts agreement with
Basketball-Reference's BPM from corr 0.708 / MAE 3.83 to 0.945 / 0.65 (500+ minutes).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_logs` | `DataFrame` |  | per-player-per-game box lines (`nba_box_logs`'s `player`); must carry `game_id` when `granularity="game"`. |
| `team_logs` | `DataFrame` |  | per-team-per-game lines incl. `plus_minus` (`nba_box_logs`'s `team`); must carry `game_id` when `granularity="game"`. |
| `positions` | `DataFrame` |  | listed positions (`nba_player_positions`): player_id, position_num. |
| `team_adjust` | `bool` | `True` | apply the team adjustment (True) or return raw box-BPM (False). |
| `granularity` | `str` | `'season'` | `"season"` (default) aggregates every row in `player_logs`/ `team_logs` into one row per player. `"game"` runs the exact same pipeline independently per `game_id` (position/role are estimated game-native, mirroring `NbaBpmModel`'s existing fold-native design) and returns one row per (game_id, player_id) with a leading `game_id` column; `gp` is always 1 in this mode. |
| `league_ortg` | `float \| None` | `None` | The league offensive rating (points per 100) the OBPM adjustment is relative to. `None` (default) takes it from `team_logs`: per `season_id` when that column is there (stats.nba.com's season_id also encodes the season type, so regular season and playoffs stay apart), else over all of `team_logs`, which must then be one season and season type. Pass it when `team_logs` is not a whole league, for example one game scored on its own. |
| `return_as_pandas` | `bool` | `False` | return pandas instead of polars. |

**Returns**

`"season"`: frame with `player_id`, `obpm`, `dbpm`, `bpm`, `min`, `gp` (Int64 player_id/gp, Float64 obpm/dbpm/bpm/min). `"game"`: the same columns prefixed with `game_id` (Utf8), one row per (game_id, player_id). Empty (that schema) input -> zero-row frame with the same schema; never raises on empty.

No returns table is published for this function: no capture: its positions input is nba_player_positions output, which reads stats.nba.com (HTTP 403 to the datacenter IP the docs are built on).

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `granularity` is not `"season"`/`"game"`, or if `granularity="game"` is requested but `player_logs` has no `game_id` column. |

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

No returns table is published for this function: no capture: its input is a user-built player-age panel (player_id, age, value) that no package function produces.

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

No returns table is published for this function: no capture: its input is a multi-season player rating panel with ages per season that no package function produces.

**Example**

```python
from sportsdataverse.nba import nba_darko, nba_player_ages
proj = nba_darko(rating_panel, ages_panel)
print(proj.sort("projected_rating", descending=True).head())
```

### nba_decay_rapm {#nba_decay_rapm}

`nba_decay_rapm(possessions: 'pl.DataFrame', *, asof: 'Optional[datetime.date]' = None, half_life_days: 'float' = 180.0, alphas: 'Optional[np.ndarray]' = None, league_id: 'Optional[str]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Time-decay RAPM: ridge weighted by `0.5 ** (days_ago / half_life_days)`.

`asof=None` disables decay: every possession is weighted `1.0` and the
fit uses **exactly** plain `~sportsdataverse.nba.nba_rapm.nba_rapm`'s
own penalty (`~sportsdataverse.nba.nba_rapm.rapm_penalty`, the league's
fixed value) so the two agree byte-for-byte (see
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
| `alphas` | `Optional[ndarray]` | `None` | Optional RidgeCV alpha grid override. `None` (default) auto-selects plain RAPM's league penalty (`~sportsdataverse.nba.nba_rapm.rapm_penalty`) when `asof is None` or `oracle_rapm_alphas` (evaluated at the possession count) when `asof` is set. |
| `league_id` | `Optional[str]` | `None` | `"00"` / `"10"` / `"20"`, the league whose plain-RAPM penalty the `asof=None` branch fits at; `None` reads it from the `game_id` prefix (`~sportsdataverse.nba.nba_rapm.rapm_penalty`). |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

Frame with `DECAY_RAPM_SCHEMA`. Empty input, or an `asof` that drops every possession, -> zero-row frame.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `o_decay_rapm` | Float64 |  |
| `d_decay_rapm` | Float64 |  |
| `decay_rapm` | Float64 |  |
| `off_poss` | Int64 |  |
| `def_poss` | Int64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `asof` is not `None` but *possessions* lacks a `game_date` column. |

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
(`draft_prob`) heads fit in `tools/models/nba_draft/fit_draft_model.py`.

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

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

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
(`sportsdataverse/nba/models/nba_in_game_wp.ubj` for NBA, `wnba_in_game_wp.ubj`
for WNBA): a shallow monotone-constrained xgboost booster fit by
`tools/models/nba_prediction/train_in_game_wp.py` (NBA on 2022, 2023 and 2025,
held out on 2026; WNBA on 2021-2024, held out on 2025). Within the final minute
the clock reaches the model only through `margin_per_sqrt_sec`, so at a fixed
lead the probability moves toward the leader as time runs out. This is the raw
model output: it never applies the game result. `nba_game_page.score_win_prob` sets the
last play of a finished game to the result, and that settle step is small now but
not always zero (the WNBA booster saturates near 0.99 for a lead of 5 or more).

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

One row per play: the `in_game_features` columns plus `home_win_prob`.

| col_name | type | description |
|---|---|---|
| `score_diff` | Float64 |  |
| `sec_left` | Float64 |  |
| `sqrt_sec_left` | Float64 |  |
| `pregame_logit` | Float64 |  |
| `home_has_ball` | Int64 |  |
| `margin_per_sqrt_sec` | Float64 | Score margin over the time left, score_diff / sqrt(sec_left + 1): a fixed lead grows as the clock runs down. The basketball in-game boosters read the final minute only through this column. |
| `sqrt_sec_left_60` | Float64 | Square root of the seconds left, floored at one minute (sqrt(max(sec_left, 60))): the clock the basketball in-game boosters read outside the final minute. |
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

### nba_player_impact_context {#nba_player_impact_context}

`nba_player_impact_context(impact: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Append `<m>_n`, `<m>_rank` and `<m>_pct` to a `player_impact` table.

Ranks are computed within `(season, season_type)` over the players whose `<m>_n` reaches
the metric's gate (module docstring); a playoff run rarely reaches the regular-season gates,
so most playoff ranks are null. A row's rank and percentile are also null when its metric is
null or non-finite, when its `season` or `season_type` is null, when fewer than
`~sportsdataverse._common.league_context.MIN_COHORT_PLAYERS` players qualify in the
group, or when every qualifying value in the group is equal. Nothing is dropped, and running
it on its own output gives the same frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `impact` | `DataFrame` |  | A `player_impact` table: `season`, `season_type`, `off_poss`, `def_poss` and the 16 metrics of `IMPACT_RANKED`, one row per player, season and season type. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA, which picks the sample gates. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

`impact` with `<m>_n` (Int64), then `<m>_rank` and `<m>_pct` (Float64) appended for every metric of `IMPACT_RANKED`, in that order.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `teams` | String | Nested list of member-team membership spans. |
| `season` | Int64 | Season year. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `o_rapm` | Float64 | Offensive regularized adjusted plus-minus per 100 possessions from the single-season ridge fit over possession-level lineup indicators, at the fixed NBA penalty described under rapm; positive means the player raised his team's scoring rate while on offense. |
| `d_rapm` | Float64 | Defensive regularized adjusted plus-minus per 100 possessions, negated from the raw points-allowed coefficient so a positive value marks a defender who suppresses opponent scoring; fit at the fixed NBA penalty described under rapm. |
| `rapm` | Float64 | Total regularized adjusted plus-minus per 100 possessions, exactly the sum of o_rapm and d_rapm. The ridge penalty is one fixed NBA value (2,683, sportsdataverse.nba.nba_rapm.RAPM_PENALTY) in every season and season type, so the scale compares across seasons (regular-season sd 1.53 to 1.99 per 100 over players with 400 possessions, 1997-2026; a shorter season, such as the 1999 lockout (1.53), shrinks harder, so part of a spread difference is sample size). Seasons published before November 2026 were fit at a per-season RidgeCV pick (1,931 or 5,179 in the regular season, up to 37,276 in the playoffs), and their spread jumps with it (sd 1.15 to 2.34): do not compare those values across seasons or with values fit at the fixed penalty. |
| `off_poss` | Int64 | Number of possessions the player was on the floor on offense, the count of design-matrix rows carrying his offensive indicator and therefore the offensive-side sample size behind o_rapm. |
| `def_poss` | Int64 | Number of possessions the player was on the floor on defense, the sample size behind d_rapm; it tracks off_poss almost exactly because substitutions rarely split an offense-defense pair. |
| `o_adj_rapm` | Float64 | Offensive prior-informed RAPM per 100 possessions: the ridge fit shrunk toward the player's box-score (SPM) prior. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025-26 +0.32). A player with no prior (a rookie, or no previous-season SPM) is shrunk toward 0 instead. Releases fit before #1137, whatever their build date, fit no intercept, so the league's points per possession leaked into every coefficient: o_adj_rapm averaged about +12.03 and d_adj_rapm -10.84 (2025-26), a thinly-used player kept less of that offset, and only their sum, adj_rapm, read on a common scale. |
| `d_adj_rapm` | Float64 | Defensive prior-informed RAPM per 100 possessions, positive for a defender who lowers opponent scoring. In releases fit with sportsdataverse-py #1137 or later it is relative to an average player, up to any offset the SPM prior carries (possession-weighted league mean near 0: 2025-26 +0.06). Releases fit before #1137, whatever their build date, averaged about -10.84 here (see o_adj_rapm). |
| `adj_rapm` | Float64 | Total prior-informed RAPM per 100 possessions, exactly the sum of o_adj_rapm and d_adj_rapm. Releases fit with sportsdataverse-py #1137 or later fit the ridge with an unpenalized intercept; releases fit before it (whatever their build date) fit none, which leaned this total toward heavy-minute players. Releases fit with #1150 or later also use one ridge penalty per league (NBA 3,728, WNBA 2,683) and the SPM prior doubled (nba_adj_rapm.ADJ_RAPM_PENALTY / ADJ_RAPM_PRIOR_SCALE); earlier ones took a per-season RidgeCV pick, whose spread collapsed in some seasons (WNBA 2004 sd 0.64 against plain RAPM's 1.47). |
| `ospm` | Float64 | Offensive statistical plus-minus per 100 possessions: the player's per-100 box-score feature vector scored through ridge coefficients trained on that season's o_rapm target. |
| `dspm` | Float64 | Defensive statistical plus-minus per 100 possessions from the same box-score feature vector scored through coefficients trained on the d_rapm target. |
| `spm` | Float64 | Total statistical plus-minus per 100 possessions, exactly the sum of ospm and dspm. |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |
| `obpm` | Float64 | Offensive box plus-minus per 100 possessions for this season and season type only, team-adjusted on its own so a team's minute-weighted obpm is its offensive rating minus the league's, divided by 5 (a traded player counted with his first team; league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 put the whole team adjustment here (regular-season league minute-weighted mean about -2.9). |
| `dbpm` | Float64 | Defensive box plus-minus per 100 possessions for this season and season type only: bpm minus obpm, so a team's minute-weighted dbpm is (the league's offensive rating minus its defensive rating) / 5, positive for a good defense (league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 left it unadjusted (regular-season league minute-weighted mean about +2.9, mostly positive). |
| `bpm` | Float64 | Box plus-minus (BPM 2.0) per 100 possessions for this season and season type only, not a career value: the box-score estimate of total impact. The team adjustment makes a team's minute-weighted bpm its net rating per 100 divided by 5 (the five %Min slots sum to the net rating; a traded player's minutes count with the first team he appears for in the logs). Releases fit before sportsdataverse-py #1148 targeted the full net rating, five times BPM 2.0's team mean. |
| `war` | Float64 | Wins above replacement, computed as (rapm minus a replacement level of -2.0 per 100) times total possessions divided by 100, divided by a points-per-win constant calibrated each season by regressing team wins on full-season point margin. |
| `boban_filtered_skill` | Float64 | BOBAN Kalman-filtered skill estimate at the end of the player's observed multi-season RAPM panel, i.e. his current-form rating after aging drift and possession-weighted observation noise. |
| `boban_projected_rating` | Float64 | One-season-ahead BOBAN forecast, the filtered skill plus the empirical aging-curve drift at the player's last observed age; both season_type rows of a player-season carry the same value because the projection is not playoff-specific. |
| `boban_projected_sd` | Float64 | Standard deviation of the one-season-ahead BOBAN forecast, the square root of the filtered state variance plus the Kalman process variance; it sits at roughly 10.19 for a player with only one season in the panel, whose diffuse prior variance was never updated. |
| `darko_filtered_skill` | Float64 | Deprecated alias of boban_filtered_skill, kept for one release; use that column. |
| `darko_projected_rating` | Float64 | Deprecated alias of boban_projected_rating, kept for one release; use that column. |
| `darko_projected_sd` | Float64 | Deprecated alias of boban_projected_sd, kept for one release; use that column. |
| `o_rapm_n` | Int64 | Possessions o_rapm is measured over: off_poss, his offensive possessions. o_rapm_rank and o_rapm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_rapm_n` | Int64 | Possessions d_rapm is measured over: def_poss, his defensive possessions. d_rapm_rank and d_rapm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `rapm_n` | Int64 | Possessions rapm is measured over: off_poss + def_poss, his possessions at both ends. rapm_rank and rapm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `o_adj_rapm_n` | Int64 | Possessions o_adj_rapm is measured over: off_poss, his offensive possessions. o_adj_rapm_rank and o_adj_rapm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `d_adj_rapm_n` | Int64 | Possessions d_adj_rapm is measured over: def_poss, his defensive possessions. d_adj_rapm_rank and d_adj_rapm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `adj_rapm_n` | Int64 | Possessions adj_rapm is measured over: off_poss + def_poss, his possessions at both ends. adj_rapm_rank and adj_rapm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `ospm_n` | Int64 | Possessions ospm is measured over: off_poss, his offensive possessions. ospm_rank and ospm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dspm_n` | Int64 | Possessions dspm is measured over: def_poss, his defensive possessions. dspm_rank and dspm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `spm_n` | Int64 | Possessions spm is measured over: off_poss + def_poss, his possessions at both ends. spm_rank and spm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `obpm_n` | Int64 | Possessions obpm is measured over: off_poss, his offensive possessions. obpm_rank and obpm_pct are set only when it reaches the league's gate (1,700 NBA, 610 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `dbpm_n` | Int64 | Possessions dbpm is measured over: def_poss, his defensive possessions. dbpm_rank and dbpm_pct are set only when it reaches the league's gate (1,600 NBA, 1,200 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `bpm_n` | Int64 | Possessions bpm is measured over: off_poss + def_poss, his possessions at both ends. bpm_rank and bpm_pct are set only when it reaches the league's gate (3,200 NBA, 1,600 WNBA), the sample at which a player's on-court rating of the same side is half signal (sportsdataverse._common.rate_gates.ON_OFF_MIN_N); a regularized or box rating is less noisy, so the gate is conservative. |
| `war_n` | Int64 | Possessions behind war: off_poss + def_poss. war is a count, not a rate, so its rank has no sample gate. |
| `boban_filtered_skill_n` | Int64 | Possessions behind boban_filtered_skill: off_poss + def_poss, this season's possessions at both ends. The boban filtered skill is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_filtered_skill_rank and boban_filtered_skill_pct are set only when this reaches the league's gate (3,200 NBA, 1,600 WNBA). Conservative, since the filter also carries earlier seasons. |
| `boban_projected_rating_n` | Int64 | Possessions behind boban_projected_rating: off_poss + def_poss, this season's possessions at both ends. The boban next-season projection is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_projected_rating_rank and boban_projected_rating_pct are set only when this reaches the league's gate (3,200 NBA, 1,600 WNBA). Conservative, since the filter also carries earlier seasons. |
| `boban_projected_sd_n` | Int64 | Possessions behind boban_projected_sd: off_poss + def_poss, this season's possessions at both ends. The boban projection's standard deviation is a multi-season Kalman estimate with no single-side sample, so it takes the net on-court gate, the closest existing one: boban_projected_sd_rank and boban_projected_sd_pct are set only when this reaches the league's gate (3,200 NBA, 1,600 WNBA). Conservative, since the filter also carries earlier seasons. |
| `o_rapm_rank` | Float64 | Rank of o_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_rapm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_rapm_rank` | Float64 | Rank of d_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_rapm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `rapm_rank` | Float64 | Rank of rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when rapm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `o_adj_rapm_rank` | Float64 | Rank of o_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when o_adj_rapm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `d_adj_rapm_rank` | Float64 | Rank of d_adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when d_adj_rapm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `adj_rapm_rank` | Float64 | Rank of adj_rapm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when adj_rapm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `ospm_rank` | Float64 | Rank of ospm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when ospm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dspm_rank` | Float64 | Rank of dspm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dspm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `spm_rank` | Float64 | Rank of spm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when spm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `obpm_rank` | Float64 | Rank of obpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when obpm_n is under the league's gate (1,700 NBA, 610 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `dbpm_rank` | Float64 | Rank of dbpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when dbpm_n is under the league's gate (1,600 NBA, 1,200 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `bpm_rank` | Float64 | Rank of bpm among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when bpm_n is under the league's gate (3,200 NBA, 1,600 WNBA), and null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. Regular-season gates: a playoff run rarely reaches one. |
| `war_rank` | Float64 | Rank of war among the season and season type's ranked players, 1 = best (higher is better), ties averaged; ungated (every player with a finite war qualifies), null when its season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value in the group is equal. |
| `boban_filtered_skill_rank` | Float64 | Rank of boban_filtered_skill among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when boban_filtered_skill_n is under the league's gate (3,200 NBA, 1,600 WNBA), when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `boban_projected_rating_rank` | Float64 | Rank of boban_projected_rating among the season and season type's ranked players, 1 = best (higher is better), ties averaged; null when boban_projected_rating_n is under the league's gate (3,200 NBA, 1,600 WNBA), when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `boban_projected_sd_rank` | Float64 | Rank of boban_projected_sd among the season and season type's ranked players, ASCENDING: 1 = the smallest sd, the most certain projection; ties averaged; null when boban_projected_sd_n is under the league's gate (3,200 NBA, 1,600 WNBA), when season or season_type is null, in a group of fewer than 10 ranked players, or when every ranked value is equal. |
| `o_rapm_pct` | Float64 | Percentile of o_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `d_rapm_pct` | Float64 | Percentile of d_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `rapm_pct` | Float64 | Percentile of rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `o_adj_rapm_pct` | Float64 | Percentile of o_adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `d_adj_rapm_pct` | Float64 | Percentile of d_adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `adj_rapm_pct` | Float64 | Percentile of adj_rapm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `ospm_pct` | Float64 | Percentile of ospm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `dspm_pct` | Float64 | Percentile of dspm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `spm_pct` | Float64 | Percentile of spm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `obpm_pct` | Float64 | Percentile of obpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `dbpm_pct` | Float64 | Percentile of dbpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `bpm_pct` | Float64 | Percentile of bpm_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `war_pct` | Float64 | Percentile of war_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_filtered_skill_pct` | Float64 | Percentile of boban_filtered_skill_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_projected_rating_pct` | Float64 | Percentile of boban_projected_rating_rank, 100 * (n + 1 - rank) / (n + 1) over the n ranked players of the season and season type; null where the rank is. |
| `boban_projected_sd_pct` | Float64 | Percentile of boban_projected_sd_rank, 100 * (n + 1 - rank) / (n + 1): high means a more certain projection (a smaller sd), not a larger one; null where the rank is. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |
| `KeyError` | If a metric, `season`, `season_type`, `off_poss` or `def_poss` is missing. |

**Example**

```python
from sportsdataverse.nba import load_nba_player_impact, nba_player_impact_context
ranked = nba_player_impact_context(load_nba_player_impact(2026))
ranked.sort("adj_rapm_rank").select("player_name", "adj_rapm", "adj_rapm_rank", "adj_rapm_pct").head()
```

### nba_player_impact_league_averages {#nba_player_impact_league_averages}

`nba_player_impact_league_averages(impact: 'pl.DataFrame', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

The league-average rows of a `player_impact` table, one block per season type.

The football F1 rows of `~sportsdataverse._common.league_context.league_averages` over
the players who pass each metric's sample gate, so `n` is that metric's qualified cohort:
the players with a finite value and enough possessions. It is reported even where
`nba_player_impact_context` leaves the ranks null (fewer than 10 qualifiers, or all
equal). `category` is `player_impact_` plus the `season_type` lowercased with spaces as
underscores: `player_impact_regular_season` and `player_impact_playoffs` for the releases'
two season types. `qualifier_min` is null (the gates differ by metric).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `impact` | `DataFrame` |  | A `player_impact` table, as `nba_player_impact_context` takes. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA: the `level` label and the sample gates. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per `(season, season type, metric)` in `~sportsdataverse._common.league_context.LEAGUE_AVERAGES_SCHEMA`; an empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the league-average row, as the END year the player_impact table stamps (2026 = 2025-26). |
| `level` | String | Population the row describes: nba or wnba (the league_id passed). |
| `entity` | String | Kind of row summarized: always player for the impact table. |
| `category` | String | Table and season type the row summarizes: player_impact_regular_season or player_impact_playoffs. |
| `metric` | String | Impact metric the row summarizes, one of the 13 ranked columns (o_rapm through war). |
| `mean` | Float64 | Unweighted mean of the metric over the season type's ranked players, those whose <metric>_n reaches its sample gate. |
| `median` | Float64 | Median of the metric over the same ranked players as mean. |
| `sd` | Float64 | Sample standard deviation (ddof 1) of the metric over the same ranked players; null with one player. |
| `n` | Int64 | Number of ranked players with a finite value: the n of the <metric>_pct formula, not the possession count <metric>_n. |
| `qualifier_min` | Float64 | Always null here: each metric has its own possession gate rather than one table-wide threshold. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |
| `KeyError` | If a metric, `season`, `off_poss` or `def_poss` is missing. |

**Example**

```python
from sportsdataverse.nba import load_nba_player_impact, nba_player_impact_league_averages
nba_player_impact_league_averages(load_nba_player_impact(2026))
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
