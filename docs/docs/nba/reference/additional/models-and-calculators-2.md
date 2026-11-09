---
title: "NBA — additional Python functions — Models and calculators: nba_draft–render_report"
sidebar_label: "Models and calculators: nba_draft–render_report"
sidebar_position: 12
description: "NBA — additional Python functions — Models and calculators: nba_draft–render_report — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Models and calculators: nba_draft–render_report

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
| `impact` | `DataFrame` |  | A `player_impact` table: `season`, `season_type`, `off_poss`, `def_poss` and the 13 metrics of `IMPACT_RANKED`, one row per player, season and season type. |
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
| `adj_rapm` | Float64 | Total prior-informed RAPM per 100 possessions, exactly the sum of o_adj_rapm and d_adj_rapm. Releases fit with sportsdataverse-py #1137 or later fit the ridge with an unpenalized intercept; releases fit before it (whatever their build date) fit none, which leaned this total toward heavy-minute players. |
| `ospm` | Float64 | Offensive statistical plus-minus per 100 possessions: the player's per-100 box-score feature vector scored through ridge coefficients trained on that season's o_rapm target. |
| `dspm` | Float64 | Defensive statistical plus-minus per 100 possessions from the same box-score feature vector scored through coefficients trained on the d_rapm target. |
| `spm` | Float64 | Total statistical plus-minus per 100 possessions, exactly the sum of ospm and dspm. |
| `min` | Float64 | Minutes played. |
| `gp` | Int64 | Games played. |
| `obpm` | Float64 | Offensive box plus-minus per 100 possessions for this season and season type only, team-adjusted on its own so a team's minute-weighted obpm is its offensive rating minus the league's, divided by 5 (league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 put the whole team constant here (league mean about -2.9). |
| `dbpm` | Float64 | Defensive box plus-minus per 100 possessions for this season and season type only: bpm minus obpm, so it lands on the team's defensive rating relative to the league (league minute-weighted mean near 0). Releases fit before sportsdataverse-py #1148 left it unadjusted (league mean about +2.9, nearly every player positive). |
| `bpm` | Float64 | Box plus-minus (BPM 2.0) per 100 possessions for this season and season type only, not a career value: the box-score estimate of total impact. The team adjustment makes a team's minute-weighted bpm its net rating per 100 divided by 5 (the five %Min slots sum to the net rating). Releases fit before sportsdataverse-py #1148 used the full net rating, five times too large. |
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

One row per (season, team_id): `season, team_id, adj_off_rtg, adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg, raw_pace, games, rank, adj_net_z`, then `<m>_rank`, `<m>_pct` and `<m>_n` for each of the seven ratings (BB-D2, the football F5/F6 grammar of `sportsdataverse._common.league_context`). `_rank` is 1 for the best team in the season: the highest offensive and net ratings, the lowest `*_def_rtg`, the fastest pace. `_pct` is the Weibull percentile. Both are null for a team under half the season's most games, and for every team when fewer than five qualifying teams have the rating or they all share one value. On a full season the half-games rule drops the All-Star teams; with `as_of_date` the most games is taken before the cut, so an early cut can also null regular teams. `_n` counts the sample: possessions (rounded) for the ratings, games for the paces. Empty input returns that schema with zero rows.

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
| `adj_off_rtg_rank` | Float64 | Rank of adj_off_rtg, the opponent-adjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the highest value ranks 1, average ties. |
| `adj_def_rtg_rank` | Float64 | Rank of adj_def_rtg, the opponent-adjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the lowest value ranks 1 (fewer points allowed is better), average ties. |
| `adj_net_rtg_rank` | Float64 | Rank of adj_net_rtg, the opponent-adjusted net rating (adj_off_rtg minus adj_def_rtg), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the highest value ranks 1, average ties. |
| `adj_pace_rank` | Float64 | Rank of adj_pace, the opponent-adjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the fastest team ranks 1 (pace has no good direction), average ties. |
| `raw_off_rtg_rank` | Float64 | Rank of raw_off_rtg, the unadjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the highest value ranks 1, average ties. |
| `raw_def_rtg_rank` | Float64 | Rank of raw_def_rtg, the unadjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the lowest value ranks 1 (fewer points allowed is better), average ties. |
| `raw_pace_rank` | Float64 | Rank of raw_pace, the unadjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify; the fastest team ranks 1 (pace has no good direction), average ties. |
| `adj_off_rtg_pct` | Float64 | Percentile of adj_off_rtg, the opponent-adjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_def_rtg_pct` | Float64 | Percentile of adj_def_rtg, the opponent-adjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_net_rtg_pct` | Float64 | Percentile of adj_net_rtg, the opponent-adjusted net rating (adj_off_rtg minus adj_def_rtg), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_pace_pct` | Float64 | Percentile of adj_pace, the opponent-adjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `raw_off_rtg_pct` | Float64 | Percentile of raw_off_rtg, the unadjusted offensive rating (points scored per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `raw_def_rtg_pct` | Float64 | Percentile of raw_def_rtg, the unadjusted defensive rating (points allowed per 100 possessions), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `raw_pace_pct` | Float64 | Percentile of raw_pace, the unadjusted pace (possessions per game), among the season's qualified teams (at least half the most games any team played, which leaves out the one-game All-Star teams); null for a non-qualifier or when fewer than five teams qualify: 100 * (n + 1 - rank) / (n + 1) with n the ranked teams, so the best team sits near 100 and none is exactly 0 or 100. |
| `adj_off_rtg_n` | Int64 | Sample size of adj_off_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `adj_def_rtg_n` | Int64 | Sample size of adj_def_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `adj_net_rtg_n` | Int64 | Sample size of adj_net_rtg: the box-score possessions the rating is computed over on both ends (offense plus defense, twice the possession estimate), summed over the team's games and rounded. |
| `adj_pace_n` | Int64 | Sample size of adj_pace: the games the pace is computed over (equal to games). |
| `raw_off_rtg_n` | Int64 | Sample size of raw_off_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `raw_def_rtg_n` | Int64 | Sample size of raw_def_rtg: the box-score possessions the rating is computed over, summed over the team's games and rounded. |
| `raw_pace_n` | Int64 | Sample size of raw_pace: the games the pace is computed over (equal to games). |

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
