---
title: "CFB — additional Python functions — Models and calculators: add_era–special_teams"
sidebar_label: "Models and calculators: add_era–special_teams"
sidebar_position: 4
description: "CFB — additional Python functions — Models and calculators: add_era–special_teams — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Models and calculators: add_era–special_teams

### add_era_columns {#add_era_columns}

`add_era_columns(df: 'pl.DataFrame', model: 'str', season: 'int | None' = None) -> 'pl.DataFrame'`

Add the era column(s) `model` consumes, using ITS card's cuts.

The cuts are read from the published contract, never restated here. That is
the fix for cfbfastR-cfb-data#70, where both consumers kept a private copy of
the era boundary, both drifted to a 2017 cut the trainer never used, and
2018-2020 scored an era off the models trained with them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame carrying a `season` column, or any frame when `season` is given explicitly. |
| `model` | `str` |  | Bundle stem, used to look up the era contract. |
| `season` | `int \| None` | `None` | Season to use when `df` has no `season` column -- the hand-built-row case. |

**Returns**

`df` with the contract's columns added. Returned unchanged when the model declares no era contract, or when the columns are already present.

**Example**

```python
from sportsdataverse.cfb.model_calculators import add_era_columns
add_era_columns(pl.DataFrame({"season": [2018]}), "xpass_model")
```

### assert_rating_scale {#assert_rating_scale}

`assert_rating_scale(ratings: 'pl.DataFrame', *, era: 'str' = 'modern', tol: 'float' = 1.6) -> 'float'`

Warn if the ratings have drifted off the scale the constants were fit on.

THE FAILURE THIS PREVENTS. `net_points_scale` is a frozen statement about
a relationship between two things: rating units and points. When the
ratings change -- a different ridge penalty, a rescale, a rebuilt corpus --
the constant silently becomes wrong while every function keeps returning
plausible numbers. That is exactly what happened: the shipped 44.5367 was
fit on 2026-07-28, the ridge lambda moved on 08-01, the corpus was rebuilt
on 08-02, and nothing failed. Measured out-of-sample the result was a
calibration slope of 0.55 -- predictions stretched nearly 2x wider than
reality -- for two days, undetected.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | Team ratings frame carrying an `adj_net` column, as returned by `cfb_ratings.efficiency_ratings`. Frames without that column, or with fewer than 30 rows, are too thin to judge and return `1.0` unchecked. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`, used only to name the era in the warning text. |
| `tol` | `float` | `1.6` | Fold-change tolerance. The check fires outside `[1/tol, tol]`. |

**Returns**

The observed/fitted sd ratio. `1.0` when the frame is too thin to judge, so a caller can treat "1.0" as "no evidence of drift" either way.

**Example**

```python
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_game_predict import assert_rating_scale
ratings = cfb_ratings.efficiency_ratings(2024)
ratio = assert_rating_scale(ratings)

# Treat a large drift as a refit signal, not a nuisance warning

assert ratio < 1.6, "refit the constants before trusting predictions"
```

### calculate_completion_probability {#calculate_completion_probability}

`calculate_completion_probability(df, *, season=None, return_as_pandas=False)`

Completion probability for each pass attempt.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `score_diff`, `seconds_remaining`, `is_home`, `period`, `passing_down`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `cp` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_completion_probability
calculate_completion_probability(df, season=2024)
```

### calculate_epa {#calculate_epa}

`calculate_epa(df, *, season=None, return_as_pandas=False)`

Expected points added: the change in EP across a play.

Recomputes `ep` when it is absent, matching nflfastR's behaviour. Requires
`ep_end` -- the expected points after the play -- because EPA is a
difference and this function scores rows, not sequences.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame with the EP features and an `ep_end` column. |
| `season` |  | `None` | Unused by the EP model; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `ep` (if it was absent) and `epa` appended.

**Example**

```python
from sportsdataverse.cfb import calculate_epa
calculate_epa(pbp)
```

### calculate_expected_points {#calculate_expected_points}

`calculate_expected_points(df, *, season=None, return_as_pandas=False)`

Expected points for each row.

Mirrors `sportsdataverse.nfl.calculate_expected_points()`. The EP booster
is `multi:softprob` over seven next-score classes; this collapses those
probabilities to a points expectation using the package's own
`ep_class_to_score_mapping` rather than restating the class order.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `TimeSecsRem`, `yards_to_goal`, `distance`, `down_1` through `down_4` and `pos_score_diff_start`. |
| `season` |  | `None` | Unused by this model (EP consumes no era feature); accepted so every calculator shares one signature. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with the seven class probability columns and an `ep` column appended.

**Example**

```python
from sportsdataverse.cfb import calculate_expected_points
calculate_expected_points(pbp)
```

### calculate_field_goal_probability {#calculate_field_goal_probability}

`calculate_field_goal_probability(df, *, season=None, return_as_pandas=False)`

Field-goal make probability for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `yards_to_goal`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with an `fg_make_prob` column appended. Named `fg_make_prob`, not `fg_prob`: `calculate_expected_points` emits `fg_prob` for the probability the NEXT SCORE is a field goal, which is a different quantity. Sharing the name made chaining the two silently lossy. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_field_goal_probability
calculate_field_goal_probability(df, season=2024)
```

### calculate_fourth_down {#calculate_fourth_down}

`calculate_fourth_down(df, *, season=None, return_as_pandas=False)`

Fourth-down conversion model output for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `posteam_total`, `posteam_spread`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `fd_conversion_prob` (probability the gain reaches `distance`) and `fd_expected_yards` appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_fourth_down
calculate_fourth_down(df, season=2024)
```

### calculate_qbr {#calculate_qbr}

`calculate_qbr(df, *, season=None, return_as_pandas=False)`

Model QBR for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `qbr_epa`, `sack_epa`, `pass_epa`, `rush_epa`, `pen_epa`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `qbr` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_qbr
calculate_qbr(df, season=2024)
```

### calculate_two_point_probability {#calculate_two_point_probability}

`calculate_two_point_probability(df, *, season=None, return_as_pandas=False)`

Two-point conversion success probability.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `posteam_spread`, `posteam_total`, `pos_score_diff`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `two_pt_prob` column appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_two_point_probability
calculate_two_point_probability(df, season=2024)
```

### calculate_win_probability {#calculate_win_probability}

`calculate_win_probability(df, *, season=None, return_as_pandas=False)`

Win probability for each row.

Selects the booster the way the pipeline does: `wp_spread` when the frame
carries a `spread_time` column, `wp_naive` otherwise. The naive model is
the spread model minus that single feature, so which one applies is decided
by whether the caller has spread information at all.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying the win-probability features. Include `spread_time` to use the spread model. |
| `season` |  | `None` | Unused by these models; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with a `wp` column appended.

**Example**

```python
from sportsdataverse.cfb import calculate_win_probability
calculate_win_probability(pbp)
```

### calculate_wpa {#calculate_wpa}

`calculate_wpa(df, *, season=None, return_as_pandas=False)`

Win probability added: the change in WP across a play.

Recomputes `wp` when it is absent. Requires `wp_end` for the same reason
`calculate_epa` requires `ep_end`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame with the WP features and a `wp_end` column. |
| `season` |  | `None` | Unused by these models; accepted for signature consistency. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with `wp` (if it was absent) and `wpa` appended.

**Example**

```python
from sportsdataverse.cfb import calculate_wpa
calculate_wpa(pbp)
```

### calculate_xpass {#calculate_xpass}

`calculate_xpass(df, *, season=None, return_as_pandas=False)`

Expected pass probability for each row.

Mirrors the shape of `sportsdataverse.nfl`'s calculators. Rows may come
from a play-by-play frame or be typed by hand to ask a hypothetical; only
the model card's declared columns are required, and extra columns pass
through untouched.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` |  |  | Frame carrying `down`, `distance`, `yards_to_goal`, `pos_score_diff`, `TimeSecsRem`, `period`, plus either a `season` column or the `season` argument when the model consumes an era feature. |
| `season` |  | `None` | Season used to derive era columns when `df` has none. |
| `return_as_pandas` |  | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

`df` with an `xpass` column (probability the play is a pass) appended. Input columns are preserved, so chaining two calculators is lossless.

**Example**

```python
from sportsdataverse.cfb import calculate_xpass
calculate_xpass(df, season=2024)
```

### cfb_adjusted_epa {#cfb_adjusted_epa}

`cfb_adjusted_epa(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season opponent-adjusted per-team EPA from a season's play-by-play.

Fits one ridge of per-play `EPA` on offense-team, defense-team, and
home-field indicators (every team shrunk toward the league average by its own
play count) over the `0.05 <= wp_before_naive <= 0.95` pass
and rush plays, nets each team's per-game raw EPA against the opponent's
fitted strength, and averages to a season figure. In-sample/descriptive (the
fit uses the whole season); for leak-free per-game values use
`cfb_adjusted_epa_by_game`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the columns listed in the module docstring. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (~7% at a full season, most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (`0.1 <= wp_before <= 0.9` band, standardized ridge with the first team id as the reference level, lambda 0.035). pre598 reads `wp_before` instead of `wp_before_naive`. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per team (>= 2 valid games): `team_id`, `pos_team`, `valid_games`, `adj_off_epa`, `adj_def_epa`, `off_strength_faced`, `def_strength_faced`, `net_adj_epa` and their `*_rank` columns.

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
cfb.cfb_adjusted_epa(pbp).sort("net_adj_epa_rank").head()

# NFL team summaries (the pre-#598 method; reads wp_before)

cfb.cfb_adjusted_epa(nfl_plays, method="pre598")
```

### cfb_adjusted_epa_by_game {#cfb_adjusted_epa_by_game}

`cfb_adjusted_epa_by_game(plays: 'pl.DataFrame | pd.DataFrame', *, ridge_lambda: 'float | None' = None, method: "Literal['current', 'pre598']" = 'current', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game.

For each week `w` the opponent-strength ridge is fit on FIT_WP`-band plays
from **weeks before `w` only**, then that week's games are adjusted with
those as-of strengths -- so the value uses no future information and is valid
as an in-season power-rating / model feature. Week 1 (no prior) yields null
adjustments; not-yet-seen opponents fall back to the league baseline (an
average team), and teams seen on few plays are shrunk most of the way there.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame \| DataFrame` |  | A cfbfastR-schema play-by-play frame (polars or pandas) with the module-docstring columns **plus** `week`. One season at a time. |
| `ridge_lambda` | `float \| None` | `None` | Ridge penalty. Under `method="current"` it is per play of a full team season: each team keeps `n / (n + ridge_lambda * 577)` of its own signal for its `n` fit plays and is shrunk toward the league average by the rest (~7% at a full season, most of it on a handful of plays); must be > 0. Under `method="pre598"` it is passed unscaled to the old standardized ridge (the per-observation penalty; no 577 scaling, no positivity check). `None` (default) means 0.075 for `"current"` (the owner's choice, ADJ_EPA_LAMBDA`) and 0.035 for `"pre598"`. |
| `method` | `Literal['current', 'pre598']` | `'current'` | `"current"` (default) or `"pre598"`, the fit this function used before #598 (see `cfb_adjusted_epa`). pre598 also keeps the old week order: it sorts by `week` alone and does not read `seasonType`, so postseason games that restart at week 1 are fit with (and leak into) the regular season, exactly as before. It exists for nfl-data's NFL team summaries, is not validated for NFL either, and is kept only for continuity until NFL is validated. |
| `return_as_pandas` | `bool` | `False` | Return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (game, team), sorted by `week` then `team_id`: `game_id`, `week`, `team_id`, `opponent_id`, `pos_team`, `raw_off_epa`, `adj_off_epa`, `raw_def_epa`, `adj_def_epa`, `off_strength_faced` (opponent offense), `def_strength_faced` (opponent defense), `net_adj_epa`. The `adj_*` / `net` columns are null for week 1 (and any week with no prior fit).

**Example**

```python
import sportsdataverse.cfb as cfb
pbp = cfb.load_cfb_pbp(seasons=[2023])
tg = cfb.cfb_adjusted_epa_by_game(pbp)
tg.filter(pl.col("week") >= 5).sort("net_adj_epa", descending=True).head()
```

### cfb_compute_results {#cfb_compute_results}

`cfb_compute_results(teams: 'pl.DataFrame', games: 'pl.DataFrame', week_num: 'int', *, rng: 'Optional[np.random.Generator]' = None, elo: 'Optional[Dict[str, float]]' = None, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame]'`

Default results generator — nflseedR's dynamic ELO model for CFB.

Fills `result` for week `week_num` games that are still unplayed and
updates each team's ELO rating from that week's results (real results
included). Constants are nflseedR's `nflseedR_compute_results` exactly,
minus the NFL rest-day adjustment (CFB plays weekly — documented
simplification).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `teams` | `DataFrame` |  | Per-sim team table (`sim`, `team`, `conference`, optionally `elo` carried over from the previous week). |
| `games` | `DataFrame` |  | Per-sim games table (engine schema; see `sportsdataverse.cfb.cfb_standings`). |
| `week_num` | `int` |  | The week to fill. |
| `rng` | `Optional[Generator]` | `None` | numpy Generator (seeded by `cfb_simulations`). A fresh default generator is created when omitted. |
| `elo` | `Optional[Dict[str, float]]` | `None` | Optional initial ratings `{team: elo}` applied to every sim. Teams missing from the dict start at 1500. When neither `elo` nor a `teams.elo` column exists, ratings initialize randomly at `N(1500, 150)` per (sim, team) — nflseedR behavior. |

**Returns**

`{"teams": ..., "games": ...}` — updated frames, mirroring nflseedR's returned list.

| col_name | type | description |
|---|---|---|
| `sim` | integer | Simulation identifier the game row belongs to (1..n simulated seasons; ELO ratings never mix across simulations). |
| `week` | integer | Week of the season the game is played in; only games matching the requested week_num are filled. |
| `game_type` | character | Game classification in the seedr engine schema - REG (regular season), CONF_CHAMP (conference championship) or POST (postseason/CFP). |
| `home_team` | character | Team name of the home team in the simulated game (returned games frame). |
| `away_team` | character | Team name of the away team in the simulated game (returned games frame). |
| `result` | double | Home-team margin of victory (home score minus away score) - real results are preserved and the target week's unplayed games are filled from the ELO model. |
| `neutral` | integer | Neutral-site flag (1 = neutral site, 0 = true home game; only non-neutral games receive the ELO home bump). |

**Example**

```python
from sportsdataverse.cfb.cfb_simulations import cfb_compute_results
out = cfb_compute_results(teams, games, 5, rng=rng)
teams, games = out["teams"], out["games"]
```

### cfb_draft_projection {#cfb_draft_projection}

`cfb_draft_projection(target_draft_year: 'int', *, division: 'str' = 'fbs', history_years: 'list[int] | None' = None, l2: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame]'`

Project NFL-draft probability per player + expected picks per team.

Fits an L2 logistic of `drafted` on `[recruit_stars, talent_points,
career_production_z, class_year]` over draft years strictly before the
target (the as-of boundary, enforced internally), then scores the target
year's eligible players.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_draft_year` | `int` |  | Draft year to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_years` | `list[int] \| None` | `None` | Training draft years (default: the five before target). |
| `l2` | `float` | `1.0` | Logistic L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, both frames return as pandas. |

**Returns**

`{"players": ..., "teams": ...}` — players: `draft_year` (Int64), `team_id` / `player_id` / `player_name` (Utf8), `draft_prob` (Float64); teams: `draft_year`, `team_id`, `proj_draft_picks` (Float64, the sum of member draft probabilities). Zero-row (typed) frames when no data is available.

**Example**

```python
from sportsdataverse.cfb import cfb_draft_projection
out = cfb_draft_projection(2024)
out["teams"].sort("proj_draft_picks", descending=True).head(10)
```

### cfb_field_position {#cfb_field_position}

`cfb_field_position(seasons: 'Union[int, list[int]]', *, exclude_garbage: 'bool' = True, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Team-season field-position value: avg start, drive EP, margin, pts/drive.

Derives one row per drive from `load_cfb_pbp`, values each starting
yard line with the bundled EP curve, and aggregates per (season, team):
`avg_start_yardline` (yards from own goal, higher = better),
`fp_ep` (mean drive-start EP), `fp_margin` (own `fp_ep` minus the
mean drive-start EP of opponents' drives faced), and
`points_per_drive` (mean realized offensive points: TD=7, FG=3;
non-offensive negative results such as safeties and defensive return
TDs are floored to 0 before averaging).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | season or list of seasons (hosted pbp covers 2002-2021). |
| `exclude_garbage` | `bool` | `True` | drop drives that start in Connelly garbage time. |
| `return_as_pandas` | `bool` | `False` | return a pandas `DataFrame` instead of polars. |

**Returns**

One row per (season, team_id); zero-row frame with the documented schema on empty input.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the field-position stats cover. |
| `team_id` | character | Team ESPN id (character join key). |
| `drives` | integer | Offensive drives counted (garbage-time drives excluded by default). |
| `avg_start_yardline` | double | Mean drive-start yard line from the team's own goal (higher = better field position). |
| `fp_ep` | double | Mean bundled expected points of the team's drive starts. |
| `fp_margin` | double | Own fp_ep minus the mean drive-start EP of opponents' drives faced. |
| `points_per_drive` | double | Mean realized offensive points per drive (TD=7, FG=3). |

**Example**

```python
from sportsdataverse.cfb import cfb_field_position
df = cfb_field_position([2021])
print(df.shape)

# Pipeline next step (one line)

df.sort("fp_margin", descending=True).head()
```

### cfb_predict_games {#cfb_predict_games}

`cfb_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Predict a whole schedule of games from a ratings frame (vectorized).

Applies the three closed-form predictors across every row of `games` in
one pass. `ratings` is joined twice -- once on `home_team_id` and once on
`away_team_id` -- so each game carries both teams' `adj_net` / `adj_off_epa`
/ `adj_def_epa` / `off_pace`. The totals model's `game_pace` factor is
computed here as `home_off_pace * away_off_pace / league_avg_pace`, where the
league average is the mean `off_pace` of the passed ratings frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | Schedule frame with `game_id`, `home_team_id`, `away_team_id`, and `neutral_site` columns. The two team-id columns must share the dtype of `ratings["team_id"]` (asserted before the join). |
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id`, `adj_net`, `adj_off_epa`, `adj_def_epa`, and `off_pace`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per game with `game_id`, `home_team_id`, `away_team_id`, `neutral_site`, `exp_margin`, `home_win_prob`, `exp_total`.

| col_name | type | description |
|---|---|---|
| `game_id` | integer | Game identifier carried through from the input schedule. |
| `home_team_id` | character | Home team ESPN id (character; the ratings `team_id` join key). |
| `away_team_id` | character | Away team ESPN id (character; the ratings `team_id` join key). |
| `neutral_site` | logical | Whether the game is at a neutral site (home-field advantage is dropped when true). |
| `exp_margin` | double | Expected home scoring margin in points (net_points_scale * net rating differential + the ridge-native home-field advantage on non-neutral fields). |
| `home_win_prob` | double | Home win probability, Phi(exp_margin / margin_sd) under a Gaussian margin model. |
| `exp_total` | double | Expected combined point total from the fitted efficiency + pace totals model. |

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import cfb_predict_games
from sportsdataverse.cfb import cfb_ratings
from sportsdataverse.cfb.cfb_schedule import cfb_schedule  # schedule loader
ratings = cfb_ratings(2023)
preds = cfb_predict_games(schedule_2023, ratings)
```

### cfb_ratings {#cfb_ratings}

`cfb_ratings(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, config: 'RatingsConfig | None' = None, fbs_only: 'bool' = True, drop_kneels: 'bool' = True, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

One row per team: the full CFB ratings spine (off/def/ST EPA + FEI).

Public orchestrator over `efficiency_ratings`,
`special_teams_ratings`, and `fei_ratings`. Loads play-by-play
+ schedule via `sportsdataverse.cfb.cfb_loaders.load_cfb_pbp` /
`sportsdataverse.cfb.cfb_loaders.load_cfb_schedule`, joins the
schedule's per-game date onto the plays, optionally applies the
as-of-date leakage boundary
(`sportsdataverse.cfb.cfb_prediction_constants.as_of_ratings_split`),
then fits all three component ratings on the (optionally filtered) plays
and reshapes them into one wide per-team table with dense ranks and a
net-rating z-score.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (e.g. `2023`) or a list of seasons to pool into one combined fit. |
| `as_of_date` | `date \| None` | `None` | When given, the leakage boundary -- only plays from games with `date < as_of_date` are used to fit the ratings (mirrors what was knowable heading into that date). `None` (default) uses the full season(s), unfiltered. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs forwarded to all three component functions. Defaults to `RatingsConfig` when omitted. |
| `fbs_only` | `bool` | `True` | Keep only FBS-vs-FBS games (gameonpaper `cfb-team-summaries` parity) -- both of the schedule's `home_division` / `away_division` must be `"fbs"`. Default True. Skipped (all games kept) when the schedule lacks the division columns; pass False to rate FCS opponents as regular teams. |
| `drop_kneels` | `bool` | `True` | Strip kneel-downs before fitting (gameonpaper parity). Default True. Uses a pipeline `kneel_down` flag when present, otherwise the play-text regex (`kneel` / `takes a knee`) plus the end-of-half anonymized-TEAM-run clock heuristic; skipped when neither a flag nor a play-text column exists. Pass False to let kneels with non-null EPA flow into the fit. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; otherwise polars. |

**Returns**

A DataFrame with one row per `team_id`, columns in this order: `season` (Int64 -- the single passed season for the common single-season call; `null` for a pooled multi-season call, since no single season applies to every row), `team_id` (Utf8), `adj_off_epa`, `adj_def_epa` (Float64, from `efficiency_ratings`), `adj_st_epa` (Float64, from `special_teams_ratings`), `adj_net` (Float64 -- offense minus defense only; special teams is a separate column, not folded in), `fei_off`, `fei_def`, `fei_net` (Float64, from `fei_ratings`), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model uses), `off_rank` (Int64, dense rank on `adj_off_epa` descending), `def_rank` (Int64, dense rank on `adj_def_epa` **ascending** -- fewer EPA allowed ranks better), `net_rank` (Int64, dense rank on `adj_net` descending), `net_z` (Float64, z-score of `adj_net`), `fei_off_rank` (Int64, dense rank on `fei_off` descending), `fei_def_rank` (Int64, dense rank on `fei_def` **ascending** -- fewer drive EPA allowed ranks better), `fei_net_rank` (Int64, dense rank on `fei_net` descending). Zero-row (correctly-typed) when the requested season(s) have no published pbp/schedule asset, or when `as_of_date` filters out every play.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import cfb_ratings
ratings = cfb_ratings(2023)
ratings.sort("net_rank").head()

# As-of-date leakage boundary

import datetime as dt
week3 = cfb_ratings(2023, as_of_date=dt.date(2023, 9, 18))

# Pandas round-trip

ratings_pd = cfb_ratings(2023, return_as_pandas=True)
```

### cfb_recruiting_projection {#cfb_recruiting_projection}

`cfb_recruiting_projection(target_season: 'int', *, division: 'str' = 'fbs', history_seasons: 'list[int] | None' = None, alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Project team wins / scoring margin for a season from preseason roster features.

Fits a ridge regression of realized wins (and average scoring margin) on
`[talent_composite, blue_chip_ratio, off_returning, def_returning,
prior_wins]` over strictly-prior seasons, then predicts the target season
from its preseason-known features. The as-of boundary is enforced
internally: rows with `season >= target_season` never enter training even
if `history_seasons` includes them.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int` |  | Season to project. |
| `division` | `str` | `'fbs'` | Division slug for constants lookups. |
| `history_seasons` | `list[int] \| None` | `None` | Seasons to draw training rows from (default: the six seasons before `target_season`). |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per team: `season` (Int64, = target), `team_id` (Utf8 ESPN id), `pred_wins`, `pred_margin` (Float64), `pred_net_epa` (Float64, currently null -- the adjusted-EPA target's hosted pbp source 404s). Zero-row (typed) when no history is available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Target season being projected (equals the requested target_season). |
| `team_id` | character | ESPN team id as a string (integer-origin). |
| `pred_wins` | double | Ridge-projected season win total from preseason roster features. |
| `pred_margin` | double | Ridge-projected average scoring margin per game. |
| `pred_net_epa` | double | Reserved adjusted-EPA projection - currently null (the hosted pbp source 404s). |

**Example**

```python
from sportsdataverse.cfb import cfb_recruiting_projection
proj = cfb_recruiting_projection(2024)
proj.sort("pred_wins", descending=True).head(10)
```

### cfb_roster_talent {#cfb_roster_talent}

`cfb_roster_talent(seasons: 'int | list[int]', *, division: 'str' = 'fbs', composite_247: 'pl.DataFrame | None' = None, max_class_size: 'int' = 25, rank_decay: 'float' = 0.75, recruits: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Team-talent composite per team-season (247 Team Talent Composite style).

Talent is the class-recency-weighted sum of per-recruit star points over the
trailing eligible recruiting classes (window = the length of the division's
`class_recency_weights`). When a 247 team-talent snapshot is supplied via
`composite_247`, its value overrides the derived composite for matched
team-seasons (the derived value remains the fallback).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Target season or list of seasons to rate. |
| `division` | `str` | `'fbs'` | Division slug for `get_constants` (star points, weights). |
| `composite_247` | `DataFrame \| None` | `None` | Optional frame with `season` (Int64), `team_id` (Utf8), `talent_247` (Float64). Join-key dtypes are asserted. |
| `max_class_size` | `int` | `25` | Top-N recruits per class that count toward `talent_composite`, ranked by star points. Defaults to the FBS limit of 25 initial counters. Raise it only deliberately: an uncapped sum measures class VOLUME, which put Air Force 7th nationally on 200 signees at a 0.000 blue-chip ratio. Largely superseded by `rank_decay`; retained as a hard floor. |
| `rank_decay` | `float` | `0.75` | Diminishing-returns exponent on a recruit's rank within their class (see RANK_DECAY`). 0.0 restores the flat sum. The default 0.75 was selected by sweeping against Spearman with actual wins, not chosen by taste. |
| `recruits` | `DataFrame \| None` | `None` | Pre-loaded per-recruit frame (the `load_recruit_classes` contract). Supplying it SKIPS the 247 fetch entirely, which is what the cfbfastR-cfb-data producer does when compiling from the raw store: a class is immutable once signed, but the composite spans a 4-season window, so fetching live re-pulled the same frozen classes once per target season (~20 min per call). Callers passing this own the frame's completeness. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per `(season, team_id)`: `team` (Utf8), `talent_composite` (Float64), `talent_rank` (Int64 dense rank desc within season), `blue_chip_ratio` (Float64), `n_recruits` (Int64). Zero-row (typed) when no recruits load.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the talent composite describes (trailing eligible classes aggregated). |
| `team_id` | character | 247Sports signed-institution team key as a string (integer-origin; joins to the recruit feed, not ESPN). |
| `team` | character | 247Sports full team name - the cross-source name-join key (the recruit-feed and talent-feed id spaces differ). |
| `talent_composite` | double | Class-recency-weighted sum of per-recruit star points (247 Team Talent Composite style); the 247 snapshot value when composite_247 is supplied. |
| `talent_rank` | integer | Dense rank on talent_composite descending within season (best = 1). |
| `blue_chip_ratio` | double | Share of the trailing four signing classes rated 4+ stars. |
| `n_recruits` | integer | Total signees across the trailing recruiting-class window. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import cfb_roster_talent
tal = cfb_roster_talent(2023)
tal.sort("talent_rank").head(10)
```

### cfb_simulations {#cfb_simulations}

`cfb_simulations(games: 'FrameLike', teams: 'FrameLike', compute_results: 'Optional[ComputeResultsFn]' = None, *, simulations: 'int' = 10000, playoff_seeds: 'int' = 12, tiebreaker_depth: 'str' = 'SOS', sim_include: 'str' = 'POST', rankings: 'Optional[FrameLike]' = None, seed: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Dict[str, Union[pl.DataFrame, Any]]'`

Simulate college football seasons (nflseedR-style week loop).

Replicates the input season `simulations` times, fills unplayed games
week by week through the pluggable `compute_results`, then simulates
the postseason (conference championships + CFP bracket) and aggregates
per-team probabilities. See the module docstring for every documented
CFB simplification.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `FrameLike` |  | One season of games in the engine schema (`season` or `sim`, `week`, `game_type`, `home_team`, `away_team`, `result` — null = unplayed, `neutral`). Played results are kept as-is. |
| `teams` | `FrameLike` |  | Team table (`team`, `conference`). |
| `compute_results` | `Optional[ComputeResultsFn]` | `None` | Results generator with the signature `fn(teams, games, week_num, **kwargs) -> {"teams": ..., "games": ...}` filling `result` for that week's unplayed games only. Defaults to `cfb_compute_results` (dynamic ELO). |
| `simulations` | `int` | `10000` | Number of simulated seasons (sequential, no chunking). |
| `playoff_seeds` | `int` | `12` | CFP field size passed to `cfb_playoff_seeds`. |
| `tiebreaker_depth` | `str` | `'SOS'` | nflseedR depth ladder (`RANDOM` < `PRE-SOV` < `SOS` < `POINTS`) used by every standings computation. |
| `sim_include` | `str` | `'POST'` | How deep to simulate: `"REG"` (regular season only), `"CONF"` (+ conference championships) or `"POST"` (+ CFP bracket, default). |
| `rankings` | `Optional[FrameLike]` | `None` | Optional committee rankings (`team`, `rank`) forwarded to `cfb_playoff_seeds`. When None, seeding falls back to the per-sim standings ordering (documented in `cfb_playoff_seeds`). |
| `seed` | `Optional[int]` | `None` | Seed for the numpy RNG (deterministic runs). |
| `return_as_pandas` | `bool` | `False` | Return pandas DataFrames instead of polars. |

**Returns**

Dict of frames mirroring the nflseedR summary list: * `"standings"` — per (sim, team) standings incl. `conf_rank`, `conf_champ` and (`sim_include="POST"`) `seed`. * `"games"` — all games incl. simulated results and generated postseason rows. * `"overall"` — per-team probabilities (`won_conf`, `made_playoff`, `first_round_bye`, `won_cfp`) and mean record columns. * `"game_summary"` — per unique matchup: games played, home win / tie rates and mean margin.

| col_name | type | description |
|---|---|---|
| `team` | character | Team name the simulated probabilities belong to (overall summary frame). |
| `conference` | character | Conference the team belongs to; null or "FBS Independents" marks an independent. |
| `wins` | double | Mean wins per simulated season (all game types through the conference championship). |
| `losses` | double | Mean losses per simulated season (all game types through the conference championship). |
| `ties` | double | Mean ties per simulated season. |
| `win_pct` | double | Mean overall win percentage across the simulated seasons. |
| `won_conf` | double | Share of simulations in which the team won its conference (CONF_CHAMP game winner, or rank-1 fallback). |
| `made_playoff` | double | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye` | double | Share of simulations in which the team earned a CFP first-round bye (seed 4 or better). |
| `won_cfp` | double | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb import cfb_simulations
out = cfb_simulations(games, teams, simulations=100, seed=42,
                      playoff_seeds=12)
print(out["overall"].sort("won_cfp", descending=True).head())

# Regular season only

out = cfb_simulations(games, teams, simulations=100,
                      sim_include="REG", seed=1)
```

### efficiency_ratings {#efficiency_ratings}

`efficiency_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted offensive/defensive efficiency.

Fits the offense/defense ridge from `cfb_adjusted_epa` on the
competitive plays in `plays` (`min_competitive_wp <= wp_before <=
max_competitive_wp`), then nets each team's raw per-game EPA (all
pass/rush plays, garbage time included) against the opponent's fitted
strength and averages across games -- the R `adjust_epa` /
gameonpaper `team_agg.R` statistic and scale (a top team nets
~0.30-0.40/play; the pre-2026-07-28 coefficient+intercept scale ran
~1.8x hotter). The ridge's dropped reference team nets normally from
its own games.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`). Callers pass an already as-of-date-filtered frame; this function is pure. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id`: `team_id` (Utf8), `adj_off_epa` / `adj_def_epa` / `adj_net` (Float64), `games` (Int64), `off_pace` (Float64 -- scrimmage plays per game, the tempo input the totals model consumes). Empty (zero-row, correctly-typed) when `plays` has no competitive plays.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings
ratings = efficiency_ratings(pbp)
ratings.sort("adj_net", descending=True).head()

# Custom ridge penalty

from sportsdataverse.cfb.cfb_prediction_constants import RatingsConfig
ratings = efficiency_ratings(pbp, config=RatingsConfig(ridge_lambda=100.0))
```

### fei_ratings {#fei_ratings}

`fei_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted per-drive efficiency (FEI-style).

The Fremeau Efficiency Index rates teams on drive value above expectation
given starting field position. The cfbfastR-schema `plays` frame this
package works with carries no starting-field-position column, so this
function uses the documented fallback: per-play EPA summed within each
`(game_id, drive_id)` group stands in for drive value, and that
aggregate is fit through the same opponent-adjustment ridge as
`efficiency_ratings` / `special_teams_ratings` -- no forked
solver. Offline validation against the Fremeau FEI oracle put this
fallback's team ranking at Spearman 0.967.

`cfb_adjusted_epa._prepare` filters to individual pass/rush plays and
is not reused here (drive value should reflect every play on the drive,
special-teams snaps included); the `hfa` treatment is reproduced
directly, matching `special_teams_ratings`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying every column in `cfb_adjusted_epa._REQUIRED_COLUMNS` (`game_id`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `wp_before`) plus `drive_id`. Not pre-aggregated to drives -- this function does that grouping itself. |
| `config` | `RatingsConfig \| None` | `None` | Ratings tuning knobs. Only `ridge_lambda` is consulted here; defaults to `RatingsConfig` when omitted. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing as `pos_team_id` on at least one drive: `team_id` (Utf8), `fei_off` / `fei_def` / `fei_net` (Float64). The ridge's dropped reference team is re-added at the shared intercept (`fei_net == 0.0`). Zero-row (correctly-typed) when `plays` has no rows with a non-null `EPA`.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import fei_ratings
fei = fei_ratings(pbp)
fei.sort("fei_net", descending=True).head()
```

### fit_field_position_ep {#fit_field_position_ep}

`fit_field_position_ep(drives: 'pl.DataFrame', *, start_col: 'str' = 'drive_start_yardline', pts_col: 'str' = 'drive_next_score_pts') -> 'pl.DataFrame'`

Fit the monotone EP-by-starting-yardline curve from a drives frame.

Groups drives by starting yard line (from own goal), takes the mean
next-score points, and applies sample-count-weighted isotonic regression
(weight = number of drives at each starting yard line, non-decreasing),
interpolated onto the full 1..99 grid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `DataFrame` |  | one row per drive. |
| `start_col` | `str` | `'drive_start_yardline'` | starting yard line from own goal (1..99). |
| `pts_col` | `str` | `'drive_next_score_pts'` | net next-score points for the drive's offense. |

**Returns**

`yardline_own: Int64 (1..99), ep: Float64` -- monotone non-decreasing. Empty input returns a zero-row frame.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99). |
| `ep` | double | Fitted expected points for a drive starting at this yard line (isotonic, non-decreasing). |

**Example**

```python
import polars as pl
from sportsdataverse.cfb.cfb_field_position import fit_field_position_ep
curve = fit_field_position_ep(drives_frame)
```

### get_2pt_probs {#get_2pt_probs}

`get_2pt_probs(pbp_df: 'Any') -> 'pd.DataFrame'`

Two-point-conversion decision surface (cfb4th `get_2pt_wp`).

Treats each row as "the scoring team just made a touchdown; decide between
the extra point and going for two". Enumerates the three point outcomes
(`0` / `1` / `2`) of the try, scores the opponent's ensuing-drive WP for
each from the scoring team's perspective, and combines them with the
two-point conversion probability (bundled CFB model) and the empirical CFB
extra-point make rate (XP_MAKE_PROB`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` | `Any` |  | Play-by-play frame (polars or pandas) carrying the `start.*` state columns in `sportsdataverse.cfb.cfb_fourth_down._PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus: * `two_pt_wp` -- `prob_2pt * wp(pts=2) + (1 - prob_2pt) * wp(pts=0)`. * `xp_wp` -- `prob_xp * wp(pts=1) + (1 - prob_xp) * wp(pts=0)` with `prob_xp = _XP_MAKE_PROB`. * `prob_2pt` -- the bundled-model two-point conversion probability. * `two_pt_recommendation` -- `"go_for_2"` iff `two_pt_wp > xp_wp` else `"kick_xp"` (None where the inputs are NaN). * `two_pt_wp_diff` -- `two_pt_wp - xp_wp` (positive => go for 2). When the two-point model isn't bundled (`TWO_PT_MODEL_AVAILABLE` is False) or the required state columns are missing, all decision columns are null -- probabilities are never fabricated.

**Example**

```python
from sportsdataverse.cfb.cfb_two_point import get_2pt_probs
out = get_2pt_probs(touchdown_rows)
print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())
```

### get_4th_down_probs {#get_4th_down_probs}

`get_4th_down_probs(pbp_df) -> 'pd.DataFrame'`

Full 4th-down decision surface (cfb4th `add_4th_probs`) + recommendation.

Runs `get_go_wp`, `get_fg_wp`, `get_punt_wp` on the
fourth-down rows and adds the combined option columns plus:

* `fourth_down_recommendation` -- the max-WP choice among `{go, punt,
  field_goal}` (NaN options are excluded; when the FG model isn't bundled,
  `field_goal` is excluded from the comparison).
* `go_wp_diff` / `punt_wp_diff` / `fg_wp_diff` -- each option's WP minus
  the recommended option's WP (the recommended option's diff is 0, the others
  <= 0). NaN where the option WP is NaN.
* `go_boost` -- cfb4th's headline number: `100 * (go_wp - max(fg_wp,
  punt_wp))` in percentage points.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` with the decision columns added. Empty input returns the input plus empty decision columns.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_4th_down_probs(fourth_down_rows)
print(out[["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"]].head())
```

### get_fg_wp {#get_fg_wp}

`get_fg_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of attempting a field goal (cfb4th `get_fg_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `fg_make_prob`, `make_fg_wp`, `miss_fg_wp` and `fg_wp` (= make_prob*make_wp + (1-make_prob)*miss_wp, from the kicking team's perspective). All four are NaN when the FG model is not bundled (`FG_MODEL_AVAILABLE` is False) -- probabilities are never fabricated.

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_fg_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_fg_wp(fourth_down_rows)
print(out[["fg_make_prob", "fg_wp"]].head())
```

### get_go_wp {#get_go_wp}

`get_go_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of going for it on 4th down (cfb4th `get_go_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations carrying the `start.*` state columns in PBP_COLS`. |

**Returns**

A pandas copy of `pbp_df` plus `go_wp` (prob-weighted WP of going for it), `first_down_prob` (P(conversion)), `wp_succeed` (mean WP over conversion outcomes) and `wp_fail` (mean WP over failure outcomes). `go_wp` is always in [0, 1]; the conditional columns are in [0, 1] but can be NaN for degenerate goal-line plays where one outcome bucket is empty (matches the R reference `pivot_wider` NA behavior).

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_go_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_go_wp(fourth_down_rows)
print(out[["go_wp", "first_down_prob"]].head())
```

### get_punt_wp {#get_punt_wp}

`get_punt_wp(pbp_df) -> 'pd.DataFrame'`

Expected win probability of punting on 4th down (cfb4th `get_punt_wp`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp_df` |  |  | Play-by-play frame (polars or pandas) of fourth-down situations. |

**Returns**

A pandas copy of `pbp_df` plus `punt_wp` (prob-weighted WP of punting, from the punting team's perspective). `punt_wp` is NaN where the punt end-yardline distribution has no support for the play's `yards_to_goal` (e.g. inside the 31, where punting is dominated and the cfb4th table is empty -- matching the R reference's left-join NA behavior).

**Example**

```python
from sportsdataverse.cfb.cfb_fourth_down import get_punt_wp

import polars as pl

# The `start.*` state contract -- a 4th & 10 from midfield, tied,
# early in the 2nd quarter. Every column here is required; a missing
# one raises KeyError naming it.
fourth_down_rows = pl.DataFrame(
    [
        {
            "start.down": 4,
            "start.distance": 10,
            "start.yardsToEndzone": 50,
            "start.pos_team_spread": 3.0,
            "pos_score_diff_start": 0,
            "start.TimeSecsRem": 900,
            "start.adj_TimeSecsRem": 1800,
            "start.pos_team_receives_2H_kickoff": 1,
            "start.posTeamTimeouts": 3,
            "start.defPosTeamTimeouts": 3,
            "start.is_home": 1,
            "period": 2,
            "season": 2023,
            "overUnder": 55.5,
            "homeTeamSpread": -3.0,
        }
    ]
)

out = get_punt_wp(fourth_down_rows)
print(out[["punt_wp"]].head())
```

### load_draft_outcomes {#load_draft_outcomes}

`load_draft_outcomes(years: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NFL draft picks with the college of each pick, for the requested draft years.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `years` | `int \| list[int]` |  | A draft year or list of draft years. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per pick: `draft_year` (Int64), `college` (Utf8 PFR-style college name), `player_id` (Utf8 ESPN college athlete id; null for older drafts), `player_name` (Utf8), `round` / `pick` (Int64), `position` (Utf8). Zero-row (typed) when the source is unavailable.

| col_name | type | description |
|---|---|---|
| `draft_year` | integer | NFL draft year of the pick. |
| `college` | character | College of the pick (PFR-style name, e.g. "Ohio St."). |
| `player_id` | character | ESPN college athlete id as a string (null for older drafts). |
| `player_name` | character | Player name as listed on the pick record. |
| `round` | integer | Round of the NFL draft the player was selected in (1-7 in the modern format). |
| `pick` | integer | Overall pick number. |
| `position` | character | Position drafted at (PFR abbreviation). |

**Example**

```python
from sportsdataverse.cfb import load_draft_outcomes
picks = load_draft_outcomes([2023, 2024])
picks.group_by("college").len().sort("len", descending=True).head()
```

### load_fp_curve {#load_fp_curve}

`load_fp_curve() -> 'pl.DataFrame'`

Load the bundled EP-by-yardline curve (no network, no first-use download).

**Returns**

`yardline_own: Int64 (1..99), ep: Float64`.

| col_name | type | description |
|---|---|---|
| `yardline_own` | integer | Starting yard line from the offense's own goal (1-99). |
| `ep` | double | Bundled expected points for a drive starting at this yard line (2018-2021 fit). |

**Example**

```python
from sportsdataverse.cfb.cfb_field_position import load_fp_curve
curve = load_fp_curve()
```

### load_recruit_classes {#load_recruit_classes}

`load_recruit_classes(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Load recruiting classes as per-recruit rows from the 247 RDB feed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single recruiting-class year or a list of them. |
| `division` | `str` | `'fbs'` | Division slug (reserved for constant lookups downstream; the feed itself is queried for all of college football). |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per committed recruit: `season` (Int64), `team_id` (Utf8 — the 247 committed-team key), `team` (Utf8 full name — the downstream name-join key, since the 247 recruit-team key differs from the 247 talent-composite key), `recruit_id` (Utf8), `stars` (Int64), `grade` (Float64 247 composite rating), `position` (Utf8). Zero-row (typed) when no data is available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Recruiting-class year the recruit signed in. |
| `team_id` | character | 247Sports signed-institution team key as a string (falls back to the committed institution when unsigned). |
| `team` | character | Signed-institution full name (falls back to committed) - the downstream name-join key. |
| `recruit_id` | character | 247Sports recruit key as a string (integer-origin). |
| `stars` | integer | 247 composite star rating (1-5; null for unrated recruits). |
| `grade` | double | 247 composite rating on the 0-100 scale. |
| `position` | character | Primary position abbreviation from the 247 recruit record. |

**Example**

```python
from sportsdataverse.cfb.cfb_roster_talent import load_recruit_classes
rec = load_recruit_classes([2022, 2023])
rec.group_by("team").len().sort("len", descending=True).head()
```

### normalize_pbp_columns {#normalize_pbp_columns}

`normalize_pbp_columns(df: 'pl.DataFrame', model: 'str') -> 'pl.DataFrame'`

Add card-named copies of any play-by-play columns `df` already carries.

A hand-built frame using the card's own names passes through untouched; a
pbp frame gains the names the card asks for. Copies rather than renames, so
nothing the caller passed in is removed.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Caller's frame. |
| `model` | `str` |  | Bundle stem, used to look up which features are wanted. |

**Returns**

`df` plus any alias columns that could be resolved.

**Example**

```python
normalize_pbp_columns(pbp, "xpass_model")
```

### predict_from_card {#predict_from_card}

`predict_from_card(df: 'pl.DataFrame', model: 'str', booster: 'Any') -> 'np.ndarray'`

Score `df` with `booster`, validated and ordered by the model's card.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `DataFrame` |  | Frame carrying at least the model's declared features. Extra columns are ignored, so a full pbp frame passes through unchanged. |
| `model` | `str` |  | Bundle stem, used to look up the card and to name the model in any error. |
| `booster` | `Any` |  | The loaded `xgboost.Booster`. |

**Returns**

The booster's raw predictions.

**Example**

```python
from sportsdataverse.cfb.model_calculators import predict_from_card
predict_from_card(pbp, "xpass_model", booster)
```

### predict_margin {#predict_margin}

`predict_margin(home_adj_net: 'float', away_adj_net: 'float', neutral: 'bool', *, era: 'str' = 'modern', games_played: 'float | None' = None) -> 'float'`

Expected home scoring margin from the two net ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_net` | `float` |  | Home team's opponent-adjusted net rating (`adj_net` from `cfb_ratings.efficiency_ratings`). |
| `away_adj_net` | `float` |  | Away team's opponent-adjusted net rating. |
| `neutral` | `bool` |  | Whether the game is at a neutral site (no home-field advantage). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted slope, `hfa_points` and the attenuation curve. |
| `games_played` | `float \| None` | `None` | Games behind the WEAKER of the two as-of ratings. Supplying it selects the games-played slope (see `slope_for_games`) and is worth ~0.6 MAE; omitting it falls back to the flat `net_points_scale`, which is the average over the curve. |

**Returns**

The expected margin (home minus away), in points: `slope * (home_adj_net - away_adj_net) + hfa_points` on a home field, or without the HFA term on a neutral one. HFA is added in POINTS, not routed through the slope. The previous form multiplied an EPA-scale `2 * hfa_epa` by `net_points_scale`, which tied the two together and let them drift apart unnoticed -- the shipped pair implied ~1.65 points against a measured ~3.0.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_margin
predict_margin(0.30, 0.10, neutral=False)

# With games-played, which selects the attenuation-corrected slope

predict_margin(0.30, 0.10, neutral=False, games_played=9)
```

### predict_total {#predict_total}

`predict_total(home_adj_off: 'float', home_adj_def: 'float', away_adj_off: 'float', away_adj_def: 'float', game_pace: 'float', *, era: 'str' = 'modern') -> 'float'`

Expected combined point total from the four efficiency ratings + tempo.

Fitted linear model `total_intercept + total_scale * sum4 + total_pace_scale *
game_pace`, where `sum4 = home_adj_off + away_adj_def + away_adj_off +
home_adj_def`. The four ratings are summed because each side's scoring rises
with its own offense and with the opponent's EPA-*allowed* (`adj_def` is
lower = better defense). `game_pace` (the matchup's expected scrimmage plays,
`home_off_pace * away_off_pace / league_avg_pace`) enters because a total is a
*sum* -- tempo scales both sides' points the same way, so it compounds into the
total (whereas in the margin, a differential, pace cancels). All three
coefficients are fitted on 2023 actual totals.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `home_adj_off` | `float` |  | Home offense adjusted EPA/play (`adj_off_epa`). |
| `home_adj_def` | `float` |  | Home defense adjusted EPA/play allowed (`adj_def_epa`). |
| `away_adj_off` | `float` |  | Away offense adjusted EPA/play. |
| `away_adj_def` | `float` |  | Away defense adjusted EPA/play allowed. |
| `game_pace` | `float` |  | Expected scrimmage plays for the matchup, i.e. `home_off_pace * away_off_pace / league_avg_pace` from the ratings' `off_pace` column (`cfb_predict_games` computes this for you). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS` supplying the fitted `total_intercept` / `total_scale` / `total_pace_scale`. |

**Returns**

The expected combined total points.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import predict_total
predict_total(0.20, -0.05, 0.10, 0.02, game_pace=66.0)
```

### slope_for_games {#slope_for_games}

`slope_for_games(games_played: 'float | None', *, era: 'str' = 'modern') -> 'float'`

Points per unit of rating differential, given how many games back it.

A single slope is wrong. An as-of rating built on two games is a far
noisier predictor than one built on twelve, and OLS slopes attenuate
toward zero as predictor noise grows -- so the correct multiplier is
smaller early and grows through the season. Measured, walk-forward on
2014-2025:

    0-3 games -> 10.62      6-7 games -> 42.00
    4-5 games -> 26.06      8+  games -> 54.49

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games_played` | `float \| None` |  | Games behind the as-of rating. When two ratings back a prediction this should be the WEAKER (smaller) of the two, since the noisier rating binds the attenuation. `None` selects the flat `net_points_scale`. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

The points-per-rating-unit slope for that bucket, or the flat `net_points_scale` when `games_played` is `None` or falls outside every bucket. The flat value is the average over the curve, so it is a safe default rather than a silent zero.

**Example**

```python
from sportsdataverse.cfb.cfb_game_predict import slope_for_games
slope_for_games(2)      # early season -- heavily attenuated
slope_for_games(11)     # late season -- near the full slope

# Unknown game count falls back to the flat scale

slope_for_games(None)
```

### special_teams_ratings {#special_teams_ratings}

`special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: a per-unit special-teams EPA composite.

Special teams was empirically found NOT to obey the offense-minus-defense
symmetry `efficiency_ratings` / `fei_ratings` rely on, and not
to benefit from opponent adjustment, when validated against the 2023 SP+
special-teams oracle (`tests/fixtures/cfb_prediction/sp_plus_2023.parquet`
`sp_special`):

* The executing `pos_team` owns the EPA on a kickoff / punt / field
  goal. The `def_pos_team` "coverage" side reflects the opposing
  returner's skill, not the coverage team's, and is not recoverable from
  EPA -- adding any coverage unit *lowers* SP+ agreement (0.77 -> 0.58),
  so coverage/defense units are excluded entirely (see the module's
  special-teams unit patterns).
* The opponent-adjustment ridge (`cfb_adjusted_epa._fit_opponent_ridge`)
  *hurts* agreement (0.72 vs 0.77) -- special teams is only weakly
  opponent-dependent, so this function does not fit a ridge at all.
* Splitting the offense-side plays into per-phase units (field goal, punt,
  kick return) is what helps. Each unit's per-team mean EPA/play is
  centered on that unit's league-wide per-play mean and the three
  centered deviations are summed -- true EPA units. This centered form
  reached Spearman 0.865 against SP+ special teams, beating both the
  originally-shipped z-scored composite (0.768 -- dimensionless, std
  ~1.7, range +-5 under an epa` column name; replaced 2026-07-28)
  and a single-unit offense-minus-intercept ridge fit (0.703).

`adj_st_epa` is therefore the sum, over the three special-teams units
(field goal, punt, kick return), of each unit's per-team mean EPA/play
above the unit's league average. A team with no plays in a given unit
contributes 0 for that unit (not a penalty). `config` is accepted for
signature parity with
`efficiency_ratings` / `fei_ratings` but is unused -- there is
no ridge (and therefore no `ridge_lambda`) in this recipe.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | A cfbfastR-schema play-by-play frame carrying `game_id`, `pos_team_id`, `EPA`, and `play_type`. Not pre-filtered to special-teams plays -- this function does that filtering itself. |
| `config` | `RatingsConfig \| None` | `None` | Unused (kept for signature parity across the three rating functions). See the note above. |

**Returns**

A `polars.DataFrame` with one row per `team_id` appearing anywhere in `plays`: `team_id` (Utf8), `adj_st_epa` (Float64, the sum of per-unit executing-team mean EPA/play above each unit's league average). Teams with no special-teams plays get `adj_st_epa == 0.0`. Zero-row (correctly-typed) when `plays` has no special-teams plays.

**Example**

```python
from sportsdataverse.cfb.cfb_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```
