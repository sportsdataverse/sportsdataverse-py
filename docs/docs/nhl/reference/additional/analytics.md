---
title: "NHL — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 4
description: "NHL — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — Analytics

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

### nhl_edge_skating_value {#nhl_edge_skating_value}

`nhl_edge_skating_value(*, season: 'int', league: 'str' = 'nhl', detail_frames: 'pl.DataFrame | None' = None, method: "Literal['zscore', 'percentile']" = 'zscore', include_zone_balance: 'bool' = False, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per-skater EDGE skating-value composite (z-score or percentile blend).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | Season end-year (e.g. `2024` for 2023-24). |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"`. PWHL short-circuits to a zero-row frame (no EDGE feed) BEFORE any network access. |
| `detail_frames` | `DataFrame \| None` | `None` | Pre-parsed EDGE aggregate (one row per skater with the COMPONENTS` columns) for offline use. When `None` on the NHL path, the live per-skater `nhl_edge_skater_*_detail` fetch would run -- not implemented offline; supply `detail_frames`. |
| `method` | `Literal['zscore', 'percentile']` | `'zscore'` | `"zscore"` (default, original composite) or `"percentile"` -- see the module docstring's flesh-out note. |
| `include_zone_balance` | `bool` | `False` | add the derived `oz_dz_time_balance` component when `dz_time_pct` is present in `detail_frames` (default `False` -- preserves the original 4-component output). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-skater frame: `player_id`, `season`, `top_speed`, `distance_km`, `speed_bursts_20`, `oz_time_pct`, `skating_value`, `skating_value_rank` (1 = fastest composite), plus `oz_dz_time_balance` when `include_zone_balance=True` and derivable. PWHL (or empty/absent input) returns a zero-row frame with the base schema.

**Example**

```python
from sportsdataverse.nhl.nhl_edge_value import nhl_edge_skating_value

out = nhl_edge_skating_value(season=2024, detail_frames=edge_df)

# Percentile composite + zone-balance component

out = nhl_edge_skating_value(
    season=2024, detail_frames=edge_df, method="percentile", include_zone_balance=True
)
```

### nhl_faceoff_value {#nhl_faceoff_value}

`nhl_faceoff_value(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per-player context-adjusted faceoff-win value.

Fits `fit_faceoff_context` on the taker-perspective expansion of
every faceoff in `pbp`, then aggregates each player's win rate above
the context expectation and a zone-weighted `faceoff_value` using
`get_constants(league).faceoff_zone_weights`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key for `~sportsdataverse.nhl.nhl_microstat_constants.get_constants` (`"nhl"` or `"pwhl"`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `faceoffs_taken`, `faceoffs_won`, `fo_win_pct`, `fo_win_pct_above_exp`, `faceoff_value`. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_faceoff_value import nhl_faceoff_value

out = nhl_faceoff_value(pbp)

# PWHL

out_pwhl = nhl_faceoff_value(pwhl_pbp, league="pwhl")
```

### nhl_game_total {#nhl_game_total}

`nhl_game_total(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-game expected total goals -- a thin re-export of model ②'s expected-goals helper.

Satisfies the "props + total" grouping of model ③ (the brief's player-prop
surface) without a second implementation of the goals math: this calls
the exact same `sportsdataverse.nhl.nhl_market.predict_total` that
`sportsdataverse.nhl.nhl_market.nhl_predict_games` uses internally.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | a schedule-shaped frame with `game_id`, `home_team`, `away_team`, `neutral_site`. |
| `ratings` | `DataFrame` |  | the output of `sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings`. |
| `league` | `str` | `'nhl'` | resolves HFA/sigma/total_scale via `get_constants`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame with `game_id`, `exp_total`. |col_name |type | |:---------|:------| |game_id |String | |exp_total |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_player_props import nhl_game_total
nhl_game_total(games, ratings)
```

### nhl_in_game_win_prob {#nhl_in_game_win_prob}

`nhl_in_game_win_prob(pbp: 'pl.DataFrame', pregame_home_prob: 'float', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-play live home win probability from the bundled in-game logistic.

Builds `in_game_features` from `pbp`, scores them with the
committed logistic (`sportsdataverse/nhl/models/<league>_in_game_wp.json`
-- no first-use download; the coefficients are trained offline by
`dev/nhl_prediction/train_in_game_wp.py` and committed), and returns
`sigmoid(coef . features + intercept)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a play-by-play frame shaped like `load_nhl_pbp_full`. |
| `pregame_home_prob` | `float` |  | the model (2) pregame home win probability anchor. |
| `league` | `str` | `'nhl'` | resolves the bundled artifact filename via `get_constants`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per play, with a single `home_win_prob: Float64` column.

**Example**

```python
from sportsdataverse.nhl.nhl_market import nhl_in_game_win_prob, win_prob_from_margin

pregame_p = win_prob_from_margin(0.3)
wp = nhl_in_game_win_prob(pbp, pregame_home_prob=pregame_p)
print(wp.tail())
```

### nhl_penalty_value {#nhl_penalty_value}

`nhl_penalty_value(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per-player net penalty drawn/taken value.

Tallies each player's penalties drawn (`player_id == drawn_player_id`)
and taken (`player_id == committed_player_id`), split minor/major, and
converts the net into expected goals:
`net_penalty_value = (minors_drawn - minors_taken) * pp_goal_value +
(majors_drawn - majors_taken) * major_penalty_value` using
`get_constants(league)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key for `~sportsdataverse.nhl.nhl_microstat_constants.get_constants`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `penalties_drawn`, `penalties_taken`, `minors_drawn`, `minors_taken`, `majors_drawn`, `majors_taken`, `net_penalties`, `net_penalty_value`. League-wide `net_penalty_value` sums to (approximately) zero by construction -- every penalty taken by one player is drawn by another. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_penalty_value import nhl_penalty_value

out = nhl_penalty_value(pbp)

# PWHL

out_pwhl = nhl_penalty_value(pwhl_pbp, league="pwhl")
```

### nhl_player_props {#nhl_player_props}

`nhl_player_props(seasons: 'Union[int, list[int]]', *, league: 'str' = 'nhl', as_of_date: '_dt.date | None' = None, stats: 'tuple[str, ...]' = ('shots', 'points'), return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Empirical-Bayes shots/points player-prop projections.

For every player-game in `load_nhl_skater_boxscores`, projects that
game's shots-on-goal / points from the player's **strictly prior** games
(leakage-safe per row by construction), EB-shrunk toward a position prior,
then adjusted by an opponent matchup multiplier (model ① `adj_xga`) and a
team game-script tilt (model ② native `exp_margin` -- never the market
line). **See the module docstring's leakage-scope note:** the per-player
rate is strictly as-of, but the matchup/game-script ratings are a single
snapshot (as-of `as_of_date` if given, else full-season), not
per-projected-game ratings.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Union[int, list[int]]` |  | an int or iterable of seasons (`load_nhl_skater_boxscores` only publishes seasons >= 2024). |
| `league` | `str` | `'nhl'` | resolves `prop_kappa`/`pos_priors`/`prop_team_volume_slope` via `get_constants`. |
| `as_of_date` | `date \| None` | `None` | if given, only games strictly before this date are projected AND the matchup/game-script ratings snapshot is computed as-of this cutoff. NOTE: the per-player usage rate is strictly-prior regardless of this arg (it never needed a cutoff); this arg tightens *which* games are projected and the *single* ratings snapshot, but does not make the ratings per-projected-game as-of (a documented approximation -- see the module docstring). |
| `stats` | `tuple[str, ...]` | `('shots', 'points')` | which stat families to project (`"shots"`, `"points"`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per (player, game, stat). Empty/malformed input returns a zero-row frame with the documented schema. |col_name |type | |:---------|:------| |season |Int64 | |game_id |String | |player_id |String | |team |String | |opp_team |String | |stat |String | |proj_mean |Float64| |proj_sd |Float64| |p_over |Float64| |line |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_player_props import nhl_player_props

props = nhl_player_props(2024, stats=("shots",))
print(props.sort("proj_mean", descending=True).head())

# Pipeline next step (one line)

props.filter(pl.col("player_id") == "8478402")
```

### nhl_predict_games {#nhl_predict_games}

`nhl_predict_games(games: 'pl.DataFrame', ratings: 'pl.DataFrame', *, league: 'str' = 'nhl', odds: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Vectorized pregame margin/win-prob/total (+ market edge) over a schedule.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | a schedule-shaped frame with `game_id`, `home_team`, `away_team`, `neutral_site` (`home_team`/`away_team` must share `nhl_team_ratings`'s `team` dtype -- asserted below). |
| `ratings` | `DataFrame` |  | the output of `sportsdataverse.nhl.nhl_team_ratings.nhl_team_ratings` (`team`, `adj_xgf`, `adj_xga`). |
| `league` | `str` | `'nhl'` | resolves HFA/sigma via `get_constants`. |
| `odds` | `Optional[DataFrame]` | `None` | optional frame with `game_id`, `close_puck_line_home`; when supplied, `market_edge = exp_margin - close_puck_line_home`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

A polars (or pandas) DataFrame, one row per game. |col_name |type | |:-------------|:------| |game_id |String | |home_team |String | |away_team |String | |neutral_site |Boolean| |exp_margin |Float64| |home_win_prob |Float64| |exp_total |Float64| |market_edge |Float64|

**Example**

```python
from sportsdataverse.nhl.nhl_market import nhl_predict_games
preds = nhl_predict_games(games, ratings)
print(preds.sort("home_win_prob", descending=True).head())
```

### nhl_zone_transitions {#nhl_zone_transitions}

`nhl_zone_transitions(pbp: 'pl.DataFrame', *, league: 'str' = 'nhl', tags: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per-player controlled/dump entry & exit rates + xG-weighted values.

`entry_value = controlled_entries * zone_entry_value_controlled +
dump_entries * zone_entry_value_dump`; `exit_value = exits *
zone_exit_value` -- all from `get_constants(league)`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | Parsed pbp frame (Task-0.1 contract). |
| `league` | `str` | `'nhl'` | League key for the value constants. |
| `tags` | `DataFrame \| None` | `None` | Optional ground-truth controlled/dump override (see `infer_zone_transitions`). |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

Per-player frame: `player_id`, `controlled_entries`, `dump_entries`, `exits`, `controlled_entry_rate`, `entry_value`, `exit_value`. Zero-row input returns a zero-row frame with this schema.

**Example**

```python
from sportsdataverse.nhl.nhl_zone_transitions import nhl_zone_transitions

out = nhl_zone_transitions(pbp)

# PWHL

out_pwhl = nhl_zone_transitions(pwhl_pbp, league="pwhl")
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
