---
title: "NHL — additional Python functions — NHL native"
sidebar_label: "NHL native"
sidebar_position: 2
description: "NHL — additional Python functions — NHL native — function reference in sdv-py, the SportsDataverse Python package."
---
# NHL — additional Python functions — NHL native

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

### nhl_pbp_disk {#nhl_pbp_disk}

`nhl_pbp_disk(game_id, path_to_json)`

_No description available._

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` |  |  |  |
| `path_to_json` |  |  |  |

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

### nhl_records_coach_milestone_wins {#nhl_records_coach_milestone_wins}

`nhl_records_coach_milestone_wins(wins: 'int', playoffs: 'bool' = False, **filters) -> 'Dict'`

Coaches who reached a wins milestone in fewest games.

Wraps one of the `/coach-fewest-games-to-{N}-wins` or
`/coach-fewest-games-to-{N}-playoff-wins` paths.

Supported *wins* values: `50, 100, 150, 200, 300, 400, 500, 600, 700,
800, 900, 1000` (regular season); `50, 100, 150` (playoffs).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `wins` | `int` |  | Milestone win total (e.g. `100`). |
| `playoffs` | `bool` | `False` | If `True`, use the playoff-wins path. |

**Returns**

Coaches who hit the milestone, sorted by games needed.

### nhl_records_comeback_wins {#nhl_records_comeback_wins}

`nhl_records_comeback_wins(scope: 'str' = 'league', **filters) -> 'Dict'`

Comeback wins from a multi-goal deficit.

Wraps:
  * `GET /comeback-league-wins` when *scope* is `"league"`.
  * `GET /comeback-franchise-wins` when *scope* is `"franchise"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `scope` | `str` | `'league'` | `"league"` (default) or `"franchise"`. |

**Returns**

Games where the team overcame a deficit to win.

### nhl_records_consecutive_goal_seasons {#nhl_records_consecutive_goal_seasons}

`nhl_records_consecutive_goal_seasons(goals: 'int' = 50, **filters) -> 'Dict'`

Skaters with the most consecutive N-goal seasons.

Wraps one of:
  * `GET /consecutive-20-goal-seasons`
  * `GET /consecutive-30-goal-seasons`
  * `GET /consecutive-40-goal-seasons`
  * `GET /consecutive-50-goal-seasons`
  * `GET /consecutive-60-goal-seasons`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `goals` | `int` | `50` | Goal threshold — one of `20, 30, 40, 50, 60`. |

**Returns**

Skaters sorted by consecutive-season streak.

### nhl_records_fastest_goals {#nhl_records_fastest_goals}

`nhl_records_fastest_goals(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals by one team in a single game.

Wraps one of:
  * `GET /fastest-2-goals-one-team`
  * `GET /fastest-3-goals-one-team`
  * `GET /fastest-4-goals-one-team`
  * `GET /fastest-5-goals-one-team`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Goal count — one of `2, 3, 4, 5`. |

**Returns**

Games where the milestone was set, sorted by elapsed time (fastest first).

### nhl_records_fastest_goals_both_teams {#nhl_records_fastest_goals_both_teams}

`nhl_records_fastest_goals_both_teams(n_goals: 'int' = 2, **filters) -> 'Dict'`

Fastest N goals combined (both teams) in a single game.

Wraps one of:
  * `GET /fastest-2-goals-both-teams`
  * `GET /fastest-3-goals-both-teams`
  * `GET /fastest-4-goals-both-teams`
  * `GET /fastest-5-goals-both-teams`
  * `GET /fastest-6-goals-both-teams`

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `n_goals` | `int` | `2` | Combined goal count — one of `2, 3, 4, 5, 6`. |

**Returns**

Sorted by elapsed time (fastest first).

### nhl_records_games_played_streak_skaters {#nhl_records_games_played_streak_skaters}

`nhl_records_games_played_streak_skaters(active_only: 'bool' = False, **filters) -> 'Dict'`

Consecutive games-played streaks for skaters.

Wraps `GET /games-played-streak-skaters` (career) or
`GET /games-played-active-streak-skaters` (currently active streaks).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `active_only` | `bool` | `False` | If `True`, return only active streaks. |

**Returns**

Skaters sorted by streak length.

### nhl_scoreboard {#nhl_scoreboard}

`nhl_scoreboard(date: 'Optional[str]' = None, team: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Dict'`

In-game scoreboard payload (renamed from `nhl_web_scoreboard`).

Picks among three mutually-exclusive NHL api-web forms (kept hand-written
because the URL-builder codegen can't represent the 3-way branch):

* `GET /v1/scoreboard/{team}/now` -- team-scoped now (when `team` set),
* `GET /v1/scoreboard/{date}` -- league-wide on a date,
* `GET /v1/scoreboard/now` -- league-wide now (both args None).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `Optional[str]` | `None` | `YYYY-MM-DD`; `None` -> `/now`. Mutually exclusive with `team`. |
| `team` | `Optional[str]` | `None` | 3-letter abbreviation; takes precedence over `date`. |
| `return_parsed` | `bool` | `True` | dispatch the raw payload through `parse_nhl_web_scoreboard`. |
| `return_as_pandas` | `bool` | `False` | with `return_parsed`, return pandas instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `scoreboard_date` | character | Calendar date (YYYY-MM-DD) for which this scoreboard snapshot was retrieved from the NHL api-web feed. |
| `id` | integer | Unique player identifier. |
| `season` | integer | Season year (echoed from arg). |
| `game_type` | integer | Game type the row belongs to. |
| `game_date` | character | Game date. |
| `game_center_link` | character | Link to the NHL game center page. |
| `start_time_utc` | character | Scheduled start time in UTC. |
| `eastern_utc_offset` | character | Eastern time UTC offset. |
| `venue_utc_offset` | character | Venue UTC offset. |
| `tv_broadcasts` | character | Nested list of TV broadcast details. |
| `game_state` | character | Game state (e.g., FINAL, LIVE). |
| `game_schedule_state` | character | Schedule state of the game. |
| `tickets_link` | character | URL to the English-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `tickets_link_fr` | character | URL to the French-language ticket purchase page for the game, as provided by the NHL api-web scoreboard. |
| `period` | double | Period number. |
| `three_min_recap` | character | Link to the three-minute recap. |
| `three_min_recap_fr` | character | Link to the French three-minute recap. |
| `venue_default` | character | Venue name (default language). |
| `away_team_id` | integer | Away team identifier. |
| `away_team_name_default` | character | Full English-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_name_fr` | character | Full French-language team name for the away team, as returned by the NHL api-web scoreboard feed. |
| `away_team_common_name_default` | character | Away team common name (default language). |
| `away_team_place_name_with_preposition_default` | character | Away team place name with preposition (default). |
| `away_team_place_name_with_preposition_fr` | character | Away team place name with preposition (French). |
| `away_team_abbrev` | character | Away team abbreviation. |
| `away_team_score` | double | Away team final score. |
| `away_team_logo` | character | URL to the away team logo. |
| `home_team_id` | integer | Home team identifier. |
| `home_team_name_default` | character | Full English-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_name_fr` | character | Full French-language team name for the home team, as returned by the NHL api-web scoreboard feed. |
| `home_team_common_name_default` | character | Home team common name (default language). |
| `home_team_place_name_with_preposition_default` | character | Home team place name with preposition (default). |
| `home_team_place_name_with_preposition_fr` | character | Home team place name with preposition (French). |
| `home_team_abbrev` | character | Home team abbreviation. |
| `home_team_score` | double | Home team final score. |
| `home_team_logo` | character | URL to the home team logo. |
| `period_descriptor_number` | double | Period number. |
| `period_descriptor_period_type` | character | Period type (e.g., REG, OT). |
| `period_descriptor_max_regulation_periods` | double | Maximum number of regulation periods. |
| `series_status_round` | integer | Playoff round number for this game's series (1 = first round, 2 = second round, etc.). |
| `series_status_series_abbrev` | character | Short abbreviation string identifying the specific playoff series matchup (e.g., 'A1' for a particular bracket slot). |
| `series_status_game` | integer | Game number within the current playoff series (e.g., 1 through 7) for the game represented in this scoreboard row. |
| `series_status_top_seed_team_abbrev` | character | Three-letter abbreviation for the higher-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_top_seed_wins` | integer | Number of wins accumulated by the higher-seeded team in the current playoff series as of this scoreboard snapshot. |
| `series_status_bottom_seed_team_abbrev` | character | Three-letter abbreviation for the lower-seeded team in the playoff series context embedded in the scoreboard game entry. |
| `series_status_bottom_seed_wins` | integer | Number of wins accumulated by the lower-seeded team in the current playoff series as of this scoreboard snapshot. |
| `period_descriptor_ot_periods` | double | Number of overtime periods played when the game extended beyond regulation, as reported in the scoreboard period descriptor. |
| `away_team_record` | character |  |
| `home_team_record` | character |  |
| `away_team_common_name_fr` | character | Away team common name (French). |
| `home_team_common_name_fr` | character | Home team common name (French). |

**Example**

```python
nhl_scoreboard(date="2024-03-01")
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

### nhl_special_teams_value {#nhl_special_teams_value}

`nhl_special_teams_value(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, league: 'str' = 'nhl', return_as_pandas: 'bool' = False, _stints: 'pl.DataFrame | None' = None) -> "'pl.DataFrame | pd.DataFrame'"`

Per-skater power-play/penalty-kill value (goals) above/below league baseline.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_nhl_pbp_full`-shaped frame. |
| `shifts` | `DataFrame` |  | a `load_nhl_shifts`-shaped frame. |
| `model_dir` | `str \| None` | `None` | passed through to `nhl_xg`. |
| `league` | `str` | `'nhl'` | `"nhl"` or `"pwhl"` -- selects `league_xg_rate_pp`/pk` via `LEAGUE_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |
| `_stints` | `DataFrame \| None` | `None` | internal test hook -- inject a pre-built stints frame. |

**Returns**

`player_id:Int64, pp_toi_minutes:Float64, pk_toi_minutes:Float64, pp_value:Float64, pk_value:Float64`. Empty input returns a zero-row frame with this schema.

**Example**

```python
import polars as pl
from sportsdataverse.nhl.nhl_special_teams import nhl_special_teams_value
pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
shifts = pl.read_parquet("tests/fixtures/nhl_player_impact/shifts_sample.parquet")
st = nhl_special_teams_value(pbp, shifts, model_dir="tests/fixtures/nhl_player_impact/xg_models")
print(st.sort("pp_value", descending=True).head(10))
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
