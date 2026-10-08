---
title: "CFB — additional Python functions — Analytics: cfb_season–play_type"
sidebar_label: "Analytics: cfb_season–play_type"
sidebar_position: 13
description: "CFB — additional Python functions — Analytics: cfb_season–play_type — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — Analytics: cfb_season–play_type

### cfb_season_odds {#cfb_season_odds}

`cfb_season_odds(seasons: 'int | list[int]', *, as_of_date: 'datetime.date | None' = None, n_sims: 'int' = 10000, playoff_seeds: 'int' = 12, seed: 'int' = 0, era: 'str' = 'modern', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Ratings-driven season Monte Carlo: conference / playoff / championship odds.

Thin wrapper over `cfb_simulations.cfb_simulations` -- it builds the ratings
with `cfb_ratings.cfb_ratings`, converts the schedule to the engine format
with `cfb_standings.cfb_games_from_schedule` (re-keyed on ESPN `team_id` so
the ratings align), and feeds `make_ratings_compute_results` as the sampler.
All season / standings / bracket machinery is reused; unplayed games are simulated,
played games (before `as_of_date`) are kept. Only FBS programs (schedule
`division == "fbs"`) enter the simulated universe; non-FBS opponents stay in the
game set -- their games still count toward FBS records -- but can never reach the
standings, the playoff field, or the output.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A single season (an `int`, or a one-element list). Multiple seasons raise `ValueError` -- the simulation engine is single-season. |
| `as_of_date` | `date \| None` | `None` | Leakage boundary applied to BOTH the ratings vintage and the game set. Ratings are fit only on plays from games with `date < as_of_date` (`cfb_ratings.cfb_ratings`), and schedule results from `start_date` on/after `as_of_date` are masked so those games are simulated instead of replayed; masked postseason rows are dropped (the matchup is itself an outcome) and regenerated from each sim's own standings. `None` uses the full season as-is. |
| `n_sims` | `int` | `10000` | Number of simulated seasons. |
| `playoff_seeds` | `int` | `12` | CFP field size. |
| `seed` | `int` | `0` | RNG seed for reproducibility. |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per team: `season`, `team_id` (Utf8), `exp_wins`, `conf_title_prob`, `playoff_prob`, `first_round_bye_prob`, `cfp_champ_prob` (Float64 probabilities in [0, 1]). Zero-row (typed) when no ratings/schedule are available.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season simulated (null for a pooled multi-season call). |
| `team_id` | character | Team ESPN id (character join key). |
| `exp_wins` | double | Mean wins per simulated season. |
| `conf_title_prob` | double | Share of simulations in which the team won its conference. |
| `playoff_prob` | double | Share of simulations in which the team made the College Football Playoff field. |
| `first_round_bye_prob` | double | Share of simulations in which the team earned a CFP first-round bye. |
| `cfp_champ_prob` | double | Share of simulations in which the team won the College Football Playoff national championship. |

**Example**

```python
from sportsdataverse.cfb.cfb_season_odds import cfb_season_odds
odds = cfb_season_odds(2023, n_sims=2000)
odds.sort("cfp_champ_prob", descending=True).head()
```

### cfb_transfer_impact {#cfb_transfer_impact}

`cfb_transfer_impact(target_season: 'int | list[int]', *, division: 'str' = 'fbs', alpha: 'float' = 1.0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Net transfer talent and its projected win-total impact per team-season.

`pred_win_delta` comes from an on-demand ridge of realized win deltas on
`net_transfer_talent` fitted over strictly-prior seasons (the as-of
boundary is enforced internally per target season).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `target_season` | `int \| list[int]` |  | Season (or list) to score. |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `alpha` | `float` | `1.0` | Ridge L2 penalty. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

Per (season, team_id): `net_transfer_talent` (Float64), `pred_win_delta` (Float64). Zero-row (typed) when no data.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season the net transfer talent describes. |
| `team_id` | character | ESPN team id as a string (rosters team_id, cast Int64 to Utf8). |
| `net_transfer_talent` | double | Incoming minus outgoing transfer talent points for the season. |
| `pred_win_delta` | double | Ridge-projected win-total change from net transfer talent (as-of fit; weak observed validity - see the strict-xfail gate). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_impact
imp = cfb_transfer_impact(2024)
imp.sort("net_transfer_talent", descending=True).head(10)
```

### cfb_transfer_moves {#cfb_transfer_moves}

`cfb_transfer_moves(seasons: 'int | list[int]', *, division: 'str' = 'fbs', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Transfer moves inferred from year-over-year roster diffs.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | Destination season(s) to extract moves for (each compares S-1 -> S). |
| `division` | `str` | `'fbs'` | Division slug for the star-points constants. |
| `return_as_pandas` | `bool` | `False` | If True, return a pandas DataFrame; otherwise polars. |

**Returns**

One row per move side: `season` (Int64, the destination season), `team_id` (Utf8 ESPN team id), `player_id` (Utf8 ESPN athlete id), `direction` ("in" | "out"), `prior_team_id` (Utf8 ESPN team id of the season S-1 team), `talent_points` (Float64, name-joined to the `cfb_recruits` release; the 0-star default when the player has no recruit rating). Zero-row (typed) when rosters are unavailable.

| col_name | type | description |
|---|---|---|
| `season` | integer | Destination season of the move (compares rosters S-1 to S). |
| `team_id` | character | ESPN team id (as a string) of the side this row describes (destination for "in", origin for "out"). |
| `player_id` | character | ESPN athlete id as a string. |
| `direction` | character | Move side - "in" (arriving at team_id) or "out" (leaving team_id). |
| `prior_team_id` | character | ESPN team id (as a string) of the season S-1 team. |
| `talent_points` | double | Recruit-star talent points (name-matched to the 247 recruit record; 0-star default when unrated). |

**Example**

```python
from sportsdataverse.cfb import cfb_transfer_moves
moves = cfb_transfer_moves(2024)
moves.filter(pl.col("direction") == "in").group_by("team_id").len()
```

### create_drive_summary {#create_drive_summary}

`create_drive_summary(drives: list[dict] | dict, frame: polars.dataframe.frame.DataFrame, home_id: str | int, away_id: str | int, periods: set[int] | str | None = None) -> dict | None`

Build the StatBroadcast-style drive summary, chart, and long-play lists.

A drive belongs to the quarter it STARTED in. On a windowed build the
full drive sequence still provides context (running score, the previous
drive for OBTAINED and points-off-turnovers), but only in-window drives
are counted, charted, or listed. `largest_lead` and the time-leading /
time-tied split window too: the score state is read from the whole
regulation play sequence and then clipped to the window's clock intervals
(one per contiguous run of quarters, so a gapped set never charges the
quarter it skipped). Under `"ot"` the clock has no axis to integrate over
and only `largest_lead` ships.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `drives` | `list[dict]` |  | the ESPN drives grouping, in game order (`previous` plus the in-progress `current` drive, if any). |
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `periods` | `set[int] \| str \| None` | `None` | optional window -- a set of quarter numbers (e.g. `{1, 2}`) or the string `"ot"` (every period > 4). `None` = full game. |

**Returns**

`{"teams": {...}, "chart": [...], "scores": [...], "longPlays": {...}}` keyed by team id, or `None` when the inputs are unusable (no drives, empty frame, or an empty window).

**Example**

```python
summary = create_drive_summary(drives, game.plays_frame, "52", "61")
```

### create_situational_stats {#create_situational_stats}

`create_situational_stats(frame: polars.dataframe.frame.DataFrame, home_id: str | int, away_id: str | int, window_expr: polars.expr.expr.Expr | None = None) -> dict | None`

Build the situational team-stats block from a plays frame.

`two_minute` and `middle_8` are omitted from a windowed build: both name
a clock window of their own, so intersecting them with another window
describes neither (middle-8 inside Q1 is empty). Every other section,
`pace` and `non_garbage` included, is computed on the windowed slice and
ships with it.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `frame` | `pl.DataFrame` |  | the enriched plays frame from `CFBPlayProcess.run_processing_pipeline` (`plays_frame`). |
| `home_id` | `str \| int` |  | ESPN home team id. |
| `away_id` | `str \| int` |  | ESPN away team id. |
| `window_expr` | `Expr \| None` | `None` | optional polars filter expression windowing the windowable sections to that slice (e.g. `pl.col("period") == 3`). `None` = full game. |

**Returns**

`{"teams": {<team_id>: {<section>: ...}}}` or `None` when the frame is unusable or the window is empty.

**Example**

```python
stats = create_situational_stats(game.plays_frame, "52", "61")
```

### make_ratings_compute_results {#make_ratings_compute_results}

`make_ratings_compute_results(ratings: 'pl.DataFrame', *, era: 'str' = 'modern') -> 'ComputeResultsFn'`

Build a `cfb_simulations` `compute_results` closure from fixed ratings.

The returned closure implements the engine's results contract -- `(teams, games,
week_num, *, rng, **kwargs) -> {"teams", "games"}` -- filling every unplayed
`week == week_num` game's `result` with a sampled home margin
`round(Normal(exp_margin, margin_sd))`, where `exp_margin` is
`cfb_game_predict.predict_margin` on the two teams' `adj_net` (home-field
applied unless `neutral`). Unlike the default elo sampler the ratings are
**fixed**, so `teams` passes through unchanged (no elo update). Postseason games
(`game_type != "REG"`) re-break a sampled tie by win probability.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ratings` | `DataFrame` |  | A `cfb_ratings.cfb_ratings`-style frame with `team_id` and `adj_net`. Teams absent from it are treated as league-average (0.0). |
| `era` | `str` | `'modern'` | Era key into `cfb_prediction_constants.CFB_CONSTANTS`. |

**Returns**

A `compute_results` callable suitable for `cfb_simulations(..., compute_results=...)`.

**Example**

```python
import numpy as np, polars as pl
from sportsdataverse.cfb.cfb_season_odds import make_ratings_compute_results
cr = make_ratings_compute_results(pl.DataFrame({"team_id": ["A", "B"], "adj_net": [0.3, -0.3]}))
teams = pl.DataFrame({"sim": [1, 1], "team": ["A", "B"], "conference": ["X", "X"]})
games = pl.DataFrame({"sim": [1], "week": [1], "home_team": ["A"], "away_team": ["B"],
                      "neutral": [0], "result": [None]})
cr(teams, games, 1, rng=np.random.default_rng(0))["games"]
```

### play_type_family_expr {#play_type_family_expr}

`play_type_family_expr(source: 'str' = 'play_type_canonical') -> 'pl.Expr'`

Build the polars expression mapping a canonical type to its phase family.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source` | `str` | `'play_type_canonical'` | Name of the canonical play-type column. |

**Returns**

A `pl.Expr` aliased `play_type_family`; unmapped values yield null.

**Example**

```python
import polars as pl
from sportsdataverse.cfb import add_play_type_canonical

pbp = pl.DataFrame({"type.text": ["Rush", "Timeout"]})
add_play_type_canonical(pbp).filter(
    pl.col("play_type_family") != "administrative"
)
```
