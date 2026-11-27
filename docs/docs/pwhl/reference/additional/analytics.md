---
title: "PWHL — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 5
description: "PWHL — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — Analytics

### pwhl_game_story {#pwhl_game_story}

`pwhl_game_story(pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', skater_box: 'pl.DataFrame', xg_shots: 'pl.DataFrame | None' = None, *, return_as_pandas: 'bool' = False) -> 'dict[str, pl.DataFrame] | dict[str, pd.DataFrame]'`

How the winner won it, for one or many PWHL games: edges in goals, the moment, periods, runs, performers.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_pwhl_pbp` plays, one or many games, in the release's game order. |
| `schedule` | `DataFrame` |  | `load_pwhl_schedules` rows: the home and away team ids and the official final. |
| `skater_box` | `DataFrame` |  | `load_pwhl_skater_boxscores` rows (points, goals). |
| `xg_shots` | `DataFrame \| None` | `None` | `load_pwhl_xg_pbp` rows; without them the `xg` and `goaltending` edges are left out. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"summary", "edges", "moment", "periods", "runs", "performers"}`, the frames of `~sportsdataverse._common.game_story.GAME_STORY_SCHEMAS`. The goals-by-situation edges add up to the margin exactly. The moment's `rule` is `deciding_score` (no PWHL in-game win probability).

`summary`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's HockeyTech (PWHL) game id. |
| `home_team_id` | Int64 | Home team's HockeyTech (PWHL) team id. |
| `away_team_id` | Int64 | Away team's HockeyTech (PWHL) team id. |
| `home_score` | Int64 | Home team's official final score. |
| `away_score` | Int64 | Away team's official final score. |
| `winner_team_id` | Int64 | Winning team's id; null on a tie. |
| `loser_team_id` | Int64 | Losing team's id; null on a tie. |
| `margin` | Int64 | Winner's final score less the loser's; 0 on a tie. |
| `decided` | Boolean | True when the game has a winner (false on a tie). |
| `periods_agree` | Boolean | True when the period sums (from the play-by-play's running score) equal the official final; false flags a source whose plays end on another score. |
| `n_periods` | Int64 | Periods played, overtimes included. |
| `overtime` | Boolean | True when the game went past regulation. |

`edges`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's HockeyTech (PWHL) game id. |
| `winner_team_id` | Int64 | Winning team's id (null on a tie, where the edges read from the home side's view). |
| `loser_team_id` | Int64 | Losing team's id (null on a tie). |
| `edge` | String | Edge name. Accounting: even_strength_goals, power_play_goals, short_handed_goals, empty_net_goals, penalty_shot_goals, shootout; together the official margin exactly. Splits, with the xG shots: xg, goaltending (goals saved above expected). |
| `kind` | String | accounting for edges that split the game between them (their points_worth add up to the margin, exactly or nearly; see the function's docstring), split for overlapping cuts. |
| `unit` | String | Always goals: the scoring unit the edges are priced in. |
| `winner_value` | Float64 | Winner's value for the edge in the edge's own unit: points for a points edge, a rate for a rate edge. |
| `loser_value` | Float64 | Loser's value for the edge, in the same unit as winner_value. |
| `points_worth` | Float64 | The winner's edge in the sport's scoring unit: the winner's points from the edge less the loser's. Positive favours the winner. |
| `rank` | Int64 | Rank of the edge within the game by points_worth, 1 = largest; equal worths keep the edge list's order. |

`moment`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's HockeyTech (PWHL) game id. |
| `play_id` | Int64 | The play's position in the release (null for a shootout's closing row). |
| `order` | Int64 | The play's position in the game's play order. |
| `period` | Int64 | Period of the play. |
| `clock` | String | Game clock of the play as the source shows it. |
| `team_id` | Int64 | Team of the play: the event's team (hockey), the batting team (baseball), the scoring team (soccer). |
| `text` | String | Play description as the source writes it. |
| `wpa` | Float64 | Home win probability after the play less before it. |
| `winner_wpa` | Float64 | The same change from the winner's side (null on a tie). |
| `home_score` | Int64 | Home score after the play. |
| `away_score` | Int64 | Away score after the play. |
| `rule` | String | Always deciding_score: no in-game win-probability model ships for this league, so the moment is the winner's go-ahead-for-good score. |

`periods`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's HockeyTech (PWHL) game id. |
| `period` | Int64 | Period number, from 1; overtimes follow regulation. |
| `period_label` | String | P1-P3, then OT, OT2, ... and SO for a shootout. |
| `is_overtime` | Boolean | True for a period after regulation. |
| `home_pts` | Int64 | Home points in the period: its last running score less the previous period's. |
| `away_pts` | Int64 | Away points in the period. |
| `home_cum` | Int64 | Home running score at the end of the period. |
| `away_cum` | Int64 | Away running score at the end of the period. |

`runs`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's HockeyTech (PWHL) game id. |
| `team_id` | Int64 | Team that made the run. |
| `points` | Int64 | The side's largest unanswered run: points it scored in a row with no score by the other side (a score correction stays in the run and counts against its side). |
| `start_order` | Int64 | Play order of the run's first score. |
| `end_order` | Int64 | Play order of the run's last score. |
| `start_period` | Int64 | Period of the run's first score. |
| `end_period` | Int64 | Period of the run's last score. |
| `home_score_after` | Int64 | Home score at the end of the run. |
| `away_score_after` | Int64 | Away score at the end of the run. |

`performers`:

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | The game's HockeyTech (PWHL) game id. |
| `team_id` | Int64 | Team of the top performer. |
| `player_id` | Int64 | The side's top performer's player id. |
| `player_name` | String | Player's display name. |
| `metric` | String | Always points: goals plus assists, the metric the performer is chosen by. |
| `value` | Float64 | The player's points, goals plus assists (ties go to goals). |

**Example**

```python
import polars as pl
from sportsdataverse.pwhl import (
    load_pwhl_pbp, load_pwhl_schedules, load_pwhl_skater_boxscores, load_pwhl_xg_pbp,
)
from sportsdataverse.pwhl.pwhl_game_story import pwhl_game_story
story = pwhl_game_story(
    load_pwhl_pbp([2026]), load_pwhl_schedules([2026]), load_pwhl_skater_boxscores([2026]),
    load_pwhl_xg_pbp([2026]),
)
story["edges"].filter(pl.col("rank") == 1)

# Every shootout

story["periods"].filter(pl.col("period_label") == "SO")
```

### pwhl_game_total {#pwhl_game_total}

`pwhl_game_total(games: 'Any', ratings: 'Any', *, league: 'str' = 'pwhl', **kwargs: 'Any') -> 'Any'`

PWHL per-game expected total goals (re-export of the expected-goals helper).

Delegates to `sportsdataverse.nhl.nhl_player_props.nhl_game_total`
with `league="pwhl"` defaulted.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `Any` |  | a schedule-shaped frame. |
| `ratings` | `Any` |  | a `pwhl_team_ratings`-shaped frame. |
| `league` | `str` | `'pwhl'` | league key (defaults to `"pwhl"`). |

**Returns**

The NHL core's `game_id`/`exp_total` frame, computed with PWHL constants.

**Example**

```python
from sportsdataverse.pwhl.pwhl_player_props import pwhl_game_total
totals = pwhl_game_total(games, ratings)
```

### pwhl_in_game_win_prob {#pwhl_in_game_win_prob}

`pwhl_in_game_win_prob(pbp: 'Any', pregame_home_prob: 'float', *, league: 'str' = 'pwhl', **kwargs: 'Any') -> 'Any'`

PWHL per-play live home win probability from the bundled in-game model.

Delegates to `sportsdataverse.nhl.nhl_market.nhl_in_game_win_prob`
with `league="pwhl"` defaulted. NOTE: requires a committed
`pwhl_in_game_wp` artifact, deferred until PWHL data lands (see module
docstring); calling it before then raises a clear `FileNotFoundError`
from the artifact loader, not a silent bad result.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `Any` |  | a play-by-play frame shaped like `load_nhl_pbp_full`. |
| `pregame_home_prob` | `float` |  | the pregame home win probability anchor. |
| `league` | `str` | `'pwhl'` | league key (defaults to `"pwhl"`). |

**Returns**

The NHL core's per-play `home_win_prob` frame.

**Example**

```python
from sportsdataverse.pwhl.pwhl_market import pwhl_in_game_win_prob
wp = pwhl_in_game_win_prob(pbp, pregame_home_prob=0.5)
```

### pwhl_player_props {#pwhl_player_props}

`pwhl_player_props(seasons: 'Any', *, league: 'str' = 'pwhl', **kwargs: 'Any') -> 'Any'`

PWHL empirical-Bayes shots/points player-prop projections.

Delegates to `sportsdataverse.nhl.nhl_player_props.nhl_player_props`
with `league="pwhl"` defaulted and the PWHL box loader and ratings bound
(see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `Any` |  | an int or iterable of seasons. |
| `league` | `str` | `'pwhl'` | league key (defaults to `"pwhl"`). |

**Returns**

The NHL core's per-(player, game, stat) projection frame.

**Example**

```python
from sportsdataverse.pwhl.pwhl_player_props import pwhl_player_props
props = pwhl_player_props(2025)
```

### pwhl_predict_games {#pwhl_predict_games}

`pwhl_predict_games(games: 'Any', ratings: 'Any', *, league: 'str' = 'pwhl', **kwargs: 'Any') -> 'Any'`

PWHL vectorized pregame margin/win-prob/total (+ market edge).

Delegates to `sportsdataverse.nhl.nhl_market.nhl_predict_games` with
`league="pwhl"` defaulted.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `Any` |  | a schedule-shaped frame (`game_id`, `home_team`, `away_team`, `neutral_site`). |
| `ratings` | `Any` |  | a `pwhl_team_ratings`-shaped frame. |
| `league` | `str` | `'pwhl'` | league key (defaults to `"pwhl"`). |

**Returns**

The NHL core's per-game prediction frame, computed with PWHL constants.

**Example**

```python
from sportsdataverse.pwhl.pwhl_market import pwhl_predict_games
preds = pwhl_predict_games(games, ratings)
```
