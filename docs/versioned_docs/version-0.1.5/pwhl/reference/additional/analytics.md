---
title: "PWHL — additional Python functions — Analytics"
sidebar_label: "Analytics"
sidebar_position: 4
description: "PWHL — additional Python functions — Analytics — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — Analytics

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
with `league="pwhl"` defaulted.

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
props = pwhl_player_props(2024)
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
