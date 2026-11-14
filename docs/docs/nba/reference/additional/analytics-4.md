---
title: "NBA — additional Python functions — Analytics: nba_player–nba_raw"
sidebar_label: "Analytics: nba_player–nba_raw"
sidebar_position: 18
description: "NBA — additional Python functions — Analytics: nba_player–nba_raw — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_player–nba_raw

### nba_player_on_off {#nba_player_on_off}

`nba_player_on_off(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', min_poss: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Player on/off: each player's team with him on the floor and without him, per team-season.

The definitions are in `sportsdataverse._common.player_on_off`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `min_poss` | `int \| None` | `None` | The rank gate, offensive plus defensive possessions both on and off the floor. Defaults to 400 (NBA) or 200 (WNBA). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id)`: `games` on the floor and `team_games`, then for `_on` and `_off` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions); `<rating>_diff` (on minus off); `<m>_rank` / `<m>_pct` for the on and diff ratings (within the season, among players with at least `min_poss` both ways) and `<m>_n` for every rating. `poss_on + poss_off` is the team's possessions. Then his span with the team: `team_games_span` (the team's games from his first game for it, or the day after his last for an earlier team that season, to the day before his first for a later team, else the season's end, by the schedule's `game_date`), `span_first_date` / `span_last_date`, the off side over that span (`<total>_off_span` and `<rating>_off_span`, not ranked) and `<rating>_diff_span` (on minus it): for a player traded or signed mid-season, the on/off over his time with the team. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
| `games` | Int64 | Games the player was on the floor for the team. |
| `team_games` | Int64 | Games the team played in the season (with play-by-play and possessions); games well short of it marks a player who missed time or was with the team for part of the season. |
| `minutes_on` | Float64 | Game-clock minutes of the team's season with the player on the floor. |
| `poss_on` | Int64 | The team's offensive possessions with the player on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_on` | Int64 | The team's defensive possessions with the player on the floor. |
| `pts_on` | Int64 | The team's points with the player on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_on` | Int64 | The opponent's points with the player on the floor, credited as pts_on is. |
| `plus_minus_on` | Int64 | pts_on - opp_pts_on. |
| `off_rtg_on` | Float64 | Offensive rating with the player on the floor: pts_on per 100 poss_on; null with no possessions. |
| `def_rtg_on` | Float64 | Defensive rating with the player on the floor: opp_pts_on per 100 opp_poss_on; null with no possessions. |
| `net_rtg_on` | Float64 | Net rating with the player on the floor: off_rtg_on - def_rtg_on; null when either is null. |
| `minutes_off` | Float64 | Game-clock minutes of the team's season with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left). |
| `poss_off` | Int64 | The team's offensive possessions with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left) (a possession belongs to the five on the floor when it starts). |
| `opp_poss_off` | Int64 | The team's defensive possessions with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left). |
| `pts_off` | Int64 | The team's points with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left), each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_off` | Int64 | The opponent's points with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left), credited as pts_off is. |
| `plus_minus_off` | Int64 | pts_off - opp_pts_off. |
| `off_rtg_off` | Float64 | Offensive rating with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left): pts_off per 100 poss_off; null with no possessions. |
| `def_rtg_off` | Float64 | Defensive rating with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left): opp_pts_off per 100 opp_poss_off; null with no possessions. |
| `net_rtg_off` | Float64 | Net rating with the player off the floor: the rest of the team's season, the games he missed included (and, for a player traded or signed mid-season, the games before he joined or after he left): off_rtg_off - def_rtg_off; null when either is null. |
| `off_rtg_diff` | Float64 | On-off offensive rating: off_rtg_on - off_rtg_off; null when either is null. |
| `def_rtg_diff` | Float64 | On-off defensive rating: def_rtg_on - def_rtg_off; null when either is null. |
| `net_rtg_diff` | Float64 | On-off net rating: net_rtg_on - net_rtg_off; null when either is null. |
| `off_rtg_on_rank` | Float64 | Rank of off_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose off_rtg_on_n is at least 1,700 offensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_on_rank` | Float64 | Rank of def_rtg_on within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose def_rtg_on_n is at least 1,600 defensive possessions on the floor (the sample at which the rate was, on the fit seasons, half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_on_rank` | Float64 | Rank of net_rtg_on within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose net_rtg_on_n is at least 3,200 possessions on the floor (offense plus defense) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `off_rtg_diff_rank` | Float64 | Rank of off_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose off_rtg_diff_n is at least 2,400 offensive possessions on the thinner side (on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `def_rtg_diff_rank` | Float64 | Rank of def_rtg_diff within the season, league-wide (1 = best, lowest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose def_rtg_diff_n is at least 3,400 defensive possessions on the thinner side (on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `net_rtg_diff_rank` | Float64 | Rank of net_rtg_diff within the season, league-wide (1 = best, highest first, ties averaged), among players with at least min_poss possessions (offense plus defense) both on and off the floor (default 400 (NBA) or 200 (WNBA)), whose net_rtg_diff_n is at least 7,900 possessions on the thinner side (offense plus defense, on or off the floor) (the sample at which the rate is half signal, rate_gates); null under that gate or below 10 qualifiers. |
| `off_rtg_on_pct` | Float64 | Percentile of off_rtg_on among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `def_rtg_on_pct` | Float64 | Percentile of def_rtg_on among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `net_rtg_on_pct` | Float64 | Percentile of net_rtg_on among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `off_rtg_diff_pct` | Float64 | Percentile of off_rtg_diff among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `def_rtg_diff_pct` | Float64 | Percentile of def_rtg_diff among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `net_rtg_diff_pct` | Float64 | Percentile of net_rtg_diff among the same qualified players: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null where the rank is. |
| `off_rtg_on_n` | Int64 | Sample size of off_rtg_on: poss_on. |
| `def_rtg_on_n` | Int64 | Sample size of def_rtg_on: opp_poss_on. |
| `net_rtg_on_n` | Int64 | Sample size of net_rtg_on: poss_on + opp_poss_on. |
| `off_rtg_off_n` | Int64 | Sample size of off_rtg_off: poss_off. |
| `def_rtg_off_n` | Int64 | Sample size of def_rtg_off: opp_poss_off. |
| `net_rtg_off_n` | Int64 | Sample size of net_rtg_off: poss_off + opp_poss_off. |
| `off_rtg_diff_n` | Int64 | Sample size of off_rtg_diff: the smaller of off_rtg_on_n and off_rtg_off_n. |
| `def_rtg_diff_n` | Int64 | Sample size of def_rtg_diff: the smaller of def_rtg_on_n and def_rtg_off_n. |
| `net_rtg_diff_n` | Int64 | Sample size of net_rtg_diff: the smaller of net_rtg_on_n and net_rtg_off_n. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_player_on_off import nba_player_on_off
on_off = nba_player_on_off(2025)
on_off.sort("net_rtg_diff_rank").head()
```

### nba_player_play_styles {#nba_player_play_styles}

`nba_player_play_styles(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Offensive and defensive play-style archetypes per player-season (BB-D20).

The method is in `sportsdataverse._common.play_style` and the features in
`sportsdataverse.nba.nba_play_style`. A player qualifies on a side with at least a quarter
of the season's most minutes (`PLAYER_MIN_SHARE`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). Offense covers 2016+, defense 2018+; an uncovered side is null. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(player_id, season)` qualifying on either side: `player_id` (Int64), `season`, `player_name`, `team_id` (Int64), `min` (season minutes) and for `off_` and `def_` the `archetype`, `cluster`, `dist_to_center`, `second_archetype` and `margin`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | stats.nba.com player id. |
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `player_name` | String | Player's display name on stats.nba.com. |
| `team_id` | Int64 | stats.nba.com team id (the league-dashboard team; a traded player's last team). |
| `min` | Float64 | Regular-season minutes; the qualifier is a quarter of the season's most minutes. |
| `off_archetype` | String | Offensive play-style archetype: the label of the nearest frozen KMeans center to the player-season's within-season z-scored features; null when the player does not qualify on that side or the season is not covered. |
| `off_cluster` | Int64 | Index of the offensive archetype's center in the bundled artifact (0-based). |
| `off_dist_to_center` | Float64 | Euclidean distance (z-space) to the offensive archetype's center; small means a prototypical example. |
| `off_second_archetype` | String | The next-nearest offensive archetype, the style a hybrid leans toward. |
| `off_margin` | Float64 | Distance to the second center minus distance to the nearest; small means a hybrid between the two archetypes. |
| `def_archetype` | String | Defensive play-style archetype: the label of the nearest frozen KMeans center to the player-season's within-season z-scored features; null when the player does not qualify on that side or the season is not covered. |
| `def_cluster` | Int64 | Index of the defensive archetype's center in the bundled artifact (0-based). |
| `def_dist_to_center` | Float64 | Euclidean distance (z-space) to the defensive archetype's center; small means a prototypical example. |
| `def_second_archetype` | String | The next-nearest defensive archetype, the style a hybrid leans toward. |
| `def_margin` | Float64 | Distance to the second center minus distance to the nearest; small means a hybrid between the two archetypes. |

**Example**

```python
from sportsdataverse.nba.nba_play_style import nba_player_play_styles
styles = nba_player_play_styles(2025)
styles.group_by("off_archetype").len()
```

### nba_player_positions {#nba_player_positions}

`nba_player_positions(season: 'str', *, league_id: 'str' = '00', fetch: 'Optional[Callable[..., pl.DataFrame]]' = None) -> 'pl.DataFrame'`

Fetch league-wide listed positions for a season as numeric 1-5.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | NBA season, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | LeagueID (`"00"` NBA, `"10"` WNBA, `"20"` G-League). |
| `fetch` | `Optional[Callable[..., DataFrame]]` | `None` | Injectable `nba_stats_playerindex` replacement for offline tests. |

**Returns**

Frame with columns `player_id:Int64, position_num:Float64`.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Raises**

| Exception | Description |
|---|---|
| `ImportError` | If `curl_cffi` is not installed (required by the stats runtime). |

**Example**

```python
from sportsdataverse.nba import nba_player_positions
pos = nba_player_positions("2023-24")
print(pos.head())

# Offline / injectable fetch for testing

import polars as pl
stub = lambda **kw: pl.DataFrame({"person_id": [1], "position": ["PG"]})
pos = nba_player_positions("2023-24", fetch=stub)
```

### nba_player_props {#nba_player_props}

`nba_player_props(season: 'int', game_id: 'str', home_team_id: 'str', away_team_id: 'str', *, league_id: 'str' = '00', as_of_date: 'Union[dt.date, None]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Per-player expected prop lines + team pace projection for a matchup.

Loads the season's player box logs + team ratings, keeps only games played
strictly before `as_of_date` (both inputs), computes per-minute
`player_rates`, and projects each player's line onto their mean
minutes and the matchup's pace factor (`exp_poss / avg_pace`). Only the
two teams in the matchup are returned.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024`). |
| `game_id` | `str` |  | The game id. When `as_of_date` is `None` and this game is in the season's box logs or schedule, its date becomes the cut, so the default call never sees the game's own or any later line. |
| `home_team_id` | `str` |  | Home team id. |
| `away_team_id` | `str` |  | Away team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `as_of_date` | `Union[date, None]` | `None` | Only box lines and ratings games with `game_date < as_of_date` (strict) are used. `None` = the target game's date from the box logs, else from the schedule (one extra schedule read, only when the game is missing from the logs); a game in neither gets no cut and an `~sportsdataverse.errors.AsOfCutWarning`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player on either team: `player_id, team_id, stat_pts_exp, stat_reb_exp, stat_ast_exp, stat_fg3m_exp, pace_proj`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `team_id` | String | Unique team identifier. |
| `stat_pts_exp` | Float64 |  |
| `stat_reb_exp` | Float64 |  |
| `stat_ast_exp` | Float64 |  |
| `stat_fg3m_exp` | Float64 |  |
| `pace_proj` | Float64 |  |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | The box logs carry no `game_date` (the as-of cut cannot be applied). |

**Example**

```python
from sportsdataverse.nba.nba_player_props import nba_player_props
props = nba_player_props(2024, "401585828", "2", "6")

# Backtest as of a date

import datetime as dt
props = nba_player_props(2024, "401585828", "2", "6", as_of_date=dt.date(2024, 1, 15))
```

### nba_player_season_box_rates {#nba_player_season_box_rates}

`nba_player_season_box_rates(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

NBA per-36 points, rebounds and assists per player-season, ranked, with the season usage.

Over the regular season (a playoff or play-in game is measured against it), from ESPN's player box. `pts_per36` / `reb_per36` /
`ast_per36` are 36 x the total / minutes, each with `_n` (the minutes) and
`_rank` / `_pct` among the season's players whose minutes reach the rate's gate (the
minutes at which it is half signal on odd / even games). `usg_pct` is the season usage
(FGA + 0.44 FTA + TOV over the team's while on the floor), not ranked.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (2026 is 2025-26). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, athlete_id)` (the ESPN id): `team_id` (his last game's team), `games`, `minutes`, `pts` / `reb` / `ast`, `usg_pct` with `usg_pct_n`, and the three per-36 rates with `_n` / `_rank` / `_pct`. A season without a release returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `athlete_id` | Int64 | ESPN athlete id. |
| `team_id` | Int64 | ESPN team id of the player's last game of the season (a traded player has one row). |
| `games` | Int64 | Games played (the pros' regular season, every college game). |
| `minutes` | Float64 | Minutes played in those games. |
| `pts` | Int64 | Points in those games. |
| `reb` | Int64 | Rebounds in those games. |
| `ast` | Int64 | Assists in those games. |
| `usg_pct` | Float64 | Season usage: 100 x sum(uses) / sum(minutes x team uses / (team minutes / 5)), uses = FGA + 0.44 FTA + TOV; not ranked. |
| `usg_pct_n` | Int64 | Minutes of the games with a usage behind usg_pct. |
| `pts_per36` | Float64 | Points per 36 minutes: 36 x pts / minutes. |
| `pts_per36_n` | Int64 | Minutes behind pts_per36. |
| `reb_per36` | Float64 | Rebounds per 36 minutes: 36 x reb / minutes. |
| `reb_per36_n` | Int64 | Minutes behind reb_per36. |
| `ast_per36` | Float64 | Assists per 36 minutes: 36 x ast / minutes. |
| `ast_per36_n` | Int64 | Minutes behind ast_per36. |
| `pts_per36_rank` | Float64 | Rank of pts_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `pts_per36_pct` | Float64 | Percentile of pts_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `reb_per36_rank` | Float64 | Rank of reb_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `reb_per36_pct` | Float64 | Percentile of reb_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |
| `ast_per36_rank` | Float64 | Rank of ast_per36 in the league-season (1 best, average ties) among players whose minutes reach its gate (split-half r = 0.5); null under the gate or with fewer than 10 qualifiers. |
| `ast_per36_pct` | Float64 | Percentile of ast_per36 among the same qualifiers, 100 (n + 1 - rank) / (n + 1). |

**Example**

```python
from sportsdataverse.nba.nba_player_box_expected import nba_player_season_box_rates
rates = nba_player_season_box_rates(2026)
rates.sort("pts_per36_rank").select("athlete_id", "minutes", "pts_per36", "pts_per36_pct").head()
```

### nba_player_shot_zones {#nba_player_shot_zones}

`nba_player_shot_zones(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3.

One row per player, team and season over the regular season (a traded player has a row per
team). Zones and the unlocated attempts are as in the module docstring.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id)` (stats.nba.com ids, Int64) with `player_name`, `games`, `fga`, `fgm`, `unlocated_fga` and, for each zone `z` (`rim`, `mid`, `corner_3`, `above_break_3`), `{z}_fga`, `{z}_fgm`, `{z}_ast` (assisted makes), `{z}_fg_pct`, `{z}_freq` (share of the located attempts) and `{z}_ast_pct` (share of the makes assisted). The four `{z}_fga` plus `unlocated_fga` add up to `fga`. Every rate has `<m>_n` (its denominator), `<m>_rank` (1 = highest) and `<m>_pct` (the Weibull percentile), ranked within the season among players whose `<m>_n` reaches the rate's gate and null under ten players. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com player (person) id. |
| `player_name` | String | Player name as the shots release spells it. |
| `games` | Int64 | The player's regular-season games with at least one field-goal attempt for this team. |
| `fga` | Int64 | Field-goal attempts by the player (regular-season games). |
| `fgm` | Int64 | Field goals made by the player. |
| `unlocated_fga` | Int64 | Attempts by the player with no real location (the WNBA's 1997-2010 hoop-centre placeholders; 0 elsewhere): counted in fga, in no zone. |
| `rim_fga` | Int64 | Attempts by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_fgm` | Int64 | Makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `rim_ast` | Int64 | Assisted makes by the player at the rim (the restricted area: shot_distance under 4 ft). |
| `mid_fga` | Int64 | Attempts by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_fgm` | Int64 | Makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `mid_ast` | Int64 | Assisted makes by the player in the mid zone (every other two: the paint outside the restricted area and the mid-range). |
| `corner_3_fga` | Int64 | Attempts by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_fgm` | Int64 | Makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `corner_3_ast` | Int64 | Assisted makes by the player from the corner (a three from the baseline band, y_legacy <= 87.5, below the break). |
| `above_break_3_fga` | Int64 | Attempts by the player from above the break (every other three). |
| `above_break_3_fgm` | Int64 | Makes by the player from above the break (every other three). |
| `above_break_3_ast` | Int64 | Assisted makes by the player from above the break (every other three). |
| `rim_fg_pct` | Float64 | rim_fgm / rim_fga; null with no attempt. |
| `rim_freq` | Float64 | rim_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `rim_ast_pct` | Float64 | Share of rim_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `mid_fg_pct` | Float64 | mid_fgm / mid_fga; null with no attempt. |
| `mid_freq` | Float64 | mid_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `mid_ast_pct` | Float64 | Share of mid_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `corner_3_fg_pct` | Float64 | corner_3_fgm / corner_3_fga; null with no attempt. |
| `corner_3_freq` | Float64 | corner_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `corner_3_ast_pct` | Float64 | Share of corner_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `above_break_3_fg_pct` | Float64 | above_break_3_fgm / above_break_3_fga; null with no attempt. |
| `above_break_3_freq` | Float64 | above_break_3_fga / (fga - unlocated_fga): the zone's share of the located attempts (the four zones add up to 1); null with none. |
| `above_break_3_ast_pct` | Float64 | Share of above_break_3_fgm that were assisted, over the makes (every make's assist is in its description); null with none. |
| `rim_fg_pct_n` | Int64 | Sample size of rim_fg_pct: rim_fga. |
| `rim_freq_n` | Int64 | Sample size of rim_freq: located attempts (fga - unlocated_fga). |
| `rim_ast_pct_n` | Int64 | Sample size of rim_ast_pct: makes in the zone. |
| `mid_fg_pct_n` | Int64 | Sample size of mid_fg_pct: mid_fga. |
| `mid_freq_n` | Int64 | Sample size of mid_freq: located attempts (fga - unlocated_fga). |
| `mid_ast_pct_n` | Int64 | Sample size of mid_ast_pct: makes in the zone. |
| `corner_3_fg_pct_n` | Int64 | Sample size of corner_3_fg_pct: corner_3_fga. |
| `corner_3_freq_n` | Int64 | Sample size of corner_3_freq: located attempts (fga - unlocated_fga). |
| `corner_3_ast_pct_n` | Int64 | Sample size of corner_3_ast_pct: makes in the zone. |
| `above_break_3_fg_pct_n` | Int64 | Sample size of above_break_3_fg_pct: above_break_3_fga. |
| `above_break_3_freq_n` | Int64 | Sample size of above_break_3_freq: located attempts (fga - unlocated_fga). |
| `above_break_3_ast_pct_n` | Int64 | Sample size of above_break_3_ast_pct: makes in the zone. |
| `rim_fg_pct_rank` | Float64 | Rank of rim_fg_pct within the season (the highest ranks 1; ties average), among players whose rim_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_freq_rank` | Float64 | Rank of rim_freq within the season (the highest ranks 1; ties average), among players whose rim_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_ast_pct_rank` | Float64 | Rank of rim_ast_pct within the season (the highest ranks 1; ties average), among players whose rim_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_fg_pct_rank` | Float64 | Rank of mid_fg_pct within the season (the highest ranks 1; ties average), among players whose mid_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_freq_rank` | Float64 | Rank of mid_freq within the season (the highest ranks 1; ties average), among players whose mid_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `mid_ast_pct_rank` | Float64 | Rank of mid_ast_pct within the season (the highest ranks 1; ties average), among players whose mid_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_fg_pct_rank` | Float64 | Rank of corner_3_fg_pct within the season (the highest ranks 1; ties average), among players whose corner_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_freq_rank` | Float64 | Rank of corner_3_freq within the season (the highest ranks 1; ties average), among players whose corner_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `corner_3_ast_pct_rank` | Float64 | Rank of corner_3_ast_pct within the season (the highest ranks 1; ties average), among players whose corner_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_fg_pct_rank` | Float64 | Rank of above_break_3_fg_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_fg_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_freq_rank` | Float64 | Rank of above_break_3_freq within the season (the highest ranks 1; ties average), among players whose above_break_3_freq_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `above_break_3_ast_pct_rank` | Float64 | Rank of above_break_3_ast_pct within the season (the highest ranks 1; ties average), among players whose above_break_3_ast_pct_n reaches the rate's split-half gate (PLAYER_MIN_N); null under the gate, for a rate with no gate (never half signal), or with fewer than ten players ranked. |
| `rim_fg_pct_pct` | Float64 | Percentile of rim_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_freq_pct` | Float64 | Percentile of rim_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `rim_ast_pct_pct` | Float64 | Percentile of rim_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_fg_pct_pct` | Float64 | Percentile of mid_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_freq_pct` | Float64 | Percentile of mid_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `mid_ast_pct_pct` | Float64 | Percentile of mid_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_fg_pct_pct` | Float64 | Percentile of corner_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_freq_pct` | Float64 | Percentile of corner_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `corner_3_ast_pct_pct` | Float64 | Percentile of corner_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_fg_pct_pct` | Float64 | Percentile of above_break_3_fg_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_freq_pct` | Float64 | Percentile of above_break_3_freq within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `above_break_3_ast_pct_pct` | Float64 | Percentile of above_break_3_ast_pct within the season among the same ranked rows: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_shot_zone_profiles import nba_player_shot_zones
zones = nba_player_shot_zones(2025)
zones.sort("corner_3_fga", descending=True).select("player_name", "corner_3_fga", "corner_3_fg_pct").head()

# WNBA via ``league_id``

wnba = nba_player_shot_zones(2025, league_id="10")
```

### nba_player_war {#nba_player_war}

`nba_player_war(seasons: 'int | list[int]', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WAR and VORP per player-season, from our BPM, RAPM and BOBAN (regular season).

Each rating is anchored to the team (its players' possession-weighted mean rating over the
season is the team's net rating / 5; it is a season constraint, not an estimate for each
five-man lineup) and centred on the league, so 0 is a league-average player. `vorp` is points per
100 team possessions above a replacement player over one team's season of possessions
(Basketball-Reference's VORP scale); `war` is wins above replacement. A replacement-level
player has 0, and a season's `war` adds up to the league's wins minus those of a team of
replacement players. `rating` / `war` are the mean of the three sources; `rating_<s>`
/ `war_<s>` are each one. The replacement level and points per win are fitted per league
(`~sportsdataverse._common.player_war.REPLACEMENT_LEVEL`,
`~sportsdataverse._common.player_war.PTS_PER_WIN`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, player_id)` (stats.nba.com ids, Int64): `player_name`, `team_id` (the team he played most possessions for), `teams` (how many he played for), `min`, `poss` (on-court possessions, offensive plus defensive over 2), `rating` and `rating_bpm` / `rating_rapm` / `rating_boban` (anchored, per 100), `vorp`, `war` with `war_lo` / `war_hi` (a nominal 90% interval from a bootstrap over each team-season's games: the sampling uncertainty of the team side and the playing time) and `war_bpm` / `war_rapm` / `war_boban` (the source spread), with `_rank` (1 = most), `_pct` and `_n` (possessions) for `vorp` and `war` among every player of the season. Sorted by season and `war`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26; the WNBA season is its calendar year). |
| `player_id` | Int64 | stats.nba.com player id. |
| `player_name` | String | Player name, from the player impact release. |
| `team_id` | Int64 | stats.nba.com team id of the team he played the most possessions for. |
| `teams` | Int64 | How many teams he played for in the regular season. |
| `min` | Float64 | Regular-season minutes (the player impact release's). |
| `poss` | Float64 | On-court possessions, offensive plus defensive over 2, counted from the stats.nba.com possessions (every possession row, BB-D3's convention). The minutes basis of vorp and war. |
| `rating` | Float64 | Points per 100 possessions above a league-average player: the mean of rating_bpm, rating_rapm and rating_boban. |
| `rating_bpm` | Float64 | Our BPM per 100, team-anchored and centred: the release's bpm with its team constant repaired (minute-weighted team mean = net rating / 5, BPM 2.0's), minus the player's team mean, plus the team's net rating / 5, centred so the league's possession-weighted mean is 0. |
| `rating_rapm` | Float64 | Our prior-informed RAPM (adj_rapm) per 100, team-anchored and centred like rating_bpm. Only the net adj_rapm is read: the o_adj_rapm / d_adj_rapm split carries a +11 / -11 offset. |
| `rating_boban` | Float64 | Our BOBAN (boban_filtered_skill) per 100, team-anchored and centred like rating_bpm. |
| `vorp` | Float64 | Value over replacement player: (rating - R) * poss / the season's possessions per team, R the league's replacement level (REPLACEMENT_LEVEL; NBA -1.98, WNBA -3.18 per 100). Basketball-Reference's VORP scale: a season of possessions at R + 1 is 1. |
| `war` | Float64 | Wins above replacement: (rating - R) * poss / 100 / points per win (PTS_PER_WIN; NBA 31.56, WNBA 31.31). 0 at replacement level; a season's war adds up to the league's wins minus a team of replacement players' (.189 NBA 2024-25, .104 WNBA 2025). |
| `war_lo` | Float64 | Low end of the nominal 90% interval for war (5th percentile of 1000 bootstrap replicates, each team-season seeded on its own): each team-season's games resampled with replacement (a game's player possessions, team possessions and margin move together), then his possessions, the team net rating, the anchoring and the centring recomputed. The source ratings and the replacement level are held fixed, so this is the sampling uncertainty of the team side and the playing time; the war_bpm / war_rapm / war_boban spread is the source uncertainty. |
| `war_hi` | Float64 | High end of the nominal 90% interval for war (95th percentile of the same 1000 game-resampled replicates as war_lo). |
| `war_bpm` | Float64 | war from rating_bpm alone. Every source shares the league's replacement level, so war_bpm, war_rapm and war_boban add up to the same league total. |
| `war_rapm` | Float64 | war from rating_rapm alone. |
| `war_boban` | Float64 | war from rating_boban alone. BOBAN shrinks low-minute players toward average, so the replacement pool carries more war_boban than war_bpm (the replacement level is a box-score level). |
| `vorp_rank` | Float64 | Rank of vorp within the season among every player (1 = most; ties average); null under 10 players. |
| `war_rank` | Float64 | Rank of war within the season among every player (1 = most; ties average); null under 10 players. |
| `vorp_pct` | Float64 | Percentile of vorp within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `war_pct` | Float64 | Percentile of war within the season: 100 * (n + 1 - rank) / (n + 1), n the players ranked; null when the rank is null. |
| `vorp_n` | Int64 | Sample size of vorp: on-court possessions, rounded. |
| `war_n` | Int64 | Sample size of war: on-court possessions, rounded. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_player_war import nba_player_war
war = nba_player_war(2026)
war.select("player_name", "poss", "rating", "vorp", "war").head(10)

# WNBA via ``league_id``

wnba = nba_player_war(2025, league_id="10")
```

### nba_player_wowy {#nba_player_wowy}

`nba_player_wowy(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

WOWY pairs: a team's season with two teammates together, apart and both off, per pair.

The definitions are in `sportsdataverse._common.player_on_off`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id_a, player_id_b)` (`player_id_a` the smaller id) for every pair of the team's players: for `_both` (together), `_a_only`, `_b_only` and `_neither` the `minutes`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus` and `off_rtg` / `def_rtg` / `net_rtg`. The four splits' totals add up to the team's season; the ratings do not add. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id_a` | Int64 | The first player of the pair, the smaller id. |
| `player_id_b` | Int64 | The second player of the pair, the larger id. |
| `minutes_both` | Float64 | Game-clock minutes of the team's season with both A and B on the floor. |
| `poss_both` | Int64 | The team's offensive possessions with both A and B on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_both` | Int64 | The team's defensive possessions with both A and B on the floor. |
| `pts_both` | Int64 | The team's points with both A and B on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_both` | Int64 | The opponent's points with both A and B on the floor, credited as pts_both is. |
| `plus_minus_both` | Int64 | pts_both - opp_pts_both. |
| `off_rtg_both` | Float64 | Offensive rating with both A and B on the floor: pts_both per 100 poss_both; null with no possessions. |
| `def_rtg_both` | Float64 | Defensive rating with both A and B on the floor: opp_pts_both per 100 opp_poss_both; null with no possessions. |
| `net_rtg_both` | Float64 | Net rating with both A and B on the floor: off_rtg_both - def_rtg_both; null when either is null. |
| `minutes_a_only` | Float64 | Game-clock minutes of the team's season with A on the floor and B off. |
| `poss_a_only` | Int64 | The team's offensive possessions with A on the floor and B off (a possession belongs to the five on the floor when it starts). |
| `opp_poss_a_only` | Int64 | The team's defensive possessions with A on the floor and B off. |
| `pts_a_only` | Int64 | The team's points with A on the floor and B off, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_a_only` | Int64 | The opponent's points with A on the floor and B off, credited as pts_a_only is. |
| `plus_minus_a_only` | Int64 | pts_a_only - opp_pts_a_only. |
| `off_rtg_a_only` | Float64 | Offensive rating with A on the floor and B off: pts_a_only per 100 poss_a_only; null with no possessions. |
| `def_rtg_a_only` | Float64 | Defensive rating with A on the floor and B off: opp_pts_a_only per 100 opp_poss_a_only; null with no possessions. |
| `net_rtg_a_only` | Float64 | Net rating with A on the floor and B off: off_rtg_a_only - def_rtg_a_only; null when either is null. |
| `minutes_b_only` | Float64 | Game-clock minutes of the team's season with B on the floor and A off. |
| `poss_b_only` | Int64 | The team's offensive possessions with B on the floor and A off (a possession belongs to the five on the floor when it starts). |
| `opp_poss_b_only` | Int64 | The team's defensive possessions with B on the floor and A off. |
| `pts_b_only` | Int64 | The team's points with B on the floor and A off, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_b_only` | Int64 | The opponent's points with B on the floor and A off, credited as pts_b_only is. |
| `plus_minus_b_only` | Int64 | pts_b_only - opp_pts_b_only. |
| `off_rtg_b_only` | Float64 | Offensive rating with B on the floor and A off: pts_b_only per 100 poss_b_only; null with no possessions. |
| `def_rtg_b_only` | Float64 | Defensive rating with B on the floor and A off: opp_pts_b_only per 100 opp_poss_b_only; null with no possessions. |
| `net_rtg_b_only` | Float64 | Net rating with B on the floor and A off: off_rtg_b_only - def_rtg_b_only; null when either is null. |
| `minutes_neither` | Float64 | Game-clock minutes of the team's season with neither A nor B on the floor. |
| `poss_neither` | Int64 | The team's offensive possessions with neither A nor B on the floor (a possession belongs to the five on the floor when it starts). |
| `opp_poss_neither` | Int64 | The team's defensive possessions with neither A nor B on the floor. |
| `pts_neither` | Int64 | The team's points with neither A nor B on the floor, each scoring action credited to the lineup at that action (a free throw to the lineup at the last foul). |
| `opp_pts_neither` | Int64 | The opponent's points with neither A nor B on the floor, credited as pts_neither is. |
| `plus_minus_neither` | Int64 | pts_neither - opp_pts_neither. |
| `off_rtg_neither` | Float64 | Offensive rating with neither A nor B on the floor: pts_neither per 100 poss_neither; null with no possessions. |
| `def_rtg_neither` | Float64 | Defensive rating with neither A nor B on the floor: opp_pts_neither per 100 opp_poss_neither; null with no possessions. |
| `net_rtg_neither` | Float64 | Net rating with neither A nor B on the floor: off_rtg_neither - def_rtg_neither; null when either is null. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"` or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_player_on_off import nba_player_wowy
pairs = nba_player_wowy(2025)
pairs.filter(team_id=1610612738).head()
```

### nba_playtype_ratings {#nba_playtype_ratings}

`nba_playtype_ratings(season: 'str', *, league_id: 'str' = '00', off_team: 'Optional[pl.DataFrame]' = None, def_team: 'Optional[pl.DataFrame]' = None, schedule: 'Optional[pl.DataFrame]' = None, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Season play-type-adjusted offensive/defensive team ratings.

Fetches (or uses injected) Synergy offensive/defensive team frames plus the
league schedule, computes raw per-type efficiency
(`raw_playtype_efficiency`), opponent-adjusts it
(`adjust_playtype_efficiency`), then rolls up to one row per team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `str` |  | Season string, e.g. `"2023-24"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA (default), `"10"` WNBA, `"20"` G-League. |
| `off_team` | `Optional[DataFrame]` | `None` | Injected Synergy offensive team frame (bypasses the live fetch -- used for tests / oracle fixtures). |
| `def_team` | `Optional[DataFrame]` | `None` | Injected Synergy defensive team frame. |
| `schedule` | `Optional[DataFrame]` | `None` | Injected `team_id`/`opp_team_id` schedule frame. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id` (Int64), `adj_off`/`adj_def`/`adj_net` (Float64) roll-ups, plus per-type wide columns `adj_off_ppp_<playtype>`/`adj_def_ppp_<playtype>`/`off_freq_<playtype>` (Float64) for each play type present in the data. `adj_off = Σ_t off_freq_t · adj_off_ppp_t · 100` (symmetric for `adj_def` off `def_freq_t`); `adj_net = adj_off - adj_def`. Returns a zero-row frame with the base roll-up schema when the upstream fetch is empty (sparse-coverage leagues never raise).

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_playtype_ratings
r = nba_playtype_ratings("2023-24")
print(r.sort("adj_off", descending=True).head(10))

# Injected offline (oracle / test) path

r = nba_playtype_ratings("2023-24", off_team=off_df, def_team=def_df, schedule=sched_df)

# Pipeline next step

r.filter(pl.col("adj_net") > 0).sort("adj_net", descending=True)
```

### nba_possession_rate_rows {#nba_possession_rate_rows}

`nba_possession_rate_rows(possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season') -> 'pl.DataFrame'`

Map `nba_stats_possessions` rows onto the team-season-rates possession frame.

Keeps the games of `schedule` whose `season_type` matches, takes the home team from the
schedule, and sets each possession's start clock to the previous possession's end clock in the
period (the period length for the first one). Points come from the made-shot counts. A game
whose first period runs past 15 minutes is played in two 20-minute halves (the WNBA through
2005): its periods are 20 minutes, `late` starts with the 2nd half and period 3 is overtime.

`neutral_site` comes from `sportsdataverse.nba.nba_neutral_site`: the 2020 bubbles and
the curated games abroad and NBA Cup knockout games in Las Vegas, by stats `game_id`. The
stats schedule has no neutral-site or venue column, and ESPN's `neutral_site` misses most of
them (it marks the 2023-24 Cup semifinals False).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows. |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `season_type`, `home_team_id`). |
| `league_id` | `str` | `'00'` | `"00"` NBA (12-minute quarters) or `"10"` WNBA (10-minute quarters). |
| `season_type` | `str` | `'regular-season'` | The schedule `season_type` to keep: `"regular-season"` (the default, stats.nba.com's "Regular Season"; NBA Cup group and knockout games count, the Cup final does not), `"preseason"`, `"playoffs"` or `"play-in"`. |

**Returns**

One row per possession in `~sportsdataverse._common.team_season_rates.RATE_ROWS_SCHEMA`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends. |
| `game_id` | String | stats.nba.com game id. |
| `period` | Int64 | Period of the possession; overtimes follow the last regulation period. |
| `possession_number` | Int64 | Order of the possession in the game. |
| `late` | Boolean | True in the last regulation period (the 4th quarter; the 2nd half of a two-halves game) or an overtime: the periods the clutch window can fall in. |
| `start_clock` | Float64 | Seconds left in the period when the possession starts (when the previous possession ended). |
| `end_clock` | Float64 | Seconds left in the period when the possession ends. |
| `offense_team_id` | Int64 | stats.nba.com id of the team with the ball. |
| `defense_team_id` | Int64 | stats.nba.com id of the team on defense. |
| `offense_is_home` | Boolean | True when the offense is the (nominal) home team; it signs the running score for the clutch margin. |
| `neutral_site` | Boolean | Always False: the schedule names a home team for every NBA and WNBA game. |
| `points` | Int64 | Points the offense scored on the possession, from its made shots and free throws (2 * fg2m + 3 * fg3m + ftm). |
| `fga` | Float64 | Offense field-goal attempts on the possession. |
| `fgm` | Float64 | Offense field goals made on the possession. |
| `fg3m` | Float64 | Offense three-pointers made on the possession. |
| `fta` | Float64 | Offense free-throw attempts on the possession. |
| `oreb` | Float64 | Offense offensive rebounds on the possession. |
| `dreb` | Float64 | Defense defensive rebounds on the possession (the other half of the offensive-rebound chances). |
| `tov` | Float64 | Offense turnovers on the possession. |
| `ff_poss` | Float64 | Turnover-rate denominator: 1 per possession. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions, load_nba_stats_schedules
from sportsdataverse.nba.nba_team_season_rates import nba_possession_rate_rows
rows = nba_possession_rate_rows(load_nba_stats_possessions(2025), load_nba_stats_schedules(2025))
```

### nba_pregame_projections {#nba_pregame_projections}

`nba_pregame_projections(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rated pregame projection for every ESPN game of an NBA / WNBA season, as of each game's date.

The number the game page's pregame card prints: the closed form on each team's
`team_strength` before the game; see the module docstring for the model and the as-of
boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2026` is 2025-26); the WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per game of the ESPN schedule release (scheduled games included): `season`, `game_id`, `game_date`, `as_of_date` (the ratings use regular-season possessions dated strictly before it), `home_team_id` / `away_team_id` (ESPN ids, Int32), `neutral_site`, `stats_game_id`, each side's `rating` (`adj_net_rtg`, per 100), `rank` (`adj_net_rtg_rank`), `games` and `pace`, `hfa` (points, 0 at a neutral site), `exp_home_margin` (points) and `rated_home_prob`. The ratings and prediction are null for a game with no stats match or a side with no rating yet.

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season, named by the year it ends (2027 is 2026-27; the WNBA season is its calendar year). |
| `game_id` | Int32 | ESPN game id (Int32). |
| `game_date` | Date | Game date (ESPN schedule). |
| `as_of_date` | Date | The ratings use the completed games dated strictly before this date: the game's date, or the day after the last completed game for a game not yet reached. |
| `home_team_id` | Int32 | ESPN home team id (Int32). |
| `away_team_id` | Int32 | ESPN away team id (Int32). |
| `neutral_site` | Boolean | Neutral-site flag; a neutral game gets no home term. |
| `stats_game_id` | String | The stats.nba.com / stats.wnba.com game id matched on date and team names; null for a game with no match (All-Star games, the WNBA Commissioner's Cup final). |
| `home_rating` | Float64 | Home team's team_strength adj_net_rtg before the game: the possession ridge over the regular-season possessions dated before as_of_date, points per 100 over an average opponent on a neutral court. |
| `away_rating` | Float64 | Away team's adj_net_rtg before the game, on the same basis. |
| `home_rank` | Float64 | Home team's adj_net_rtg_rank as of the date (team_strength's rank: 1 = best, among teams with at least half the most games). |
| `away_rank` | Float64 | Away team's adj_net_rtg_rank as of the date. |
| `home_games` | Int64 | Home team's regular-season games behind its rating. |
| `away_games` | Int64 | Away team's games behind its rating. |
| `home_pace` | Float64 | Home team's team_strength pace (possessions per 48 minutes NBA, 40 WNBA) as of the date. |
| `away_pace` | Float64 | Away team's pace as of the date. |
| `hfa` | Float64 | Home-court term in points (the league's fitted hfa: 2.184 NBA, 1.184 WNBA), 0 at a neutral site. |
| `exp_home_margin` | Float64 | Expected home margin in points, the producer's closed form on the two ratings (positive favours the home team). |
| `rated_home_prob` | Float64 | Pregame home win probability, Phi(exp_home_margin / margin_sd) with the league's fitted margin_sd; null when a side has no rating. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_pregame_projection import nba_pregame_projections
out = nba_pregame_projections(2026)
out.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

# WNBA

nba_pregame_projections(2025, league_id="10")
```

### nba_rated_priors {#nba_rated_priors}

`nba_rated_priors(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Rated pregame home win probability for every ESPN game of a season, as of each game's date.

The in-game win-probability prior when a game has no betting line
(`~sportsdataverse.nba.nba_game_page.home_prior`'s `rated_home_prob`); see the module
docstring for the model and the as-of boundary.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2026` is 2025-26); the WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per game of the ESPN schedule release (scheduled games included): `season`, `game_id`, `game_date`, `home_team_id`, `away_team_id` (ESPN ids, Int32, as in the pbp release), `neutral_site`, `stats_game_id`, the two teams' adjusted net ratings before the game (`home_adj_net_rtg`, `away_adj_net_rtg`, per 100), the home term `hfa` (per 100, applied to both offenses), `exp_home_margin` (points) and `rated_home_prob`. The last six are null for a game with no stats.nba.com match (All-Star games and the WNBA Commissioner's Cup final).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `game_id` | Int64 | ESPN game id (Int32, as in the pbp release). |
| `game_date` | Date | Game date (ESPN schedule); the ratings use the games dated strictly before it. |
| `home_team_id` | Int64 | ESPN home team id. |
| `away_team_id` | Int64 | ESPN away team id. |
| `neutral_site` | Boolean | ESPN's neutral-site flag; a neutral game gets no home term. |
| `stats_game_id` | String | The stats.nba.com / stats.wnba.com game id matched on date and team names; null for a game with no match (All-Star games). |
| `home_adj_net_rtg` | Float64 | Home team's adjusted net rating before the game, points per 100 over an average opponent on a neutral court (the season sim's as-of possession ridge, shrunk toward last season's regressed rating). |
| `away_adj_net_rtg` | Float64 | Away team's adjusted net rating before the game, points per 100, on the same basis. |
| `hfa` | Float64 | The as-of ridge's home term, points per 100 for the home offense over a neutral court (applied to both offenses). |
| `exp_home_margin` | Float64 | Expected home margin in points: (home_adj_net_rtg - away_adj_net_rtg + 2 * hfa) * possessions per game / 100, without the home term at a neutral site. |
| `rated_home_prob` | Float64 | Rated pregame home win probability: Phi(exp_home_margin / margin_sd) with the season sim's probit scale. Pass it to score_win_prob / game_page_tables as rated_home_prob to make it the prior of a game with no line; without it that game gets home-court advantage (0.5 at a neutral site). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba.nba_rated_prior import nba_rated_priors
priors = nba_rated_priors(2026)
priors.select("game_id", "game_date", "exp_home_margin", "rated_home_prob").head()

# WNBA

nba_rated_priors(2025, league_id="10")
```

### nba_ratings_panel {#nba_ratings_panel}

`nba_ratings_panel(model: 'AnyModel', possessions: 'pl.DataFrame', dates: 'Optional[Sequence[datetime.date]]' = None, *, return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Player-ratings-through-date long panel: one row per (player_id, date).

Refit-per-checkpoint (v1; no warm-start incrementality) — each date's row
calls `ratings_as_of` independently, so the panel is leakage-free by
construction: a possession dated after a given checkpoint can never affect
that checkpoint's row, no matter what other dates are also being computed
or what future rows exist in `possessions`. Cost is a full refit per
checkpoint date; for a season's sparse RAPM-family design this is seconds
per date, not minutes — acceptable for a nightly/daily cadence but not for
live in-game updating (out of scope; see spec non-goals).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model` | `AnyModel` |  | A harness model conforming to `nba_model_validation.AnyModel`. |
| `possessions` | `DataFrame` |  | A possession+lineup frame with a `game_date` (`pl.Date`) column (as emitted by `compile_nba_season`). |
| `dates` | `Optional[Sequence[date]]` | `None` | Checkpoint dates to compute. `None` (default) uses every distinct `game_date` present in `possessions`, sorted ascending — a rating for every game day, matching what EPM/LEBRON publish nightly. Duplicates are deduped; input order does not matter (the output is always sorted by date). |
| `return_as_pandas` | `bool` | `False` | Return pandas instead of polars. |

**Returns**

Long frame with `RATINGS_PANEL_SCHEMA` columns (`player_id`, `date`, `o_rating`, `d_rating`, `rating`). Zero-row (that schema) when `possessions` is empty or no date yields any players.

No returns table is published for this function: no capture: its possessions input needs a game_date column, and neither nba_possessions nor the released possessions carry one.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `possessions` (non-empty) lacks a `game_date` column. |

**Example**

```python
import datetime
from sportsdataverse.nba.nba_model_validation import RidgeRapmModel
from sportsdataverse.nba.nba_ratings_panel import nba_ratings_panel

checkpoints = [datetime.date(2023, 11, 1), datetime.date(2023, 12, 1)]
panel = nba_ratings_panel(RidgeRapmModel(), season_poss, dates=checkpoints)
print(panel.filter(pl.col("player_id") == 201939).sort("date"))

# Every game day, no explicit grid

panel = nba_ratings_panel(RidgeRapmModel(), season_poss)
```

### nba_raw_store_season_frame {#nba_raw_store_season_frame}

`nba_raw_store_season_frame(endpoint: 'str', season: 'int', variant: 'Optional[str]' = None, *, result_set: 'Optional[str]' = None, raw_store_dir: 'RawStoreDir' = None) -> "Optional['pl.DataFrame']"`

Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame.

The per-game half of the store is served by the read-through per-game path;
this is the season-keyed half (`leaguegamelog`, `playerindex`,
`leaguedashplayerbiostats`, ...) that the `-raw` scraper writes as

* `{endpoint}/{season}/{variant}.json` -- parameterized captures, where
  *variant* is the slugified parameter sweep (e.g. `"regular-season"`,
  `"regular-season_totals"`), and
* `{endpoint}/{season}.json` -- unparameterized captures (e.g.
  `playerindex`), i.e. `variant=None`.

Roots may be a local checkout or an `http(s)://` base (the raw repo served
over raw.githubusercontent / a CDN), so a consumer -- notably the
`hoopR-nba-stats-data` model producer -- runs clone-free in CI against the
same committed tree the per-game compile reads.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `endpoint` | `str` |  | stats.nba.com endpoint slug (the store subdirectory). |
| `season` | `int` |  | Season **END** year (2024 = 2023-24) -- the key both halves of the store are filed under. |
| `variant` | `Optional[str]` | `None` | Capture variant slug, or `None` for an unparameterized capture. |
| `result_set` | `Optional[str]` | `None` | Named result set to return when the payload carries several; defaults to the first frame that parses. |
| `raw_store_dir` | `RawStoreDir` | `None` | Store root spec (dir or URL base) or per-endpoint mapping; `None` falls back to the env vars, `""` disables. |

**Returns**

The parsed `polars.DataFrame`, or `None` when the store is unset, the capture is absent, or the payload carries no usable frame -- so a caller can cleanly fall back to a live fetch.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `video_available` | Int64 | Video available. |

**Example**

```python
from sportsdataverse.nba import nba_raw_store_season_frame
base = "https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
logs = nba_raw_store_season_frame("leaguegamelog", 2024, "regular-season", raw_store_dir=base)

# Fall back to a live fetch when the capture is absent

frame = nba_raw_store_season_frame("playerindex", 2024, raw_store_dir=base)
positions = frame if frame is not None else nba_stats_playerindex(season="2023-24")
```
