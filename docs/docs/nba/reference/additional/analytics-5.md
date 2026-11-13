---
title: "NBA — additional Python functions — Analytics: nba_season–nba_team"
sidebar_label: "Analytics: nba_season–nba_team"
sidebar_position: 19
description: "NBA — additional Python functions — Analytics: nba_season–nba_team — function reference in sdv-py, the SportsDataverse Python package."
---
# NBA — additional Python functions — Analytics: nba_season–nba_team

### nba_season_sim {#nba_season_sim}

`nba_season_sim(season: 'int', *, as_of_date: 'dt.date | None' = None, league_id: 'str' = '00', n_sims: 'int' = 10000, seed: 'int' = 20261103, chances: 'list[float] | None' = None, cup_schedule: 'pl.DataFrame | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Season sim as of a date: projected W-L, division, seed, play-in, playoff and lottery odds.

Reads the `{nba,wnba}_stats_schedules` and `_possessions` releases for the season and the
one before, fits the game model on the regular-season games played before `as_of_date`,
shrunk toward last season's final ratings (`as_of_game_model`, `preseason_prior`),
and simulates the rest (`simulate_pro_season`). Before the first game the ratings are
the prior alone. The rules applied to each simulated season
are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | The season, named by the year it ends (`2027` is 2026-27); the WNBA season is its calendar year. |
| `as_of_date` | `date \| None` | `None` | Games on or after this date are simulated even when the release has their score (a backtest). None: every game with a score counts as played. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `n_sims` | `int` | `10000` | Simulated seasons (Monte Carlo error: see `SIM_SCHEMA`'s `_se` columns). |
| `seed` | `int` | `20261103` | Seeds the generator; the same release data and seed give the same output. |
| `chances` | `list[float] \| None` | `None` | Lottery chances per slot, worst record first, one per lottery team; overrides the published table (needed for the 2027 WNBA lottery's seven teams, not published yet). |
| `cup_schedule` | `DataFrame \| None` | `None` | NBA: stats.nba.com's schedule for the season (`~sportsdataverse.nba.nba_stats.nba_stats_scheduleleaguev2`), which marks the Cup games. Read only while the release lists TBD Cup games; None fetches it then (a residential connection), and a failed fetch falls back to league-average opponents for the Cup slots with a warning. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team (stats.nba.com `team_id`, Int64), sorted by conference and mean seed: * `wins` / `losses` so far, `games_remaining`, and the as-of `adj_net_rtg` with its posterior sd `adj_net_rtg_sd` (capped so two teams' shift carries at most 3/4 of a game's variance; it binds only near the prior, before many games are played). * `proj_wins` / `proj_losses` (means), `proj_wins_sd`, the 10th and 90th percentiles, and `proj_wins_se` (the Monte Carlo error of the mean). * `p_division` (NBA), `p_seed_1` .. `p_seed_15` (the regular-season finish in the conference, NBA, or the league, WNBA) and `mean_seed`. * `p_top4` (first-round home court), `p_top6` (NBA, straight to the playoffs), `p_play_in` (NBA, seeds 7-10), `p_playoffs` (NBA: top 6 or through the play-in; WNBA: top 8) and its Monte Carlo `p_playoffs_se`. * `p_lottery`, `p_pick_1`, `p_drawn_pick` (a pick from the drawing: the NBA's top four, the WNBA's top two) and `mean_pick` (given a lottery finish). The pick columns are null when the league has no published chances for that many lottery teams. * `n_sims`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `as_of_date` | Date | The as-of date: games on or after it are simulated, the ones before it are played. Null when every game with a score counts as played. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West) from the 2004-05 alignment; "WNBA" for every WNBA team (one table). |
| `division` | String | NBA division (Atlantic, Central, Southeast, Northwest, Pacific, Southwest); "WNBA" for every WNBA team. |
| `wins` | Int64 | Regular-season wins in the games already played. |
| `losses` | Int64 | Regular-season losses in the games already played. |
| `games_remaining` | Int64 | Regular-season games on the schedule still to play (simulated). |
| `adj_net_rtg` | Float64 | As-of adjusted net rating from the possession ridge at the sim's penalty: points per 100 over an average opponent on a neutral court. |
| `adj_net_rtg_sd` | Float64 | Posterior sd of adj_net_rtg (points per 100) from the ridge, capped so two teams' shift carries at most 3/4 of a game's variance; the sd of the per-season rating shift each simulated season draws. |
| `proj_wins` | Float64 | Mean final regular-season wins over the simulated seasons (played wins plus simulated ones). |
| `proj_losses` | Float64 | Mean final regular-season losses over the simulated seasons. |
| `proj_wins_sd` | Float64 | Standard deviation of the final wins over the simulated seasons. |
| `proj_wins_p10` | Float64 | 10th percentile of the final wins over the simulated seasons. |
| `proj_wins_p90` | Float64 | 90th percentile of the final wins over the simulated seasons. |
| `proj_wins_se` | Float64 | Monte Carlo standard error of proj_wins: proj_wins_sd / sqrt(n_sims). |
| `p_division` | Float64 | Share of simulated seasons in which the team wins its division under the NBA tiebreakers; null for the WNBA. |
| `p_seed_1` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 1: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_2` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 2: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_3` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 3: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_4` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 4: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_5` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 5: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_6` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 6: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_7` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 7: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_8` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 8: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_9` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 9: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_10` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 10: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_11` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 11: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_12` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 12: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_13` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 13: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_14` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 14: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `p_seed_15` | Float64 | Share of simulated seasons in which the team finishes the regular season seeded 15: in its conference for the NBA, in the league for the WNBA (before the NBA play-in). 0 for a seed past the number of teams. |
| `mean_seed` | Float64 | Mean regular-season seed over the simulated seasons (1 = best). |
| `p_top4` | Float64 | Share of simulated seasons with a top-4 seed: first-round home court. |
| `p_top6` | Float64 | NBA: share of simulated seasons with a top-6 conference seed, straight to the playoffs; null for the WNBA. |
| `p_play_in` | Float64 | NBA: share of simulated seasons with a conference seed of 7 to 10, the play-in; null for the WNBA. |
| `p_playoffs` | Float64 | Share of simulated seasons in which the team makes the playoffs: NBA top 6 or through the simulated play-in; WNBA top 8. |
| `p_playoffs_se` | Float64 | Monte Carlo standard error of p_playoffs: sqrt(p (1 - p) / n_sims). |
| `p_lottery` | Float64 | Share of simulated seasons in which the team misses the playoffs and enters the draft lottery. |
| `p_pick_1` | Float64 | Share of simulated seasons in which the team's own lottery slot draws the first pick; null when the league has no published chances for that many lottery teams. |
| `p_drawn_pick` | Float64 | Share of simulated seasons in which the team's slot gets a drawn pick (NBA top 4, WNBA top 2); null as p_pick_1. |
| `mean_pick` | Float64 | Mean draft pick over the simulated seasons in which the team is in the lottery; null when it never is, or as p_pick_1. |
| `n_sims` | Int64 | Number of simulated seasons behind every share on the row. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_season_sim import nba_season_sim
sim = nba_season_sim(2026, as_of_date=dt.date(2026, 2, 13))
sim.select("team_id", "wins", "proj_wins", "p_playoffs", "p_pick_1").head()

# WNBA via ``league_id``

nba_season_sim(2025, as_of_date=dt.date(2025, 7, 1), league_id="10")
```

### nba_season_standings {#nba_season_standings}

`nba_season_standings(games: 'pl.DataFrame', *, league_id: 'str' = '00', seed: 'int' = 0, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Standings with the league's tiebreakers from completed regular-season games.

NBA: conference seeds and division ranks under the NBA tie-break procedure (division leaders
first, postseason-eligible = a top-10 conference record). WNBA: one league table under the
WNBA procedure. The rules and their sources are in `sportsdataverse.nba.nba_season_rules`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `games` | `DataFrame` |  | One row per game with `season`, `home_team_id`, `away_team_id` (Int64 stats.nba.com ids), `home_pts` and `away_pts`; rows without both scores are not played yet and are skipped. A `load_nba_stats_schedules` frame can be passed as is: with a `season_type` column only the `"regular-season"` rows count (the NBA Cup final, preseason, play-in and playoffs do not). |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `seed` | `int` | `0` | Seeds the drawing that ends a tie no criterion breaks. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)` that played: `conference` / `division` (NBA; the WNBA rows say `"WNBA"`), `games`, `wins`, `losses`, `win_pct`, `point_diff`, `division_rank` (1 = the division winner), `division_leader`, `seed` (rank in the conference for the NBA, in the league for the WNBA, 1 = best) and `games_back` (behind the seed-1 team). Sorted by season, conference and seed.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26); the WNBA season is its calendar year. |
| `team_id` | Int64 | stats.nba.com team id (Int64). |
| `conference` | String | NBA conference (East / West); "WNBA" for every WNBA team. |
| `division` | String | NBA division; "WNBA" for every WNBA team. |
| `games` | Int64 | Regular-season games played. |
| `wins` | Int64 | Regular-season wins. |
| `losses` | Int64 | Regular-season losses. |
| `win_pct` | Float64 | Regular-season winning percentage, wins divided by games played; the first key of the seeding. |
| `point_diff` | Int64 | Points scored minus points allowed over the regular-season games. |
| `division_rank` | Int64 | Rank within the division under the NBA tiebreakers, 1 = the division winner (the WNBA rows rank the league). |
| `division_leader` | Boolean | True for the NBA division winner after the division tiebreak; False for every WNBA row. |
| `seed` | Int64 | Seed under the league's tiebreakers: rank in the conference (NBA) or the league (WNBA), 1 = best. |
| `games_back` | Float64 | Games behind the seed-1 team of the conference (NBA) or league (WNBA): ((leader wins - wins) + (losses - leader losses)) / 2. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or an NBA team id is unknown. |
| `TypeError` | If the team-id columns are not Int64. |

**Example**

```python
from sportsdataverse.nba.nba_loaders import load_nba_stats_schedules
from sportsdataverse.nba.nba_season_rules import nba_season_standings
nba_season_standings(load_nba_stats_schedules(2025)).filter(pl.col("seed") <= 10)
```

### nba_shot_value {#nba_shot_value}

`nba_shot_value(player_ids: "'list[int]'", season: 'str', *, league_id: 'str' = '00', include_context: 'bool' = False, return_as_pandas: 'bool' = False) -> "'dict[str, Union[pl.DataFrame, pd.DataFrame]]'"`

One-call shot-value spine: fetch, score, and run all five models.

Fetches each player's `shotchartdetail`, scores per-shot expected points
from the free `LeagueAverages` zone table, and returns the scored shots
plus shooter talent, selection quality, and zone-value maps (and the
defender/shot-clock context tables when `include_context=True`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `player_ids` | `list[int]` |  | Player ids to fetch. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `include_context` | `bool` | `False` | Also fetch + return the `playerdashptshots` defender/shot-clock context tables, once per player and team his fetched shots came from. |
| `return_as_pandas` | `bool` | `False` | Return pandas frames instead of polars. |

**Returns**

`{"shots", "talent", "selection", "zones"}` (plus `"context"` when requested). An empty fetch returns a dict of zero-row frames.

**Example**

```python
from sportsdataverse.nba import nba_shot_value
out = nba_shot_value([201939], "2022-23")
out["talent"].head()
```

### nba_shot_value_lineups {#nba_shot_value_lineups}

`nba_shot_value_lineups(group_id: 'str', season: 'str', *, team_id: 'int', league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> "'Union[pl.DataFrame, pd.DataFrame]'"`

Scored per-shot frame for one 5-man lineup (`shotchartlineupdetail`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `group_id` | `str` |  | The 5-man lineup group id (dash-joined player ids); kept `Utf8`. |
| `season` | `str` |  | Season string, e.g. `"2022-23"`. |
| `team_id` | `int` |  | The lineup's team id. |
| `league_id` | `str` | `'00'` | `"00"` NBA, `"10"` WNBA, `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

The lineup's shots scored by `score_shot_xpoints` (with `xpoints`). Empty fetch returns the augmented zero-row schema.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba import nba_shot_value_lineups
df = nba_shot_value_lineups("201939-202691-...", "2022-23", team_id=1610612744)
```

### nba_situation_rows {#nba_situation_rows}

`nba_situation_rows(possessions: 'pl.DataFrame', pbp: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season') -> 'pl.DataFrame'`

Map the stats.nba.com releases onto the situational-splits possession frame.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows, every possession of each game (BB-D7 reads the previous possession and the starters). |
| `pbp` | `DataFrame` |  | `load_nba_stats_pbp` / `load_wnba_stats_pbp` rows of the same games (`game_id`, `order_index`, `event_type`, `seconds_remaining`, `score_home`, `score_away`). |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | The schedule `season_type` to keep (as `~sportsdataverse.nba.nba_team_season_rates.nba_possession_rate_rows`). |

**Returns**

One row per possession in `~sportsdataverse._common.situational_splits.situation_row_schema` (Int64 player ids).

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
| `reg_periods` | Int64 | Regulation periods of the game: 4 quarters, or 2 halves. |
| `is_transition` | Boolean | The possession is transition (BB-D7's flag for the pros, the release's own for college). |
| `is_garbage_time` | Boolean | The possession is in garbage time (bigballR's rule; BB-D7 for the pros, the release's flag for college). |
| `is_second_chance` | Boolean | The possession had an offensive rebound; null in college (the NCAA possessions carry no rebound counts). |
| `off_1` | Int64 | Offense player 1 on the floor as the possession starts. |
| `off_2` | Int64 | Offense player 2 on the floor as the possession starts. |
| `off_3` | Int64 | Offense player 3 on the floor as the possession starts. |
| `off_4` | Int64 | Offense player 4 on the floor as the possession starts. |
| `off_5` | Int64 | Offense player 5 on the floor as the possession starts. |
| `def_1` | Int64 | Defense player 1 on the floor as the possession starts. |
| `def_2` | Int64 | Defense player 2 on the floor as the possession starts. |
| `def_3` | Int64 | Defense player 3 on the floor as the possession starts. |
| `def_4` | Int64 | Defense player 4 on the floor as the possession starts. |
| `def_5` | Int64 | Defense player 5 on the floor as the possession starts. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` or `season_type` is unknown, or `pbp` misses the events of a possession of a `season_type` game (the other season types' games are not read). |

**Example**

```python
from sportsdataverse.nba import nba_loaders as L
from sportsdataverse.nba.nba_situational import nba_situation_rows
rows = nba_situation_rows(L.load_nba_stats_possessions(2025), L.load_nba_stats_pbp(2025),
                          L.load_nba_stats_schedules(2025))
```

### nba_situational_lineup {#nba_situational_lineup}

`nba_situational_lineup(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Five-man unit splits by situation: the team splits over the possessions each unit started.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`. |
| `top_n` | `int \| None` | `None` | As `nba_situational_team`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, lineup_id, family, split)`: `lineup_id` is the five player ids sorted and joined with `-` (BB-D4's id), `player_1..5` the same ids, then the columns of `nba_situational_team`. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `lineup_id` | String | The five player ids of the unit on the floor as the possession starts, sorted and joined with '-' (BB-D4's id). |
| `player_1` | Int64 | Player 1 of the unit, in lineup_id order. |
| `player_2` | Int64 | Player 2 of the unit, in lineup_id order. |
| `player_3` | Int64 | Player 3 of the unit, in lineup_id order. |
| `player_4` | Int64 | Player 4 of the unit, in lineup_id order. |
| `player_5` | Int64 | Player 5 of the unit, in lineup_id order. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id`, `season_type` or `top_n` is invalid. |

**Example**

```python
from sportsdataverse.nba.nba_situational import nba_situational_lineup
units = nba_situational_lineup(2026).filter(split="transition")
units.sort("poss", descending=True).head()
```

### nba_situational_player {#nba_situational_player}

`nba_situational_player(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Player on-floor splits by situation: the team splits over the possessions each player started on the floor.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`. |
| `top_n` | `int \| None` | `None` | As `nba_situational_team`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, player_id, family, split)`, the columns of `nba_situational_team`: the team's possessions, points and ratings with the player on the floor as the possession starts. `player_id` is the stats.nba.com person id (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `player_id` | Int64 | stats.nba.com person id. A possession counts for the five on the floor as it starts, with all its points. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id`, `season_type` or `top_n` is invalid. |

**Example**

```python
from sportsdataverse.nba.nba_situational import nba_situational_player
clutch = nba_situational_player(2026).filter(split="clutch")
clutch.filter(clutch["net_rtg_n"] >= 200).sort("net_rtg", descending=True).head()
```

### nba_situational_team {#nba_situational_team}

`nba_situational_team(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Team splits by situation: clutch, period, half, garbage time, score, venue, opponent, play type, chance.

The families and their splits are in `sportsdataverse._common.situational_splits`; within
a family the splits add up to the `all` row.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `top_n` | `int \| None` | `None` | How many of the season's best teams (BB-D19 regular-season `adj_net_rtg`) are `vs_top`. Default 10 (NBA) or 4 (WNBA). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, family, split)`: `games`, `minutes` / `seconds`, `poss` / `opp_poss`, `pts` / `opp_pts`, `plus_minus`, `off_rtg` / `def_rtg` / `net_rtg` (per 100 possessions) and their `_n`. Ids are the stats.nba.com ids (Int64). Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | Games with at least one possession in the split (does not add up across a family). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id`, `season_type` or `top_n` is invalid. |

**Example**

```python
from sportsdataverse.nba.nba_situational import nba_situational_team
splits = nba_situational_team(2026)
splits.filter(split="clutch").sort("net_rtg", descending=True).head()
```

### nba_situational_team_games {#nba_situational_team_games}

`nba_situational_team_games(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', top_n: 'int | None' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Team splits by situation, one game at a time: `nba_situational_team` per game.

The same possessions and labels, grouped by game too, so a team-season's rows of a split add
up to its `nba_situational_team` row (`games`, `seconds`, `poss`, `opp_poss`,
`pts`, `opp_pts`). The `opponent` family keeps the season's top `top_n`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`. |
| `top_n` | `int \| None` | `None` | As `nba_situational_team`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, game_id, family, split)`, the columns of `nba_situational_team` plus `game_id` (the stats.nba.com game id); `games` is 1. A split the team did not play in the game has no row. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `game_id` | String | stats.nba.com game id. |
| `family` | String | The split family: all, clutch, period, half, garbage, score, venue, opponent, play_type or chance; within a family the splits add up to the all row. |
| `split` | String | The split: all; clutch / not_clutch; p1..p4 / ot; h1 / h2 / ot; garbage_time / not_garbage_time; leading / tied / trailing; home / away / neutral; vs_top / vs_rest; transition / half_court; second_chance / first_chance. |
| `games` | Int64 | 1: the row is one game (summed over a team-season's rows it is the season row's games). |
| `minutes` | Float64 | Game-clock minutes of the split's possessions (seconds / 60). |
| `seconds` | Float64 | Game-clock seconds of the split's possessions: each possession's start clock minus its end clock. |
| `poss` | Int64 | Offensive possessions in the split. |
| `opp_poss` | Int64 | Defensive possessions in the split. |
| `pts` | Int64 | Points scored on the offensive possessions. |
| `opp_pts` | Int64 | Points allowed on the defensive possessions. |
| `plus_minus` | Int64 | Plus-minus over the split: pts - opp_pts. |
| `off_rtg` | Float64 | Offensive rating: pts per 100 poss; null with no offensive possessions. |
| `def_rtg` | Float64 | Defensive rating: opp_pts per 100 opp_poss; null with no defensive possessions. |
| `net_rtg` | Float64 | Net rating: off_rtg - def_rtg; null when either is null. |
| `off_rtg_n` | Int64 | Sample size of off_rtg: poss. |
| `def_rtg_n` | Int64 | Sample size of def_rtg: opp_poss. |
| `net_rtg_n` | Int64 | Sample size of net_rtg: poss + opp_poss. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id`, `season_type` or `top_n` is invalid. |

**Example**

```python
from sportsdataverse.nba.nba_situational import nba_situational_team_games
game = nba_situational_team_games(2026).filter(game_id="0022500001")
game.filter(family="score").select("team_id", "split", "poss", "pts", "opp_pts")
```

### nba_stint_rows {#nba_stint_rows}

`nba_stint_rows(pbp: 'pl.DataFrame', game_lineups: 'pl.DataFrame', possessions: 'pl.DataFrame', schedule: 'pl.DataFrame', *, league_id: 'str' = '00') -> 'pl.DataFrame'`

Map the stats.nba.com releases onto the stint-row frame.

One row per play-by-play action in game order, on the lineup at that action: its clock, its
points (a free throw's on the row of the game's last foul) and the possessions that start at
it. Games not in `schedule` are dropped, and so are possessions whose offense is neither team.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | `load_nba_stats_pbp` / `load_wnba_stats_pbp` rows (`game_id`, `order_index`, `action_number`, `period`, `seconds_remaining`, `score_home`, `score_away`, `is_free_throw`, `is_foul`, `season`). |
| `game_lineups` | `DataFrame` |  | `load_nba_stats_game_lineups` / `load_wnba_stats_game_lineups` rows. |
| `possessions` | `DataFrame` |  | `load_nba_stats_possessions` / `load_wnba_stats_possessions` rows (`game_id`, `offense_team_id`, `start_order_index`). |
| `schedule` | `DataFrame` |  | `load_nba_stats_schedules` / `load_wnba_stats_schedules` rows (`game_id`, `home_team_id`, `away_team_id`). |
| `league_id` | `str` | `'00'` | `"00"` NBA (12-minute quarters) or `"10"` WNBA (10-minute quarters). |

**Returns**

Rows in `sportsdataverse._common.game_stints.stint_row_schema` with Int64 player ids: `period`, `clock` (the action's `seconds_remaining`) and `order` (its `order_index`).

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season of the action. |
| `game_id` | String | stats.nba.com game id. |
| `home_team_id` | Int64 | stats.nba.com team id of the home team, from the schedule. |
| `away_team_id` | Int64 | stats.nba.com team id of the away team, from the schedule. |
| `home_1` | Int64 | Home player 1 on the floor. |
| `home_2` | Int64 | Home player 2 on the floor. |
| `home_3` | Int64 | Home player 3 on the floor. |
| `home_4` | Int64 | Home player 4 on the floor. |
| `home_5` | Int64 | Home player 5 on the floor. |
| `away_1` | Int64 | Away player 1 on the floor. |
| `away_2` | Int64 | Away player 2 on the floor. |
| `away_3` | Int64 | Away player 3 on the floor. |
| `away_4` | Int64 | Away player 4 on the floor. |
| `away_5` | Int64 | Away player 5 on the floor. |
| `seconds` | Float64 | Game clock that ran into this action (from the previous action of the period). |
| `home_pts` | Int64 | Points the home team scored on this action; a free throw's points sit on the row of the game's last foul. |
| `away_pts` | Int64 | Points the away team scored on this action; a free throw's points sit on the row of the game's last foul. |
| `home_poss` | Int64 | Possessions of the home team that start at this action. |
| `away_poss` | Int64 | Possessions of the away team that start at this action. |
| `period` | Int64 | Period of the action. |
| `clock` | Float64 | Period clock (seconds left) after the action. |
| `order` | Int64 | The action's order_index (its place in the game). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`. |

**Example**

```python
from sportsdataverse.nba import nba_loaders as L
from sportsdataverse.nba.nba_game_stints import nba_stint_rows
rows = nba_stint_rows(L.load_nba_stats_pbp(2025), L.load_nba_stats_game_lineups(2025),
                      L.load_nba_stats_possessions(2025), L.load_nba_stats_schedules(2025))
```

### nba_team_clutch {#nba_team_clutch}

`nba_team_clutch(season: 'int', *, league_id: 'str' = '00', return_as_pandas: 'bool' = False) -> 'Union[pl.DataFrame, pd.DataFrame]'`

Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team.

Loads the season's clutch net rating (`nba_stats_leaguedashteamclutch`)
and full-game net baseline (`nba_stats_leaguedashteamstats`), computes
`clutch_delta`, and applies `shrink_clutch`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | End year of the season (e.g. `2024` for 2023-24). |
| `league_id` | `str` | `'00'` | `"00"` NBA / `"10"` WNBA / `"20"` G-League. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per team: `season, team_id, clutch_net_rating, adj_net_rtg, clutch_delta, clutch_skill_shrunk, clutch_poss`. Empty input returns that schema with zero rows.

No returns table is published for this function: no capture: it reads stats.nba.com, which answers HTTP 403 to the datacenter IP the docs are built on; the function works from a residential IP.

**Example**

```python
from sportsdataverse.nba.nba_clutch import nba_team_clutch
skill = nba_team_clutch(2024)
skill.sort("clutch_skill_shrunk", descending=True).head()
```

### nba_team_game_possessions {#nba_team_game_possessions}

`nba_team_game_possessions(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Per team and game: the possession counts behind `nba_team_season_rates`.

The season table's `poss` / `opp_poss` / `pts` / `opp_pts` one game at a time, from the
same possessions, so a team-season's rows of a split add up to its season row exactly (their
count is `games`). Rate any set of games the same way: sum the rows, then divide once
(`off_rtg = 100 * pts / poss`; pace `48 * 60 * (poss + opp_poss) / 2 / seconds`, 40 in the
WNBA). The definitions are in `sportsdataverse._common.team_season_rates`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id, game_id, split)`: a game has an `all` row, a `home` or `away` row (neither at a neutral site) and, when it has clutch possessions, `close` and `clutch` rows. `team_id` is the stats.nba.com team id (Int64), `game_id` the stats.nba.com game id. `poss` / `opp_poss` are the team's offensive / defensive possessions, `pts` / `opp_pts` their points and `seconds` the game clock they used. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `game_id` | String | stats.nba.com game id. The game this row's possessions are from. |
| `split` | String | The season table's split this game is in: all (every game); home or away (neither at a neutral site); close and clutch when the game has clutch possessions. A team-season's rows of a split add up to its nba_team_season_rates row. |
| `poss` | Int64 | Offensive possessions in the split, counted from possession rows. |
| `opp_poss` | Int64 | Defensive possessions in the split (the opponents' offensive possessions). |
| `pts` | Int64 | Points scored on the team's offensive possessions in the split. Counted from each possession's made shots and free throws. |
| `opp_pts` | Int64 | Points allowed on the team's defensive possessions in the split. |
| `seconds` | Float64 | Game-clock seconds the team's possessions in the split used; pace over a set of games is regulation minutes x 60 x (poss + opp_poss) / 2 / summed seconds. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
import polars as pl
from sportsdataverse.nba.nba_team_season_rates import nba_team_game_possessions
games = nba_team_game_possessions(2026).filter(split="all")
games.group_by("team_id").agg(pl.col("pts").sum() * 100 / pl.col("poss").sum())
```

### nba_team_luck {#nba_team_luck}

`nba_team_luck(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Luck and close-game record: Pythagorean expected wins vs actual wins, and close-game W-L.

One row per team and season. `exp_wins` is the Pythagorean expectation on the season's
final scores, `games * pts^x / (pts^x + opp_pts^x)`, with the exponent `x` fitted per
league on past seasons (`~sportsdataverse._common.team_luck.PYTHAG_EXPONENT`).
`luck_wins` is `wins - exp_wins`. A close game is one within 5 points at the start of a
possession in play in the last 5:00 of the 4th quarter or in overtime: one that starts there,
or starts before 5:00 and ends after it (stats.nba.com's clutch window; BB-D3's `close` split).
For the WNBA (`league_id="10"`) through 2005, two 20-minute halves, the regulation window is
the last 5:00 of the 2nd half; overtime counts as above.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`, matched to the schedule's `season_type`. The exponent was fitted on regular seasons; on a handful of playoff or play-in games `exp_wins` is only indicative. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`; `team_id` is the stats.nba.com team id (Int64). `games`, `wins`, `losses`, `pts` and `opp_pts` are from the final scores; `win_pct`, `pythag_win_pct`, `exp_wins` and `luck_wins` (`wins - exp_wins`) follow. `pbp_games` counts the games with published possessions, and `close_games`, `close_wins`, `close_losses` and `close_win_pct` (null with no close game) are the close games among them. `pythag_win_pct`, `luck_wins` and `close_win_pct` have `<m>_rank` (1 = highest), `<m>_pct` (the Weibull percentile) and `<m>_n` (games, or close games); ranks are within the season, among teams with at least half the most games (`close_win_pct` also needs possessions for half of them and five close games), and null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_id` | Int64 | stats.nba.com team id (the 10-digit franchise id). |
| `games` | Int64 | Games in the season type's completed games (the stats schedule's final scores). |
| `wins` | Int64 | Wins in the season type's completed games (the stats schedule's final scores). |
| `losses` | Int64 | Losses in the season type's completed games (the stats schedule's final scores). |
| `pts` | Int64 | Points scored in those games (final scores). |
| `opp_pts` | Int64 | Points allowed in those games (final scores). |
| `win_pct` | Float64 | Win fraction: wins divided by games, from the final scores. |
| `pythag_win_pct` | Float64 | Pythagorean win fraction: pts^x / (pts^x + opp_pts^x), x = PYTHAG_EXPONENT['nba'] (PYTHAG_EXPONENT['wnba'] with league_id='10'), fitted on past seasons by tools/fit_team_luck_pythag.py. |
| `exp_wins` | Float64 | Expected wins: pythag_win_pct * games. |
| `luck_wins` | Float64 | Luck in wins: wins minus exp_wins. Positive means the team won more games than its scoring says it should have. |
| `pbp_games` | Int64 | Games with published possessions (nba_stats_possessions); close_games is counted among these. A game whose late possessions are missing from the release counts as not close. |
| `close_games` | Int64 | Close games: at least one possession in play with 5:00 or less left (starting there, or starting before 5:00 and ending after it) in the 4th quarter (the 2nd half for league_id='10' through 2005, when the WNBA played two 20-minute halves) or an overtime, the score within 5 as that possession starts (stats.nba.com's clutch window; BB-D3's close split). The margin is the running sum of the possessions' points, so it can drift from the scoreboard: NBA 2025-26 agrees with an independent ESPN play-by-play recount on 1215 of 1229 games. |
| `close_wins` | Int64 | Wins in close games. |
| `close_losses` | Int64 | Losses in close games. |
| `close_win_pct` | Float64 | close_wins / close_games; null with no close game. |
| `pythag_win_pct_rank` | Float64 | Rank of pythag_win_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers, or when every qualifier has the same value. |
| `luck_wins_rank` | Float64 | Rank of luck_wins within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season; null for a non-qualifier, a null value, or fewer than five qualifiers, or when every qualifier has the same value. |
| `close_win_pct_rank` | Float64 | Rank of close_win_pct within the season (the highest value ranks 1; ties average), among teams that played at least half the most games any team played in the season, have possessions for at least half their games and played at least five close games (MIN_CLOSE_GAMES); null otherwise, for a null value, or with fewer than five qualifiers, or when every qualifier has the same value. |
| `pythag_win_pct_pct` | Float64 | Percentile of pythag_win_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `luck_wins_pct` | Float64 | Percentile of luck_wins within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `close_win_pct_pct` | Float64 | Percentile of close_win_pct within the season among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked, so the best team is under 100 and nobody is 0; null when the rank is null. |
| `pythag_win_pct_n` | Int64 | Sample size of pythag_win_pct: games. |
| `luck_wins_n` | Int64 | Sample size of luck_wins: games. |
| `close_win_pct_n` | Int64 | Sample size of close_win_pct: close games. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above (checked before any data is loaded). |

**Example**

```python
from sportsdataverse.nba.nba_team_luck import nba_team_luck
luck = nba_team_luck(2026)
luck.sort("luck_wins", descending=True).select("team_id", "wins", "exp_wins", "luck_wins").head()

# WNBA via ``league_id``

wnba = nba_team_luck(2025, league_id="10")
```

### nba_team_play_styles {#nba_team_play_styles}

`nba_team_play_styles(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Defensive play-style archetypes per team-season (BB-D20); team offense has none (see the module).

The method is in `sportsdataverse._common.play_style` and the features in
`sportsdataverse.nba.nba_play_style`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26), 2016+. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(team_id, season)`: `team_id` (Int64), `season`, `team_name` and the `def_archetype`, `def_cluster`, `def_dist_to_center`, `def_second_archetype` and `def_margin`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | stats.nba.com team id. |
| `season` | Int64 | Season, named by the year it ends (2026 is 2025-26). |
| `team_name` | String | Team's full name on stats.nba.com (city and nickname). |
| `def_archetype` | String | Defensive play-style archetype: the label of the nearest frozen KMeans center to the team-season's within-season z-scored features; null when the team does not qualify on that side or the season is not covered. |
| `def_cluster` | Int64 | Index of the defensive archetype's center in the bundled artifact (0-based). |
| `def_dist_to_center` | Float64 | Euclidean distance (z-space) to the defensive archetype's center; small means a prototypical example. |
| `def_second_archetype` | String | The next-nearest defensive archetype, the style a hybrid leans toward. |
| `def_margin` | Float64 | Distance to the second center minus distance to the nearest; small means a hybrid between the two archetypes. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If a feature is null for a whole season (a release stopped carrying it). |

**Example**

```python
from sportsdataverse.nba.nba_play_style import nba_team_play_styles
teams = nba_team_play_styles(2025)
```

### nba_team_season_flow {#nba_team_season_flow}

`nba_team_season_flow(seasons: 'int | list[int]', *, league_id: 'str' = '00', season_type: 'str' = 'regular-season', return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Game flow per team and season, with ranks, percentiles and `_n`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2026` is 2025-26). The WNBA season is its calendar year. |
| `league_id` | `str` | `'00'` | `"00"` NBA or `"10"` WNBA. |
| `season_type` | `str` | `'regular-season'` | `"regular-season"` (default), `"preseason"`, `"playoffs"` or `"play-in"`. |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per `(season, team_id)`: `games`, `wins`; `lead_pct`, `trail_pct`, `tied_pct`, `avg_margin` and `avg_wp` (weighted by game clock); `lead_changes_pg` and `times_tied_pg`; `largest_lead`, `largest_comeback` and `max_run` (season bests); `runs_10` / `opp_runs_10` and their per-game rates; `avg_longest_drought_sec`; `def_poss`, `stops`, `stop_pct`; `kills`, `double_kills`, `kills_pg`, `double_kills_pg` and `kills_per_100`. The ten rated metrics carry `<m>_rank` (1 = best; `opp_runs_10_pg` and `avg_longest_drought_sec` rank low first), `<m>_pct` and `<m>_n` (games; the games with possessions for `kills_pg` / `double_kills_pg` and with a win probability for `avg_wp`; defensive possessions for `stop_pct` and `kills_per_100`), among teams with at least half the season's most games, null under five teams. Empty input returns that schema with zero rows.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (the WNBA season is its calendar year). |
| `team_id` | Int64 | stats.nba.com team id. |
| `games` | Int64 | Games played (completed games of the season type). |
| `wins` | Int64 | Games the team won among its completed games of the season type (pts > opp_pts). |
| `trail_pct` | Float64 | Share of the team's game clock spent behind (sum trail_sec / sum game_seconds). |
| `tied_pct` | Float64 | Share of the team's game clock spent tied. |
| `lead_changes_pg` | Float64 | Lead changes per game. |
| `times_tied_pg` | Float64 | Times tied per game. |
| `largest_lead` | Int64 | The team's biggest lead of the season. |
| `largest_comeback` | Int64 | The biggest deficit the team overcame in a win this season (0 if none). |
| `max_run` | Int64 | The team's biggest n-0 run of the season. |
| `runs_10` | Int64 | The team's runs of 10 or more unanswered points, summed over the season. |
| `opp_runs_10` | Int64 | The opponents' runs of 10 or more unanswered points against the team, summed. |
| `def_poss` | Int64 | Defensive possessions, summed. |
| `stops` | Int64 | Stops (scoreless defensive possessions), summed. |
| `kills` | Int64 | Kills (three straight stops), summed. |
| `double_kills` | Int64 | Double kills (six straight stops), summed. |
| `avg_margin` | Float64 | Game control: the score margin averaged over the season's game clock. |
| `avg_wp` | Float64 | Game control: the in-game win probability averaged over the season's game clock. |
| `lead_pct` | Float64 | Share of the team's game clock spent ahead (sum lead_sec / sum game_seconds). |
| `runs_10_pg` | Float64 | The team's 10-0 (or better) runs per game. |
| `opp_runs_10_pg` | Float64 | The opponents' 10-0 (or better) runs against the team per game. |
| `avg_longest_drought_sec` | Float64 | The team's longest scoring drought, in seconds, averaged over its games. |
| `stop_pct` | Float64 | stops / def_poss; null with no possessions. |
| `kills_pg` | Float64 | Kills per game. |
| `double_kills_pg` | Float64 | Double kills per game. |
| `kills_per_100` | Float64 | Kills per 100 defensive possessions; null with no possessions. |
| `avg_margin_rank` | Float64 | Rank of avg_margin within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_wp_rank` | Float64 | Rank of avg_wp within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `lead_pct_rank` | Float64 | Rank of lead_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `runs_10_pg_rank` | Float64 | Rank of runs_10_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `opp_runs_10_pg_rank` | Float64 | Rank of opp_runs_10_pg within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_longest_drought_sec_rank` | Float64 | Rank of avg_longest_drought_sec within the season (the lowest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `stop_pct_rank` | Float64 | Rank of stop_pct within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_pg_rank` | Float64 | Rank of kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `double_kills_pg_rank` | Float64 | Rank of double_kills_pg within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `kills_per_100_rank` | Float64 | Rank of kills_per_100 within the season (the highest value ranks 1; ties average), among teams with at least half the season's most games; null for a non-qualifier, a null value, or fewer than five qualifiers. |
| `avg_margin_pct` | Float64 | Percentile of avg_margin among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_wp_pct` | Float64 | Percentile of avg_wp among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `lead_pct_pct` | Float64 | Percentile of lead_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `runs_10_pg_pct` | Float64 | Percentile of runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `opp_runs_10_pg_pct` | Float64 | Percentile of opp_runs_10_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_longest_drought_sec_pct` | Float64 | Percentile of avg_longest_drought_sec among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `stop_pct_pct` | Float64 | Percentile of stop_pct among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_pg_pct` | Float64 | Percentile of kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `double_kills_pg_pct` | Float64 | Percentile of double_kills_pg among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `kills_per_100_pct` | Float64 | Percentile of kills_per_100 among the same qualified teams: 100 * (n + 1 - rank) / (n + 1), n the number ranked; null when the rank is null. |
| `avg_margin_n` | Int64 | Sample size of avg_margin: games. |
| `avg_wp_n` | Int64 | Sample size of avg_wp: the games with a win probability. |
| `lead_pct_n` | Int64 | Sample size of lead_pct: games. |
| `runs_10_pg_n` | Int64 | Sample size of runs_10_pg: games. |
| `opp_runs_10_pg_n` | Int64 | Sample size of opp_runs_10_pg: games. |
| `avg_longest_drought_sec_n` | Int64 | Sample size of avg_longest_drought_sec: games. |
| `stop_pct_n` | Int64 | Sample size of stop_pct: the teams defensive possessions. |
| `kills_pg_n` | Int64 | Sample size of kills_pg: the games with published possessions. |
| `double_kills_pg_n` | Int64 | Sample size of double_kills_pg: the games with published possessions. |
| `kills_per_100_n` | Int64 | Sample size of kills_per_100: the teams defensive possessions. |
| `kills_per75` | Float64 | Kills per 75 defensive possessions: 0.75 * kills_per_100 (BB-G8's per-75 basis); null with no possessions. |
| `kills_per75_rank` | Float64 | Rank of kills_per75: kills_per_100_rank, copied (scaling by 0.75 keeps the order). |
| `kills_per75_pct` | Float64 | Percentile of kills_per75: kills_per_100_pct, copied (scaling by 0.75 keeps the order). |
| `kills_per75_n` | Int64 | Sample size of kills_per75: kills_per_100_n, copied. |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | If `league_id` is not `"00"` or `"10"`, or `season_type` is not one of the four above. |

**Example**

```python
from sportsdataverse.nba.nba_game_flow import nba_team_season_flow
nba_team_season_flow(2026).sort("kills_pg_rank").select("team_id", "kills_pg", "avg_wp").head()
```
