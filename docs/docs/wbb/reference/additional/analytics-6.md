---
title: "Women's college basketball — additional Python functions — Analytics: wbb_tournament–weighted_avg"
sidebar_label: "Analytics: wbb_tournament–weighted_avg"
sidebar_position: 16
description: "Women's college basketball — additional Python functions — Analytics: wbb_tournament–weighted_avg — function reference in sdv-py, the SportsDataverse Python package."
---
# Women's college basketball — additional Python functions — Analytics: wbb_tournament–weighted_avg

### wbb_tournament_run_players {#wbb_tournament_run_players}

`wbb_tournament_run_players(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Each women's player's run line against the regular season. Delegates to `mbb_tournament_run_players`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per player and run: totals, per-game lines, `ts_pct`, the regular season's and the shift.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `run_id` | String | Run key: '{season}-{tournament_key}-{ESPN team id}', one team's games in one tournament. |
| `team_id` | Int64 | ESPN team id of the player's team in the run. |
| `athlete_id` | Int64 | ESPN athlete id. |
| `athlete_display_name` | String | The player's name. |
| `games` | Int64 | Run games played (not marked did-not-play, with minutes or any box count). |
| `min` | Float64 | Total minutes in the run. |
| `pts` | Int64 | Total points in the run: the sum of the player's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made in the run: the sum of the player's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted in the run: the sum of the player's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made in the run: the sum of the player's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted in the run: the sum of the player's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made in the run: the sum of the player's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted in the run: the sum of the player's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds in the run: the sum of the player's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds in the run: the sum of the player's ESPN game boxes. |
| `reb` | Int64 | Total total rebounds in the run: the sum of the player's ESPN game boxes. |
| `ast` | Int64 | Total assists in the run: the sum of the player's ESPN game boxes. |
| `stl` | Int64 | Total steals in the run: the sum of the player's ESPN game boxes. |
| `blk` | Int64 | Total blocks in the run: the sum of the player's ESPN game boxes. |
| `tov` | Int64 | Total turnovers in the run: the sum of the player's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls in the run: the sum of the player's ESPN game boxes. |
| `min_pg` | Float64 | Minutes per run game played. |
| `pts_pg` | Float64 | Points per run game played. |
| `reb_pg` | Float64 | Total rebounds per run game played. |
| `ast_pg` | Float64 | Assists per run game played. |
| `stl_pg` | Float64 | Steals per run game played. |
| `blk_pg` | Float64 | Blocks per run game played. |
| `tov_pg` | Float64 | Turnovers per run game played. |
| `fg3m_pg` | Float64 | Three-pointers made per run game played. |
| `ts_pct` | Float64 | True shooting in the run: PTS / (2 * (FGA + 0.44 * FTA)); null with no attempts. |
| `reg_games` | Int64 | Regular-season games played, without the conference-tournament games (every team the player played for); null with none. |
| `reg_min_pg` | Float64 | Minutes per regular-season game played. |
| `reg_pts_pg` | Float64 | Points per regular-season game played. |
| `reg_reb_pg` | Float64 | Total rebounds per regular-season game played. |
| `reg_ast_pg` | Float64 | Assists per regular-season game played. |
| `reg_stl_pg` | Float64 | Steals per regular-season game played. |
| `reg_blk_pg` | Float64 | Blocks per regular-season game played. |
| `reg_tov_pg` | Float64 | Turnovers per regular-season game played. |
| `reg_fg3m_pg` | Float64 | Three-pointers made per regular-season game played. |
| `reg_ts_pct` | Float64 | True shooting over the player's regular season. |
| `min_pg_diff` | Float64 | min_pg - reg_min_pg: the run against the player's regular season (positive is up). |
| `pts_pg_diff` | Float64 | pts_pg - reg_pts_pg: the run against the player's regular season (positive is up). |
| `reb_pg_diff` | Float64 | reb_pg - reg_reb_pg: the run against the player's regular season (positive is up). |
| `ast_pg_diff` | Float64 | ast_pg - reg_ast_pg: the run against the player's regular season (positive is up). |
| `stl_pg_diff` | Float64 | stl_pg - reg_stl_pg: the run against the player's regular season (positive is up). |
| `blk_pg_diff` | Float64 | blk_pg - reg_blk_pg: the run against the player's regular season (positive is up). |
| `tov_pg_diff` | Float64 | tov_pg - reg_tov_pg: the run against the player's regular season (positive is up). |
| `fg3m_pg_diff` | Float64 | fg3m_pg - reg_fg3m_pg: the run against the player's regular season (positive is up). |
| `ts_pct_diff` | Float64 | ts_pct - reg_ts_pct. |

**Example**

```python
from sportsdataverse.wbb import wbb_tournament_run_players
wbb_tournament_run_players(2025).sort("pts", descending=True).head()
```

### wbb_tournament_run_teams {#wbb_tournament_run_teams}

`wbb_tournament_run_teams(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Each women's run's record and box totals. Delegates to `mbb_tournament_run_teams(league="womens")`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

One row per run with a final game: the run's `tournament_name` and `tournament_kind` (`ncaa`, `conference` or `postseason`), the W-L, points for and against and the summed counts.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `run_id` | String | Run key: '{season}-{tournament_key}-{ESPN team id}', one team's games in one tournament. |
| `tournament_name` | String | The run's tournament name, as on its tournament_runs rows. |
| `tournament_kind` | String | The run's tournament kind, as on its tournament_runs rows: ncaa (the NCAA tournament), conference (a conference tournament) or postseason (the NIT, WNIT, CBI, Crown, WBIT and other postseason events). |
| `team_id` | Int64 | ESPN team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `games` | Int64 | Final games in the run. |
| `wins` | Int64 | Run wins. |
| `losses` | Int64 | Run losses. |
| `opp_pts` | Int64 | Opponent points over the run's final games. |
| `pts` | Int64 | Total points over the run's final games: the sum of the team's ESPN game boxes. |
| `fgm` | Int64 | Total field goals made over the run's final games: the sum of the team's ESPN game boxes. |
| `fga` | Int64 | Total field goals attempted over the run's final games: the sum of the team's ESPN game boxes. |
| `fg3m` | Int64 | Total three-pointers made over the run's final games: the sum of the team's ESPN game boxes. |
| `fg3a` | Int64 | Total three-pointers attempted over the run's final games: the sum of the team's ESPN game boxes. |
| `ftm` | Int64 | Total free throws made over the run's final games: the sum of the team's ESPN game boxes. |
| `fta` | Int64 | Total free throws attempted over the run's final games: the sum of the team's ESPN game boxes. |
| `oreb` | Int64 | Total offensive rebounds over the run's final games: the sum of the team's ESPN game boxes. |
| `dreb` | Int64 | Total defensive rebounds over the run's final games: the sum of the team's ESPN game boxes. |
| `reb` | Int64 | Total total rebounds over the run's final games: the sum of the team's ESPN game boxes. |
| `ast` | Int64 | Total assists over the run's final games: the sum of the team's ESPN game boxes. |
| `stl` | Int64 | Total steals over the run's final games: the sum of the team's ESPN game boxes. |
| `blk` | Int64 | Total blocks over the run's final games: the sum of the team's ESPN game boxes. |
| `tov` | Int64 | Total turnovers over the run's final games: the sum of the team's ESPN game boxes. |
| `pf` | Int64 | Total personal fouls over the run's final games: the sum of the team's ESPN game boxes. |

**Example**

```python
from sportsdataverse.wbb import wbb_tournament_run_teams
wbb_tournament_run_teams(2025).head()
```

### wbb_tournament_runs {#wbb_tournament_runs}

`wbb_tournament_runs(seasons: 'int | list[int]', *, return_as_pandas: 'bool' = False) -> 'pl.DataFrame | pd.DataFrame'`

Women's tournament games, once per side, keyed to each team's run (NCAA, WNIT, WBIT, conference ...).

Delegates to `sportsdataverse.mbb.mbb_tournament_runs.mbb_tournament_runs` with `league="womens"`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `int \| list[int]` |  | A season or list of seasons, named by the year it ends (`2025` is 2024-25). |
| `return_as_pandas` | `bool` | `False` | Return a pandas frame instead of polars. |

**Returns**

Two rows per tournament game, as the mbb function: the run keys, the tournament's name and kind, the round, the score, the run's record after the game and `run_result`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season, named by the year it ends (2025 is 2024-25). |
| `run_id` | String | Run key: '{season}-{tournament_key}-{ESPN team id}', one team's games in one tournament. |
| `tournament_key` | String | The tournament: ESPN's tournament_id as text, or the headline's event name as a slug where the id is missing (older seasons). |
| `tournament_id` | Int64 | ESPN tournament id (22 is the NCAA tournament); an untagged game takes the id of the tagged tournament of its season with the same event name, else null. |
| `tournament_name` | String | The event name most of the tournament's game headlines carry (the text before ' - '). |
| `tournament_kind` | String | ncaa (ESPN tournament 22, First Four onward), conference (a conference tournament: a tagged regular-season tournament with a conference group, from February 15) or postseason (the NIT, WNIT, CBI, Crown, WBIT and other postseason events). |
| `conference_id` | Int64 | ESPN group id of the conference, for a conference tournament; null otherwise. |
| `round_name` | String | The rest of the game headline after the event name (for example 'East Region - 1st Round', 'Final'), without a trailing site. |
| `game_number` | Int64 | Game number within the run, by tip-off. |
| `game_id` | Int64 | ESPN game id. |
| `game_date` | String | Game date (US). |
| `completed` | Boolean | True when the game is final. |
| `home_away` | String | home or away, or neutral at a neutral site. |
| `team_id` | Int64 | The run's team: ESPN team id. |
| `team_abbreviation` | String | The team's abbreviation. |
| `team_display_name` | String | The team's display name. |
| `team_score` | Int64 | The team's points (null before tip-off). |
| `opponent_team_id` | Int64 | The opponent's ESPN team id. |
| `opponent_abbreviation` | String | The opponent's abbreviation. |
| `opponent_display_name` | String | The opponent's display name. |
| `opponent_score` | Int64 | The opponent's points (null before tip-off). |
| `winner_team_id` | Int64 | The game winner's ESPN team id; null until the game is final. |
| `wins` | Int64 | The run's wins after this game. |
| `losses` | Int64 | The run's losses after this game. |
| `run_games` | Int64 | Games listed in the run (played and scheduled), as of the release, repeated on every row of the run: an earlier game's row already shows how far the run went, so never use it as a feature for that game. |
| `run_result` | String | champion (won the tournament's last listed game, by tip-off, once final, when it is the only game on the tournament's last date), eliminated (lost its last listed game) or alive. The outcome as of the release, repeated on every row of the run, not as of that game: never use it as a feature for an earlier game. |

**Example**

```python
from sportsdataverse.wbb import wbb_tournament_runs
runs = wbb_tournament_runs(2025)
runs.filter(runs["run_result"] == "champion")
```

### weighted_avg {#weighted_avg}

`weighted_avg(mutable_acc: 'LineupStatSet', obj: 'LineupStatSet') -> 'None'`

Merge `obj` into `mutable_acc` with possession weighting.

Faithful port of `LineupUtils.weightedAvg` (`LineupUtils.ts:645`).
Mutates `mutable_acc` in place (matching the upstream mutable-state
contract) and returns `None`. Each call accumulates a **weighted
sum**, not a weighted average -- the companion `completeWeightedAvg`
(upstream `LineupUtils.ts:752`, not yet ported) divides by the
accumulated weight totals to finish the average. The per-field weight
used at each merge step is derived from `obj`'s *own* totals (e.g.
that single lineup's `total_off_fga`), not from any running total on
`mutable_acc` -- callers accumulating many lineups must call
`weighted_avg` once per lineup so every lineup contributes its own
weight.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `mutable_acc` | `LineupStatSet` |  | The running accumulator (`LineupStatSet`). Mutated in place; fields absent from the accumulator are initialized to `{"value": 0.0}` (plus `old_value` / `override` when `obj`'s field carries a luck-adjustment `override` marker) before `obj`'s contribution is added. |
| `obj` | `LineupStatSet` |  | The per-lineup `LineupStatSet` document to merge in. |

**Returns**

None. `mutable_acc` is mutated in place.

**Raises**

| Exception | Description |
|---|---|
| `NotImplementedError` | If `obj` carries a `game_info` key -- that merge branch is deliberately deferred (see module docstring). |

**Example**

```python
from sportsdataverse.mbb.mbb_lineup_stats import weighted_avg

acc: dict = {}
weighted_avg(acc, lineup_a)
weighted_avg(acc, lineup_b)
print(acc["off_poss"]["value"])  # plain sum (SUM_FIELDS)

# Two-lineup possession-weighted merge

acc = {}
for lineup in three_lineups:
    weighted_avg(acc, lineup)
# acc now holds weighted SUMS; complete_weighted_avg (not yet
# ported) is required to turn these into rate-stat averages.
```
