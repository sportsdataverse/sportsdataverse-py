---
title: "NBA — NBA Stats API (stats.nba.com) — League dashboards (2)"
sidebar_label: "League dashboards (2)"
sidebar_position: 8
description: "NBA — NBA Stats API (stats.nba.com) — League dashboards (2) — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — League dashboards (2)

## nba_stats_leaguedashteamstats

GET /stats/leaguedashteamstats

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguedashteamstats`

**Valid URL:** [https://stats.nba.com/stats/leaguedashteamstats?LeagueID=00](https://stats.nba.com/stats/leaguedashteamstats?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TwoWay` | `two_way_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_leaguedashteamstats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3m` | numeric | Three-point field goals made. |
| `fg3a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Rebounds per game. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | integer | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | integer | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | integer | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | integer | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | integer | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | integer | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | integer | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | integer | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | integer | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | integer | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | integer | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | integer | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | integer | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | integer | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | integer | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | integer | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | integer | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | integer | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | integer | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaguedashteamstats-example}

```python
nba_stats_leaguedashteamstats(league_id='00')
```

_Last validated n/a._
