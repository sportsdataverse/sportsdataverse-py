---
title: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team and player"
sidebar_label: "Team and player"
sidebar_position: 18
description: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team and player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team and player

## nbagl_stats_teamplayerdashboard

Team player dashboard.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamplayerdashboard`

**Valid URL:** [https://stats.nba.com/stats/teamplayerdashboard?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1612709914&VsConference=&VsDivision=](https://stats.nba.com/stats/teamplayerdashboard?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=1612709914&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
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
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamplayerdashboard-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`TeamOverall`, `PlayersSeasonTotals`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**TeamOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `group_value` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**PlayersSeasonTotals**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `nickname` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `dd2` | Int64 | Double-doubles for the requested NBA or WNBA Stats split. |
| `td3` | Int64 | Triple-doubles for the requested NBA or WNBA Stats split. |
| `wnba_fantasy_pts` | Float64 | Wnba fantasy points for the requested NBA or WNBA Stats split. |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `nba_fantasy_pts_rank` | Int64 | Rank for NBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dd2_rank` | Int64 | Rank for double-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `td3_rank` | Int64 | Rank for triple-doubles within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `wnba_fantasy_pts_rank` | Int64 | Rank for WNBA fantasy points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `team_count` | Int64 | NBA or WNBA Stats value for team count in the teamplayerdashboard result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamplayerdashboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamplayerdashboard-example}

```python
nbagl_stats_teamplayerdashboard(league_id='20', season='2024-25')
```

## nbagl_stats_teamplayeronoffdetails

Team player on off details.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamplayeronoffdetails`

**Valid URL:** [https://stats.nba.com/stats/teamplayeronoffdetails?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=](https://stats.nba.com/stats/teamplayeronoffdetails?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamplayeronoffdetails-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallTeamPlayerOnOffDetails`, `PlayersOnCourtTeamPlayerOnOffDetails`, `PlayersOffCourtTeamPlayerOnOffDetails`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallTeamPlayerOnOffDetails**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**PlayersOnCourtTeamPlayerOnOffDetails**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String | Stats API identifier for vs player identifier associated with this NBA or WNBA Stats row. |
| `vs_player_name` | String | Display name for vs player name associated with this NBA or WNBA Stats row. |
| `court_status` | String | Indicates whether the compared player was on court or off court for the split row. |
| `gp` | String |  |
| `w` | String |  |
| `l` | String |  |
| `w_pct` | String |  |
| `min` | String |  |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String |  |
| `ftm` | String |  |
| `fta` | String |  |
| `ft_pct` | String |  |
| `oreb` | String |  |
| `dreb` | String |  |
| `reb` | String |  |
| `ast` | String |  |
| `tov` | String |  |
| `stl` | String |  |
| `blk` | String |  |
| `blka` | String | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | String |  |
| `pfd` | String | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | String |  |
| `plus_minus` | String |  |
| `gp_rank` | String | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | String | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | String | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | String | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | String | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | String | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | String | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | String | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | String | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | String | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | String | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | String | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | String | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | String | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | String | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | String | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | String | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | String | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | String | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | String | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | String | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | String | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | String | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | String | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | String | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | String | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**PlayersOffCourtTeamPlayerOnOffDetails**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String | Stats API identifier for vs player identifier associated with this NBA or WNBA Stats row. |
| `vs_player_name` | String | Display name for vs player name associated with this NBA or WNBA Stats row. |
| `court_status` | String | Indicates whether the compared player was on court or off court for the split row. |
| `gp` | String |  |
| `w` | String |  |
| `l` | String |  |
| `w_pct` | String |  |
| `min` | String |  |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String |  |
| `ftm` | String |  |
| `fta` | String |  |
| `ft_pct` | String |  |
| `oreb` | String |  |
| `dreb` | String |  |
| `reb` | String |  |
| `ast` | String |  |
| `tov` | String |  |
| `stl` | String |  |
| `blk` | String |  |
| `blka` | String | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | String |  |
| `pfd` | String | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | String |  |
| `plus_minus` | String |  |
| `gp_rank` | String | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | String | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | String | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | String | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | String | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | String | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | String | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | String | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | String | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | String | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | String | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | String | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | String | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | String | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | String | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | String | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | String | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | String | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | String | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | String | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | String | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | String | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | String | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | String | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | String | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | String | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamplayeronoffdetails-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamplayeronoffdetails-example}

```python
nbagl_stats_teamplayeronoffdetails(league_id='20', season='2024-25')
```

## nbagl_stats_teamplayeronoffsummary

Team player on off summary.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamplayeronoffsummary`

**Valid URL:** [https://stats.nba.com/stats/teamplayeronoffsummary?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=](https://stats.nba.com/stats/teamplayeronoffsummary?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamplayeronoffsummary-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallTeamPlayerOnOffSummary`, `PlayersOnCourtTeamPlayerOnOffSummary`, `PlayersOffCourtTeamPlayerOnOffSummary`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallTeamPlayerOnOffSummary**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `group_value` | String | Specific grouping value for this dashboard or split row. |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Float64 |  |
| `dreb` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fgm_rank` | Int64 | Rank for field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fga_rank` | Int64 | Rank for field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg_pct_rank` | Int64 | Rank for field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_m_rank` | Int64 | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | Int64 | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_pct_rank` | Int64 | Rank for three-point field-goal percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ftm_rank` | Int64 | Rank for free throws made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fta_rank` | Int64 | Rank for free throws attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ft_pct_rank` | Int64 | Rank for free-throw percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `oreb_rank` | Int64 | Rank for offensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `dreb_rank` | Int64 | Rank for defensive rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `reb_rank` | Int64 | Rank for total rebounds within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `ast_rank` | Int64 | Rank for assists within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `tov_rank` | Int64 | Rank for turnovers within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `stl_rank` | Int64 | Rank for steals within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blk_rank` | Int64 | Rank for blocks within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `blka_rank` | Int64 | Rank for blocked field-goal attempts against within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pf_rank` | Int64 | Rank for personal fouls within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pfd_rank` | Int64 | Rank for personal fouls drawn within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `pts_rank` | Int64 | Rank for points within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `plus_minus_rank` | Int64 | Rank for plus-minus within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**PlayersOnCourtTeamPlayerOnOffSummary**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `gp` | String |  |
| `min` | String |  |
| `plus_minus` | String |  |
| `off_rating` | String | Offensive rating (points produced per 100 possessions). |
| `def_rating` | String | Defensive rating (points allowed per 100 possessions). |
| `net_rating` | String |  |

**PlayersOffCourtTeamPlayerOnOffSummary**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `gp` | String |  |
| `min` | String |  |
| `plus_minus` | String |  |
| `off_rating` | String | Offensive rating (points produced per 100 possessions). |
| `def_rating` | String | Defensive rating (points allowed per 100 possessions). |
| `net_rating` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamplayeronoffsummary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamplayeronoffsummary-example}

```python
nbagl_stats_teamplayeronoffsummary(league_id='20', season='2024-25')
```
