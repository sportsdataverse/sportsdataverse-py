---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Team"
sidebar_label: "Team"
sidebar_position: 19
description: "WNBA — WNBA Stats API (stats.wnba.com) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Team

## wnba_stats_teamdetails

GET /stats/teamdetails

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamdetails`

**Valid URL:** [https://stats.wnba.com/stats/teamdetails?TeamID=1611661328](https://stats.wnba.com/stats/teamdetails?TeamID=1611661328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_teamdetails-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`TeamBackground`, `TeamHistory`, `TeamSocialSites`, `TeamAwardsChampionships`, `TeamAwardsConf`, `TeamAwardsDiv`, `TeamHof`, `TeamRetired`, `TeamAwardsCommCup`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**TeamBackground**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `abbreviation` | character | Short abbreviation. |
| `nickname` | character | Team or athlete nickname. |
| `yearfounded` | integer | Year the franchise was founded. |
| `city` | character | Venue city. |
| `arena` | character | Arena. |
| `arenacapacity` | character | Seating capacity of the team's home arena. |
| `owner` | character | Name of the team's owner or ownership group. |
| `generalmanager` | character | Name of the team's general manager. |
| `headcoach` | character | Name of the team's head coach. |
| `dleagueaffiliation` | character | Name of the team's G League (formerly D-League) affiliate. |

**TeamHistory**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `city` | character | Venue city. |
| `nickname` | character | Team or athlete nickname. |
| `yearfounded` | integer | Year the franchise was founded. |
| `yearactivetill` | integer |  |

**TeamSocialSites**

| col_name | type | description |
|---|---|---|
| `accounttype` | character |  |
| `website_link` | character |  |

**TeamAwardsChampionships**

| col_name | type | description |
|---|---|---|
| `yearawarded` | integer |  |
| `oppositeteam` | character |  |

**TeamAwardsConf**

| col_name | type | description |
|---|---|---|
| `yearawarded` | integer |  |
| `oppositeteam` | character |  |

**TeamAwardsDiv**

| col_name | type | description |
|---|---|---|
| `yearawarded` | integer |  |
| `oppositeteam` | character |  |

**TeamHof**

| col_name | type | description |
|---|---|---|
| `playerid` | integer | Playerid. |
| `player` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `jersey` | character | Jersey number worn by the player. |
| `seasonswithteam` | character |  |
| `year` | integer | 4-digit year. |

**TeamRetired**

| col_name | type | description |
|---|---|---|
| `playerid` | integer | Playerid. |
| `player` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `jersey` | character | Jersey number worn by the player. |
| `seasonswithteam` | character |  |
| `year` | integer | 4-digit year. |

**TeamAwardsCommCup**

| col_name | type | description |
|---|---|---|
| `yearawarded` | integer |  |
| `oppositeteam` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teamdetails-example}

```python
wnba_stats_teamdetails()
```

_Last validated n/a._

## wnba_stats_teamestimatedmetrics

GET /stats/teamestimatedmetrics

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamestimatedmetrics`

**Valid URL:** [https://stats.wnba.com/stats/teamestimatedmetrics?LeagueID=10&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/teamestimatedmetrics?LeagueID=10&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May; with season type ``Playoffs`` (or ``commonplayoffseries``) from October, with ``All Star`` from the August after its July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns {#wnba_stats_teamestimatedmetrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `e_off_rating` | numeric | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | numeric | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | numeric | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_pace` | numeric | Estimated pace for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | numeric | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | numeric | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | numeric | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | numeric | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tm_tov_pct` | numeric | Estimated team turnover percentage for the requested NBA or WNBA Stats split. |
| `gp_rank` | integer | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | integer | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | integer | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | integer | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | integer | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_off_rating_rank` | integer | Rank for e offensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_def_rating_rank` | integer | Rank for e defensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_net_rating_rank` | integer | Rank for e net rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_ast_ratio_rank` | integer | Rank for e assists ratio within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_oreb_pct_rank` | integer | Rank for e offensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_dreb_pct_rank` | integer | Rank for e defensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_reb_pct_rank` | integer | Rank for e rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_tm_tov_pct_rank` | integer | Rank for e team turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | integer | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teamestimatedmetrics-example}

```python
wnba_stats_teamestimatedmetrics(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_teamgamelog

GET /stats/teamgamelog

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamgamelog`

**Valid URL:** [https://stats.wnba.com/stats/teamgamelog?DateFrom=&DateTo=&LeagueID=10&Season=2024&SeasonType=Regular+Season&TeamID=1611661328](https://stats.wnba.com/stats/teamgamelog?DateFrom=&DateTo=&LeagueID=10&Season=2024&SeasonType=Regular+Season&TeamID=1611661328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May; with season type ``Playoffs`` (or ``commonplayoffseries``) from October, with ``All Star`` from the August after its July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_teamgamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | integer | Minutes played. |
| `fgm` | integer | Field goals made. |
| `fga` | integer | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | integer | Three-point field goals made. |
| `fg3_a` | integer | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | integer | Free throws made. |
| `fta` | integer | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | integer | Offensive rebounds. |
| `dreb` | integer | Defensive rebounds. |
| `reb` | integer | Total rebounds. |
| `ast` | integer | Assists. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `tov` | integer | Turnovers. |
| `pf` | integer | Personal fouls. |
| `pts` | integer | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teamgamelog-example}

```python
wnba_stats_teamgamelog(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_teamgamelogs

GET /stats/teamgamelogs

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamgamelogs`

**Valid URL:** [https://stats.wnba.com/stats/teamgamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OppTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.wnba.com/stats/teamgamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OppTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_player_game_logs_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OppTeamID` | `opp_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May; with season type ``Playoffs`` (or ``commonplayoffseries``) from October, with ``All Star`` from the August after its July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_teamgamelogs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `matchup` | character | Matchup. |
| `wl` | character | Wl. |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
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
| `fg3_m_rank` | integer | Rank for three-point field goals made within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `fg3_a_rank` | integer | Rank for three-point field goals attempted within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
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
| `available_flag` | integer | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teamgamelogs-example}

```python
wnba_stats_teamgamelogs(league_id='10', season_nullable='2024')
```

_Last validated n/a._

## wnba_stats_teaminfocommon

GET /stats/teaminfocommon

**Endpoint URL:** `GET https://stats.wnba.com/stats/teaminfocommon`

**Valid URL:** [https://stats.wnba.com/stats/teaminfocommon?LeagueID=10&Season=2024&SeasonType=Regular+Season&TeamID=1611661328](https://stats.wnba.com/stats/teaminfocommon?LeagueID=10&Season=2024&SeasonType=Regular+Season&TeamID=1611661328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May; with season type ``Playoffs`` (or ``commonplayoffseries``) from October, with ``All Star`` from the August after its July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_teaminfocommon-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`TeamInfoCommon`, `TeamSeasonRanks`, `AvailableSeasons`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**TeamInfoCommon**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_conference` | character | Conference the team belongs to. |
| `team_division` | character | Division the team belongs to. |
| `team_code` | character | Internal team code. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `pct` | numeric | Win percentage. |
| `conf_rank` | integer | Team's current rank within its conference. |
| `div_rank` | integer | Team's current rank within its division. |
| `min_year` | character | Minimum year queried (echoes `min_year`). |
| `max_year` | character | Maximum year queried (echoes `max_year`). |

**TeamSeasonRanks**

| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `season_id` | character | Unique season identifier. |
| `team_id` | integer | Unique team identifier. |
| `pts_rank` | integer |  |
| `pts_pg` | numeric |  |
| `reb_rank` | integer |  |
| `reb_pg` | numeric |  |
| `ast_rank` | integer |  |
| `ast_pg` | numeric |  |
| `opp_pts_rank` | integer |  |
| `opp_pts_pg` | numeric |  |

**AvailableSeasons**

| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teaminfocommon-example}

```python
wnba_stats_teaminfocommon(league_id='10', season_nullable='2024')
```

_Last validated n/a._

## wnba_stats_teamvsplayer

GET /stats/teamvsplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamvsplayer`

**Valid URL:** [https://stats.wnba.com/stats/teamvsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=1611661328&VsConference=&VsDivision=&VsPlayerID=1628932](https://stats.wnba.com/stats/teamvsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=1611661328&VsConference=&VsDivision=&VsPlayerID=1628932)

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
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May; with season type ``Playoffs`` (or ``commonplayoffseries``) from October, with ``All Star`` from the August after its July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_teamvsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `vsPlayerOverall`, `OnOffCourt`, `ShotDistanceOverall`, `ShotDistanceOnCourt`, `ShotDistanceOffCourt`, `ShotAreaOverall`, `ShotAreaOnCourt`, `ShotAreaOffCourt`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**vsPlayerOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `player_id` | integer | Unique player identifier. |
| `gp` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `w_pct` | numeric | Wins percentage (0-1 decimal). |
| `min` | numeric | Minutes played. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `tov` | numeric | Turnovers. |
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `blka` | numeric | Shot attempts blocked by opponents (blocks against). |
| `pf` | numeric | Personal fouls. |
| `pfd` | numeric | Personal fouls drawn. |
| `pts` | numeric | Points scored. |
| `plus_minus` | numeric | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `dd2` | integer | Double-doubles recorded over the split. |
| `td3` | integer | Triple-doubles recorded over the split. |
| `wnba_fantasy_pts` | numeric | Fantasy points under the WNBA's fantasy scoring formula. |
| `gp_rank` | integer | League rank of the row's games played for the season and split. |
| `w_rank` | integer | League rank of the row's wins for the season and split. |
| `l_rank` | integer | League rank of the row's losses for the season and split. |
| `w_pct_rank` | integer | League rank of the row's win percentage for the season and split. |
| `min_rank` | integer | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | integer | League rank of the row's field goals made for the season and split. |
| `fga_rank` | integer | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | integer | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | integer | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | integer | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | integer | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | integer | League rank of the row's free throws made for the season and split. |
| `fta_rank` | integer | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | integer | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | integer | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | integer | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | integer | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | integer | League rank of the row's assists for the season and split. |
| `tov_rank` | integer | League rank of the row's turnovers for the season and split. |
| `stl_rank` | integer | League rank of the row's steals for the season and split. |
| `blk_rank` | integer | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | integer | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | integer | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | integer | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | integer | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | integer | League rank of the row's plus-minus point differential while on the floor for the season and split. |
| `nba_fantasy_pts_rank` | integer | League rank of the row's NBA fantasy points (league scoring formula) for the season and split. |
| `dd2_rank` | integer | League rank of the row's double-doubles for the season and split. |
| `td3_rank` | integer | League rank of the row's triple-doubles for the season and split. |
| `wnba_fantasy_pts_rank` | integer | League rank of the row's WNBA fantasy points (league scoring formula) for the season and split. |
| `team_count` | integer | Number of distinct teams aggregated into the split row. |

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | character |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
| `gp` | character | Games played. |
| `w` | character | Wins. |
| `l` | character | Losses. |
| `w_pct` | character | Wins percentage (0-1 decimal). |
| `min` | character | Minutes played. |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `fg3_m` | character | Three-point field goals made. |
| `fg3_a` | character | Three-point field goal attempts. |
| `fg3_pct` | character | Three-point field goal percentage (0-1). |
| `ftm` | character | Free throws made. |
| `fta` | character | Free throw attempts. |
| `ft_pct` | character | Free throw percentage (0-1). |
| `oreb` | character | Offensive rebounds. |
| `dreb` | character | Defensive rebounds. |
| `reb` | character | Total rebounds. |
| `ast` | character | Assists. |
| `tov` | character | Turnovers. |
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `blka` | character | Shot attempts blocked by opponents (blocks against). |
| `pf` | character | Personal fouls. |
| `pfd` | character | Personal fouls drawn. |
| `pts` | character | Points scored. |
| `plus_minus` | character | Plus/minus point differential while on court. |
| `gp_rank` | character | League rank of the row's games played for the season and split. |
| `w_rank` | character | League rank of the row's wins for the season and split. |
| `l_rank` | character | League rank of the row's losses for the season and split. |
| `w_pct_rank` | character | League rank of the row's win percentage for the season and split. |
| `min_rank` | character | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | character | League rank of the row's field goals made for the season and split. |
| `fga_rank` | character | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | character | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | character | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | character | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | character | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | character | League rank of the row's free throws made for the season and split. |
| `fta_rank` | character | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | character | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | character | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | character | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | character | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | character | League rank of the row's assists for the season and split. |
| `tov_rank` | character | League rank of the row's turnovers for the season and split. |
| `stl_rank` | character | League rank of the row's steals for the season and split. |
| `blk_rank` | character | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | character | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | character | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | character | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | character | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | character | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | character |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | character |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | character |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | character | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `vs_player_id` | character |  |
| `vs_player_name` | character |  |
| `court_status` | character |  |
| `group_value` | character | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | character | Field goals made. |
| `fga` | character | Field goal attempts. |
| `fg_pct` | character | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teamvsplayer-example}

```python
wnba_stats_teamvsplayer(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_teamyearbyyearstats

GET /stats/teamyearbyyearstats

**Endpoint URL:** `GET https://stats.wnba.com/stats/teamyearbyyearstats`

**Valid URL:** [https://stats.wnba.com/stats/teamyearbyyearstats?LeagueID=10&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661328](https://stats.wnba.com/stats/teamyearbyyearstats?LeagueID=10&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_teamyearbyyearstats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `year` | character | 4-digit year. |
| `gp` | integer | Games played. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Win percentage (0-1 decimal). |
| `conf_rank` | integer | Team's final rank within its conference for the season. |
| `div_rank` | integer | Team's final rank within its division for the season. |
| `po_wins` | integer | Playoff wins recorded by the team that season. |
| `po_losses` | integer | Playoff losses recorded by the team that season. |
| `conf_count` | integer | Number of teams in the team's conference that season. |
| `div_count` | integer | Number of teams in the team's division that season. |
| `nba_finals_appearance` | character | Whether the team reached the league finals that season (e.g. "FINALS APPEARANCE" or "N/A"). |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ftm` | numeric | Free throws made. |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `oreb` | numeric | Offensive rebounds. |
| `dreb` | numeric | Defensive rebounds. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `pf` | numeric | Personal fouls. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `blk` | numeric | Blocks. |
| `pts` | numeric | Points scored. |
| `pts_rank` | integer | League rank of the team's points scored for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_teamyearbyyearstats-example}

```python
wnba_stats_teamyearbyyearstats(league_id='10')
```

_Last validated n/a._
