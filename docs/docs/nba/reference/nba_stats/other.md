---
title: "NBA — NBA Stats API (stats.nba.com) — Other"
sidebar_label: "Other"
sidebar_position: 29
description: "NBA — NBA Stats API (stats.nba.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Other

## nba_stats_alltimeleadersgrids

GET /stats/alltimeleadersgrids

**Endpoint URL:** `GET https://stats.nba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00&PerMode=PerGame&SeasonType=Regular+Season&TopX=10](https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00&PerMode=PerGame&SeasonType=Regular+Season&TopX=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `TopX` | `topx` |  |  | `Y` |  |

### Returns {#nba_stats_alltimeleadersgrids-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GPLeaders`, `PTSLeaders`, `ASTLeaders`, `STLLeaders`, `OREBLeaders`, `DREBLeaders`, `REBLeaders`, `BLKLeaders`, `FGMLeaders`, `FGALeaders`, `FG_PCTLeaders`, `TOVLeaders`, `FG3MLeaders`, `FG3ALeaders`, `FG3_PCTLeaders`, `PFLeaders`, `FTMLeaders`, `FTALeaders`, `FT_PCTLeaders`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GPLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `gp` | integer | Games played. |
| `gp_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**PTSLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `pts` | numeric | Points scored. |
| `pts_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**ASTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `ast` | numeric | Assists. |
| `ast_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**STLLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `stl` | numeric | Steals. |
| `stl_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**OREBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `oreb` | numeric | Offensive rebounds. |
| `oreb_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**DREBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `dreb` | numeric | Defensive rebounds. |
| `dreb_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**REBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `reb` | numeric | Rebounds per game. |
| `reb_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**BLKLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `blk` | numeric | Blocks. |
| `blk_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FGMLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | numeric | Field goals made. |
| `fgm_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FGALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fga` | numeric | Field goal attempts. |
| `fga_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FG_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg_pct_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**TOVLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `tov` | numeric | Turnovers. |
| `tov_rank` | integer | All-time league rank of the player's career turnover total on the leaders grid. |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FG3MLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fg3_m_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FG3ALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fg3_a` | numeric | Three-point field goal attempts. |
| `fg3_a_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FG3_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `fg3_pct_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**PFLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `pf` | numeric | Personal fouls. |
| `pf_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FTMLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `ftm` | numeric | Free throws made. |
| `ftm_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FTALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fta` | numeric | Free throw attempts. |
| `fta_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**FT_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `ft_pct_rank` | integer |  |
| `is_active_flag` | character | Flag indicating whether the player is currently active in the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_alltimeleadersgrids-example}

```python
nba_stats_alltimeleadersgrids(league_id='00')
```

_Last validated n/a._

## nba_stats_assistleaders

GET /stats/assistleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/assistleaders`

**Valid URL:** [https://stats.nba.com/stats/assistleaders?LeagueID=00&PerMode=PerGame&PlayerOrTeam=Team&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/assistleaders?LeagueID=00&PerMode=PerGame&PlayerOrTeam=Team&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns {#nba_stats_assistleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `ast` | numeric | Assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_assistleaders-example}

```python
nba_stats_assistleaders(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_assisttracker

GET /stats/assisttracker

**Endpoint URL:** `GET https://stats.nba.com/stats/assisttracker`

**Valid URL:** [https://stats.nba.com/stats/assisttracker?LeagueID=00&OpponentTeamID=0&PerMode=PerGame&Season=2024-25&SeasonType=Regular+Season&TeamID=0](https://stats.nba.com/stats/assisttracker?LeagueID=00&OpponentTeamID=0&PerMode=PerGame&Season=2024-25&SeasonType=Regular+Season&TeamID=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `GameScope` | `game_scope_simple_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `PlayerExperience` | `player_experience_nullable` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_abbreviation_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_assisttracker-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_assisttracker-example}

```python
nba_stats_assisttracker(league_id='00', season_nullable='2024-25')
```

_Last validated n/a._

## nba_stats_drafthistory

GET /stats/drafthistory

**Endpoint URL:** `GET https://stats.nba.com/stats/drafthistory`

**Valid URL:** [https://stats.nba.com/stats/drafthistory?College=&LeagueID=00&OverallPick=&RoundNum=&RoundPick=&Season=2024&TeamID=0&TopX=](https://stats.nba.com/stats/drafthistory?College=&LeagueID=00&OverallPick=&RoundNum=&RoundPick=&Season=2024&TeamID=0&TopX=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OverallPick` | `overall_pick_nullable` |  |  | `Y` |  |
| `RoundNum` | `round_num_nullable` |  |  | `Y` |  |
| `RoundPick` | `round_pick_nullable` |  |  | `Y` |  |
| `Season` | `season_year_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TopX` | `topx_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_drafthistory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_name` | character | Player name. |
| `season` | character | Season year. |
| `round_number` | integer | Numeric round. |
| `round_pick` | integer | Round pick. |
| `overall_pick` | integer | Overall pick. |
| `draft_type` | character | NBA or WNBA Stats value for draft type in the drafthistory result set. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `organization` | character | Organization. |
| `organization_type` | character | Organization type. |
| `player_profile_flag` | integer | Player profile flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_drafthistory-example}

```python
nba_stats_drafthistory(league_id='00', season_year_nullable='2024')
```

_Last validated n/a._

## nba_stats_fantasywidget

GET /stats/fantasywidget

**Endpoint URL:** `GET https://stats.nba.com/stats/fantasywidget`

**Valid URL:** [https://stats.nba.com/stats/fantasywidget?ActivePlayers=N&DateFrom=&DateTo=&LastNGames=0&LeagueID=00&Location=&Month=&OpponentTeamID=0&PORound=&PlayerID=&Position=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&TodaysOpponent=0&TodaysPlayers=N&VsConference=&VsDivision=](https://stats.nba.com/stats/fantasywidget?ActivePlayers=N&DateFrom=&DateTo=&LastNGames=0&LeagueID=00&Location=&Month=&OpponentTeamID=0&PORound=&PlayerID=&Position=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&TodaysOpponent=0&TodaysPlayers=N&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActivePlayers` | `active_players` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TodaysOpponent` | `todays_opponent` |  |  | `Y` |  |
| `TodaysPlayers` | `todays_players` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_fantasywidget-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `player_position` | character | Position of the player accordinng to NGS |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `gp` | integer | Games played. |
| `min` | numeric | Minutes played. |
| `fan_duel_pts` | numeric | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | numeric | Fantasy points under the NBA's fantasy scoring formula. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Rebounds per game. |
| `ast` | numeric | Assists. |
| `blk` | numeric | Blocks. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `fg3_m` | numeric | Three-point field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fta` | numeric | Free throw attempts. |
| `ft_pct` | numeric | Free throw percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_fantasywidget-example}

```python
nba_stats_fantasywidget(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_gamerotation

GET /stats/gamerotation

**Endpoint URL:** `GET https://stats.nba.com/stats/gamerotation`

**Valid URL:** [https://stats.nba.com/stats/gamerotation?GameID=0022200021&LeagueID=00](https://stats.nba.com/stats/gamerotation?GameID=0022200021&LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_gamerotation-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`AwayTeam`, `HomeTeam`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**AwayTeam**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_first` | character | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | character | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | numeric | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | numeric | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | integer | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | numeric | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**HomeTeam**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_first` | character | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | character | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | numeric | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | numeric | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | integer | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | numeric | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_gamerotation-example}

```python
nba_stats_gamerotation(league_id='00')
```

_Last validated n/a._

## nba_stats_homepageleaders

GET /stats/homepageleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/homepageleaders`

**Valid URL:** [https://stats.nba.com/stats/homepageleaders?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatCategory=Points](https://stats.nba.com/stats/homepageleaders?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatCategory=Points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatCategory` | `stat_category` |  |  | `Y` |  |

### Returns {#nba_stats_homepageleaders-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageLeaders`, `LeagueAverage`, `LeagueMax`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageLeaders**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `pts` | numeric | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `efg_pct` | numeric | Effective field goal percentage, as a decimal. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | numeric | Points scored per 48 minutes played. |

**LeagueAverage**

| col_name | type | description |
|---|---|---|
| `pts` | numeric | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `efg_pct` | numeric | Effective field goal percentage, as a decimal. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | numeric | Points scored per 48 minutes played. |

**LeagueMax**

| col_name | type | description |
|---|---|---|
| `pts` | numeric | Points scored. |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |
| `ft_pct` | numeric | Free throw percentage (0-1). |
| `efg_pct` | numeric | Effective field goal percentage, as a decimal. |
| `ts_pct` | numeric | True shooting percentage (0-1). |
| `pts_per48` | numeric | Points scored per 48 minutes played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_homepageleaders-example}

```python
nba_stats_homepageleaders(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_homepagev2

GET /stats/homepagev2

**Endpoint URL:** `GET https://stats.nba.com/stats/homepagev2`

**Valid URL:** [https://stats.nba.com/stats/homepagev2?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatType=Traditional](https://stats.nba.com/stats/homepagev2?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatType=Traditional)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatType` | `stat_type` |  |  | `Y` |  |

### Returns {#nba_stats_homepagev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageStat1`, `HomePageStat2`, `HomePageStat3`, `HomePageStat4`, `HomePageStat5`, `HomePageStat6`, `HomePageStat7`, `HomePageStat8`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageStat1**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `pts` | numeric | Points scored. |

**HomePageStat2**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `reb` | numeric | Rebounds per game. |

**HomePageStat3**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `ast` | numeric | Assists. |

**HomePageStat4**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `stl` | numeric | Steals. |

**HomePageStat5**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**HomePageStat6**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `ft_pct` | numeric | Free throw percentage (0-1). |

**HomePageStat7**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**HomePageStat8**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `blk` | numeric | Blocks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_homepagev2-example}

```python
nba_stats_homepagev2(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_hustlestatsboxscore

GET /stats/hustlestatsboxscore

**Endpoint URL:** `GET https://stats.nba.com/stats/hustlestatsboxscore`

**Valid URL:** [https://stats.nba.com/stats/hustlestatsboxscore?GameID=0022200021](https://stats.nba.com/stats/hustlestatsboxscore?GameID=0022200021)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_hustlestatsboxscore-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HustleStatsAvailable`, `PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HustleStatsAvailable**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `hustle_status` | integer | Hustle status. |

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `start_position` | character | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | character | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | integer | Points scored. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2_pt` | numeric | Opponent two-point attempts contested. |
| `contested_shots_3_pt` | numeric | Opponent three-point attempts contested. |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | numeric | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | numeric | Loose balls recovered while on defense. |
| `loose_balls_recovered` | numeric | Total loose balls recovered. |
| `off_boxouts` | numeric | Box-outs recorded on the offensive glass. |
| `def_boxouts` | numeric | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | numeric | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | numeric | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | numeric | Box-outs executed. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `minutes` | character | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | integer | Points scored. |
| `contested_shots` | numeric | Defensively contested shots. |
| `contested_shots_2_pt` | numeric | Opponent two-point attempts contested. |
| `contested_shots_3_pt` | numeric | Opponent three-point attempts contested. |
| `deflections` | numeric | Defensive deflections. |
| `charges_drawn` | numeric | Charges drawn. |
| `screen_assists` | numeric | Screen assists (resulting in a basket). |
| `screen_ast_pts` | numeric | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | numeric | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | numeric | Loose balls recovered while on defense. |
| `loose_balls_recovered` | numeric | Total loose balls recovered. |
| `off_boxouts` | numeric | Box-outs recorded on the offensive glass. |
| `def_boxouts` | numeric | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | numeric | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | numeric | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | numeric | Box-outs executed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_hustlestatsboxscore-example}

```python
nba_stats_hustlestatsboxscore()
```

_Last validated n/a._

## nba_stats_infographicfanduelplayer

GET /stats/infographicfanduelplayer

**Endpoint URL:** `GET https://stats.nba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.nba.com/stats/infographicfanduelplayer?GameID=0022201086](https://stats.nba.com/stats/infographicfanduelplayer?GameID=0022201086)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_infographicfanduelplayer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `location` | character | Location. |
| `fan_duel_pts` | numeric | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | numeric | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `usg_pct` | numeric | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `min` | numeric | Minutes played. |
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
| `reb` | integer | Rebounds per game. |
| `ast` | integer | Assists. |
| `tov` | integer | Turnovers. |
| `stl` | integer | Steals. |
| `blk` | integer | Blocks. |
| `blka` | integer | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | integer | Personal fouls. |
| `pfd` | integer | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | integer | Points scored. |
| `plus_minus` | integer | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_infographicfanduelplayer-example}

```python
nba_stats_infographicfanduelplayer()
```

_Last validated n/a._

## nba_stats_leaderstiles

GET /stats/leaderstiles

**Endpoint URL:** `GET https://stats.nba.com/stats/leaderstiles`

**Valid URL:** [https://stats.nba.com/stats/leaderstiles?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&Stat=PTS](https://stats.nba.com/stats/leaderstiles?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&Stat=PTS)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `Stat` | `stat` |  |  | `Y` |  |

### Returns {#nba_stats_leaderstiles-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`LeadersTiles`, `AllTimeSeasonHigh`, `LastSeasonHigh`, `LowSeasonHigh`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**LeadersTiles**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | numeric | Points scored. |

**AllTimeSeasonHigh**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `pts` | numeric | Points scored. |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |

**LastSeasonHigh**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Rank. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | numeric | Points scored. |

**LowSeasonHigh**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `pts` | numeric | Points scored. |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_leaderstiles-example}

```python
nba_stats_leaderstiles(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_matchupsrollup

GET /stats/matchupsrollup

**Endpoint URL:** `GET https://stats.nba.com/stats/matchupsrollup`

**Valid URL:** [https://stats.nba.com/stats/matchupsrollup?DefPlayerID=&DefTeamID=0&LeagueID=00&OffPlayerID=&OffTeamID=0&PerMode=Totals&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/matchupsrollup?DefPlayerID=&DefTeamID=0&LeagueID=00&OffPlayerID=&OffTeamID=0&PerMode=Totals&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DefPlayerID` | `def_player_id_nullable` |  |  | `Y` |  |
| `DefTeamID` | `def_team_id_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OffPlayerID` | `off_player_id_nullable` |  |  | `Y` |  |
| `OffTeamID` | `off_team_id_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns {#nba_stats_matchupsrollup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `percent_of_time` | numeric | Time value for percent of time in the NBA or WNBA Stats result set. |
| `def_player_id` | integer | Stats API identifier for defensive player identifier associated with this NBA or WNBA Stats row. |
| `def_player_name` | character | Display name for defensive player name associated with this NBA or WNBA Stats row. |
| `gp` | integer | Games played. |
| `matchup_min` | numeric | NBA or WNBA Stats value for matchup minutes in the matchupsrollup result set. |
| `partial_poss` | numeric | Estimated partial possessions credited to the stint or rotation interval. |
| `player_pts` | numeric | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `team_pts` | numeric | Scoring or score-margin metric for team points in the requested NBA or WNBA Stats split. |
| `matchup_ast` | numeric | NBA or WNBA Stats value for matchup assists in the matchupsrollup result set. |
| `matchup_tov` | numeric | Turnover or loose-ball metric for matchup turnovers in the requested NBA or WNBA Stats split. |
| `matchup_blk` | numeric | NBA or WNBA Stats value for matchup blocks in the matchupsrollup result set. |
| `matchup_fgm` | numeric | Shooting metric for matchup fgm in the requested NBA or WNBA Stats split. |
| `matchup_fga` | numeric | Shooting metric for matchup fga in the requested NBA or WNBA Stats split. |
| `matchup_fg_pct` | numeric | Percentage or rate for matchup field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_fg3_m` | numeric | Shooting metric for matchup fg3m in the requested NBA or WNBA Stats split. |
| `matchup_fg3_a` | numeric | Shooting metric for matchup fg3a in the requested NBA or WNBA Stats split. |
| `matchup_fg3_pct` | numeric | Percentage or rate for matchup three-point field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_ftm` | numeric | NBA or WNBA Stats value for matchup ftm in the matchupsrollup result set. |
| `matchup_fta` | numeric | NBA or WNBA Stats value for matchup fta in the matchupsrollup result set. |
| `sfl` | numeric | NBA or WNBA Stats value for sfl in the matchupsrollup result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_matchupsrollup-example}

```python
nba_stats_matchupsrollup(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_playbyplayv3

GET /stats/playbyplayv3

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv3?EndPeriod=0&GameID=0022201086&StartPeriod=0](https://stats.nba.com/stats/playbyplayv3?EndPeriod=0&GameID=0022201086&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#nba_stats_playbyplayv3-returns}

**`return_parsed=True`** (default) — the output of `parse_nba_stats_result_sets`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parse_nba_stats_result_sets emits no columns for the committed capture tests/fixtures/nba_stats/endpoints/playbyplayv3.json.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playbyplayv3-example}

```python
nba_stats_playbyplayv3()
```

_Last validated n/a._

## nba_stats_playoffpicture

GET /stats/playoffpicture

**Endpoint URL:** `GET https://stats.nba.com/stats/playoffpicture`

**Valid URL:** [https://stats.nba.com/stats/playoffpicture?LeagueID=00&SeasonID=22022](https://stats.nba.com/stats/playoffpicture?LeagueID=00&SeasonID=22022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonID` | `season_id` |  |  | `Y` |  |

### Returns {#nba_stats_playoffpicture-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`EastConfPlayoffPicture`, `WestConfPlayoffPicture`, `EastConfStandings`, `WestConfStandings`, `EastConfRemainingGames`, `WestConfRemainingGames`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**EastConfPlayoffPicture**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `high_seed_rank` | integer |  |
| `high_seed_team` | character |  |
| `high_seed_team_id` | integer |  |
| `low_seed_rank` | integer |  |
| `low_seed_team` | character |  |
| `low_seed_team_id` | integer |  |
| `high_seed_series_w` | integer |  |
| `high_seed_series_l` | integer |  |
| `high_seed_series_remaining_g` | integer |  |
| `high_seed_series_remaining_home_g` | integer |  |
| `high_seed_series_remaining_away_g` | integer |  |

**WestConfPlayoffPicture**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `high_seed_rank` | integer |  |
| `high_seed_team` | character |  |
| `high_seed_team_id` | integer |  |
| `low_seed_rank` | integer |  |
| `low_seed_team` | character |  |
| `low_seed_team_id` | integer |  |
| `high_seed_series_w` | integer |  |
| `high_seed_series_l` | integer |  |
| `high_seed_series_remaining_g` | integer |  |
| `high_seed_series_remaining_home_g` | integer |  |
| `high_seed_series_remaining_away_g` | integer |  |

**EastConfStandings**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `rank` | integer | Rank. |
| `team` | character | Team-side label or team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | integer | Unique team identifier. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `pct` | numeric | Win percentage. |
| `div` | character | Abbreviation of the team's division. |
| `conf` | character | character. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `gb` | numeric | Games behind the conference leader. |
| `gr_over_500` | integer | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | integer | Remaining home games against teams with winning records. |
| `gr_over_500_away` | integer | Remaining road games against teams with winning records. |
| `gr_under_500` | integer | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | integer | Remaining home games against teams with losing records. |
| `gr_under_500_away` | integer | Remaining road games against teams with losing records. |
| `ranking_criteria` | integer | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | integer | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | integer | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | integer | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | integer | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | character | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**WestConfStandings**

| col_name | type | description |
|---|---|---|
| `conference` | character | Conference name. |
| `rank` | integer | Rank. |
| `team` | character | Team-side label or team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_id` | integer | Unique team identifier. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `pct` | numeric | Win percentage. |
| `div` | character | Abbreviation of the team's division. |
| `conf` | character | character. |
| `home` | character | Home. |
| `away` | character | Away record. |
| `gb` | numeric | Games behind the conference leader. |
| `gr_over_500` | integer | Remaining games against teams with winning (over .500) records. |
| `gr_over_500_home` | integer | Remaining home games against teams with winning records. |
| `gr_over_500_away` | integer | Remaining road games against teams with winning records. |
| `gr_under_500` | integer | Remaining games against teams with losing (under .500) records. |
| `gr_under_500_home` | integer | Remaining home games against teams with losing records. |
| `gr_under_500_away` | integer | Remaining road games against teams with losing records. |
| `ranking_criteria` | integer | Code for the ranking or tiebreak criteria applied to the team in the playoff picture. |
| `clinched_playoffs` | integer | Flag (1/0) for whether the team has clinched a playoff berth. |
| `clinched_conference` | integer | Flag (1/0) for whether the team has clinched the conference title. |
| `clinched_division` | integer | Flag (1/0) for whether the team has clinched its division. |
| `eliminated_playoffs` | integer | Flag (1/0) for whether the team has been eliminated from playoff contention. |
| `sosa_remaining` | character | Strength of schedule of the team's remaining opponents (combined opponent winning percentage). |

**EastConfRemainingGames**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `team_id` | integer | Unique team identifier. |
| `remaining_g` | integer |  |
| `remaining_home_g` | integer |  |
| `remaining_away_g` | integer |  |

**WestConfRemainingGames**

| col_name | type | description |
|---|---|---|
| `team` | character | Team-side label or team identifier. |
| `team_id` | integer | Unique team identifier. |
| `remaining_g` | integer |  |
| `remaining_home_g` | integer |  |
| `remaining_away_g` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_playoffpicture-example}

```python
nba_stats_playoffpicture(league_id='00')
```

_Last validated n/a._

## nba_stats_synergyplaytypes

GET /stats/synergyplaytypes

**Endpoint URL:** `GET https://stats.nba.com/stats/synergyplaytypes`

**Valid URL:** [https://stats.nba.com/stats/synergyplaytypes?LeagueID=00&PerMode=PerGame&PlayType=Isolation&PlayerOrTeam=P&SeasonType=Regular+Season&SeasonYear=2024-25&TypeGrouping=Offensive](https://stats.nba.com/stats/synergyplaytypes?LeagueID=00&PerMode=PerGame&PlayType=Isolation&PlayerOrTeam=P&SeasonType=Regular+Season&SeasonYear=2024-25&TypeGrouping=Offensive)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayType` | `play_type_nullable` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `SeasonYear` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time to the latest season that has rows: an NBA season from the November after its late-October tip-off (``2025-26`` until October 2026), a G League season from the January after, a Summer League from its August (July 2026's is ``2026-27``), a draft combine from June, a draft (``drafthistory``, a year) from July, and with season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) a season from the May its playoffs start. A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `TypeGrouping` | `type_grouping_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_synergyplaytypes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | character | Unique season identifier. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `play_type` | character | Play type description. |
| `type_grouping` | character | Whether the play-type row measures the offensive or defensive side. |
| `percentile` | numeric | League percentile of the row's points per possession for the play type, as a decimal. |
| `gp` | integer | Games played. |
| `poss_pct` | numeric | Poss percentage (0-1 decimal). |
| `ppp` | numeric | Points scored per possession on the play type (Synergy play-type tracking). |
| `fg_pct` | numeric | Field goal percentage (0-1). |
| `ft_poss_pct` | numeric | Share of play-type possessions ending in free throws, as a decimal. |
| `tov_poss_pct` | numeric | Share of play-type possessions ending in a turnover, as a decimal. |
| `sf_poss_pct` | numeric | Share of play-type possessions on which a shooting foul was drawn, as a decimal. |
| `plusone_poss_pct` | numeric | Share of play-type possessions producing an and-one, as a decimal. |
| `score_poss_pct` | numeric | Share of play-type possessions on which points were scored, as a decimal. |
| `efg_pct` | numeric | Effective field goal percentage on the play type, as a decimal. |
| `poss` | numeric | Poss. |
| `pts` | numeric | Points scored. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fgmx` | numeric | Field goals missed on the play type. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_synergyplaytypes-example}

```python
nba_stats_synergyplaytypes(league_id='00', season='2024-25')
```

_Last validated n/a._
