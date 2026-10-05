---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Other"
sidebar_label: "Other"
sidebar_position: 26
description: "WNBA — WNBA Stats API (stats.wnba.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Other

## wnba_stats_alltimeleadersgrids

GET /stats/alltimeleadersgrids

**Endpoint URL:** `GET https://stats.wnba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.wnba.com/stats/alltimeleadersgrids?LeagueID=10&PerMode=PerGame&SeasonType=Regular+Season&TopX=10](https://stats.wnba.com/stats/alltimeleadersgrids?LeagueID=10&PerMode=PerGame&SeasonType=Regular+Season&TopX=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `TopX` | `topx` |  |  | `Y` |  |

### Returns {#wnba_stats_alltimeleadersgrids-returns}

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
| `reb` | numeric | Total rebounds. |
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

### Example {#wnba_stats_alltimeleadersgrids-example}

```python
wnba_stats_alltimeleadersgrids(league_id='10')
```

_Last validated n/a._

## wnba_stats_assistleaders

GET /stats/assistleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/assistleaders`

**Valid URL:** [https://stats.wnba.com/stats/assistleaders?LeagueID=10&PerMode=PerGame&PlayerOrTeam=Team&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/assistleaders?LeagueID=10&PerMode=PerGame&PlayerOrTeam=Team&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |

### Returns {#wnba_stats_assistleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `player_id` | integer | Unique player identifier. |
| `player` | character | Player name. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | character | Jersey number worn by the player. |
| `player_position` | character | Position of the player accordinng to NGS |
| `ast` | numeric | Assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_assistleaders-example}

```python
wnba_stats_assistleaders(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_assisttracker

GET /stats/assisttracker

**Endpoint URL:** `GET https://stats.wnba.com/stats/assisttracker`

**Valid URL:** [https://stats.wnba.com/stats/assisttracker?LeagueID=10&OpponentTeamID=0&PerMode=PerGame&Season=2024&SeasonType=Regular+Season&TeamID=0](https://stats.wnba.com/stats/assisttracker?LeagueID=10&OpponentTeamID=0&PerMode=PerGame&Season=2024&SeasonType=Regular+Season&TeamID=0)

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
| `Season` | `season_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` |  |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_assisttracker-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_assisttracker-example}

```python
wnba_stats_assisttracker(league_id='10', season_nullable='2024')
```

_Last validated n/a._

## wnba_stats_draftcombinestats

GET /stats/draftcombinestats

**Endpoint URL:** `GET https://stats.wnba.com/stats/draftcombinestats`

**Valid URL:** [https://stats.wnba.com/stats/draftcombinestats?LeagueID=10&SeasonYear=2024](https://stats.wnba.com/stats/draftcombinestats?LeagueID=10&SeasonYear=2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_all_time` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |

### Returns {#wnba_stats_draftcombinestats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `player_id` | integer | Unique player identifier. |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `player_name` | character | Player name. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height_wo_shoes` | numeric | Height measured without shoes, in inches. |
| `height_wo_shoes_ft_in` | character | Height without shoes formatted as feet and inches. |
| `height_w_shoes` | character | Height measured with shoes, in inches. |
| `height_w_shoes_ft_in` | character | Height with shoes formatted as feet and inches. |
| `weight` | character | Player weight in pounds. |
| `wingspan` | numeric | Wingspan measured at the combine, in inches. |
| `wingspan_ft_in` | character | Wingspan formatted as feet and inches. |
| `standing_reach` | numeric | Standing reach measured at the combine, in inches. |
| `standing_reach_ft_in` | character | Standing reach formatted as feet and inches. |
| `body_fat_pct` | character | Body fat percentage measured at the combine. |
| `hand_length` | numeric | Hand length measured at the combine, in inches. |
| `hand_width` | numeric | Hand width measured at the combine, in inches. |
| `standing_vertical_leap` | numeric | Standing (no-step) vertical leap, in inches. |
| `max_vertical_leap` | numeric | Maximum (running) vertical leap, in inches. |
| `lane_agility_time` | numeric | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | numeric | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | numeric | Three-quarter-court sprint time, in seconds. |
| `bench_press` | character | Repetitions of 185 pounds completed on the bench press. |
| `spot_fifteen_corner_left` | character | Made-attempted result (e.g. "3-5") from the 15-foot left corner spot-up shooting station at the combine. |
| `spot_fifteen_break_left` | character | Made-attempted result (e.g. "3-5") from the 15-foot left wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_top_key` | character | Made-attempted result (e.g. "3-5") from the 15-foot top of the key spot-up shooting station at the combine. |
| `spot_fifteen_break_right` | character | Made-attempted result (e.g. "3-5") from the 15-foot right wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_corner_right` | character | Made-attempted result (e.g. "3-5") from the 15-foot right corner spot-up shooting station at the combine. |
| `spot_college_corner_left` | character | Made-attempted result (e.g. "3-5") from the college three-point left corner spot-up shooting station at the combine. |
| `spot_college_break_left` | character | Made-attempted result (e.g. "3-5") from the college three-point left wing (break) spot-up shooting station at the combine. |
| `spot_college_top_key` | character | Made-attempted result (e.g. "3-5") from the college three-point top of the key spot-up shooting station at the combine. |
| `spot_college_break_right` | character | Made-attempted result (e.g. "3-5") from the college three-point right wing (break) spot-up shooting station at the combine. |
| `spot_college_corner_right` | character | Made-attempted result (e.g. "3-5") from the college three-point right corner spot-up shooting station at the combine. |
| `spot_nba_corner_left` | character | Made-attempted result (e.g. "3-5") from the NBA three-point left corner spot-up shooting station at the combine. |
| `spot_nba_break_left` | character | Made-attempted result (e.g. "3-5") from the NBA three-point left wing (break) spot-up shooting station at the combine. |
| `spot_nba_top_key` | character | Made-attempted result (e.g. "3-5") from the NBA three-point top of the key spot-up shooting station at the combine. |
| `spot_nba_break_right` | character | Made-attempted result (e.g. "3-5") from the NBA three-point right wing (break) spot-up shooting station at the combine. |
| `spot_nba_corner_right` | character | Made-attempted result (e.g. "3-5") from the NBA three-point right corner spot-up shooting station at the combine. |
| `off_drib_fifteen_break_left` | character | Made-attempted result from the 15-foot left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_top_key` | character | Made-attempted result from the 15-foot top of the key off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_break_right` | character | Made-attempted result from the 15-foot right wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_break_left` | character | Made-attempted result from the college three-point left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_top_key` | character | Made-attempted result from the college three-point top of the key off-the-dribble shooting station at the combine. |
| `off_drib_college_break_right` | character | Made-attempted result from the college three-point right wing (break) off-the-dribble shooting station at the combine. |
| `on_move_fifteen` | character | Made-attempted result from the 15-foot shooting-on-the-move station at the combine. |
| `on_move_college` | character | Made-attempted result from the college three-point shooting-on-the-move station at the combine. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_draftcombinestats-example}

```python
wnba_stats_draftcombinestats(league_id='10', season_all_time='2024')
```

_Last validated n/a._

## wnba_stats_drafthistory

GET /stats/drafthistory

**Endpoint URL:** `GET https://stats.wnba.com/stats/drafthistory`

**Valid URL:** [https://stats.wnba.com/stats/drafthistory?College=&LeagueID=10&OverallPick=&RoundNum=&RoundPick=&Season=2024&TeamID=0&TopX=](https://stats.wnba.com/stats/drafthistory?College=&LeagueID=10&OverallPick=&RoundNum=&RoundPick=&Season=2024&TeamID=0&TopX=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OverallPick` | `overall_pick_nullable` |  |  | `Y` |  |
| `RoundNum` | `round_num_nullable` |  |  | `Y` |  |
| `RoundPick` | `round_pick_nullable` |  |  | `Y` |  |
| `Season` | `season_year_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TopX` | `topx_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_drafthistory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_name` | character | Player name. |
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
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

### Example {#wnba_stats_drafthistory-example}

```python
wnba_stats_drafthistory(league_id='10', season_year_nullable='2024')
```

_Last validated n/a._

## wnba_stats_fantasywidget

GET /stats/fantasywidget

**Endpoint URL:** `GET https://stats.wnba.com/stats/fantasywidget`

**Valid URL:** [https://stats.wnba.com/stats/fantasywidget?ActivePlayers=N&DateFrom=&DateTo=&LastNGames=0&LeagueID=10&Location=&Month=&OpponentTeamID=0&PORound=&PlayerID=&Position=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&TodaysOpponent=0&TodaysPlayers=N&VsConference=&VsDivision=](https://stats.wnba.com/stats/fantasywidget?ActivePlayers=N&DateFrom=&DateTo=&LastNGames=0&LeagueID=10&Location=&Month=&OpponentTeamID=0&PORound=&PlayerID=&Position=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&TodaysOpponent=0&TodaysPlayers=N&VsConference=&VsDivision=)

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
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TodaysOpponent` | `todays_opponent` |  |  | `Y` |  |
| `TodaysPlayers` | `todays_players` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_fantasywidget-returns}

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
| `reb` | numeric | Total rebounds. |
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

### Example {#wnba_stats_fantasywidget-example}

```python
wnba_stats_fantasywidget(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_gamerotation

GET /stats/gamerotation

**Endpoint URL:** `GET https://stats.wnba.com/stats/gamerotation`

**Valid URL:** [https://stats.wnba.com/stats/gamerotation?GameID=1022200034&LeagueID=10](https://stats.wnba.com/stats/gamerotation?GameID=1022200034&LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_gamerotation-returns}

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

### Example {#wnba_stats_gamerotation-example}

```python
wnba_stats_gamerotation(league_id='10')
```

_Last validated n/a._

## wnba_stats_homepageleaders

GET /stats/homepageleaders

**Endpoint URL:** `GET https://stats.wnba.com/stats/homepageleaders`

**Valid URL:** [https://stats.wnba.com/stats/homepageleaders?GameScope=Season&LeagueID=10&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024&SeasonType=Regular+Season&StatCategory=Points](https://stats.wnba.com/stats/homepageleaders?GameScope=Season&LeagueID=10&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024&SeasonType=Regular+Season&StatCategory=Points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatCategory` | `stat_category` |  |  | `Y` |  |

### Returns {#wnba_stats_homepageleaders-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageLeaders`, `LeagueAverage`, `LeagueMax`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageLeaders**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
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

### Example {#wnba_stats_homepageleaders-example}

```python
wnba_stats_homepageleaders(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_homepagev2

GET /stats/homepagev2

**Endpoint URL:** `GET https://stats.wnba.com/stats/homepagev2`

**Valid URL:** [https://stats.wnba.com/stats/homepagev2?GameScope=Season&LeagueID=10&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024&SeasonType=Regular+Season&StatType=Traditional](https://stats.wnba.com/stats/homepagev2?GameScope=Season&LeagueID=10&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024&SeasonType=Regular+Season&StatType=Traditional)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `StatType` | `stat_type` |  |  | `Y` |  |

### Returns {#wnba_stats_homepagev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageStat1`, `HomePageStat2`, `HomePageStat3`, `HomePageStat4`, `HomePageStat5`, `HomePageStat6`, `HomePageStat7`, `HomePageStat8`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageStat1**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | numeric | Points scored. |

**HomePageStat2**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `reb` | numeric | Total rebounds. |

**HomePageStat3**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `ast` | numeric | Assists. |

**HomePageStat4**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `stl` | numeric | Steals. |

**HomePageStat5**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**HomePageStat6**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `ft_pct` | numeric | Free throw percentage (0-1). |

**HomePageStat7**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `fg3_pct` | numeric | Three-point field goal percentage (0-1). |

**HomePageStat8**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `blk` | numeric | Blocks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_homepagev2-example}

```python
wnba_stats_homepagev2(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_hustlestatsboxscore

GET /stats/hustlestatsboxscore

**Endpoint URL:** `GET https://stats.wnba.com/stats/hustlestatsboxscore`

**Valid URL:** [https://stats.wnba.com/stats/hustlestatsboxscore](https://stats.wnba.com/stats/hustlestatsboxscore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#wnba_stats_hustlestatsboxscore-returns}

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

### Example {#wnba_stats_hustlestatsboxscore-example}

```python
wnba_stats_hustlestatsboxscore()
```

_Last validated n/a._

## wnba_stats_infographicfanduelplayer

GET /stats/infographicfanduelplayer

**Endpoint URL:** `GET https://stats.wnba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.wnba.com/stats/infographicfanduelplayer?GameID=1022200034](https://stats.wnba.com/stats/infographicfanduelplayer?GameID=1022200034)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#wnba_stats_infographicfanduelplayer-returns}

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
| `location` | character | Filter results by game location. |
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
| `reb` | integer | Total rebounds. |
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

### Example {#wnba_stats_infographicfanduelplayer-example}

```python
wnba_stats_infographicfanduelplayer()
```

_Last validated n/a._

## wnba_stats_leaderstiles

GET /stats/leaderstiles

**Endpoint URL:** `GET https://stats.wnba.com/stats/leaderstiles`

**Valid URL:** [https://stats.wnba.com/stats/leaderstiles?GameScope=Season&LeagueID=10&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024&SeasonType=Regular+Season&Stat=PTS](https://stats.wnba.com/stats/leaderstiles?GameScope=Season&LeagueID=10&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024&SeasonType=Regular+Season&Stat=PTS)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time to the latest WNBA season that has rows: the current year from June (``2026`` from June 2026, ``2025`` before), a draft (``drafthistory``) from May, and with season type ``Playoffs`` (or ``commonplayoffseries``) from October. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` |  |
| `Stat` | `stat` |  |  | `Y` |  |

### Returns {#wnba_stats_leaderstiles-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`LeadersTiles`, `AllTimeSeasonHigh`, `LastSeasonHigh`, `LowSeasonHigh`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**LeadersTiles**

| col_name | type | description |
|---|---|---|
| `rank` | integer | Whether to include statistical ranks in the returned table. |
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
| `rank` | integer | Whether to include statistical ranks in the returned table. |
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

### Example {#wnba_stats_leaderstiles-example}

```python
wnba_stats_leaderstiles(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_playbyplayv2

GET /stats/playbyplayv2

**Endpoint URL:** `GET https://stats.wnba.com/stats/playbyplayv2`

**Valid URL:** [https://stats.wnba.com/stats/playbyplayv2?EndPeriod=0&GameID=1022200034&StartPeriod=0](https://stats.wnba.com/stats/playbyplayv2?EndPeriod=0&GameID=1022200034&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#wnba_stats_playbyplayv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayByPlay`, `AvailableVideo`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayByPlay**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `eventnum` | integer | Sequential event number within the game's play-by-play feed. |
| `eventmsgtype` | integer | Numeric event type code (1 = made shot, 2 = missed shot, 3 = free throw, 4 = rebound, 5 = turnover, 6 = foul, ...). |
| `eventmsgactiontype` | integer | Numeric sub-type code refining eventmsgtype (e.g. the specific shot, foul, or turnover variety). |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `wctimestring` | character | Wall-clock time of day when the event occurred. |
| `pctimestring` | character | Game clock remaining in the period when the event occurred (MM:SS). |
| `homedescription` | character | Text description of the event from the home team's perspective; empty when not a home-team action. |
| `neutraldescription` | character | Neutral text description of the event (e.g. period start/end); empty for team actions. |
| `visitordescription` | character | Text description of the event from the visiting team's perspective; empty when not a visitor action. |
| `score` | character | Final score. |
| `scoremargin` | character | Score margin after the event ('TIE' when tied); empty on non-scoring events. |
| `person1_type` | integer |  |
| `player1_id` | integer | V2 PBP primary player ID (e.g. shooter / fouler). |
| `player1_name` | character | V2 PBP primary player name. |
| `player1_team_id` | integer | Team ID of player1. |
| `player1_team_city` | character | Player1 team city. |
| `player1_team_nickname` | character | Player1 team nickname. |
| `player1_team_abbreviation` | character | Player1 team abbreviation. |
| `person2_type` | integer |  |
| `player2_id` | integer | V2 PBP secondary player ID (e.g. assister / fouled-by). |
| `player2_name` | character | V2 PBP secondary player name. |
| `player2_team_id` | integer | Team ID of player2. |
| `player2_team_city` | character | Player2 team city. |
| `player2_team_nickname` | character | Player2 team nickname. |
| `player2_team_abbreviation` | character | Player2 team abbreviation. |
| `person3_type` | integer |  |
| `player3_id` | integer | V2 PBP tertiary player ID (e.g. blocker). |
| `player3_name` | character | V2 PBP tertiary player name. |
| `player3_team_id` | integer | Team ID of player3. |
| `player3_team_city` | character | Player3 team city. |
| `player3_team_nickname` | character | Player3 team nickname. |
| `player3_team_abbreviation` | character | Player3 team abbreviation. |
| `video_available_flag` | integer | Video available flag. |

**AvailableVideo**

| col_name | type | description |
|---|---|---|
| `video_available_flag` | integer | Video available flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playbyplayv2-example}

```python
wnba_stats_playbyplayv2()
```

_Last validated n/a._

## wnba_stats_playbyplayv3

GET /stats/playbyplayv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.wnba.com/stats/playbyplayv3?EndPeriod=0&GameID=1022200034&StartPeriod=0](https://stats.wnba.com/stats/playbyplayv3?EndPeriod=0&GameID=1022200034&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#wnba_stats_playbyplayv3-returns}

**`return_parsed=True`** (default) — the output of `parse_wnba_stats_result_sets`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parse_nba_stats_result_sets emits no columns for the committed capture tests/fixtures/wnba_stats/endpoints/playbyplayv3.json.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playbyplayv3-example}

```python
wnba_stats_playbyplayv3()
```

_Last validated n/a._

## wnba_stats_videostatus

GET /stats/videostatus

**Endpoint URL:** `GET https://stats.wnba.com/stats/videostatus`

**Valid URL:** [https://stats.wnba.com/stats/videostatus?GameDate=2022-06-10&LeagueID=10](https://stats.wnba.com/stats/videostatus?GameDate=2022-06-10&LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_videostatus-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `visitor_team_city` | character | City name of the visiting team. |
| `visitor_team_name` | character | Nickname of the visiting team. |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_city` | character | Home team city / location. |
| `home_team_name` | character | Home team name. |
| `home_team_abbreviation` | character | Home team abbreviation. |
| `game_status` | integer | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | integer | Flag indicating whether game video is available in the league's stats video system. |
| `pt_xyz_available` | integer | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_videostatus-example}

```python
wnba_stats_videostatus(league_id='10')
```

_Last validated n/a._
