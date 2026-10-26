---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Player: playergamelogs–playervsplayer"
sidebar_label: "Player: playergamelogs–playervsplayer"
sidebar_position: 10
description: "WNBA — WNBA Stats API (stats.wnba.com) — Player: playergamelogs–playervsplayer — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Player: playergamelogs–playervsplayer

## wnba_stats_playergamelogs

Player game logs.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamelogs`

**Valid URL:** [https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

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
| `OppTeamID` | `oppteamid` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playergamelogs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_year` | String | Season year string ('YYYY-YY' format). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `nickname` | String | Team or athlete nickname. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 | Personal fouls. |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
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
| `available_flag` | Int64 | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |
| `min_sec` | String | Minutes and seconds played, formatted as a game-clock duration string. |
| `team_count` | Int64 | NBA or WNBA Stats value for team count in the playergamelogs result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playergamelogs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playergamelogs-example}

```python
wnba_stats_playergamelogs(league_id='10', season_nullable='2024')
```

## wnba_stats_playergamestreakfinder

Player game streak finder.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamestreakfinder`

**Valid URL:** [https://stats.wnba.com/stats/playergamestreakfinder?ActiveStreaksOnly=&Conference=&DateFrom=&DateTo=&Division=&DraftNumber=&DraftRound=&DraftTeamID=0&DraftYear=&EqAST=&EqBLK=&EqDD=&EqDREB=&EqFG3A=&EqFG3M=&EqFG3_PCT=&EqFGA=&EqFGM=&EqFG_PCT=&EqFTA=&EqFTM=&EqFT_PCT=&EqMINUTES=&EqOREB=&EqPF=&EqPTS=&EqREB=&EqSTL=&EqTD=&EqTOV=&GameID=&GtAST=&GtBLK=&GtDD=&GtDREB=&GtFG3A=&GtFG3M=&GtFG3_PCT=&GtFGA=&GtFGM=&GtFG_PCT=&GtFTA=&GtFTM=&GtFT_PCT=&GtMINUTES=&GtOREB=&GtPF=&GtPTS=&GtREB=&GtSTL=&GtTD=&GtTOV=&LeagueID=10&Location=&LtAST=&LtBLK=&LtDD=&LtDREB=&LtFG3A=&LtFG3M=&LtFG3_PCT=&LtFGA=&LtFGM=&LtFG_PCT=&LtFTA=&LtFTM=&LtFT_PCT=&LtMINUTES=&LtOREB=&LtPF=&LtPTS=&LtREB=&LtSTL=&LtTD=&LtTOV=&MinGames=&Outcome=&PORound=&PlayerID=&RookieYear=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&VsTeamID=0&YearsExperience=](https://stats.wnba.com/stats/playergamestreakfinder?ActiveStreaksOnly=&Conference=&DateFrom=&DateTo=&Division=&DraftNumber=&DraftRound=&DraftTeamID=0&DraftYear=&EqAST=&EqBLK=&EqDD=&EqDREB=&EqFG3A=&EqFG3M=&EqFG3_PCT=&EqFGA=&EqFGM=&EqFG_PCT=&EqFTA=&EqFTM=&EqFT_PCT=&EqMINUTES=&EqOREB=&EqPF=&EqPTS=&EqREB=&EqSTL=&EqTD=&EqTOV=&GameID=&GtAST=&GtBLK=&GtDD=&GtDREB=&GtFG3A=&GtFG3M=&GtFG3_PCT=&GtFGA=&GtFGM=&GtFG_PCT=&GtFTA=&GtFTM=&GtFT_PCT=&GtMINUTES=&GtOREB=&GtPF=&GtPTS=&GtREB=&GtSTL=&GtTD=&GtTOV=&LeagueID=10&Location=&LtAST=&LtBLK=&LtDD=&LtDREB=&LtFG3A=&LtFG3M=&LtFG3_PCT=&LtFGA=&LtFGM=&LtFG_PCT=&LtFTA=&LtFTM=&LtFT_PCT=&LtMINUTES=&LtOREB=&LtPF=&LtPTS=&LtREB=&LtSTL=&LtTD=&LtTOV=&MinGames=&Outcome=&PORound=&PlayerID=&RookieYear=&Season=2024&SeasonSegment=&SeasonType=Regular+Season&StarterBench=&TeamID=0&VsConference=&VsDivision=&VsTeamID=0&YearsExperience=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActiveStreaksOnly` | `active_streaks_only_nullable` |  |  | `Y` |  |
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `DraftNumber` | `draft_number_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftTeamID` | `draft_team_id_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `EqAST` | `eq_ast_nullable` |  |  | `Y` |  |
| `EqBLK` | `eq_blk_nullable` |  |  | `Y` |  |
| `EqDD` | `eq_dd_nullable` |  |  | `Y` |  |
| `EqDREB` | `eq_dreb_nullable` |  |  | `Y` |  |
| `EqFG3A` | `eq_fg3a_nullable` |  |  | `Y` |  |
| `EqFG3M` | `eq_fg3m_nullable` |  |  | `Y` |  |
| `EqFG3_PCT` | `eq_fg3_pct_nullable` |  |  | `Y` |  |
| `EqFGA` | `eq_fga_nullable` |  |  | `Y` |  |
| `EqFGM` | `eq_fgm_nullable` |  |  | `Y` |  |
| `EqFG_PCT` | `eq_fg_pct_nullable` |  |  | `Y` |  |
| `EqFTA` | `eq_fta_nullable` |  |  | `Y` |  |
| `EqFTM` | `eq_ftm_nullable` |  |  | `Y` |  |
| `EqFT_PCT` | `eq_ft_pct_nullable` |  |  | `Y` |  |
| `EqMINUTES` | `eq_minutes_nullable` |  |  | `Y` |  |
| `EqOREB` | `eq_oreb_nullable` |  |  | `Y` |  |
| `EqPF` | `eq_pf_nullable` |  |  | `Y` |  |
| `EqPTS` | `eq_pts_nullable` |  |  | `Y` |  |
| `EqREB` | `eq_reb_nullable` |  |  | `Y` |  |
| `EqSTL` | `eq_stl_nullable` |  |  | `Y` |  |
| `EqTD` | `eq_td_nullable` |  |  | `Y` |  |
| `EqTOV` | `eq_tov_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GtAST` | `gt_ast_nullable` |  |  | `Y` |  |
| `GtBLK` | `gt_blk_nullable` |  |  | `Y` |  |
| `GtDD` | `gt_dd_nullable` |  |  | `Y` |  |
| `GtDREB` | `gt_dreb_nullable` |  |  | `Y` |  |
| `GtFG3A` | `gt_fg3a_nullable` |  |  | `Y` |  |
| `GtFG3M` | `gt_fg3m_nullable` |  |  | `Y` |  |
| `GtFG3_PCT` | `gt_fg3_pct_nullable` |  |  | `Y` |  |
| `GtFGA` | `gt_fga_nullable` |  |  | `Y` |  |
| `GtFGM` | `gt_fgm_nullable` |  |  | `Y` |  |
| `GtFG_PCT` | `gt_fg_pct_nullable` |  |  | `Y` |  |
| `GtFTA` | `gt_fta_nullable` |  |  | `Y` |  |
| `GtFTM` | `gt_ftm_nullable` |  |  | `Y` |  |
| `GtFT_PCT` | `gt_ft_pct_nullable` |  |  | `Y` |  |
| `GtMINUTES` | `gt_minutes_nullable` |  |  | `Y` |  |
| `GtOREB` | `gt_oreb_nullable` |  |  | `Y` |  |
| `GtPF` | `gt_pf_nullable` |  |  | `Y` |  |
| `GtPTS` | `gt_pts_nullable` |  |  | `Y` |  |
| `GtREB` | `gt_reb_nullable` |  |  | `Y` |  |
| `GtSTL` | `gt_stl_nullable` |  |  | `Y` |  |
| `GtTD` | `gt_td_nullable` |  |  | `Y` |  |
| `GtTOV` | `gt_tov_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LtAST` | `lt_ast_nullable` |  |  | `Y` |  |
| `LtBLK` | `lt_blk_nullable` |  |  | `Y` |  |
| `LtDD` | `lt_dd_nullable` |  |  | `Y` |  |
| `LtDREB` | `lt_dreb_nullable` |  |  | `Y` |  |
| `LtFG3A` | `lt_fg3a_nullable` |  |  | `Y` |  |
| `LtFG3M` | `lt_fg3m_nullable` |  |  | `Y` |  |
| `LtFG3_PCT` | `lt_fg3_pct_nullable` |  |  | `Y` |  |
| `LtFGA` | `lt_fga_nullable` |  |  | `Y` |  |
| `LtFGM` | `lt_fgm_nullable` |  |  | `Y` |  |
| `LtFG_PCT` | `lt_fg_pct_nullable` |  |  | `Y` |  |
| `LtFTA` | `lt_fta_nullable` |  |  | `Y` |  |
| `LtFTM` | `lt_ftm_nullable` |  |  | `Y` |  |
| `LtFT_PCT` | `lt_ft_pct_nullable` |  |  | `Y` |  |
| `LtMINUTES` | `lt_minutes_nullable` |  |  | `Y` |  |
| `LtOREB` | `lt_oreb_nullable` |  |  | `Y` |  |
| `LtPF` | `lt_pf_nullable` |  |  | `Y` |  |
| `LtPTS` | `lt_pts_nullable` |  |  | `Y` |  |
| `LtREB` | `lt_reb_nullable` |  |  | `Y` |  |
| `LtSTL` | `lt_stl_nullable` |  |  | `Y` |  |
| `LtTD` | `lt_td_nullable` |  |  | `Y` |  |
| `LtTOV` | `lt_tov_nullable` |  |  | `Y` |  |
| `MinGames` | `min_games_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsTeamID` | `vs_team_id_nullable` |  |  | `Y` |  |
| `YearsExperience` | `years_experience_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playergamestreakfinder-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_name_last_first` | String | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `player_id` | Int64 | Unique player identifier. |
| `gamestreak` | Int64 | NBA or WNBA Stats value for gamestreak in the playergamestreakfinder result set. |
| `startdate` | String | Date or timestamp for startdate in the NBA or WNBA Stats result set. |
| `enddate` | String | Date or timestamp for enddate in the NBA or WNBA Stats result set. |
| `activestreak` | Int64 | NBA or WNBA Stats value for activestreak in the playergamestreakfinder result set. |
| `numseasons` | Int64 | NBA or WNBA Stats value for numseasons in the playergamestreakfinder result set. |
| `lastseason` | String | NBA or WNBA Stats value for lastseason in the playergamestreakfinder result set. |
| `firstseason` | String | NBA or WNBA Stats value for firstseason in the playergamestreakfinder result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playergamestreakfinder-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playergamestreakfinder-example}

```python
wnba_stats_playergamestreakfinder(league_id='10', season_nullable='2024')
```

## wnba_stats_playerindex

Player index.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerindex`

**Valid URL:** [https://stats.wnba.com/stats/playerindex?College=&Country=&DraftPick=&DraftRound=&DraftYear=&Height=&Historical=1&LeagueID=10&Season=2024&TeamID=0&Weight=](https://stats.wnba.com/stats/playerindex?College=&Country=&DraftPick=&DraftRound=&DraftYear=&Height=&Historical=1&LeagueID=10&Season=2024&TeamID=0&Weight=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Active` | `active_nullable` |  |  | `Y` |  |
| `AllStar` | `allstar_nullable` |  |  | `Y` |  |
| `College` | `college_nullable` |  |  | `Y` |  |
| `Country` | `country_nullable` |  |  | `Y` |  |
| `DraftPick` | `draft_pick_nullable` |  |  | `Y` |  |
| `DraftRound` | `draft_round_nullable` |  |  | `Y` |  |
| `DraftYear` | `draft_year_nullable` |  |  | `Y` |  |
| `Height` | `height_nullable` |  |  | `Y` |  |
| `Historical` | `historical_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#wnba_stats_playerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_last_name` | String | Player's last name. |
| `player_first_name` | String | Player's first name. |
| `player_slug` | String | URL-safe player identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `jersey_number` | String | Jersey number. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `college` | String | College or school attended. |
| `country` | String | Country (full name or code). |
| `draft_year` | Int64 | Draft year (4-digit). |
| `draft_round` | Int64 | Round of the draft selection. |
| `draft_number` | Int64 | Overall pick the player was selected with, as a string; 'Undrafted' when never drafted. |
| `roster_status` | Float64 | Roster status flag: 1 when the player is on a current roster, 0 otherwise. |
| `from_year` | String | First season. |
| `to_year` | String | Most recent season. |
| `pts` | Float64 | Points scored. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stats_timeframe` | String | Time value for stats timeframe in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playerindex-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playerindex-example}

```python
wnba_stats_playerindex(league_id='10', season='2024')
```

## wnba_stats_playernextngames

Playernextngames.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playernextngames`

**Valid URL:** [https://stats.wnba.com/stats/playernextngames?LeagueID=10&NumberOfGames=2147483647&PlayerID=1628932&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/playernextngames?LeagueID=10&NumberOfGames=2147483647&PlayerID=1628932&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `NumberOfGames` | `number_of_games` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season_all` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_playernextngames-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `home_team_name` | String | Home team name. |
| `visitor_team_name` | String | Full name of the visiting team in the upcoming game. |
| `home_team_abbreviation` | String | Home team's three-letter tricode (e.g. 'LAL'). |
| `visitor_team_abbreviation` | String | Abbreviation of the visiting team in the upcoming game. |
| `home_team_nickname` | String | Home team's nickname (e.g. 'Lakers'). |
| `visitor_team_nickname` | String | Nickname of the visiting team in the upcoming game. |
| `game_time` | String | Game start time. |
| `home_wl` | String | Home team's win-loss record entering the upcoming game. |
| `visitor_wl` | String | Visiting team's win-loss record entering the upcoming game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playernextngames-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playernextngames-example}

```python
wnba_stats_playernextngames(league_id='10', season_all='2024')
```

## wnba_stats_playerprofilev2

Player profile v2.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerprofilev2`

**Valid URL:** [https://stats.wnba.com/stats/playerprofilev2?LeagueID=10&PerMode=Totals&PlayerID=1628932](https://stats.wnba.com/stats/playerprofilev2?LeagueID=10&PerMode=Totals&PlayerID=1628932)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playerprofilev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonTotalsRegularSeason`, `CareerTotalsRegularSeason`, `SeasonTotalsPostSeason`, `CareerTotalsPostSeason`, `SeasonTotalsAllStarSeason`, `CareerTotalsAllStarSeason`, `SeasonTotalsCollegeSeason`, `CareerTotalsCollegeSeason`, `SeasonTotalsPreseason`, `CareerTotalsPreseason`, `SeasonRankingsRegularSeason`, `SeasonRankingsPostSeason`, `SeasonHighs`, `CareerHighs`, `NextGame`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**SeasonTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**CareerTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**SeasonTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**CareerTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**SeasonTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**CareerTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**SeasonTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `organization_id` | String |  |
| `school_name` | String | College or school of the season row (SeasonTotalsCollegeSeason set). |
| `player_age` | String | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `min` | String | Minutes played. |
| `fgm` | String | Field goals made. |
| `fga` | String | Field goal attempts. |
| `fg_pct` | String | Field goal percentage (0-1). |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String | Three-point field goal percentage (0-1). |
| `ftm` | String | Free throws made. |
| `fta` | String | Free throw attempts. |
| `ft_pct` | String | Free throw percentage (0-1). |
| `oreb` | String | Offensive rebounds. |
| `dreb` | String | Defensive rebounds. |
| `reb` | String | Total rebounds. |
| `ast` | String | Assists. |
| `stl` | String | Steals. |
| `blk` | String | Blocks. |
| `tov` | String | Turnovers. |
| `pf` | String | Personal fouls. |
| `pts` | String | Points scored. |

**CareerTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | String | Unique player identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `organization_id` | String |  |
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `min` | String | Minutes played. |
| `fgm` | String | Field goals made. |
| `fga` | String | Field goal attempts. |
| `fg_pct` | String | Field goal percentage (0-1). |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String | Three-point field goal percentage (0-1). |
| `ftm` | String | Free throws made. |
| `fta` | String | Free throw attempts. |
| `ft_pct` | String | Free throw percentage (0-1). |
| `oreb` | String | Offensive rebounds. |
| `dreb` | String | Defensive rebounds. |
| `reb` | String | Total rebounds. |
| `ast` | String | Assists. |
| `stl` | String | Steals. |
| `blk` | String | Blocks. |
| `tov` | String | Turnovers. |
| `pf` | String | Personal fouls. |
| `pts` | String | Points scored. |

**SeasonTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**CareerTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `gp` | Int64 | Games played. |
| `gs` | Int64 | Games started. |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `pf` | Float64 | Personal fouls. |
| `pts` | Float64 | Points scored. |

**SeasonRankingsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | String | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `rank_pg_min` | Int64 |  |
| `rank_pg_fgm` | Int64 |  |
| `rank_pg_fga` | Int64 |  |
| `rank_fg_pct` | Int64 |  |
| `rank_pg_fg3_m` | Int64 |  |
| `rank_pg_fg3_a` | Int64 |  |
| `rank_fg3_pct` | Int64 |  |
| `rank_pg_ftm` | Int64 |  |
| `rank_pg_fta` | Int64 |  |
| `rank_ft_pct` | Int64 |  |
| `rank_pg_oreb` | Int64 |  |
| `rank_pg_dreb` | Int64 |  |
| `rank_pg_reb` | Int64 |  |
| `rank_pg_ast` | Int64 |  |
| `rank_pg_stl` | Int64 |  |
| `rank_pg_blk` | Int64 |  |
| `rank_pg_tov` | Int64 |  |
| `rank_pg_pts` | Int64 |  |
| `rank_pg_eff` | Int64 |  |

**SeasonRankingsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | String | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `rank_pg_min` | Int64 |  |
| `rank_pg_fgm` | Int64 |  |
| `rank_pg_fga` | Int64 |  |
| `rank_fg_pct` | Int64 |  |
| `rank_pg_fg3_m` | Int64 |  |
| `rank_pg_fg3_a` | Int64 |  |
| `rank_fg3_pct` | Int64 |  |
| `rank_pg_ftm` | Int64 |  |
| `rank_pg_fta` | Int64 |  |
| `rank_ft_pct` | Int64 |  |
| `rank_pg_oreb` | Int64 |  |
| `rank_pg_dreb` | Int64 |  |
| `rank_pg_reb` | Int64 |  |
| `rank_pg_ast` | Int64 |  |
| `rank_pg_stl` | Int64 |  |
| `rank_pg_blk` | Int64 |  |
| `rank_pg_tov` | Int64 |  |
| `rank_pg_pts` | Int64 |  |
| `rank_pg_eff` | Int64 |  |

**SeasonHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `vs_team_id` | Int64 |  |
| `vs_team_city` | String |  |
| `vs_team_name` | String |  |
| `vs_team_abbreviation` | String |  |
| `stat` | String | Stat. |
| `stat_value` | Int64 | Stat value. |
| `stat_order` | Int64 |  |
| `date_est` | String |  |

**CareerHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `vs_team_id` | Int64 |  |
| `vs_team_city` | String |  |
| `vs_team_name` | String |  |
| `vs_team_abbreviation` | String |  |
| `stat` | String | Stat. |
| `stat_value` | Int64 | Stat value. |
| `stat_order` | Int64 |  |
| `date_est` | String |  |

**NextGame**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `game_time` | String | Game start time. |
| `location` | String | Filter results by game location. |
| `player_team_id` | Int64 | Team id of the player's current team in the NextGame set. |
| `player_team_city` | String |  |
| `player_team_nickname` | String |  |
| `player_team_abbreviation` | String |  |
| `vs_team_id` | Int64 |  |
| `vs_team_city` | String |  |
| `vs_team_nickname` | String |  |
| `vs_team_abbreviation` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playerprofilev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playerprofilev2-example}

```python
wnba_stats_playerprofilev2(league_id='10')
```

## wnba_stats_playervsplayer

Player vs player.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playervsplayer`

**Valid URL:** [https://stats.wnba.com/stats/playervsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=1628932&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&VsConference=&VsDivision=&VsPlayerID=1629488](https://stats.wnba.com/stats/playervsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=1628932&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&VsConference=&VsDivision=&VsPlayerID=1629488)

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
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playervsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `OnOffCourt`, `ShotDistanceOverall`, `ShotDistanceOnCourt`, `ShotDistanceOffCourt`, `ShotAreaOverall`, `ShotAreaOnCourt`, `ShotAreaOffCourt`, `PlayerInfo`, `VsPlayerInfo`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String |  |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 | Personal fouls. |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Float64 | Free throws made. |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Float64 | Offensive rebounds. |
| `dreb` | Float64 | Defensive rebounds. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `tov` | Float64 | Turnovers. |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 | Personal fouls. |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 | Points scored. |
| `plus_minus` | Float64 | Plus/minus point differential while on court. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String |  |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String |  |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `vs_player_id` | Int64 | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | String | Name of the comparison (vs.) player. |
| `court_status` | String | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | String |  |
| `fgm` | Float64 | Field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**PlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `display_first_last` | String |  |
| `display_last_comma_first` | String |  |
| `display_fi_last` | String |  |
| `birthdate` | String | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | String | Player's school / college (when distinct from 'college'). |
| `country` | String | Country (full name or code). |
| `last_affiliation` | String |  |

**VsPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `display_first_last` | String |  |
| `display_last_comma_first` | String |  |
| `display_fi_last` | String |  |
| `birthdate` | String | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | String | Player's school / college (when distinct from 'college'). |
| `country` | String | Country (full name or code). |
| `last_affiliation` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playervsplayer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playervsplayer-example}

```python
wnba_stats_playervsplayer(league_id='10', season='2024')
```
