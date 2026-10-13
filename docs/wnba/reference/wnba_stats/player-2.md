# WNBA — WNBA Stats API (stats.wnba.com) — Player: playergamestreakfinder–playervsplayer

> WNBA — WNBA Stats API (stats.wnba.com) — Player: playergamestreakfinder–playervsplayer — function reference in sdv-py, the SportsDataverse Python package.

## wnba_stats_playergamestreakfinder

GET /stats/playergamestreakfinder

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
| `player_name_last_first` | character | Player display name formatted as Last, First for sorting in NBA or WNBA Stats tables. |
| `player_id` | integer | Unique player identifier. |
| `gamestreak` | integer | NBA or WNBA Stats value for gamestreak in the playergamestreakfinder result set. |
| `startdate` | character | Date or timestamp for startdate in the NBA or WNBA Stats result set. |
| `enddate` | character | Date or timestamp for enddate in the NBA or WNBA Stats result set. |
| `activestreak` | integer | NBA or WNBA Stats value for activestreak in the playergamestreakfinder result set. |
| `numseasons` | integer | NBA or WNBA Stats value for numseasons in the playergamestreakfinder result set. |
| `lastseason` | character | NBA or WNBA Stats value for lastseason in the playergamestreakfinder result set. |
| `firstseason` | character | NBA or WNBA Stats value for firstseason in the playergamestreakfinder result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playergamestreakfinder-example}

```python
wnba_stats_playergamestreakfinder(league_id='10', season_nullable='2024')
```

_Last validated n/a._

## wnba_stats_playerindex

GET /stats/playerindex

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
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player_last_name` | character | Player's last name. |
| `player_first_name` | character | Player's first name. |
| `player_slug` | character | URL-safe player identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_slug` | character | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `jersey_number` | character | Jersey number. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `height` | character | Player height (string e.g. '6-2' or inches). |
| `weight` | character | Player weight in pounds. |
| `college` | character | College or school attended. |
| `country` | character | Country (full name or code). |
| `draft_year` | integer | Draft year (4-digit). |
| `draft_round` | integer | Round of the draft selection. |
| `draft_number` | integer | Overall pick the player was selected with, as a string; 'Undrafted' when never drafted. |
| `roster_status` | numeric | Roster status flag: 1 when the player is on a current roster, 0 otherwise. |
| `from_year` | character | First season. |
| `to_year` | character | Most recent season. |
| `pts` | numeric | Points scored. |
| `reb` | numeric | Total rebounds. |
| `ast` | numeric | Assists. |
| `stats_timeframe` | character | Time value for stats timeframe in the NBA or WNBA Stats result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerindex-example}

```python
wnba_stats_playerindex(league_id='10', season='2024')
```

_Last validated n/a._

## wnba_stats_playernextngames

GET /stats/playernextngames

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
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `home_team_id` | integer | Unique identifier for the home team. |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `home_team_name` | character | Home team name. |
| `visitor_team_name` | character | Full name of the visiting team in the upcoming game. |
| `home_team_abbreviation` | character | Home team's three-letter tricode (e.g. 'LAL'). |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team in the upcoming game. |
| `home_team_nickname` | character | Home team's nickname (e.g. 'Lakers'). |
| `visitor_team_nickname` | character | Nickname of the visiting team in the upcoming game. |
| `game_time` | character | Game start time. |
| `home_wl` | character | Home team's win-loss record entering the upcoming game. |
| `visitor_wl` | character | Visiting team's win-loss record entering the upcoming game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playernextngames-example}

```python
wnba_stats_playernextngames(league_id='10', season_all='2024')
```

_Last validated n/a._

## wnba_stats_playerprofilev2

GET /stats/playerprofilev2

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
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsAllStarSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `organization_id` | character |  |
| `school_name` | character | College or school of the season row (SeasonTotalsCollegeSeason set). |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
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
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**CareerTotalsCollegeSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | character | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `organization_id` | character |  |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
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
| `stl` | character | Steals. |
| `blk` | character | Blocks. |
| `tov` | character | Turnovers. |
| `pf` | character | Personal fouls. |
| `pts` | character | Points scored. |

**SeasonTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | numeric | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**CareerTotalsPreseason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `gp` | integer | Games played. |
| `gs` | integer | Games started. |
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
| `stl` | numeric | Steals. |
| `blk` | numeric | Blocks. |
| `tov` | numeric | Turnovers. |
| `pf` | numeric | Personal fouls. |
| `pts` | numeric | Points scored. |

**SeasonRankingsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `rank_pg_min` | integer |  |
| `rank_pg_fgm` | integer |  |
| `rank_pg_fga` | integer |  |
| `rank_fg_pct` | integer |  |
| `rank_pg_fg3_m` | integer |  |
| `rank_pg_fg3_a` | integer |  |
| `rank_fg3_pct` | integer |  |
| `rank_pg_ftm` | integer |  |
| `rank_pg_fta` | integer |  |
| `rank_ft_pct` | integer |  |
| `rank_pg_oreb` | integer |  |
| `rank_pg_dreb` | integer |  |
| `rank_pg_reb` | integer |  |
| `rank_pg_ast` | integer |  |
| `rank_pg_stl` | integer |  |
| `rank_pg_blk` | integer |  |
| `rank_pg_tov` | integer |  |
| `rank_pg_pts` | integer |  |
| `rank_pg_eff` | integer |  |

**SeasonRankingsPostSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `season_id` | character | Unique season identifier. |
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | character | NBA or WNBA Stats value for player age in the playerprofilev2 result set. |
| `gp` | character | Games played. |
| `gs` | character | Games started. |
| `rank_pg_min` | integer |  |
| `rank_pg_fgm` | integer |  |
| `rank_pg_fga` | integer |  |
| `rank_fg_pct` | integer |  |
| `rank_pg_fg3_m` | integer |  |
| `rank_pg_fg3_a` | integer |  |
| `rank_fg3_pct` | integer |  |
| `rank_pg_ftm` | integer |  |
| `rank_pg_fta` | integer |  |
| `rank_ft_pct` | integer |  |
| `rank_pg_oreb` | integer |  |
| `rank_pg_dreb` | integer |  |
| `rank_pg_reb` | integer |  |
| `rank_pg_ast` | integer |  |
| `rank_pg_stl` | integer |  |
| `rank_pg_blk` | integer |  |
| `rank_pg_tov` | integer |  |
| `rank_pg_pts` | integer |  |
| `rank_pg_eff` | integer |  |

**SeasonHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character | Stat. |
| `stat_value` | integer | Stat value. |
| `stat_order` | integer |  |
| `date_est` | character |  |

**CareerHighs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Unique player identifier. |
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_name` | character |  |
| `vs_team_abbreviation` | character |  |
| `stat` | character | Stat. |
| `stat_value` | integer | Stat value. |
| `stat_order` | integer |  |
| `date_est` | character |  |

**NextGame**

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `game_time` | character | Game start time. |
| `location` | character | Filter results by game location. |
| `player_team_id` | integer | Team id of the player's current team in the NextGame set. |
| `player_team_city` | character |  |
| `player_team_nickname` | character |  |
| `player_team_abbreviation` | character |  |
| `vs_team_id` | integer |  |
| `vs_team_city` | character |  |
| `vs_team_nickname` | character |  |
| `vs_team_abbreviation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playerprofilev2-example}

```python
wnba_stats_playerprofilev2(league_id='10')
```

_Last validated n/a._

## wnba_stats_playervsplayer

GET /stats/playervsplayer

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
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
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

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
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

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | character |  |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | character | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `vs_player_id` | integer | Stats API player id of the comparison (vs.) player. |
| `vs_player_name` | character | Name of the comparison (vs.) player. |
| `court_status` | character | Whether the split covers minutes with the vs. player on or off the court. |
| `group_value` | character |  |
| `fgm` | numeric | Field goals made. |
| `fga` | numeric | Field goal attempts. |
| `fg_pct` | numeric | Field goal percentage (0-1). |

**PlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_first_last` | character |  |
| `display_last_comma_first` | character |  |
| `display_fi_last` | character |  |
| `birthdate` | character | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | character | Player's school / college (when distinct from 'college'). |
| `country` | character | Country (full name or code). |
| `last_affiliation` | character |  |

**VsPlayerInfo**

| col_name | type | description |
|---|---|---|
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `first_name` | character | Player's first name. |
| `last_name` | character | Player's last name. |
| `display_first_last` | character |  |
| `display_last_comma_first` | character |  |
| `display_fi_last` | character |  |
| `birthdate` | character | Date of birth as an ISO timestamp string (e.g. '1984-12-30T00:00:00'). |
| `school` | character | Player's school / college (when distinct from 'college'). |
| `country` | character | Country (full name or code). |
| `last_affiliation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#wnba_stats_playervsplayer-example}

```python
wnba_stats_playervsplayer(league_id='10', season='2024')
```

_Last validated n/a._
