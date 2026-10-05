---
title: "NBA — NBA Stats API (stats.nba.com) — Franchise"
sidebar_label: "Franchise"
sidebar_position: 6
description: "NBA — NBA Stats API (stats.nba.com) — Franchise — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Franchise

## nba_stats_franchisehistory

GET /stats/franchisehistory

**Endpoint URL:** `GET https://stats.nba.com/stats/franchisehistory`

**Valid URL:** [https://stats.nba.com/stats/franchisehistory?LeagueID=00](https://stats.nba.com/stats/franchisehistory?LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_franchisehistory-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`FranchiseHistory`, `DefunctTeams`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**FranchiseHistory**

| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `start_year` | character | Span starting year. |
| `end_year` | character | Span ending year. |
| `years` | integer | Years. |
| `games` | integer | Games played. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Win percentage (0-1 decimal). |
| `po_appearances` | integer | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | integer | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | integer | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | integer | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**DefunctTeams**

| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team_city` | character | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | character | Full team display name (e.g. 'Las Vegas Aces'). |
| `start_year` | character | Span starting year. |
| `end_year` | character | Span ending year. |
| `years` | integer | Years. |
| `games` | integer | Games played. |
| `wins` | integer | Total wins. |
| `losses` | integer | Total losses. |
| `win_pct` | numeric | Win percentage (0-1 decimal). |
| `po_appearances` | integer | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | integer | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | integer | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | integer | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_franchisehistory-example}

```python
nba_stats_franchisehistory(league_id='00')
```

_Last validated n/a._

## nba_stats_franchiseleaders

GET /stats/franchiseleaders

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseleaders`

**Valid URL:** [https://stats.nba.com/stats/franchiseleaders?LeagueID=00&TeamID=1611661324](https://stats.nba.com/stats/franchiseleaders?LeagueID=00&TeamID=1611661324)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nba_stats_franchiseleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | integer | Unique team identifier. |
| `pts` | integer | Points scored. |
| `pts_person_id` | integer | Stats API identifier for points person identifier associated with this NBA or WNBA Stats row. |
| `pts_player` | character | Scoring or score-margin metric for points player in the requested NBA or WNBA Stats split. |
| `ast` | integer | Assists. |
| `ast_person_id` | integer | Stats API identifier for assists person identifier associated with this NBA or WNBA Stats row. |
| `ast_player` | character | NBA or WNBA Stats value for assists player in the franchiseleaders result set. |
| `reb` | integer | Rebounds per game. |
| `reb_person_id` | integer | Stats API identifier for rebounds person identifier associated with this NBA or WNBA Stats row. |
| `reb_player` | character | Rebounding metric for rebounds player in the requested NBA or WNBA Stats split. |
| `blk` | integer | Blocks. |
| `blk_person_id` | integer | Stats API identifier for blocks person identifier associated with this NBA or WNBA Stats row. |
| `blk_player` | character | NBA or WNBA Stats value for blocks player in the franchiseleaders result set. |
| `stl` | integer | Steals. |
| `stl_person_id` | integer | Stats API identifier for steals person identifier associated with this NBA or WNBA Stats row. |
| `stl_player` | character | NBA or WNBA Stats value for steals player in the franchiseleaders result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_franchiseleaders-example}

```python
nba_stats_franchiseleaders(league_id='00')
```

_Last validated n/a._

## nba_stats_franchiseleaderswrank

GET /stats/franchiseleaderswrank

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseleaderswrank`

**Valid URL:** [https://stats.nba.com/stats/franchiseleaderswrank?LeagueID=00&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661324](https://stats.nba.com/stats/franchiseleaderswrank?LeagueID=00&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661324)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nba_stats_franchiseleaderswrank-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player` | character | Player name. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | integer | Flag indicating whether the franchise leader is still active with the team. |
| `gp` | integer | Games played. |
| `minutes` | numeric | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
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
| `reb` | numeric | Rebounds per game. |
| `ast` | numeric | Assists. |
| `pf` | numeric | Personal fouls. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `blk` | numeric | Blocks. |
| `pts` | numeric | Points scored. |
| `f_rank_gp` | integer | Franchise all-time rank of the player's career games played. |
| `f_rank_minutes` | integer | Franchise all-time rank of the player's career minutes played. |
| `f_rank_fgm` | integer | Franchise all-time rank of the player's career field goals made. |
| `f_rank_fga` | integer | Franchise all-time rank of the player's career field goals attempted. |
| `f_rank_fg_pct` | integer | Franchise all-time rank of the player's career field goal percentage. |
| `f_rank_fg3_m` | integer | Franchise all-time rank of the player's career three-point field goals made. |
| `f_rank_fg3_a` | integer | Franchise all-time rank of the player's career three-point field goals attempted. |
| `f_rank_fg3_pct` | integer | Franchise all-time rank of the player's career three-point field goal percentage. |
| `f_rank_ftm` | integer | Franchise all-time rank of the player's career free throws made. |
| `f_rank_fta` | integer | Franchise all-time rank of the player's career free throws attempted. |
| `f_rank_ft_pct` | integer | Franchise all-time rank of the player's career free throw percentage. |
| `f_rank_oreb` | integer | Franchise all-time rank of the player's career offensive rebounds. |
| `f_rank_dreb` | integer | Franchise all-time rank of the player's career defensive rebounds. |
| `f_rank_reb` | integer | Franchise all-time rank of the player's career total rebounds. |
| `f_rank_ast` | integer | Franchise all-time rank of the player's career assists. |
| `f_rank_pf` | integer | Franchise all-time rank of the player's career personal fouls committed. |
| `f_rank_stl` | integer | Franchise all-time rank of the player's career steals. |
| `f_rank_tov` | integer | Franchise all-time rank of the player's career turnovers. |
| `f_rank_blk` | integer | Franchise all-time rank of the player's career blocked shots. |
| `f_rank_pts` | integer | Franchise all-time rank of the player's career points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_franchiseleaderswrank-example}

```python
nba_stats_franchiseleaderswrank(league_id='00')
```

_Last validated n/a._

## nba_stats_franchiseplayers

GET /stats/franchiseplayers

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseplayers`

**Valid URL:** [https://stats.nba.com/stats/franchiseplayers?LeagueID=00&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661319](https://stats.nba.com/stats/franchiseplayers?LeagueID=00&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661319)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nba_stats_franchiseplayers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | character | League identifier ('10' = WNBA). |
| `team_id` | integer | Unique team identifier. |
| `team` | character | Team-side label or team identifier. |
| `person_id` | integer | Unique player identifier (V3 endpoints). |
| `player` | character | Player name. |
| `season_type` | character | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | integer | Flag indicating whether the player is still active with the franchise. |
| `gp` | integer | Games played. |
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
| `reb` | numeric | Rebounds per game. |
| `ast` | numeric | Assists. |
| `pf` | numeric | Personal fouls. |
| `stl` | numeric | Steals. |
| `tov` | numeric | Turnovers. |
| `blk` | numeric | Blocks. |
| `pts` | numeric | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_franchiseplayers-example}

```python
nba_stats_franchiseplayers(league_id='00')
```

_Last validated n/a._
