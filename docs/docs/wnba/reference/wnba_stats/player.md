---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Player: playerawards–playergamelog"
sidebar_label: "Player: playerawards–playergamelog"
sidebar_position: 9
description: "WNBA — WNBA Stats API (stats.wnba.com) — Player: playerawards–playergamelog — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Player: playerawards–playergamelog

## wnba_stats_playerawards

Player awards.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerawards`

**Valid URL:** [https://stats.wnba.com/stats/playerawards?PlayerID=1628932](https://stats.wnba.com/stats/playerawards?PlayerID=1628932)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playerawards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `team` | String | Team-side label or team identifier. |
| `description` | String | Long-form description text. |
| `all_nba_team_number` | String | NBA or WNBA Stats value for all NBA team number in the playerawards result set. |
| `season` | String | Season identifier (4-digit year or 'YYYY-YY' string). |
| `month` | String | NBA or WNBA Stats value for month in the playerawards result set. |
| `week` | String | Week number. |
| `conference` | String | Filter players or teams by conference. |
| `type` | String | Record type / category. |
| `subtype1` | String | NBA or WNBA Stats value for subtype1 in the playerawards result set. |
| `subtype2` | String | NBA or WNBA Stats value for subtype2 in the playerawards result set. |
| `subtype3` | String | NBA or WNBA Stats value for subtype3 in the playerawards result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playerawards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playerawards-example}

```python
wnba_stats_playerawards()
```

## wnba_stats_playercareerbycollegerollup

Player career by college rollup.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerbycollegerollup`

**Valid URL:** [https://stats.wnba.com/stats/playercareerbycollegerollup?LeagueID=10&PerMode=Totals&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/playercareerbycollegerollup?LeagueID=10&PerMode=Totals&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_playercareerbycollegerollup-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`East`, `South`, `Midwest`, `West`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**East**

| col_name | type | description |
|---|---|---|
| `region` | String | Region label. |
| `seed` | Int64 | Region seed slot of the college in the stats API's career-by-college rollup grid. |
| `college` | String | College or school attended. |
| `players` | Int64 | Nested list of per-player box scores. |
| `gp` | Int64 | Games played. |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |

**South**

| col_name | type | description |
|---|---|---|
| `region` | String | Region label. |
| `seed` | Int64 | Region seed slot of the college in the stats API's career-by-college rollup grid. |
| `college` | String | College or school attended. |
| `players` | Int64 | Nested list of per-player box scores. |
| `gp` | Int64 | Games played. |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |

**Midwest**

| col_name | type | description |
|---|---|---|
| `region` | String | Region label. |
| `seed` | Int64 | Region seed slot of the college in the stats API's career-by-college rollup grid. |
| `college` | String | College or school attended. |
| `players` | Int64 | Nested list of per-player box scores. |
| `gp` | Int64 | Games played. |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |

**West**

| col_name | type | description |
|---|---|---|
| `region` | String | Region label. |
| `seed` | Int64 | Region seed slot of the college in the stats API's career-by-college rollup grid. |
| `college` | String | College or school attended. |
| `players` | Int64 | Nested list of per-player box scores. |
| `gp` | Int64 | Games played. |
| `min` | Float64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playercareerbycollegerollup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playercareerbycollegerollup-example}

```python
wnba_stats_playercareerbycollegerollup(league_id='10', season_nullable='2024')
```

## wnba_stats_playercareerstats

Player career stats.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercareerstats`

**Valid URL:** [https://stats.wnba.com/stats/playercareerstats?LeagueID=10&PerMode=Totals&PlayerID=1628932](https://stats.wnba.com/stats/playercareerstats?LeagueID=10&PerMode=Totals&PlayerID=1628932)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode36` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |

### Returns {#wnba_stats_playercareerstats-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonTotalsRegularSeason`, `CareerTotalsRegularSeason`, `SeasonTotalsPostSeason`, `CareerTotalsPostSeason`, `SeasonTotalsAllStarSeason`, `CareerTotalsAllStarSeason`, `SeasonTotalsCollegeSeason`, `CareerTotalsCollegeSeason`, `SeasonTotalsShowcaseSeason`, `CareerTotalsShowcaseSeason`, `SeasonRankingsRegularSeason`, `SeasonRankingsPostSeason`, `SeasonHighs`, `CareerHighs`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**SeasonTotalsRegularSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
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
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
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
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
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
| `player_age` | String | NBA or WNBA Stats value for player age in the playercareerstats result set. |
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

**SeasonTotalsShowcaseSeason**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `season_id` | String | Unique season identifier. |
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `player_age` | Float64 | NBA or WNBA Stats value for player age in the playercareerstats result set. |
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

**CareerTotalsShowcaseSeason**

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
| `player_age` | String | NBA or WNBA Stats value for player age in the playercareerstats result set. |
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
| `player_age` | String | NBA or WNBA Stats value for player age in the playercareerstats result set. |
| `gp` | String | Games played. |
| `gs` | String | Games started. |
| `rank_pg_min` | Int64 |  |
| `rank_pg_fgm` | Int64 |  |
| `rank_pg_fga` | Int64 |  |
| `rank_fg_pct` | Int64 |  |
| `rank_pg_fg3_m` | Int64 |  |
| `rank_pg_fg3_a` | Int64 |  |
| `rank_fg3_pct` | String |  |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playercareerstats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playercareerstats-example}

```python
wnba_stats_playercareerstats(league_id='10')
```

## wnba_stats_playercompare

Player compare.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playercompare`

**Valid URL:** [https://stats.wnba.com/stats/playercompare?Conference=&DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerIDList=100720%2C202250%2C204319%2C1627668%2C1628931&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=&VsPlayerIDList=202252%2C203399%2C1631022%2C1628878%2C204333](https://stats.wnba.com/stats/playercompare?Conference=&DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerIDList=100720%2C202250%2C204319%2C1627668%2C1628931&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision=&VsPlayerIDList=202252%2C203399%2C1631022%2C1628878%2C204333)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
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
| `PlayerIDList` | `player_id_list` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerIDList` | `vs_player_id_list` |  |  | `Y` |  |

### Returns {#wnba_stats_playercompare-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`OverallCompare`, `Individual`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**OverallCompare**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `description` | String | Long-form description text. |
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

**Individual**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `description` | String | Long-form description text. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playercompare-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playercompare-example}

```python
wnba_stats_playercompare(league_id='10', season='2024')
```

## wnba_stats_playerestimatedmetrics

Player estimated metrics.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerestimatedmetrics`

**Valid URL:** [https://stats.wnba.com/stats/playerestimatedmetrics?LeagueID=10&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/playerestimatedmetrics?LeagueID=10&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_playerestimatedmetrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `gp` | Int64 | Games played. |
| `w` | Int64 | Wins. |
| `l` | Int64 | Losses. |
| `w_pct` | Float64 | Wins percentage (0-1 decimal). |
| `min` | Float64 | Minutes played. |
| `e_off_rating` | Float64 | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | Float64 | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | Float64 | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | Float64 | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | Float64 | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | Float64 | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | Float64 | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tov_pct` | Float64 | Estimated turnovers percentage for the requested NBA or WNBA Stats split. |
| `e_usg_pct` | Float64 | Estimated usage percentage for the requested NBA or WNBA Stats split. |
| `e_pace` | Float64 | Estimated pace for the requested NBA or WNBA Stats split. |
| `gp_rank` | Int64 | Rank for games played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_rank` | Int64 | Rank for wins within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `l_rank` | Int64 | Rank for losses within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `w_pct_rank` | Int64 | Rank for winning percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `min_rank` | Int64 | Rank for minutes played within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_off_rating_rank` | Int64 | Rank for e offensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_def_rating_rank` | Int64 | Rank for e defensive rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_net_rating_rank` | Int64 | Rank for e net rating within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_ast_ratio_rank` | Int64 | Rank for e assists ratio within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_oreb_pct_rank` | Int64 | Rank for e offensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_dreb_pct_rank` | Int64 | Rank for e defensive rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_reb_pct_rank` | Int64 | Rank for e rebounds percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_tov_pct_rank` | Int64 | Rank for e turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_usg_pct_rank` | Int64 | Rank for e usage percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | Int64 | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playerestimatedmetrics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playerestimatedmetrics-example}

```python
wnba_stats_playerestimatedmetrics(league_id='10', season='2024')
```

## wnba_stats_playerfantasyprofile

Player fantasy profile.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerfantasyprofile`

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofile?LeagueID=10&MeasureType=Base&PaceAdjust=N&PerMode=Totals&PlayerID=1628932&PlusMinus=N&Rank=N&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/playerfantasyprofile?LeagueID=10&MeasureType=Base&PaceAdjust=N&PerMode=Totals&PlayerID=1628932&PlusMinus=N&Rank=N&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `MeasureType` | `measure_type` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_playerfantasyprofile-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `Location`, `LastNGames`, `DaysRestModified`, `Opponent`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
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
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**Location**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
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
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**LastNGames**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
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
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**DaysRestModified**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `season_year` | String | Season year string ('YYYY-YY' format). |
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
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**Opponent**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
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
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playerfantasyprofile-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playerfantasyprofile-example}

```python
wnba_stats_playerfantasyprofile(league_id='10', season='2024')
```

## wnba_stats_playerfantasyprofilebargraph

Player fantasy profile bar graph.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playerfantasyprofilebargraph`

**Valid URL:** [https://stats.wnba.com/stats/playerfantasyprofilebargraph?LeagueID=10&PlayerID=1628932&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/playerfantasyprofilebargraph?LeagueID=10&PlayerID=1628932&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_playerfantasyprofilebargraph-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`SeasonAvg`, `LastFiveGamesAvg`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**SeasonAvg**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `fan_duel_pts` | Float64 | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | Float64 | Points scored. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `fg3_m` | Float64 | Three-point field goals made. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**LastFiveGamesAvg**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `fan_duel_pts` | Float64 | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `pts` | Float64 | Points scored. |
| `reb` | Float64 | Total rebounds. |
| `ast` | Float64 | Assists. |
| `fg3_m` | Float64 | Three-point field goals made. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `stl` | Float64 | Steals. |
| `blk` | Float64 | Blocks. |
| `tov` | Float64 | Turnovers. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playerfantasyprofilebargraph-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playerfantasyprofilebargraph-example}

```python
wnba_stats_playerfantasyprofilebargraph(league_id='10', season='2024')
```

## wnba_stats_playergamelog

Player game log.

**Endpoint URL:** `GET https://stats.wnba.com/stats/playergamelog`

**Valid URL:** [https://stats.wnba.com/stats/playergamelog?DateFrom=&DateTo=&LeagueID=10&PlayerID=1628932&Season=2024&SeasonType=Regular+Season](https://stats.wnba.com/stats/playergamelog?DateFrom=&DateTo=&LeagueID=10&PlayerID=1628932&Season=2024&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season year, e.g. ``2024``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A WNBA season tips off in mid-May and becomes the default in June (``2025`` through May 2026, ``2026`` from June 2026); a draft (``drafthistory``, mid-April) in May. With season type ``Playoffs`` (or ``commonplayoffseries``) the WNBA rolls over in October, after its mid-September playoffs start; with ``All Star`` in August, after the July game. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.wnba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |

### Returns {#wnba_stats_playergamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `player_id` | Int64 | Unique player identifier. |
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `matchup` | String | Matchup. |
| `wl` | String | Wl. |
| `min` | Int64 | Minutes played. |
| `fgm` | Int64 | Field goals made. |
| `fga` | Int64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ftm` | Int64 | Free throws made. |
| `fta` | Int64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `oreb` | Int64 | Offensive rebounds. |
| `dreb` | Int64 | Defensive rebounds. |
| `reb` | Int64 | Total rebounds. |
| `ast` | Int64 | Assists. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `tov` | Int64 | Turnovers. |
| `pf` | Int64 | Personal fouls. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |
| `video_available` | Int64 | Video available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playergamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playergamelog-example}

```python
wnba_stats_playergamelog(league_id='10', season='2024')
```
