---
title: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Franchise"
sidebar_label: "Franchise"
sidebar_position: 4
description: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Franchise — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Franchise

## nbagl_stats_franchisehistory

Franchise history.

**Endpoint URL:** `GET https://stats.nba.com/stats/franchisehistory`

**Valid URL:** [https://stats.nba.com/stats/franchisehistory?LeagueID=20](https://stats.nba.com/stats/franchisehistory?LeagueID=20)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_franchisehistory-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`FranchiseHistory`, `DefunctTeams`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**FranchiseHistory**

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `start_year` | String | First season (start year) of the franchise span, as a string (e.g. '1949'). |
| `end_year` | String | Last season (start year) of the franchise span, as a string; the current season for an active franchise. |
| `years` | Int64 |  |
| `games` | Int64 |  |
| `wins` | Int64 |  |
| `losses` | Int64 |  |
| `win_pct` | Float64 |  |
| `po_appearances` | Int64 | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | Int64 | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | Int64 | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | Int64 | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**DefunctTeams**

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `start_year` | String | First season (start year) of the franchise span, as a string (e.g. '1949'). |
| `end_year` | String | Last season (start year) of the franchise span, as a string; the current season for an active franchise. |
| `years` | Int64 |  |
| `games` | Int64 |  |
| `wins` | Int64 |  |
| `losses` | Int64 |  |
| `win_pct` | Float64 |  |
| `po_appearances` | Int64 | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | Int64 | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | Int64 | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | Int64 | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_franchisehistory-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_franchisehistory-example}

```python
nbagl_stats_franchisehistory(league_id='20')
```

## nbagl_stats_franchiseleaders

Franchise leaders.

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseleaders`

**Valid URL:** [https://stats.nba.com/stats/franchiseleaders?LeagueID=20&TeamID=1612709914](https://stats.nba.com/stats/franchiseleaders?LeagueID=20&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_franchiseleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `pts` | Int64 |  |
| `pts_person_id` | Int64 | Stats API identifier for points person identifier associated with this NBA or WNBA Stats row. |
| `pts_player` | String | Scoring or score-margin metric for points player in the requested NBA or WNBA Stats split. |
| `ast` | Int64 |  |
| `ast_person_id` | Int64 | Stats API identifier for assists person identifier associated with this NBA or WNBA Stats row. |
| `ast_player` | String | NBA or WNBA Stats value for assists player in the franchiseleaders result set. |
| `reb` | Int64 |  |
| `reb_person_id` | Int64 | Stats API identifier for rebounds person identifier associated with this NBA or WNBA Stats row. |
| `reb_player` | String | Rebounding metric for rebounds player in the requested NBA or WNBA Stats split. |
| `blk` | Int64 |  |
| `blk_person_id` | Int64 | Stats API identifier for blocks person identifier associated with this NBA or WNBA Stats row. |
| `blk_player` | String | NBA or WNBA Stats value for blocks player in the franchiseleaders result set. |
| `stl` | Int64 |  |
| `stl_person_id` | Int64 | Stats API identifier for steals person identifier associated with this NBA or WNBA Stats row. |
| `stl_player` | String | NBA or WNBA Stats value for steals player in the franchiseleaders result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_franchiseleaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_franchiseleaders-example}

```python
nbagl_stats_franchiseleaders(league_id='20')
```

## nbagl_stats_franchiseleaderswrank

Franchise leaders w rank.

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseleaderswrank`

**Valid URL:** [https://stats.nba.com/stats/franchiseleaderswrank?LeagueID=20&PerMode=Totals&SeasonType=Regular+Season&TeamID=1612709914](https://stats.nba.com/stats/franchiseleaderswrank?LeagueID=20&PerMode=Totals&SeasonType=Regular+Season&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_franchiseleaderswrank-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team` | String |  |
| `person_id` | Int64 |  |
| `player` | String |  |
| `season_type` | String |  |
| `active_with_team` | Int64 | Flag indicating whether the franchise leader is still active with the team. |
| `gp` | Int64 |  |
| `minutes` | Float64 |  |
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
| `pf` | Float64 |  |
| `stl` | Float64 |  |
| `tov` | Float64 |  |
| `blk` | Float64 |  |
| `pts` | Float64 |  |
| `f_rank_gp` | Int64 | Franchise all-time rank of the player's career games played. |
| `f_rank_minutes` | Int64 | Franchise all-time rank of the player's career minutes played. |
| `f_rank_fgm` | Int64 | Franchise all-time rank of the player's career field goals made. |
| `f_rank_fga` | Int64 | Franchise all-time rank of the player's career field goals attempted. |
| `f_rank_fg_pct` | Int64 | Franchise all-time rank of the player's career field goal percentage. |
| `f_rank_fg3_m` | Int64 | Franchise all-time rank of the player's career three-point field goals made. |
| `f_rank_fg3_a` | Int64 | Franchise all-time rank of the player's career three-point field goals attempted. |
| `f_rank_fg3_pct` | Int64 | Franchise all-time rank of the player's career three-point field goal percentage. |
| `f_rank_ftm` | Int64 | Franchise all-time rank of the player's career free throws made. |
| `f_rank_fta` | Int64 | Franchise all-time rank of the player's career free throws attempted. |
| `f_rank_ft_pct` | Int64 | Franchise all-time rank of the player's career free throw percentage. |
| `f_rank_oreb` | Int64 | Franchise all-time rank of the player's career offensive rebounds. |
| `f_rank_dreb` | Int64 | Franchise all-time rank of the player's career defensive rebounds. |
| `f_rank_reb` | Int64 | Franchise all-time rank of the player's career total rebounds. |
| `f_rank_ast` | Int64 | Franchise all-time rank of the player's career assists. |
| `f_rank_pf` | Int64 | Franchise all-time rank of the player's career personal fouls committed. |
| `f_rank_stl` | Int64 | Franchise all-time rank of the player's career steals. |
| `f_rank_tov` | Int64 | Franchise all-time rank of the player's career turnovers. |
| `f_rank_blk` | Int64 | Franchise all-time rank of the player's career blocked shots. |
| `f_rank_pts` | Int64 | Franchise all-time rank of the player's career points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_franchiseleaderswrank-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_franchiseleaderswrank-example}

```python
nbagl_stats_franchiseleaderswrank(league_id='20')
```

## nbagl_stats_franchiseplayers

Franchise players.

**Endpoint URL:** `GET https://stats.nba.com/stats/franchiseplayers`

**Valid URL:** [https://stats.nba.com/stats/franchiseplayers?LeagueID=20&PerMode=Totals&SeasonType=Regular+Season&TeamID=1612709914](https://stats.nba.com/stats/franchiseplayers?LeagueID=20&PerMode=Totals&SeasonType=Regular+Season&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_franchiseplayers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `team_id` | Int64 |  |
| `team` | String |  |
| `person_id` | Int64 |  |
| `player` | String |  |
| `season_type` | String |  |
| `active_with_team` | Int64 | Flag indicating whether the player is still active with the franchise. |
| `gp` | Int64 |  |
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
| `pf` | Float64 |  |
| `stl` | Float64 |  |
| `tov` | Float64 |  |
| `blk` | Float64 |  |
| `pts` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_franchiseplayers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_franchiseplayers-example}

```python
nbagl_stats_franchiseplayers(league_id='20')
```
