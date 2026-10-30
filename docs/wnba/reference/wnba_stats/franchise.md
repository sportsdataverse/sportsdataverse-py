# WNBA — WNBA Stats API (stats.wnba.com) — Franchise

> WNBA — WNBA Stats API (stats.wnba.com) — Franchise — function reference in sdv-py, the SportsDataverse Python package.

## wnba_stats_franchisehistory

Franchise history.

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchisehistory`

**Valid URL:** [https://stats.wnba.com/stats/franchisehistory?LeagueID=10](https://stats.wnba.com/stats/franchisehistory?LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_franchisehistory-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`FranchiseHistory`, `DefunctTeams`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**FranchiseHistory**

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `start_year` | String | First season (start year) of the franchise span, as a string (e.g. '1949'). |
| `end_year` | String | Last season (start year) of the franchise span, as a string; the current season for an active franchise. |
| `years` | Int64 | Years. |
| `games` | Int64 | Games played. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_pct` | Float64 | Win percentage (0-1 decimal). |
| `po_appearances` | Int64 | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | Int64 | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | Int64 | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | Int64 | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**DefunctTeams**

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `start_year` | String | First season (start year) of the franchise span, as a string (e.g. '1949'). |
| `end_year` | String | Last season (start year) of the franchise span, as a string; the current season for an active franchise. |
| `years` | Int64 | Years. |
| `games` | Int64 | Games played. |
| `wins` | Int64 | Total wins. |
| `losses` | Int64 | Total losses. |
| `win_pct` | Float64 | Win percentage (0-1 decimal). |
| `po_appearances` | Int64 | NBA or WNBA Stats value for playoff appearances in the franchisehistory result set. |
| `div_titles` | Int64 | NBA or WNBA Stats value for div titles in the franchisehistory result set. |
| `conf_titles` | Int64 | NBA or WNBA Stats value for conf titles in the franchisehistory result set. |
| `league_titles` | Int64 | NBA or WNBA Stats value for league titles in the franchisehistory result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_franchisehistory-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_franchisehistory-example}

```python
wnba_stats_franchisehistory(league_id='10')
```

## wnba_stats_franchiseleaders

Franchise leaders.

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchiseleaders`

**Valid URL:** [https://stats.wnba.com/stats/franchiseleaders?LeagueID=10&TeamID=1611661324](https://stats.wnba.com/stats/franchiseleaders?LeagueID=10&TeamID=1611661324)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_franchiseleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Unique team identifier. |
| `pts` | Int64 | Points scored. |
| `pts_person_id` | Int64 | Stats API identifier for points person identifier associated with this NBA or WNBA Stats row. |
| `pts_player` | String | Scoring or score-margin metric for points player in the requested NBA or WNBA Stats split. |
| `ast` | Int64 | Assists. |
| `ast_person_id` | Int64 | Stats API identifier for assists person identifier associated with this NBA or WNBA Stats row. |
| `ast_player` | String | NBA or WNBA Stats value for assists player in the franchiseleaders result set. |
| `reb` | Int64 | Total rebounds. |
| `reb_person_id` | Int64 | Stats API identifier for rebounds person identifier associated with this NBA or WNBA Stats row. |
| `reb_player` | String | Rebounding metric for rebounds player in the requested NBA or WNBA Stats split. |
| `blk` | Int64 | Blocks. |
| `blk_person_id` | Int64 | Stats API identifier for blocks person identifier associated with this NBA or WNBA Stats row. |
| `blk_player` | String | NBA or WNBA Stats value for blocks player in the franchiseleaders result set. |
| `stl` | Int64 | Steals. |
| `stl_person_id` | Int64 | Stats API identifier for steals person identifier associated with this NBA or WNBA Stats row. |
| `stl_player` | String | NBA or WNBA Stats value for steals player in the franchiseleaders result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_franchiseleaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_franchiseleaders-example}

```python
wnba_stats_franchiseleaders(league_id='10')
```

## wnba_stats_franchiseleaderswrank

Franchise leaders w rank.

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchiseleaderswrank`

**Valid URL:** [https://stats.wnba.com/stats/franchiseleaderswrank?LeagueID=10&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661324](https://stats.wnba.com/stats/franchiseleaderswrank?LeagueID=10&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661324)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_franchiseleaderswrank-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team` | String | Team-side label or team identifier. |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player` | String | Player name. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | Int64 | Flag indicating whether the franchise leader is still active with the team. |
| `gp` | Int64 | Games played. |
| `minutes` | Float64 | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
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
| `pf` | Float64 | Personal fouls. |
| `stl` | Float64 | Steals. |
| `tov` | Float64 | Turnovers. |
| `blk` | Float64 | Blocks. |
| `pts` | Float64 | Points scored. |
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

### Raises {#wnba_stats_franchiseleaderswrank-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_franchiseleaderswrank-example}

```python
wnba_stats_franchiseleaderswrank(league_id='10')
```

## wnba_stats_franchiseplayers

Franchise players.

**Endpoint URL:** `GET https://stats.wnba.com/stats/franchiseplayers`

**Valid URL:** [https://stats.wnba.com/stats/franchiseplayers?LeagueID=10&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661319](https://stats.wnba.com/stats/franchiseplayers?LeagueID=10&PerMode=Totals&SeasonType=Regular+Season&TeamID=1611661319)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in October, ``All Star`` in August. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#wnba_stats_franchiseplayers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier ('10' = WNBA). |
| `team_id` | Int64 | Unique team identifier. |
| `team` | String | Team-side label or team identifier. |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player` | String | Player name. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `active_with_team` | Int64 | Flag indicating whether the player is still active with the franchise. |
| `gp` | Int64 | Games played. |
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
| `pf` | Float64 | Personal fouls. |
| `stl` | Float64 | Steals. |
| `tov` | Float64 | Turnovers. |
| `blk` | Float64 | Blocks. |
| `pts` | Float64 | Points scored. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_franchiseplayers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_franchiseplayers-example}

```python
wnba_stats_franchiseplayers(league_id='10')
```
