---
title: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Other: alltimeleadersgrids–playbyplayv2"
sidebar_label: "Other: alltimeleadersgrids–playbyplayv2"
sidebar_position: 23
description: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Other: alltimeleadersgrids–playbyplayv2 — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Other: alltimeleadersgrids–playbyplayv2

## nbagl_stats_alltimeleadersgrids

All time leaders grids.

**Endpoint URL:** `GET https://stats.nba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=20&PerMode=PerGame&SeasonType=Regular+Season&TopX=10](https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=20&PerMode=PerGame&SeasonType=Regular+Season&TopX=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TopX` | `topx` |  |  | `Y` |  |

### Returns {#nbagl_stats_alltimeleadersgrids-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GPLeaders`, `PTSLeaders`, `ASTLeaders`, `STLLeaders`, `OREBLeaders`, `DREBLeaders`, `REBLeaders`, `BLKLeaders`, `FGMLeaders`, `FGALeaders`, `FG_PCTLeaders`, `TOVLeaders`, `FG3MLeaders`, `FG3ALeaders`, `FG3_PCTLeaders`, `PFLeaders`, `FTMLeaders`, `FTALeaders`, `FT_PCTLeaders`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GPLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `gp` | Int64 |  |
| `gp_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**PTSLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `pts` | Float64 |  |
| `pts_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**ASTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `ast` | Float64 |  |
| `ast_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**STLLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `stl` | Float64 |  |
| `stl_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**OREBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `oreb` | Float64 |  |
| `oreb_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**DREBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `dreb` | Float64 |  |
| `dreb_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**REBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `reb` | Float64 |  |
| `reb_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**BLKLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `blk` | Float64 |  |
| `blk_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FGMLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fgm` | Float64 |  |
| `fgm_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FGALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fga` | Float64 |  |
| `fga_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fg_pct` | Float64 |  |
| `fg_pct_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**TOVLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `tov` | Float64 |  |
| `tov_rank` | Int64 | All-time league rank of the player's career turnover total on the leaders grid. |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG3MLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_m_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG3ALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_a_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG3_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fg3_pct` | Float64 |  |
| `fg3_pct_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**PFLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `pf` | Float64 |  |
| `pf_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FTMLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `ftm` | Float64 |  |
| `ftm_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FTALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `fta` | Float64 |  |
| `fta_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FT_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `ft_pct` | Float64 |  |
| `ft_pct_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_alltimeleadersgrids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_alltimeleadersgrids-example}

```python
nbagl_stats_alltimeleadersgrids(league_id='20')
```

## nbagl_stats_assistleaders

Assist leaders.

**Endpoint URL:** `GET https://stats.nba.com/stats/assistleaders`

**Valid URL:** [https://stats.nba.com/stats/assistleaders?LeagueID=20&PerMode=PerGame&PlayerOrTeam=Team&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/assistleaders?LeagueID=20&PerMode=PerGame&PlayerOrTeam=Team&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |

### Returns {#nbagl_stats_assistleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `ast` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_assistleaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_assistleaders-example}

```python
nbagl_stats_assistleaders(league_id='20', season='2024-25')
```

## nbagl_stats_assisttracker

Assist tracker.

**Endpoint URL:** `GET https://stats.nba.com/stats/assisttracker`

**Valid URL:** [https://stats.nba.com/stats/assisttracker?LeagueID=20&OpponentTeamID=0&PerMode=PerGame&Season=2024-25&SeasonType=Regular+Season&TeamID=0](https://stats.nba.com/stats/assisttracker?LeagueID=20&OpponentTeamID=0&PerMode=PerGame&Season=2024-25&SeasonType=Regular+Season&TeamID=0)

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
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_assisttracker-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_assisttracker-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_assisttracker-example}

```python
nbagl_stats_assisttracker(league_id='20', season_nullable='2024-25')
```

## nbagl_stats_cumestatsteam

Cume stats team.

**Endpoint URL:** `GET https://stats.nba.com/stats/cumestatsteam`

**Valid URL:** [https://stats.nba.com/stats/cumestatsteam?GameIDs=2022500137&LeagueID=20&Season=2024-25&SeasonType=Regular+Season&TeamID=1612709925](https://stats.nba.com/stats/cumestatsteam?GameIDs=2022500137&LeagueID=20&Season=2024-25&SeasonType=Regular+Season&TeamID=1612709925)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameIDs` | `game_ids` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_cumestatsteam-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GameByGameStats`, `TotalTeamStats`, `GLeagueFreeThrows`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GameByGameStats**

| col_name | type | description |
|---|---|---|
| `jersey_num` | String |  |
| `player` | String |  |
| `person_id` | String |  |
| `team_id` | String |  |
| `gp` | String |  |
| `gs` | String |  |
| `actual_minutes` | String | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | String | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | String | Field goals made over the aggregated games. |
| `fga` | String |  |
| `fg_pct` | String |  |
| `fg3` | String | Three-point field goals made over the aggregated games. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String |  |
| `ft` | String | Free throws made over the aggregated games. |
| `fta` | String |  |
| `ft_pct` | String |  |
| `off_reb` | String | Offensive rebounds over the aggregated games. |
| `def_reb` | String | Defensive rebounds over the aggregated games. |
| `tot_reb` | String | Total rebounds over the aggregated games. |
| `ast` | String |  |
| `pf` | String |  |
| `dq` | String | Disqualifications (fouled out) over the aggregated games. |
| `stl` | String |  |
| `turnovers` | String |  |
| `blk` | String |  |
| `pts` | String |  |
| `max_actual_minutes` | String | Most whole minutes played in any single aggregated game. |
| `max_actual_seconds` | String | Seconds component paired with the single-game maximum minutes. |
| `max_reb` | String | Most rebounds recorded in any single aggregated game. |
| `max_ast` | String | Most assists recorded in any single aggregated game. |
| `max_stl` | String | Most steals recorded in any single aggregated game. |
| `max_turnovers` | String | Most turnovers recorded in any single aggregated game. |
| `max_blkp` | String | Most blocked shots recorded in any single aggregated game. |
| `max_pts` | String | Most points recorded in any single aggregated game. |
| `avg_actual_minutes` | String | Average whole minutes played per aggregated game. |
| `avg_actual_seconds` | String | Average seconds component of playing time per aggregated game. |
| `avg_reb` | String | Average rebounds per game over the aggregated games. |
| `avg_ast` | String | Average assists per game over the aggregated games. |
| `avg_stl` | String | Average steals per game over the aggregated games. |
| `avg_turnovers` | String | Average turnovers per game over the aggregated games. |
| `avg_blkp` | String | Average blocked shots per game over the aggregated games. |
| `avg_pts` | String | Average points per game over the aggregated games. |
| `per_min_reb` | String | Rebounds per minute played over the aggregated games. |
| `per_min_ast` | String | Assists per minute played over the aggregated games. |
| `per_min_stl` | String | Steals per minute played over the aggregated games. |
| `per_min_turnovers` | String | Turnovers per minute played over the aggregated games. |
| `per_min_blk` | String | Blocked shots per minute played over the aggregated games. |
| `per_min_pts` | String | Points per minute played over the aggregated games. |

**TotalTeamStats**

| col_name | type | description |
|---|---|---|
| `city` | String |  |
| `nickname` | String |  |
| `team_id` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_home` | Int64 |  |
| `l_home` | Int64 |  |
| `w_road` | Int64 |  |
| `l_road` | Int64 |  |
| `team_turnovers` | Int64 |  |
| `team_rebounds` | Int64 | Team rebounds (rebounds credited to the team rather than a player). |
| `gp` | Int64 |  |
| `gs` | Int64 |  |
| `actual_minutes` | Int64 | Whole minutes of actual playing time accumulated over the aggregated games. |
| `actual_seconds` | Int64 | Leftover seconds of actual playing time beyond the whole minutes. |
| `fg` | Int64 | Field goals made over the aggregated games. |
| `fga` | Int64 |  |
| `fg_pct` | Float64 |  |
| `fg3` | Int64 | Three-point field goals made over the aggregated games. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ft` | Int64 | Free throws made over the aggregated games. |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `off_reb` | Int64 | Offensive rebounds over the aggregated games. |
| `def_reb` | Int64 | Defensive rebounds over the aggregated games. |
| `tot_reb` | Int64 | Total rebounds over the aggregated games. |
| `ast` | Int64 |  |
| `pf` | Int64 |  |
| `stl` | Int64 |  |
| `total_turnovers` | Int64 |  |
| `blk` | Int64 |  |
| `pts` | Int64 |  |
| `avg_reb` | Float64 | Average rebounds per game over the aggregated games. |
| `avg_pts` | Float64 | Average points per game over the aggregated games. |
| `dq` | Int64 | Disqualifications (fouled out) over the aggregated games. |

**GLeagueFreeThrows**

| col_name | type | description |
|---|---|---|
| `city` | String |  |
| `nickname` | String |  |
| `team_id` | Int64 |  |
| `ft` | Int64 | Free throws made over the aggregated games. |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `ft_1_pt` | Float64 |  |
| `fta_1_pt` | Float64 |  |
| `ft_pct_1_pt` | Float64 |  |
| `ft_2_pt` | Float64 |  |
| `fta_2_pt` | Float64 |  |
| `ft_pct_2_pt` | Float64 |  |
| `ft_3_pt` | Float64 |  |
| `fta_3_pt` | Float64 |  |
| `ft_pct_3_pt` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_cumestatsteam-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_cumestatsteam-example}

```python
nbagl_stats_cumestatsteam(league_id='20', season='2024-25')
```

## nbagl_stats_draftcombinestats

Draft combine stats.

**Endpoint URL:** `GET https://stats.nba.com/stats/draftcombinestats`

**Valid URL:** [https://stats.nba.com/stats/draftcombinestats?LeagueID=20&SeasonYear=2024-25](https://stats.nba.com/stats/draftcombinestats?LeagueID=20&SeasonYear=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `SeasonYear` | `season_all_time` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nbagl_stats_draftcombinestats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | String |  |
| `player_id` | Int64 |  |
| `first_name` | String |  |
| `last_name` | String |  |
| `player_name` | String |  |
| `position` | String |  |
| `height_wo_shoes` | Float64 | Height measured without shoes, in inches. |
| `height_wo_shoes_ft_in` | String | Height without shoes formatted as feet and inches. |
| `height_w_shoes` | String | Height measured with shoes, in inches. |
| `height_w_shoes_ft_in` | String | Height with shoes formatted as feet and inches. |
| `weight` | String |  |
| `wingspan` | Float64 | Wingspan measured at the combine, in inches. |
| `wingspan_ft_in` | String | Wingspan formatted as feet and inches. |
| `standing_reach` | Float64 | Standing reach measured at the combine, in inches. |
| `standing_reach_ft_in` | String | Standing reach formatted as feet and inches. |
| `body_fat_pct` | String | Body fat percentage measured at the combine. |
| `hand_length` | Float64 | Hand length measured at the combine, in inches. |
| `hand_width` | Float64 | Hand width measured at the combine, in inches. |
| `standing_vertical_leap` | Float64 | Standing (no-step) vertical leap, in inches. |
| `max_vertical_leap` | Float64 | Maximum (running) vertical leap, in inches. |
| `lane_agility_time` | Float64 | Lane agility drill time, in seconds. |
| `modified_lane_agility_time` | Float64 | Modified (shuttle) lane agility drill time, in seconds. |
| `three_quarter_sprint` | Float64 | Three-quarter-court sprint time, in seconds. |
| `bench_press` | String | Repetitions of 185 pounds completed on the bench press. |
| `spot_fifteen_corner_left` | String | Made-attempted result (e.g. "3-5") from the 15-foot left corner spot-up shooting station at the combine. |
| `spot_fifteen_break_left` | String | Made-attempted result (e.g. "3-5") from the 15-foot left wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_top_key` | String | Made-attempted result (e.g. "3-5") from the 15-foot top of the key spot-up shooting station at the combine. |
| `spot_fifteen_break_right` | String | Made-attempted result (e.g. "3-5") from the 15-foot right wing (break) spot-up shooting station at the combine. |
| `spot_fifteen_corner_right` | String | Made-attempted result (e.g. "3-5") from the 15-foot right corner spot-up shooting station at the combine. |
| `spot_college_corner_left` | String | Made-attempted result (e.g. "3-5") from the college three-point left corner spot-up shooting station at the combine. |
| `spot_college_break_left` | String | Made-attempted result (e.g. "3-5") from the college three-point left wing (break) spot-up shooting station at the combine. |
| `spot_college_top_key` | String | Made-attempted result (e.g. "3-5") from the college three-point top of the key spot-up shooting station at the combine. |
| `spot_college_break_right` | String | Made-attempted result (e.g. "3-5") from the college three-point right wing (break) spot-up shooting station at the combine. |
| `spot_college_corner_right` | String | Made-attempted result (e.g. "3-5") from the college three-point right corner spot-up shooting station at the combine. |
| `spot_nba_corner_left` | String | Made-attempted result (e.g. "3-5") from the NBA three-point left corner spot-up shooting station at the combine. |
| `spot_nba_break_left` | String | Made-attempted result (e.g. "3-5") from the NBA three-point left wing (break) spot-up shooting station at the combine. |
| `spot_nba_top_key` | String | Made-attempted result (e.g. "3-5") from the NBA three-point top of the key spot-up shooting station at the combine. |
| `spot_nba_break_right` | String | Made-attempted result (e.g. "3-5") from the NBA three-point right wing (break) spot-up shooting station at the combine. |
| `spot_nba_corner_right` | String | Made-attempted result (e.g. "3-5") from the NBA three-point right corner spot-up shooting station at the combine. |
| `off_drib_fifteen_break_left` | String | Made-attempted result from the 15-foot left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_top_key` | String | Made-attempted result from the 15-foot top of the key off-the-dribble shooting station at the combine. |
| `off_drib_fifteen_break_right` | String | Made-attempted result from the 15-foot right wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_break_left` | String | Made-attempted result from the college three-point left wing (break) off-the-dribble shooting station at the combine. |
| `off_drib_college_top_key` | String | Made-attempted result from the college three-point top of the key off-the-dribble shooting station at the combine. |
| `off_drib_college_break_right` | String | Made-attempted result from the college three-point right wing (break) off-the-dribble shooting station at the combine. |
| `on_move_fifteen` | String | Made-attempted result from the 15-foot shooting-on-the-move station at the combine. |
| `on_move_college` | String | Made-attempted result from the college three-point shooting-on-the-move station at the combine. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_draftcombinestats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_draftcombinestats-example}

```python
nbagl_stats_draftcombinestats(league_id='20', season_all_time='2024-25')
```

## nbagl_stats_fantasywidget

Fantasy widget.

**Endpoint URL:** `GET https://stats.nba.com/stats/fantasywidget`

**Valid URL:** [https://stats.nba.com/stats/fantasywidget?ActivePlayers=N&DateFrom=&DateTo=&LastNGames=0&LeagueID=20&Location=&Month=&OpponentTeamID=0&PORound=&PlayerID=&Position=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&TodaysOpponent=0&TodaysPlayers=N&VsConference=&VsDivision=](https://stats.nba.com/stats/fantasywidget?ActivePlayers=N&DateFrom=&DateTo=&LastNGames=0&LeagueID=20&Location=&Month=&OpponentTeamID=0&PORound=&PlayerID=&Position=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&TodaysOpponent=0&TodaysPlayers=N&VsConference=&VsDivision=)

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TodaysOpponent` | `todays_opponent` |  |  | `Y` |  |
| `TodaysPlayers` | `todays_players` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_fantasywidget-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `gp` | Int64 |  |
| `min` | Float64 |  |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |
| `pts` | Float64 |  |
| `reb` | Float64 |  |
| `ast` | Float64 |  |
| `blk` | Float64 |  |
| `stl` | Float64 |  |
| `tov` | Float64 |  |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |
| `fta` | Float64 |  |
| `ft_pct` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_fantasywidget-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_fantasywidget-example}

```python
nbagl_stats_fantasywidget(league_id='20', season='2024-25')
```

## nbagl_stats_gamerotation

Game rotation.

**Endpoint URL:** `GET https://stats.nba.com/stats/gamerotation`

**Valid URL:** [https://stats.nba.com/stats/gamerotation?GameID=2022500137&LeagueID=20](https://stats.nba.com/stats/gamerotation?GameID=2022500137&LeagueID=20)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_gamerotation-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`AwayTeam`, `HomeTeam`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**AwayTeam**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `person_id` | Int64 |  |
| `player_first` | String | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | String | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | Float64 | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | Float64 | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | Int64 | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | Float64 | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | Float64 | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**HomeTeam**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `person_id` | Int64 |  |
| `player_first` | String | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | String | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | Float64 | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | Float64 | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | Int64 | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | Float64 | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | Float64 | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_gamerotation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_gamerotation-example}

```python
nbagl_stats_gamerotation(league_id='20')
```

## nbagl_stats_homepagev2

Homepage v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/homepagev2`

**Valid URL:** [https://stats.nba.com/stats/homepagev2?GameScope=Season&LeagueID=20&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatType=Traditional](https://stats.nba.com/stats/homepagev2?GameScope=Season&LeagueID=20&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatType=Traditional)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `StatType` | `stat_type` |  |  | `Y` |  |

### Returns {#nbagl_stats_homepagev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageStat1`, `HomePageStat2`, `HomePageStat3`, `HomePageStat4`, `HomePageStat5`, `HomePageStat6`, `HomePageStat7`, `HomePageStat8`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageStat1**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `pts` | Float64 |  |

**HomePageStat2**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `reb` | Float64 |  |

**HomePageStat3**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `ast` | Float64 |  |

**HomePageStat4**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `stl` | Float64 |  |

**HomePageStat5**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `fg_pct` | Float64 |  |

**HomePageStat6**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `ft_pct` | Float64 |  |

**HomePageStat7**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `fg3_pct` | Float64 |  |

**HomePageStat8**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `blk` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_homepagev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_homepagev2-example}

```python
nbagl_stats_homepagev2(league_id='20', season='2024-25')
```

## nbagl_stats_hustlestatsboxscore

Hustle stats box score.

**Endpoint URL:** `GET https://stats.nba.com/stats/hustlestatsboxscore`

**Valid URL:** [https://stats.nba.com/stats/hustlestatsboxscore?GameID=2022500137](https://stats.nba.com/stats/hustlestatsboxscore?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_hustlestatsboxscore-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HustleStatsAvailable`, `PlayerStats`, `TeamStats`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HustleStatsAvailable**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `hustle_status` | Int64 |  |

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `player_id` | String |  |
| `player_name` | String |  |
| `start_position` | String | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | String |  |
| `minutes` | Int64 |  |
| `pts` | Int64 |  |
| `contested_shots` | Int64 |  |
| `contested_shots_2_pt` | Int64 | Opponent two-point attempts contested. |
| `contested_shots_3_pt` | Int64 | Opponent three-point attempts contested. |
| `deflections` | Int64 |  |
| `charges_drawn` | Int64 |  |
| `screen_assists` | Int64 |  |
| `screen_ast_pts` | Int64 | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | Int64 | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | Int64 | Loose balls recovered while on defense. |
| `loose_balls_recovered` | Int64 | Total loose balls recovered. |
| `off_boxouts` | Int64 | Box-outs recorded on the offensive glass. |
| `def_boxouts` | Int64 | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | Int64 | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | Int64 | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | Int64 |  |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `team_id` | String |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_city` | String |  |
| `minutes` | Int64 |  |
| `pts` | Int64 |  |
| `contested_shots` | Int64 |  |
| `contested_shots_2_pt` | Int64 | Opponent two-point attempts contested. |
| `contested_shots_3_pt` | Int64 | Opponent three-point attempts contested. |
| `deflections` | Int64 |  |
| `charges_drawn` | Int64 |  |
| `screen_assists` | Int64 |  |
| `screen_ast_pts` | Int64 | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | Int64 | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | Int64 | Loose balls recovered while on defense. |
| `loose_balls_recovered` | Int64 | Total loose balls recovered. |
| `off_boxouts` | Int64 | Box-outs recorded on the offensive glass. |
| `def_boxouts` | Int64 | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | Int64 | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | Int64 | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_hustlestatsboxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_hustlestatsboxscore-example}

```python
nbagl_stats_hustlestatsboxscore()
```

## nbagl_stats_infographicfanduelplayer

Infographic fanduel player.

**Endpoint URL:** `GET https://stats.nba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.nba.com/stats/infographicfanduelplayer?GameID=2022500137](https://stats.nba.com/stats/infographicfanduelplayer?GameID=2022500137)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_infographicfanduelplayer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `team_id` | Int64 |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `jersey_num` | String |  |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `location` | String |  |
| `fan_duel_pts` | Float64 | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `usg_pct` | Float64 | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
| `min` | Float64 |  |
| `fgm` | Int64 |  |
| `fga` | Int64 |  |
| `fg_pct` | Float64 |  |
| `fg3_m` | Int64 | Three-point field goals made. |
| `fg3_a` | Int64 | Three-point field goal attempts. |
| `fg3_pct` | Float64 |  |
| `ftm` | Int64 |  |
| `fta` | Int64 |  |
| `ft_pct` | Float64 |  |
| `oreb` | Int64 |  |
| `dreb` | Int64 |  |
| `reb` | Int64 |  |
| `ast` | Int64 |  |
| `tov` | Int64 |  |
| `stl` | Int64 |  |
| `blk` | Int64 |  |
| `blka` | Int64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Int64 |  |
| `pfd` | Int64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Int64 |  |
| `plus_minus` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_infographicfanduelplayer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_infographicfanduelplayer-example}

```python
nbagl_stats_infographicfanduelplayer()
```

## nbagl_stats_leaderstiles

Leaders tiles.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaderstiles`

**Valid URL:** [https://stats.nba.com/stats/leaderstiles?GameScope=Season&LeagueID=20&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&Stat=PTS](https://stats.nba.com/stats/leaderstiles?GameScope=Season&LeagueID=20&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&Stat=PTS)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `Stat` | `stat` |  |  | `Y` |  |

### Returns {#nbagl_stats_leaderstiles-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`LeadersTiles`, `AllTimeSeasonHigh`, `LastSeasonHigh`, `LowSeasonHigh`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**LeadersTiles**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `pts` | Float64 |  |

**AllTimeSeasonHigh**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `pts` | Float64 |  |
| `season_year` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |

**LastSeasonHigh**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 |  |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `pts` | Float64 |  |

**LowSeasonHigh**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `player_name` | String |  |
| `pts` | Float64 |  |
| `season_year` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_leaderstiles-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_leaderstiles-example}

```python
nbagl_stats_leaderstiles(league_id='20', season='2024-25')
```

## nbagl_stats_playbyplayv2

Play by play v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv2`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv2?EndPeriod=0&GameID=2022500137&StartPeriod=0](https://stats.nba.com/stats/playbyplayv2?EndPeriod=0&GameID=2022500137&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#nbagl_stats_playbyplayv2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`PlayByPlay`, `AvailableVideo`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**PlayByPlay**

| col_name | type | description |
|---|---|---|
| `game_id` | String |  |
| `eventnum` | Int64 | Sequential event number within the game's play-by-play feed. |
| `eventmsgtype` | Int64 | Numeric event type code (1 = made shot, 2 = missed shot, 3 = free throw, 4 = rebound, 5 = turnover, 6 = foul, ...). |
| `eventmsgactiontype` | Int64 | Numeric sub-type code refining eventmsgtype (e.g. the specific shot, foul, or turnover variety). |
| `period` | Int64 |  |
| `wctimestring` | String | Wall-clock time of day when the event occurred. |
| `pctimestring` | String | Game clock remaining in the period when the event occurred (MM:SS). |
| `homedescription` | String | Text description of the event from the home team's perspective; empty when not a home-team action. |
| `neutraldescription` | String | Neutral text description of the event (e.g. period start/end); empty for team actions. |
| `visitordescription` | String | Text description of the event from the visiting team's perspective; empty when not a visitor action. |
| `score` | String |  |
| `scoremargin` | String | Score margin after the event ('TIE' when tied); empty on non-scoring events. |
| `person1_type` | Int64 |  |
| `player1_id` | Int64 |  |
| `player1_name` | String |  |
| `player1_team_id` | Int64 |  |
| `player1_team_city` | String |  |
| `player1_team_nickname` | String |  |
| `player1_team_abbreviation` | String |  |
| `person2_type` | Int64 |  |
| `player2_id` | Int64 |  |
| `player2_name` | String |  |
| `player2_team_id` | Int64 |  |
| `player2_team_city` | String |  |
| `player2_team_nickname` | String |  |
| `player2_team_abbreviation` | String |  |
| `person3_type` | Int64 |  |
| `player3_id` | Int64 |  |
| `player3_name` | String |  |
| `player3_team_id` | Int64 |  |
| `player3_team_city` | String |  |
| `player3_team_nickname` | String |  |
| `player3_team_abbreviation` | String |  |
| `video_available_flag` | Int64 |  |

**AvailableVideo**

| col_name | type | description |
|---|---|---|
| `video_available_flag` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_playbyplayv2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_playbyplayv2-example}

```python
nbagl_stats_playbyplayv2()
```
