# NBA — NBA Stats API (stats.nba.com) — Other: alltimeleadersgrids–playbyplayv3

> NBA — NBA Stats API (stats.nba.com) — Other: alltimeleadersgrids–playbyplayv3 — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_alltimeleadersgrids

All time leaders grids.

**Endpoint URL:** `GET https://stats.nba.com/stats/alltimeleadersgrids`

**Valid URL:** [https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00&PerMode=PerGame&SeasonType=Regular+Season&TopX=10](https://stats.nba.com/stats/alltimeleadersgrids?LeagueID=00&PerMode=PerGame&SeasonType=Regular+Season&TopX=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `TopX` | `topx` |  |  | `Y` |  |

### Returns {#nba_stats_alltimeleadersgrids-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`GPLeaders`, `PTSLeaders`, `ASTLeaders`, `STLLeaders`, `OREBLeaders`, `DREBLeaders`, `REBLeaders`, `BLKLeaders`, `FGMLeaders`, `FGALeaders`, `FG_PCTLeaders`, `TOVLeaders`, `FG3MLeaders`, `FG3ALeaders`, `FG3_PCTLeaders`, `PFLeaders`, `FTMLeaders`, `FTALeaders`, `FT_PCTLeaders`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**GPLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `gp` | Int64 | Games played. |
| `gp_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**PTSLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `pts` | Float64 | Points scored. |
| `pts_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**ASTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `ast` | Float64 | Assists. |
| `ast_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**STLLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `stl` | Float64 | Steals. |
| `stl_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**OREBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `oreb` | Float64 | Offensive rebounds. |
| `oreb_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**DREBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `dreb` | Float64 | Defensive rebounds. |
| `dreb_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**REBLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `reb` | Float64 | Rebounds per game. |
| `reb_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**BLKLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `blk` | Float64 | Blocks. |
| `blk_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FGMLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fgm` | Float64 | Field goals made. |
| `fgm_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FGALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fga` | Float64 | Field goal attempts. |
| `fga_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg_pct_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**TOVLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `tov` | Float64 | Turnovers. |
| `tov_rank` | Int64 | All-time league rank of the player's career turnover total on the leaders grid. |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG3MLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fg3_m_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG3ALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fg3_a` | Float64 | Three-point field goal attempts. |
| `fg3_a_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FG3_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `fg3_pct_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**PFLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `pf` | Float64 | Personal fouls. |
| `pf_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FTMLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `ftm` | Float64 | Free throws made. |
| `ftm_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FTALeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `fta` | Float64 | Free throw attempts. |
| `fta_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**FT_PCTLeaders**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `ft_pct_rank` | Int64 |  |
| `is_active_flag` | String | Flag indicating whether the player is currently active in the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_alltimeleadersgrids-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_alltimeleadersgrids-example}

```python
nba_stats_alltimeleadersgrids(league_id='00')
```

## nba_stats_assistleaders

Assist leaders.

**Endpoint URL:** `GET https://stats.nba.com/stats/assistleaders`

**Valid URL:** [https://stats.nba.com/stats/assistleaders?LeagueID=00&PerMode=PerGame&PlayerOrTeam=Team&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/assistleaders?LeagueID=00&PerMode=PerGame&PlayerOrTeam=Team&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |

### Returns {#nba_stats_assistleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `ast` | Float64 | Assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_assistleaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_assistleaders-example}

```python
nba_stats_assistleaders(league_id='00', season='2024-25')
```

## nba_stats_assisttracker

Assist tracker.

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
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `StarterBench` | `starter_bench_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `Weight` | `weight_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_assisttracker-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `assists` | Float64 | Total assists. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_assisttracker-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_assisttracker-example}

```python
nba_stats_assisttracker(league_id='00', season_nullable='2024-25')
```

## nba_stats_drafthistory

Draft history.

**Endpoint URL:** `GET https://stats.nba.com/stats/drafthistory`

**Valid URL:** [https://stats.nba.com/stats/drafthistory?College=&LeagueID=00&OverallPick=&RoundNum=&RoundPick=&Season=2024&TeamID=0&TopX=](https://stats.nba.com/stats/drafthistory?College=&LeagueID=00&OverallPick=&RoundNum=&RoundPick=&Season=2024&TeamID=0&TopX=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `College` | `college_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `OverallPick` | `overall_pick_nullable` |  |  | `Y` |  |
| `RoundNum` | `round_num_nullable` |  |  | `Y` |  |
| `RoundPick` | `round_pick_nullable` |  |  | `Y` |  |
| `Season` | `season_year_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TopX` | `topx_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_drafthistory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_name` | String | Player name. |
| `season` | String | Season year. |
| `round_number` | Int64 | Numeric round. |
| `round_pick` | Int64 | Round pick. |
| `overall_pick` | Int64 | Overall pick. |
| `draft_type` | String | NBA or WNBA Stats value for draft type in the drafthistory result set. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `organization` | String | Organization. |
| `organization_type` | String | Organization type. |
| `player_profile_flag` | Int64 | Player profile flag. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_drafthistory-raises}

| Exception | Description |
|---|---|
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). `curl_cffi` is not installed. stats.nba.com/stats.wnba.com TLS-fingerprint-block plain `requests`, so the live transport requires it (`pip install curl_cffi`, or `pip install sportsdataverse[all]`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The host answered HTTP 404. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The host answered HTTP 400 / 422 (it rejected a parameter value). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other non-2xx (401 / 403 / 429 / 5xx; a missing required `Season` draws an empty HTTP 500), a blank or non-JSON 2xx body, or a connection failure (timeout, reset), once `SDV_PY_NBA_STATS_RETRIES` retries are exhausted. |

### Example {#nba_stats_drafthistory-example}

```python
nba_stats_drafthistory(league_id='00', season_year_nullable='2024')
```

## nba_stats_fantasywidget

Fantasy widget.

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `TodaysOpponent` | `todays_opponent` |  |  | `Y` |  |
| `TodaysPlayers` | `todays_players` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_fantasywidget-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `gp` | Int64 | Games played. |
| `min` | Float64 | Minutes played. |
| `fan_duel_pts` | Float64 | Fantasy points under FanDuel's scoring formula. |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |
| `pts` | Float64 | Points scored. |
| `reb` | Float64 | Rebounds per game. |
| `ast` | Float64 | Assists. |
| `blk` | Float64 | Blocks. |
| `stl` | Float64 | Steals. |
| `tov` | Float64 | Turnovers. |
| `fg3_m` | Float64 | Three-point field goals made. |
| `fga` | Float64 | Field goal attempts. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fta` | Float64 | Free throw attempts. |
| `ft_pct` | Float64 | Free throw percentage (0-1). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_fantasywidget-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_fantasywidget-example}

```python
nba_stats_fantasywidget(league_id='00', season='2024-25')
```

## nba_stats_gamerotation

Game rotation.

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
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
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
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `person_id` | Int64 | Unique player identifier (V3 endpoints). |
| `player_first` | String | NBA or WNBA Stats value for player first in the gamerotation result set. |
| `player_last` | String | NBA or WNBA Stats value for player last in the gamerotation result set. |
| `in_time_real` | Float64 | Real-time clock value when the player entered the game rotation stint. |
| `out_time_real` | Float64 | Real-time clock value when the player exited the game rotation stint. |
| `player_pts` | Int64 | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `pt_diff` | Float64 | NBA or WNBA Stats value for pt diff in the gamerotation result set. |
| `usg_pct` | Float64 | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_gamerotation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_gamerotation-example}

```python
nba_stats_gamerotation(league_id='00')
```

## nba_stats_homepageleaders

Homepage leaders.

**Endpoint URL:** `GET https://stats.nba.com/stats/homepageleaders`

**Valid URL:** [https://stats.nba.com/stats/homepageleaders?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatCategory=Points](https://stats.nba.com/stats/homepageleaders?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatCategory=Points)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `StatCategory` | `stat_category` |  |  | `Y` |  |

### Returns {#nba_stats_homepageleaders-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageLeaders`, `LeagueAverage`, `LeagueMax`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageLeaders**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `pts` | Float64 | Points scored. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `efg_pct` | Float64 | Effective field goal percentage, as a decimal. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `pts_per48` | Float64 | Points scored per 48 minutes played. |

**LeagueAverage**

| col_name | type | description |
|---|---|---|
| `pts` | Float64 | Points scored. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `efg_pct` | Float64 | Effective field goal percentage, as a decimal. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `pts_per48` | Float64 | Points scored per 48 minutes played. |

**LeagueMax**

| col_name | type | description |
|---|---|---|
| `pts` | Float64 | Points scored. |
| `fg_pct` | Float64 | Field goal percentage (0-1). |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |
| `ft_pct` | Float64 | Free throw percentage (0-1). |
| `efg_pct` | Float64 | Effective field goal percentage, as a decimal. |
| `ts_pct` | Float64 | True shooting percentage (0-1). |
| `pts_per48` | Float64 | Points scored per 48 minutes played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_homepageleaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_homepageleaders-example}

```python
nba_stats_homepageleaders(league_id='00', season='2024-25')
```

## nba_stats_homepagev2

Homepage v2.

**Endpoint URL:** `GET https://stats.nba.com/stats/homepagev2`

**Valid URL:** [https://stats.nba.com/stats/homepagev2?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatType=Traditional](https://stats.nba.com/stats/homepagev2?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&StatType=Traditional)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `StatType` | `stat_type` |  |  | `Y` |  |

### Returns {#nba_stats_homepagev2-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`HomePageStat1`, `HomePageStat2`, `HomePageStat3`, `HomePageStat4`, `HomePageStat5`, `HomePageStat6`, `HomePageStat7`, `HomePageStat8`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**HomePageStat1**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `pts` | Float64 | Points scored. |

**HomePageStat2**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `reb` | Float64 | Rebounds per game. |

**HomePageStat3**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `ast` | Float64 | Assists. |

**HomePageStat4**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `stl` | Float64 | Steals. |

**HomePageStat5**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `fg_pct` | Float64 | Field goal percentage (0-1). |

**HomePageStat6**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `ft_pct` | Float64 | Free throw percentage (0-1). |

**HomePageStat7**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `fg3_pct` | Float64 | Three-point field goal percentage (0-1). |

**HomePageStat8**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `blk` | Float64 | Blocks. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_homepagev2-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_homepagev2-example}

```python
nba_stats_homepagev2(league_id='00', season='2024-25')
```

## nba_stats_hustlestatsboxscore

Hustle stats box score.

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
| `game_id` | String | Unique game identifier. |
| `hustle_status` | Int64 | Hustle status. |

**PlayerStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `start_position` | String | Position the player started the game at (F, C, or G); empty for reserves. |
| `comment` | String | Player status / inactive reason (e.g. 'DNP - Coach's Decision', 'Inactive'). |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | Int64 | Points scored. |
| `contested_shots` | Float64 | Defensively contested shots. |
| `contested_shots_2_pt` | Float64 | Opponent two-point attempts contested. |
| `contested_shots_3_pt` | Float64 | Opponent three-point attempts contested. |
| `deflections` | Float64 | Defensive deflections. |
| `charges_drawn` | Float64 | Charges drawn. |
| `screen_assists` | Float64 | Screen assists (resulting in a basket). |
| `screen_ast_pts` | Float64 | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | Float64 | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | Float64 | Loose balls recovered while on defense. |
| `loose_balls_recovered` | Float64 | Total loose balls recovered. |
| `off_boxouts` | Float64 | Box-outs recorded on the offensive glass. |
| `def_boxouts` | Float64 | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | Float64 | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | Float64 | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | Float64 | Box-outs executed. |

**TeamStats**

| col_name | type | description |
|---|---|---|
| `game_id` | String | Unique game identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_city` | String | Team city or region (e.g. 'Las Vegas'). |
| `minutes` | String | Minutes played, formatted MM:SS (V3 PT-duration parsed) or decimal minutes (V2). |
| `pts` | Int64 | Points scored. |
| `contested_shots` | Float64 | Defensively contested shots. |
| `contested_shots_2_pt` | Float64 | Opponent two-point attempts contested. |
| `contested_shots_3_pt` | Float64 | Opponent three-point attempts contested. |
| `deflections` | Float64 | Defensive deflections. |
| `charges_drawn` | Float64 | Charges drawn. |
| `screen_assists` | Float64 | Screen assists (resulting in a basket). |
| `screen_ast_pts` | Float64 | Points teammates scored directly off the row's screen assists. |
| `off_loose_balls_recovered` | Float64 | Loose balls recovered while on offense. |
| `def_loose_balls_recovered` | Float64 | Loose balls recovered while on defense. |
| `loose_balls_recovered` | Float64 | Total loose balls recovered. |
| `off_boxouts` | Float64 | Box-outs recorded on the offensive glass. |
| `def_boxouts` | Float64 | Box-outs recorded on the defensive glass. |
| `box_out_player_team_rebs` | Float64 | Team rebounds secured following the row's box-outs. |
| `box_out_player_rebs` | Float64 | Rebounds the player secured directly off their own box-outs. |
| `box_outs` | Float64 | Box-outs executed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_hustlestatsboxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_hustlestatsboxscore-example}

```python
nba_stats_hustlestatsboxscore()
```

## nba_stats_infographicfanduelplayer

Infographic fanduel player.

**Endpoint URL:** `GET https://stats.nba.com/stats/infographicfanduelplayer`

**Valid URL:** [https://stats.nba.com/stats/infographicfanduelplayer?GameID=0022201086](https://stats.nba.com/stats/infographicfanduelplayer?GameID=0022201086)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_infographicfanduelplayer-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `jersey_num` | String | Jersey number worn by the player. |
| `player_position` | String | Player's position abbreviation as stats.nba.com lists it (e.g. 'G', 'F-G', 'C'). |
| `location` | String | Location. |
| `fan_duel_pts` | Float64 | Scoring or score-margin metric for fan duel points in the requested NBA or WNBA Stats split. |
| `nba_fantasy_pts` | Float64 | Nba fantasy points for the requested NBA or WNBA Stats split. |
| `usg_pct` | Float64 | Percentage or rate for usage percentage in the requested NBA or WNBA Stats split. |
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
| `reb` | Int64 | Rebounds per game. |
| `ast` | Int64 | Assists. |
| `tov` | Int64 | Turnovers. |
| `stl` | Int64 | Steals. |
| `blk` | Int64 | Blocks. |
| `blka` | Int64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Int64 | Personal fouls. |
| `pfd` | Int64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Int64 | Points scored. |
| `plus_minus` | Int64 | Plus/minus point differential while on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_infographicfanduelplayer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_infographicfanduelplayer-example}

```python
nba_stats_infographicfanduelplayer()
```

## nba_stats_leaderstiles

Leaders tiles.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaderstiles`

**Valid URL:** [https://stats.nba.com/stats/leaderstiles?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&Stat=PTS](https://stats.nba.com/stats/leaderstiles?GameScope=Season&LeagueID=00&PlayerOrTeam=Team&PlayerScope=All+Players&Season=2024-25&SeasonType=Regular+Season&Stat=PTS)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameScope` | `game_scope_detailed` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team` |  |  | `Y` |  |
| `PlayerScope` | `player_scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `Stat` | `stat` |  |  | `Y` |  |

### Returns {#nba_stats_leaderstiles-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`LeadersTiles`, `AllTimeSeasonHigh`, `LastSeasonHigh`, `LowSeasonHigh`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**LeadersTiles**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | Float64 | Points scored. |

**AllTimeSeasonHigh**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `pts` | Float64 | Points scored. |
| `season_year` | String | Season year string ('YYYY-YY' format). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |

**LastSeasonHigh**

| col_name | type | description |
|---|---|---|
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player_id` | Int64 | Unique player identifier. |
| `player` | String | Player name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `pts` | Float64 | Points scored. |

**LowSeasonHigh**

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `pts` | Float64 | Points scored. |
| `season_year` | String | Season year string ('YYYY-YY' format). |
| `team_id` | Int64 | Unique team identifier. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_leaderstiles-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_leaderstiles-example}

```python
nba_stats_leaderstiles(league_id='00', season='2024-25')
```

## nba_stats_matchupsrollup

Matchups rollup.

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
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |

### Returns {#nba_stats_matchupsrollup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | String | Unique season identifier. |
| `position` | String | Listed roster position (G, F, C, etc.). |
| `percent_of_time` | Float64 | Time value for percent of time in the NBA or WNBA Stats result set. |
| `def_player_id` | Int64 | Stats API identifier for defensive player identifier associated with this NBA or WNBA Stats row. |
| `def_player_name` | String | Display name for defensive player name associated with this NBA or WNBA Stats row. |
| `gp` | Int64 | Games played. |
| `matchup_min` | Float64 | NBA or WNBA Stats value for matchup minutes in the matchupsrollup result set. |
| `partial_poss` | Float64 | Estimated partial possessions credited to the stint or rotation interval. |
| `player_pts` | Float64 | Scoring or score-margin metric for player points in the requested NBA or WNBA Stats split. |
| `team_pts` | Float64 | Scoring or score-margin metric for team points in the requested NBA or WNBA Stats split. |
| `matchup_ast` | Float64 | NBA or WNBA Stats value for matchup assists in the matchupsrollup result set. |
| `matchup_tov` | Float64 | Turnover or loose-ball metric for matchup turnovers in the requested NBA or WNBA Stats split. |
| `matchup_blk` | Float64 | NBA or WNBA Stats value for matchup blocks in the matchupsrollup result set. |
| `matchup_fgm` | Float64 | Shooting metric for matchup fgm in the requested NBA or WNBA Stats split. |
| `matchup_fga` | Float64 | Shooting metric for matchup fga in the requested NBA or WNBA Stats split. |
| `matchup_fg_pct` | Float64 | Percentage or rate for matchup field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_fg3_m` | Float64 | Shooting metric for matchup fg3m in the requested NBA or WNBA Stats split. |
| `matchup_fg3_a` | Float64 | Shooting metric for matchup fg3a in the requested NBA or WNBA Stats split. |
| `matchup_fg3_pct` | Float64 | Percentage or rate for matchup three-point field goals percentage in the requested NBA or WNBA Stats split. |
| `matchup_ftm` | Float64 | NBA or WNBA Stats value for matchup ftm in the matchupsrollup result set. |
| `matchup_fta` | Float64 | NBA or WNBA Stats value for matchup fta in the matchupsrollup result set. |
| `sfl` | Float64 | NBA or WNBA Stats value for sfl in the matchupsrollup result set. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_matchupsrollup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_matchupsrollup-example}

```python
nba_stats_matchupsrollup(league_id='00', season='2024-25')
```

## nba_stats_playbyplayv3

Play by play v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.nba.com/stats/playbyplayv3?EndPeriod=0&GameID=0022201086&StartPeriod=0](https://stats.nba.com/stats/playbyplayv3?EndPeriod=0&GameID=0022201086&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#nba_stats_playbyplayv3-returns}

**`return_parsed=True`** (default) — the output of `parse_nba_stats_result_sets`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: the committed capture tests/fixtures/nba_stats/endpoints/playbyplayv3.json is not a resultSets envelope (top-level keys: game, meta), which parse_nba_stats_result_sets does not read, so the parsed output is empty; use return_parsed=False for the raw payload.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_playbyplayv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_playbyplayv3-example}

```python
nba_stats_playbyplayv3()
```
