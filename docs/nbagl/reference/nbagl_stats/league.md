# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — League

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — League — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_leaguegamelog

League game log.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguegamelog`

**Valid URL:** [https://stats.nba.com/stats/leaguegamelog?Counter=0&DateFrom=&DateTo=&Direction=ASC&LeagueID=20&PlayerOrTeam=T&Season=2024-25&SeasonType=Regular+Season&Sorter=DATE](https://stats.nba.com/stats/leaguegamelog?Counter=0&DateFrom=&DateTo=&Direction=ASC&LeagueID=20&PlayerOrTeam=T&Season=2024-25&SeasonType=Regular+Season&Sorter=DATE)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Counter` | `counter` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Direction` | `direction` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PlayerOrTeam` | `player_or_team_abbreviation` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `Sorter` | `sorter` |  |  | `Y` |  |

### Returns {#nbagl_stats_leaguegamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `game_id` | String |  |
| `game_date` | String |  |
| `matchup` | String |  |
| `wl` | String |  |
| `min` | Int64 |  |
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
| `stl` | Int64 |  |
| `blk` | Int64 |  |
| `tov` | Int64 |  |
| `pf` | Int64 |  |
| `pts` | Int64 |  |
| `plus_minus` | Int64 |  |
| `video_available` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_leaguegamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_leaguegamelog-example}

```python
nbagl_stats_leaguegamelog(league_id='20', season='2024-25')
```

## nbagl_stats_leagueleaders

League leaders.

**Endpoint URL:** `GET https://stats.nba.com/stats/leagueleaders`

**Valid URL:** [https://stats.nba.com/stats/leagueleaders?ActiveFlag=&LeagueID=20&PerMode=Totals&Scope=S&Season=2024-25&SeasonType=Regular+Season&StatCategory=PTS](https://stats.nba.com/stats/leagueleaders?ActiveFlag=&LeagueID=20&PerMode=Totals&Scope=S&Season=2024-25&SeasonType=Regular+Season&StatCategory=PTS)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ActiveFlag` | `active_flag_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode48` |  |  | `Y` |  |
| `Scope` | `scope` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `StatCategory` | `stat_category_abbreviation` |  |  | `Y` |  |

### Returns {#nbagl_stats_leagueleaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 |  |
| `rank` | Int64 | Rank of the row within the returned leaderboard (1 = best) for the stat the endpoint sorts by. |
| `player` | String |  |
| `team_id` | Int64 |  |
| `team` | String |  |
| `gp` | Int64 |  |
| `min` | Float64 |  |
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
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `tov` | Float64 |  |
| `pts` | Float64 |  |
| `eff` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_leagueleaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_leagueleaders-example}

```python
nbagl_stats_leagueleaders(league_id='20', season='2024-25')
```

## nbagl_stats_leaguelineupviz

League lineup viz.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguelineupviz`

**Valid URL:** [https://stats.nba.com/stats/leaguelineupviz?Conference=&DateFrom=&DateTo=&Division=&GameSegment=&GroupQuantity=5&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&MinutesMin=10&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/leaguelineupviz?Conference=&DateFrom=&DateTo=&Division=&GameSegment=&GroupQuantity=5&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&MinutesMin=10&Month=0&OpponentTeamID=0&Outcome=&PORound=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&TeamID=0&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `Conference` | `conference_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `Division` | `division_simple_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GroupQuantity` | `group_quantity` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `MeasureType` | `measure_type_detailed_defense` |  |  | `Y` |  |
| `MinutesMin` | `minutes_min` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PaceAdjust` | `pace_adjust` |  |  | `Y` |  |
| `PerMode` | `per_mode_detailed` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_leaguelineupviz-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | String |  |
| `group_name` | String | The lineup's five players as a ' - ' separated string of abbreviated names. |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `min` | Float64 |  |
| `off_rating` | Float64 | Points scored per 100 possessions with the lineup on the floor (offensive rating). |
| `def_rating` | Float64 | Points allowed per 100 possessions with the lineup on the floor (defensive rating). |
| `net_rating` | Float64 |  |
| `pace` | Float64 |  |
| `ts_pct` | Float64 |  |
| `fta_rate` | Float64 | Free throw attempts per field goal attempt for the lineup. |
| `tm_ast_pct` | Float64 | Percentage of the lineup's made field goals that were assisted, as a decimal. |
| `pct_fga_2_pt` | Float64 | Share of field goal attempts taken as two-pointers, as a decimal. |
| `pct_fga_3_pt` | Float64 | Share of field goal attempts taken as three-pointers, as a decimal. |
| `pct_pts_2_pt_mr` | Float64 | Share of points scored on mid-range two-pointers, as a decimal. |
| `pct_pts_fb` | Float64 | Share of points scored on fast breaks, as a decimal. |
| `pct_pts_ft` | Float64 | Share of points scored at the free throw line, as a decimal. |
| `pct_pts_paint` | Float64 | Share of points scored in the paint, as a decimal. |
| `pct_ast_fgm` | Float64 | Percentage of made field goals that were assisted, as a decimal. |
| `pct_uast_fgm` | Float64 | Percentage of made field goals that were unassisted, as a decimal. |
| `opp_fg3_pct` | Float64 | Opponent three-point percentage against the lineup, as a decimal. |
| `opp_efg_pct` | Float64 | Opponent effective field goal percentage against the lineup, as a decimal. |
| `opp_fta_rate` | Float64 | Opponent free throw attempt rate against the lineup. |
| `opp_tov_pct` | Float64 | Opponent turnover percentage forced by the lineup. |
| `sum_tm_min` | Float64 | Total team minutes summed across the lineup's stints on the floor. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_leaguelineupviz-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_leaguelineupviz-example}

```python
nbagl_stats_leaguelineupviz(league_id='20', season='2024-25')
```

## nbagl_stats_leagueplayerondetails

League player on details.

**Endpoint URL:** `GET https://stats.nba.com/stats/leagueplayerondetails`

**Valid URL:** [https://stats.nba.com/stats/leagueplayerondetails?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=](https://stats.nba.com/stats/leagueplayerondetails?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=)

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
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_leagueplayerondetails-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the grouping family used for this dashboard or split row. |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | Int64 | Stats API identifier for vs player identifier associated with this NBA or WNBA Stats row. |
| `vs_player_name` | String | Display name for vs player name associated with this NBA or WNBA Stats row. |
| `court_status` | String | Indicates whether the compared player was on court or off court for the split row. |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
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
| `tov` | Float64 |  |
| `stl` | Float64 |  |
| `blk` | Float64 |  |
| `blka` | Float64 | Blocked field-goal attempts against for the requested NBA or WNBA Stats split. |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn for the requested NBA or WNBA Stats split. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_leagueplayerondetails-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_leagueplayerondetails-example}

```python
nbagl_stats_leagueplayerondetails(league_id='20', season='2024-25')
```

## nbagl_stats_leaguestandingsv3

League standings v3.

**Endpoint URL:** `GET https://stats.nba.com/stats/leaguestandingsv3`

**Valid URL:** [https://stats.nba.com/stats/leaguestandingsv3?LeagueID=20&Season=2024-25&SeasonType=Regular+Season&SeasonYear=](https://stats.nba.com/stats/leaguestandingsv3?LeagueID=20&Season=2024-25&SeasonType=Regular+Season&SeasonYear=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `SeasonYear` | `season_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_leaguestandingsv3-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `league_id` | String | League identifier used in compact NBA Stats schedule and scoreboard result sets. |
| `season_id` | String | Stats API identifier for seasonid associated with this NBA or WNBA Stats row. |
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_slug` | String | URL slug for teamslug used by NBA or WNBA Stats pages. |
| `conference` | String |  |
| `conference_record` | String | NBA or WNBA Stats value for conferencerecord in the leaguestandingsv3 result set. |
| `playoff_rank` | Int64 | NBA or WNBA Stats value for playoffrank in the leaguestandingsv3 result set. |
| `clinch_indicator` | String | NBA or WNBA Stats value for clinchindicator in the leaguestandingsv3 result set. |
| `division` | String |  |
| `division_record` | String | NBA or WNBA Stats value for divisionrecord in the leaguestandingsv3 result set. |
| `division_rank` | Int64 | NBA or WNBA Stats value for divisionrank in the leaguestandingsv3 result set. |
| `wins` | Int64 |  |
| `losses` | Int64 |  |
| `win_pct` | Float64 | Winning percentage for the team or split represented by this row. |
| `league_rank` | Int64 | NBA or WNBA Stats value for leaguerank in the leaguestandingsv3 result set. |
| `record` | String |  |
| `home` | String |  |
| `road` | String |  |
| `l10` | String |  |
| `last10_home` | String | NBA or WNBA Stats value for last10home in the leaguestandingsv3 result set. |
| `last10_road` | String | NBA or WNBA Stats value for last10road in the leaguestandingsv3 result set. |
| `ot` | String |  |
| `three_pts_or_less` | String | Scoring or score-margin metric for threeptsorless in the requested NBA or WNBA Stats split. |
| `ten_pts_or_more` | String | Scoring or score-margin metric for tenptsormore in the requested NBA or WNBA Stats split. |
| `long_home_streak` | Int64 | NBA or WNBA Stats value for longhomestreak in the leaguestandingsv3 result set. |
| `str_long_home_streak` | String | NBA or WNBA Stats value for strlonghomestreak in the leaguestandingsv3 result set. |
| `long_road_streak` | Int64 | NBA or WNBA Stats value for longroadstreak in the leaguestandingsv3 result set. |
| `str_long_road_streak` | String | NBA or WNBA Stats value for strlongroadstreak in the leaguestandingsv3 result set. |
| `long_win_streak` | Int64 | NBA or WNBA Stats value for longwinstreak in the leaguestandingsv3 result set. |
| `long_loss_streak` | Int64 | NBA or WNBA Stats value for longlossstreak in the leaguestandingsv3 result set. |
| `current_home_streak` | Int64 | NBA or WNBA Stats value for currenthomestreak in the leaguestandingsv3 result set. |
| `str_current_home_streak` | String | NBA or WNBA Stats value for strcurrenthomestreak in the leaguestandingsv3 result set. |
| `current_road_streak` | Int64 | NBA or WNBA Stats value for currentroadstreak in the leaguestandingsv3 result set. |
| `str_current_road_streak` | String | NBA or WNBA Stats value for strcurrentroadstreak in the leaguestandingsv3 result set. |
| `current_streak` | Int64 | NBA or WNBA Stats value for currentstreak in the leaguestandingsv3 result set. |
| `str_current_streak` | String |  |
| `conference_games_back` | Float64 | NBA or WNBA Stats value for conferencegamesback in the leaguestandingsv3 result set. |
| `division_games_back` | Float64 | NBA or WNBA Stats value for divisiongamesback in the leaguestandingsv3 result set. |
| `clinched_conference_title` | Int64 | Flag indicating clinchedconferencetitle for the requested NBA or WNBA Stats context. |
| `clinched_division_title` | Int64 | Flag indicating clincheddivisiontitle for the requested NBA or WNBA Stats context. |
| `clinched_playoff_birth` | Int64 | Flag indicating clinchedplayoffbirth for the requested NBA or WNBA Stats context. |
| `clinched_play_in` | Int64 | Flag indicating clinchedplayin for the requested NBA or WNBA Stats context. |
| `eliminated_conference` | Int64 | Flag indicating eliminatedconference for the requested NBA or WNBA Stats context. |
| `eliminated_division` | Int64 | Flag indicating eliminateddivision for the requested NBA or WNBA Stats context. |
| `ahead_at_half` | String | NBA or WNBA Stats value for aheadathalf in the leaguestandingsv3 result set. |
| `behind_at_half` | String | NBA or WNBA Stats value for behindathalf in the leaguestandingsv3 result set. |
| `tied_at_half` | String | NBA or WNBA Stats value for tiedathalf in the leaguestandingsv3 result set. |
| `ahead_at_third` | String | NBA or WNBA Stats value for aheadatthird in the leaguestandingsv3 result set. |
| `behind_at_third` | String | NBA or WNBA Stats value for behindatthird in the leaguestandingsv3 result set. |
| `tied_at_third` | String | NBA or WNBA Stats value for tiedatthird in the leaguestandingsv3 result set. |
| `score100_pts` | String | Scoring or score-margin metric for score100pts in the requested NBA or WNBA Stats split. |
| `opp_score100_pts` | String | Scoring or score-margin metric for oppscore100pts in the requested NBA or WNBA Stats split. |
| `opp_over500` | String | NBA or WNBA Stats value for oppover500 in the leaguestandingsv3 result set. |
| `lead_in_fgpct` | String | Shooting metric for leadinfgpct in the requested NBA or WNBA Stats split. |
| `lead_in_reb` | String | Rebounding metric for leadinreb in the requested NBA or WNBA Stats split. |
| `fewer_turnovers` | String | Turnover or loose-ball metric for fewerturnovers in the requested NBA or WNBA Stats split. |
| `points_pg` | Float64 | Scoring or score-margin metric for pointspg in the requested NBA or WNBA Stats split. |
| `opp_points_pg` | Float64 | Scoring or score-margin metric for opppointspg in the requested NBA or WNBA Stats split. |
| `diff_points_pg` | Float64 | Scoring or score-margin metric for diffpointspg in the requested NBA or WNBA Stats split. |
| `vs_east` | String | NBA or WNBA Stats value for vseast in the leaguestandingsv3 result set. |
| `vs_atlantic` | String | NBA or WNBA Stats value for vsatlantic in the leaguestandingsv3 result set. |
| `vs_central` | String | NBA or WNBA Stats value for vscentral in the leaguestandingsv3 result set. |
| `vs_southeast` | String | NBA or WNBA Stats value for vssoutheast in the leaguestandingsv3 result set. |
| `vs_west` | String | NBA or WNBA Stats value for vswest in the leaguestandingsv3 result set. |
| `vs_northwest` | String | NBA or WNBA Stats value for vsnorthwest in the leaguestandingsv3 result set. |
| `vs_pacific` | String | NBA or WNBA Stats value for vspacific in the leaguestandingsv3 result set. |
| `vs_southwest` | String | NBA or WNBA Stats value for vssouthwest in the leaguestandingsv3 result set. |
| `jan` | String | Value for January in the endpoint's monthly NBA or WNBA Stats split. |
| `feb` | String | Value for February in the endpoint's monthly NBA or WNBA Stats split. |
| `mar` | String | Value for March in the endpoint's monthly NBA or WNBA Stats split. |
| `apr` | String | Value for April in the endpoint's monthly NBA or WNBA Stats split. |
| `may` | String | Value for May in the endpoint's monthly NBA or WNBA Stats split. |
| `jun` | String | Value for June in the endpoint's monthly NBA or WNBA Stats split. |
| `jul` | String | Value for July in the endpoint's monthly NBA or WNBA Stats split. |
| `aug` | String | Value for August in the endpoint's monthly NBA or WNBA Stats split. |
| `sep` | String | Value for September in the endpoint's monthly NBA or WNBA Stats split. |
| `oct` | String | Value for October in the endpoint's monthly NBA or WNBA Stats split. |
| `nov` | String | Value for November in the endpoint's monthly NBA or WNBA Stats split. |
| `dec` | String | Value for December in the endpoint's monthly NBA or WNBA Stats split. |
| `score_80_plus` | String | Scoring or score-margin metric for score 80 plus in the requested NBA or WNBA Stats split. |
| `opp_score_80_plus` | String | Opponent score 80 plus for the requested NBA or WNBA team, player, lineup, or game split. |
| `score_below_80` | String | Scoring or score-margin metric for score below 80 in the requested NBA or WNBA Stats split. |
| `opp_score_below_80` | String | Opponent score below 80 for the requested NBA or WNBA team, player, lineup, or game split. |
| `total_points` | Int64 | Scoring or score-margin metric for totalpoints in the requested NBA or WNBA Stats split. |
| `opp_total_points` | Int64 | Scoring or score-margin metric for opptotalpoints in the requested NBA or WNBA Stats split. |
| `diff_total_points` | Int64 | Scoring or score-margin metric for difftotalpoints in the requested NBA or WNBA Stats split. |
| `league_games_back` | Float64 | NBA or WNBA Stats value for leaguegamesback in the leaguestandingsv3 result set. |
| `playoff_seeding` | Int64 | NBA or WNBA Stats value for playoffseeding in the leaguestandingsv3 result set. |
| `clinched_post_season` | Int64 | Flag indicating clinchedpostseason for the requested NBA or WNBA Stats context. |
| `neutral` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_leaguestandingsv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_leaguestandingsv3-example}

```python
nbagl_stats_leaguestandingsv3(league_id='20', season='2024-25')
```
