# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team

> NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Team — function reference in sdv-py, the SportsDataverse Python package.

## nbagl_stats_teamdetails

Team details.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamdetails`

**Valid URL:** [https://stats.nba.com/stats/teamdetails?TeamID=1612709914](https://stats.nba.com/stats/teamdetails?TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamdetails-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`TeamBackground`, `TeamHistory`, `TeamSocialSites`, `TeamAwardsChampionships`, `TeamAwardsConf`, `TeamAwardsDiv`, `TeamHof`, `TeamRetired`, `TeamAwardsCommCup`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**TeamBackground**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `abbreviation` | String |  |
| `nickname` | String |  |
| `yearfounded` | Int64 | Year the franchise was founded. |
| `city` | String |  |
| `arena` | String |  |
| `arenacapacity` | String | Seating capacity of the team's home arena. |
| `owner` | String | Name of the team's owner or ownership group. |
| `generalmanager` | String | Name of the team's general manager. |
| `headcoach` | String | Name of the team's head coach. |
| `dleagueaffiliation` | String | Name of the team's G League (formerly D-League) affiliate. |

**TeamHistory**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `city` | String |  |
| `nickname` | String |  |
| `yearfounded` | Int64 | Year the franchise was founded. |
| `yearactivetill` | Int64 |  |

**TeamSocialSites**

| col_name | type | description |
|---|---|---|
| `accounttype` | String |  |
| `website_link` | String |  |

**TeamAwardsChampionships**

| col_name | type | description |
|---|---|---|
| `yearawarded` | Int64 |  |
| `oppositeteam` | String |  |

**TeamAwardsConf**

| col_name | type | description |
|---|---|---|
| `yearawarded` | Int64 |  |
| `oppositeteam` | String |  |

**TeamAwardsDiv**

| col_name | type | description |
|---|---|---|
| `yearawarded` | Int64 |  |
| `oppositeteam` | String |  |

**TeamHof**

| col_name | type | description |
|---|---|---|
| `playerid` | Int64 |  |
| `player` | String |  |
| `position` | String |  |
| `jersey` | String |  |
| `seasonswithteam` | String |  |
| `year` | Int64 |  |

**TeamRetired**

| col_name | type | description |
|---|---|---|
| `playerid` | Int64 |  |
| `player` | String |  |
| `position` | String |  |
| `jersey` | String |  |
| `seasonswithteam` | String |  |
| `year` | Int64 |  |

**TeamAwardsCommCup**

| col_name | type | description |
|---|---|---|
| `yearawarded` | Int64 |  |
| `oppositeteam` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamdetails-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamdetails-example}

```python
nbagl_stats_teamdetails()
```

## nbagl_stats_teamestimatedmetrics

Team estimated metrics.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamestimatedmetrics`

**Valid URL:** [https://stats.nba.com/stats/teamestimatedmetrics?LeagueID=20&Season=2024-25&SeasonType=Regular+Season](https://stats.nba.com/stats/teamestimatedmetrics?LeagueID=20&Season=2024-25&SeasonType=Regular+Season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |

### Returns {#nbagl_stats_teamestimatedmetrics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_name` | String |  |
| `team_id` | Int64 |  |
| `gp` | Int64 |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
| `min` | Float64 |  |
| `e_off_rating` | Float64 | Estimated offensive rating for the requested NBA or WNBA Stats split. |
| `e_def_rating` | Float64 | Estimated defensive rating for the requested NBA or WNBA Stats split. |
| `e_net_rating` | Float64 | Estimated net rating for the requested NBA or WNBA Stats split. |
| `e_pace` | Float64 | Estimated pace for the requested NBA or WNBA Stats split. |
| `e_ast_ratio` | Float64 | Estimated assist ratio for the requested NBA or WNBA Stats split. |
| `e_oreb_pct` | Float64 | Estimated offensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_dreb_pct` | Float64 | Estimated defensive rebound percentage for the requested NBA or WNBA Stats split. |
| `e_reb_pct` | Float64 | Estimated rebound percentage for the requested NBA or WNBA Stats split. |
| `e_tm_tov_pct` | Float64 | Estimated team turnover percentage for the requested NBA or WNBA Stats split. |
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
| `e_tm_tov_pct_rank` | Int64 | Rank for e team turnovers percentage within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |
| `e_pace_rank` | Int64 | Rank for e pace within the requested NBA or WNBA Stats leaderboard or split, where 1 is the leader. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamestimatedmetrics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamestimatedmetrics-example}

```python
nbagl_stats_teamestimatedmetrics(league_id='20', season='2024-25')
```

## nbagl_stats_teamgamelog

Team game log.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamgamelog`

**Valid URL:** [https://stats.nba.com/stats/teamgamelog?DateFrom=&DateTo=&LeagueID=20&Season=2024-25&SeasonType=Regular+Season&TeamID=1612709914](https://stats.nba.com/stats/teamgamelog?DateFrom=&DateTo=&LeagueID=20&Season=2024-25&SeasonType=Regular+Season&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamgamelog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `game_id` | String |  |
| `game_date` | String |  |
| `matchup` | String |  |
| `wl` | String |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `w_pct` | Float64 |  |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamgamelog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamgamelog-example}

```python
nbagl_stats_teamgamelog(league_id='20', season='2024-25')
```

## nbagl_stats_teamgamelogs

Team game logs.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamgamelogs`

**Valid URL:** [https://stats.nba.com/stats/teamgamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OppTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/teamgamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OppTeamID=0&Outcome=&PORound=&PerMode=Totals&Period=0&PlayerID=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

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
| `OppTeamID` | `opp_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `PORound` | `po_round_nullable` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple_nullable` |  |  | `Y` |  |
| `Period` | `period_nullable` |  |  | `Y` |  |
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `ShotClockRange` | `shot_clock_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamgamelogs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_year` | String |  |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `game_id` | String |  |
| `game_date` | String |  |
| `matchup` | String |  |
| `wl` | String |  |
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
| `available_flag` | Int64 | Flag indicating whether the requested NBA or WNBA Stats video or data asset is available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamgamelogs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamgamelogs-example}

```python
nbagl_stats_teamgamelogs(league_id='20', season_nullable='2024-25')
```

## nbagl_stats_teaminfocommon

Team info common.

**Endpoint URL:** `GET https://stats.nba.com/stats/teaminfocommon`

**Valid URL:** [https://stats.nba.com/stats/teaminfocommon?LeagueID=20&Season=2024-25&SeasonType=Regular+Season&TeamID=1612709914](https://stats.nba.com/stats/teaminfocommon?LeagueID=20&Season=2024-25&SeasonType=Regular+Season&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_nullable` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_teaminfocommon-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`TeamInfoCommon`, `TeamSeasonRanks`, `AvailableSeasons`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**TeamInfoCommon**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `season_year` | String |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `team_abbreviation` | String |  |
| `team_conference` | String | Conference the team belongs to. |
| `team_division` | String | Division the team belongs to. |
| `team_code` | String | Lowercase slug of the team used in stats.nba.com URLs (e.g. 'lakers'). |
| `team_slug` | String |  |
| `w` | Int64 |  |
| `l` | Int64 |  |
| `pct` | Float64 |  |
| `conf_rank` | Int64 | Team's current rank within its conference. |
| `div_rank` | Int64 | Team's current rank within its division. |
| `min_year` | String |  |
| `max_year` | String |  |

**TeamSeasonRanks**

| col_name | type | description |
|---|---|---|
| `league_id` | String |  |
| `season_id` | String |  |
| `team_id` | Int64 |  |
| `pts_rank` | Int64 |  |
| `pts_pg` | Float64 |  |
| `reb_rank` | Int64 |  |
| `reb_pg` | Float64 |  |
| `ast_rank` | Int64 |  |
| `ast_pg` | Float64 |  |
| `opp_pts_rank` | Int64 |  |
| `opp_pts_pg` | Float64 |  |

**AvailableSeasons**

| col_name | type | description |
|---|---|---|
| `season_id` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teaminfocommon-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teaminfocommon-example}

```python
nbagl_stats_teaminfocommon(league_id='20', season_nullable='2024-25')
```

## nbagl_stats_teamvsplayer

Team vs player.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamvsplayer`

**Valid URL:** [https://stats.nba.com/stats/teamvsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=&VsPlayerID=1631457](https://stats.nba.com/stats/teamvsplayer?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=20&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PaceAdjust=N&PerMode=Totals&Period=0&PlayerID=&PlusMinus=N&Rank=N&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=1612709914&VsConference=&VsDivision=&VsPlayerID=1631457)

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
| `PlayerID` | `player_id_nullable` |  |  | `Y` |  |
| `PlusMinus` | `plus_minus` |  |  | `Y` |  |
| `Rank` | `rank` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_playoffs` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsPlayerID` | `vs_player_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamvsplayer-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Overall`, `vsPlayerOverall`, `OnOffCourt`, `ShotDistanceOverall`, `ShotDistanceOnCourt`, `ShotDistanceOffCourt`, `ShotAreaOverall`, `ShotAreaOnCourt`, `ShotAreaOffCourt`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Overall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
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
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**vsPlayerOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `player_id` | Int64 |  |
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
| `blka` | Float64 | Shot attempts blocked by opponents (blocks against). |
| `pf` | Float64 |  |
| `pfd` | Float64 | Personal fouls drawn. |
| `pts` | Float64 |  |
| `plus_minus` | Float64 |  |
| `nba_fantasy_pts` | Float64 | Fantasy points under the NBA's fantasy scoring formula. |
| `dd2` | Int64 | Double-doubles recorded over the split. |
| `td3` | Int64 | Triple-doubles recorded over the split. |
| `wnba_fantasy_pts` | Float64 | Fantasy points under the WNBA's fantasy scoring formula. |
| `gp_rank` | Int64 | League rank of the row's games played for the season and split. |
| `w_rank` | Int64 | League rank of the row's wins for the season and split. |
| `l_rank` | Int64 | League rank of the row's losses for the season and split. |
| `w_pct_rank` | Int64 | League rank of the row's win percentage for the season and split. |
| `min_rank` | Int64 | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | Int64 | League rank of the row's field goals made for the season and split. |
| `fga_rank` | Int64 | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | Int64 | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | Int64 | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | Int64 | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | Int64 | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | Int64 | League rank of the row's free throws made for the season and split. |
| `fta_rank` | Int64 | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | Int64 | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | Int64 | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | Int64 | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | Int64 | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | Int64 | League rank of the row's assists for the season and split. |
| `tov_rank` | Int64 | League rank of the row's turnovers for the season and split. |
| `stl_rank` | Int64 | League rank of the row's steals for the season and split. |
| `blk_rank` | Int64 | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | Int64 | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | Int64 | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | Int64 | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | Int64 | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | Int64 | League rank of the row's plus-minus point differential while on the floor for the season and split. |
| `nba_fantasy_pts_rank` | Int64 | League rank of the row's NBA fantasy points (league scoring formula) for the season and split. |
| `dd2_rank` | Int64 | League rank of the row's double-doubles for the season and split. |
| `td3_rank` | Int64 | League rank of the row's triple-doubles for the season and split. |
| `wnba_fantasy_pts_rank` | Int64 | League rank of the row's WNBA fantasy points (league scoring formula) for the season and split. |
| `team_count` | Int64 | Number of distinct teams aggregated into the split row. |

**OnOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `gp` | String |  |
| `w` | String |  |
| `l` | String |  |
| `w_pct` | String |  |
| `min` | String |  |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |
| `fg3_m` | String | Three-point field goals made. |
| `fg3_a` | String | Three-point field goal attempts. |
| `fg3_pct` | String |  |
| `ftm` | String |  |
| `fta` | String |  |
| `ft_pct` | String |  |
| `oreb` | String |  |
| `dreb` | String |  |
| `reb` | String |  |
| `ast` | String |  |
| `tov` | String |  |
| `stl` | String |  |
| `blk` | String |  |
| `blka` | String | Shot attempts blocked by opponents (blocks against). |
| `pf` | String |  |
| `pfd` | String | Personal fouls drawn. |
| `pts` | String |  |
| `plus_minus` | String |  |
| `gp_rank` | String | League rank of the row's games played for the season and split. |
| `w_rank` | String | League rank of the row's wins for the season and split. |
| `l_rank` | String | League rank of the row's losses for the season and split. |
| `w_pct_rank` | String | League rank of the row's win percentage for the season and split. |
| `min_rank` | String | League rank of the row's minutes played for the season and split. |
| `fgm_rank` | String | League rank of the row's field goals made for the season and split. |
| `fga_rank` | String | League rank of the row's field goals attempted for the season and split. |
| `fg_pct_rank` | String | League rank of the row's field goal percentage for the season and split. |
| `fg3_m_rank` | String | League rank of the row's three-point field goals made for the season and split. |
| `fg3_a_rank` | String | League rank of the row's three-point field goals attempted for the season and split. |
| `fg3_pct_rank` | String | League rank of the row's three-point field goal percentage for the season and split. |
| `ftm_rank` | String | League rank of the row's free throws made for the season and split. |
| `fta_rank` | String | League rank of the row's free throws attempted for the season and split. |
| `ft_pct_rank` | String | League rank of the row's free throw percentage for the season and split. |
| `oreb_rank` | String | League rank of the row's offensive rebounds for the season and split. |
| `dreb_rank` | String | League rank of the row's defensive rebounds for the season and split. |
| `reb_rank` | String | League rank of the row's total rebounds for the season and split. |
| `ast_rank` | String | League rank of the row's assists for the season and split. |
| `tov_rank` | String | League rank of the row's turnovers for the season and split. |
| `stl_rank` | String | League rank of the row's steals for the season and split. |
| `blk_rank` | String | League rank of the row's blocked shots for the season and split. |
| `blka_rank` | String | League rank of the row's shot attempts blocked by opponents (blocks against) for the season and split. |
| `pf_rank` | String | League rank of the row's personal fouls committed for the season and split. |
| `pfd_rank` | String | League rank of the row's personal fouls drawn for the season and split. |
| `pts_rank` | String | League rank of the row's points scored for the season and split. |
| `plus_minus_rank` | String | League rank of the row's plus-minus point differential while on the floor for the season and split. |

**ShotDistanceOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotDistanceOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |

**ShotDistanceOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |

**ShotAreaOverall**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `team_id` | Int64 |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `fgm` | Float64 |  |
| `fga` | Float64 |  |
| `fg_pct` | Float64 |  |

**ShotAreaOnCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |

**ShotAreaOffCourt**

| col_name | type | description |
|---|---|---|
| `group_set` | String | Name of the split group the row belongs to (e.g. Overall, By Opponent, By Month). |
| `team_id` | String |  |
| `team_abbreviation` | String |  |
| `team_name` | String |  |
| `vs_player_id` | String |  |
| `vs_player_name` | String |  |
| `court_status` | String |  |
| `group_value` | String | Value of the split within the group (e.g. a specific opponent, month, or result). |
| `fgm` | String |  |
| `fga` | String |  |
| `fg_pct` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamvsplayer-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamvsplayer-example}

```python
nbagl_stats_teamvsplayer(league_id='20', season='2024-25')
```

## nbagl_stats_teamyearbyyearstats

Team year by year stats.

**Endpoint URL:** `GET https://stats.nba.com/stats/teamyearbyyearstats`

**Valid URL:** [https://stats.nba.com/stats/teamyearbyyearstats?LeagueID=20&PerMode=Totals&SeasonType=Regular+Season&TeamID=1612709914](https://stats.nba.com/stats/teamyearbyyearstats?LeagueID=20&PerMode=Totals&SeasonType=Regular+Season&TeamID=1612709914)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `PerMode` | `per_mode_simple` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id` |  |  | `Y` |  |

### Returns {#nbagl_stats_teamyearbyyearstats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 |  |
| `team_city` | String |  |
| `team_name` | String |  |
| `year` | String |  |
| `gp` | Int64 |  |
| `wins` | Int64 |  |
| `losses` | Int64 |  |
| `win_pct` | Float64 |  |
| `conf_rank` | Int64 | Team's final rank within its conference for the season. |
| `div_rank` | Int64 | Team's final rank within its division for the season. |
| `po_wins` | Int64 | Playoff wins recorded by the team that season. |
| `po_losses` | Int64 | Playoff losses recorded by the team that season. |
| `conf_count` | Int64 | Number of teams in the team's conference that season. |
| `div_count` | Int64 | Number of teams in the team's division that season. |
| `nba_finals_appearance` | String | Whether the team reached the league finals that season (e.g. "FINALS APPEARANCE" or "N/A"). |
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
| `pts_rank` | Int64 | League rank of the team's points scored for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_teamyearbyyearstats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_teamyearbyyearstats-example}

```python
nbagl_stats_teamyearbyyearstats(league_id='20')
```
