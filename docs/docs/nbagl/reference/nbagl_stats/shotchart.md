---
title: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Shot charts"
sidebar_label: "Shot charts"
sidebar_position: 16
description: "NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Shot charts — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA G League — G League Stats API (stats.nba.com, LeagueID=20) — Shot charts

## nbagl_stats_shotchartdetail

Shot chart detail.

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartdetail`

**Valid URL:** [https://stats.nba.com/stats/shotchartdetail?ContextMeasure=FGA&DateFrom=&DateTo=&GameID=&GameSegment=&LastNGames=0&LeagueID=20&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&PlayerID=1642443&PlayerPosition=&RookieYear=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=](https://stats.nba.com/stats/shotchartdetail?ContextMeasure=FGA&DateFrom=&DateTo=&GameID=&GameSegment=&LastNGames=0&LeagueID=20&Location=&Month=0&OpponentTeamID=0&Outcome=&Period=0&PlayerID=1642443&PlayerPosition=&RookieYear=&Season=2024-25&SeasonSegment=&SeasonType=Regular+Season&TeamID=0&VsConference=&VsDivision=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ContextMeasure` | `context_measure_simple` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `PlayerPosition` | `player_position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `Season` | `season_nullable` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_shotchartdetail-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`Shot_Chart_Detail`, `LeagueAverages`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**Shot_Chart_Detail**

| col_name | type | description |
|---|---|---|
| `grid_type` | String | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `game_id` | String |  |
| `game_event_id` | String |  |
| `player_id` | String |  |
| `player_name` | String |  |
| `team_id` | String |  |
| `team_name` | String |  |
| `period` | String |  |
| `minutes_remaining` | String |  |
| `seconds_remaining` | String |  |
| `event_type` | String |  |
| `action_type` | String |  |
| `shot_type` | String |  |
| `shot_zone_basic` | String |  |
| `shot_zone_area` | String |  |
| `shot_zone_range` | String |  |
| `shot_distance` | String |  |
| `loc_x` | String |  |
| `loc_y` | String |  |
| `shot_attempted_flag` | String |  |
| `shot_made_flag` | String |  |
| `game_date` | String |  |
| `htm` | String | Home team abbreviation for the game. |
| `vtm` | String | Visiting team abbreviation for the game. |

**LeagueAverages**

| col_name | type | description |
|---|---|---|
| `grid_type` | String | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `shot_zone_basic` | String |  |
| `shot_zone_area` | String |  |
| `shot_zone_range` | String |  |
| `fga` | Int64 |  |
| `fgm` | Int64 |  |
| `fg_pct` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_shotchartdetail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_shotchartdetail-example}

```python
nbagl_stats_shotchartdetail(league_id='20', season_nullable='2024-25')
```

## nbagl_stats_shotchartleaguewide

Shot chart league wide.

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartleaguewide`

**Valid URL:** [https://stats.nba.com/stats/shotchartleaguewide?LeagueID=20&Season=2024-25](https://stats.nba.com/stats/shotchartleaguewide?LeagueID=20&Season=2024-25)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |

### Returns {#nbagl_stats_shotchartleaguewide-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `grid_type` | String | NBA or WNBA Stats value for grid type in the shotchartleaguewide result set. |
| `shot_zone_basic` | String |  |
| `shot_zone_area` | String |  |
| `shot_zone_range` | String |  |
| `fga` | Int64 |  |
| `fgm` | Int64 |  |
| `fg_pct` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_shotchartleaguewide-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_shotchartleaguewide-example}

```python
nbagl_stats_shotchartleaguewide(league_id='20', season='2024-25')
```

## nbagl_stats_shotchartlineupdetail

Shot chart lineup detail.

**Endpoint URL:** `GET https://stats.nba.com/stats/shotchartlineupdetail`

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `GROUP_ID` | `group_id` |  | `Y` |  | GROUP_ID: required; there is no G League default for it. |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `LastNGames` | `last_n_games_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `Month` | `month_nullable` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. A G League regular season starts in late December and becomes the default in January (``2025-26`` from January 2026); a draft combine (May) in June. With season type ``Playoffs`` (or ``commonplayoffseries``) it rolls over in May, after the late-March playoffs start; the G League has no All-Star rows. A month table cannot follow a lockout or pandemic calendar: pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season`` or ``Playoffs`` (each endpoint takes a subset; the G League has no ``All Star`` rows). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` rolls over in May. |
| `TeamID` | `team_id_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |

### Returns {#nbagl_stats_shotchartlineupdetail-returns}

**`return_parsed=True`** (default) — A dict of DataFrames keyed by result-set name (`ShotChartLineupDetail`, `ShotChartLineupLeagueAverage`) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**ShotChartLineupDetail**

| col_name | type | description |
|---|---|---|
| `grid_type` | String | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `game_id` | String |  |
| `game_event_id` | String |  |
| `group_id` | String |  |
| `group_name` | String | The lineup's five players as a ' - ' separated string of abbreviated names. |
| `player_id` | String |  |
| `player_name` | String |  |
| `team_id` | String |  |
| `team_name` | String |  |
| `period` | String |  |
| `minutes_remaining` | String |  |
| `seconds_remaining` | String |  |
| `event_type` | String |  |
| `action_type` | String |  |
| `shot_type` | String |  |
| `shot_zone_basic` | String |  |
| `shot_zone_area` | String |  |
| `shot_zone_range` | String |  |
| `shot_distance` | String |  |
| `loc_x` | String |  |
| `loc_y` | String |  |
| `shot_attempted_flag` | String |  |
| `shot_made_flag` | String |  |
| `game_date` | String |  |
| `htm` | String | Home team abbreviation for the game. |
| `vtm` | String | Visiting team abbreviation for the game. |

**ShotChartLineupLeagueAverage**

| col_name | type | description |
|---|---|---|
| `grid_type` | String | Shot chart grid type label returned by the stats API (e.g. "Shot Chart Detail"). |
| `shot_zone_basic` | String |  |
| `shot_zone_area` | String |  |
| `shot_zone_range` | String |  |
| `fga` | Int64 |  |
| `fgm` | Int64 |  |
| `fg_pct` | Float64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nbagl_stats_shotchartlineupdetail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nbagl_stats_shotchartlineupdetail-example}

```python
nbagl_stats_shotchartlineupdetail(league_id='20', season='2024-25')
```
