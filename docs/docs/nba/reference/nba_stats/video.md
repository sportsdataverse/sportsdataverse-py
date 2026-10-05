---
title: "NBA — NBA Stats API (stats.nba.com) — Video"
sidebar_label: "Video"
sidebar_position: 29
description: "NBA — NBA Stats API (stats.nba.com) — Video — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — NBA Stats API (stats.nba.com) — Video

## nba_stats_videodetailsasset

GET /stats/videodetailsasset

**Endpoint URL:** `GET https://stats.nba.com/stats/videodetailsasset`

**Valid URL:** [https://stats.nba.com/stats/videodetailsasset?ContextMeasure=FGA&LastNGames=0&Month=0&OpponentTeamID=0&Period=0&PlayerID=2544&Season=2024-25&SeasonType=Regular+Season&TeamID=1610612747&VsDivision=&VsConference=&StartRange=&StartPeriod=&SeasonSegment=&RookieYear=&RangeType=&Position=&PointDiff=&Outcome=&Location=&LeagueID=00&GameSegment=&GameID=&EndRange=&EndPeriod=&DateTo=&DateFrom=&ContextFilter=&ClutchTime=&AheadBehind=](https://stats.nba.com/stats/videodetailsasset?ContextMeasure=FGA&LastNGames=0&Month=0&OpponentTeamID=0&Period=0&PlayerID=2544&Season=2024-25&SeasonType=Regular+Season&TeamID=1610612747&VsDivision=&VsConference=&StartRange=&StartPeriod=&SeasonSegment=&RookieYear=&RangeType=&Position=&PointDiff=&Outcome=&Location=&LeagueID=00&GameSegment=&GameID=&EndRange=&EndPeriod=&DateTo=&DateFrom=&ContextFilter=&ClutchTime=&AheadBehind=)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `ContextMeasure` | `context_measure_detailed` |  |  | `Y` |  |
| `LastNGames` | `last_n_games` |  |  | `Y` |  |
| `Month` | `month` |  |  | `Y` |  |
| `OpponentTeamID` | `opponent_team_id` |  |  | `Y` |  |
| `Period` | `period` |  |  | `Y` |  |
| `PlayerID` | `player_id` |  |  | `Y` |  |
| `Season` | `season` |  |  | `Y` | Season label, e.g. ``2024-25``. Defaults at call time by a calendar cutoff, the month after a season's first games, so it can lag the newest rows by a few weeks. An NBA season tips off in late October and becomes the default in November (``2025-26`` through October 2026, ``2026-27`` from November 2026); a G League season (regular season from late December) in January; a Summer League (played in July, which stats.nba.com labels ``2026-27`` in 2026) in August; a draft combine (May) in June; a draft (``drafthistory``, a year; late June) in July. With season type ``Playoffs`` / ``PlayIn`` (or ``commonplayoffseries``) the NBA and the G League roll over in May, after their playoffs start; with ``All Star`` the NBA rolls over in March, after the February game (the G League has no All-Star rows and keeps its own rule). A month table cannot follow a lockout or pandemic calendar (1998-99, 2011-12, 2020-21): pass a season then. Without one stats.nba.com answers an empty HTTP 500 or every season summed. |
| `SeasonType` | `season_type_all_star` |  |  | `Y` | Season type, a label: ``Regular Season``, ``Pre Season``, ``Playoffs``, ``PlayIn`` or ``All Star`` (each endpoint takes a subset). Not ESPN's numeric code: ``3`` is HTTP 400. A default season follows it: ``Playoffs`` / ``PlayIn`` roll over in May (NBA, G League), ``All Star`` in March (NBA). |
| `TeamID` | `team_id` |  |  | `Y` |  |
| `VsDivision` | `vs_division_nullable` |  |  | `Y` |  |
| `VsConference` | `vs_conference_nullable` |  |  | `Y` |  |
| `StartRange` | `start_range_nullable` |  |  | `Y` |  |
| `StartPeriod` | `start_period_nullable` |  |  | `Y` |  |
| `SeasonSegment` | `season_segment_nullable` |  |  | `Y` |  |
| `RookieYear` | `rookie_year_nullable` |  |  | `Y` |  |
| `RangeType` | `range_type_nullable` |  |  | `Y` |  |
| `Position` | `position_nullable` |  |  | `Y` |  |
| `PointDiff` | `point_diff_nullable` |  |  | `Y` |  |
| `Outcome` | `outcome_nullable` |  |  | `Y` |  |
| `Location` | `location_nullable` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |
| `GameSegment` | `game_segment_nullable` |  |  | `Y` |  |
| `GameID` | `game_id_nullable` |  |  | `Y` |  |
| `EndRange` | `end_range_nullable` |  |  | `Y` |  |
| `EndPeriod` | `end_period_nullable` |  |  | `Y` |  |
| `DateTo` | `date_to_nullable` |  |  | `Y` |  |
| `DateFrom` | `date_from_nullable` |  |  | `Y` |  |
| `ContextFilter` | `context_filter_nullable` |  |  | `Y` |  |
| `ClutchTime` | `clutch_time_nullable` |  |  | `Y` |  |
| `AheadBehind` | `ahead_behind_nullable` |  |  | `Y` |  |

### Returns {#nba_stats_videodetailsasset-returns}

**`return_parsed=True`** (default) — A dict of two DataFrames keyed `videoUrls` (clip URLs, durations, thumbnails) and `playlist` (per-event game metadata) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**videoUrls**

| col_name | type | description |
|---|---|---|
| `uuid` | character | Uuid. |
| `sdur` | integer | Sdur. |
| `surl` | character | Surl. |
| `sth` | character | Sth. |
| `mdur` | integer | Mdur. |
| `murl` | character | Murl. |
| `mth` | character | Mth. |
| `ldur` | integer | Ldur. |
| `lurl` | character | Lurl. |
| `lth` | character | Lth. |
| `vtt` | character | Vtt. |
| `scc` | character | Scc. |
| `srt` | character | Srt. |

**playlist**

| col_name | type | description |
|---|---|---|
| `gi` | character | Gi. |
| `ei` | integer | Ei. |
| `y` | integer | Y. |
| `m` | character | M. |
| `d` | character | D. |
| `gc` | character | Gc. |
| `p` | integer | P. |
| `dsc` | character | Dsc. |
| `ha` | character | Ha. |
| `hid` | integer | Hid. |
| `va` | character | Va. |
| `vid` | integer | Vid. |
| `hpb` | integer | Hpb. |
| `hpa` | integer | Hpa. |
| `vpb` | integer | Vpb. |
| `vpa` | integer | Vpa. |
| `pta` | integer | Pta. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videodetailsasset-example}

```python
nba_stats_videodetailsasset(league_id='00', season='2024-25')
```

_Last validated n/a._

## nba_stats_videoevents

GET /stats/videoevents

**Endpoint URL:** `GET https://stats.nba.com/stats/videoevents`

**Valid URL:** [https://stats.nba.com/stats/videoevents?GameEventID=10&GameID=0021700807](https://stats.nba.com/stats/videoevents?GameEventID=10&GameID=0021700807)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameEventID` | `game_event_id` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_videoevents-returns}

**`return_parsed=True`** (default) — A dict of two DataFrames keyed `videoUrls` (clip URLs, durations, thumbnails) and `playlist` (per-event game metadata) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**videoUrls**

| col_name | type | description |
|---|---|---|
| `uuid` | character | Uuid. |
| `dur` | character | Dur. |
| `stt` | character | Stt. |
| `stp` | character | Stp. |
| `sth` | character | Sth. |
| `stw` | character | Stw. |
| `mtt` | character | Mtt. |
| `mtp` | character | Mtp. |
| `mth` | character | Mth. |
| `mtw` | character | Mtw. |
| `ltt` | character | Ltt. |
| `ltp` | character | Ltp. |
| `lth` | character | Lth. |
| `ltw` | character | Ltw. |

**playlist**

| col_name | type | description |
|---|---|---|
| `gi` | character | Gi. |
| `ei` | integer | Ei. |
| `y` | integer | Y. |
| `m` | character | M. |
| `d` | character | D. |
| `gc` | character | Gc. |
| `p` | integer | P. |
| `dsc` | character | Dsc. |
| `ha` | character | Ha. |
| `va` | character | Va. |
| `hpb` | integer | Hpb. |
| `hpa` | integer | Hpa. |
| `vpb` | integer | Vpb. |
| `vpa` | integer | Vpa. |
| `pta` | integer | Pta. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videoevents-example}

```python
nba_stats_videoevents()
```

_Last validated n/a._

## nba_stats_videoeventsasset

GET /stats/videoeventsasset

**Endpoint URL:** `GET https://stats.nba.com/stats/videoeventsasset`

**Valid URL:** [https://stats.nba.com/stats/videoeventsasset?GameEventID=0&GameID=0021700807](https://stats.nba.com/stats/videoeventsasset?GameEventID=0&GameID=0021700807)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameEventID` | `game_event_id` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |

### Returns {#nba_stats_videoeventsasset-returns}

**`return_parsed=True`** (default) — A dict of two DataFrames keyed `videoUrls` (clip URLs, durations, thumbnails) and `playlist` (per-event game metadata) (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**videoUrls**

| col_name | type | description |
|---|---|---|
| `uuid` | character | Uuid. |
| `sdur` | integer | Sdur. |
| `surl` | character | Surl. |
| `sth` | character | Sth. |
| `mdur` | integer | Mdur. |
| `murl` | character | Murl. |
| `mth` | character | Mth. |
| `ldur` | integer | Ldur. |
| `lurl` | character | Lurl. |
| `lth` | character | Lth. |
| `vtt` | character | Vtt. |
| `scc` | character | Scc. |
| `srt` | character | Srt. |

**playlist**

| col_name | type | description |
|---|---|---|
| `gi` | character | Gi. |
| `ei` | integer | Ei. |
| `y` | integer | Y. |
| `m` | character | M. |
| `d` | character | D. |
| `gc` | character | Gc. |
| `p` | integer | P. |
| `dsc` | character | Dsc. |
| `ha` | character | Ha. |
| `hid` | integer | Hid. |
| `va` | character | Va. |
| `vid` | integer | Vid. |
| `hpb` | integer | Hpb. |
| `hpa` | integer | Hpa. |
| `vpb` | integer | Vpb. |
| `vpa` | integer | Vpa. |
| `pta` | integer | Pta. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videoeventsasset-example}

```python
nba_stats_videoeventsasset()
```

_Last validated n/a._

## nba_stats_videostatus

GET /stats/videostatus

**Endpoint URL:** `GET https://stats.nba.com/stats/videostatus`

**Valid URL:** [https://stats.nba.com/stats/videostatus?GameDate=2023-03-10&LeagueID=00](https://stats.nba.com/stats/videostatus?GameDate=2023-03-10&LeagueID=00)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#nba_stats_videostatus-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | Unique game identifier. |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `visitor_team_id` | integer | Unique identifier for visitor team. |
| `visitor_team_city` | character | City name of the visiting team. |
| `visitor_team_name` | character | Nickname of the visiting team. |
| `visitor_team_abbreviation` | character | Abbreviation of the visiting team. |
| `home_team_id` | integer | Unique identifier for the home team. |
| `home_team_city` | character | Home team city / location. |
| `home_team_name` | character | Home team name. |
| `home_team_abbreviation` | character | Home team abbreviation. |
| `game_status` | integer | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | integer | Flag indicating whether game video is available in the league's stats video system. |
| `pt_xyz_available` | integer | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nba_stats_videostatus-example}

```python
nba_stats_videostatus(league_id='00')
```

_Last validated n/a._
