# NBA — NBA Stats API (stats.nba.com) — Video

> NBA — NBA Stats API (stats.nba.com) — Video — function reference in sdv-py, the SportsDataverse Python package.

## nba_stats_videodetailsasset

Video details asset.

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
| `uuid` | String | Uuid. |
| `sdur` | Int64 | Sdur. |
| `surl` | String | Surl. |
| `sth` | String | Sth. |
| `mdur` | Int64 | Mdur. |
| `murl` | String | Murl. |
| `mth` | String | Mth. |
| `ldur` | Int64 | Ldur. |
| `lurl` | String | Lurl. |
| `lth` | String | Lth. |
| `vtt` | String | Vtt. |
| `scc` | String | Scc. |
| `srt` | String | Srt. |

**playlist**

| col_name | type | description |
|---|---|---|
| `gi` | String | Gi. |
| `ei` | Int64 | Ei. |
| `y` | Int64 | Y. |
| `m` | String | M. |
| `d` | String | D. |
| `gc` | String | Gc. |
| `p` | Int64 | P. |
| `dsc` | String | Dsc. |
| `ha` | String | Ha. |
| `hid` | Int64 | Hid. |
| `va` | String | Va. |
| `vid` | Int64 | Vid. |
| `hpb` | Int64 | Hpb. |
| `hpa` | Int64 | Hpa. |
| `vpb` | Int64 | Vpb. |
| `vpa` | Int64 | Vpa. |
| `pta` | Int64 | Pta. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_videodetailsasset-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_videodetailsasset-example}

```python
nba_stats_videodetailsasset(league_id='00', season='2024-25')
```

## nba_stats_videoevents

Video events.

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
| `uuid` | String | Uuid. |
| `dur` | String | Dur. |
| `stt` | String | Stt. |
| `stp` | String | Stp. |
| `sth` | String | Sth. |
| `stw` | String | Stw. |
| `mtt` | String | Mtt. |
| `mtp` | String | Mtp. |
| `mth` | String | Mth. |
| `mtw` | String | Mtw. |
| `ltt` | String | Ltt. |
| `ltp` | String | Ltp. |
| `lth` | String | Lth. |
| `ltw` | String | Ltw. |

**playlist**

| col_name | type | description |
|---|---|---|
| `gi` | String | Gi. |
| `ei` | Int64 | Ei. |
| `y` | Int64 | Y. |
| `m` | String | M. |
| `d` | String | D. |
| `gc` | String | Gc. |
| `p` | Int64 | P. |
| `dsc` | String | Dsc. |
| `ha` | String | Ha. |
| `va` | String | Va. |
| `hpb` | Int64 | Hpb. |
| `hpa` | Int64 | Hpa. |
| `vpb` | Int64 | Vpb. |
| `vpa` | Int64 | Vpa. |
| `pta` | Int64 | Pta. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_videoevents-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_videoevents-example}

```python
nba_stats_videoevents()
```

## nba_stats_videoeventsasset

Video events asset.

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
| `uuid` | String | Uuid. |
| `sdur` | Int64 | Sdur. |
| `surl` | String | Surl. |
| `sth` | String | Sth. |
| `mdur` | Int64 | Mdur. |
| `murl` | String | Murl. |
| `mth` | String | Mth. |
| `ldur` | Int64 | Ldur. |
| `lurl` | String | Lurl. |
| `lth` | String | Lth. |
| `vtt` | String | Vtt. |
| `scc` | String | Scc. |
| `srt` | String | Srt. |

**playlist**

| col_name | type | description |
|---|---|---|
| `gi` | String | Gi. |
| `ei` | Int64 | Ei. |
| `y` | Int64 | Y. |
| `m` | String | M. |
| `d` | String | D. |
| `gc` | String | Gc. |
| `p` | Int64 | P. |
| `dsc` | String | Dsc. |
| `ha` | String | Ha. |
| `hid` | Int64 | Hid. |
| `va` | String | Va. |
| `vid` | Int64 | Vid. |
| `hpb` | Int64 | Hpb. |
| `hpa` | Int64 | Hpa. |
| `vpb` | Int64 | Vpb. |
| `vpa` | Int64 | Vpa. |
| `pta` | Int64 | Pta. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_videoeventsasset-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_videoeventsasset-example}

```python
nba_stats_videoeventsasset()
```

## nba_stats_videostatus

Video status.

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
| `game_id` | String | Unique game identifier. |
| `game_date` | String | Game date (YYYY-MM-DD). |
| `visitor_team_id` | Int64 | Unique identifier for visitor team. |
| `visitor_team_city` | String | City name of the visiting team. |
| `visitor_team_name` | String | Nickname of the visiting team. |
| `visitor_team_abbreviation` | String | Abbreviation of the visiting team. |
| `home_team_id` | Int64 | Unique identifier for the home team. |
| `home_team_city` | String | Home team city / location. |
| `home_team_name` | String | Home team name. |
| `home_team_abbreviation` | String | Home team's three-letter tricode (e.g. 'PHI'). |
| `game_status` | Int64 | Game status label. |
| `game_status_text` | String | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | Int64 | Flag indicating whether game video is available in the league's stats video system. |
| `pt_xyz_available` | Int64 | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nba_stats_videostatus-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#nba_stats_videostatus-example}

```python
nba_stats_videostatus(league_id='00')
```
