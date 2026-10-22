---
title: "WNBA — WNBA Stats API (stats.wnba.com) — Other: playbyplayv3–videostatus"
sidebar_label: "Other: playbyplayv3–videostatus"
sidebar_position: 28
description: "WNBA — WNBA Stats API (stats.wnba.com) — Other: playbyplayv3–videostatus — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WNBA — WNBA Stats API (stats.wnba.com) — Other: playbyplayv3–videostatus

## wnba_stats_playbyplayv3

GET /stats/playbyplayv3

**Endpoint URL:** `GET https://stats.wnba.com/stats/playbyplayv3`

**Valid URL:** [https://stats.wnba.com/stats/playbyplayv3?EndPeriod=0&GameID=1022200034&StartPeriod=0](https://stats.wnba.com/stats/playbyplayv3?EndPeriod=0&GameID=1022200034&StartPeriod=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `EndPeriod` | `end_period` |  |  | `Y` |  |
| `GameID` | `game_id` |  |  | `Y` |  |
| `StartPeriod` | `start_period` |  |  | `Y` |  |

### Returns {#wnba_stats_playbyplayv3-returns}

**`return_parsed=True`** (default) — the output of `parse_wnba_stats_result_sets`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: the committed capture tests/fixtures/wnba_stats/endpoints/playbyplayv3.json holds no rows, so parse_nba_stats_result_sets emits no columns; a capture with rows needs a residential IP (stats.nba.com / stats.wnba.com hang on datacenter IPs) and SDV_PY_NBA_STATS_LIVE=1.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_playbyplayv3-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_playbyplayv3-example}

```python
wnba_stats_playbyplayv3()
```

_Last validated n/a._

## wnba_stats_videostatus

GET /stats/videostatus

**Endpoint URL:** `GET https://stats.wnba.com/stats/videostatus`

**Valid URL:** [https://stats.wnba.com/stats/videostatus?GameDate=2022-06-10&LeagueID=10](https://stats.wnba.com/stats/videostatus?GameDate=2022-06-10&LeagueID=10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `GameDate` | `game_date` |  |  | `Y` |  |
| `LeagueID` | `league_id` |  |  | `Y` |  |

### Returns {#wnba_stats_videostatus-returns}

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
| `home_team_abbreviation` | character | Home team's three-letter tricode (e.g. 'PHI'). |
| `game_status` | integer | Game status label. |
| `game_status_text` | character | Game status display text (e.g. 'Final', '4:32 - 4th'). |
| `is_available` | integer | Flag indicating whether game video is available in the league's stats video system. |
| `pt_xyz_available` | integer | Pt xyz available. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#wnba_stats_videostatus-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |
| `ImportError` | `curl_cffi` is not installed -- stats.nba.com / stats.wnba.com block plain `requests`, so the transport needs it (`pip install curl_cffi` or `sportsdataverse[all]`). |

### Example {#wnba_stats_videostatus-example}

```python
wnba_stats_videostatus(league_id='10')
```

_Last validated n/a._
