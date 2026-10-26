---
title: MLS — ESPN CDN API (cdn.espn.com)
sidebar_label: ESPN CDN API (cdn.espn.com)
description: "MLS — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 24
toc_max_heading_level: 2
---
# MLS — ESPN CDN API (cdn.espn.com)

`sportsdataverse.soccer.mls` — 1 endpoint.

## espn_mls_cdn_scoreboard

espn.com scoreboard page data for one day (one week for football), one row per game. The page's sbData block is a Site v2 scoreboard payload.

**Endpoint URL:** `GET https://cdn.espn.com/core/usa.1/scoreboard`

**Valid URL:** [https://cdn.espn.com/core/usa.1/scoreboard?xhr=1&date=20250115](https://cdn.espn.com/core/usa.1/scoreboard?xhr=1&date=20250115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_mls_cdn_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN event id. |
| `uid` | String |  |
| `date` | String | Match start timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | Int64 | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | Int64 | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | String | ESPN slug of the event's season type (e.g. 'regular-season', 'post-season'; soccer competitions use their own, such as 'final' or 'torneo-apertura'). |
| `status_type_id` | Int64 |  |
| `status_type_name` | String |  |
| `status_type_state` | String |  |
| `status_type_completed` | Boolean |  |
| `status_type_description` | String |  |
| `status_type_detail` | String |  |
| `status_type_short_detail` | String |  |
| `status_clock` | Float64 | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | String |  |
| `status_period` | Int64 | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | Boolean | Whether the match is played at a neutral venue. |
| `conference_competition` | Boolean |  |
| `attendance` | Int64 |  |
| `venue_id` | Int64 |  |
| `venue_full_name` | String |  |
| `venue_city` | String |  |
| `venue_state` | String |  |
| `venue_indoor` | Boolean |  |
| `broadcast` | String | Every network name of the competition in feed order, deduped and ", "-joined (an entry with no names gives its market); "" when the game lists no broadcast. |
| `note` | String | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | Int64 |  |
| `home_name` | String |  |
| `home_abbreviation` | String |  |
| `home_display_name` | String |  |
| `home_location` | String |  |
| `home_color` | String |  |
| `home_alternate_color` | String |  |
| `home_logo` | String |  |
| `home_score` | String | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | Boolean |  |
| `home_rank` | String |  |
| `away_id` | Int64 |  |
| `away_name` | String |  |
| `away_abbreviation` | String |  |
| `away_display_name` | String |  |
| `away_location` | String |  |
| `away_color` | String |  |
| `away_alternate_color` | String |  |
| `away_logo` | String |  |
| `away_score` | String | Away team's score. For cricket, the innings string. |
| `away_winner` | Boolean |  |
| `away_rank` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mls_cdn_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mls_cdn_scoreboard-example}

```python
espn_mls_cdn_scoreboard(date='20250115')
```
