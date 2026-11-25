---
title: Golf — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
description: "Golf — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 20
toc_max_heading_level: 2
---
# Golf — ESPN site API (v2)

`sportsdataverse.golf` — 4 endpoints.

## espn_golf_scoreboard

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard?dates=20240115&limit=500](https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard?dates=20240115&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | One day (YYYYMMDD) or one calendar year (YYYY). A range (YYYYMMDD-YYYYMMDD) is rejected with HTTP 400; request each day, or use the Core v2 ``events`` wrapper. |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_id` | String | ESPN event id for the match. |
| `event_name` | String | Tournament name (e.g. "Masters Tournament"). |
| `event_date` | String | Tournament start date, ISO 8601 UTC as ESPN sends it. |
| `event_status` | String | Tournament status name (e.g. "STATUS_FINAL", "STATUS_IN_PROGRESS", "STATUS_SCHEDULED"). |
| `player_id` | String | ESPN golfer (athlete) id, as a string. |
| `player_name` | String | Golfer's display name. |
| `player_short_name` | String | Golfer's short name (e.g. "R. McIlroy"). |
| `country` | String | Country the golfer represents, from the flag ESPN shows beside the name. |
| `amateur` | Boolean | True for an amateur in the field. Null on the scoreboard, which does not carry it. |
| `sort_order` | Int64 | Leaderboard order (1 = leader). Players tied on score keep distinct values here. |
| `position` | String | Displayed position, with a "T" prefix for a tie (e.g. "1", "T3"). Null for a player off the board (missed cut, withdrawn) and on the scoreboard. |
| `position_is_tie` | Boolean | True when the position is shared. Null for a player off the board and on the scoreboard. |
| `player_status` | String | Player status name (e.g. "STATUS_FINISH", "STATUS_CUT", "STATUS_WD", "STATUS_IN_PROGRESS"). Null on the scoreboard. |
| `thru` | Int64 | Hole the player last completed in the current round (18 when the round is done). Null on the scoreboard. |
| `to_par` | Int64 | Total score relative to par as an integer ("E" is 0, "-12" is -12); null when no score is posted. |
| `to_par_display` | String | Total score relative to par as ESPN displays it (e.g. "-12", "E", "+3"). |
| `total_strokes` | Int64 | Total strokes over the rounds played. On the scoreboard it is summed from round_1 to round_4. |
| `round_1` | Int64 | Strokes in round 1; null when the round was not played. A round in progress holds the strokes so far (see thru); a playoff is never a round. |
| `round_2` | Int64 | Strokes in round 2; null when the round was not played. |
| `round_3` | Int64 | Strokes in round 3; null when the round was not played (a missed cut, or not started). |
| `round_4` | Int64 | Strokes in round 4; null when the round was not played (a missed cut, a 54-hole event, or not started). |
| `round_1_to_par` | String | Round 1 score relative to par as ESPN displays it (e.g. "-5"); null when the round was not played. |
| `round_2_to_par` | String | Round 2 score relative to par as ESPN displays it; null when the round was not played. |
| `round_3_to_par` | String | Round 3 score relative to par as ESPN displays it; null when the round was not played. |
| `round_4_to_par` | String | Round 4 score relative to par as ESPN displays it; null when the round was not played. |
| `earnings` | Float64 | Prize money won, in the tour's currency (0.0 for a missed cut). Null on the scoreboard. |
| `cup_points` | Float64 | Season-long points earned at the event (FedExCup on the PGA Tour, the Race to the CME Globe on the LPGA). Null on the scoreboard. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_scoreboard-example}

```python
espn_golf_scoreboard(league='pga', dates='20240115')
```

## espn_golf_leaderboard

Tournament leaderboard: the full field of one golf event (the current one when `event_id` is omitted), one row per player.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=pga&event=401811941](https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=pga&event=401811941)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | ESPN event (tournament) id; omitted, the tour's current or most recent event. |

### Returns {#espn_golf_leaderboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_id` | String | ESPN event (tournament) id, as a string. |
| `event_name` | String | Tournament name (e.g. "Masters Tournament"). |
| `event_date` | String | Tournament start date, ISO 8601 UTC as ESPN sends it. |
| `event_status` | String | Tournament status name (e.g. "STATUS_FINAL", "STATUS_IN_PROGRESS", "STATUS_SCHEDULED"). |
| `player_id` | String | ESPN golfer (athlete) id, as a string. |
| `player_name` | String | Golfer's display name. |
| `player_short_name` | String | Golfer's short name (e.g. "R. McIlroy"). |
| `country` | String | Country the golfer represents, from the flag ESPN shows beside the name. |
| `amateur` | Boolean | True for an amateur in the field. Null on the scoreboard, which does not carry it. |
| `sort_order` | Int64 | Leaderboard order (1 = leader). Players tied on score keep distinct values here. |
| `position` | String | Displayed position, with a "T" prefix for a tie (e.g. "1", "T3"). Null for a player off the board (missed cut, withdrawn) and on the scoreboard. |
| `position_is_tie` | Boolean | True when the position is shared. Null for a player off the board and on the scoreboard. |
| `player_status` | String | Player status name (e.g. "STATUS_FINISH", "STATUS_CUT", "STATUS_WD", "STATUS_IN_PROGRESS"). Null on the scoreboard. |
| `thru` | Int64 | Hole the player last completed in the current round (18 when the round is done). Null on the scoreboard. |
| `to_par` | Int64 | Total score relative to par as an integer ("E" is 0, "-12" is -12); null when no score is posted. |
| `to_par_display` | String | Total score relative to par as ESPN displays it (e.g. "-12", "E", "+3"). |
| `total_strokes` | Int64 | Total strokes over the rounds played. On the scoreboard it is summed from round_1 to round_4. |
| `round_1` | Int64 | Strokes in round 1; null when the round was not played. A round in progress holds the strokes so far (see thru); a playoff is never a round. |
| `round_2` | Int64 | Strokes in round 2; null when the round was not played. |
| `round_3` | Int64 | Strokes in round 3; null when the round was not played (a missed cut, or not started). |
| `round_4` | Int64 | Strokes in round 4; null when the round was not played (a missed cut, a 54-hole event, or not started). |
| `round_1_to_par` | String | Round 1 score relative to par as ESPN displays it (e.g. "-5"); null when the round was not played. |
| `round_2_to_par` | String | Round 2 score relative to par as ESPN displays it; null when the round was not played. |
| `round_3_to_par` | String | Round 3 score relative to par as ESPN displays it; null when the round was not played. |
| `round_4_to_par` | String | Round 4 score relative to par as ESPN displays it; null when the round was not played. |
| `earnings` | Float64 | Prize money won, in the tour's currency (0.0 for a missed cut). Null on the scoreboard. |
| `cup_points` | Float64 | Season-long points earned at the event (FedExCup on the PGA Tour, the Race to the CME Globe on the LPGA). Null on the scoreboard. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_leaderboard-example}

```python
espn_golf_leaderboard(league='pga', event_id='401811941')
```

## espn_golf_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/pga/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/golf/pga/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the article. |
| `now_id` | String | ESPN 'now' feed id. |
| `content_key` | String | Internal content key. |
| `data_source_identifier` | String | Source-system identifier. |
| `type` | String | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | String | Article headline. |
| `description` | String | Article summary/description. |
| `last_modified` | String | Last-modified timestamp (ISO 8601). |
| `published` | String | Publish timestamp (ISO 8601). |
| `images` | String | Article images (list, stringified). |
| `categories` | String | Article categories (list, stringified). |
| `premium` | Boolean | Whether the article is premium/paywalled. |
| `byline` | String | Author byline string as published by ESPN. |
| `links_web_href` | String | Web article URL. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_news-example}

```python
espn_golf_news(league='pga')
```

## espn_golf_statistics_league

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/pga/statistics](https://site.api.espn.com/apis/site/v2/sports/golf/pga/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_golf_statistics_league-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: league, season, status, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_statistics_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_statistics_league-example}

```python
espn_golf_statistics_league(league='pga')
```
