---
title: Golf — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
description: "Golf — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 20
toc_max_heading_level: 2
---
# Golf — ESPN site API (v2)

`sportsdataverse.golf` — 9 endpoints.

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
| `player_status` | String | Player status name (e.g. "STATUS_FINISH", "STATUS_CUT", "STATUS_IN_PROGRESS"). ESPN files a withdrawal and a disqualification under "STATUS_CUT" too; player_status_abbr separates them. Null on the scoreboard. |
| `player_status_abbr` | String | Player status as ESPN abbreviates it ("F" finished, "CUT" missed cut, "WD" withdrawn, "DQ" disqualified). Null on the scoreboard. |
| `playoff` | Boolean | True for a player who was in the event's playoff, False otherwise. Null on the scoreboard. |
| `thru` | Int64 | Holes completed in the current round (18 when the round is done), whatever hole the round started on. 18 for a playoff player (playoff holes are not a round). Null on the scoreboard. |
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
| `player_status` | String | Player status name (e.g. "STATUS_FINISH", "STATUS_CUT", "STATUS_IN_PROGRESS"). ESPN files a withdrawal and a disqualification under "STATUS_CUT" too; player_status_abbr separates them. Null on the scoreboard. |
| `player_status_abbr` | String | Player status as ESPN abbreviates it ("F" finished, "CUT" missed cut, "WD" withdrawn, "DQ" disqualified). Null on the scoreboard. |
| `playoff` | Boolean | True for a player who was in the event's playoff, False otherwise. Null on the scoreboard. |
| `thru` | Int64 | Holes completed in the current round (18 when the round is done), whatever hole the round started on. 18 for a playoff player (playoff holes are not a round). Null on the scoreboard. |
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

## espn_golf_conferences

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/tgl/groups](https://site.api.espn.com/apis/site/v2/sports/golf/tgl/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_golf_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | Int64 |  |
| `name` | String |  |
| `abbreviation` | String |  |
| `short_name` | String |  |
| `is_conference` | Boolean |  |
| `parent_group_id` | Int64 |  |
| `depth` | Int64 |  |
| `children_count` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_conferences-example}

```python
espn_golf_conferences(league='tgl')
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

## espn_golf_teams_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/tgl/teams?limit=1000](https://site.api.espn.com/apis/site/v2/sports/golf/tgl/teams?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_golf_teams_site-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | String | Secondary team color as a hex string (no leading '#'). |
| `team_color` | String | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | String | Full team display name (location + nickname). |
| `team_id` | Int64 | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | Boolean | Whether the team is currently active. |
| `team_is_all_star` | Boolean | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | String | Team location / city (e.g. "Boston"). |
| `team_logos` | String | Pipe-delimited logo image URLs. |
| `team_name` | String | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | String | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | String | Abbreviated display name for compact UIs. |
| `team_slug` | String | URL slug used in ESPN web paths. |
| `team_uid` | String | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_teams_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_teams_site-example}

```python
espn_golf_teams_site(league='tgl')
```

## espn_golf_team

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/pga/teams/4](https://site.api.espn.com/apis/site/v2/sports/golf/pga/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_golf_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String |  |
| `team_alternate_color` | String |  |
| `team_color` | String |  |
| `team_display_name` | String |  |
| `team_franchise_$ref` | String |  |
| `team_franchise_abbreviation` | String |  |
| `team_franchise_color` | String |  |
| `team_franchise_display_name` | String |  |
| `team_franchise_id` | Int64 |  |
| `team_franchise_is_active` | Boolean |  |
| `team_franchise_location` | String |  |
| `team_franchise_name` | String |  |
| `team_franchise_short_display_name` | String |  |
| `team_franchise_slug` | String |  |
| `team_franchise_team_$ref` | String |  |
| `team_franchise_uid` | String |  |
| `team_franchise_venue_$ref` | String |  |
| `team_franchise_venue_address_city` | String |  |
| `team_franchise_venue_address_state` | String |  |
| `team_franchise_venue_full_name` | String |  |
| `team_franchise_venue_grass` | Boolean |  |
| `team_franchise_venue_guid` | String |  |
| `team_franchise_venue_id` | Int64 |  |
| `team_franchise_venue_images` | String |  |
| `team_franchise_venue_indoor` | Boolean |  |
| `team_franchise_venue_short_name` | String |  |
| `team_groups_id` | Int64 |  |
| `team_groups_is_conference` | Boolean |  |
| `team_groups_parent_id` | Int64 |  |
| `team_id` | Int64 |  |
| `team_is_active` | Boolean |  |
| `team_links` | String |  |
| `team_location` | String |  |
| `team_logos` | String |  |
| `team_name` | String |  |
| `team_next_event` | String |  |
| `team_record_items` | String |  |
| `team_short_display_name` | String |  |
| `team_slug` | String |  |
| `team_standing_summary` | String |  |
| `team_uid` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_team-example}

```python
espn_golf_team(league='pga', team_id='4')
```

## espn_golf_team_schedule

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/golf/pga/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/golf/tgl/teams/130349/schedule](https://site.api.espn.com/apis/site/v2/sports/golf/tgl/teams/130349/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_golf_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric event identifier. |
| `date` | String | Event timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `time_valid` | Boolean | Whether the event time is confirmed. |
| `competitions` | String | Competition detail (list of dicts, stringified): competitors, venue, status. |
| `links` | String | Related links (list, stringified). |
| `season_year` | Int64 | Four-digit season year. |
| `season_display_name` | String | Human-readable season label (e.g. '2024-25'). |
| `season_type_id` | Int64 | ESPN numeric identifier for the season type. |
| `season_type_type` | Int64 | Season type numeric code. |
| `season_type_name` | String | Season type name (e.g. Regular Season). |
| `season_type_abbreviation` | String | Season type abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_team_schedule-example}

```python
espn_golf_team_schedule(league='tgl', team_id='130349')
```

## espn_golf_standings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/golf/pga/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/golf/tgl/standings](https://site.api.espn.com/apis/v2/sports/golf/tgl/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns {#espn_golf_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | String | Group name. |
| `group_abbreviation` | String | Group abbreviation. |
| `team_id` | Int64 | Team id. |
| `team_name` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_location` | String | Team location. |
| `team_logo` | String | Team logo. |
| `avg_points_against` | Float64 | Avg points against. |
| `avg_points_for` | Float64 | Avg points for. |
| `clincher` | Float64 | Clincher. |
| `differential` | Float64 | Differential. |
| `division_win_percent` | Float64 | Division win percent. |
| `games_behind` | Float64 | Games behind. |
| `league_win_percent` | Float64 | League win percent. |
| `losses` | Float64 | Losses. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points` | Float64 | Points. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `win_percent` | Float64 | Win percent. |
| `wins` | Float64 | Wins. |
| `games_ahead` | Float64 | Games ahead. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `road` | String | Road. |
| `vs. div.` | String | Vs. div.. |
| `vs. conf.` | String | Vs. conf.. |
| `last ten games` | String | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_golf_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_golf_standings-example}

```python
espn_golf_standings(league='tgl')
```
