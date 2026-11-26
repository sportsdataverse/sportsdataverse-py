---
title: "Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — Other"
sidebar_label: "Other"
sidebar_position: 2
description: "Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — Other

## pgatour_schedule

A tour's season schedule: one row per tournament (id, dates, course, purse, champion, points on offer), completed events first.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "Schedule", "query": "query Schedule($tourCode: String!, $year: String) {\n  schedule(tourCode: $tourCode, year: $year) {\n    tour\n    seasonYear\n    completed { ...ScheduleMonthFields }\n    upcoming { ...ScheduleMonthFields }\n  }\n}\n\nfragment ScheduleMonthFields on ScheduleMonth {\n  month\n  year\n  tournaments {\n    id\n    tournamentName\n    date\n    startDate\n    sequenceNumber\n    tournamentStatus\n    courseName\n    city\n    state\n    stateCode\n    country\n    countryCode\n    purse\n    champion\n    championId\n    championEarnings\n    tourStandingHeading\n    tourStandingValue\n  }\n}\n", "variables": {"tourCode": "R", "year": "2026"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `year` | `year` | body |  |  | `Y` | Season year, e.g. 2026; omitted, the current season. |

### Returns {#pgatour_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year the schedule belongs to (e.g. 2026). |
| `schedule_status` | String | "completed" for an event already played, "upcoming" for one still to come. |
| `tournament_id` | String | PGA TOUR tournament id (tour code + season + event number, e.g. "R2026014"); the id the leaderboard, scorecard, tee-times and shot-details wrappers take. |
| `tournament_name` | String | Tournament's full name, sponsor included (e.g. "Travelers Championship"). |
| `date` | String | Tournament dates as the site displays them (e.g. "Apr 9 - 12"). |
| `start_date` | String | Tournament start (UTC). |
| `sequence_number` | Int64 | Event's order in the season. |
| `tournament_status` | String | Tournament status (e.g. "COMPLETED", "IN_PROGRESS", "NOT_STARTED"). |
| `course_name` | String | Course hosting the event (e.g. "TPC River Highlands"). |
| `city` | String | City the host course is in (e.g. "Cromwell"). |
| `state` | String | Host state or province. |
| `state_code` | String | Host state or province abbreviation. |
| `country` | String | Country the host course is in, spelled out (e.g. "United States of America"). |
| `country_code` | String | Host country code (e.g. "USA"). |
| `purse` | String | Total purse as displayed (e.g. "$9,100,000"). |
| `champion` | String | Winner's name; null before the event finishes. |
| `champion_id` | String | Winner's PGA TOUR player id. |
| `champion_earnings` | String | Winner's share of the purse as displayed. |
| `standing_heading` | String | Season points race the event counts toward (e.g. "FEDEXCUP"). |
| `standing_value` | String | Points on offer to the winner as displayed (e.g. "500 pts"). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_schedule-example}

```python
pgatour_schedule(tour_code='R', year=2026)
```

## pgatour_leaderboard

One tournament's leaderboard: position, total to par, strokes, holes completed, today's score, round scores and projected FedExCup rank per player.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "LeaderboardCompressedV3", "query": "query LeaderboardCompressedV3($id: ID!) {\n  leaderboardCompressedV3(id: $id) {\n    id\n    payload\n  }\n}\n", "variables": {"id": "R2026014"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `tournament_id` | body |  | `Y` |  | Tournament id (tournament_id in pgatour_schedule), e.g. R2026014. |

### Returns {#pgatour_leaderboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `tournament_status` | String | Tournament status (e.g. "COMPLETED", "IN_PROGRESS"). |
| `sort_order` | Int64 | Leaderboard order (0 = leader). |
| `player_id` | String | PGA TOUR player id. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `display_name` | String | Player's full name. |
| `short_name` | String | Player's short name (e.g. "R. McIlroy"). |
| `country` | String | Player's country code (e.g. "NIR"). |
| `amateur` | Boolean | True for an amateur. |
| `position` | String | Position as displayed ("1", "T3", "CUT", "WD"). |
| `total` | String | Total score to par as displayed ("-12", "E", "+6"). |
| `to_par` | Int64 | Total score to par as an integer (E is 0); null before a score is posted. |
| `total_strokes` | Int64 | Total strokes over the rounds played. |
| `thru` | String | Holes completed in the current round as displayed ("F" when finished, "-" when not started). |
| `today` | String | Score to par in the current round as displayed. |
| `current_round` | Int64 | Round in play (or last played). |
| `player_state` | String | Player's state in the event (e.g. "COMPLETE", "ACTIVE", "CUT"). |
| `round_1` | Int64 | Strokes in round 1; null when not played. |
| `round_2` | Int64 | Strokes in round 2; null when not played. |
| `round_3` | Int64 | Strokes in round 3; null when not played (a missed cut, or not started). |
| `round_4` | Int64 | Strokes in round 4; null when not played. |
| `fedexcup_official` | String | Player's official FedExCup rank going into the event, as displayed. |
| `fedexcup_projected` | String | Player's FedExCup rank projected from this leaderboard, as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_leaderboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_leaderboard-example}

```python
pgatour_leaderboard(tournament_id='R2026014')
```

## pgatour_scorecard

One player's scorecard for a tournament: one row per hole per round (par, yardage, strokes, birdie / bogey status, running score to par).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "ScorecardCompressedV3", "query": "query ScorecardCompressedV3($tournamentId: ID!, $playerId: ID!) {\n  scorecardCompressedV3(tournamentId: $tournamentId, playerId: $playerId) {\n    id\n    payload\n  }\n}\n", "variables": {"tournamentId": "R2026014", "playerId": "28237"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |
| `playerId` | `player_id` | body |  | `Y` |  | PGA TOUR player id (player_id in pgatour_leaderboard), e.g. 28237. |

### Returns {#pgatour_scorecard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id (the answer's own id carries the player id too; it is stripped). |
| `player_id` | String | PGA TOUR player id. |
| `round_number` | Int64 | Tournament round the hole was played in (1-4). |
| `course_id` | String | Course id (an event played on more than one course carries one per round). |
| `course_name` | String | Course the round was played on (e.g. "Augusta National Golf Club"). |
| `hole_number` | Int64 | Hole number (1-18). |
| `par` | Int64 | Par of the hole (3, 4 or 5). |
| `yardage` | Int64 | Hole length in yards that round. |
| `score` | Int64 | Strokes on the hole; null when the hole is not yet played. |
| `score_status` | String | Result on the hole (e.g. "BIRDIE", "PAR", "BOGEY", "EAGLE", "DOUBLE_BOGEY"). |
| `round_to_par` | String | Running score to par for the round after this hole, as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_scorecard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_scorecard-example}

```python
pgatour_scorecard(player_id='28237', tournament_id='R2026014')
```

## pgatour_tee_times

A tournament's tee times: one row per player per group per round (tee time in UTC, starting tee, course).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "TeeTimesCompressedV2", "query": "query TeeTimesCompressedV2($id: ID!) {\n  teeTimesCompressedV2(id: $id) {\n    id\n    payload\n  }\n}\n", "variables": {"id": "R2026014"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |

### Returns {#pgatour_tee_times-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `round_number` | Int64 | Tournament round the tee times are for (1-4). |
| `round_status` | String | Round status (e.g. "COMPLETE", "IN_PROGRESS", "UPCOMING"). |
| `group_number` | Int64 | Tee-time group number within the round. |
| `tee_time` | String | Group's tee time (UTC). |
| `start_tee` | Int64 | Starting hole (1, or 10 for a two-tee start). |
| `back_nine` | Boolean | True when the group starts on the back nine. |
| `course_id` | String | PGA TOUR course id of the course the group tees off on (e.g. "014"). |
| `course_name` | String | Course the group tees off on (e.g. "Augusta National Golf Club"). |
| `player_id` | String | PGA TOUR player id. |
| `display_name` | String | Player's full name. |
| `country` | String | Player's country code. |
| `amateur` | Boolean | True for an amateur. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_tee_times-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_tee_times-example}

```python
pgatour_tee_times(tournament_id='R2026014')
```

## pgatour_stat_catalog

Every stat a tour publishes: category, sub-category, stat id and title. The stat ids are what pgatour_stat_details takes.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "StatOverview", "query": "query StatOverview($tourCode: TourCode!, $year: Int) {\n  statOverview(tourCode: $tourCode, year: $year) {\n    tourCode\n    year\n    categories {\n      category\n      displayName\n      subCategories {\n        displayName\n        stats { statId statTitle }\n      }\n    }\n  }\n}\n", "variables": {"tourCode": "R", "year": 2026}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `year` | `year` | body |  |  | `Y` | Season year; omitted, the current season. |

### Returns {#pgatour_stat_catalog-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tour_code` | String | Tour the catalog lists stats for (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `year` | Int64 | Season the catalog lists stats for (e.g. 2026). |
| `category` | String | Stat category key (e.g. "STROKES_GAINED", "SCORING"). |
| `category_name` | String | Stat category name (e.g. "Strokes Gained"). |
| `sub_category` | String | Sub-category the stat is listed under. |
| `stat_id` | String | PGA TOUR stat id (e.g. "02675" for SG Total); the id pgatour_stat_details takes. |
| `stat_title` | String | Stat's display name (e.g. "Approaches from 50-125 yards"). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_stat_catalog-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_stat_catalog-example}

```python
pgatour_stat_catalog(tour_code='R', year=2026)
```

## pgatour_stat_details

One stat's season table, long form: one row per player per stat column (rank, column name, value as displayed).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "StatDetails", "query": "query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int) {\n  statDetails(tourCode: $tourCode, statId: $statId, year: $year) {\n    tourCode\n    year\n    statId\n    statType\n    statTitle\n    statDescription\n    tourAvg\n    lastProcessed\n    statHeaders\n    rows {\n      __typename\n      ... on StatDetailsPlayer {\n        playerId\n        playerName\n        country\n        countryFlag\n        rank\n        rankDiff\n        rankChangeTendency\n        stats { statName statValue }\n      }\n    }\n  }\n}\n", "variables": {"statId": "120", "tourCode": "R", "year": 2026}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `statId` | `stat_id` | body |  | `Y` |  | Stat id (stat_id in pgatour_stat_catalog), as a string (leading zeros matter), e.g. '120' (scoring average) or '02675' (SG Total). |
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `year` | `year` | body |  |  | `Y` | Season year; omitted, the current season. |

### Returns {#pgatour_stat_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tour_code` | String | Tour the stat table is for (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `year` | Int64 | Season the stat table covers (e.g. 2026). |
| `stat_id` | String | PGA TOUR stat id. |
| `stat_title` | String | Stat name (e.g. "Scoring Average"). |
| `stat_type` | String | Stat window (e.g. "YTD"). |
| `tour_average` | String | Tour average for the stat, as displayed. |
| `player_id` | String | PGA TOUR player id. |
| `player_name` | String | Player's full name. |
| `country` | String | Player's country. |
| `country_code` | String | Player's country code. |
| `rank` | Int64 | Player's rank in the stat. |
| `rank_change` | String | Change in rank since the last update, as displayed. |
| `stat_name` | String | The stat table's column (e.g. "Avg", "Total Strokes", "Total Rounds"). |
| `stat_value` | String | That column's value as displayed (e.g. "68.619", "5,293"). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_stat_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_stat_details-example}

```python
pgatour_stat_details(stat_id='120', tour_code='R', year=2026)
```

## pgatour_tour_cup

A season-long points race (the FedExCup by default): one row per player with official and projected rank and points.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "TourCupCombined", "query": "query TourCupCombined($tourCode: TourCode!, $id: String, $year: Int) {\n  tourCupCombined(tourCode: $tourCode, id: $id, year: $year) {\n    id\n    title\n    season\n    description\n    players {\n      __typename\n      ... on TourCupCombinedPlayer {\n        id\n        firstName\n        lastName\n        displayName\n        shortName\n        country\n        countryFlag\n        rankingData { projected official event movement movementAmount }\n        pointData { projected official event movement movementAmount }\n      }\n    }\n  }\n}\n", "variables": {"tourCode": "R", "id": "02671", "year": 2025}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `id` | `cup_id` | body |  |  | `Y` | Points race id, as a string (leading zeros matter); 02671 (the default) is the FedExCup, 02698 the FedExCup Fall. |
| `year` | `year` | body |  |  | `Y` | Season year; omitted, the current season. |

### Returns {#pgatour_tour_cup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `cup_id` | String | Points race id with season (e.g. "R-02671-2025" for the 2025 FedExCup). |
| `title` | String | Points race name (e.g. "FedExCup Standings"). |
| `season` | Int64 | Season year (read from the race's title, e.g. "2025 Standings"). |
| `player_id` | String | PGA TOUR player id. |
| `first_name` | String | Player's first (given) name. |
| `last_name` | String | Player's last (family) name. |
| `display_name` | String | Player's full name as the site displays it (e.g. "Wyndham Clark"). |
| `country` | String | Country the player represents. |
| `country_code` | String | Three-letter code of the country the player represents (e.g. "IRL"). |
| `rank_official` | String | Official rank as displayed ("-" for none). |
| `rank_projected` | String | Rank projected from live results, as displayed. |
| `rank_movement` | String | Direction of the projected rank change (e.g. "UP", "DOWN", "CONSTANT"). |
| `points_official` | String | Official points as displayed. |
| `points_projected` | String | Points projected from live results, as displayed. |
| `points_event` | String | Points earned at the current event, as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_tour_cup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_tour_cup-example}

```python
pgatour_tour_cup(tour_code='R', year=2025)
```

## pgatour_shot_details

ShotLink shot tracking for one player's round: one row per stroke with description, distance, lie and start / end coordinates.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "ShotDetailsV4Compressed", "query": "query ShotDetailsV4Compressed($tournamentId: ID!, $playerId: ID!, $round: Int!, $includeRadar: Boolean) {\n  shotDetailsV4Compressed(tournamentId: $tournamentId, playerId: $playerId, round: $round, includeRadar: $includeRadar) {\n    id\n    payload\n  }\n}\n", "variables": {"tournamentId": "R2026006", "playerId": "59095", "round": 4}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026006. |
| `playerId` | `player_id` | body |  | `Y` |  | PGA TOUR player id, e.g. 59095. |
| `round` | `round` | body |  | `Y` |  | Round number (1-4). |

### Returns {#pgatour_shot_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `player_id` | String | PGA TOUR player id. |
| `round_number` | Int64 | Tournament round the shots were played in (1-4). |
| `hole_number` | Int64 | Hole the shots were played on (1-18). |
| `par` | Int64 | Par of the hole (3, 4 or 5). |
| `yardage` | Int64 | Hole length in yards. |
| `hole_score` | Int64 | Strokes on the hole. |
| `hole_status` | String | Result on the hole (e.g. "BIRDIE", "PAR"). |
| `stroke_number` | Int64 | Stroke number on the hole. |
| `stroke_type` | String | Stroke kind (e.g. "STROKE", "PENALTY", "DROP"). |
| `play_by_play` | String | ShotLink description of the shot (e.g. "356 yds to left fairway, 134 yds to hole"). |
| `distance` | String | Shot distance as displayed (e.g. "356 yds"). |
| `distance_remaining` | String | Distance to the hole after the shot, as displayed. |
| `from_location_code` | String | ShotLink lie code the shot was played from (e.g. "OTB" tee box). |
| `from_location` | String | Lie the shot was played from (e.g. "Tee Box"). |
| `to_location_code` | String | ShotLink lie code the shot finished in (e.g. "ELF" left fairway). |
| `to_location` | String | Lie the shot finished in (e.g. "Left Fairway"). |
| `final_stroke` | Boolean | True for the stroke that holed out. |
| `from_x` | Float64 | ShotLink tourcast x coordinate of the shot's start. |
| `from_y` | Float64 | ShotLink tourcast y coordinate of the shot's start. |
| `from_z` | Float64 | ShotLink tourcast z (elevation) coordinate of the shot's start. |
| `to_x` | Float64 | ShotLink tourcast x coordinate where the shot finished. |
| `to_y` | Float64 | ShotLink tourcast y coordinate where the shot finished. |
| `to_z` | Float64 | ShotLink tourcast z (elevation) coordinate where the shot finished. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_shot_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_shot_details-example}

```python
pgatour_shot_details(player_id='59095', round=4, tournament_id='R2026006')
```

## pgatour_schedule_years

The seasons a tour has a published schedule for: one row per season, the current one flagged. The seasons are what pgatour_schedule(year=) takes.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "ScheduleYears", "query": "query ScheduleYears($tourCode: TourCode!) {\n  scheduleYears(tourCode: $tourCode) {\n    years {\n      default\n      displayValue\n      queryValue\n    }\n  }\n}\n", "variables": {"tourCode": "R"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |

### Returns {#pgatour_schedule_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season year with a published schedule, as pgatour_schedule(year=) takes it (e.g. 2023 for the 2022-2023 season). |
| `display_season` | String | Season as the site labels it (e.g. "2026", "2022-2023"). |
| `is_default` | Boolean | True for the season the site shows by default (the current one). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_schedule_years-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_schedule_years-example}

```python
pgatour_schedule_years(tour_code='R')
```

## pgatour_tournaments

Tournament headers: one row per tournament id (name, dates, status, current round, format, city, time zone).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "Tournaments", "query": "query Tournaments($ids: [ID!]) {\n  tournaments(ids: $ids) {\n    id\n    tournamentName\n    seasonYear\n    displayDate\n    tournamentStatus\n    roundStatus\n    roundDisplay\n    currentRound\n    formatType\n    scoredLevel\n    city\n    state\n    country\n    timezone\n    tournamentLocation\n  }\n}\n", "variables": {"ids": ["R2026014"]}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `ids` | `tournament_ids` | body |  | `Y` |  | Tournament id, or a list of them (tournament_id in pgatour_schedule), e.g. R2026014. |

### Returns {#pgatour_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id (tour code + season + event number, e.g. "R2026014"). |
| `tournament_name` | String | Tournament name (sponsor title as listed). |
| `season` | Int64 | Season year the tournament belongs to. |
| `display_date` | String | Tournament dates as the site displays them (e.g. "Apr 9 - 12, 2026"). |
| `tournament_status` | String | Tournament status (e.g. "COMPLETED", "IN_PROGRESS", "NOT_STARTED"). |
| `round_status` | String | Status of the current round (e.g. "OFFICIAL", "IN_PROGRESS"). |
| `round_display` | String | Current round as displayed (e.g. "R4"). |
| `current_round` | Int64 | Current (or last) round number. |
| `format_type` | String | Scoring format (e.g. "STROKE_PLAY"). |
| `scored_level` | String | Depth of scoring the site publishes (e.g. "BASIC", "TOURCAST" for ShotLink events). |
| `city` | String | City of the host course. |
| `state` | String | Host state or province. |
| `country` | String | Country of the host course. |
| `timezone` | String | Host time zone (IANA name, e.g. "America/New_York"). |
| `location` | String | Location as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_tournaments-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_tournaments-example}

```python
pgatour_tournaments(tournament_ids='R2026014')
```

## pgatour_past_champions

Every past champion of a tournament: one row per edition (season, winner, total and score to par).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "TournamentOverview", "query": "query TournamentOverview($tournamentId: ID!) {\n  tournamentOverview(tournamentId: $tournamentId) {\n    formatType\n    pastChampions {\n      year\n      displaySeason\n      playerId\n      displayName\n      countryCode\n      total\n      score\n    }\n  }\n}\n", "variables": {"tournamentId": "R2026014"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |

### Returns {#pgatour_past_champions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id the champions were asked for (the answer does not repeat it). |
| `format_type` | String | Tournament scoring format (e.g. "STROKE_PLAY"). |
| `season` | Int64 | Season of the edition the champion won. |
| `display_season` | String | Season as displayed. |
| `player_id` | String | Champion's PGA TOUR player id. |
| `display_name` | String | Champion's full name. |
| `country_code` | String | Champion's country code (e.g. "NIR"). |
| `total` | Int64 | Champion's total strokes. |
| `to_par` | String | Champion's score to par as displayed (e.g. "-12"). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_past_champions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_past_champions-example}

```python
pgatour_past_champions(tournament_id='R2026014')
```

## pgatour_hole_by_hole

Hole-by-hole scores of one round for the whole field: one row per player per hole (par, yardage, strokes, birdie / bogey status).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "LeaderboardHoleByHole", "query": "query LeaderboardHoleByHole($tournamentId: ID!, $round: Int) {\n  leaderboardHoleByHole(tournamentId: $tournamentId, round: $round) {\n    tournamentId\n    currentRound\n    playerData {\n      playerId\n      courseId\n      courseCode\n      out\n      in\n      total\n      totalToPar\n      scores {\n        holeNumber\n        sequenceNumber\n        par\n        yardage\n        score\n        roundScore\n        status\n      }\n    }\n  }\n}\n", "variables": {"tournamentId": "R2026014", "round": 4}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |
| `round` | `round` | body |  |  | `Y` | Round number (1-4); omitted, the current round. |

### Returns {#pgatour_hole_by_hole-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `player_id` | String | PGA TOUR player id. |
| `course_id` | String | Course id the player played the round on. |
| `course_code` | String | Course code (e.g. "AN"). |
| `hole_number` | Int64 | Hole number (1-18). |
| `sequence_number` | Int64 | Order the player played the hole in (0-based; differs from hole_number on a two-tee start). |
| `par` | Int64 | Par of the hole. |
| `yardage` | Int64 | Hole yardage for the round. |
| `score` | Int64 | Strokes on the hole; null on a hole not yet played. |
| `score_status` | String | Score relative to par ("EAGLE", "BIRDIE", "PAR", "BOGEY", "DOUBLE_BOGEY", ...). |
| `round_to_par` | String | Player's running score to par for the round after the hole, as displayed (e.g. "-1", "E"). |
| `front_nine` | Int64 | Player's strokes on holes 1-9 for the round. |
| `back_nine` | Int64 | Player's strokes on holes 10-18 for the round. |
| `round_total` | Int64 | Player's strokes for the round. |
| `round_total_to_par` | String | Player's round score to par as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_hole_by_hole-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_hole_by_hole-example}

```python
pgatour_hole_by_hole(round=4, tournament_id='R2026014')
```

## pgatour_leaderboard_stats

A tournament's leaderboard stat table, long form: one row per player per stat (win / top 10 / make-cut chances by default).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "LeaderboardStats", "query": "query LeaderboardStats($id: ID!, $statsType: LeaderboardStatsType) {\n  leaderboardStats(id: $id, statsType: $statsType) {\n    id\n    type\n    statIds\n    titles\n    players {\n      playerId\n      stats {\n        statId\n        value\n        rank\n        sortValue\n        color\n      }\n    }\n  }\n}\n", "variables": {"id": "R2026014", "statsType": "PROBABILITY"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |
| `statsType` | `stats_type` | body |  |  | `Y` | PROBABILITY (win, top 10 and make-cut chances), STROKES_GAINED or ODDS. An event without that table answers no rows. |

### Returns {#pgatour_leaderboard_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `stats_type` | String | Stat table (PROBABILITY, STROKES_GAINED or ODDS). |
| `player_id` | String | PGA TOUR player id. |
| `stat_id` | String | Stat column id (e.g. "win", "top10", "makeCut"). |
| `stat_title` | String | Stat column heading the site shows (e.g. "Win %"). |
| `stat_value` | String | Stat value as displayed (e.g. "12.3%"). |
| `rank` | String | Player's rank on the stat as displayed ("1", "T5"). |
| `sort_value` | Float64 | Stat value as a number (a probability is a fraction, 0-1). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_leaderboard_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_leaderboard_stats-example}

```python
pgatour_leaderboard_stats(stats_type='PROBABILITY', tournament_id='R2026014')
```

## pgatour_field

A tournament's field: one row per entered player, then the alternates (status, world ranking, how they qualified).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "Field", "query": "query Field($id: ID!, $includeWithdrawn: Boolean) {\n  field(id: $id, includeWithdrawn: $includeWithdrawn) {\n    id\n    players { ...FieldPlayer }\n    alternates { ...FieldPlayer }\n  }\n}\n\nfragment FieldPlayer on PlayerField {\n  id\n  firstName\n  lastName\n  displayName\n  shortName\n  country\n  countryFlag\n  amateur\n  alternate\n  withdrawn\n  status\n  owgr\n  qualifier\n  rankingPoints\n}\n", "variables": {"id": "R2026014", "includeWithdrawn": true}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |
| `includeWithdrawn` | `include_withdrawn` | body |  |  | `Y` | True to keep players who withdrew. |

### Returns {#pgatour_field-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `player_id` | String | PGA TOUR player id. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `display_name` | String | Name as the field list shows it (e.g. "McIlroy, Rory"). |
| `short_name` | String | Short name (e.g. "R. McIlroy"). |
| `country` | String | Country the player represents. |
| `country_code` | String | Country code (e.g. "NIR"). |
| `amateur` | Boolean | True for an amateur. |
| `alternate` | Boolean | True for an alternate (not yet in the field). |
| `withdrawn` | Boolean | True for a player who withdrew. |
| `status` | String | Field status (e.g. "IN"). |
| `owgr` | String | Official World Golf Ranking position as displayed. |
| `qualifier` | String | Exemption category the player got in under. |
| `ranking_points` | String | Ranking points as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_field-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_field-example}

```python
pgatour_field(tournament_id='R2026014')
```

## pgatour_past_results

One edition's final leaderboard: one row per player (position, total, score to par, round scores).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "TournamentPastResults", "query": "query TournamentPastResults($id: ID!, $year: Int) {\n  tournamentPastResults(id: $id, year: $year) {\n    id\n    players {\n      position\n      total\n      parRelativeScore\n      player {\n        id\n        firstName\n        lastName\n        displayName\n        shortName\n        country\n        countryFlag\n        amateur\n      }\n      rounds {\n        score\n        parRelativeScore\n      }\n    }\n  }\n}\n", "variables": {"id": "R2026014", "year": 20250}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `tournament_id` | body |  | `Y` |  | Tournament id of any edition, e.g. R2026014. |
| `year` | `year` | body |  |  | `Y` | Edition season, e.g. 2025 (sent as the API's five-digit 20250; a five-digit value is sent as given). Omitted, the edition in tournament_id. |

### Returns {#pgatour_past_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | Tournament id of the edition the API answered with (e.g. "R2025014" for the 2025 Masters). |
| `season` | Int64 | Season of that edition (read from tournament_id). |
| `position` | String | Finishing position as displayed ("1", "T5", "CUT", "WD"). |
| `player_id` | String | PGA TOUR player id. |
| `first_name` | String | First name (the API can send it empty). |
| `last_name` | String | Last name (the API can send it empty). |
| `display_name` | String | Player's full name. |
| `short_name` | String | Short name (e.g. "R. McIlroy"). |
| `country` | String | Country the player represents. |
| `country_code` | String | Country code (e.g. "NIR"). |
| `amateur` | Boolean | True for an amateur. |
| `total` | Int64 | Total strokes over the event. |
| `to_par` | String | Score to par as displayed (e.g. "-11"). |
| `round_1` | Int64 | Strokes in round 1. |
| `round_2` | Int64 | Strokes in round 2. |
| `round_3` | Int64 | Strokes in round 3; null when the player missed the cut. |
| `round_4` | Int64 | Strokes in round 4; null when the player missed the cut. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_past_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_past_results-example}

```python
pgatour_past_results(tournament_id='R2026014', year=2025)
```

## pgatour_course_stats

Hole difficulty for every course of a tournament: one row per hole per round (scoring average, rank, eagles to double bogeys).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "CourseStats", "query": "query CourseStats($tournamentId: ID!) {\n  courseStats(tournamentId: $tournamentId) {\n    tournamentId\n    courses {\n      courseId\n      courseName\n      courseCode\n      par\n      yardage\n      hostCourse\n      roundHoleStats {\n        roundHeader\n        roundNum\n        holeStats {\n          __typename\n          ... on CourseHoleStats {\n            courseHoleNum\n            parValue\n            yards\n            scoringAverage\n            scoringAverageDiff\n            rank\n            eagles\n            birdies\n            pars\n            bogeys\n            doubleBogey\n          }\n        }\n      }\n    }\n  }\n}\n", "variables": {"tournamentId": "R2026014"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |

### Returns {#pgatour_course_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `course_id` | String | Course id (the course_id pgatour_hole_details takes; leading zeros matter, e.g. "014"). |
| `course_name` | String | Course name as listed. |
| `course_code` | String | Course code (e.g. "AN"). |
| `course_par` | Int64 | Par of the whole course. |
| `course_yardage` | Int64 | Course yardage. |
| `host_course` | Boolean | True for the host course of a multi-course event. |
| `round_number` | Int64 | Round number; null on the all-rounds rows. |
| `round_header` | String | Round label ("All Rounds", "Round 1", ...). |
| `hole_number` | Int64 | Hole number (1-18). |
| `par` | Int64 | Par of the hole. |
| `yards` | Int64 | Hole yardage for the tournament. |
| `scoring_average` | Float64 | Field's scoring average on the hole. |
| `scoring_average_diff` | Float64 | Scoring average minus par (positive plays over par). |
| `difficulty_rank` | Int64 | Hole's difficulty rank on the course (1 = hardest). |
| `eagles` | Int64 | Eagles (or better) made on the hole. |
| `birdies` | Int64 | Birdies made on the hole. |
| `pars` | Int64 | Pars made on the hole. |
| `bogeys` | Int64 | Bogeys made on the hole. |
| `double_bogeys` | Int64 | Double bogeys (or worse) made on the hole. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_course_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_course_stats-example}

```python
pgatour_course_stats(tournament_id='R2026014')
```

## pgatour_hole_details

One hole of one course over a tournament: one row (par, yardage, difficulty rank, counts and shares of eagles to double bogeys).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "HoleDetails", "query": "query HoleDetails($tournamentId: ID!, $courseId: ID!, $hole: Int!) {\n  holeDetails(tournamentId: $tournamentId, courseId: $courseId, hole: $hole) {\n    tournamentId\n    courseId\n    holeNum\n    holeInfo {\n      par\n      yards\n      rank\n      rounds\n      scoringAverageDiff\n    }\n    statsSummary {\n      scoringAverage\n      eagles\n      eaglesPercent\n      birdies\n      birdiesPercent\n      pars\n      parsPercent\n      bogeys\n      bogeysPercent\n      doubleBogeys\n      doubleBogeysPercent\n    }\n  }\n}\n", "variables": {"tournamentId": "R2026014", "courseId": "014", "hole": 12}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tournamentId` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |
| `courseId` | `course_id` | body |  | `Y` |  | Course id as a string (course_id in pgatour_course_stats; leading zeros matter), e.g. '014'. |
| `hole` | `hole` | body |  | `Y` |  | Hole number (1-18). |

### Returns {#pgatour_hole_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `course_id` | String | Course id (leading zeros matter, e.g. "014"). |
| `hole_number` | Int64 | Hole number (1-18). |
| `par` | Int64 | Par of the hole. |
| `yards` | Int64 | Hole yardage for the tournament. |
| `difficulty_rank` | Int64 | Hole's difficulty rank on the course (1 = hardest). |
| `rounds_played` | Int64 | Rounds played on the hole over the tournament. |
| `scoring_average_diff` | Float64 | Scoring average minus par (positive plays over par). |
| `eagles` | Int64 | Eagles (or better) made on the hole. |
| `eagles_pct` | String | Share of rounds with an eagle, as displayed (e.g. "1%"). |
| `birdies` | Int64 | Birdies made on the hole. |
| `birdies_pct` | String | Share of rounds with a birdie, as displayed. |
| `pars` | Int64 | Pars made on the hole. |
| `pars_pct` | String | Share of rounds with a par, as displayed. |
| `bogeys` | Int64 | Bogeys made on the hole. |
| `bogeys_pct` | String | Share of rounds with a bogey, as displayed. |
| `double_bogeys` | Int64 | Double bogeys (or worse) made on the hole. |
| `double_bogeys_pct` | String | Share of rounds with a double bogey or worse, as displayed. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_hole_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_hole_details-example}

```python
pgatour_hole_details(course_id='014', hole=12, tournament_id='R2026014')
```

## pgatour_scorecard_stats

One player's stat lines for a tournament: one row per stat per round (performance, scoring and strokes gained, with the season-to-date value).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "ScorecardStatsV3Compressed", "query": "query ScorecardStatsV3Compressed($id: ID!, $playerId: ID!) {\n  scorecardStatsV3Compressed(id: $id, playerId: $playerId) {\n    id\n    payload\n  }\n}\n", "variables": {"id": "R2026014", "playerId": "28237"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `tournament_id` | body |  | `Y` |  | Tournament id, e.g. R2026014. |
| `playerId` | `player_id` | body |  | `Y` |  | PGA TOUR player id, e.g. 28237. |

### Returns {#pgatour_scorecard_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tournament_id` | String | PGA TOUR tournament id. |
| `round` | String | Round number as a string ("1" .. "4"; "-1" is the whole tournament). |
| `round_name` | String | Round label ("Round 1", ..., "All"). |
| `round_status` | String | Round status (e.g. "OFFICIAL"). |
| `stat_group` | String | Stat group ("performance", "scoring" or "strokes_gained"). |
| `stat_id` | String | PGA TOUR stat id of the line, where it has one. |
| `stat_name` | String | Stat label (e.g. "Birdies", "SG: Total"). |
| `stat_value` | String | Player's value for the round as displayed. |
| `rank` | String | Player's rank in the field on the stat for the round, as displayed ("1", "T7"). |
| `year_to_date` | String | Player's season-to-date value of the stat as displayed ("-" when none). |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_scorecard_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_scorecard_stats-example}

```python
pgatour_scorecard_stats(player_id='28237', tournament_id='R2026014')
```
