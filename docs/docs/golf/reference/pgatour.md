---
title: Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY)
sidebar_label: PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY)
description: "Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY)

`sportsdataverse.golf` — 10 endpoints.

> **Note:** Runtime only: each call sends one GraphQL query to `orchestrator.pgatour.com/graphql`. sdv-py publishes no release assets from this API and ships no bulk or backfill tooling for it.

> **Note:** The request body is `{operationName, query, variables}`; the query document ships in `sportsdataverse/golf/pgatour_queries/`. Every request also carries an `x-api-key` header (left out of the example request): the public key pgatour.com's own frontend sends, read from the site on first use. Set the `PGA_API_KEY` environment variable to supply a key yourself.

> **Note:** Tour codes: `R` PGA TOUR, `S` PGA TOUR Champions, `H` Korn Ferry Tour. Tournament ids are the tour code, the season and the event number (`R2026014` is the 2026 Masters): read them from `pgatour_schedule`.

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
| `season` | String | Season year the schedule belongs to (e.g. 2026). |
| `schedule_status` | String | "completed" for an event already played, "upcoming" for one still to come. |
| `tournament_id` | String | PGA TOUR tournament id (tour code + season + event number, e.g. "R2026014"); the id the leaderboard, scorecard, tee-times and shot-details wrappers take. |
| `tournament_name` | String | Tournament name. |
| `date` | String | Tournament dates as the site displays them (e.g. "Apr 9 - 12"). |
| `start_date` | String | Tournament start (UTC). |
| `sequence_number` | Int64 | Event's order in the season. |
| `tournament_status` | String | Tournament status (e.g. "COMPLETED", "IN_PROGRESS", "NOT_STARTED"). |
| `course_name` | String | Host course. |
| `city` | String | Host city. |
| `state` | String | Host state or province. |
| `state_code` | String | Host state or province abbreviation. |
| `country` | String | Host country. |
| `country_code` | String | Host country code (e.g. "USA"). |
| `purse` | String | Total purse as displayed (e.g. "$9,100,000"). |
| `champion` | String | Winner's name; null before the event finishes. |
| `champion_id` | String | Winner's PGA TOUR player id. |
| `champion_earnings` | String | Winner's share of the purse as displayed. |
| `standing_heading` | String | Season points race the event counts toward (e.g. "FEDEXCUP"). |
| `standing_value` | String | Points on offer to the winner as displayed (e.g. "500 pts"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_schedule-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_leaderboard-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

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
| `round_number` | Int64 | Round number. |
| `course_id` | String | Course id (an event played on more than one course carries one per round). |
| `course_name` | String | Course name. |
| `hole_number` | Int64 | Hole number (1-18). |
| `par` | Int64 | Hole par. |
| `yardage` | Int64 | Hole length in yards that round. |
| `score` | Int64 | Strokes on the hole; null when the hole is not yet played. |
| `score_status` | String | Result on the hole (e.g. "BIRDIE", "PAR", "BOGEY", "EAGLE", "DOUBLE_BOGEY"). |
| `round_to_par` | String | Running score to par for the round after this hole, as displayed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_scorecard-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

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
| `round_number` | Int64 | Round number. |
| `round_status` | String | Round status (e.g. "COMPLETE", "IN_PROGRESS", "UPCOMING"). |
| `group_number` | Int64 | Tee-time group number within the round. |
| `tee_time` | String | Group's tee time (UTC). |
| `start_tee` | Int64 | Starting hole (1, or 10 for a two-tee start). |
| `back_nine` | Boolean | True when the group starts on the back nine. |
| `course_id` | String | Course id. |
| `course_name` | String | Course name. |
| `player_id` | String | PGA TOUR player id. |
| `display_name` | String | Player's full name. |
| `country` | String | Player's country code. |
| `amateur` | Boolean | True for an amateur. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_tee_times-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_tee_times-example}

```python
pgatour_tee_times(tournament_id='R2026014')
```

## pgatour_player

One player's profile: name, country, world ranking, height, weight, birth date and place, year turned pro, school, residence.

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "Player", "query": "query Player($id: ID!) {\n  player(id: $id) {\n    id\n    firstName\n    lastName\n    displayName\n    country\n    countryFlag\n    bioLink\n    owgr\n    playerBio {\n      heightImperial\n      heightMeters\n      weightImperial\n      weightKilograms\n      age\n      born\n      birthplace { city state country }\n      turnedPro\n      school\n      degree\n      graduationYear\n      residence { city state country }\n      family\n    }\n  }\n}\n", "variables": {"id": "28237"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `id` | `player_id` | body |  | `Y` |  | PGA TOUR player id, e.g. 28237. |

### Returns {#pgatour_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | String | PGA TOUR player id. |
| `first_name` | String | First name. |
| `last_name` | String | Last name. |
| `display_name` | String | Full name. |
| `country` | String | Country the player represents. |
| `country_code` | String | Country code (e.g. "NIR"). |
| `owgr` | String | Official World Golf Ranking position as displayed. |
| `height` | String | Height in feet and inches as displayed. |
| `height_meters` | String | Height in meters. |
| `weight` | String | Weight in pounds. |
| `weight_kilograms` | String | Weight in kilograms. |
| `age` | String | Age in years. |
| `born` | String | Birth date as displayed (e.g. "May 4, 1989"). |
| `birthplace_city` | String | Birthplace city. |
| `birthplace_state` | String | Birthplace state or province. |
| `birthplace_country` | String | Birthplace country. |
| `turned_pro` | String | Year the player turned professional. |
| `school` | String | College or university. |
| `residence_city` | String | City of residence. |
| `residence_state` | String | State or province of residence. |
| `residence_country` | String | Country of residence. |
| `family` | String | Family as listed on the profile. |
| `bio_link` | String | The player's pgatour.com profile URL. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_player-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_player-example}

```python
pgatour_player(player_id='28237')
```

## pgatour_player_directory

A tour's player directory: one row per player (id, name, country, age, college, year turned pro).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "PlayerDirectory", "query": "query PlayerDirectory($tourCode: TourCode!, $active: Boolean) {\n  playerDirectory(tourCode: $tourCode, active: $active) {\n    tourCode\n    players {\n      id\n      isActive\n      firstName\n      lastName\n      shortName\n      displayName\n      country\n      countryFlag\n      playerBio { age education turnedPro }\n    }\n  }\n}\n", "variables": {"tourCode": "R"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `active` | `active` | body |  |  | `Y` | True for active players only; omitted, every player in the directory. |

### Returns {#pgatour_player_directory-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tour_code` | String | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |
| `player_id` | String | PGA TOUR player id. |
| `is_active` | Boolean | True for a player active on the tour. |
| `first_name` | String | First name. |
| `last_name` | String | Last name. |
| `short_name` | String | Short name. |
| `display_name` | String | Full name. |
| `country` | String | Country the player represents. |
| `country_code` | String | Country code. |
| `age` | String | Age in years. |
| `education` | String | College or university. |
| `turned_pro` | String | Year the player turned professional. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_player_directory-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_player_directory-example}

```python
pgatour_player_directory(tour_code='R')
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
| `tour_code` | String | Tour code. |
| `year` | Int64 | Season year. |
| `category` | String | Stat category key (e.g. "STROKES_GAINED", "SCORING"). |
| `category_name` | String | Stat category name (e.g. "Strokes Gained"). |
| `sub_category` | String | Sub-category the stat is listed under. |
| `stat_id` | String | PGA TOUR stat id (e.g. "02675" for SG Total); the id pgatour_stat_details takes. |
| `stat_title` | String | Stat name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_stat_catalog-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

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
| `tour_code` | String | Tour code. |
| `year` | Int64 | Season year. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_stat_details-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

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
| `season` | String | Season year (read from the race's title, e.g. "2025 Standings"). |
| `player_id` | String | PGA TOUR player id. |
| `first_name` | String | First name. |
| `last_name` | String | Last name. |
| `display_name` | String | Full name. |
| `country` | String | Country the player represents. |
| `country_code` | String | Country code. |
| `rank_official` | String | Official rank as displayed ("-" for none). |
| `rank_projected` | String | Rank projected from live results, as displayed. |
| `rank_movement` | String | Direction of the projected rank change (e.g. "UP", "DOWN", "CONSTANT"). |
| `points_official` | String | Official points as displayed. |
| `points_projected` | String | Points projected from live results, as displayed. |
| `points_event` | String | Points earned at the current event, as displayed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_tour_cup-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

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
| `round_number` | Int64 | Round number. |
| `hole_number` | Int64 | Hole number. |
| `par` | Int64 | Hole par. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pgatour_shot_details-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | The API answered null for the request (an unknown tournament, player or stat id). |
| `ValueError` | The API rejected the query variables. |
| `sportsdataverse.errors.AssetFetchError` | Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_shot_details-example}

```python
pgatour_shot_details(player_id='59095', round=4, tournament_id='R2026006')
```
