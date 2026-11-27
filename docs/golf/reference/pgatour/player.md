# Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — Player

> Golf — PGA TOUR GraphQL API (orchestrator.pgatour.com; runtime only, public frontend key or $PGA_API_KEY) — Player — function reference in sdv-py, the SportsDataverse Python package.

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
| `first_name` | String | Player's first (given) name. |
| `last_name` | String | Player's last (family) name. |
| `display_name` | String | Player's full name as the site displays it (e.g. "Rory McIlroy"). |
| `country` | String | Country the player represents. |
| `country_code` | String | Country code (e.g. "NIR"). |
| `owgr` | String | Official World Golf Ranking position as displayed. |
| `height` | String | Height in feet and inches as displayed. |
| `height_meters` | String | Height in meters. |
| `weight` | String | Weight in pounds. |
| `weight_kilograms` | String | Weight in kilograms. |
| `age` | String | Player's age in years, as text (e.g. "37"). |
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

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

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
| `first_name` | String | Player's first (given) name. |
| `last_name` | String | Player's last (family) name. |
| `short_name` | String | Player's first initial and last name (e.g. "L Åberg"). |
| `display_name` | String | Player's full name as the site displays it (e.g. "Ludvig Åberg"). |
| `country` | String | Country the player represents. |
| `country_code` | String | Three-letter code of the country the player represents (e.g. "SWE"). |
| `age` | String | Player's age in years, as text (e.g. "27"); null when the profile gives none. |
| `education` | String | College or university. |
| `turned_pro` | String | Year the player turned professional. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_player_directory-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_player_directory-example}

```python
pgatour_player_directory(tour_code='R')
```

## pgatour_player_results

A player's career results on a tour: one row per tournament edition played (finish, score to par, total, points, money, round scores).

**Endpoint URL:** `POST https://orchestrator.pgatour.com/graphql`

**Example request:** `curl -X POST 'https://orchestrator.pgatour.com/graphql' -H 'Content-Type: application/json' -d '{"operationName": "PlayerProfileTournamentResults", "query": "query PlayerProfileTournamentResults($playerId: ID!, $tourCode: TourCode) {\n  playerProfileTournamentResults(playerId: $playerId, tourCode: $tourCode) {\n    playerId\n    tournaments {\n      tournamentNum\n      tournaments {\n        tournamentId\n        tournamentName\n        year\n        startDate\n        courseName\n        position\n        toPar\n        total\n        points\n        pointsRank\n        winnings\n        roundScores {\n          roundNum\n          roundScore\n        }\n      }\n    }\n  }\n}\n", "variables": {"playerId": "28237", "tourCode": "R"}}'`

| API Parameter | Python | In | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|:---:|---|
| `playerId` | `player_id` | body |  | `Y` |  | PGA TOUR player id, e.g. 28237. |
| `tourCode` | `tour_code` | body |  |  | `Y` | Tour code (R PGA TOUR, S PGA TOUR Champions, H Korn Ferry Tour). |

### Returns {#pgatour_player_results-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | String | PGA TOUR player id. |
| `tournament_num` | String | Tournament's permanent number (usually the last three digits of its tournament ids, e.g. "014"). |
| `tournament_id` | String | Tournament id of the edition (e.g. "R2015009"). |
| `tournament_name` | String | Tournament name in that edition's season. |
| `season` | Int64 | Season of the edition. |
| `start_date` | String | Edition start date as displayed (e.g. "3.19.2015"). |
| `course_name` | String | Course the edition was played on. |
| `position` | String | Finishing position as displayed ("1", "T11", "CUT", "DNS"). |
| `to_par` | String | Score to par as displayed (e.g. "-11"). |
| `total` | Int64 | Total strokes; null for a match-play event. |
| `points` | String | Season points race (FedExCup) points earned, as displayed. |
| `points_rank` | String | Points race rank after the event, as displayed. |
| `winnings` | String | Prize money as displayed (e.g. "$151,200.00"). |
| `round_1` | Int64 | Strokes in round 1. |
| `round_2` | Int64 | Strokes in round 2. |
| `round_3` | Int64 | Strokes in round 3; null for a round not played. |
| `round_4` | Int64 | Strokes in round 4; null for a round not played. |

**`return_parsed=False`** — the operation's answer (`dict`), its compressed `payload` decoded; `pgatour_past_champions` adds the `tournamentId` it was asked for, which the answer does not repeat.

### Raises {#pgatour_player_results-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API answered null for the request (an unknown tournament, player or stat id), or `pgatour_past_results` was asked for a season the tournament has no edition in. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API rejected the query variables, or a variable is not of its type (a year that is not a season). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Any other GraphQL error, or a compressed payload that does not decode. |

### Example {#pgatour_player_results-example}

```python
pgatour_player_results(player_id='28237', tour_code='R')
```
