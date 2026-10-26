---
title: "BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# BNXT League — BNXT League Sportpress API (bnxt.sportpress.info, Belgium and the Netherlands) — Other

## sportpress_ui_strings

UI string tables of the site in English, French and Dutch (one row per language and key).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/server-config`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/server-config](https://bnxt.sportpress.info/api/v1/server-config)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sportpress_ui_strings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `language` | String | ISO 639-1 code of the UI language: en, fr (French) or nl (Dutch). |
| `key` | String | Dotted UI string key such as 'statistics.individual.points'; 302 keys per language. |
| `text` | String | The UI text in that language; 11 keys label statistics columns, the rest are menus and buttons. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_ui_strings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_ui_strings-example}

```python
sportpress_ui_strings()
```

## sportpress_club_seasons

Seasons (END years) in which Belgian and Dutch clubs have teams.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/club/seasons`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/club/seasons?clubs%5B0%5D=1&clubs%5B1%5D=2](https://bnxt.sportpress.info/api/v1/competition-team/club/seasons?clubs%5B0%5D=1&clubs%5B1%5D=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sportpress_club_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year in which the clubs (countries) have teams; includes 2021 = 2020-21. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_club_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_club_seasons-example}

```python
sportpress_club_seasons()
```

## sportpress_referees

Referees of the league (staff title 2).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/staff/list`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/staff/list?title_id=2](https://bnxt.sportpress.info/api/v1/staff/list?title_id=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#sportpress_referees-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `staff_id` | Int64 | Staff member id (key space: sportpress staff); the referees are title 2. |
| `name` | String | Referee name 'First Last' (free text). |
| `title_id` | Int64 | Staff title id: 1 = Head coach, 2 = Referee; Assistant coach has its own id. |
| `title_name` | String | Staff title in English: Head coach, Assistant coach or Referee. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_referees-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_referees-example}

```python
sportpress_referees()
```

## sportpress_phases

The competition of a season with its phases (regular season, playoffs, supercup).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/phase/season/{season}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/phase/season/2026](https://bnxt.sportpress.info/api/v1/phase/season/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26), the API's own key; no mapping is applied. |

### Returns {#sportpress_phases-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year (2026 = 2025-26), the sdv convention and the API's own season key. |
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `competition_name` | String | Competition name, 'BNXT League 2025-2026'. |
| `phase_id` | Int64 | Phase id (key space: sportpress phases); the {phaseId} of the standings route. New every season. |
| `phase_name` | String | Phase name as entered in the CMS; names differ every season. |
| `standings_visible` | Boolean | Whether the phase shows standings; false for playoff phases, which answer the standings route empty. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_phases-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_phases-example}

```python
sportpress_phases(season=2026)
```

## sportpress_competition_teams

Every competition team of one competition: ids, names and country.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/all`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/all?competition_id=24](https://bnxt.sportpress.info/api/v1/competition-team/all?competition_id=24)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |

### Returns {#sportpress_competition_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id the team plays in (key space: sportpress competitions); the competition_id the route was asked for. |
| `season` | Int64 | Season END year of the team's competition. |
| `uu_team_id` | Int64 | Stable club key across seasons (key space: sportpress clubs-as-teams); competition_team_id changes every season, this does not. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `club_id` | Int64 | Country of the team (key space: sportpress clubs): 1 = Belgium, 2 = Netherlands. The provider calls the country a club. |
| `club_name` | String | Country name in English, Belgium or Netherlands. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_competition_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_competition_teams-example}

```python
sportpress_competition_teams(competition_id=24)
```

## sportpress_competition_staff

Coaching staff (head coach, assistants) of every competition team of one competition.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/competition-team/all`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/competition-team/all?competition_id=24](https://bnxt.sportpress.info/api/v1/competition-team/all?competition_id=24)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |

### Returns {#sportpress_competition_staff-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id of the team the staff member works for. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `staff_id` | Int64 | Staff member id (key space: sportpress staff). |
| `name` | String | Staff member name 'First Last' (free text). |
| `title_id` | Int64 | Staff title id: 1 = Head coach; Assistant coach has its own id. |
| `title_name` | String | Staff title in English: Head coach or Assistant coach. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_competition_staff-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_competition_staff-example}

```python
sportpress_competition_staff(competition_id=24)
```

## sportpress_standings

Standings of one phase of a competition: record, points, home/away split, last five and streak.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/standings/competition/{competition_id}/phase/{phase_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/standings/competition/24/phase/169](https://bnxt.sportpress.info/api/v1/standings/competition/24/phase/169)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Required. Competition id (integer), ONE per season of the league: 24 = 2025-26, 25 = 2026-27 (``sportpress_phases`` maps a season to it). |
| `phase_id` | `phase_id` |  | `Y` |  | Required. Phase id (integer) of the competition, from ``sportpress_phases`` (``phase_id``). |

### Returns {#sportpress_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `phase_id` | Int64 | Phase id (key space: sportpress phases), a stage of one competition such as the regular season or the playoffs; new every season. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `team` | String | Team display name including the sponsor; varies with language and playoff suffixes: never join on it. |
| `team_short_name` | String | Three-letter club code (ANT, MON, ...), stable across seasons in the samples. |
| `club_id` | Int64 | Country of the team (key space: sportpress clubs): 1 = Belgium, 2 = Netherlands. The provider calls the country a club. |
| `club_name` | String | Country name in English, Belgium or Netherlands. |
| `position` | Int64 | Rank in the table, 1 = first. |
| `played` | Int64 | Games played in the phase (win + loss + even). |
| `win` | Int64 | Games won in the phase. |
| `loss` | Int64 | Games lost in the phase. |
| `even` | Int64 | Games drawn; 0 in every sample (basketball has no draws). |
| `score` | Int64 | Points scored in the phase. |
| `against` | Int64 | Points conceded in the phase. |
| `score_difference` | Int64 | score - against. |
| `percent` | Float64 | Win percentage 0-100, rounded to 1 decimal. |
| `point` | Int64 | League points: 2 per win plus 1 per loss. |
| `point_percent` | Float64 | Share of the possible league points won, 0-1 rounded to 2 decimals. |
| `home_win` | Int64 | Wins in home games. |
| `home_loss` | Int64 | Losses in home games. |
| `away_win` | Int64 | Wins in away games. |
| `away_loss` | Int64 | Losses in away games. |
| `last5_win` | Int64 | Wins among the last five games. |
| `last5_loss` | Int64 | Losses among the last five games. |
| `streak_is_win` | Boolean | True for a winning streak, false for a losing one. |
| `streak_count` | Int64 | Length of the current streak in games. |
| `group` | String | Group label for split phases; null in every sample. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_standings-example}

```python
sportpress_standings(competition_id=24, phase_id=169)
```

## sportpress_schedule

Every game of a season for Belgian and Dutch clubs, with teams, scores, phase and venue.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/schedule/club/{season}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/schedule/club/2026?clubs%5B0%5D=1&clubs%5B1%5D=2&month=-1](https://bnxt.sportpress.info/api/v1/schedule/club/2026?clubs%5B0%5D=1&clubs%5B1%5D=2&month=-1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26), the API's own key; no mapping is applied. |
| `month` | `month` |  |  | `Y` | Calendar month filter; -1 (default) = the whole season in one body. |
| `phase_id` | `phase_id` |  |  | `Y` | Restrict to one phase (integer), from ``sportpress_phases``. Omitted = every phase. |

### Returns {#sportpress_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | Game id (key space: sportpress games), unique across seasons: the key of every game route. |
| `game_time` | String | Tip-off as a naive datetime. The source carries no offset; the league's local time is assumed and unverified. |
| `status` | String | Game status: finished, or ready for an upcoming game. |
| `competition_id` | Int64 | Competition id (key space: sportpress competitions): ONE season of the league (24 = 2025-26, 25 = 2026-27). Not the season number. The key of the standings, ranking, slider and box routes. |
| `season` | Int64 | Season END year of the game's competition. |
| `phase_id` | Int64 | Phase id (key space: sportpress phases), a stage of one competition such as the regular season or the playoffs; new every season. |
| `phase_name` | String | Phase name as entered in the CMS ('Regular season 2026-27'). |
| `round_name` | String | Round label as entered in the CMS; often empty. |
| `round_priority` | Int64 | Sort order of the round within the phase (provider's priority); meaning of the values is unverified. |
| `period_name` | String | Name of the period the game is in or ended in; 'Extra2' means the second overtime. |
| `arena` | String | Name of the arena where the game is played. |
| `home_competition_team_id` | Int64 | Home side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `home_team` | String | Home side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `home_team_short_name` | String | Home side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `home_score` | Int64 | Home side. Final points of the side, including overtime. |
| `away_competition_team_id` | Int64 | Away side. Competition-team id (key space: sportpress competition teams): a team in ONE competition, so the same club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). Joins competitors, rosters, box scores, film and standings. The stable club key across seasons is `uu_team_id`. |
| `away_team` | String | Away side. Display name incl. the sponsor. Teams of the playoff phases carry a ' playoff' suffix (2025-26), and standings names vary with X-Localization ('Partena Professional Okapi Aalst Playoff' (en) vs 'Okapi Aalst Playoff' (nl, fr)): never join on it. |
| `away_team_short_name` | String | Away side. Three-letter club code (CHA, OOS, ANT, MON, LIM, ...), stable across seasons in the samples. |
| `away_score` | Int64 | Away side. Final points of the side, including overtime. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_schedule-example}

```python
sportpress_schedule(season=2026)
```

## sportpress_season_roster

Every roster entry of a season: player, team, jersey, height, weight and position.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/roster/season-players/{season}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/roster/season-players/2026](https://bnxt.sportpress.info/api/v1/roster/season-players/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26), the API's own key; no mapping is applied. |

### Returns {#sportpress_season_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year the roster was asked for. |
| `roster_id` | Int64 | Roster id (key space: sportpress roster entries): one player on one competition team in one season, so a player has a new roster_id for every team and season. player_id is the person. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `jersey` | String | Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already lost there. |
| `weight` | Int64 | Weight in kg; null when unknown. |
| `height` | Int64 | Height in cm; null when unknown. |
| `active` | Boolean | True for a current roster entry; the lists hold current entries only. |
| `position_id` | Int64 | Position id (key space: sportpress positions; 3 = small_forward). |
| `position` | String | Position code: point_guard, shooting_guard, small_forward, power_forward, center, and the hybrids point_guard-shooting_guard, shooting_guard-small_forward, small_forward-power_forward, power_forward-center. |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_season_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_season_roster-example}

```python
sportpress_season_roster(season=2026)
```

## sportpress_nationality_codes

Distinct player nationality codes of a season (dirty: lower case, pairs, empty).

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/roster/season-nationality-codes/{season}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/roster/season-nationality-codes/2026](https://bnxt.sportpress.info/api/v1/roster/season-nationality-codes/2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  | `Y` |  | Required. Season END year (2026 = 2025-26), the API's own key; no mapping is applied. |

### Returns {#sportpress_nationality_codes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season` | Int64 | Season END year the codes were asked for. |
| `nationality_code` | String | A distinct nationality code of the season's players, as served (dirty). |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_nationality_codes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_nationality_codes-example}

```python
sportpress_nationality_codes(season=2026)
```

## sportpress_roster_find

One roster entry with its player, team and season.

**Endpoint URL:** `GET https://bnxt.sportpress.info/api/v1/roster/find/{roster_id}`

**Valid URL:** [https://bnxt.sportpress.info/api/v1/roster/find/11193](https://bnxt.sportpress.info/api/v1/roster/find/11193)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `roster_id` | `roster_id` |  | `Y` |  | Required. Roster id (integer): one player on one team in one season; from the roster frames. |

### Returns {#sportpress_roster_find-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `roster_id` | Int64 | Roster id (key space: sportpress roster entries): one player on one competition team in one season, so a player has a new roster_id for every team and season. player_id is the person. |
| `competition_team_id` | Int64 | Competition-team id (key space: sportpress competition teams): a team in ONE competition, so a club has a new id every season (Antwerp = 3 in 2021-22, 506 in 2025-26, 541 in 2026-27). uu_team_id is the stable club key. |
| `jersey` | String | Shirt number as text; the box score sends it as an integer, so a zero-padded '00' is already lost there. |
| `weight` | Int64 | Weight in kg; null when unknown. |
| `height` | Int64 | Height in cm; null when unknown. |
| `active` | Boolean | True for a current roster entry; former entries are false. |
| `position_id` | Int64 | Position id (key space: sportpress positions; 3 = small_forward). |
| `position` | String | Position code: point_guard, shooting_guard, small_forward, power_forward, center, and the hybrids point_guard-shooting_guard, shooting_guard-small_forward, small_forward-power_forward, power_forward-center. |
| `player_id` | Int64 | Player id (key space: sportpress persons): stable across seasons and teams, distinct from roster_id. Not a Genius or FIBA person id. |
| `first_name` | String | Given name of the player as entered in the CMS. |
| `last_name` | String | Family name of the player as entered in the CMS. |
| `nationality_code` | String | Nationality code in FIBA style (USA, BEL, NED): DIRTY, with lower-case variants, '/'-joined pairs (USA/BEL), the empty string and 'HOL'; normalise before joining. |
| `birth_date` | String | Date of birth as shown on the public player page; null when unknown. |
| `uu_team_id` | Int64 | Stable club key across seasons of the team. |
| `team` | String | Team display name including the sponsor. |
| `team_short_name` | String | Three-letter club code of the team. |
| `competition_id` | Int64 | Competition id the roster entry's team plays in. |
| `season` | Int64 | Season END year of the roster entry's competition. |

**`return_parsed=False`** — the raw body (`dict`): `{'data': ...}` (the settings object for the site config, the nine-category object for the rankings) with contact fields dropped, plus `_request` (the season, competition, team, roster or game ids asked for).

### Raises {#sportpress_roster_find-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (HTTP 404, an empty `data` list: an unknown id or a season without data, an empty ranking). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key: 'key rotated?', an error or changed envelope, a game that is not finished, a body for another season, competition, team or game). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#sportpress_roster_find-example}

```python
sportpress_roster_find(roster_id=11193)
```
