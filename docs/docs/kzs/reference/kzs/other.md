---
title: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "KZS — KZS FMS public API (Basketball Federation of Slovenia) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# KZS — KZS FMS public API (Basketball Federation of Slovenia) — Other

## kzs_seasons

Every season (39: 1991/1992 to 2029/2030) with its opaque id and the sdv END year.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/seasons/`

**Valid URL:** [https://api.kzs.si/api/v1/public/seasons/?limit=1000](https://api.kzs.si/api/v1/public/seasons/?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#kzs_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_id` | Int64 | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `name` | String | Season, 'START/END' (2025/2026); the sdv season is the END year. |
| `is_active` | Boolean | True for the current season only (2026/2027 on 2026-10-12). |
| `active` | Boolean | Same as `isActive`. |
| `season` | Int64 | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_seasons-example}

```python
kzs_seasons()
```

## kzs_competitions

Competitions of a season (leagues, cups, youth, 3x3, national teams), or of every season.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/competitions/`

**Valid URL:** [https://api.kzs.si/api/v1/public/competitions/?limit=10000&season=2026](https://api.kzs.si/api/v1/public/competitions/?limit=10000&season=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026/2027); the wrapper maps it to KZS' opaque season id. Omitted = every season (627 competitions). |

### Returns {#kzs_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `name` | String | Competition name, Slovenian; the sponsor name changes over the years (glossary in kzs-returns.md). |
| `gender` | String | MALE or FEMALE. |
| `category` | String | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | String | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `season_id` | Int64 | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season_name` | String | Season, 'START/END' (2025/2026); sdv season = the END year. |
| `season_is_active` | Boolean | True for the current season only (2026/2027 on 2026-10-12). |
| `season_active` | Boolean | Same as `isActive`. |
| `type` | String | Competition type (code table); absent on 2 competitions. |
| `season` | Int64 | The competition's season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_competitions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_competitions-example}

```python
kzs_competitions(season=2026)
```

## kzs_competition

One competition: season, gender, category, rank and type (its phases are kzs_phases).

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/competitions/{competition_id}`

**Valid URL:** [https://api.kzs.si/api/v1/public/competitions/579](https://api.kzs.si/api/v1/public/competitions/579)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `name` | String | Competition name, Slovenian (glossary in kzs-returns.md). |
| `season_id` | Int64 | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season_name` | String | Season, 'START/END' (2025/2026); sdv season = the END year. |
| `season_is_active` | Boolean | True for the current season only (2026/2027 on 2026-10-12). |
| `season_active` | Boolean | Same as `isActive`. |
| `gender` | String | MALE or FEMALE. |
| `category` | String | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | String | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `type` | String | Competition type (code table). |
| `season` | Int64 | Season of the record: opaque id and 'START/END' name. |
| `social_medias` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_competition-example}

```python
kzs_competition(competition_id=579)
```

## kzs_phases

The phases of a competition (regular season, play-offs, finals) with dates and type.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/competitions/{competition_id}`

**Valid URL:** [https://api.kzs.si/api/v1/public/competitions/579](https://api.kzs.si/api/v1/public/competitions/579)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_phases-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `season` | Int64 | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `phase_id` | Int64 | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `name` | String | Phase name, Slovenian: 'Redni del' = regular season, 'Četrtfinale' = quarterfinal, 'Polfinale' = semifinal, 'Finale' = final, '1. del' / '2. del' = first / second stage. |
| `start_date` | String | Phase start date (UTC midnight). |
| `end_date` | String | Phase end date (UTC midnight). |
| `type` | String | Phase format (code table). |
| `groups` | String | Groups of the phase (empty when the phase has one table). |
| `wins_needed` | Int64 | Wins needed to take a best-of series. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_phases-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_phases-example}

```python
kzs_phases(competition_id=579)
```

## kzs_rounds

Number of rounds of each phase of a competition.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/competitions/{competition_id}/rounds`

**Valid URL:** [https://api.kzs.si/api/v1/public/competitions/579/rounds](https://api.kzs.si/api/v1/public/competitions/579/rounds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `phase_id` | Int64 | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `rounds` | Int64 | Number of rounds in the phase. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_rounds-example}

```python
kzs_rounds(competition_id=579)
```

## kzs_matches

Matches of a competition (optionally inside a from/to window) with the Genius LiveStats id.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/?limit=1000&competitionId=579&from=2025-10-01T00%3A00%3A00.000Z&to=2025-10-31T23%3A59%3A59.000Z](https://api.kzs.si/api/v1/public/matches/?limit=1000&competitionId=579&from=2025-10-01T00%3A00%3A00.000Z&to=2025-10-31T23%3A59%3A59.000Z)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |
| `from` | `date_from` |  |  | `Y` | Earliest tip-off, an ISO instant such as 2025-10-01T00:00:00.000Z (a bare date is a 400). Omitted = the whole competition. |
| `to` | `date_to` |  |  | `Y` | Latest tip-off, an ISO instant such as 2025-10-31T23:59:59.000Z. Omitted = the whole competition. |

### Returns {#kzs_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | KZS match id. Joins to /matches/{matchId} (and its /playByPlay, /stats) and stats `matchId`. |
| `competition_id` | Int64 | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `competition_name` | String | Competition name (Slovenian; the sponsor name changes: Liga Telemach 2007-15, NovaKBM 2015-24, OTP banka 2024-). |
| `competition_type` | String | Competition type (code table). |
| `competition_phase_id` | Int64 | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `competition_phase_name` | String | Phase name, Slovenian ('Redni del' = regular season, '1. del' = first stage). |
| `transferred` | Boolean | True when the match was carried over from an earlier phase (results transferred). |
| `season_id` | Int64 | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season` | Int64 | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `genius_match_id` | Int64 | Genius LiveStats match id parsed from fiba_live_stats_url (org KZS); null before 2015/16. |
| `organiser_team_id` | Int64 | Team id of the organiser (home) side; null in older headers, which send an empty organiser. |
| `round` | Int64 | Round (matchday) number within the phase. |
| `fiba_live_stats_url` | String | Genius Sports LiveStats URL 'https://fibalivestats.dcd.shared.geniussports.com/u/KZS/{geniusMatchId}/'; the number is the Genius match id that keys genius-livestats/ data.json. Absent before 2015-16 (no live stats). |
| `game_index` | Int64 | Game number within the round (fixture order). |
| `date_time` | String | Tip-off, UTC (ISO 8601 'Z'). |
| `attendance` | Int64 | Spectators (0 when not recorded). |
| `sport_hall_name` | String | Name of the venue (sports hall) where the game is played or the team is based. |
| `sport_hall_address` | String | Venue address, one line. |
| `sport_hall_id` | Int64 | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `first_team_id` | Int64 | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. First (home) side. |
| `first_team_team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. First (home) team. |
| `is_first_team_placeholder` | Boolean | True when the first (home) slot is a bracket placeholder, not yet a team. |
| `second_team_id` | Int64 | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. Second (away) side. |
| `second_team_team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. Second (away) team. |
| `is_second_team_placeholder` | Boolean | True when the second (away) slot is a placeholder. |
| `first_team_name` | String | First (home) team name. |
| `first_team_short_name` | String | First team short name ('' when none). |
| `second_team_name` | String | Second (away) team name. |
| `second_team_short_name` | String | Second team short name ('' when none). |
| `first_team_score` | Int64 | First (home) team final score. |
| `second_team_score` | Int64 | Second (away) team final score. |
| `score_inserted_manually` | Boolean | True when the score was typed in by the federation rather than taken from the live-stats feed. |
| `first_team_logo_uuid` | String | First (home) team logo: image file id, served at https://api.kzs.si/public/images/{uuid} (PNG). |
| `second_team_logo_uuid` | String | Second (away) team logo: image file id, served at https://api.kzs.si/public/images/{uuid} (PNG). |
| `first_team_code` | String | First team 3-letter code (club code). |
| `second_team_code` | String | Second team 3-letter code. |
| `status` | String | Match status: FINISHED in every sample (POSTPONED filtered by the site on other lists). |
| `delegated` | Boolean | True once officials are assigned to the match. |
| `referee1_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee1_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee1_person_first_name` | String | The official's given name. |
| `referee1_person_last_name` | String | The official's family name. |
| `referee1_person_gender` | String | MALE or FEMALE. |
| `referee1_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee2_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee2_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee2_person_first_name` | String | The official's given name. |
| `referee2_person_last_name` | String | The official's family name. |
| `referee2_person_gender` | String | MALE or FEMALE. |
| `referee2_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee3_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee3_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee3_person_first_name` | String | The official's given name. |
| `referee3_person_last_name` | String | The official's family name. |
| `referee3_person_gender` | String | MALE or FEMALE. |
| `referee3_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `commissioner_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `commissioner_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `commissioner_person_first_name` | String | The official's given name. |
| `commissioner_person_last_name` | String | The official's family name. |
| `commissioner_person_gender` | String | MALE or FEMALE. |
| `commissioner_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee_controller_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee_controller_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee_controller_person_first_name` | String | The official's given name. |
| `referee_controller_person_last_name` | String | The official's family name. |
| `referee_controller_person_gender` | String | MALE or FEMALE. |
| `referee_controller_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `match_evaluation_id` | Int64 | Id of the officials' evaluation record (internal; no public route). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_matches-example}

```python
kzs_matches(competition_id=579, date_from='2025-10-01T00:00:00.000Z', date_to='2025-10-31T23:59:59.000Z')
```

## kzs_match

Match header: teams, scores, venue, officials' names, best performers and status.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839](https://api.kzs.si/api/v1/public/matches/101839)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_match-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | KZS match id. Joins to /matches/{matchId} (and its /playByPlay, /stats) and stats `matchId`. |
| `competition_id` | Int64 | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `competition_name` | String | Competition name (Slovenian; the sponsor name changes: Liga Telemach 2007-15, NovaKBM 2015-24, OTP banka 2024-). |
| `competition_type` | String | Competition type (code table). |
| `competition_phase_id` | Int64 | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `competition_phase_name` | String | Phase name, Slovenian (see `phases[].name`). |
| `transferred` | Boolean | True when the match was carried over from an earlier phase (results transferred). |
| `season_id` | Int64 | Opaque KZS season id (27 = 2026/2027); the sdv season is the END year in season. |
| `season` | Int64 | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `genius_match_id` | Int64 | Genius LiveStats match id parsed from fiba_live_stats_url (org KZS); null before 2015/16. |
| `organiser_team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. The organising (home) team. |
| `round` | Int64 | Round (matchday) number within the phase. |
| `fiba_live_stats_url` | String | Genius Sports LiveStats URL 'https://fibalivestats.dcd.shared.geniussports.com/u/KZS/{geniusMatchId}/'; the number is the Genius match id that keys genius-livestats/ data.json. Absent before 2015-16 (no live stats). |
| `game_index` | Int64 | Game number within the round (fixture order). |
| `date_time` | String | Tip-off, UTC (ISO 8601 'Z'). |
| `attendance` | Int64 | Spectators (0 when not recorded). |
| `first_team_id` | Int64 | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. First (home) side. |
| `first_team_team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `first_team_name` | String | First (home) team name; absent on 2007-08 headers. |
| `first_team_home_jersey_color` | String | First team home colour, hex. |
| `first_team_guest_jersey_color` | String | First team away colour, hex. |
| `first_team_logo_uuid` | String | First (home) team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `first_team_code` | String | First team 3-letter code. |
| `first_team_forfeited` | Boolean | True when this team forfeited. |
| `first_team_is_placeholder` | Boolean | True when the slot is a bracket placeholder. |
| `first_team_best_performer_player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `first_team_best_performer_player_first_name` | String | The player's given name as registered with KZS. |
| `first_team_best_performer_player_last_name` | String | The player's family name as registered with KZS. |
| `first_team_best_performer_player_photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `first_team_best_performer_efficiency` | Int64 | Efficiency (PIR) in the match. |
| `first_team_best_performer_points` | Int64 | Points in the match. |
| `first_team_best_performer_rebounds` | Int64 | Rebounds in the match. |
| `first_team_best_performer_assists` | Int64 | Assists in the match. |
| `first_team_best_performer_steals` | Int64 | Steals in the match. |
| `first_team_best_performer_blocks` | Int64 | Blocks in the match. |
| `second_team_id` | Int64 | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. Second (away) side. |
| `second_team_team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `second_team_name` | String | Second (away) team name; absent on 2007-08 headers. |
| `second_team_home_jersey_color` | String | Second team home colour, hex. |
| `second_team_guest_jersey_color` | String | Second team away colour, hex. |
| `second_team_logo_uuid` | String | Second (away) team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `second_team_code` | String | Second team 3-letter code. |
| `second_team_forfeited` | Boolean | True when this team forfeited. |
| `second_team_is_placeholder` | Boolean | True when the slot is a bracket placeholder. |
| `second_team_best_performer_player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `second_team_best_performer_player_first_name` | String | The player's given name as registered with KZS. |
| `second_team_best_performer_player_last_name` | String | The player's family name as registered with KZS. |
| `second_team_best_performer_player_photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `second_team_best_performer_efficiency` | Int64 | Efficiency (PIR) in the match. |
| `second_team_best_performer_points` | Int64 | Points in the match. |
| `second_team_best_performer_rebounds` | Int64 | Rebounds in the match. |
| `second_team_best_performer_assists` | Int64 | Assists in the match. |
| `second_team_best_performer_steals` | Int64 | Steals in the match. |
| `second_team_best_performer_blocks` | Int64 | Blocks in the match. |
| `sport_hall_id` | Int64 | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `sport_hall_name` | String | Name of the venue (sports hall) where the game is played or the team is based. |
| `first_team_score` | Int64 | First (home) team final score. |
| `second_team_score` | Int64 | Second (away) team final score. |
| `score_inserted_manually` | Boolean | True when the score was typed in by the federation rather than taken from the live-stats feed. |
| `status` | String | Match status: FINISHED in every sample (the capture banks final games only); the site also filters POSTPONED matches. |
| `referee1_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee1_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee1_person_first_name` | String | The official's given name. |
| `referee1_person_last_name` | String | The official's family name. |
| `referee1_person_gender` | String | MALE or FEMALE. |
| `referee1_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee2_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee2_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee2_person_first_name` | String | The official's given name. |
| `referee2_person_last_name` | String | The official's family name. |
| `referee2_person_gender` | String | MALE or FEMALE. |
| `referee2_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee3_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee3_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee3_person_first_name` | String | The official's given name. |
| `referee3_person_last_name` | String | The official's family name. |
| `referee3_person_gender` | String | MALE or FEMALE. |
| `referee3_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `commissioner_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `commissioner_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `commissioner_person_first_name` | String | The official's given name. |
| `commissioner_person_last_name` | String | The official's family name. |
| `commissioner_person_gender` | String | MALE or FEMALE. |
| `commissioner_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee_controller_id` | Int64 | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee_controller_person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee_controller_person_first_name` | String | The official's given name. |
| `referee_controller_person_last_name` | String | The official's family name. |
| `referee_controller_person_gender` | String | MALE or FEMALE. |
| `referee_controller_ranks` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `gender` | String | MALE or FEMALE. |
| `live_stats` | Boolean | True when the match has live stats (box score and play-by-play). |
| `without_stats` | Boolean | True when the match keeps no statistics. |
| `delegated` | Boolean | True once officials are assigned to the match. |
| `home_jersey_color` | String | Home jersey colour, hex '#RRGGBB'. |
| `guest_jersey_color` | String | Away jersey colour, hex. |
| `bests` | String | Each team's top player per stat. |
| `match_evaluation_id` | Int64 | Id of the officials' evaluation record (internal; no public route). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_match-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_match-example}

```python
kzs_match(match_id=101839)
```

## kzs_period_scores

Points by match part (quarters and overtimes) from the match header.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839](https://api.kzs.si/api/v1/public/matches/101839)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_period_scores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `score_id` | Int64 | Period-score row id. |
| `match_part` | String | Period: QUARTER1-QUARTER4, then OT1 for the first overtime (OT2 and on by the same pattern, unobserved). |
| `first_team_score` | Int64 | First (home) team points in the period. |
| `second_team_score` | Int64 | Second (away) team points in the period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_period_scores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_period_scores-example}

```python
kzs_period_scores(match_id=101839)
```

## kzs_pbp

Play-by-play of a final game (2015/16 on, with gaps), one row per action with shot x/y.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}/playByPlay`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839/playByPlay](https://api.kzs.si/api/v1/public/matches/101839/playByPlay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_pbp-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `action_number` | Int64 | 1-based position of the action in the feed, which is in game order. |
| `team_side` | String | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `source` | String | Feed the action came from: GENIUS_SPORTS in every sample. |
| `clock` | String | Time remaining in the period, 'MM:SS:CC' (minutes, seconds, hundredths): '09:56:00' = 9:56 left. |
| `action_type` | String | Action type (code table). |
| `sub_type` | String | Action subtype ('' when none; code table). |
| `qualifiers` | String | Semicolon-separated qualifiers ('' when none; code table). |
| `x` | Float64 | Location along the court length, 0-100 percent (see the coordinate section): shots, and from 2020-21 fouls and turnovers; 0.0 when the row has no location. |
| `y` | Float64 | Location across the court width, 0-100 percent (see the coordinate section); 0.0 when the row has no location. |
| `area` | String | Court zone of the action (code table): set on shots, and from 2020-21 on located fouls and turnovers; '' on rows without a location and on every shot of early 2015-16 games. |
| `side` | String | Basket end: LEFT (x≈0) or RIGHT (x≈100). Absent on non-shot rows before 2020-21 and on some 2020-21 rows (read absent on a shot as LEFT). |
| `success` | Boolean | True when the shot / free throw was made (true on non-shot rows). |
| `score1` | Int64 | First (home) team score after the action, a numeric STRING. |
| `score2` | Int64 | Second (away) team score after the action, a numeric STRING. |
| `period` | Int64 | Period number; overtime restarts at 1 with periodType OVERTIME. |
| `period_type` | String | REGULAR or OVERTIME. |
| `part` | String | Period label: QUARTER1-QUARTER4, then OT1 (OT2 and on by the same pattern, unobserved). |
| `match_team_player_id` | Int64 | Match-player id: the player's entry in this one match; NOT the player id. No captured route is keyed by it: join on `playerId` on the same row. |
| `player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `match_team_player_first_name` | String | The acting player's given name, as entered for this match. |
| `match_team_player_last_name` | String | The acting player's family name, as entered for this match. |
| `team_application_id` | Int64 | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `team_name` | String | Team name for the season, sponsor included (e.g. 'Perspektiva Ilirija'). |
| `match_team_id` | Int64 | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. |
| `player_photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `shirt_number` | String | Shirt number worn by the acting player, sent as a string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_pbp-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_pbp-example}

```python
kzs_pbp(match_id=101839)
```

## kzs_pbp_players

The players the play-by-play feed lists for each side of a final game.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}/playByPlay`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839/playByPlay](https://api.kzs.si/api/v1/public/matches/101839/playByPlay)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_pbp_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `team_side` | String | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. Players listed here are the first (home) team. |
| `first_name` | String | The home player's (as in the feed) given name. |
| `last_name` | String | The home player's (as in the feed) family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_pbp_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_pbp_players-example}

```python
kzs_pbp_players(match_id=101839)
```

## kzs_box_score

Box score of a final game (2015/16 on): one row per player of both teams.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}/stats`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839/stats](https://api.kzs.si/api/v1/public/matches/101839/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `team_side` | String | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `team_id` | Int64 | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `team_name` | String | Name of the team for the season, sponsor included. |
| `minutes` | Float64 | The player's minutes played, decimal minutes (39.67 = 39 min 40 s); the team line sums its players (225.0 = 5 x 45 with overtime). |
| `two_pm` | Int64 | The player's two-point field goals made. |
| `two_pa` | Int64 | The player's two-point field goals attempted. |
| `two_p_perc` | Float64 | The player's two-point percentage, 0-100. |
| `three_pm` | Int64 | The player's three-point field goals made. |
| `three_pa` | Int64 | The player's three-point field goals attempted. |
| `three_p_perc` | Float64 | The player's three-point percentage, 0-100. |
| `f_tm` | Int64 | The player's free throws made. |
| `f_ta` | Int64 | The player's free throws attempted. |
| `f_t_perc` | Float64 | The player's free-throw percentage, 0-100. |
| `fg_m` | Int64 | The player's field goals made. |
| `fg_a` | Int64 | The player's field goals attempted. |
| `fg_perc` | Float64 | The player's field-goal percentage, 0-100. |
| `offensive_rebounds` | Int64 | The player's offensive rebounds. |
| `defensive_rebounds` | Int64 | The player's defensive rebounds. |
| `total_rebounds` | Int64 | The player's total rebounds. |
| `assists` | Int64 | The player's assists. |
| `steals` | Int64 | The player's steals. |
| `turnovers` | Int64 | The player's turnovers. |
| `blocks_in_favor` | Int64 | The player's shots blocked (blocks made). |
| `blocks_against` | Int64 | The player's own shots that were blocked. |
| `foul_commited` | Int64 | The player's fouls committed. |
| `foul_received` | Int64 | The player's fouls drawn. |
| `p_ir` | Int64 | The player's PIR, performance index rating ((points + rebounds + assists + steals + blocks + fouls drawn) - (missed field goals + missed free throws + turnovers + shots blocked + fouls committed)). |
| `plus_minus` | Int64 | The player's plus-minus: points scored minus allowed while on court (team line: the final margin). |
| `assists_turnover_ratio` | Float64 | The player's assist-to-turnover ratio (0 in every sample: not computed). |
| `defensive_rating` | Float64 | The player's defensive rating (0 in every sample: not computed). |
| `efficiency` | Int64 | The player's efficiency as the feed computes it (equal to `pIR` on 2025-26 team lines). |
| `points` | Int64 | The player's points. |
| `efficiency_custom` | Float64 | The player's custom efficiency as the federation computes it (equal to `efficiency` on player rows; 0 on team lines). |
| `efficiency_game_score` | Float64 | The player's game score (0 in every sample: not computed). |
| `fast_break_points_attempted` | Int64 | The player's fast-break attempts (0 in every sample: not computed). |
| `fast_break_points_made` | Int64 | The player's fast-break makes (0 in every sample: not computed). |
| `usage_percentage` | Float64 | The player's usage percentage (0 in every sample: not computed). |
| `m_vp_votes` | Int64 | The player's MVP votes (0 in every sample). |
| `minus` | Int64 | The player's points allowed on court (0 in every sample: not computed). |
| `offensive_rating` | Float64 | The player's offensive rating (0 in every sample: not computed). |
| `p_er` | Float64 | The player's PER (0 in every sample: not computed). |
| `plus` | Int64 | The player's points scored on court (0 in every sample: not computed). |
| `points_fast_break` | Int64 | The player's fast-break points. |
| `points_in_the_paint` | Int64 | The player's points in the paint. |
| `points_in_the_paint_attempted` | Int64 | The player's paint attempts (0 in every sample: not computed). |
| `points_in_the_paint_made` | Int64 | The player's paint makes (0 in every sample: not computed). |
| `points_second_chance` | Int64 | The player's second-chance points. |
| `second_chance_points_attempted` | Int64 | The player's second-chance attempts (0 in every sample: not computed). |
| `second_chance_points_made` | Int64 | The player's second-chance makes (0 in every sample: not computed). |
| `true_shooting_attempts` | Int64 | The player's true-shooting attempts (0 in every sample: not computed). |
| `true_shooting_percentage` | Float64 | The player's true-shooting percentage (0 in every sample: not computed). |
| `player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `match_team_player_firstname` | String | The player's given name, as entered for this match (lower-case n in this route). |
| `match_team_player_lastname` | String | The player's family name, as entered for this match (lower-case n in this route). |
| `match_team_player_year_of_birth` | String | Birth year from the match entry, sent as a string ('2002'); the profile `person.yearOfBirth` is an integer. |
| `jersey_number` | String | Shirt number worn, sent as a string ('1', '55'). |
| `starting_lineup` | Boolean | True when the player started. |
| `captain` | Boolean | Games as captain. |
| `played` | Boolean | True when the player got on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_box_score-example}

```python
kzs_box_score(match_id=101839)
```

## kzs_score_changes

The score at the end of each minute of a final game.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{match_id}/stats`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/101839/stats](https://api.kzs.si/api/v1/public/matches/101839/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (match_id in kzs_matches), e.g. 101839. Global across seasons. |

### Returns {#kzs_score_changes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | KZS match id. Joins to /matches/{matchId} (and its /playByPlay, /stats) and stats `matchId`. |
| `minute` | Int64 | Game minute (1-40, then overtime minutes). |
| `score1` | Int64 | First (home) team score at the end of the minute, an integer. |
| `score2` | Int64 | Second (away) team score at the end of the minute, an integer. |
| `total_minutes` | Int64 | Game length in minutes (40, 45 with one overtime). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_score_changes-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_score_changes-example}

```python
kzs_score_changes(match_id=101839)
```

## kzs_standings

Standings of a competition, one row per team in each phase (and group).

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/standings`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/standings?competitionId=579](https://api.kzs.si/api/v1/public/matches/standings?competitionId=579)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `phase_id` | Int64 | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `phase_name` | String | Phase name, Slovenian (see `phases[].name`). |
| `phase_type` | String | Phase format (code table). |
| `group_id` | Int64 | Group id inside the phase (null when the table is the phase's own). |
| `group_name` | String | Group name inside the phase (null when the table is the phase's own). |
| `position` | Int64 | 1-based position of the row in its table, in the order the API lists it (the wire has no rank column). |
| `team_id` | Int64 | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `team_name` | String | Team name for the season, sponsor included. |
| `club_code` | String | Club 3-letter code. |
| `logo_uuid` | String | Team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `games_played` | Int64 | Games played in the phase. |
| `won` | Int64 | Wins in the phase. |
| `lost` | Int64 | Losses in the phase. |
| `pts_plus` | Int64 | Points scored by the team through its games played. |
| `pts_minus` | Int64 | Points allowed. |
| `home_won` | Int64 | Home games the team has won. |
| `home_lost` | Int64 | Home games the team has lost. |
| `away_won` | Int64 | Away games the team has won. |
| `away_lost` | Int64 | Away games the team has lost. |
| `points` | Int64 | Table points: 2 per win, 1 per loss (Krka 26-1 = 53). |
| `draws` | Int64 | Draws (0: basketball has none). |
| `lost_points` | Int64 | Table points deducted (0 in every sample). |
| `tie_break_position` | Int64 | Final position after tie-breaks (absent on playoff-phase rows). |
| `pts_difference` | Int64 | Point differential (scored - allowed). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_standings-example}

```python
kzs_standings(competition_id=579)
```

## kzs_playing_days

Match days of a competition and the number of games on each.

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/matches/{competition_id}/playingDays`

**Valid URL:** [https://api.kzs.si/api/v1/public/matches/579/playingDays](https://api.kzs.si/api/v1/public/matches/579/playingDays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_id` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_playing_days-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `date` | String | Calendar date of the match day, 'YYYY-MM-DD'. |
| `no_of_games` | Int64 | Matches on that day. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_playing_days-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_playing_days-example}

```python
kzs_playing_days(competition_id=579)
```

## kzs_roster

A team's roster in one competition (the rows carry no team or competition id).

**Endpoint URL:** `GET https://api.kzs.si/api/v1/public/teamPlayers/`

**Valid URL:** [https://api.kzs.si/api/v1/public/teamPlayers/?limit=1000&teamId=17551&competitionId=579](https://api.kzs.si/api/v1/public/teamPlayers/?limit=1000&teamId=17551&competitionId=579)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `teamId` | `team_id` |  | `Y` |  | Team id (team_id in kzs_teams): one club team in one season, e.g. 17551. Not the match-team id nor the club id. |
| `competitionId` | `competition_id` |  | `Y` |  | Competition id (competition_id in kzs_competitions): one per competition per season, e.g. 579 (men's first league 2025/26). |

### Returns {#kzs_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `competition_id` | Int64 | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `roster_entry_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `player_id` | Int64 | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `identification_number` | Int64 | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `person_id` | Int64 | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `first_name` | String | The player's given name. |
| `last_name` | String | The player's family name. |
| `year_of_birth` | Int64 | Year of birth (players: public bio). |
| `nationality_id` | Int64 | Country id (FMS internal; use the ISO codes). |
| `nationality_alpha2_code` | String | ISO 3166-1 alpha-2 country code. |
| `nationality_alpha3_code` | String | ISO 3166-1 alpha-3 country code. |
| `nationality_name` | String | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `nationality_eng_name` | String | Country name in English. |
| `homegrown` | Boolean | True when the player counts as homegrown (domači igralec) for roster limits. |
| `photo_uuid` | String | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `positions` | String | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `jersey_number` | String | Shirt number worn, sent as a string ('1', '55'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The API answered HTTP 400 (its only error status: an unknown id or a bad parameter). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is unknown or partial: a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |

### Example {#kzs_roster-example}

```python
kzs_roster(competition_id=579, team_id=17551)
```
