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
| `season_id` | integer | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `name` | character | Season, 'START/END' (2025/2026); the sdv season is the END year. |
| `is_active` | logical | True for the current season only (2026/2027 on 2026-10-12). |
| `active` | logical | Same as `isActive`. |
| `season` | integer | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_seasons-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_seasons-example}

```python
kzs_seasons()
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `name` | character | Competition name, Slovenian; the sponsor name changes over the years (glossary in kzs-returns.md). |
| `gender` | character | MALE or FEMALE. |
| `category` | character | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | character | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `season_id` | integer | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season_name` | character | Season, 'START/END' (2025/2026); sdv season = the END year. |
| `season_is_active` | logical | True for the current season only (2026/2027 on 2026-10-12). |
| `season_active` | logical | Same as `isActive`. |
| `type` | character | Competition type (code table); absent on 2 competitions. |
| `season` | integer | The competition's season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_competitions-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_competitions-example}

```python
kzs_competitions(season=2026)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `name` | character | Competition name, Slovenian (glossary in kzs-returns.md). |
| `season_id` | integer | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season_name` | character | Season, 'START/END' (2025/2026); sdv season = the END year. |
| `season_is_active` | logical | True for the current season only (2026/2027 on 2026-10-12). |
| `season_active` | logical | Same as `isActive`. |
| `gender` | character | MALE or FEMALE. |
| `category` | character | Age category: ABSOLUTE (senior) or U8-U23 / ANY (code table in kzs-returns.md). |
| `rank` | character | Tier: FIRST-FOURTH or TOURNAMENT (code table). |
| `type` | character | Competition type (code table). |
| `season` | integer | Season of the record: opaque id and 'START/END' name. |
| `social_medias` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_competition-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_competition-example}

```python
kzs_competition(competition_id=579)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `season` | integer | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `phase_id` | integer | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `name` | character | Phase name, Slovenian: 'Redni del' = regular season, 'Četrtfinale' = quarterfinal, 'Polfinale' = semifinal, 'Finale' = final, '1. del' / '2. del' = first / second stage. |
| `start_date` | character | Phase start date (UTC midnight). |
| `end_date` | character | Phase end date (UTC midnight). |
| `type` | character | Phase format (code table). |
| `groups` | character | Groups of the phase (empty when the phase has one table). |
| `wins_needed` | integer | Wins needed to take a best-of series. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_phases-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_phases-example}

```python
kzs_phases(competition_id=579)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `phase_id` | integer | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `rounds` | integer | Number of rounds in the phase. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_rounds-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_rounds-example}

```python
kzs_rounds(competition_id=579)
```

_Last validated n/a._

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
| `match_id` | integer | KZS match id. Joins to /matches/{matchId} (and its /playByPlay, /stats) and stats `matchId`. |
| `competition_id` | integer | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `competition_name` | character | Competition name (Slovenian; the sponsor name changes: Liga Telemach 2007-15, NovaKBM 2015-24, OTP banka 2024-). |
| `competition_type` | character | Competition type (code table). |
| `competition_phase_id` | integer | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `competition_phase_name` | character | Phase name, Slovenian ('Redni del' = regular season, '1. del' = first stage). |
| `transferred` | logical | True when the match was carried over from an earlier phase (results transferred). |
| `season_id` | integer | Season id: OPAQUE, not a year (1 = 2000/2001 ... 27 = 2026/2027, 31 = 1991/1992). The season `name` is 'START/END'; the sdv season is the END year (the second half of `name`). |
| `season` | integer | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `genius_match_id` | integer | Genius LiveStats match id parsed from fiba_live_stats_url (org KZS); null before 2015/16. |
| `organiser_team_id` | integer | Team id of the organiser (home) side; null in older headers, which send an empty organiser. |
| `round` | integer | Round (matchday) number within the phase. |
| `fiba_live_stats_url` | character | Genius Sports LiveStats URL 'https://fibalivestats.dcd.shared.geniussports.com/u/KZS/{geniusMatchId}/'; the number is the Genius match id that keys genius-livestats/ data.json. Absent before 2015-16 (no live stats). |
| `game_index` | integer | Game number within the round (fixture order). |
| `date_time` | character | Tip-off, UTC (ISO 8601 'Z'). |
| `attendance` | integer | Spectators (0 when not recorded). |
| `sport_hall_name` | character | Name of the venue (sports hall) where the game is played or the team is based. |
| `sport_hall_address` | character | Venue address, one line. |
| `sport_hall_id` | integer | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `first_team_id` | integer | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. First (home) side. |
| `first_team_team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. First (home) team. |
| `is_first_team_placeholder` | logical | True when the first (home) slot is a bracket placeholder, not yet a team. |
| `second_team_id` | integer | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. Second (away) side. |
| `second_team_team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. Second (away) team. |
| `is_second_team_placeholder` | logical | True when the second (away) slot is a placeholder. |
| `first_team_name` | character | First (home) team name. |
| `first_team_short_name` | character | First team short name ('' when none). |
| `second_team_name` | character | Second (away) team name. |
| `second_team_short_name` | character | Second team short name ('' when none). |
| `first_team_score` | integer | First (home) team final score. |
| `second_team_score` | integer | Second (away) team final score. |
| `score_inserted_manually` | logical | True when the score was typed in by the federation rather than taken from the live-stats feed. |
| `first_team_logo_uuid` | character | First (home) team logo: image file id, served at https://api.kzs.si/public/images/{uuid} (PNG). |
| `second_team_logo_uuid` | character | Second (away) team logo: image file id, served at https://api.kzs.si/public/images/{uuid} (PNG). |
| `first_team_code` | character | First team 3-letter code (club code). |
| `second_team_code` | character | Second team 3-letter code. |
| `status` | character | Match status: FINISHED in every sample (POSTPONED filtered by the site on other lists). |
| `delegated` | logical | True once officials are assigned to the match. |
| `referee1_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee1_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee1_person_first_name` | character | The official's given name. |
| `referee1_person_last_name` | character | The official's family name. |
| `referee1_person_gender` | character | MALE or FEMALE. |
| `referee1_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee2_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee2_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee2_person_first_name` | character | The official's given name. |
| `referee2_person_last_name` | character | The official's family name. |
| `referee2_person_gender` | character | MALE or FEMALE. |
| `referee2_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee3_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee3_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee3_person_first_name` | character | The official's given name. |
| `referee3_person_last_name` | character | The official's family name. |
| `referee3_person_gender` | character | MALE or FEMALE. |
| `referee3_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `commissioner_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `commissioner_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `commissioner_person_first_name` | character | The official's given name. |
| `commissioner_person_last_name` | character | The official's family name. |
| `commissioner_person_gender` | character | MALE or FEMALE. |
| `commissioner_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee_controller_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee_controller_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee_controller_person_first_name` | character | The official's given name. |
| `referee_controller_person_last_name` | character | The official's family name. |
| `referee_controller_person_gender` | character | MALE or FEMALE. |
| `referee_controller_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `match_evaluation_id` | integer | Id of the officials' evaluation record (internal; no public route). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_matches-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_matches-example}

```python
kzs_matches(competition_id=579, date_from='2025-10-01T00:00:00.000Z', date_to='2025-10-31T23:59:59.000Z')
```

_Last validated n/a._

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
| `match_id` | integer | KZS match id. Joins to /matches/{matchId} (and its /playByPlay, /stats) and stats `matchId`. |
| `competition_id` | integer | Competition id: one per competition per season (men's top league 2025-26 = 579, 2026-27 = 608). Joins to /competitions/{competitionId} and the `competitionId` query params. |
| `competition_name` | character | Competition name (Slovenian; the sponsor name changes: Liga Telemach 2007-15, NovaKBM 2015-24, OTP banka 2024-). |
| `competition_type` | character | Competition type (code table). |
| `competition_phase_id` | integer | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `competition_phase_name` | character | Phase name, Slovenian (see `phases[].name`). |
| `transferred` | logical | True when the match was carried over from an earlier phase (results transferred). |
| `season_id` | integer | Opaque KZS season id (27 = 2026/2027); the sdv season is the END year in season. |
| `season` | character | Season END year (2026/2027 = 2027), from the row's own season name or id, else from the request. |
| `genius_match_id` | integer | Genius LiveStats match id parsed from fiba_live_stats_url (org KZS); null before 2015/16. |
| `organiser_team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. The organising (home) team. |
| `round` | integer | Round (matchday) number within the phase. |
| `fiba_live_stats_url` | character | Genius Sports LiveStats URL 'https://fibalivestats.dcd.shared.geniussports.com/u/KZS/{geniusMatchId}/'; the number is the Genius match id that keys genius-livestats/ data.json. Absent before 2015-16 (no live stats). |
| `game_index` | integer | Game number within the round (fixture order). |
| `date_time` | character | Tip-off, UTC (ISO 8601 'Z'). |
| `attendance` | integer | Spectators (0 when not recorded). |
| `first_team_id` | integer | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. First (home) side. |
| `first_team_team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `first_team_name` | character | First (home) team name; absent on 2007-08 headers. |
| `first_team_home_jersey_color` | character | First team home colour, hex. |
| `first_team_guest_jersey_color` | character | First team away colour, hex. |
| `first_team_logo_uuid` | character | First (home) team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `first_team_code` | character | First team 3-letter code. |
| `first_team_forfeited` | logical | True when this team forfeited. |
| `first_team_is_placeholder` | logical | True when the slot is a bracket placeholder. |
| `first_team_best_performer_player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `first_team_best_performer_player_first_name` | character | The player's given name as registered with KZS. |
| `first_team_best_performer_player_last_name` | character | The player's family name as registered with KZS. |
| `first_team_best_performer_player_photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `first_team_best_performer_efficiency` | integer | Efficiency (PIR) in the match. |
| `first_team_best_performer_points` | integer | Points in the match. |
| `first_team_best_performer_rebounds` | integer | Rebounds in the match. |
| `first_team_best_performer_assists` | integer | Assists in the match. |
| `first_team_best_performer_steals` | integer | Steals in the match. |
| `first_team_best_performer_blocks` | integer | Blocks in the match. |
| `second_team_id` | integer | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. Second (away) side. |
| `second_team_team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `second_team_name` | character | Second (away) team name; absent on 2007-08 headers. |
| `second_team_home_jersey_color` | character | Second team home colour, hex. |
| `second_team_guest_jersey_color` | character | Second team away colour, hex. |
| `second_team_logo_uuid` | character | Second (away) team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `second_team_code` | character | Second team 3-letter code. |
| `second_team_forfeited` | logical | True when this team forfeited. |
| `second_team_is_placeholder` | logical | True when the slot is a bracket placeholder. |
| `second_team_best_performer_player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `second_team_best_performer_player_first_name` | character | The player's given name as registered with KZS. |
| `second_team_best_performer_player_last_name` | character | The player's family name as registered with KZS. |
| `second_team_best_performer_player_photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `second_team_best_performer_efficiency` | integer | Efficiency (PIR) in the match. |
| `second_team_best_performer_points` | integer | Points in the match. |
| `second_team_best_performer_rebounds` | integer | Rebounds in the match. |
| `second_team_best_performer_assists` | integer | Assists in the match. |
| `second_team_best_performer_steals` | integer | Steals in the match. |
| `second_team_best_performer_blocks` | integer | Blocks in the match. |
| `sport_hall_id` | integer | Sport hall (venue) id. /sportHalls/{id} is in the site bundle, not captured. |
| `sport_hall_name` | character | Name of the venue (sports hall) where the game is played or the team is based. |
| `first_team_score` | integer | First (home) team final score. |
| `second_team_score` | integer | Second (away) team final score. |
| `score_inserted_manually` | logical | True when the score was typed in by the federation rather than taken from the live-stats feed. |
| `status` | character | Match status: FINISHED in every sample (the capture banks final games only); the site also filters POSTPONED matches. |
| `referee1_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee1_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee1_person_first_name` | character | The official's given name. |
| `referee1_person_last_name` | character | The official's family name. |
| `referee1_person_gender` | character | MALE or FEMALE. |
| `referee1_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee2_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee2_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee2_person_first_name` | character | The official's given name. |
| `referee2_person_last_name` | character | The official's family name. |
| `referee2_person_gender` | character | MALE or FEMALE. |
| `referee2_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee3_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee3_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee3_person_first_name` | character | The official's given name. |
| `referee3_person_last_name` | character | The official's family name. |
| `referee3_person_gender` | character | MALE or FEMALE. |
| `referee3_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `commissioner_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `commissioner_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `commissioner_person_first_name` | character | The official's given name. |
| `commissioner_person_last_name` | character | The official's family name. |
| `commissioner_person_gender` | character | MALE or FEMALE. |
| `commissioner_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `referee_controller_id` | integer | Official id (referee/commissioner registry); a different key space from `person.id`. /referees/ and /commissioners/ are in the site bundle but not captured. |
| `referee_controller_person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `referee_controller_person_first_name` | character | The official's given name. |
| `referee_controller_person_last_name` | character | The official's family name. |
| `referee_controller_person_gender` | character | MALE or FEMALE. |
| `referee_controller_ranks` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `gender` | character | MALE or FEMALE. |
| `live_stats` | logical | True when the match has live stats (box score and play-by-play). |
| `without_stats` | logical | True when the match keeps no statistics. |
| `delegated` | logical | True once officials are assigned to the match. |
| `home_jersey_color` | character | Home jersey colour, hex '#RRGGBB'. |
| `guest_jersey_color` | character | Away jersey colour, hex. |
| `bests` | character | Each team's top player per stat. |
| `match_evaluation_id` | integer | Id of the officials' evaluation record (internal; no public route). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_match-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_match-example}

```python
kzs_match(match_id=101839)
```

_Last validated n/a._

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
| `match_id` | integer | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `score_id` | integer | Period-score row id. |
| `match_part` | character | Period: QUARTER1-QUARTER4, then OT1 for the first overtime (OT2 and on by the same pattern, unobserved). |
| `first_team_score` | integer | First (home) team points in the period. |
| `second_team_score` | integer | Second (away) team points in the period. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_period_scores-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_period_scores-example}

```python
kzs_period_scores(match_id=101839)
```

_Last validated n/a._

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
| `match_id` | integer | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `action_number` | integer | 1-based position of the action in the feed, which is in game order. |
| `team_side` | character | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `source` | character | Feed the action came from: GENIUS_SPORTS in every sample. |
| `clock` | character | Time remaining in the period, 'MM:SS:CC' (minutes, seconds, hundredths): '09:56:00' = 9:56 left. |
| `action_type` | character | Action type (code table). |
| `sub_type` | character | Action subtype ('' when none; code table). |
| `qualifiers` | character | Semicolon-separated qualifiers ('' when none; code table). |
| `x` | numeric | Location along the court length, 0-100 percent (see the coordinate section): shots, and from 2020-21 fouls and turnovers; 0.0 when the row has no location. |
| `y` | numeric | Location across the court width, 0-100 percent (see the coordinate section); 0.0 when the row has no location. |
| `area` | character | Court zone of the action (code table): set on shots, and from 2020-21 on located fouls and turnovers; '' on rows without a location and on every shot of early 2015-16 games. |
| `side` | character | Basket end: LEFT (x≈0) or RIGHT (x≈100). Absent on non-shot rows before 2020-21 and on some 2020-21 rows (read absent on a shot as LEFT). |
| `success` | logical | True when the shot / free throw was made (true on non-shot rows). |
| `score1` | integer | First (home) team score after the action, a numeric STRING. |
| `score2` | integer | Second (away) team score after the action, a numeric STRING. |
| `period` | integer | Period number; overtime restarts at 1 with periodType OVERTIME. |
| `period_type` | character | REGULAR or OVERTIME. |
| `part` | character | Period label: QUARTER1-QUARTER4, then OT1 (OT2 and on by the same pattern, unobserved). |
| `match_team_player_id` | integer | Match-player id: the player's entry in this one match; NOT the player id. No captured route is keyed by it: join on `playerId` on the same row. |
| `player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `match_team_player_first_name` | character | The acting player's given name, as entered for this match. |
| `match_team_player_last_name` | character | The acting player's family name, as entered for this match. |
| `team_application_id` | integer | Team-application id: the team's registration (entry) for the season; play-by-play `teamApplicationId` is the same key space. |
| `team_name` | character | Team name for the season, sponsor included (e.g. 'Perspektiva Ilirija'). |
| `match_team_id` | integer | Match-team id: this team's entry in this one match (unique per match and side). Joins to play-by-play `matchTeamId`; NOT a team id. |
| `player_photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `shirt_number` | character | Shirt number worn by the acting player, sent as a string. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_pbp-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_pbp-example}

```python
kzs_pbp(match_id=101839)
```

_Last validated n/a._

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
| `match_id` | integer | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `team_side` | character | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. Players listed here are the first (home) team. |
| `first_name` | character | The home player's (as in the feed) given name. |
| `last_name` | character | The home player's (as in the feed) family name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_pbp_players-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_pbp_players-example}

```python
kzs_pbp_players(match_id=101839)
```

_Last validated n/a._

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
| `match_id` | integer | Match id: the federation key of a game, global across seasons (joins kzs_matches and kzs_match). |
| `team_side` | character | Which side of the match the row belongs to: home (the organiser, firstTeam on the wire) or away. |
| `team_id` | integer | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `team_name` | character | Name of the team for the season, sponsor included. |
| `minutes` | numeric | The player's minutes played, decimal minutes (39.67 = 39 min 40 s); the team line sums its players (225.0 = 5 x 45 with overtime). |
| `two_pm` | integer | The player's two-point field goals made. |
| `two_pa` | integer | The player's two-point field goals attempted. |
| `two_p_perc` | numeric | The player's two-point percentage, 0-100. |
| `three_pm` | integer | The player's three-point field goals made. |
| `three_pa` | integer | The player's three-point field goals attempted. |
| `three_p_perc` | numeric | The player's three-point percentage, 0-100. |
| `f_tm` | integer | The player's free throws made. |
| `f_ta` | integer | The player's free throws attempted. |
| `f_t_perc` | numeric | The player's free-throw percentage, 0-100. |
| `fg_m` | integer | The player's field goals made. |
| `fg_a` | integer | The player's field goals attempted. |
| `fg_perc` | numeric | The player's field-goal percentage, 0-100. |
| `offensive_rebounds` | integer | The player's offensive rebounds. |
| `defensive_rebounds` | integer | The player's defensive rebounds. |
| `total_rebounds` | integer | The player's total rebounds. |
| `assists` | integer | The player's assists. |
| `steals` | integer | The player's steals. |
| `turnovers` | integer | The player's turnovers. |
| `blocks_in_favor` | integer | The player's shots blocked (blocks made). |
| `blocks_against` | integer | The player's own shots that were blocked. |
| `foul_commited` | integer | The player's fouls committed. |
| `foul_received` | integer | The player's fouls drawn. |
| `p_ir` | integer | The player's PIR, performance index rating ((points + rebounds + assists + steals + blocks + fouls drawn) - (missed field goals + missed free throws + turnovers + shots blocked + fouls committed)). |
| `plus_minus` | integer | The player's plus-minus: points scored minus allowed while on court (team line: the final margin). |
| `assists_turnover_ratio` | numeric | The player's assist-to-turnover ratio (0 in every sample: not computed). |
| `defensive_rating` | numeric | The player's defensive rating (0 in every sample: not computed). |
| `efficiency` | integer | The player's efficiency as the feed computes it (equal to `pIR` on 2025-26 team lines). |
| `points` | integer | The player's points. |
| `efficiency_custom` | numeric | The player's custom efficiency as the federation computes it (equal to `efficiency` on player rows; 0 on team lines). |
| `efficiency_game_score` | numeric | The player's game score (0 in every sample: not computed). |
| `fast_break_points_attempted` | integer | The player's fast-break attempts (0 in every sample: not computed). |
| `fast_break_points_made` | integer | The player's fast-break makes (0 in every sample: not computed). |
| `usage_percentage` | numeric | The player's usage percentage (0 in every sample: not computed). |
| `m_vp_votes` | integer | The player's MVP votes (0 in every sample). |
| `minus` | integer | The player's points allowed on court (0 in every sample: not computed). |
| `offensive_rating` | numeric | The player's offensive rating (0 in every sample: not computed). |
| `p_er` | numeric | The player's PER (0 in every sample: not computed). |
| `plus` | integer | The player's points scored on court (0 in every sample: not computed). |
| `points_fast_break` | integer | The player's fast-break points. |
| `points_in_the_paint` | integer | The player's points in the paint. |
| `points_in_the_paint_attempted` | integer | The player's paint attempts (0 in every sample: not computed). |
| `points_in_the_paint_made` | integer | The player's paint makes (0 in every sample: not computed). |
| `points_second_chance` | integer | The player's second-chance points. |
| `second_chance_points_attempted` | integer | The player's second-chance attempts (0 in every sample: not computed). |
| `second_chance_points_made` | integer | The player's second-chance makes (0 in every sample: not computed). |
| `true_shooting_attempts` | integer | The player's true-shooting attempts (0 in every sample: not computed). |
| `true_shooting_percentage` | numeric | The player's true-shooting percentage (0 in every sample: not computed). |
| `player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `match_team_player_firstname` | character | The player's given name, as entered for this match (lower-case n in this route). |
| `match_team_player_lastname` | character | The player's family name, as entered for this match (lower-case n in this route). |
| `match_team_player_year_of_birth` | character | Birth year from the match entry, sent as a string ('2002'); the profile `person.yearOfBirth` is an integer. |
| `jersey_number` | character | Shirt number worn, sent as a string ('1', '55'). |
| `starting_lineup` | logical | True when the player started. |
| `captain` | logical | Games as captain. |
| `played` | logical | True when the player got on court. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_box_score-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_box_score-example}

```python
kzs_box_score(match_id=101839)
```

_Last validated n/a._

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
| `match_id` | integer | KZS match id. Joins to /matches/{matchId} (and its /playByPlay, /stats) and stats `matchId`. |
| `minute` | integer | Game minute (1-40, then overtime minutes). |
| `score1` | integer | First (home) team score at the end of the minute, an integer. |
| `score2` | integer | Second (away) team score at the end of the minute, an integer. |
| `total_minutes` | integer | Game length in minutes (40, 45 with one overtime). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_score_changes-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_score_changes-example}

```python
kzs_score_changes(match_id=101839)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `phase_id` | integer | Competition-phase id. Joins to `competitions[].competitionPhaseId` on matches and `/competitions/{competitionId}` `phases[].id`. |
| `phase_name` | character | Phase name, Slovenian (see `phases[].name`). |
| `phase_type` | character | Phase format (code table). |
| `group_id` | integer | Group id inside the phase (null when the table is the phase's own). |
| `group_name` | character | Group name inside the phase (null when the table is the phase's own). |
| `position` | integer | 1-based position of the row in its table, in the order the API lists it (the wire has no rank column). |
| `team_id` | integer | Team id: one club team in one season (Ilirija 2025-26 = 17551). Joins to /teams/{teamId}, standings `teamId`, box-score `teamId` and the `teamId` query params. |
| `team_name` | character | Team name for the season, sponsor included. |
| `club_code` | character | Club 3-letter code. |
| `logo_uuid` | character | Team logo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `games_played` | integer | Games played in the phase. |
| `won` | integer | Wins in the phase. |
| `lost` | integer | Losses in the phase. |
| `pts_plus` | integer | Points scored by the team through its games played. |
| `pts_minus` | integer | Points allowed. |
| `home_won` | integer | Home games the team has won. |
| `home_lost` | integer | Home games the team has lost. |
| `away_won` | integer | Away games the team has won. |
| `away_lost` | integer | Away games the team has lost. |
| `points` | integer | Table points: 2 per win, 1 per loss (Krka 26-1 = 53). |
| `draws` | integer | Draws (0: basketball has none). |
| `lost_points` | integer | Table points deducted (0 in every sample). |
| `tie_break_position` | integer | Final position after tie-breaks (absent on playoff-phase rows). |
| `pts_difference` | integer | Point differential (scored - allowed). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_standings-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_standings-example}

```python
kzs_standings(competition_id=579)
```

_Last validated n/a._

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
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `date` | character | Calendar date of the match day, 'YYYY-MM-DD'. |
| `no_of_games` | integer | Matches on that day. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_playing_days-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_playing_days-example}

```python
kzs_playing_days(competition_id=579)
```

_Last validated n/a._

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
| `team_id` | integer | Team id: one club team in one season (joins kzs_teams); not the match-team id and not the club id. |
| `competition_id` | integer | Competition id: one per competition per season (men's first league 2025/26 = 579, 2026/27 = 608). |
| `roster_entry_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `player_id` | integer | Player id (KZS player registry). Joins to /players/{playerId}, box-score and play-by-play `playerId`. |
| `identification_number` | integer | Player's KZS registration (licence) number; a different key space from the player id, used by no captured route. |
| `person_id` | integer | Person id: the federation's person record behind a player, coach or official; a different key space from player, coach and official ids. |
| `first_name` | character | The player's given name. |
| `last_name` | character | The player's family name. |
| `year_of_birth` | integer | Year of birth (players: public bio). |
| `nationality_id` | integer | Country id (FMS internal; use the ISO codes). |
| `nationality_alpha2_code` | character | ISO 3166-1 alpha-2 country code. |
| `nationality_alpha3_code` | character | ISO 3166-1 alpha-3 country code. |
| `nationality_name` | character | Country name in Slovenian (e.g. 'Združene države' = United States); use `engName` or the ISO codes. |
| `nationality_eng_name` | character | Country name in English. |
| `homegrown` | logical | True when the player counts as homegrown (domači igralec) for roster limits. |
| `photo_uuid` | character | Player photo: image file id, served at https://api.kzs.si/public/images/{uuid}. |
| `positions` | character | Unobserved: null, or an empty array, in every sample, so its type and meaning are unconfirmed. |
| `jersey_number` | character | Shirt number worn, sent as a string ('1', '55'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#kzs_roster-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the API has nothing for the request (an empty list, a competition without standings, a final game with no play-by-play or box score). |
| `sportsdataverse.errors.AssetFetchError` | the fetch failed or the answer is unknown or partial: HTTP 400 (the API's only error status: an unknown id or a bad parameter), a full list page, a game that is not final, an envelope whose status is not OK, or a body for another competition, match, team, player or season than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#kzs_roster-example}

```python
kzs_roster(competition_id=579, team_id=17551)
```

_Last validated n/a._
