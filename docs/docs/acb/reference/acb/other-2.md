---
title: "ACB — ACB API (api2.acb.com, Spain) — Other: calendar–search"
sidebar_label: "Other: calendar–search"
sidebar_position: 6
description: "ACB — ACB API (api2.acb.com, Spain) — Other: calendar–search — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Other: calendar–search

## acb_calendar

Every round of a season with its games: one call lists every game id.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Calendar/season-calendar`

**Valid URL:** [https://api2.acb.com/api/seasondata/Calendar/season-calendar?season=2027](https://api2.acb.com/api/seasondata/Calendar/season-calendar?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `round_id` | Int64 | Global round id (6017 = Liga 2026-27 round 3); not the round number. |
| `round_number` | Int64 | Round number within its phase. |
| `subphase_id` | Int64 | Sub-phase id of the round (playoff stages); null in the regular season. |
| `subphase_number` | Int64 | Sub-phase number of the round; null in the regular season. |
| `week_id` | Int64 | Not sent by the calendar route (always null). |
| `match_id` | Int64 | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `home_team_id` | Int64 | Home season-team entry id (new every season; joins the body's team list). |
| `away_team_id` | Int64 | Away season-team entry id (new every season; joins the body's team list). |
| `home_score` | Int64 | Home points (null before the match) |
| `away_score` | Int64 | Away points (null before the match) |
| `start` | String | Tip-off, ISO 8601 UTC |
| `status` | String | Match state (x-match-statuses) |
| `is_on_half_time` | Boolean | At half time (live) |
| `current_quarter` | Int64 | Current or last quarter (5+ = overtime); not reliable after the match (1 in the 2017 series rows) |
| `home_club_id` | Int64 | Stable club id of the home team (looked up in the body's team list). |
| `home_team` | String | Home team name for the season (looked up in the body's team list). |
| `home_team_abbr` | String | Home team three-letter abbreviation (looked up in the body's team list). |
| `away_club_id` | Int64 | Stable club id of the away team (looked up in the body's team list). |
| `away_team` | String | Away team name for the season (looked up in the body's team list). |
| `away_team_abbr` | String | Away team three-letter abbreviation (looked up in the body's team list). |
| `ticketing_link` | String | Ticket shop URL; null when none. |
| `broadcasters` | String | Broadcast channel names of the game joined with '; ' (the first is the primary). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_calendar-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_calendar-example}

```python
acb_calendar(season=2027)
```

## acb_league_zones

League-wide shooting by court zone for a season.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/ShotChart/league-zones`

**Valid URL:** [https://api2.acb.com/api/seasondata/ShotChart/league-zones?season=2027](https://api2.acb.com/api/seasondata/ShotChart/league-zones?season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_league_zones-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa. |
| `edition_id` | Int64 | Season id (editionId) = season START year - 1935 (91 = 2026-27). The wrappers take the END year, `season`. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `phase_id` | Int64 | Phase the statistics cover; null in the sample (read as all phases, unverified). |
| `zone` | String | Court zone (acb_codes.SHOT_ZONES): restricted_area, paint, mid_range_*, corner_three_*, wing_three_*, top_three. |
| `attempts` | Int64 | Field-goal attempts in the zone, league-wide. |
| `made` | Int64 | Field goals made in the zone, league-wide. |
| `points_scored` | Int64 | Points scored from the zone, league-wide. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_league_zones-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_league_zones-example}

```python
acb_league_zones(season=2027)
```

## acb_next_matches

A club's most recent and upcoming games.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/next-matches`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/next-matches?clubId=2&numberOfMatches=5](https://api2.acb.com/api/seasondata/TeamProfile/next-matches?clubId=2&numberOfMatches=5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |
| `numberOfMatches` | `number_of_matches` |  |  | `Y` | How many games to return (the site asks for 5). |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_next_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `match_id` | Int64 | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `round_type` | String | Phase code: LR = Liga Regular (regular season), PO = playoffs (a bundle value, not in the samples); null on every row of /Menu/competition-data and /TeamProfile/next-matches |
| `round_id` | Int64 | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | Int64 | Round number within the phase |
| `group_id` | Int64 | Group id in group-stage competitions |
| `subphase_id` | Int64 | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | Int64 | Sub-phase number |
| `week_id` | Int64 | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `status` | String | Match state (x-match-statuses) |
| `is_on_half_time` | Boolean | At half time (live) |
| `current_quarter` | Int64 | Current or last quarter (0 before the match and in some finished rows) |
| `home_score` | Int64 | Home points (null before the match) |
| `away_score` | Int64 | Away points (null before the match) |
| `start` | String | Tip-off, ISO 8601 UTC |
| `home_wins` | Int64 | Home team's wins so far |
| `home_losses` | Int64 | Home team's losses so far |
| `away_wins` | Int64 | Away team's wins so far |
| `away_losses` | Int64 | Away team's losses so far |
| `home_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | String | Team name that season (sponsor names change) |
| `home_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | String | Team name that season (sponsor names change) |
| `away_team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | Int64 | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | Boolean | Not sent by this route (always null). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_next_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_next_matches-example}

```python
acb_next_matches(club_id=2, number_of_matches=5)
```

## acb_club_data

Club profile: arena, contacts and links.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/club-data`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/club-data?clubId=2](https://api2.acb.com/api/seasondata/TeamProfile/club-data?clubId=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |

### Returns {#acb_club_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `team` | String | Team name that season (sponsor names change) |
| `team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | Int64 | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `stadium_name` | String | Arena name (the club's home venue). |
| `stadium_address` | String | Arena street address. |
| `stadium_capacity` | Int64 | Arena capacity. |
| `president` | String | Club president's name; null for most clubs. |
| `foundation_year` | Int64 | Year the club was founded. |
| `web_url` | String | Club website URL. |
| `ticketing_url` | String | Club ticket shop URL. |
| `merchandising_url` | String | Club merchandise shop URL (the API's merchanUrl). |
| `instagram_url` | String | Club Instagram URL. |
| `twitter_url` | String | Club X / Twitter URL. |
| `facebook_url` | String | Club Facebook URL. |
| `youtube_url` | String | Club YouTube URL. |
| `tiktok_url` | String | Club TikTok URL. |
| `validated` | Boolean | True when the club profile is validated by the league. |
| `temporary_roster` | Boolean | True while the club's roster is provisional. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_club_data-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_club_data-example}

```python
acb_club_data(club_id=2)
```

## acb_current_roster

A club's players and staff for an edition (default: the current one).

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/TeamProfile/current-roster`

**Valid URL:** [https://api2.acb.com/api/seasondata/TeamProfile/current-roster?clubId=2&season=2027](https://api2.acb.com/api/seasondata/TeamProfile/current-roster?clubId=2&season=2027)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `clubId` | `club_id` |  | `Y` |  | Required. ACB club id (integer), stable across seasons and sponsor names (2 = Barca, 9 = Real Madrid, 13 = Valencia Basket). Not the season-team ``team_id``. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (youth). Omitted = the API's default competition. |
| `season` | `season` |  |  | `Y` | Season END year (2027 = 2026-27; the first ACB season, 1983-84, is 1984). ``acb_runtime`` sends the API's ``editionId`` = season - 1936. Omitted = the current season. |

### Returns {#acb_current_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `club_id` | Int64 | Stable club id asked for (the body names no club). |
| `role` | String | player or staff (head coach and assistants). |
| `person_id` | Int64 | ACB licence number of the person; a player's player_id elsewhere. |
| `person_name` | String | Display name as the site shows it. |
| `first_name` | String | First name(s); null when the API does not send it. |
| `last_name` | String | Last name(s); null when the API does not send it. |
| `shirt_number` | String | Shirt number as a string; null for staff. |
| `game_role` | String | Position or staff role in Spanish (Base, Escolta, Alero, Ala-pivot, Pivot; Entrenador, Segundo Entrenador). |
| `licensing` | String | Licence code (for example EUR, JFL, CRE); meaning unverified. |
| `age` | Int64 | Age in years as sent by the roster route. |
| `nationality` | String | Nationality as sent, a Spanish country name ('Serbia', 'EE.UU.'). |
| `nationality_iso` | String | ISO 3166-1 alpha-2 code of nationality, from the 31 observed names; null for an unseen name. |
| `height_cm` | Int64 | Height in centimetres (players only). |
| `is_license_active` | Boolean | True when the licence is active in the current season. |
| `edition_id` | Int64 | Season id the person record was read for (START year - 1935). |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `temporary_roster` | Boolean | True while the roster is provisional. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_current_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_current_roster-example}

```python
acb_current_roster(club_id=2, season=2027)
```

## acb_career_stints

A player's club stints, one row per stint.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/PlayerProfile/career`

**Valid URL:** [https://api2.acb.com/api/seasondata/PlayerProfile/career?playerLicenceId=20209407](https://api2.acb.com/api/seasondata/PlayerProfile/career?playerLicenceId=20209407)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerLicenceId` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |

### Returns {#acb_career_stints-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | ACB licence number asked for. |
| `initial_year` | Int64 | Year the stint starts. |
| `end_year` | Int64 | Year the stint ends (the season's END year). |
| `team_id` | Int64 | Team entry of the stint's latest edition. |
| `club_id` | Int64 | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `team` | String | Team name that season (sponsor names change) |
| `team_abbr` | String | Three-letter abbreviation (BAR, RMB) |
| `edition_id` | Int64 | Season id of the stint's team entry (START year - 1935). |
| `n_editions` | Int64 | Editions the stint spans. |
| `has_no_validated_data` | Boolean | True when the stint has no validated data. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_career_stints-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_career_stints-example}

```python
acb_career_stints(player_id=20209407)
```

## acb_career_trophies

A player's trophies, one row per competition.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/PlayerProfile/career`

**Valid URL:** [https://api2.acb.com/api/seasondata/PlayerProfile/career?playerLicenceId=20209407](https://api2.acb.com/api/seasondata/PlayerProfile/career?playerLicenceId=20209407)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `playerLicenceId` | `player_id` |  | `Y` |  | Required. ACB licence number (integer): the person id everywhere (box score, play-by-play, shots, rosters). 20209407 = Edy Tavares, 20212243 = Olek Balcerowski. |

### Returns {#acb_career_trophies-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_id` | Int64 | ACB licence number asked for. |
| `competition_id` | Int64 | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `times_champion` | Int64 | Times the player won the competition. |
| `times_runner_up` | Int64 | Times the player was runner-up in the competition. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_career_trophies-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_career_trophies-example}

```python
acb_career_trophies(player_id=20209407)
```

## acb_search

Search players, teams and coaches by name.

**Endpoint URL:** `GET https://api2.acb.com/api/seasondata/Search/search`

**Valid URL:** [https://api2.acb.com/api/seasondata/Search/search?searchTerm=tavares&limit=5](https://api2.acb.com/api/seasondata/Search/search?searchTerm=tavares&limit=5)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `searchTerm` | `search_term` |  | `Y` |  | Required. Name fragment (accent-insensitive on the player rows); a blank term is refused. |
| `limit` | `limit` |  |  | `Y` | Maximum results per kind (players, teams, coaches). |

### Returns {#acb_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `kind` | String | player, team or coach. |
| `person_id` | Int64 | ACB licence number (players, coaches) or the team entry id (teams). |
| `name` | String | Display name (nickname) of a player or coach; the team name of a team. |
| `short_name` | String | Short display name ('T. Martin'). |
| `first_name` | String | Legal first name(s) |
| `last_name` | String | Legal last name(s) |
| `shirt_number` | String | Shirt number, a string ('' or null when unknown) |
| `game_role` | String | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `is_license_active` | Boolean | The licence is active in the current season |
| `club_id` | Int64 | Stable club id of a team hit; null for players. |
| `edition_id` | Int64 | Season id of the record; null in search results. |
| `season` | Int64 | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Raises {#acb_search-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The ACB has nothing for the request (an unknown id, play-by-play or shots before 2016-17, a club with no games that season); a game outside a playoff series is zero rows. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed or the answer is not the one asked for (a rejected key, an error envelope or stub, a body for another season, round, club or player, a game that is not finished). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. A missing or non-integer id, or a season before 1984 (raised before any request), or the API's HTTP 400 / 422. |

### Example {#acb_search-example}

```python
acb_search(limit=5, search_term='tavares')
```
