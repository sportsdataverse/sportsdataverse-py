---
title: "ACB — ACB API (api2.acb.com, Spain) — Other: next_matches–search"
sidebar_label: "Other: next_matches–search"
sidebar_position: 5
description: "ACB — ACB API (api2.acb.com, Spain) — Other: next_matches–search — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ACB — ACB API (api2.acb.com, Spain) — Other: next_matches–search

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
| `match_id` | integer | Match id. Legacy ids (seasons up to 2019-20) sit below about 20000 (12559 = 2005-06 final, 17718 = 2016-17 final); from 2020-21 they start at about 100000 (102920 in 2021-22, 105401 in 2026-27). One key space across competitions: the matchId param of every matchdata route. |
| `round_type` | character | Phase code: LR = Liga Regular (regular season), PO = playoffs (a bundle value, not in the samples); null on every row of /Menu/competition-data and /TeamProfile/next-matches |
| `round_id` | integer | Round id: global across seasons, not the round number (6017 = Liga 2026-27 round 3; 2600 = 2016-17 round 1) |
| `round_number` | integer | Round number within the phase |
| `group_id` | integer | Group id in group-stage competitions |
| `subphase_id` | integer | Sub-phase id inside a phase (a playoff round or a cup round, e.g. 291, 292, 293); null in the regular season |
| `subphase_number` | integer | Sub-phase number |
| `week_id` | integer | Week id: global across seasons (2989 = the week of 2026-10-10) |
| `status` | character | Match state (x-match-statuses) |
| `is_on_half_time` | logical | At half time (live) |
| `current_quarter` | integer | Current or last quarter (0 before the match and in some finished rows) |
| `home_score` | integer | Home points (null before the match) |
| `away_score` | integer | Away points (null before the match) |
| `start` | character | Tip-off, ISO 8601 UTC |
| `home_wins` | integer | Home team's wins so far |
| `home_losses` | integer | Home team's losses so far |
| `away_wins` | integer | Away team's wins so far |
| `away_losses` | integer | Away team's losses so far |
| `home_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `home_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `home_team` | character | Team name that season (sponsor names change) |
| `home_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `away_team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `away_club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `away_team` | character | Team name that season (sponsor names change) |
| `away_team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | integer | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `is_selected_local` | logical | Not sent by this route (always null). |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_next_matches-example}

```python
acb_next_matches(club_id=2, number_of_matches=5)
```

_Last validated n/a._

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
| `team_id` | integer | Season-team id: one team's entry in one competition edition (4467 = Barça in Liga 2026-27, 4388 = Real Madrid in Liga 2025-26). Changes every season and competition. Joins every `homeTeamId` / `awayTeamId` / `teamId` / `teamEditionId`. Not the club id. |
| `club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `team` | character | Team name that season (sponsor names change) |
| `team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `edition_id` | integer | Season id (`editionId`) = season START year - 1935 (91 = 2026-27, 81 = 2016-17, 70 = 2005-06, 48 = 1983-84, the first). The sdv season (END year) is editionId + 1936. Season lists also carry `seasonStartYear` / `seasonEndYear` |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `stadium_name` | character | Arena name (the club's home venue). |
| `stadium_address` | character | Arena street address. |
| `stadium_capacity` | integer | Arena capacity. |
| `president` | character | Club president's name; null for most clubs. |
| `foundation_year` | integer | Year the club was founded. |
| `web_url` | character | Club website URL. |
| `ticketing_url` | character | Club ticket shop URL. |
| `merchandising_url` | character | Club merchandise shop URL (the API's merchanUrl). |
| `instagram_url` | character | Club Instagram URL. |
| `twitter_url` | character | Club X / Twitter URL. |
| `facebook_url` | character | Club Facebook URL. |
| `youtube_url` | character | Club YouTube URL. |
| `tiktok_url` | character | Club TikTok URL. |
| `validated` | logical | True when the club profile is validated by the league. |
| `temporary_roster` | logical | True while the club's roster is provisional. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_club_data-example}

```python
acb_club_data(club_id=2)
```

_Last validated n/a._

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
| `club_id` | integer | Stable club id asked for (the body names no club). |
| `role` | character | player or staff (head coach and assistants). |
| `person_id` | integer | ACB licence number of the person; a player's player_id elsewhere. |
| `person_name` | character | Display name as the site shows it. |
| `first_name` | character | First name(s); null when the API does not send it. |
| `last_name` | character | Last name(s); null when the API does not send it. |
| `shirt_number` | character | Shirt number as a string; null for staff. |
| `game_role` | character | Position or staff role in Spanish (Base, Escolta, Alero, Ala-pivot, Pivot; Entrenador, Segundo Entrenador). |
| `licensing` | character | Licence code (for example EUR, JFL, CRE); meaning unverified. |
| `age` | integer | Age in years as sent by the roster route. |
| `nationality` | character | Nationality as sent, a Spanish country name ('Serbia', 'EE.UU.'). |
| `nationality_iso` | character | ISO 3166-1 alpha-2 code of nationality, from the 31 observed names; null for an unseen name. |
| `height_cm` | integer | Height in centimetres (players only). |
| `is_license_active` | logical | True when the licence is active in the current season. |
| `edition_id` | integer | Season id the person record was read for (START year - 1935). |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |
| `temporary_roster` | logical | True while the roster is provisional. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_current_roster-example}

```python
acb_current_roster(club_id=2, season=2027)
```

_Last validated n/a._

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
| `player_id` | integer | ACB licence number asked for. |
| `initial_year` | integer | Year the stint starts. |
| `end_year` | integer | Year the stint ends (the season's END year). |
| `team_id` | integer | Team entry of the stint's latest edition. |
| `club_id` | integer | Club id, stable across seasons and sponsor names (2 = Barça, 9 = Real Madrid, 13 = Valencia Basket): the `clubId` request param and every `homeClubId` / `awayClubId`. |
| `team` | character | Team name that season (sponsor names change) |
| `team_abbr` | character | Three-letter abbreviation (BAR, RMB) |
| `edition_id` | integer | Season id of the stint's team entry (START year - 1935). |
| `n_editions` | integer | Editions the stint spans. |
| `has_no_validated_data` | logical | True when the stint has no validated data. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_career_stints-example}

```python
acb_career_stints(player_id=20209407)
```

_Last validated n/a._

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
| `player_id` | integer | ACB licence number asked for. |
| `competition_id` | integer | Competition id: 1 Liga Endesa, 2 Copa del Rey, 3 Supercopa Endesa, 10 Minicopa Endesa (x-competitions) |
| `times_champion` | integer | Times the player won the competition. |
| `times_runner_up` | integer | Times the player was runner-up in the competition. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_career_trophies-example}

```python
acb_career_trophies(player_id=20209407)
```

_Last validated n/a._

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
| `kind` | character | player, team or coach. |
| `person_id` | integer | ACB licence number (players, coaches) or the team entry id (teams). |
| `name` | character | Display name (nickname) of a player or coach; the team name of a team. |
| `short_name` | character | Short display name ('T. Martin'). |
| `first_name` | character | Legal first name(s) |
| `last_name` | character | Legal last name(s) |
| `shirt_number` | character | Shirt number, a string ('' or null when unknown) |
| `game_role` | character | Position or staff role, in Spanish: Base (point guard), Escolta (shooting guard), Alero (small forward), Ala-pivot (power forward), Pivot (centre); Entrenador (head coach), Segundo Entrenador / Entrenador Ayudante (assistant coach). Null or '' in many rows |
| `is_license_active` | logical | The licence is active in the current season |
| `club_id` | integer | Stable club id of a team hit; null for players. |
| `edition_id` | integer | Season id of the record; null in search results. |
| `season` | integer | Season END year (2027 = 2026-27) = edition_id + 1936: the key the wrappers take. |

**`return_parsed=False`** — the raw body (`dict`): the route's JSON plus `_request` (the ids asked for, in snake_case, with `season` the END year).

### Example {#acb_search-example}

```python
acb_search(limit=5, search_term='tavares')
```

_Last validated n/a._
