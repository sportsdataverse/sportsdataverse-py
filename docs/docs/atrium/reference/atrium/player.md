---
title: "ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Player"
sidebar_label: "Player"
sidebar_position: 1
description: "ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Player

## atrium_players

The players of a season, with position, nationality, birth date, age and height.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/persons`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/persons](https://embed-api.eui.connect.sportradar.com/v1/embed/12/persons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `entityId` | `entity_id` |  |  | `Y` | Team UUID (entity_id in atrium_teams or any fixture row). |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID of the roster list. |
| `person_id` | character | Player UUID (person), shared across seasons and sites. |
| `name` | character | Player name as the site prints it. |
| `bib` | character | Jersey number, as text. |
| `entity_id` | character | Team UUID the player is rostered with that season. |
| `team_name` | character | Team name in the site's language. |
| `position_code` | character | Position code (C, PG, SF, ...), language-independent. |
| `position_name` | character | Position name in the site's language. |
| `nationality` | character | Player's nationality in the site's language (e.g. Belgique). |
| `date_of_birth` | date | Player's date of birth. |
| `age` | integer | Player's age in years when the body was generated. |
| `height` | integer | Height as an integer, centimetres presumed (unit unverified); null when the site gives none. |
| `status` | character | Player status in the site's language (Actif, Blesse). |
| `image_url` | character | URL of the Sportradar-hosted image (400 px). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_players-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_players-example}

```python
atrium_players(website_id=12)
```

_Last validated n/a._

## atrium_player

A player page: bio and the season averages the card shows.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/person_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/person_detail?personId=4458f165-f865-11f0-8ebf-539c931c1654](https://embed-api.eui.connect.sportradar.com/v1/embed/12/person_detail?personId=4458f165-f865-11f0-8ebf-539c931c1654)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `personId` | `person_id` |  | `Y` |  | Player UUID (person_id in atrium_players or a box score), e.g. 4458f165-f865-11f0-8ebf-539c931c1654. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `person_id` | character | Player UUID (person), shared across seasons and sites. |
| `name` | character | Player name as the site prints it. |
| `bib` | character | Jersey number on the card; null when none. |
| `team_id` | character | Team UUID of the player's current team on the card. |
| `team_name` | character | Name of the player's current team on the card. |
| `image_url` | character | URL of the Sportradar-hosted image (400 px). |
| `date_of_birth` | date | Player's date of birth, from the card's info items. |
| `age` | integer | Player's age in years, from the card's info items. |
| `nationality` | character | Player's nationality in the site's language, from the card's info items. |
| `points_per_game` | numeric | Points per game over the season shown on the card. |
| `rebounds_offensive_per_game` | numeric | Offensive rebounds per game over the season shown on the card. |
| `rebounds_defensive_per_game` | numeric | Defensive rebounds per game over the season shown on the card. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player-example}

```python
atrium_player(person_id='4458f165-f865-11f0-8ebf-539c931c1654', website_id=12)
```

_Last validated n/a._

## atrium_player_seasons

A player's per-game averages, one row per competition-season.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/person_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/person_detail?personId=4458f165-f865-11f0-8ebf-539c931c1654](https://embed-api.eui.connect.sportradar.com/v1/embed/12/person_detail?personId=4458f165-f865-11f0-8ebf-539c931c1654)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `personId` | `person_id` |  | `Y` |  | Player UUID (person_id in atrium_players or a box score), e.g. 4458f165-f865-11f0-8ebf-539c931c1654. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `person_id` | character | Player UUID (person), shared across seasons and sites. |
| `season_name` | character | Competition-season the line is for, in the site's language (Betclic ELITE - Playoffs 2025). |
| `season_year` | integer | The site's own year of that season (START year on LNB). |
| `season_end_year` | integer | Season END year of that season (sdv key); null when the site is unmapped. |
| `team_id` | character | Team UUID the player played for in that season. |
| `team_name` | character | Name of the team the player played for in that season. |
| `minutes_per_game` | character | Minutes per game as an ISO 8601 duration (player, that competition-season). |
| `points_per_game` | numeric | Points per game (player, that competition-season). |
| `rebounds_per_game` | numeric | Rebounds per game (player, that competition-season). |
| `rebounds_offensive_per_game` | numeric | Offensive rebounds per game (player, that competition-season). |
| `rebounds_defensive_per_game` | numeric | Defensive rebounds per game (player, that competition-season). |
| `assists_per_game` | numeric | Assists per game (player, that competition-season). |
| `steals_per_game` | numeric | Steals per game (player, that competition-season). |
| `blocks_per_game` | numeric | Blocks per game (player, that competition-season). |
| `turnovers_per_game` | numeric | Turnovers per game (player, that competition-season). |
| `fouls_personal_per_game` | numeric | Personal fouls per game (player, that competition-season). |
| `dunks_per_game` | numeric | Dunks per game (player, that competition-season). |
| `efficiency` | integer | Efficiency rating, from the site's own formula (player, that competition-season). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player_seasons-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player_seasons-example}

```python
atrium_player_seasons(person_id='4458f165-f865-11f0-8ebf-539c931c1654', website_id=12)
```

_Last validated n/a._

## atrium_player_game_log

A player's game log: one row per game with the opponent, result and the statistics the site shows.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/person_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/person_detail?sub=log&personId=4458f165-f865-11f0-8ebf-539c931c1654](https://embed-api.eui.connect.sportradar.com/v1/embed/12/person_detail?sub=log&personId=4458f165-f865-11f0-8ebf-539c931c1654)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `personId` | `person_id` |  | `Y` |  | Player UUID (person_id in atrium_players or a box score), e.g. 4458f165-f865-11f0-8ebf-539c931c1654. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_player_game_log-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `person_id` | character | Player UUID (person), shared across seasons and sites. |
| `season_id` | character | Season UUID the log is for (the season asked, else the site's default). |
| `date` | datetime | Local start of the game, naive datetime (no offset; zone not given). |
| `opponent_entity_id` | character | Team UUID of the opponent. |
| `opponent_name` | character | Name of the opponent. |
| `result` | character | Result for the player's team: W or L. |
| `result_label` | character | Result label in the site's language (V, D). |
| `score` | character | Final score as the site's text (e.g. 88 - 91); which side is listed first is unverified, so it is not split. |
| `minutes` | character | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, that game). |
| `seconds_played` | integer | Time played in whole seconds, parsed from minutes (player, that game). |
| `points` | integer | Points scored (player, that game). |
| `rebounds` | integer | Total rebounds (player, that game). |
| `rebounds_offensive` | integer | Offensive rebounds (player, that game). |
| `rebounds_defensive` | integer | Defensive rebounds (player, that game). |
| `assists` | integer | Assists (player, that game). |
| `steals` | integer | Steals (player, that game). |
| `blocks` | integer | Shots blocked (player, that game). |
| `blocks_received` | integer | Own shots blocked by an opponent (LNB only) (player, that game). |
| `turnovers` | integer | Turnovers (player, that game). |
| `fouls_total` | integer | Fouls committed (player, that game). |
| `fouls_drawn` | integer | Fouls drawn (committed against the player or team; LNBP only) (player, that game). |
| `plus_minus` | integer | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, that game). |
| `efficiency` | integer | Efficiency rating, from the site's own formula (player, that game). |
| `field_goals_made` | integer | Field goals made, two- and three-pointers together (LNBP only) (player, that game). |
| `field_goals_attempted` | integer | Field goals attempted (LNBP only) (player, that game). |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (player, that game). |
| `points_two_made` | integer | Two-point field goals made (player, that game). |
| `points_two_attempted` | integer | Two-point field goals attempted (player, that game). |
| `points_two_percentage` | numeric | Two-point percentage, 0-100 (player, that game). |
| `points_three_made` | integer | Three-point field goals made (player, that game). |
| `points_three_attempted` | integer | Three-point field goals attempted (player, that game). |
| `points_three_percentage` | numeric | Three-point percentage, 0-100 (player, that game). |
| `free_throws_made` | integer | Free throws made (player, that game). |
| `free_throws_attempted` | integer | Free throws attempted (player, that game). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (player, that game). |
| `points_fast_break` | integer | Fast-break points (LNBP only) (player, that game). |
| `dunks` | integer | Dunks (player, that game). |
| `fouls_personal` | integer | Personal fouls (player, that game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player_game_log-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player_game_log-example}

```python
atrium_player_game_log(person_id='4458f165-f865-11f0-8ebf-539c931c1654', website_id=12)
```

_Last validated n/a._

## atrium_player_stats

Player season statistics in long form (one row per player, table and statistic); ids decoded from the widget links.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/statistics_persons`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_persons](https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_persons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID of the table; null for the competition-scoped routes (their bodies carry none). |
| `table_key` | character | Statistics table the row is from: averages, shootingstatistics, totals, ... (the set is per site). |
| `table_label` | character | The table's heading in the site's language (e.g. Moyennes). |
| `person_id` | character | Player UUID decoded from the widget link beside the name (the tables carry names only); null when the link is missing or undecodable, and always null in team and competition bodies. |
| `person_name` | character | Player name as the site prints it; null in team tables. |
| `entity_id` | character | Team UUID decoded from the widget link (team tables only; player tables name the team but carry no id). |
| `entity_name` | character | Team name as the site prints it; for player rows of a stat table it is the player's team. |
| `stat_key` | character | Statistic key as the API names it (pointsPerGame, fieldGoalsMade, ...), unchanged from header key. |
| `stat_name` | character | Short column heading the site shows for the statistic (PTS, REB), in the site's language. |
| `stat_label` | character | Longer statistic label the site shows (code in the API), in the site's language. |
| `value` | numeric | Numeric value of the statistic; null when the API sends a duration or text (see value_text). |
| `value_text` | character | Non-numeric statistic value, e.g. an ISO 8601 duration such as PT12M2S for minutes per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player_stats-example}

```python
atrium_player_stats(website_id=12)
```

_Last validated n/a._
