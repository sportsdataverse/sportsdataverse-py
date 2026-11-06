# Sportradar Atrium — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Player

> Sportradar Atrium — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Player — function reference in sdv-py, the SportsDataverse Python package.

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the roster list. |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `name` | String | Player name as the site prints it. |
| `bib` | String | Jersey number, as text. |
| `entity_id` | String | Team UUID the player is rostered with that season. |
| `team_name` | String | Team name in the site's language. |
| `position_code` | String | Position code (C, PG, SF, ...), language-independent. |
| `position_name` | String | Position name in the site's language. |
| `nationality` | String | Player's nationality in the site's language (e.g. Belgique). |
| `date_of_birth` | Date | Player's date of birth. |
| `age` | Int64 | Player's age in years when the body was generated. |
| `height` | Int64 | Height as an integer, centimetres presumed (unit unverified); null when the site gives none. |
| `status` | String | Player status in the site's language (Actif, Blesse). |
| `image_url` | String | URL of the Sportradar-hosted image (400 px). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_players-example}

```python
atrium_players(website_id=12)
```

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `name` | String | Player name as the site prints it. |
| `bib` | String | Jersey number on the card; null when none. |
| `team_id` | String | Team UUID of the player's current team on the card. |
| `team_name` | String | Name of the player's current team on the card. |
| `image_url` | String | URL of the Sportradar-hosted image (400 px). |
| `date_of_birth` | Date | Player's date of birth, from the card's info items. |
| `age` | Int64 | Player's age in years, from the card's info items. |
| `nationality` | String | Player's nationality in the site's language, from the card's info items. |
| `points_per_game` | Float64 | Points per game over the season shown on the card. |
| `rebounds_offensive_per_game` | Float64 | Offensive rebounds per game over the season shown on the card. |
| `rebounds_defensive_per_game` | Float64 | Defensive rebounds per game over the season shown on the card. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player-example}

```python
atrium_player(person_id='4458f165-f865-11f0-8ebf-539c931c1654', website_id=12)
```

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `season_name` | String | Competition-season the line is for, in the site's language (Betclic ELITE - Playoffs 2025). |
| `season_year` | Int64 | The site's own year of that season (START year on LNB). |
| `season_end_year` | Int64 | Season END year of that season (sdv key); null when the site is unmapped. |
| `team_id` | String | Team UUID the player played for in that season. |
| `team_name` | String | Name of the team the player played for in that season. |
| `minutes_per_game` | String | Minutes per game as an ISO 8601 duration (player, that competition-season). |
| `points_per_game` | Float64 | Points per game (player, that competition-season). |
| `rebounds_per_game` | Float64 | Rebounds per game (player, that competition-season). |
| `rebounds_offensive_per_game` | Float64 | Offensive rebounds per game (player, that competition-season). |
| `rebounds_defensive_per_game` | Float64 | Defensive rebounds per game (player, that competition-season). |
| `assists_per_game` | Float64 | Assists per game (player, that competition-season). |
| `steals_per_game` | Float64 | Steals per game (player, that competition-season). |
| `blocks_per_game` | Float64 | Blocks per game (player, that competition-season). |
| `turnovers_per_game` | Float64 | Turnovers per game (player, that competition-season). |
| `fouls_personal_per_game` | Float64 | Personal fouls per game (player, that competition-season). |
| `dunks_per_game` | Float64 | Dunks per game (player, that competition-season). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (player, that competition-season). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player_seasons-example}

```python
atrium_player_seasons(person_id='4458f165-f865-11f0-8ebf-539c931c1654', website_id=12)
```

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `person_id` | String | Player UUID (person), shared across seasons and sites. |
| `season_id` | String | Season UUID the log is for (the season asked, else the site's default). |
| `date` | Datetime | Local start of the game, naive datetime (no offset; zone not given). |
| `opponent_entity_id` | String | Team UUID of the opponent. |
| `opponent_name` | String | Name of the opponent. |
| `result` | String | Result for the player's team: W or L. |
| `result_label` | String | Result label in the site's language (V, D). |
| `score` | String | Final score as the site's text (e.g. 88 - 91); which side is listed first is unverified, so it is not split. |
| `minutes` | String | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (player, that game). |
| `seconds_played` | Int64 | Time played in whole seconds, parsed from minutes (player, that game). |
| `points` | Int64 | Points scored (player, that game). |
| `rebounds` | Int64 | Total rebounds (player, that game). |
| `rebounds_offensive` | Int64 | Offensive rebounds (player, that game). |
| `rebounds_defensive` | Int64 | Defensive rebounds (player, that game). |
| `assists` | Int64 | Assists (player, that game). |
| `steals` | Int64 | Steals (player, that game). |
| `blocks` | Int64 | Shots blocked (player, that game). |
| `blocks_received` | Int64 | Own shots blocked by an opponent (LNB only) (player, that game). |
| `turnovers` | Int64 | Turnovers (player, that game). |
| `fouls_total` | Int64 | Fouls committed (player, that game). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against the player or team; LNBP only) (player, that game). |
| `plus_minus` | Int64 | Plus/minus, the point differential while on court (LNB only; null in some team lines) (player, that game). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (player, that game). |
| `field_goals_made` | Int64 | Field goals made, two- and three-pointers together (LNBP only) (player, that game). |
| `field_goals_attempted` | Int64 | Field goals attempted (LNBP only) (player, that game). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (player, that game). |
| `points_two_made` | Int64 | Two-point field goals made (player, that game). |
| `points_two_attempted` | Int64 | Two-point field goals attempted (player, that game). |
| `points_two_percentage` | Float64 | Two-point percentage, 0-100 (player, that game). |
| `points_three_made` | Int64 | Three-point field goals made (player, that game). |
| `points_three_attempted` | Int64 | Three-point field goals attempted (player, that game). |
| `points_three_percentage` | Float64 | Three-point percentage, 0-100 (player, that game). |
| `free_throws_made` | Int64 | Free throws made (player, that game). |
| `free_throws_attempted` | Int64 | Free throws attempted (player, that game). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (player, that game). |
| `points_fast_break` | Int64 | Fast-break points (LNBP only) (player, that game). |
| `dunks` | Int64 | Dunks (player, that game). |
| `fouls_personal` | Int64 | Personal fouls (player, that game). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player_game_log-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player_game_log-example}

```python
atrium_player_game_log(person_id='4458f165-f865-11f0-8ebf-539c931c1654', website_id=12)
```

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
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the table; null for the competition-scoped routes (their bodies carry none). |
| `table_key` | String | Statistics table the row is from: averages, shootingstatistics, totals, ... (the set is per site). |
| `table_label` | String | The table's heading in the site's language (e.g. Moyennes). |
| `person_id` | String | Player UUID decoded from the widget link beside the name (the tables carry names only); null when the link is missing or undecodable, and always null in team and competition bodies. |
| `person_name` | String | Player name as the site prints it; null in team tables. |
| `entity_id` | String | Team UUID decoded from the widget link (team tables only; player tables name the team but carry no id). |
| `entity_name` | String | Team name as the site prints it; for player rows of a stat table it is the player's team. |
| `stat_key` | String | Statistic key as the API names it (pointsPerGame, fieldGoalsMade, ...), unchanged from header key. |
| `stat_name` | String | Short column heading the site shows for the statistic (PTS, REB), in the site's language. |
| `stat_label` | String | Longer statistic label the site shows (code in the API), in the site's language. |
| `value` | Float64 | Numeric value of the statistic; null when the API sends a duration or text (see value_text). |
| `value_text` | String | Non-numeric statistic value, e.g. an ISO 8601 duration such as PT12M2S for minutes per game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_player_stats-example}

```python
atrium_player_stats(website_id=12)
```
