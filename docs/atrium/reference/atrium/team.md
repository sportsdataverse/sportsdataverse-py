# Sportradar Atrium — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Team

> Sportradar Atrium — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Team — function reference in sdv-py, the SportsDataverse Python package.

## atrium_team_box_score

One game's team lines: the team totals, the team-level counts and the game aggregates (lead changes, biggest lead).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/fixture_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/fixture_detail?sub=statistics&fixtureId=5423072b-6715-11f0-bd8b-9ffadebc148f](https://embed-api.eui.connect.sportradar.com/v1/embed/12/fixture_detail?sub=statistics&fixtureId=5423072b-6715-11f0-bd8b-9ffadebc148f)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `fixtureId` | `fixture_id` |  | `Y` |  | Game UUID (fixture_id in atrium_fixtures), e.g. 0dc631bd-c34c-11f1-8d16-4d5a21afb79d. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_team_box_score-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | String | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | String | Season UUID of the game. |
| `entity_id` | String | Team UUID (from the fixture's isHome flag). |
| `side` | String | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `minutes` | String | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (team, this game). |
| `seconds_played` | Int64 | Time played in whole seconds, parsed from minutes (team, this game). |
| `points` | Int64 | Points scored (team, this game). |
| `rebounds` | Int64 | Total rebounds (team, this game). |
| `rebounds_offensive` | Int64 | Offensive rebounds (team, this game). |
| `rebounds_defensive` | Int64 | Defensive rebounds (team, this game). |
| `assists` | Int64 | Assists (team, this game). |
| `steals` | Int64 | Steals (team, this game). |
| `blocks` | Int64 | Shots blocked (team, this game). |
| `blocks_received` | Int64 | Own shots blocked by an opponent (LNB only) (team, this game). |
| `turnovers` | Int64 | Turnovers (team, this game). |
| `fouls_total` | Int64 | Fouls committed (team, this game). |
| `fouls_drawn` | Int64 | Fouls drawn (committed against the player or team; LNBP only) (team, this game). |
| `plus_minus` | Int64 | Plus/minus, the point differential while on court (LNB only; null in some team lines) (team, this game). |
| `efficiency` | Int64 | Efficiency rating, from the site's own formula (team, this game). |
| `field_goals_made` | Int64 | Field goals made, two- and three-pointers together (LNBP only) (team, this game). |
| `field_goals_attempted` | Int64 | Field goals attempted (LNBP only) (team, this game). |
| `field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (team, this game). |
| `points_two_made` | Int64 | Two-point field goals made (team, this game). |
| `points_two_attempted` | Int64 | Two-point field goals attempted (team, this game). |
| `points_two_percentage` | Float64 | Two-point percentage, 0-100 (team, this game). |
| `points_three_made` | Int64 | Three-point field goals made (team, this game). |
| `points_three_attempted` | Int64 | Three-point field goals attempted (team, this game). |
| `points_three_percentage` | Float64 | Three-point percentage, 0-100 (team, this game). |
| `free_throws_made` | Int64 | Free throws made (team, this game). |
| `free_throws_attempted` | Int64 | Free throws attempted (team, this game). |
| `free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (team, this game). |
| `points_fast_break` | Int64 | Fast-break points (LNBP only) (team, this game). |
| `extra_fouls_total` | Int64 | Fouls committed (team, credited to the team rather than a player). |
| `extra_rebounds` | Int64 | Total rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_defensive` | Int64 | Defensive rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_offensive` | Int64 | Offensive rebounds (team, credited to the team rather than a player). |
| `extra_turnovers` | Int64 | Turnovers (team, credited to the team rather than a player). |
| `total_biggest_lead` | Int64 | Biggest lead of the game, in points (team, game total). |
| `total_biggest_scoring_run` | Int64 | Longest unanswered scoring run of the game, in points (team, game total). |
| `total_field_goals_percentage` | Float64 | Field-goal percentage, 0-100 (LNBP only) (team, game total). |
| `total_free_throws_percentage` | Float64 | Free-throw percentage, 0-100 (team, game total). |
| `total_lead_changes` | Int64 | Number of lead changes in the game (team, game total). |
| `total_points_fast_break` | Int64 | Fast-break points (LNBP only) (team, game total). |
| `total_points_from_bench` | Int64 | Points scored by non-starters (LNBP only) (team, game total). |
| `total_points_from_turnover` | Int64 | Points scored off opponent turnovers (LNBP only) (team, game total). |
| `total_time_in_lead` | String | Time the team led, as an ISO 8601 duration (PT36M11S) (team, game total). |
| `total_time_in_lead_seconds` | Int64 | Time the team led, in whole seconds (team, game total). |
| `total_turnovers` | Int64 | Turnovers (team, game total). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_team_box_score-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_team_box_score-example}

```python
atrium_team_box_score(fixture_id='5423072b-6715-11f0-bd8b-9ffadebc148f', website_id=12)
```

## atrium_teams

The teams of a season.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/entities`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/entities](https://embed-api.eui.connect.sportradar.com/v1/embed/12/entities)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | String | Season UUID of the team list. |
| `entity_id` | String | Team UUID (entity): one id per club across seasons, shared by every site that shows the team. |
| `name` | String | Team name in the site's language. |
| `image_url` | String | URL of the Sportradar-hosted image (400 px). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_teams-example}

```python
atrium_teams(website_id=12)
```

## atrium_team

A team page (summary tab): the team and its last and next game.

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/entity_detail`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/entity_detail?entityId=f43b1b0f-55bc-11f0-84d9-01499d2f0a07](https://embed-api.eui.connect.sportradar.com/v1/embed/12/entity_detail?entityId=f43b1b0f-55bc-11f0-84d9-01499d2f0a07)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `entityId` | `entity_id` |  | `Y` |  | Team UUID (entity_id in atrium_teams or any fixture row). |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `website_id` | Int64 | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `entity_id` | String | Team UUID, decoded from the page's tab links (the body does not echo it elsewhere). |
| `name` | String | Team name from the page banner. |
| `color` | String | Team colour from the banner, hex string with its #. |
| `logo_url` | String | URL of the banner's team logo. |
| `previous_fixture_id` | String | Game UUID of the last played game shown on the team page. |
| `previous_round` | String | Round of the last played game as the site words it (e.g. 30). |
| `previous_status` | String | Status code of the last played game, e.g. CONFIRMED. |
| `previous_start_time_utc` | Datetime | Start of the last played game, naive UTC datetime. |
| `previous_start_time_local` | Datetime | Start of the last played game, naive venue-local datetime. |
| `previous_venue` | String | Arena of the last played game. |
| `previous_attendance` | Int64 | Spectators at the last played game; null when not reported. |
| `previous_home_entity_id` | String | Team UUID of the last played game's home team. |
| `previous_home_name` | String | Name of the last played game's home team. |
| `previous_home_score` | Int64 | Home team's points in the last played game. |
| `previous_away_entity_id` | String | Team UUID of the last played game's away team. |
| `previous_away_name` | String | Name of the last played game's away team. |
| `previous_away_score` | Int64 | Away team's points in the last played game. |
| `next_fixture_id` | String | Game UUID of the next scheduled game shown on the team page. |
| `next_round` | String | Round of the next scheduled game as the site words it (e.g. 30). |
| `next_status` | String | Status code of the next scheduled game, e.g. CONFIRMED. |
| `next_start_time_utc` | Datetime | Start of the next scheduled game, naive UTC datetime. |
| `next_start_time_local` | Datetime | Start of the next scheduled game, naive venue-local datetime. |
| `next_venue` | String | Arena of the next scheduled game. |
| `next_attendance` | Int64 | Spectators at the next scheduled game; null when not reported. |
| `next_home_entity_id` | String | Team UUID of the next scheduled game's home team. |
| `next_home_name` | String | Name of the next scheduled game's home team. |
| `next_home_score` | Int64 | Home team's points in the next scheduled game. |
| `next_away_entity_id` | String | Team UUID of the next scheduled game's away team. |
| `next_away_name` | String | Name of the next scheduled game's away team. |
| `next_away_score` | Int64 | Away team's points in the next scheduled game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_team-example}

```python
atrium_team(entity_id='f43b1b0f-55bc-11f0-84d9-01499d2f0a07', website_id=12)
```

## atrium_team_stats

Team season statistics in long form (one row per team, table and statistic).

**Endpoint URL:** `GET https://embed-api.eui.connect.sportradar.com/v1/embed/{website_id}/statistics_entities`

**Valid URL:** [https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_entities](https://embed-api.eui.connect.sportradar.com/v1/embed/12/statistics_entities)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `website_id` | `website_id` |  | `Y` |  | Site config id: which league site's widget this is (12 LNB France, 14 LNBP Mexico, 3 NBL1 Australia, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro). Not a league id: one site serves several competitions. |
| `season` | `season` |  |  | `Y` | Season END year (2026 = 2025-26 on LNB, site 12, whose own year is the START year; the calendar year on the other sites). Needs competition_id when several competitions share that year (see atrium_seasons). Omit season and season_id for the site's default season. |
| `seasonId` | `season_id` |  |  | `Y` | Season UUID (season_id in atrium_seasons); sent as is, skipping the END-year lookup of season. |
| `competitionId` | `competition_id` |  |  | `Y` | Competition UUID (competition_id in atrium_seasons). Picks the season together with season, and is then not sent; with neither season nor season_id the API filters by it. |
| `locale` | `locale` |  |  | `Y` | Locale of the labels, e.g. en-EN or fr-FR. Omitted = the site's own language. Ids and codes never change. |

### Returns {#atrium_team_stats-returns}

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

### Raises {#atrium_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, or a fixture id the site does not serve: an unknown id, or a game of another site (the API answers both with an empty fixture)). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. Also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_team_stats-example}

```python
atrium_team_stats(website_id=12)
```
