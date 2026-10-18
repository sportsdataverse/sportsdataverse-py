---
title: "ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Team"
sidebar_label: "Team"
sidebar_position: 2
description: "ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Team — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# ATRIUM — Sportradar Atrium embed API (embed-api.eui.connect.sportradar.com) — Team

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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `fixture_id` | character | Game UUID (fixture); the key of every game frame and of the fixture_detail routes. |
| `season_id` | character | Season UUID of the game. |
| `entity_id` | character | Team UUID (from the fixture's isHome flag). |
| `side` | character | Side of the game the row belongs to: home or away (from the fixture's isHome flag). |
| `minutes` | character | Time played as an ISO 8601 duration (PT7M23S); a team's is the sum over its players (PT200M) (team, this game). |
| `seconds_played` | integer | Time played in whole seconds, parsed from minutes (team, this game). |
| `points` | integer | Points scored (team, this game). |
| `rebounds` | integer | Total rebounds (team, this game). |
| `rebounds_offensive` | integer | Offensive rebounds (team, this game). |
| `rebounds_defensive` | integer | Defensive rebounds (team, this game). |
| `assists` | integer | Assists (team, this game). |
| `steals` | integer | Steals (team, this game). |
| `blocks` | integer | Shots blocked (team, this game). |
| `blocks_received` | integer | Own shots blocked by an opponent (LNB only) (team, this game). |
| `turnovers` | integer | Turnovers (team, this game). |
| `fouls_total` | integer | Fouls committed (team, this game). |
| `fouls_drawn` | integer | Fouls drawn (committed against the player or team; LNBP only) (team, this game). |
| `plus_minus` | integer | Plus/minus, the point differential while on court (LNB only; null in some team lines) (team, this game). |
| `efficiency` | integer | Efficiency rating, from the site's own formula (team, this game). |
| `field_goals_made` | integer | Field goals made, two- and three-pointers together (LNBP only) (team, this game). |
| `field_goals_attempted` | integer | Field goals attempted (LNBP only) (team, this game). |
| `field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (team, this game). |
| `points_two_made` | integer | Two-point field goals made (team, this game). |
| `points_two_attempted` | integer | Two-point field goals attempted (team, this game). |
| `points_two_percentage` | numeric | Two-point percentage, 0-100 (team, this game). |
| `points_three_made` | integer | Three-point field goals made (team, this game). |
| `points_three_attempted` | integer | Three-point field goals attempted (team, this game). |
| `points_three_percentage` | numeric | Three-point percentage, 0-100 (team, this game). |
| `free_throws_made` | integer | Free throws made (team, this game). |
| `free_throws_attempted` | integer | Free throws attempted (team, this game). |
| `free_throws_percentage` | numeric | Free-throw percentage, 0-100 (team, this game). |
| `points_fast_break` | integer | Fast-break points (LNBP only) (team, this game). |
| `extra_fouls_total` | integer | Fouls committed (team, credited to the team rather than a player). |
| `extra_rebounds` | integer | Total rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_defensive` | integer | Defensive rebounds (team, credited to the team rather than a player). |
| `extra_rebounds_offensive` | integer | Offensive rebounds (team, credited to the team rather than a player). |
| `extra_turnovers` | integer | Turnovers (team, credited to the team rather than a player). |
| `total_biggest_lead` | integer | Biggest lead of the game, in points (team, game total). |
| `total_biggest_scoring_run` | integer | Longest unanswered scoring run of the game, in points (team, game total). |
| `total_field_goals_percentage` | numeric | Field-goal percentage, 0-100 (LNBP only) (team, game total). |
| `total_free_throws_percentage` | numeric | Free-throw percentage, 0-100 (team, game total). |
| `total_lead_changes` | integer | Number of lead changes in the game (team, game total). |
| `total_points_fast_break` | integer | Fast-break points (LNBP only) (team, game total). |
| `total_points_from_bench` | integer | Points scored by non-starters (LNBP only) (team, game total). |
| `total_points_from_turnover` | integer | Points scored off opponent turnovers (LNBP only) (team, game total). |
| `total_time_in_lead` | character | Time the team led, as an ISO 8601 duration (PT36M11S) (team, game total). |
| `total_time_in_lead_seconds` | integer | Time the team led, in whole seconds (team, game total). |
| `total_turnovers` | integer | Turnovers (team, game total). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_team_box_score-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_team_box_score-example}

```python
atrium_team_box_score(fixture_id='5423072b-6715-11f0-bd8b-9ffadebc148f', website_id=12)
```

_Last validated n/a._

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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID of the team list. |
| `entity_id` | character | Team UUID (entity): one id per club across seasons, shared by every site that shows the team. |
| `name` | character | Team name in the site's language. |
| `image_url` | character | URL of the Sportradar-hosted image (400 px). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_teams-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_teams-example}

```python
atrium_teams(website_id=12)
```

_Last validated n/a._

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
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `entity_id` | character | Team UUID, decoded from the page's tab links (the body does not echo it elsewhere). |
| `name` | character | Team name from the page banner. |
| `color` | character | Team colour from the banner, hex string with its #. |
| `logo_url` | character | URL of the banner's team logo. |
| `previous_fixture_id` | character | Game UUID of the last played game shown on the team page. |
| `previous_round` | character | Round of the last played game as the site words it (e.g. 30). |
| `previous_status` | character | Status code of the last played game, e.g. CONFIRMED. |
| `previous_start_time_utc` | datetime | Start of the last played game, naive UTC datetime. |
| `previous_start_time_local` | datetime | Start of the last played game, naive venue-local datetime. |
| `previous_venue` | character | Arena of the last played game. |
| `previous_attendance` | integer | Spectators at the last played game; null when not reported. |
| `previous_home_entity_id` | character | Team UUID of the last played game's home team. |
| `previous_home_name` | character | Name of the last played game's home team. |
| `previous_home_score` | integer | Home team's points in the last played game. |
| `previous_away_entity_id` | character | Team UUID of the last played game's away team. |
| `previous_away_name` | character | Name of the last played game's away team. |
| `previous_away_score` | integer | Away team's points in the last played game. |
| `next_fixture_id` | character | Game UUID of the next scheduled game shown on the team page. |
| `next_round` | character | Round of the next scheduled game as the site words it (e.g. 30). |
| `next_status` | character | Status code of the next scheduled game, e.g. CONFIRMED. |
| `next_start_time_utc` | datetime | Start of the next scheduled game, naive UTC datetime. |
| `next_start_time_local` | datetime | Start of the next scheduled game, naive venue-local datetime. |
| `next_venue` | character | Arena of the next scheduled game. |
| `next_attendance` | integer | Spectators at the next scheduled game; null when not reported. |
| `next_home_entity_id` | character | Team UUID of the next scheduled game's home team. |
| `next_home_name` | character | Name of the next scheduled game's home team. |
| `next_home_score` | integer | Home team's points in the next scheduled game. |
| `next_away_entity_id` | character | Team UUID of the next scheduled game's away team. |
| `next_away_name` | character | Name of the next scheduled game's away team. |
| `next_away_score` | integer | Away team's points in the next scheduled game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#atrium_team-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_team-example}

```python
atrium_team(entity_id='f43b1b0f-55bc-11f0-84d9-01499d2f0a07', website_id=12)
```

_Last validated n/a._

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

### Raises {#atrium_team_stats-raises}

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404, a fixture id the site does not serve: unknown, or a game of another site, which the API answers alike with an empty game, so it is not called a wrong scope). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

### Example {#atrium_team_stats-example}

```python
atrium_team_stats(website_id=12)
```

_Last validated n/a._
