---
title: SOCCER — FIFA public API v3 (api.fifa.com)
sidebar_label: FIFA public API v3 (api.fifa.com)
description: "SOCCER — FIFA public API v3 (api.fifa.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 13
toc_max_heading_level: 2
---
# SOCCER — FIFA public API v3 (api.fifa.com)

`sportsdataverse.soccer` — 9 endpoints.

## fifa_calendar_matches

/calendar/matches

**Endpoint URL:** `GET https://api.fifa.com/api/v3/calendar/matches`

**Valid URL:** [https://api.fifa.com/api/v3/calendar/matches?idCompetition=17&idSeason=285023&language=en&count=3](https://api.fifa.com/api/v3/calendar/matches?idCompetition=17&idSeason=285023&language=en&count=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `idCompetition` | `id_competition` |  |  | `Y` | Required. Competition id; 17 = FIFA World Cup (see /competitions). |
| `idSeason` | `id_season` |  |  | `Y` | Required. Season id; 285023 = FIFA World Cup 2026 (see /seasons?idCompetition=17). |
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |
| `count` | `count` |  |  | `Y` | Page size. List routes return ContinuationToken / ContinuationHash in the body; the request parameter that takes the token back is unverified. |

### Returns {#fifa_calendar_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_competition` | character | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `id_season` | character | FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key). |
| `id_stage` | character | Stage id within the season (Utf8 join key). |
| `id_group` | character | Group id within the stage (Utf8 join key). |
| `attendance` | character | Attendance. |
| `id_match` | character | FIFA match id (Utf8 join key). |
| `match_day` | character | Match day (round) within the stage. |
| `stage_name` | character | Localised stage name (e.g. First Stage), JSON list of `{Locale, Description}` objects. |
| `group_name` | character | Localised group name (e.g. Group A), JSON list of `{Locale, Description}` objects. |
| `competition_name` | character | Localised competition name, JSON list of `{Locale, Description}` objects. |
| `season_name` | character | Localised season name, JSON list of `{Locale, Description}` objects. |
| `season_short_name` | character | Localised short season name, JSON list of `{Locale, Description}` objects (often empty). |
| `date` | character | Kick-off in UTC (ISO 8601). |
| `local_date` | character | Kick-off in venue local time (ISO 8601; the trailing Z is not a UTC marker). |
| `home_team_score` | integer | Home team: goals scored. |
| `away_team_score` | integer | Away team: goals scored. |
| `aggregate_home_team_score` | character | Home team aggregate score over two legs (null for a single-leg tie). |
| `aggregate_away_team_score` | character | Away team aggregate score over two legs (null for a single-leg tie). |
| `home_team_penalty_score` | character | Home team: penalty shoot-out score (null when there was no shoot-out). |
| `away_team_penalty_score` | character | Away team: penalty shoot-out score (null when there was no shoot-out). |
| `last_period_update` | character | Time of the last period change (null in the captures). |
| `leg` | character | Leg number of a two-legged tie (null for a single-leg match). |
| `is_home_match` | character | IsHomeMatch flag from the API (null in the captures). |
| `is_ticket_sales_allowed` | character | IsTicketSalesAllowed flag from the API (null in the captures). |
| `match_time` | character | Match clock as displayed, e.g. 98'. |
| `second_half_time` | character | Second-half timing field (null in the captures). |
| `first_half_time` | character | First-half timing field (null in the captures). |
| `first_half_extra_time` | character | Stoppage time added to the first half, in minutes. |
| `second_half_extra_time` | character | Stoppage time added to the second half, in minutes. |
| `winner` | character | Team id of the winner (Utf8; null for a draw or an unplayed match). |
| `match_report_url` | character | URL of the match report (null in the captures). |
| `place_holder_a` | character | Draw placeholder for the home slot, e.g. A1 (group A, position 1). |
| `place_holder_b` | character | Draw placeholder for the away slot, e.g. A2 (group A, position 2). |
| `ball_possession` | character | Ball-possession block (null when not tracked). |
| `officials` | character | Match officials, JSON list (OfficialId, OfficialType, IdCountry, localised Name / NameShort). |
| `match_status` | integer | Match status code (FIFA integer enum: 0 = played, 1 = to be played). |
| `result_type` | integer | Result-type code (FIFA integer enum). |
| `match_number` | integer | Match number within the season. |
| `time_defined` | logical | Whether the kick-off time is confirmed. |
| `officiality_status` | integer | Officiality code of the result (FIFA integer enum). |
| `match_leg_info` | character | Two-legged tie details (null in the captures). |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `weather_humidity` | character | Weather: humidity (null in the captures). |
| `weather_temperature` | character | Weather: temperature (null in the captures). |
| `weather_wind_speed` | character | Weather: wind speed (null in the captures). |
| `weather_type` | character | Weather: condition code (null in the captures). |
| `weather_type_localized` | character | Weather: localised condition, JSON list of `{Locale, Description}` objects. |
| `home_score` | integer | Home team: goals scored. |
| `home_side` | character | Home team: side marker (null in the captures). |
| `home_id_team` | character | Home team: FIFA team id (Utf8 join key). |
| `home_picture_url` | character | Home team: image URL template with `{format}` and `{size}` placeholders. |
| `home_id_country` | character | Home team: three-letter FIFA country code, e.g. ARG. |
| `home_tactics` | character | Home team: formation, e.g. 4-2-3-1. |
| `home_team_type` | integer | Home team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `home_age_type` | integer | Home team: age-category code (FIFA integer enum; 7 = senior). |
| `home_team_name` | character | Home team: localised name, JSON list of `{Locale, Description}` objects. |
| `home_abbreviation` | character | Home team: abbreviation (e.g. ARG, FWC 2026). |
| `home_short_club_name` | character | Home team: short English team name. |
| `home_football_type` | integer | Home team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `home_gender` | integer | Home team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `home_id_association` | character | Home team: three-letter code of the member association, e.g. ARG. |
| `away_score` | integer | Away team: goals scored. |
| `away_side` | character | Away team: side marker (null in the captures). |
| `away_id_team` | character | Away team: FIFA team id (Utf8 join key). |
| `away_picture_url` | character | Away team: image URL template with `{format}` and `{size}` placeholders. |
| `away_id_country` | character | Away team: three-letter FIFA country code, e.g. ARG. |
| `away_tactics` | character | Away team: formation, e.g. 4-2-3-1. |
| `away_team_type` | integer | Away team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `away_age_type` | integer | Away team: age-category code (FIFA integer enum; 7 = senior). |
| `away_team_name` | character | Away team: localised name, JSON list of `{Locale, Description}` objects. |
| `away_abbreviation` | character | Away team: abbreviation (e.g. ARG, FWC 2026). |
| `away_short_club_name` | character | Away team: short English team name. |
| `away_football_type` | integer | Away team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `away_gender` | integer | Away team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `away_id_association` | character | Away team: three-letter code of the member association, e.g. ARG. |
| `stadium_id_stadium` | character | Stadium: FIFA stadium id (Utf8 join key). |
| `stadium_name` | character | Stadium: localised name, JSON list of `{Locale, Description}` objects. |
| `stadium_capacity` | character | Stadium: seating capacity. |
| `stadium_web_address` | character | Stadium: website URL. |
| `stadium_built` | character | Stadium: year built, as an ISO 8601 date. |
| `stadium_roof` | logical | Stadium: whether the stadium is roofed. |
| `stadium_turf` | character | Stadium: pitch surface (null in the captures). |
| `stadium_id_city` | character | Stadium: city id (Utf8 join key). |
| `stadium_city_name` | character | Stadium: localised city name, JSON list of `{Locale, Description}` objects. |
| `stadium_id_country` | character | Stadium: three-letter FIFA country code, e.g. ARG. |
| `stadium_postal_code` | character | Stadium: postal code. |
| `stadium_street` | character | Stadium: street address. |
| `stadium_email` | character | Stadium: contact e-mail. |
| `stadium_fax` | character | Stadium: contact fax number. |
| `stadium_phone` | character | Stadium: contact phone number. |
| `stadium_affiliation_country` | character | Stadium: affiliation country (null in the captures). |
| `stadium_affiliation_region` | character | Stadium: affiliation region (null in the captures). |
| `stadium_latitude` | character | Stadium: latitude in decimal degrees. |
| `stadium_longitude` | character | Stadium: longitude in decimal degrees. |
| `stadium_length` | character | Stadium: pitch length (null in the captures). |
| `stadium_width` | character | Stadium: pitch width (null in the captures). |
| `stadium_properties_id_ifes` | character | Stadium: IFES cross-reference id (Utf8). |
| `stadium_is_updateable` | character | Stadium: isUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_calendar_matches-example}

```python
fifa_calendar_matches(count='3', id_competition='17', id_season='285023', language='en')
```

_Last validated n/a._

## fifa_competition

/competitions/{idCompetition}

**Endpoint URL:** `GET https://api.fifa.com/api/v3/competitions/{id_competition}`

**Valid URL:** [https://api.fifa.com/api/v3/competitions/17?language=en](https://api.fifa.com/api/v3/competitions/17?language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id_competition` | `id_competition` |  | `Y` |  | Competition id; 17 = FIFA World Cup (see /competitions). |
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |

### Returns {#fifa_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_competition` | character | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_confederation` | character | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `id_member_association` | character | Member-association codes (e.g. ENG), JSON list. |
| `id_owner` | character | Code of the body that owns the competition (FIFA or a confederation). |
| `gender` | integer | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `football_type` | integer | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `team_type` | integer | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `competition_type` | integer | Competition-type code (FIFA integer enum). |
| `age_type` | integer | Age-category code (FIFA integer enum; 7 = senior). |
| `display_order` | character | Sort order for display. |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_competition-example}

```python
fifa_competition(id_competition='17', language='en')
```

_Last validated n/a._

## fifa_competitions

/competitions

**Endpoint URL:** `GET https://api.fifa.com/api/v3/competitions`

**Valid URL:** [https://api.fifa.com/api/v3/competitions?language=en&count=3](https://api.fifa.com/api/v3/competitions?language=en&count=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |
| `count` | `count` |  |  | `Y` | Page size. List routes return ContinuationToken / ContinuationHash in the body; the request parameter that takes the token back is unverified. |

### Returns {#fifa_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_competition` | character | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_confederation` | character | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `id_member_association` | character | Member-association codes (e.g. ENG), JSON list. |
| `id_owner` | character | Code of the body that owns the competition (FIFA or a confederation). |
| `gender` | integer | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `football_type` | integer | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `team_type` | integer | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `competition_type` | integer | Competition-type code (FIFA integer enum). |
| `age_type` | integer | Age-category code (FIFA integer enum; 7 = senior). |
| `display_order` | integer | Sort order for display. |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_stats_perform` | character | Stats Perform (Opta) id of the record (Utf8 cross-reference). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_competitions-example}

```python
fifa_competitions(count='3', language='en')
```

_Last validated n/a._

## fifa_live_football

/live/football

**Endpoint URL:** `GET https://api.fifa.com/api/v3/live/football`

**Valid URL:** [https://api.fifa.com/api/v3/live/football?language=en](https://api.fifa.com/api/v3/live/football?language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |

### Returns {#fifa_live_football-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_match` | character | FIFA match id (Utf8 join key). |
| `id_stage` | character | Stage id within the season (Utf8 join key). |
| `id_group` | character | Group id within the stage (Utf8 join key). |
| `id_season` | character | FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key). |
| `coverage_level` | integer | Data-coverage level of the match (integer). |
| `id_competition` | character | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `competition_name` | character | Localised competition name, JSON list of `{Locale, Description}` objects. |
| `season_name` | character | Localised season name, JSON list of `{Locale, Description}` objects. |
| `season_short_name` | character | Localised short season name, JSON list of `{Locale, Description}` objects (often empty). |
| `result_type` | integer | Result-type code (FIFA integer enum). |
| `match_day` | character | Match day (round) within the stage. |
| `match_number` | character | Match number within the season. |
| `home_team_penalty_score` | character | Home team: penalty shoot-out score (null when there was no shoot-out). |
| `away_team_penalty_score` | character | Away team: penalty shoot-out score (null when there was no shoot-out). |
| `aggregate_home_team_score` | character | Home team aggregate score over two legs (null for a single-leg tie). |
| `aggregate_away_team_score` | character | Away team aggregate score over two legs (null for a single-leg tie). |
| `weather` | character | Weather block (null when not reported). |
| `attendance` | character | Attendance. |
| `date` | character | Kick-off in UTC (ISO 8601). |
| `local_date` | character | Kick-off in venue local time (ISO 8601; the trailing Z is not a UTC marker). |
| `match_time` | character | Match clock as displayed, e.g. 98'. |
| `second_half_time` | character | Second-half timing field (null in the captures). |
| `first_half_time` | character | First-half timing field (null in the captures). |
| `first_half_extra_time` | numeric | Stoppage time added to the first half, in minutes. |
| `second_half_extra_time` | numeric | Stoppage time added to the second half, in minutes. |
| `winner` | character | Team id of the winner (Utf8; null for a draw or an unplayed match). |
| `period` | integer | Match period code (FIFA integer enum). |
| `territorial_possesion` | character | Territorial possession block (sic: the API's spelling; null in the captures). |
| `territorial_third_possesion` | character | Territorial possession by third (sic: the API's spelling; null in the captures). |
| `officials` | character | Match officials, JSON list (OfficialId, OfficialType, IdCountry, localised Name / NameShort). |
| `match_status` | integer | Match status code (FIFA integer enum: 0 = played, 1 = to be played). |
| `group_name` | character | Localised group name (e.g. Group A), JSON list of `{Locale, Description}` objects. |
| `stage_name` | character | Localised stage name (e.g. First Stage), JSON list of `{Locale, Description}` objects. |
| `officiality_status` | integer | Officiality code of the result (FIFA integer enum). |
| `time_defined` | logical | Whether the kick-off time is confirmed. |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `stadium_id_stadium` | character | Stadium: FIFA stadium id (Utf8 join key). |
| `stadium_name` | character | Stadium: localised name, JSON list of `{Locale, Description}` objects. |
| `stadium_capacity` | integer | Stadium: seating capacity. |
| `stadium_web_address` | character | Stadium: website URL. |
| `stadium_built` | character | Stadium: year built, as an ISO 8601 date. |
| `stadium_roof` | logical | Stadium: whether the stadium is roofed. |
| `stadium_turf` | character | Stadium: pitch surface (null in the captures). |
| `stadium_id_city` | character | Stadium: city id (Utf8 join key). |
| `stadium_city_name` | character | Stadium: localised city name, JSON list of `{Locale, Description}` objects. |
| `stadium_id_country` | character | Stadium: three-letter FIFA country code, e.g. ARG. |
| `stadium_postal_code` | character | Stadium: postal code. |
| `stadium_street` | character | Stadium: street address. |
| `stadium_email` | character | Stadium: contact e-mail. |
| `stadium_fax` | character | Stadium: contact fax number. |
| `stadium_phone` | character | Stadium: contact phone number. |
| `stadium_affiliation_country` | character | Stadium: affiliation country (null in the captures). |
| `stadium_affiliation_region` | character | Stadium: affiliation region (null in the captures). |
| `stadium_latitude` | numeric | Stadium: latitude in decimal degrees. |
| `stadium_longitude` | numeric | Stadium: longitude in decimal degrees. |
| `stadium_length` | character | Stadium: pitch length (null in the captures). |
| `stadium_width` | character | Stadium: pitch width (null in the captures). |
| `stadium_is_updateable` | character | Stadium: isUpdateable flag from the API (null in the captures). |
| `home_team_score` | numeric | Home team: goals scored. |
| `home_team_side` | character | Home team: side marker (null in the captures). |
| `home_team_id_team` | character | Home team: FIFA team id (Utf8 join key). |
| `home_team_picture_url` | character | Home team: image URL template with `{format}` and `{size}` placeholders. |
| `home_team_id_country` | character | Home team: three-letter FIFA country code, e.g. ARG. |
| `home_team_team_type` | integer | Home team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `home_team_age_type` | integer | Home team: age-category code (FIFA integer enum; 7 = senior). |
| `home_team_tactics` | character | Home team: formation, e.g. 4-2-3-1. |
| `home_team_team_name` | character | Home team: localised team name, JSON list of `{Locale, Description}` objects. |
| `home_team_abbreviation` | character | Home team: abbreviation (e.g. ARG, FWC 2026). |
| `home_team_coaches` | character | Home team: coaches, JSON list (IdCoach, IdCountry, Role, localised Name / Alias, ...). |
| `home_team_players` | character | Home team: line-up, JSON list (IdPlayer, ShirtNumber, Status, Captain, Position, localised PlayerName, ...). |
| `home_team_bookings` | character | Home team: cards, JSON list (Card, Period, Minute, IdPlayer, IdTeam, ...). |
| `home_team_goals` | character | Home team: goals, JSON list (Type, Period, Minute, IdPlayer, IdAssistPlayer, IdTeam, ...). |
| `home_team_substitutions` | character | Home team: substitutions, JSON list (Period, Minute, IdPlayerOff, IdPlayerOn, Reason, ...). |
| `home_team_staffs` | character | Home team: team staff, JSON list (empty in the captures). |
| `home_team_football_type` | integer | Home team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `home_team_gender` | integer | Home team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `home_team_id_association` | character | Home team: three-letter code of the member association, e.g. ARG. |
| `home_team_short_club_name` | character | Home team: short English team name. |
| `away_team_score` | numeric | Away team: goals scored. |
| `away_team_side` | character | Away team: side marker (null in the captures). |
| `away_team_id_team` | character | Away team: FIFA team id (Utf8 join key). |
| `away_team_picture_url` | character | Away team: image URL template with `{format}` and `{size}` placeholders. |
| `away_team_id_country` | character | Away team: three-letter FIFA country code, e.g. ARG. |
| `away_team_team_type` | integer | Away team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `away_team_age_type` | integer | Away team: age-category code (FIFA integer enum; 7 = senior). |
| `away_team_tactics` | character | Away team: formation, e.g. 4-2-3-1. |
| `away_team_team_name` | character | Away team: localised team name, JSON list of `{Locale, Description}` objects. |
| `away_team_abbreviation` | character | Away team: abbreviation (e.g. ARG, FWC 2026). |
| `away_team_coaches` | character | Away team: coaches, JSON list (IdCoach, IdCountry, Role, localised Name / Alias, ...). |
| `away_team_players` | character | Away team: line-up, JSON list (IdPlayer, ShirtNumber, Status, Captain, Position, localised PlayerName, ...). |
| `away_team_bookings` | character | Away team: cards, JSON list (Card, Period, Minute, IdPlayer, IdTeam, ...). |
| `away_team_goals` | character | Away team: goals, JSON list (Type, Period, Minute, IdPlayer, IdAssistPlayer, IdTeam, ...). |
| `away_team_substitutions` | character | Away team: substitutions, JSON list (Period, Minute, IdPlayerOff, IdPlayerOn, Reason, ...). |
| `away_team_staffs` | character | Away team: team staff, JSON list (empty in the captures). |
| `away_team_football_type` | integer | Away team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `away_team_gender` | integer | Away team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `away_team_id_association` | character | Away team: three-letter code of the member association, e.g. ARG. |
| `away_team_short_club_name` | character | Away team: short English team name. |
| `ball_possession_intervals` | character | Ball possession: possession per interval, JSON list. |
| `ball_possession_last_x` | character | Ball possession: possession over the most recent stretch, JSON list. |
| `ball_possession_overall_home` | numeric | Ball possession: home team share of possession, percent. |
| `ball_possession_overall_away` | numeric | Ball possession: away team share of possession, percent. |
| `properties_id_stats_perform` | character | Stats Perform (Opta) id of the record (Utf8 cross-reference). |
| `ball_possession` | numeric | Ball-possession block (null when not tracked). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_live_football-example}

```python
fifa_live_football(language='en')
```

_Last validated n/a._

## fifa_players_search

/players/search

**Endpoint URL:** `GET https://api.fifa.com/api/v3/players/search`

**Valid URL:** [https://api.fifa.com/api/v3/players/search?name=Messi&language=en](https://api.fifa.com/api/v3/players/search?name=Messi&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `name` | `name` |  |  | `Y` | Required. Search text. |
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |

### Returns {#fifa_players_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | character | FIFA player id (Utf8 join key). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `alias` | character | Localised alias, JSON list of `{Locale, Description}` objects. |
| `birth_date` | character | Date of birth (ISO 8601). |
| `weight` | numeric | Weight in kilograms. |
| `height` | numeric | Height in centimetres. |
| `birth_place` | character | Place of birth (a city name or a country code). |
| `id_country` | character | Three-letter FIFA country code, e.g. ARG. |
| `international_caps` | integer | International appearances (caps). |
| `international_debut` | character | Date of the international debut (ISO 8601). |
| `top_competition_debut` | character | Debut date in a top competition (null in the captures). |
| `picture_url` | character | Image URL template with `{format}` and `{size}` placeholders. |
| `thumbnail_url` | character | Thumbnail image URL (null in the captures). |
| `twitter_account` | character | Twitter / X handle (null in the captures). |
| `preferred_foot` | numeric | Preferred-foot code (FIFA integer enum; 9999 when unset). |
| `media_content` | character | Media content entries, JSON list (empty in the captures). |
| `localized_twitter_accounts` | character | Localised Twitter / X handles (null in the captures). |
| `goals` | integer | Goals credited to the player. |
| `player_picture` | character | Player picture block (null in the captures). |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |
| `properties_id_stats_perform` | character | Stats Perform (Opta) id of the record (Utf8 cross-reference). |
| `properties_stats_perform_ifes_id` | character | IFES id as carried by the Stats Perform feed (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_players_search-example}

```python
fifa_players_search(language='en', name='Messi')
```

_Last validated n/a._

## fifa_seasons

/seasons

**Endpoint URL:** `GET https://api.fifa.com/api/v3/seasons`

**Valid URL:** [https://api.fifa.com/api/v3/seasons?idCompetition=17&language=en&count=3](https://api.fifa.com/api/v3/seasons?idCompetition=17&language=en&count=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `idCompetition` | `id_competition` |  |  | `Y` | Required. Competition id; 17 = FIFA World Cup (see /competitions). |
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |
| `count` | `count` |  |  | `Y` | Page size. List routes return ContinuationToken / ContinuationHash in the body; the request parameter that takes the token back is unverified. |

### Returns {#fifa_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_season` | character | FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `short_name` | character | Localised short name, JSON list of `{Locale, Description}` objects (often empty). |
| `abbreviation` | character | Abbreviation (e.g. ARG, FWC 2026). |
| `id_member_association` | character | Member-association codes (e.g. ENG), JSON list. |
| `id_confederation` | character | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `id_competition` | character | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `start_date` | character | Start date (ISO 8601). |
| `end_date` | character | End date (ISO 8601). |
| `picture_url` | character | Image URL template with `{format}` and `{size}` placeholders. |
| `mascot_picture_url` | character | Mascot image URL template with `{format}` and `{size}` placeholders. |
| `match_ball_picture_url` | character | Match-ball image URL template with `{format}` and `{size}` placeholders. |
| `host_teams` | character | Host teams, JSON list of `{IdTeam}` objects. |
| `sport_type` | integer | Sport code (FIFA integer enum; 0 = football). |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |
| `properties_providers` | character | Data-provider tag of the record. |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_seasons-example}

```python
fifa_seasons(count='3', id_competition='17', language='en')
```

_Last validated n/a._

## fifa_stadiums

/stadiums

**Endpoint URL:** `GET https://api.fifa.com/api/v3/stadiums`

**Valid URL:** [https://api.fifa.com/api/v3/stadiums?language=en&count=3](https://api.fifa.com/api/v3/stadiums?language=en&count=3)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |
| `count` | `count` |  |  | `Y` | Page size. List routes return ContinuationToken / ContinuationHash in the body; the request parameter that takes the token back is unverified. |

### Returns {#fifa_stadiums-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_stadium` | character | FIFA stadium id (Utf8 join key). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `capacity` | character | Seating capacity. |
| `web_address` | character | Website URL. |
| `built` | character | Year built, as an ISO 8601 date. |
| `roof` | logical | Whether the stadium is roofed. |
| `turf` | character | Pitch surface (null in the captures). |
| `id_city` | character | City id (Utf8 join key). |
| `city_name` | character | Localised city name, JSON list of `{Locale, Description}` objects. |
| `id_country` | character | Three-letter FIFA country code, e.g. ARG. |
| `postal_code` | character | Postal code. |
| `street` | character | Street address. |
| `email` | character | Contact e-mail. |
| `fax` | character | Contact fax number. |
| `phone` | character | Contact phone number. |
| `affiliation_country` | character | Affiliation country (null in the captures). |
| `affiliation_region` | character | Affiliation region (null in the captures). |
| `latitude` | character | Latitude in decimal degrees. |
| `longitude` | character | Longitude in decimal degrees. |
| `length` | character | Pitch length (null in the captures). |
| `width` | character | Pitch width (null in the captures). |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_stadiums-example}

```python
fifa_stadiums(count='3', language='en')
```

_Last validated n/a._

## fifa_team

/teams/{idTeam}

**Endpoint URL:** `GET https://api.fifa.com/api/v3/teams/{id_team}`

**Valid URL:** [https://api.fifa.com/api/v3/teams/43922?language=en](https://api.fifa.com/api/v3/teams/43922?language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id_team` | `id_team` |  | `Y` |  | Team id (see /teams/search). |
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |

### Returns {#fifa_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_team` | character | FIFA team id (Utf8 join key). |
| `id_confederation` | character | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `active_status` | integer | Activity-status code (FIFA integer enum). |
| `type` | integer | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `age_type` | integer | Age-category code (FIFA integer enum; 7 = senior). |
| `football_type` | integer | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `gender` | integer | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_association` | character | Three-letter code of the member association, e.g. ARG. |
| `id_city` | character | City id (Utf8 join key). |
| `headquarters` | character | Headquarters (null in the captures). |
| `training_centre` | character | Training centre (null in the captures). |
| `official_site` | character | Official website (null in the captures). |
| `city` | character | City of the team's headquarters. |
| `id_country` | character | Three-letter FIFA country code, e.g. ARG. |
| `postal_code` | character | Postal code. |
| `region_name` | character | Region name (null in the captures). |
| `short_club_name` | character | Short English team name. |
| `abbreviation` | character | Abbreviation (e.g. ARG, FWC 2026). |
| `street` | character | Street address. |
| `foundation_year` | integer | Year the team or association was founded. |
| `stadium` | character | Home stadium block (null in the captures). |
| `picture_url` | character | Image URL template with `{format}` and `{size}` placeholders. |
| `thumbnail_url` | character | Thumbnail image URL (null in the captures). |
| `display_name` | character | Localised display name (e.g. ARG), JSON list of `{Locale, Description}` objects. |
| `content` | character | Editorial content entries, JSON list (empty in the captures). |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |
| `properties_stats_perform_ifes_id` | character | IFES id as carried by the Stats Perform feed (Utf8). |
| `properties_id_stats_perform` | character | Stats Perform (Opta) id of the record (Utf8 cross-reference). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_team-example}

```python
fifa_team(id_team='43922', language='en')
```

_Last validated n/a._

## fifa_teams_search

/teams/search

**Endpoint URL:** `GET https://api.fifa.com/api/v3/teams/search`

**Valid URL:** [https://api.fifa.com/api/v3/teams/search?name=Argentina&language=en](https://api.fifa.com/api/v3/teams/search?name=Argentina&language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `name` | `name` |  |  | `Y` | Required. Search text. |
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |

### Returns {#fifa_teams_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_team` | character | FIFA team id (Utf8 join key). |
| `id_confederation` | character | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `active_status` | numeric | Activity-status code (FIFA integer enum). |
| `type` | integer | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `age_type` | integer | Age-category code (FIFA integer enum; 7 = senior). |
| `football_type` | integer | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `gender` | integer | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `name` | character | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_association` | character | Three-letter code of the member association, e.g. ARG. |
| `id_city` | character | City id (Utf8 join key). |
| `headquarters` | character | Headquarters (null in the captures). |
| `training_centre` | character | Training centre (null in the captures). |
| `official_site` | character | Official website (null in the captures). |
| `city` | character | City of the team's headquarters. |
| `id_country` | character | Three-letter FIFA country code, e.g. ARG. |
| `postal_code` | character | Postal code. |
| `region_name` | character | Region name (null in the captures). |
| `short_club_name` | character | Short English team name. |
| `abbreviation` | character | Abbreviation (e.g. ARG, FWC 2026). |
| `street` | character | Street address. |
| `foundation_year` | numeric | Year the team or association was founded. |
| `stadium` | character | Home stadium block (null in the captures). |
| `picture_url` | character | Image URL template with `{format}` and `{size}` placeholders. |
| `thumbnail_url` | character | Thumbnail image URL (null in the captures). |
| `display_name` | character | Localised display name (e.g. ARG), JSON list of `{Locale, Description}` objects. |
| `content` | character | Editorial content entries, JSON list (empty in the captures). |
| `is_updateable` | character | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | character | IFES cross-reference id (Utf8). |
| `properties_stats_perform_ifes_id` | character | IFES id as carried by the Stats Perform feed (Utf8). |
| `properties_id_stats_perform` | character | Stats Perform (Opta) id of the record (Utf8 cross-reference). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Example {#fifa_teams_search-example}

```python
fifa_teams_search(language='en', name='Argentina')
```

_Last validated n/a._
