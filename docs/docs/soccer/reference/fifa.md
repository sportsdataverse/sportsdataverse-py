---
title: Soccer (all) — FIFA public API v3 (api.fifa.com)
sidebar_label: FIFA public API v3 (api.fifa.com)
description: "Soccer (all) — FIFA public API v3 (api.fifa.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 13
toc_max_heading_level: 2
---
# Soccer (all) — FIFA public API v3 (api.fifa.com)

`sportsdataverse.soccer` — 9 endpoints.

## fifa_calendar_matches

Match calendar of a competition season (first page; results and fixtures).

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
| `id_competition` | String | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `id_season` | String | FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key). |
| `id_stage` | String | Stage id within the season (Utf8 join key). |
| `id_group` | String | Group id within the stage (Utf8 join key). |
| `attendance` | String | Attendance. |
| `id_match` | String | FIFA match id (Utf8 join key). |
| `match_day` | String | Match day (round) within the stage. |
| `stage_name` | String | Localised stage name (e.g. First Stage), JSON list of `{Locale, Description}` objects. |
| `group_name` | String | Localised group name (e.g. Group A), JSON list of `{Locale, Description}` objects. |
| `competition_name` | String | Localised competition name, JSON list of `{Locale, Description}` objects. |
| `season_name` | String | Localised season name, JSON list of `{Locale, Description}` objects. |
| `season_short_name` | String | Localised short season name, JSON list of `{Locale, Description}` objects (often empty). |
| `date` | String | Kick-off in UTC (ISO 8601). |
| `local_date` | String | Kick-off in venue local time (ISO 8601; the trailing Z is not a UTC marker). |
| `home_team_score` | Int64 | Home team: goals scored. |
| `away_team_score` | Int64 | Away team: goals scored. |
| `aggregate_home_team_score` | String | Home team aggregate score over two legs (null for a single-leg tie). |
| `aggregate_away_team_score` | String | Away team aggregate score over two legs (null for a single-leg tie). |
| `home_team_penalty_score` | String | Home team: penalty shoot-out score (null when there was no shoot-out). |
| `away_team_penalty_score` | String | Away team: penalty shoot-out score (null when there was no shoot-out). |
| `last_period_update` | String | Time of the last period change (null in the captures). |
| `leg` | String | Leg number of a two-legged tie (null for a single-leg match). |
| `is_home_match` | String | IsHomeMatch flag from the API (null in the captures). |
| `is_ticket_sales_allowed` | String | IsTicketSalesAllowed flag from the API (null in the captures). |
| `match_time` | String | Match clock as displayed, e.g. 98'. |
| `second_half_time` | String | Second-half timing field (null in the captures). |
| `first_half_time` | String | First-half timing field (null in the captures). |
| `first_half_extra_time` | String | Stoppage time added to the first half, in minutes. |
| `second_half_extra_time` | String | Stoppage time added to the second half, in minutes. |
| `winner` | String | Team id of the winner (Utf8; null for a draw or an unplayed match). |
| `match_report_url` | String | URL of the match report (null in the captures). |
| `place_holder_a` | String | Draw placeholder for the home slot, e.g. A1 (group A, position 1). |
| `place_holder_b` | String | Draw placeholder for the away slot, e.g. A2 (group A, position 2). |
| `ball_possession` | String | Ball-possession block (null when not tracked). |
| `officials` | String | Match officials, JSON list (OfficialId, OfficialType, IdCountry, localised Name / NameShort). |
| `match_status` | Int64 | Match status code (FIFA integer enum: 0 = played, 1 = to be played). |
| `result_type` | Int64 | Result-type code (FIFA integer enum). |
| `match_number` | Int64 | Match number within the season. |
| `time_defined` | Boolean | Whether the kick-off time is confirmed. |
| `officiality_status` | Int64 | Officiality code of the result (FIFA integer enum). |
| `match_leg_info` | String | Two-legged tie details (null in the captures). |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `weather_humidity` | String | Weather: humidity (null in the captures). |
| `weather_temperature` | String | Weather: temperature (null in the captures). |
| `weather_wind_speed` | String | Weather: wind speed (null in the captures). |
| `weather_type` | String | Weather: condition code (null in the captures). |
| `weather_type_localized` | String | Weather: localised condition, JSON list of `{Locale, Description}` objects. |
| `home_score` | Int64 | Home team: goals scored. |
| `home_side` | String | Home team: side marker (null in the captures). |
| `home_id_team` | String | Home team: FIFA team id (Utf8 join key). |
| `home_picture_url` | String | Home team: image URL template with `{format}` and `{size}` placeholders. |
| `home_id_country` | String | Home team: three-letter FIFA country code, e.g. ARG. |
| `home_tactics` | String | Home team: formation, e.g. 4-2-3-1. |
| `home_team_type` | Int64 | Home team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `home_age_type` | Int64 | Home team: age-category code (FIFA integer enum; 7 = senior). |
| `home_team_name` | String | Home team: localised name, JSON list of `{Locale, Description}` objects. |
| `home_abbreviation` | String | Home team: abbreviation (e.g. ARG, FWC 2026). |
| `home_short_club_name` | String | Home team: short English team name. |
| `home_football_type` | Int64 | Home team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `home_gender` | Int64 | Home team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `home_id_association` | String | Home team: three-letter code of the member association, e.g. ARG. |
| `away_score` | Int64 | Away team: goals scored. |
| `away_side` | String | Away team: side marker (null in the captures). |
| `away_id_team` | String | Away team: FIFA team id (Utf8 join key). |
| `away_picture_url` | String | Away team: image URL template with `{format}` and `{size}` placeholders. |
| `away_id_country` | String | Away team: three-letter FIFA country code, e.g. ARG. |
| `away_tactics` | String | Away team: formation, e.g. 4-2-3-1. |
| `away_team_type` | Int64 | Away team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `away_age_type` | Int64 | Away team: age-category code (FIFA integer enum; 7 = senior). |
| `away_team_name` | String | Away team: localised name, JSON list of `{Locale, Description}` objects. |
| `away_abbreviation` | String | Away team: abbreviation (e.g. ARG, FWC 2026). |
| `away_short_club_name` | String | Away team: short English team name. |
| `away_football_type` | Int64 | Away team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `away_gender` | Int64 | Away team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `away_id_association` | String | Away team: three-letter code of the member association, e.g. ARG. |
| `stadium_id_stadium` | String | Stadium: FIFA stadium id (Utf8 join key). |
| `stadium_name` | String | Stadium: localised name, JSON list of `{Locale, Description}` objects. |
| `stadium_capacity` | String | Stadium: seating capacity. |
| `stadium_web_address` | String | Stadium: website URL. |
| `stadium_built` | String | Stadium: year built, as an ISO 8601 date. |
| `stadium_roof` | Boolean | Stadium: whether the stadium is roofed. |
| `stadium_turf` | String | Stadium: pitch surface (null in the captures). |
| `stadium_id_city` | String | Stadium: city id (Utf8 join key). |
| `stadium_city_name` | String | Stadium: localised city name, JSON list of `{Locale, Description}` objects. |
| `stadium_id_country` | String | Stadium: three-letter FIFA country code, e.g. ARG. |
| `stadium_postal_code` | String | Stadium: postal code. |
| `stadium_street` | String | Stadium: street address. |
| `stadium_email` | String | Stadium: contact e-mail. |
| `stadium_fax` | String | Stadium: contact fax number. |
| `stadium_phone` | String | Stadium: contact phone number. |
| `stadium_affiliation_country` | String | Stadium: affiliation country (null in the captures). |
| `stadium_affiliation_region` | String | Stadium: affiliation region (null in the captures). |
| `stadium_latitude` | String | Stadium: latitude in decimal degrees. |
| `stadium_longitude` | String | Stadium: longitude in decimal degrees. |
| `stadium_length` | String | Stadium: pitch length (null in the captures). |
| `stadium_width` | String | Stadium: pitch width (null in the captures). |
| `stadium_properties_id_ifes` | String | Stadium: IFES cross-reference id (Utf8). |
| `stadium_is_updateable` | String | Stadium: isUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_calendar_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_calendar_matches-example}

```python
fifa_calendar_matches(count='3', id_competition='17', id_season='285023', language='en')
```

## fifa_competition

One competition by id.

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
| `id_competition` | String | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_confederation` | String | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `id_member_association` | String | Member-association codes (e.g. ENG), JSON list. |
| `id_owner` | String | Code of the body that owns the competition (FIFA or a confederation). |
| `gender` | Int64 | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `football_type` | Int64 | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `team_type` | Int64 | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `competition_type` | Int64 | Competition-type code (FIFA integer enum). |
| `age_type` | Int64 | Age-category code (FIFA integer enum; 7 = senior). |
| `display_order` | String | Sort order for display. |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_competition-example}

```python
fifa_competition(id_competition='17', language='en')
```

## fifa_competitions

All FIFA competitions (first page).

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
| `id_competition` | String | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_confederation` | String | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `id_member_association` | String | Member-association codes (e.g. ENG), JSON list. |
| `id_owner` | String | Code of the body that owns the competition (FIFA or a confederation). |
| `gender` | Int64 | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `football_type` | Int64 | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `team_type` | Int64 | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `competition_type` | Int64 | Competition-type code (FIFA integer enum). |
| `age_type` | Int64 | Age-category code (FIFA integer enum; 7 = senior). |
| `display_order` | Int64 | Sort order for display. |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_stats_perform` | String | Stats Perform (Opta) id of the record (Utf8 cross-reference). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_competitions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_competitions-example}

```python
fifa_competitions(count='3', language='en')
```

## fifa_live_football

Matches live right now across FIFA competitions.

**Endpoint URL:** `GET https://api.fifa.com/api/v3/live/football`

**Valid URL:** [https://api.fifa.com/api/v3/live/football?language=en](https://api.fifa.com/api/v3/live/football?language=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `language` | `language` |  |  | `Y` | Content language, e.g. en, es, fr. |

### Returns {#fifa_live_football-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_match` | String | FIFA match id (Utf8 join key). |
| `id_stage` | String | Stage id within the season (Utf8 join key). |
| `id_group` | String | Group id within the stage (Utf8 join key). |
| `id_season` | String | FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key). |
| `coverage_level` | Int64 | Data-coverage level of the match (integer). |
| `id_competition` | String | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `competition_name` | String | Localised competition name, JSON list of `{Locale, Description}` objects. |
| `season_name` | String | Localised season name, JSON list of `{Locale, Description}` objects. |
| `season_short_name` | String | Localised short season name, JSON list of `{Locale, Description}` objects (often empty). |
| `result_type` | Int64 | Result-type code (FIFA integer enum). |
| `match_day` | String | Match day (round) within the stage. |
| `match_number` | String | Match number within the season. |
| `home_team_penalty_score` | String | Home team: penalty shoot-out score (null when there was no shoot-out). |
| `away_team_penalty_score` | String | Away team: penalty shoot-out score (null when there was no shoot-out). |
| `aggregate_home_team_score` | String | Home team aggregate score over two legs (null for a single-leg tie). |
| `aggregate_away_team_score` | String | Away team aggregate score over two legs (null for a single-leg tie). |
| `weather` | String | Weather block (null when not reported). |
| `attendance` | String | Attendance. |
| `date` | String | Kick-off in UTC (ISO 8601). |
| `local_date` | String | Kick-off in venue local time (ISO 8601; the trailing Z is not a UTC marker). |
| `match_time` | String | Match clock as displayed, e.g. 98'. |
| `second_half_time` | String | Second-half timing field (null in the captures). |
| `first_half_time` | String | First-half timing field (null in the captures). |
| `first_half_extra_time` | Int64 | Stoppage time added to the first half, in minutes. |
| `second_half_extra_time` | Int64 | Stoppage time added to the second half, in minutes. |
| `winner` | String | Team id of the winner (Utf8; null for a draw or an unplayed match). |
| `period` | Int64 | Match period code (FIFA integer enum). |
| `territorial_possesion` | String | Territorial possession block (sic: the API's spelling; null in the captures). |
| `territorial_third_possesion` | String | Territorial possession by third (sic: the API's spelling; null in the captures). |
| `officials` | String | Match officials, JSON list (OfficialId, OfficialType, IdCountry, localised Name / NameShort). |
| `match_status` | Int64 | Match status code (FIFA integer enum: 0 = played, 1 = to be played). |
| `group_name` | String | Localised group name (e.g. Group A), JSON list of `{Locale, Description}` objects. |
| `stage_name` | String | Localised stage name (e.g. First Stage), JSON list of `{Locale, Description}` objects. |
| `officiality_status` | Int64 | Officiality code of the result (FIFA integer enum). |
| `time_defined` | Boolean | Whether the kick-off time is confirmed. |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `stadium_id_stadium` | String | Stadium: FIFA stadium id (Utf8 join key). |
| `stadium_name` | String | Stadium: localised name, JSON list of `{Locale, Description}` objects. |
| `stadium_capacity` | Int64 | Stadium: seating capacity. |
| `stadium_web_address` | String | Stadium: website URL. |
| `stadium_built` | String | Stadium: year built, as an ISO 8601 date. |
| `stadium_roof` | Boolean | Stadium: whether the stadium is roofed. |
| `stadium_turf` | String | Stadium: pitch surface (null in the captures). |
| `stadium_id_city` | String | Stadium: city id (Utf8 join key). |
| `stadium_city_name` | String | Stadium: localised city name, JSON list of `{Locale, Description}` objects. |
| `stadium_id_country` | String | Stadium: three-letter FIFA country code, e.g. ARG. |
| `stadium_postal_code` | String | Stadium: postal code. |
| `stadium_street` | String | Stadium: street address. |
| `stadium_email` | String | Stadium: contact e-mail. |
| `stadium_fax` | String | Stadium: contact fax number. |
| `stadium_phone` | String | Stadium: contact phone number. |
| `stadium_affiliation_country` | String | Stadium: affiliation country (null in the captures). |
| `stadium_affiliation_region` | String | Stadium: affiliation region (null in the captures). |
| `stadium_latitude` | Float64 | Stadium: latitude in decimal degrees. |
| `stadium_longitude` | Float64 | Stadium: longitude in decimal degrees. |
| `stadium_length` | String | Stadium: pitch length (null in the captures). |
| `stadium_width` | String | Stadium: pitch width (null in the captures). |
| `stadium_is_updateable` | String | Stadium: isUpdateable flag from the API (null in the captures). |
| `home_team_score` | Int64 | Home team: goals scored. |
| `home_team_side` | String | Home team: side marker (null in the captures). |
| `home_team_id_team` | String | Home team: FIFA team id (Utf8 join key). |
| `home_team_picture_url` | String | Home team: image URL template with `{format}` and `{size}` placeholders. |
| `home_team_id_country` | String | Home team: three-letter FIFA country code, e.g. ARG. |
| `home_team_team_type` | Int64 | Home team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `home_team_age_type` | Int64 | Home team: age-category code (FIFA integer enum; 7 = senior). |
| `home_team_tactics` | String | Home team: formation, e.g. 4-2-3-1. |
| `home_team_team_name` | String | Home team: localised team name, JSON list of `{Locale, Description}` objects. |
| `home_team_abbreviation` | String | Home team: abbreviation (e.g. ARG, FWC 2026). |
| `home_team_coaches` | String | Home team: coaches, JSON list (IdCoach, IdCountry, Role, localised Name / Alias, ...). |
| `home_team_players` | String | Home team: line-up, JSON list (IdPlayer, ShirtNumber, Status, Captain, Position, localised PlayerName, ...). |
| `home_team_bookings` | String | Home team: cards, JSON list (Card, Period, Minute, IdPlayer, IdTeam, ...). |
| `home_team_goals` | String | Home team: goals, JSON list (Type, Period, Minute, IdPlayer, IdAssistPlayer, IdTeam, ...). |
| `home_team_substitutions` | String | Home team: substitutions, JSON list (Period, Minute, IdPlayerOff, IdPlayerOn, Reason, ...). |
| `home_team_staffs` | String | Home team: team staff, JSON list (empty in the captures). |
| `home_team_football_type` | Int64 | Home team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `home_team_gender` | Int64 | Home team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `home_team_id_association` | String | Home team: three-letter code of the member association, e.g. ARG. |
| `home_team_short_club_name` | String | Home team: short English team name. |
| `away_team_score` | Int64 | Away team: goals scored. |
| `away_team_side` | String | Away team: side marker (null in the captures). |
| `away_team_id_team` | String | Away team: FIFA team id (Utf8 join key). |
| `away_team_picture_url` | String | Away team: image URL template with `{format}` and `{size}` placeholders. |
| `away_team_id_country` | String | Away team: three-letter FIFA country code, e.g. ARG. |
| `away_team_team_type` | Int64 | Away team: team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `away_team_age_type` | Int64 | Away team: age-category code (FIFA integer enum; 7 = senior). |
| `away_team_tactics` | String | Away team: formation, e.g. 4-2-3-1. |
| `away_team_team_name` | String | Away team: localised team name, JSON list of `{Locale, Description}` objects. |
| `away_team_abbreviation` | String | Away team: abbreviation (e.g. ARG, FWC 2026). |
| `away_team_coaches` | String | Away team: coaches, JSON list (IdCoach, IdCountry, Role, localised Name / Alias, ...). |
| `away_team_players` | String | Away team: line-up, JSON list (IdPlayer, ShirtNumber, Status, Captain, Position, localised PlayerName, ...). |
| `away_team_bookings` | String | Away team: cards, JSON list (Card, Period, Minute, IdPlayer, IdTeam, ...). |
| `away_team_goals` | String | Away team: goals, JSON list (Type, Period, Minute, IdPlayer, IdAssistPlayer, IdTeam, ...). |
| `away_team_substitutions` | String | Away team: substitutions, JSON list (Period, Minute, IdPlayerOff, IdPlayerOn, Reason, ...). |
| `away_team_staffs` | String | Away team: team staff, JSON list (empty in the captures). |
| `away_team_football_type` | Int64 | Away team: football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `away_team_gender` | Int64 | Away team: gender code (FIFA integer enum: 1 = men, 2 = women). |
| `away_team_id_association` | String | Away team: three-letter code of the member association, e.g. ARG. |
| `away_team_short_club_name` | String | Away team: short English team name. |
| `ball_possession_intervals` | String | Ball possession: possession per interval, JSON list. |
| `ball_possession_last_x` | String | Ball possession: possession over the most recent stretch, JSON list. |
| `ball_possession_overall_home` | Float64 | Ball possession: home team share of possession, percent. |
| `ball_possession_overall_away` | Float64 | Ball possession: away team share of possession, percent. |
| `properties_id_stats_perform` | String | Stats Perform (Opta) id of the record (Utf8 cross-reference). |
| `ball_possession` | String | Ball-possession block (null when not tracked). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_live_football-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_live_football-example}

```python
fifa_live_football(language='en')
```

## fifa_players_search

Players matching a name (first page).

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
| `id_player` | String | FIFA player id (Utf8 join key). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `alias` | String | Localised alias, JSON list of `{Locale, Description}` objects. |
| `birth_date` | String | Date of birth (ISO 8601). |
| `weight` | Float64 | Weight in kilograms. |
| `height` | Float64 | Height in centimetres. |
| `birth_place` | String | Place of birth (a city name or a country code). |
| `id_country` | String | Three-letter FIFA country code, e.g. ARG. |
| `international_caps` | Int64 | International appearances (caps). |
| `international_debut` | String | Date of the international debut (ISO 8601). |
| `top_competition_debut` | String | Debut date in a top competition (null in the captures). |
| `picture_url` | String | Image URL template with `{format}` and `{size}` placeholders. |
| `thumbnail_url` | String | Thumbnail image URL (null in the captures). |
| `twitter_account` | String | Twitter / X handle (null in the captures). |
| `preferred_foot` | Int64 | Preferred-foot code (FIFA integer enum; 9999 when unset). |
| `media_content` | String | Media content entries, JSON list (empty in the captures). |
| `localized_twitter_accounts` | String | Localised Twitter / X handles (null in the captures). |
| `goals` | Int64 | Goals credited to the player. |
| `player_picture` | String | Player picture block (null in the captures). |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |
| `properties_id_stats_perform` | String | Stats Perform (Opta) id of the record (Utf8 cross-reference). |
| `properties_stats_perform_ifes_id` | String | IFES id as carried by the Stats Perform feed (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_players_search-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_players_search-example}

```python
fifa_players_search(language='en', name='Messi')
```

## fifa_seasons

Seasons of a competition (first page).

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
| `id_season` | String | FIFA season id (285023 = FIFA World Cup 2026; Utf8 join key). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `short_name` | String | Localised short name, JSON list of `{Locale, Description}` objects (often empty). |
| `abbreviation` | String | Abbreviation (e.g. ARG, FWC 2026). |
| `id_member_association` | String | Member-association codes (e.g. ENG), JSON list. |
| `id_confederation` | String | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `id_competition` | String | FIFA competition id (17 = FIFA World Cup; Utf8 join key). |
| `start_date` | String | Start date (ISO 8601). |
| `end_date` | String | End date (ISO 8601). |
| `picture_url` | String | Image URL template with `{format}` and `{size}` placeholders. |
| `mascot_picture_url` | String | Mascot image URL template with `{format}` and `{size}` placeholders. |
| `match_ball_picture_url` | String | Match-ball image URL template with `{format}` and `{size}` placeholders. |
| `host_teams` | String | Host teams, JSON list of `{IdTeam}` objects. |
| `sport_type` | Int64 | Sport code (FIFA integer enum; 0 = football). |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |
| `properties_providers` | String | Data-provider tag of the record. |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_seasons-example}

```python
fifa_seasons(count='3', id_competition='17', language='en')
```

## fifa_stadiums

Stadiums (first page).

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
| `id_stadium` | String | FIFA stadium id (Utf8 join key). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `capacity` | String | Seating capacity. |
| `web_address` | String | Website URL. |
| `built` | String | Year built, as an ISO 8601 date. |
| `roof` | Boolean | Whether the stadium is roofed. |
| `turf` | String | Pitch surface (null in the captures). |
| `id_city` | String | City id (Utf8 join key). |
| `city_name` | String | Localised city name, JSON list of `{Locale, Description}` objects. |
| `id_country` | String | Three-letter FIFA country code, e.g. ARG. |
| `postal_code` | String | Postal code. |
| `street` | String | Street address. |
| `email` | String | Contact e-mail. |
| `fax` | String | Contact fax number. |
| `phone` | String | Contact phone number. |
| `affiliation_country` | String | Affiliation country (null in the captures). |
| `affiliation_region` | String | Affiliation region (null in the captures). |
| `latitude` | String | Latitude in decimal degrees. |
| `longitude` | String | Longitude in decimal degrees. |
| `length` | String | Pitch length (null in the captures). |
| `width` | String | Pitch width (null in the captures). |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_stadiums-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_stadiums-example}

```python
fifa_stadiums(count='3', language='en')
```

## fifa_team

One team by id.

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
| `id_team` | String | FIFA team id (Utf8 join key). |
| `id_confederation` | String | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `active_status` | Int64 | Activity-status code (FIFA integer enum). |
| `type` | Int64 | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `age_type` | Int64 | Age-category code (FIFA integer enum; 7 = senior). |
| `football_type` | Int64 | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `gender` | Int64 | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_association` | String | Three-letter code of the member association, e.g. ARG. |
| `id_city` | String | City id (Utf8 join key). |
| `headquarters` | String | Headquarters (null in the captures). |
| `training_centre` | String | Training centre (null in the captures). |
| `official_site` | String | Official website (null in the captures). |
| `city` | String | City of the team's headquarters. |
| `id_country` | String | Three-letter FIFA country code, e.g. ARG. |
| `postal_code` | String | Postal code. |
| `region_name` | String | Region name (null in the captures). |
| `short_club_name` | String | Short English team name. |
| `abbreviation` | String | Abbreviation (e.g. ARG, FWC 2026). |
| `street` | String | Street address. |
| `foundation_year` | Int64 | Year the team or association was founded. |
| `stadium` | String | Home stadium block (null in the captures). |
| `picture_url` | String | Image URL template with `{format}` and `{size}` placeholders. |
| `thumbnail_url` | String | Thumbnail image URL (null in the captures). |
| `display_name` | String | Localised display name (e.g. ARG), JSON list of `{Locale, Description}` objects. |
| `content` | String | Editorial content entries, JSON list (empty in the captures). |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |
| `properties_stats_perform_ifes_id` | String | IFES id as carried by the Stats Perform feed (Utf8). |
| `properties_id_stats_perform` | String | Stats Perform (Opta) id of the record (Utf8 cross-reference). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_team-example}

```python
fifa_team(id_team='43922', language='en')
```

## fifa_teams_search

Teams matching a name (first page).

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
| `id_team` | String | FIFA team id (Utf8 join key). |
| `id_confederation` | String | Confederation code (e.g. UEFA, CONMEBOL); a JSON list on competitions and seasons. |
| `active_status` | Int64 | Activity-status code (FIFA integer enum). |
| `type` | Int64 | Team-type code (FIFA integer enum: 0 = club, 1 = national team). |
| `age_type` | Int64 | Age-category code (FIFA integer enum; 7 = senior). |
| `football_type` | Int64 | Football discipline code (FIFA integer enum; 0 = eleven-a-side football). |
| `gender` | Int64 | Gender code (FIFA integer enum: 1 = men, 2 = women). |
| `name` | String | Localised name, JSON list of `{Locale, Description}` objects. |
| `id_association` | String | Three-letter code of the member association, e.g. ARG. |
| `id_city` | String | City id (Utf8 join key). |
| `headquarters` | String | Headquarters (null in the captures). |
| `training_centre` | String | Training centre (null in the captures). |
| `official_site` | String | Official website (null in the captures). |
| `city` | String | City of the team's headquarters. |
| `id_country` | String | Three-letter FIFA country code, e.g. ARG. |
| `postal_code` | String | Postal code. |
| `region_name` | String | Region name (null in the captures). |
| `short_club_name` | String | Short English team name. |
| `abbreviation` | String | Abbreviation (e.g. ARG, FWC 2026). |
| `street` | String | Street address. |
| `foundation_year` | Int64 | Year the team or association was founded. |
| `stadium` | String | Home stadium block (null in the captures). |
| `picture_url` | String | Image URL template with `{format}` and `{size}` placeholders. |
| `thumbnail_url` | String | Thumbnail image URL (null in the captures). |
| `display_name` | String | Localised display name (e.g. ARG), JSON list of `{Locale, Description}` objects. |
| `content` | String | Editorial content entries, JSON list (empty in the captures). |
| `is_updateable` | String | IsUpdateable flag from the API (null in the captures). |
| `properties_id_ifes` | String | IFES cross-reference id (Utf8). |
| `properties_stats_perform_ifes_id` | String | IFES id as carried by the Stats Perform feed (Utf8). |
| `properties_id_stats_perform` | String | Stats Perform (Opta) id of the record (Utf8 cross-reference). |

**`return_parsed=False`** — the raw JSON `Dict` (paginated routes return the first page; the API's paging request parameter is undocumented, see the spec's x-pagination).

### Raises {#fifa_teams_search-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The FIFA API returned 404 (unknown competition or team id). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fifa_teams_search-example}

```python
fifa_teams_search(language='en', name='Argentina')
```
