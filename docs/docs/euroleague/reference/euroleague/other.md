---
title: "EuroLeague — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Other"
sidebar_label: "Other"
sidebar_position: 2
description: "EuroLeague — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# EuroLeague — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Other

## euroleague_clubs

Clubs in a season.

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons/{season_code}/clubs`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/clubs](https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/clubs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |

### Returns {#euroleague_clubs-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `code` | String | EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `name` | String | Display name. |
| `abbreviated_name` | String | Abbreviated display name. |
| `editorial_name` | String | Editorial (long-form) display name. |
| `tv_code` | String | Three-letter broadcast abbreviation of the club. |
| `is_virtual` | Boolean | Whether the club is a placeholder rather than a real club. |
| `sponsor` | String | Club sponsor. |
| `club_permanent_name` | String | Permanent club name independent of sponsor naming. |
| `club_permanent_alias` | String | Permanent club alias independent of sponsor naming. |
| `address` | String | Street address. |
| `website` | String | Official website URL. |
| `tickets_url` | String | Ticketing URL. |
| `twitter_account` | String | Twitter / X handle. |
| `venue_code` | String | Code of the venue the club or game plays at (Utf8 join key). |
| `city` | String | City the club is based in. |
| `president` | String | Club president. |
| `phone` | String | Contact phone number. |
| `images_crest` | String | URL of the club crest image. |
| `country_code` | String | ISO country code. |
| `country_name` | String | Country name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_clubs-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_clubs-example}

```python
euroleague_clubs(competition_code='E', season_code='E2025')
```

## euroleague_competitions

Competitions (EuroLeague E, EuroCup U, ...).

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions](https://api-live.euroleague.net/v2/competitions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#euroleague_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Display name. |
| `code` | String | EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_competitions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_competitions-example}

```python
euroleague_competitions()
```

## euroleague_games

Games of a season.

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons/{season_code}/games`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games](https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |
| `limit` | `limit` |  |  | `Y` | Page size (number of rows to return). |
| `offset` | `offset` |  |  | `Y` | Row offset into the full list. |

### Returns {#euroleague_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `identifier` | String | Season-qualified game identifier, e.g. E2025_1. |
| `game_code` | String | Game number within the season (1-based; Utf8 join key). |
| `round` | Int64 | Round number within the season. |
| `round_alias` | String | Short alias of the round. |
| `round_name` | String | Display name of the round. |
| `played` | Boolean | Whether the game has been played. |
| `date` | String | Scheduled tip-off (ISO 8601, venue local time). |
| `confirmed_date` | Boolean | Whether the game date is confirmed. |
| `confirmed_hour` | Boolean | Whether the tip-off time is confirmed. |
| `local_time_zone` | Int64 | UTC offset of the venue, in hours. |
| `local_date` | String | Scheduled tip-off in venue local time (ISO 8601). |
| `utc_date` | String | Scheduled tip-off in UTC (ISO 8601). |
| `audience` | Int64 | Attendance. |
| `audience_confirmed` | Boolean | Whether the attendance figure is confirmed. |
| `social_feed` | String | Social-media hashtag or feed tag for the game. |
| `operations_code` | String | Engine operations code of the game. |
| `referee4` | String | Fourth referee (null unless a fourth official is assigned). |
| `is_neutral_venue` | Boolean | Whether the game is played at a neutral venue. |
| `game_status` | String | Game status (scheduled, live, result). |
| `season_name` | String | Season: display name. |
| `season_code` | String | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `season_alias` | String | Season: short display alias. |
| `season_competition_code` | String | Season: competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `season_year` | Int64 | Season: start year of the season. |
| `season_start_date` | String | Season: start date (ISO 8601). |
| `group_id` | String | Group: provider identifier for the entity (Utf8 join key). |
| `group_order` | Int64 | Group: display order within the roster. |
| `group_name` | String | Group: display name. |
| `group_raw_name` | String | Group: group name as stored by the engine. |
| `phase_type_code` | String | Phase type code (RS = regular season, PO = playoffs, ...). |
| `phase_type_alias` | String | Phase type: short display alias. |
| `phase_type_name` | String | Phase type: display name. |
| `phase_type_is_group_phase` | Boolean | Phase type: whether the phase is played in groups. |
| `local_club_code` | String | Home side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `local_club_name` | String | Home side: club: display name. |
| `local_club_abbreviated_name` | String | Home side: club: abbreviated display name. |
| `local_club_editorial_name` | String | Home side: club: editorial (long-form) display name. |
| `local_club_tv_code` | String | Home side: club: three-letter broadcast abbreviation of the club. |
| `local_club_is_virtual` | Boolean | Home side: club: whether the club is a placeholder rather than a real club. |
| `local_club_images_crest` | String | Home side: club: URL of the club crest image. |
| `local_score` | Int64 | Home side: final score of the side. |
| `local_standings_score` | Int64 | Home side: score of the side as counted for the standings. |
| `local_partials_partials1` | Int64 | Home side: quarter scores: points scored in the 1st quarter. |
| `local_partials_partials2` | Int64 | Home side: quarter scores: points scored in the 2nd quarter. |
| `local_partials_partials3` | Int64 | Home side: quarter scores: points scored in the 3rd quarter. |
| `local_partials_partials4` | Int64 | Home side: quarter scores: points scored in the 4th quarter. |
| `road_club_code` | String | Away side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `road_club_name` | String | Away side: club: display name. |
| `road_club_abbreviated_name` | String | Away side: club: abbreviated display name. |
| `road_club_editorial_name` | String | Away side: club: editorial (long-form) display name. |
| `road_club_tv_code` | String | Away side: club: three-letter broadcast abbreviation of the club. |
| `road_club_is_virtual` | Boolean | Away side: club: whether the club is a placeholder rather than a real club. |
| `road_club_images_crest` | String | Away side: club: URL of the club crest image. |
| `road_score` | Int64 | Away side: final score of the side. |
| `road_standings_score` | Int64 | Away side: score of the side as counted for the standings. |
| `road_partials_partials1` | Int64 | Away side: quarter scores: points scored in the 1st quarter. |
| `road_partials_partials2` | Int64 | Away side: quarter scores: points scored in the 2nd quarter. |
| `road_partials_partials3` | Int64 | Away side: quarter scores: points scored in the 3rd quarter. |
| `road_partials_partials4` | Int64 | Away side: quarter scores: points scored in the 4th quarter. |
| `referee1_code` | String | First referee: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `referee1_name` | String | First referee: display name. |
| `referee1_alias` | String | First referee: short display alias. |
| `referee1_country_code` | String | First referee: ISO country code. |
| `referee1_country_name` | String | First referee: country name. |
| `referee1_images_vertical_small` | String | First referee: URL of the small portrait image. |
| `referee1_active` | Boolean | First referee: whether the record is currently active. |
| `referee2_code` | String | Second referee: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `referee2_name` | String | Second referee: display name. |
| `referee2_alias` | String | Second referee: short display alias. |
| `referee2_country_code` | String | Second referee: ISO country code. |
| `referee2_country_name` | String | Second referee: country name. |
| `referee2_images_vertical_small` | String | Second referee: URL of the small portrait image. |
| `referee2_active` | Boolean | Second referee: whether the record is currently active. |
| `referee3_code` | String | Third referee: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `referee3_name` | String | Third referee: display name. |
| `referee3_alias` | String | Third referee: short display alias. |
| `referee3_country_code` | String | Third referee: ISO country code. |
| `referee3_country_name` | String | Third referee: country name. |
| `referee3_images_vertical_small` | String | Third referee: URL of the small portrait image. |
| `referee3_active` | Boolean | Third referee: whether the record is currently active. |
| `venue_name` | String | Venue: display name. |
| `venue_code` | String | Code of the venue the club or game plays at (Utf8 join key). |
| `venue_capacity` | Int64 | Venue: seating capacity of the venue. |
| `venue_address` | String | Venue: street address. |
| `venue_images_medium` | String | Venue: URL of the medium-size venue image. |
| `venue_active` | Boolean | Venue: whether the record is currently active. |
| `venue_notes` | String | Venue: venue notes. |
| `winner_code` | String | Winning club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `winner_name` | String | Winning club: display name. |
| `winner_abbreviated_name` | String | Winning club: abbreviated display name. |
| `winner_editorial_name` | String | Winning club: editorial (long-form) display name. |
| `winner_tv_code` | String | Winning club: three-letter broadcast abbreviation of the club. |
| `winner_is_virtual` | Boolean | Winning club: whether the club is a placeholder rather than a real club. |
| `winner_images_crest` | String | Winning club: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_games-example}

```python
euroleague_games(competition_code='E', season_code='E2025')
```

## euroleague_people

People (players, coaches) in a season.

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons/{season_code}/people`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/people](https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/people)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |
| `limit` | `limit` |  |  | `Y` | Page size (number of rows to return). |
| `offset` | `offset` |  |  | `Y` | Row offset into the full list. |

### Returns {#euroleague_people-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | String | Person type code (J = player, E = coach, ...). |
| `type_name` | String | Person type name. |
| `active` | Boolean | Whether the record is currently active. |
| `start_date` | String | Start date (ISO 8601). |
| `end_date` | String | End date (ISO 8601). |
| `order` | Int64 | Display order within the roster. |
| `dorsal` | String | Jersey number as displayed. |
| `dorsal_raw` | String | Jersey number as stored by the engine. |
| `position` | Int64 | Position code of the player (engine integer). |
| `position_name` | String | Position name of the player. |
| `last_team` | String | Previous club of the person. |
| `external_id` | String | External (statistics-provider) identifier of the person (Utf8 join key). |
| `person_code` | String | Person: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `person_name` | String | Person: display name. |
| `person_alias` | String | Person: short display alias. |
| `person_alias_raw` | String | Person: short alias as stored by the engine. |
| `person_passport_name` | String | Person: given name as on the passport. |
| `person_passport_surname` | String | Person: surname as on the passport. |
| `person_jersey_name` | String | Person: name printed on the jersey. |
| `person_abbreviated_name` | String | Person: abbreviated display name. |
| `person_country_code` | String | Person: ISO country code. |
| `person_country_name` | String | Person: country name. |
| `person_height` | Int64 | Person: height in centimetres. |
| `person_weight` | Int64 | Person: weight in kilograms. |
| `person_birth_date` | String | Person: date of birth (ISO 8601). |
| `person_birth_country_code` | String | Person: birth country: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `person_birth_country_name` | String | Person: birth country: display name. |
| `person_twitter_account` | String | Person: twitter / X handle. |
| `person_instagram_account` | String | Person: instagram handle. |
| `person_facebook_account` | String | Person: facebook handle. |
| `person_is_referee` | Boolean | Person: whether the person is a referee. |
| `club_code` | String | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | String | Club: display name. |
| `club_abbreviated_name` | String | Club: abbreviated display name. |
| `club_editorial_name` | String | Club: editorial (long-form) display name. |
| `club_tv_code` | String | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | Boolean | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | String | Club: URL of the club crest image. |
| `season_name` | String | Season: display name. |
| `season_code` | String | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `season_alias` | String | Season: short display alias. |
| `season_competition_code` | String | Season: competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `season_year` | Int64 | Season: start year of the season. |
| `season_start_date` | String | Season: start date (ISO 8601). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_people-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_people-example}

```python
euroleague_people(competition_code='E', season_code='E2025')
```

## euroleague_player_stats

Season player stats, traditional (box-score totals or per-game averages) or advanced (eFG%, TS%, rebound / assist / turnover rates), one row per player.

**Endpoint URL:** `GET https://api-live.euroleague.net/v3/competitions/{competition_code}/statistics/players/{mode}`

**Valid URL:** [https://api-live.euroleague.net/v3/competitions/E/statistics/players/traditional?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame](https://api-live.euroleague.net/v3/competitions/E/statistics/players/traditional?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `mode` | `mode` |  |  | `Y` | traditional (default) or advanced; the columns depend on it (see Returns). |
| `SeasonMode` | `season_mode` |  |  | `Y` | Required. `Single` as captured (other values unverified). Default Single. |
| `SeasonCode` | `season_code` |  |  | `Y` | Required. Competition code + start year, e.g. E2025 for 2025-26. |
| `statisticMode` | `statistic_mode` |  |  | `Y` | Required. `PerGame` as captured (other values unverified). Default PerGame. |
| `limit` | `limit` |  |  | `Y` | Page size (the capture used 3). |
| `offset` | `offset` |  |  | `Y` | Row offset into the full list. |

### Returns {#euroleague_player_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` whose columns depend on `mode` (one table per value below); pass `return_as_pandas=True` for a `pandas.DataFrame`.

**traditional**

| col_name | type | description |
|---|---|---|
| `player_ranking` | Int64 | Rank of the player on the requested statistic mode. |
| `games_played` | Float64 | Games played. |
| `games_started` | Float64 | Games started. |
| `minutes_played` | Float64 | Minutes played. |
| `points_scored` | Float64 | Points scored. |
| `two_pointers_made` | Float64 | Two-point field goals made. |
| `two_pointers_attempted` | Float64 | Two-point field goals attempted. |
| `two_pointers_percentage` | String | Two-point field-goal percentage, formatted. |
| `three_pointers_made` | Float64 | Three-point field goals made. |
| `three_pointers_attempted` | Float64 | Three-point field goals attempted. |
| `three_pointers_percentage` | String | Three-point field-goal percentage, formatted. |
| `free_throws_made` | Float64 | Free throws made. |
| `free_throws_attempted` | Float64 | Free throws attempted. |
| `free_throws_percentage` | String | Free-throw percentage, formatted. |
| `offensive_rebounds` | Float64 | Offensive rebounds. |
| `defensive_rebounds` | Float64 | Defensive rebounds. |
| `total_rebounds` | Float64 | Total rebounds. |
| `assists` | Float64 | Assists. |
| `steals` | Float64 | Steals. |
| `turnovers` | Float64 | Turnovers. |
| `blocks` | Float64 | Blocks made. |
| `blocks_against` | Float64 | Shots blocked by the opponent. |
| `fouls_commited` | Float64 | Personal fouls committed. |
| `fouls_drawn` | Float64 | Fouls drawn. |
| `pir` | Float64 | Performance index rating (PIR). |
| `player_code` | String | Player: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `player_name` | String | Player: display name. |
| `player_age` | Int64 | Age of the player in years. |
| `player_image_url` | String | Player: URL of the image. |
| `player_team_code` | String | Player: euroLeague club code (Utf8 join key). |
| `player_team_tv_codes` | String | Player: three-letter broadcast abbreviation(s) of the club. |
| `player_team_name` | String | Player: club display name. |
| `player_team_image_url` | String | Player: URL of the club crest image. |

**advanced**

| col_name | type | description |
|---|---|---|
| `player_ranking` | Int64 | Rank of the player on the requested statistic mode. |
| `games_played` | Float64 | Games played. |
| `minutes_played` | Float64 | Minutes played. |
| `effective_field_goal_percentage` | String | Effective field-goal percentage, formatted. |
| `true_shooting_percentage` | String | True shooting percentage, formatted. |
| `offensive_rebounds_percentage` | String | Offensive rebound percentage, formatted. |
| `defensive_rebounds_percentage` | String | Defensive rebound percentage, formatted. |
| `rebounds_percentage` | String | Total rebound percentage, formatted. |
| `assists_to_turnovers_ratio` | Float64 | Assist-to-turnover ratio. |
| `assists_ratio` | String | Assist ratio (assists per 100 possessions used), formatted. |
| `turnovers_ratio` | String | Turnover ratio (turnovers per 100 possessions used), formatted. |
| `two_point_attempts_ratio` | String | Share of field-goal attempts that are two-pointers, formatted. |
| `three_point_attempts_ratio` | String | Share of field-goal attempts that are three-pointers, formatted. |
| `free_throws_rate` | String | Free-throw rate (free-throw attempts per field-goal attempt), formatted. |
| `possesions` | Float64 | Possessions (sic: the API spells it this way). |
| `player_code` | String | Player: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `player_name` | String | Player: display name. |
| `player_age` | Int64 | Age of the player in years. |
| `player_image_url` | String | Player: URL of the image. |
| `player_team_code` | String | Player: euroLeague club code (Utf8 join key). |
| `player_team_tv_codes` | String | Player: three-letter broadcast abbreviation(s) of the club. |
| `player_team_name` | String | Player: club display name. |
| `player_team_image_url` | String | Player: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_player_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_player_stats-example}

```python
euroleague_player_stats(competition_code='E', season_code='E2025')
```

## euroleague_rounds

Rounds of a season.

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons/{season_code}/rounds`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/rounds](https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/rounds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |

### Returns {#euroleague_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `season_code` | String | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `phase_type_code` | String | Phase type code (RS = regular season, PO = playoffs, ...). |
| `round` | Int64 | Round number within the season. |
| `index` | Int64 | Ordinal position of the round within the season. |
| `name` | String | Display name. |
| `min_game_start_date` | String | Earliest game start in the round (ISO 8601). |
| `max_game_start_date` | String | Latest game start in the round (ISO 8601). |
| `dates_formmated` | String | Human-readable date range of the round (sic: the API spells it this way). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_rounds-example}

```python
euroleague_rounds(competition_code='E', season_code='E2025')
```

## euroleague_seasons

Seasons of a competition.

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons](https://api-live.euroleague.net/v2/competitions/E/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |

### Returns {#euroleague_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Display name. |
| `code` | String | EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `alias` | String | Short display alias. |
| `competition_code` | String | Competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `year` | Int64 | Start year of the season. |
| `start_date` | String | Start date (ISO 8601). |
| `activation_date` | String | Date the season was activated in the engine (ISO 8601). |
| `end_date` | String | End date (ISO 8601). |
| `winner` | String | Winning club of the season (null while in progress). |
| `winner_code` | String | Winning club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `winner_name` | String | Winning club: display name. |
| `winner_abbreviated_name` | String | Winning club: abbreviated display name. |
| `winner_editorial_name` | String | Winning club: editorial (long-form) display name. |
| `winner_tv_code` | String | Winning club: three-letter broadcast abbreviation of the club. |
| `winner_is_virtual` | Boolean | Winning club: whether the club is a placeholder rather than a real club. |
| `winner_images_crest` | String | Winning club: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_seasons-example}

```python
euroleague_seasons(competition_code='E')
```

## euroleague_standings

Standings as of a round: basic (W-L, points, home/away/last-10 records), calendar (per-round result streaks), streaks (longest win/loss runs) or aheadbehind (records when ahead/behind/tied after Q1, the half and Q3).

**Endpoint URL:** `GET https://api-live.euroleague.net/v3/competitions/{competition_code}/seasons/{season_code}/rounds/{round}/{kind}`

**Valid URL:** [https://api-live.euroleague.net/v3/competitions/E/seasons/E2025/rounds/1/basicstandings](https://api-live.euroleague.net/v3/competitions/E/seasons/E2025/rounds/1/basicstandings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |
| `round` | `round` |  | `Y` |  | Round number within the season (the `round` field of /rounds); the standings are as of this round. |
| `kind` | `kind` |  |  | `Y` | Standings table: basicstandings (default), calendarstandings, streaks or aheadbehind; the columns depend on it (see Returns). |

### Returns {#euroleague_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` whose columns depend on `kind` (one table per value below); pass `return_as_pandas=True` for a `pandas.DataFrame`.

**basicstandings**

| col_name | type | description |
|---|---|---|
| `position` | Int64 | Position code of the player (engine integer). |
| `position_change` | String | Movement since the previous round (Up, Down, Equal). |
| `games_played` | Int64 | Games played. |
| `games_won` | Int64 | Games won. |
| `games_lost` | Int64 | Games lost. |
| `qualified` | Boolean | Whether the team has clinched qualification for the next phase. |
| `win_percentage` | String | Win percentage, formatted (e.g. 100%). |
| `points_difference` | String | Points for minus points against, signed and formatted (e.g. +19). |
| `points_for` | Int64 | Points scored. |
| `points_against` | Int64 | Points conceded. |
| `home_record` | String | Home win-loss record (W-L). |
| `away_record` | String | Away win-loss record (W-L). |
| `neutral_record` | String | Neutral-venue win-loss record (W-L). |
| `overtime_record` | String | Overtime win-loss record (W-L). |
| `last_ten_record` | String | Win-loss record over the last ten games (W-L). |
| `group_name` | String | Group: display name. |
| `last5_form` | String | Results of the last five games, oldest first (W / L), JSON-encoded. |
| `club_code` | String | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | String | Club: display name. |
| `club_abbreviated_name` | String | Club: abbreviated display name. |
| `club_editorial_name` | String | Club: editorial (long-form) display name. |
| `club_tv_code` | String | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | Boolean | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | String | Club: URL of the club crest image. |

**calendarstandings**

| col_name | type | description |
|---|---|---|
| `position` | Int64 | Position code of the player (engine integer). |
| `position_change` | String | Movement since the previous round (Up, Down, Equal). |
| `games_played` | Int64 | Games played. |
| `games_won` | Int64 | Games won. |
| `games_lost` | Int64 | Games lost. |
| `qualified` | Boolean | Whether the team has clinched qualification for the next phase. |
| `group_name` | String | Group: display name. |
| `streaks` | String | Per-round result streaks: start date, end date and W-L record of each run, JSON-encoded. |
| `club_code` | String | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | String | Club: display name. |
| `club_abbreviated_name` | String | Club: abbreviated display name. |
| `club_editorial_name` | String | Club: editorial (long-form) display name. |
| `club_tv_code` | String | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | Boolean | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | String | Club: URL of the club crest image. |

**streaks**

| col_name | type | description |
|---|---|---|
| `position` | Int64 | Position code of the player (engine integer). |
| `position_change` | String | Movement since the previous round (Up, Down, Equal). |
| `games_played` | Int64 | Games played. |
| `games_won` | Int64 | Games won. |
| `games_lost` | Int64 | Games lost. |
| `qualified` | Boolean | Whether the team has clinched qualification for the next phase. |
| `home_record` | String | Home win-loss record (W-L). |
| `away_record` | String | Away win-loss record (W-L). |
| `last10` | String | Win-loss record over the last ten games (W-L). |
| `home_last5` | String | Win-loss record over the last five home games (W-L). |
| `away_last5` | String | Win-loss record over the last five away games (W-L). |
| `longest_wins_streak_current_season` | Int64 | Longest winning streak this season. |
| `longest_loses_streak_current_season` | Int64 | Longest losing streak this season. |
| `longest_wins_streak_any_season` | Int64 | Longest winning streak in any season. |
| `longest_loses_streak_any_season` | Int64 | Longest losing streak in any season. |
| `group_name` | String | Group: display name. |
| `club_code` | String | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | String | Club: display name. |
| `club_abbreviated_name` | String | Club: abbreviated display name. |
| `club_editorial_name` | String | Club: editorial (long-form) display name. |
| `club_tv_code` | String | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | Boolean | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | String | Club: URL of the club crest image. |

**aheadbehind**

| col_name | type | description |
|---|---|---|
| `position` | Int64 | Position code of the player (engine integer). |
| `position_change` | String | Movement since the previous round (Up, Down, Equal). |
| `games_played` | Int64 | Games played. |
| `games_won` | Int64 | Games won. |
| `games_lost` | Int64 | Games lost. |
| `qualified` | Boolean | Whether the team has clinched qualification for the next phase. |
| `wins_percentage` | String | Win percentage, formatted (e.g. 100%). |
| `quater1_ahead` | String | Win-loss record when ahead after the 1st quarter (W-L; sic: the API spells quarter this way). |
| `quater1_behind` | String | Win-loss record when behind after the 1st quarter (W-L; sic: the API spells quarter this way). |
| `quater1_tied` | String | Win-loss record when tied after the 1st quarter (W-L; sic: the API spells quarter this way). |
| `half1_ahead` | String | Win-loss record when ahead at the half (W-L; sic: the API spells quarter this way). |
| `half1_behind` | String | Win-loss record when behind at the half (W-L; sic: the API spells quarter this way). |
| `half1_tied` | String | Win-loss record when tied at the half (W-L; sic: the API spells quarter this way). |
| `quater3_ahead` | String | Win-loss record when ahead after the 3rd quarter (W-L; sic: the API spells quarter this way). |
| `quater3_behind` | String | Win-loss record when behind after the 3rd quarter (W-L; sic: the API spells quarter this way). |
| `quater3_tied` | String | Win-loss record when tied after the 3rd quarter (W-L; sic: the API spells quarter this way). |
| `group_name` | String | Group: display name. |
| `club_code` | String | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | String | Club: display name. |
| `club_abbreviated_name` | String | Club: abbreviated display name. |
| `club_editorial_name` | String | Club: editorial (long-form) display name. |
| `club_tv_code` | String | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | Boolean | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | String | Club: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_standings-example}

```python
euroleague_standings(competition_code='E', round=1, season_code='E2025')
```

## euroleague_team_stats

Season team stats, traditional (box-score totals or per-game averages) or advanced (eFG%, TS%, four-factor style rates), one row per team.

**Endpoint URL:** `GET https://api-live.euroleague.net/v3/competitions/{competition_code}/statistics/teams/{mode}`

**Valid URL:** [https://api-live.euroleague.net/v3/competitions/E/statistics/teams/traditional?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame](https://api-live.euroleague.net/v3/competitions/E/statistics/teams/traditional?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `mode` | `mode` |  |  | `Y` | traditional (default) or advanced; the columns depend on it (see Returns). |
| `SeasonMode` | `season_mode` |  |  | `Y` | Required. `Single` as captured (other values unverified). Default Single. |
| `SeasonCode` | `season_code` |  |  | `Y` | Required. Competition code + start year, e.g. E2025 for 2025-26. |
| `statisticMode` | `statistic_mode` |  |  | `Y` | Required. `PerGame` as captured (other values unverified). Default PerGame. |
| `limit` | `limit` |  |  | `Y` | Page size (the capture used 3). |
| `offset` | `offset` |  |  | `Y` | Row offset into the full list. |

### Returns {#euroleague_team_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` whose columns depend on `mode` (one table per value below); pass `return_as_pandas=True` for a `pandas.DataFrame`.

**traditional**

| col_name | type | description |
|---|---|---|
| `team_ranking` | Int64 | Rank of the team on the requested statistic mode. |
| `games_played` | Float64 | Games played. |
| `minutes_played` | Float64 | Minutes played. |
| `points_scored` | Float64 | Points scored. |
| `two_pointers_made` | Float64 | Two-point field goals made. |
| `two_pointers_attempted` | Float64 | Two-point field goals attempted. |
| `two_pointers_percentage` | String | Two-point field-goal percentage, formatted. |
| `three_pointers_made` | Float64 | Three-point field goals made. |
| `three_pointers_attempted` | Float64 | Three-point field goals attempted. |
| `three_pointers_percentage` | String | Three-point field-goal percentage, formatted. |
| `free_throws_made` | Float64 | Free throws made. |
| `free_throws_attempted` | Float64 | Free throws attempted. |
| `free_throws_percentage` | String | Free-throw percentage, formatted. |
| `offensive_rebounds` | Float64 | Offensive rebounds. |
| `defensive_rebounds` | Float64 | Defensive rebounds. |
| `total_rebounds` | Float64 | Total rebounds. |
| `assists` | Float64 | Assists. |
| `steals` | Float64 | Steals. |
| `turnovers` | Float64 | Turnovers. |
| `blocks` | Float64 | Blocks made. |
| `blocks_against` | Float64 | Shots blocked by the opponent. |
| `fouls_commited` | Float64 | Personal fouls committed. |
| `fouls_drawn` | Float64 | Fouls drawn. |
| `pir` | Float64 | Performance index rating (PIR). |
| `team_code` | String | EuroLeague club code (Utf8 join key). |
| `team_tv_codes` | String | Three-letter broadcast abbreviation(s) of the club. |
| `team_name` | String | Club display name. |
| `team_image_url` | String | URL of the club crest image. |

**advanced**

| col_name | type | description |
|---|---|---|
| `team_ranking` | Int64 | Rank of the team on the requested statistic mode. |
| `games_played` | Float64 | Games played. |
| `effective_field_goal_percentage` | String | Effective field-goal percentage, formatted. |
| `true_shooting_percentage` | String | True shooting percentage, formatted. |
| `offensive_rebounds_percentage` | String | Offensive rebound percentage, formatted. |
| `defensive_rebounds_percentage` | String | Defensive rebound percentage, formatted. |
| `rebounds_percentage` | String | Total rebound percentage, formatted. |
| `assists_to_turnovers_ratio` | Float64 | Assist-to-turnover ratio. |
| `assists_ratio` | String | Assist ratio (assists per 100 possessions used), formatted. |
| `turnovers_ratio` | String | Turnover ratio (turnovers per 100 possessions used), formatted. |
| `two_point_rate` | String | Share of field-goal attempts that are two-pointers, formatted. |
| `three_point_rate` | String | Share of field-goal attempts that are three-pointers, formatted. |
| `free_throws_rate` | String | Free-throw rate (free-throw attempts per field-goal attempt), formatted. |
| `points_from_two_pointers_percentage` | String | Share of points from two-pointers, formatted. |
| `points_from_three_pointers_percentage` | String | Share of points from three-pointers, formatted. |
| `points_from_free_throws_percentage` | String | Share of points from free throws, formatted. |
| `team_code` | String | EuroLeague club code (Utf8 join key). |
| `team_tv_codes` | String | Three-letter broadcast abbreviation(s) of the club. |
| `team_name` | String | Club display name. |
| `team_image_url` | String | URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#euroleague_team_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The EuroLeague API returned 404 (unknown competition, season, round or game code). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#euroleague_team_stats-example}

```python
euroleague_team_stats(competition_code='E', season_code='E2025')
```
