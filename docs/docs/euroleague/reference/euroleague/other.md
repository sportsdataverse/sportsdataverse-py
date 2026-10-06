---
title: "EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Other"
sidebar_label: "Other"
sidebar_position: 2
description: "EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — Other

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
| `code` | character | EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `name` | character | Display name. |
| `abbreviated_name` | character | Abbreviated display name. |
| `editorial_name` | character | Editorial (long-form) display name. |
| `tv_code` | character | Three-letter broadcast abbreviation of the club. |
| `is_virtual` | logical | Whether the club is a placeholder rather than a real club. |
| `sponsor` | character | Club sponsor. |
| `club_permanent_name` | character | Permanent club name independent of sponsor naming. |
| `club_permanent_alias` | character | Permanent club alias independent of sponsor naming. |
| `address` | character | Street address. |
| `website` | character | Official website URL. |
| `tickets_url` | character | Ticketing URL. |
| `twitter_account` | character | Twitter / X handle. |
| `venue_code` | character | Code of the venue the club or game plays at (Utf8 join key). |
| `city` | character | City the club is based in. |
| `president` | character | Club president. |
| `phone` | character | Contact phone number. |
| `images_crest` | character | URL of the club crest image. |
| `country_code` | character | ISO country code. |
| `country_name` | character | Country name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_clubs-example}

```python
euroleague_clubs(competition_code='E', season_code='E2025')
```

_Last validated n/a._

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
| `name` | character | Display name. |
| `code` | character | EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_competitions-example}

```python
euroleague_competitions()
```

_Last validated n/a._

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
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `identifier` | character | Season-qualified game identifier, e.g. E2025_1. |
| `game_code` | character | Game number within the season (1-based; Utf8 join key). |
| `round` | integer | Round number within the season. |
| `round_alias` | character | Short alias of the round. |
| `round_name` | character | Display name of the round. |
| `played` | logical | Whether the game has been played. |
| `date` | character | Scheduled tip-off (ISO 8601, venue local time). |
| `confirmed_date` | logical | Whether the game date is confirmed. |
| `confirmed_hour` | logical | Whether the tip-off time is confirmed. |
| `local_time_zone` | integer | UTC offset of the venue, in hours. |
| `local_date` | character | Scheduled tip-off in venue local time (ISO 8601). |
| `utc_date` | character | Scheduled tip-off in UTC (ISO 8601). |
| `audience` | integer | Attendance. |
| `audience_confirmed` | logical | Whether the attendance figure is confirmed. |
| `social_feed` | character | Social-media hashtag or feed tag for the game. |
| `operations_code` | character | Engine operations code of the game. |
| `referee4` | character | Fourth referee (null unless a fourth official is assigned). |
| `is_neutral_venue` | logical | Whether the game is played at a neutral venue. |
| `game_status` | character | Game status (scheduled, live, result). |
| `season_name` | character | Season: display name. |
| `season_code` | character | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `season_alias` | character | Season: short display alias. |
| `season_competition_code` | character | Season: competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `season_year` | integer | Season: start year of the season. |
| `season_start_date` | character | Season: start date (ISO 8601). |
| `group_id` | character | Group: provider identifier for the entity (Utf8 join key). |
| `group_order` | integer | Group: display order within the roster. |
| `group_name` | character | Group: display name. |
| `group_raw_name` | character | Group: group name as stored by the engine. |
| `phase_type_code` | character | Phase type code (RS = regular season, PO = playoffs, ...). |
| `phase_type_alias` | character | Phase type: short display alias. |
| `phase_type_name` | character | Phase type: display name. |
| `phase_type_is_group_phase` | logical | Phase type: whether the phase is played in groups. |
| `local_club_code` | character | Home side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `local_club_name` | character | Home side: club: display name. |
| `local_club_abbreviated_name` | character | Home side: club: abbreviated display name. |
| `local_club_editorial_name` | character | Home side: club: editorial (long-form) display name. |
| `local_club_tv_code` | character | Home side: club: three-letter broadcast abbreviation of the club. |
| `local_club_is_virtual` | logical | Home side: club: whether the club is a placeholder rather than a real club. |
| `local_club_images_crest` | character | Home side: club: URL of the club crest image. |
| `local_score` | integer | Home side: final score of the side. |
| `local_standings_score` | integer | Home side: score of the side as counted for the standings. |
| `local_partials_partials1` | integer | Home side: quarter scores: points scored in the 1st quarter. |
| `local_partials_partials2` | integer | Home side: quarter scores: points scored in the 2nd quarter. |
| `local_partials_partials3` | integer | Home side: quarter scores: points scored in the 3rd quarter. |
| `local_partials_partials4` | integer | Home side: quarter scores: points scored in the 4th quarter. |
| `road_club_code` | character | Away side: club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `road_club_name` | character | Away side: club: display name. |
| `road_club_abbreviated_name` | character | Away side: club: abbreviated display name. |
| `road_club_editorial_name` | character | Away side: club: editorial (long-form) display name. |
| `road_club_tv_code` | character | Away side: club: three-letter broadcast abbreviation of the club. |
| `road_club_is_virtual` | logical | Away side: club: whether the club is a placeholder rather than a real club. |
| `road_club_images_crest` | character | Away side: club: URL of the club crest image. |
| `road_score` | integer | Away side: final score of the side. |
| `road_standings_score` | integer | Away side: score of the side as counted for the standings. |
| `road_partials_partials1` | integer | Away side: quarter scores: points scored in the 1st quarter. |
| `road_partials_partials2` | integer | Away side: quarter scores: points scored in the 2nd quarter. |
| `road_partials_partials3` | integer | Away side: quarter scores: points scored in the 3rd quarter. |
| `road_partials_partials4` | integer | Away side: quarter scores: points scored in the 4th quarter. |
| `referee1_code` | character | First referee: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `referee1_name` | character | First referee: display name. |
| `referee1_alias` | character | First referee: short display alias. |
| `referee1_country_code` | character | First referee: ISO country code. |
| `referee1_country_name` | character | First referee: country name. |
| `referee1_images_vertical_small` | character | First referee: URL of the small portrait image. |
| `referee1_active` | logical | First referee: whether the record is currently active. |
| `referee2_code` | character | Second referee: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `referee2_name` | character | Second referee: display name. |
| `referee2_alias` | character | Second referee: short display alias. |
| `referee2_country_code` | character | Second referee: ISO country code. |
| `referee2_country_name` | character | Second referee: country name. |
| `referee2_images_vertical_small` | character | Second referee: URL of the small portrait image. |
| `referee2_active` | logical | Second referee: whether the record is currently active. |
| `referee3_code` | character | Third referee: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `referee3_name` | character | Third referee: display name. |
| `referee3_alias` | character | Third referee: short display alias. |
| `referee3_country_code` | character | Third referee: ISO country code. |
| `referee3_country_name` | character | Third referee: country name. |
| `referee3_images_vertical_small` | character | Third referee: URL of the small portrait image. |
| `referee3_active` | logical | Third referee: whether the record is currently active. |
| `venue_name` | character | Venue: display name. |
| `venue_code` | character | Code of the venue the club or game plays at (Utf8 join key). |
| `venue_capacity` | integer | Venue: seating capacity of the venue. |
| `venue_address` | character | Venue: street address. |
| `venue_images_medium` | character | Venue: URL of the medium-size venue image. |
| `venue_active` | logical | Venue: whether the record is currently active. |
| `venue_notes` | character | Venue: venue notes. |
| `winner_code` | character | Winning club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `winner_name` | character | Winning club: display name. |
| `winner_abbreviated_name` | character | Winning club: abbreviated display name. |
| `winner_editorial_name` | character | Winning club: editorial (long-form) display name. |
| `winner_tv_code` | character | Winning club: three-letter broadcast abbreviation of the club. |
| `winner_is_virtual` | logical | Winning club: whether the club is a placeholder rather than a real club. |
| `winner_images_crest` | character | Winning club: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_games-example}

```python
euroleague_games(competition_code='E', season_code='E2025')
```

_Last validated n/a._

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
| `type` | character | Person type code (J = player, E = coach, ...). |
| `type_name` | character | Person type name. |
| `active` | logical | Whether the record is currently active. |
| `start_date` | character | Start date (ISO 8601). |
| `end_date` | character | End date (ISO 8601). |
| `order` | integer | Display order within the roster. |
| `dorsal` | character | Jersey number as displayed. |
| `dorsal_raw` | character | Jersey number as stored by the engine. |
| `position` | integer | Position code of the player (engine integer). |
| `position_name` | character | Position name of the player. |
| `last_team` | character | Previous club of the person. |
| `external_id` | character | External (statistics-provider) identifier of the person (Utf8 join key). |
| `person_code` | character | Person: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `person_name` | character | Person: display name. |
| `person_alias` | character | Person: short display alias. |
| `person_alias_raw` | character | Person: short alias as stored by the engine. |
| `person_passport_name` | character | Person: given name as on the passport. |
| `person_passport_surname` | character | Person: surname as on the passport. |
| `person_jersey_name` | character | Person: name printed on the jersey. |
| `person_abbreviated_name` | character | Person: abbreviated display name. |
| `person_country_code` | character | Person: ISO country code. |
| `person_country_name` | character | Person: country name. |
| `person_height` | integer | Person: height in centimetres. |
| `person_weight` | integer | Person: weight in kilograms. |
| `person_birth_date` | character | Person: date of birth (ISO 8601). |
| `person_birth_country_code` | character | Person: birth country: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `person_birth_country_name` | character | Person: birth country: display name. |
| `person_twitter_account` | character | Person: twitter / X handle. |
| `person_instagram_account` | character | Person: instagram handle. |
| `person_facebook_account` | character | Person: facebook handle. |
| `person_is_referee` | logical | Person: whether the person is a referee. |
| `club_code` | character | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | character | Club: display name. |
| `club_abbreviated_name` | character | Club: abbreviated display name. |
| `club_editorial_name` | character | Club: editorial (long-form) display name. |
| `club_tv_code` | character | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | logical | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | character | Club: URL of the club crest image. |
| `season_name` | character | Season: display name. |
| `season_code` | character | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `season_alias` | character | Season: short display alias. |
| `season_competition_code` | character | Season: competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `season_year` | integer | Season: start year of the season. |
| `season_start_date` | character | Season: start date (ISO 8601). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_people-example}

```python
euroleague_people(competition_code='E', season_code='E2025')
```

_Last validated n/a._

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
| `player_ranking` | integer | Rank of the player on the requested statistic mode. |
| `games_played` | numeric | Games played. |
| `games_started` | numeric | Games started. |
| `minutes_played` | numeric | Minutes played. |
| `points_scored` | numeric | Points scored. |
| `two_pointers_made` | numeric | Two-point field goals made. |
| `two_pointers_attempted` | numeric | Two-point field goals attempted. |
| `two_pointers_percentage` | character | Two-point field-goal percentage, formatted. |
| `three_pointers_made` | numeric | Three-point field goals made. |
| `three_pointers_attempted` | numeric | Three-point field goals attempted. |
| `three_pointers_percentage` | character | Three-point field-goal percentage, formatted. |
| `free_throws_made` | numeric | Free throws made. |
| `free_throws_attempted` | numeric | Free throws attempted. |
| `free_throws_percentage` | character | Free-throw percentage, formatted. |
| `offensive_rebounds` | numeric | Offensive rebounds. |
| `defensive_rebounds` | numeric | Defensive rebounds. |
| `total_rebounds` | numeric | Total rebounds. |
| `assists` | numeric | Assists. |
| `steals` | numeric | Steals. |
| `turnovers` | numeric | Turnovers. |
| `blocks` | numeric | Blocks made. |
| `blocks_against` | numeric | Shots blocked by the opponent. |
| `fouls_commited` | numeric | Personal fouls committed. |
| `fouls_drawn` | numeric | Fouls drawn. |
| `pir` | numeric | Performance index rating (PIR). |
| `player_code` | character | Player: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `player_name` | character | Player: display name. |
| `player_age` | integer | Age of the player in years. |
| `player_image_url` | character | Player: URL of the image. |
| `player_team_code` | character | Player: euroLeague club code (Utf8 join key). |
| `player_team_tv_codes` | character | Player: three-letter broadcast abbreviation(s) of the club. |
| `player_team_name` | character | Player: club display name. |
| `player_team_image_url` | character | Player: URL of the club crest image. |

**advanced**

| col_name | type | description |
|---|---|---|
| `player_ranking` | integer | Rank of the player on the requested statistic mode. |
| `games_played` | numeric | Games played. |
| `minutes_played` | numeric | Minutes played. |
| `effective_field_goal_percentage` | character | Effective field-goal percentage, formatted. |
| `true_shooting_percentage` | character | True shooting percentage, formatted. |
| `offensive_rebounds_percentage` | character | Offensive rebound percentage, formatted. |
| `defensive_rebounds_percentage` | character | Defensive rebound percentage, formatted. |
| `rebounds_percentage` | character | Total rebound percentage, formatted. |
| `assists_to_turnovers_ratio` | numeric | Assist-to-turnover ratio. |
| `assists_ratio` | character | Assist ratio (assists per 100 possessions used), formatted. |
| `turnovers_ratio` | character | Turnover ratio (turnovers per 100 possessions used), formatted. |
| `two_point_attempts_ratio` | character | Share of field-goal attempts that are two-pointers, formatted. |
| `three_point_attempts_ratio` | character | Share of field-goal attempts that are three-pointers, formatted. |
| `free_throws_rate` | character | Free-throw rate (free-throw attempts per field-goal attempt), formatted. |
| `possesions` | numeric | Possessions (sic: the API spells it this way). |
| `player_code` | character | Player: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `player_name` | character | Player: display name. |
| `player_age` | integer | Age of the player in years. |
| `player_image_url` | character | Player: URL of the image. |
| `player_team_code` | character | Player: euroLeague club code (Utf8 join key). |
| `player_team_tv_codes` | character | Player: three-letter broadcast abbreviation(s) of the club. |
| `player_team_name` | character | Player: club display name. |
| `player_team_image_url` | character | Player: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_player_stats-example}

```python
euroleague_player_stats(competition_code='E', season_code='E2025')
```

_Last validated n/a._

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
| `season_code` | character | Season code: competition code + start year, e.g. E2025 for 2025-26 (Utf8 join key). |
| `phase_type_code` | character | Phase type code (RS = regular season, PO = playoffs, ...). |
| `round` | integer | Round number within the season. |
| `index` | integer | Ordinal position of the round within the season. |
| `name` | character | Display name. |
| `min_game_start_date` | character | Earliest game start in the round (ISO 8601). |
| `max_game_start_date` | character | Latest game start in the round (ISO 8601). |
| `dates_formmated` | character | Human-readable date range of the round (sic: the API spells it this way). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_rounds-example}

```python
euroleague_rounds(competition_code='E', season_code='E2025')
```

_Last validated n/a._

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
| `name` | character | Display name. |
| `code` | character | EuroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `alias` | character | Short display alias. |
| `competition_code` | character | Competition code (E = EuroLeague, U = EuroCup; Utf8 join key). |
| `year` | integer | Start year of the season. |
| `start_date` | character | Start date (ISO 8601). |
| `activation_date` | character | Date the season was activated in the engine (ISO 8601). |
| `end_date` | character | End date (ISO 8601). |
| `winner` | numeric | Winning club of the season (null while in progress). |
| `winner_code` | character | Winning club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `winner_name` | character | Winning club: display name. |
| `winner_abbreviated_name` | character | Winning club: abbreviated display name. |
| `winner_editorial_name` | character | Winning club: editorial (long-form) display name. |
| `winner_tv_code` | character | Winning club: three-letter broadcast abbreviation of the club. |
| `winner_is_virtual` | character | Winning club: whether the club is a placeholder rather than a real club. |
| `winner_images_crest` | character | Winning club: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_seasons-example}

```python
euroleague_seasons(competition_code='E')
```

_Last validated n/a._

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
| `position` | integer | Position code of the player (engine integer). |
| `position_change` | character | Movement since the previous round (Up, Down, Equal). |
| `games_played` | integer | Games played. |
| `games_won` | integer | Games won. |
| `games_lost` | integer | Games lost. |
| `qualified` | logical | Whether the team has clinched qualification for the next phase. |
| `win_percentage` | character | Win percentage, formatted (e.g. 100%). |
| `points_difference` | character | Points for minus points against, signed and formatted (e.g. +19). |
| `points_for` | integer | Points scored. |
| `points_against` | integer | Points conceded. |
| `home_record` | character | Home win-loss record (W-L). |
| `away_record` | character | Away win-loss record (W-L). |
| `neutral_record` | character | Neutral-venue win-loss record (W-L). |
| `overtime_record` | character | Overtime win-loss record (W-L). |
| `last_ten_record` | character | Win-loss record over the last ten games (W-L). |
| `group_name` | character | Group: display name. |
| `last5_form` | character | Results of the last five games, oldest first (W / L), JSON-encoded. |
| `club_code` | character | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | character | Club: display name. |
| `club_abbreviated_name` | character | Club: abbreviated display name. |
| `club_editorial_name` | character | Club: editorial (long-form) display name. |
| `club_tv_code` | character | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | logical | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | character | Club: URL of the club crest image. |

**calendarstandings**

| col_name | type | description |
|---|---|---|
| `position` | integer | Position code of the player (engine integer). |
| `position_change` | character | Movement since the previous round (Up, Down, Equal). |
| `games_played` | integer | Games played. |
| `games_won` | integer | Games won. |
| `games_lost` | integer | Games lost. |
| `qualified` | logical | Whether the team has clinched qualification for the next phase. |
| `group_name` | character | Group: display name. |
| `streaks` | character | Per-round result streaks: start date, end date and W-L record of each run, JSON-encoded. |
| `club_code` | character | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | character | Club: display name. |
| `club_abbreviated_name` | character | Club: abbreviated display name. |
| `club_editorial_name` | character | Club: editorial (long-form) display name. |
| `club_tv_code` | character | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | logical | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | character | Club: URL of the club crest image. |

**streaks**

| col_name | type | description |
|---|---|---|
| `position` | integer | Position code of the player (engine integer). |
| `position_change` | character | Movement since the previous round (Up, Down, Equal). |
| `games_played` | integer | Games played. |
| `games_won` | integer | Games won. |
| `games_lost` | integer | Games lost. |
| `qualified` | logical | Whether the team has clinched qualification for the next phase. |
| `home_record` | character | Home win-loss record (W-L). |
| `away_record` | character | Away win-loss record (W-L). |
| `last10` | character | Win-loss record over the last ten games (W-L). |
| `home_last5` | character | Win-loss record over the last five home games (W-L). |
| `away_last5` | character | Win-loss record over the last five away games (W-L). |
| `longest_wins_streak_current_season` | integer | Longest winning streak this season. |
| `longest_loses_streak_current_season` | integer | Longest losing streak this season. |
| `longest_wins_streak_any_season` | integer | Longest winning streak in any season. |
| `longest_loses_streak_any_season` | integer | Longest losing streak in any season. |
| `group_name` | character | Group: display name. |
| `club_code` | character | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | character | Club: display name. |
| `club_abbreviated_name` | character | Club: abbreviated display name. |
| `club_editorial_name` | character | Club: editorial (long-form) display name. |
| `club_tv_code` | character | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | logical | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | character | Club: URL of the club crest image. |

**aheadbehind**

| col_name | type | description |
|---|---|---|
| `position` | integer | Position code of the player (engine integer). |
| `position_change` | character | Movement since the previous round (Up, Down, Equal). |
| `games_played` | integer | Games played. |
| `games_won` | integer | Games won. |
| `games_lost` | integer | Games lost. |
| `qualified` | logical | Whether the team has clinched qualification for the next phase. |
| `wins_percentage` | character | Win percentage, formatted (e.g. 100%). |
| `quater1_ahead` | character | Win-loss record when ahead after the 1st quarter (W-L; sic: the API spells quarter this way). |
| `quater1_behind` | character | Win-loss record when behind after the 1st quarter (W-L; sic: the API spells quarter this way). |
| `quater1_tied` | character | Win-loss record when tied after the 1st quarter (W-L; sic: the API spells quarter this way). |
| `half1_ahead` | character | Win-loss record when ahead at the half (W-L; sic: the API spells quarter this way). |
| `half1_behind` | character | Win-loss record when behind at the half (W-L; sic: the API spells quarter this way). |
| `half1_tied` | character | Win-loss record when tied at the half (W-L; sic: the API spells quarter this way). |
| `quater3_ahead` | character | Win-loss record when ahead after the 3rd quarter (W-L; sic: the API spells quarter this way). |
| `quater3_behind` | character | Win-loss record when behind after the 3rd quarter (W-L; sic: the API spells quarter this way). |
| `quater3_tied` | character | Win-loss record when tied after the 3rd quarter (W-L; sic: the API spells quarter this way). |
| `group_name` | character | Group: display name. |
| `club_code` | character | Club: euroLeague code of the entity (competition, season, club, person or venue; Utf8 join key). |
| `club_name` | character | Club: display name. |
| `club_abbreviated_name` | character | Club: abbreviated display name. |
| `club_editorial_name` | character | Club: editorial (long-form) display name. |
| `club_tv_code` | character | Club: three-letter broadcast abbreviation of the club. |
| `club_is_virtual` | logical | Club: whether the club is a placeholder rather than a real club. |
| `club_images_crest` | character | Club: URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_standings-example}

```python
euroleague_standings(competition_code='E', round=1, season_code='E2025')
```

_Last validated n/a._

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
| `team_ranking` | integer | Rank of the team on the requested statistic mode. |
| `games_played` | numeric | Games played. |
| `minutes_played` | numeric | Minutes played. |
| `points_scored` | numeric | Points scored. |
| `two_pointers_made` | numeric | Two-point field goals made. |
| `two_pointers_attempted` | numeric | Two-point field goals attempted. |
| `two_pointers_percentage` | character | Two-point field-goal percentage, formatted. |
| `three_pointers_made` | numeric | Three-point field goals made. |
| `three_pointers_attempted` | numeric | Three-point field goals attempted. |
| `three_pointers_percentage` | character | Three-point field-goal percentage, formatted. |
| `free_throws_made` | numeric | Free throws made. |
| `free_throws_attempted` | numeric | Free throws attempted. |
| `free_throws_percentage` | character | Free-throw percentage, formatted. |
| `offensive_rebounds` | numeric | Offensive rebounds. |
| `defensive_rebounds` | numeric | Defensive rebounds. |
| `total_rebounds` | numeric | Total rebounds. |
| `assists` | numeric | Assists. |
| `steals` | numeric | Steals. |
| `turnovers` | numeric | Turnovers. |
| `blocks` | numeric | Blocks made. |
| `blocks_against` | numeric | Shots blocked by the opponent. |
| `fouls_commited` | numeric | Personal fouls committed. |
| `fouls_drawn` | numeric | Fouls drawn. |
| `pir` | numeric | Performance index rating (PIR). |
| `team_code` | character | EuroLeague club code (Utf8 join key). |
| `team_tv_codes` | character | Three-letter broadcast abbreviation(s) of the club. |
| `team_name` | character | Club display name. |
| `team_image_url` | character | URL of the club crest image. |

**advanced**

| col_name | type | description |
|---|---|---|
| `team_ranking` | integer | Rank of the team on the requested statistic mode. |
| `games_played` | numeric | Games played. |
| `effective_field_goal_percentage` | character | Effective field-goal percentage, formatted. |
| `true_shooting_percentage` | character | True shooting percentage, formatted. |
| `offensive_rebounds_percentage` | character | Offensive rebound percentage, formatted. |
| `defensive_rebounds_percentage` | character | Defensive rebound percentage, formatted. |
| `rebounds_percentage` | character | Total rebound percentage, formatted. |
| `assists_to_turnovers_ratio` | numeric | Assist-to-turnover ratio. |
| `assists_ratio` | character | Assist ratio (assists per 100 possessions used), formatted. |
| `turnovers_ratio` | character | Turnover ratio (turnovers per 100 possessions used), formatted. |
| `two_point_rate` | character | Share of field-goal attempts that are two-pointers, formatted. |
| `three_point_rate` | character | Share of field-goal attempts that are three-pointers, formatted. |
| `free_throws_rate` | character | Free-throw rate (free-throw attempts per field-goal attempt), formatted. |
| `points_from_two_pointers_percentage` | character | Share of points from two-pointers, formatted. |
| `points_from_three_pointers_percentage` | character | Share of points from three-pointers, formatted. |
| `points_from_free_throws_percentage` | character | Share of points from free throws, formatted. |
| `team_code` | character | EuroLeague club code (Utf8 join key). |
| `team_tv_codes` | character | Three-letter broadcast abbreviation(s) of the club. |
| `team_name` | character | Club display name. |
| `team_image_url` | character | URL of the club crest image. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_team_stats-example}

```python
euroleague_team_stats(competition_code='E', season_code='E2025')
```

_Last validated n/a._
