---
title: EUROLEAGUE — EuroLeague Competition Engine API (api-live.euroleague.net v2)
sidebar_label: EuroLeague Competition Engine API (api-live.euroleague.net v2)
description: "EUROLEAGUE — EuroLeague Competition Engine API (api-live.euroleague.net v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# EUROLEAGUE — EuroLeague Competition Engine API (api-live.euroleague.net v2)

`sportsdataverse.euroleague` — 7 endpoints.

## euroleague_clubs

Clubs in a season

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

Competitions (EuroLeague E, EuroCup U, ...)

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

## euroleague_game_stats

Box score of one game

**Endpoint URL:** `GET https://api-live.euroleague.net/v2/competitions/{competition_code}/seasons/{season_code}/games/{game_code}/stats`

**Valid URL:** [https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games/1/stats](https://api-live.euroleague.net/v2/competitions/E/seasons/E2025/games/1/stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competition_code` | `competition_code` |  | `Y` |  | E = EuroLeague, U = EuroCup (see /competitions). |
| `season_code` | `season_code` |  | `Y` |  | Competition code + start year, e.g. E2025 for 2025-26. |
| `game_code` | `game_code` |  | `Y` |  | Game number within the season (1-based). |

### Returns {#euroleague_game_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `local_coach_code` | character | Home side: head coach code (Utf8 join key). |
| `local_coach_name` | character | Home side: head coach name. |
| `local_players` | character | Home side: per-player box-score rows for the side, JSON-encoded. |
| `local_team_time_played` | numeric | Home side: team-only (not attributed to a player): seconds played. |
| `local_team_valuation` | numeric | Home side: team-only (not attributed to a player): performance index rating (PIR). |
| `local_team_points` | numeric | Home side: team-only (not attributed to a player): points. |
| `local_team_field_goals_made2` | numeric | Home side: team-only (not attributed to a player): two-point field goals made. |
| `local_team_field_goals_attempted2` | numeric | Home side: team-only (not attributed to a player): two-point field goals attempted. |
| `local_team_field_goals_made3` | numeric | Home side: team-only (not attributed to a player): three-point field goals made. |
| `local_team_field_goals_attempted3` | numeric | Home side: team-only (not attributed to a player): three-point field goals attempted. |
| `local_team_free_throws_made` | numeric | Home side: team-only (not attributed to a player): free throws made. |
| `local_team_free_throws_attempted` | numeric | Home side: team-only (not attributed to a player): free throws attempted. |
| `local_team_field_goals_made_total` | numeric | Home side: team-only (not attributed to a player): field goals made. |
| `local_team_field_goals_attempted_total` | numeric | Home side: team-only (not attributed to a player): field goals attempted. |
| `local_team_accuracy_made` | numeric | Home side: team-only (not attributed to a player): shots made (accuracy numerator). |
| `local_team_accuracy_attempted` | numeric | Home side: team-only (not attributed to a player): shots attempted (accuracy denominator). |
| `local_team_total_rebounds` | numeric | Home side: team-only (not attributed to a player): total rebounds. |
| `local_team_defensive_rebounds` | numeric | Home side: team-only (not attributed to a player): defensive rebounds. |
| `local_team_offensive_rebounds` | numeric | Home side: team-only (not attributed to a player): offensive rebounds. |
| `local_team_assistances` | numeric | Home side: team-only (not attributed to a player): assists. |
| `local_team_steals` | numeric | Home side: team-only (not attributed to a player): steals. |
| `local_team_turnovers` | numeric | Home side: team-only (not attributed to a player): turnovers. |
| `local_team_blocks_favour` | numeric | Home side: team-only (not attributed to a player): blocks made. |
| `local_team_blocks_against` | numeric | Home side: team-only (not attributed to a player): shots blocked by the opponent. |
| `local_team_fouls_commited` | numeric | Home side: team-only (not attributed to a player): personal fouls committed. |
| `local_team_fouls_received` | numeric | Home side: team-only (not attributed to a player): fouls drawn. |
| `local_team_plus_minus` | numeric | Home side: team-only (not attributed to a player): plus/minus. |
| `local_total_time_played` | numeric | Home side: totals: seconds played. |
| `local_total_valuation` | numeric | Home side: totals: performance index rating (PIR). |
| `local_total_points` | numeric | Home side: totals: points. |
| `local_total_field_goals_made2` | numeric | Home side: totals: two-point field goals made. |
| `local_total_field_goals_attempted2` | numeric | Home side: totals: two-point field goals attempted. |
| `local_total_field_goals_made3` | numeric | Home side: totals: three-point field goals made. |
| `local_total_field_goals_attempted3` | numeric | Home side: totals: three-point field goals attempted. |
| `local_total_free_throws_made` | numeric | Home side: totals: free throws made. |
| `local_total_free_throws_attempted` | numeric | Home side: totals: free throws attempted. |
| `local_total_field_goals_made_total` | numeric | Home side: totals: field goals made. |
| `local_total_field_goals_attempted_total` | numeric | Home side: totals: field goals attempted. |
| `local_total_accuracy_made` | numeric | Home side: totals: shots made (accuracy numerator). |
| `local_total_accuracy_attempted` | numeric | Home side: totals: shots attempted (accuracy denominator). |
| `local_total_total_rebounds` | numeric | Home side: totals: total rebounds. |
| `local_total_defensive_rebounds` | numeric | Home side: totals: defensive rebounds. |
| `local_total_offensive_rebounds` | numeric | Home side: totals: offensive rebounds. |
| `local_total_assistances` | numeric | Home side: totals: assists. |
| `local_total_steals` | numeric | Home side: totals: steals. |
| `local_total_turnovers` | numeric | Home side: totals: turnovers. |
| `local_total_blocks_favour` | numeric | Home side: totals: blocks made. |
| `local_total_blocks_against` | numeric | Home side: totals: shots blocked by the opponent. |
| `local_total_fouls_commited` | numeric | Home side: totals: personal fouls committed. |
| `local_total_fouls_received` | numeric | Home side: totals: fouls drawn. |
| `local_total_plus_minus` | numeric | Home side: totals: plus/minus. |
| `road_coach_code` | character | Away side: head coach code (Utf8 join key). |
| `road_coach_name` | character | Away side: head coach name. |
| `road_players` | character | Away side: per-player box-score rows for the side, JSON-encoded. |
| `road_team_time_played` | numeric | Away side: team-only (not attributed to a player): seconds played. |
| `road_team_valuation` | numeric | Away side: team-only (not attributed to a player): performance index rating (PIR). |
| `road_team_points` | numeric | Away side: team-only (not attributed to a player): points. |
| `road_team_field_goals_made2` | numeric | Away side: team-only (not attributed to a player): two-point field goals made. |
| `road_team_field_goals_attempted2` | numeric | Away side: team-only (not attributed to a player): two-point field goals attempted. |
| `road_team_field_goals_made3` | numeric | Away side: team-only (not attributed to a player): three-point field goals made. |
| `road_team_field_goals_attempted3` | numeric | Away side: team-only (not attributed to a player): three-point field goals attempted. |
| `road_team_free_throws_made` | numeric | Away side: team-only (not attributed to a player): free throws made. |
| `road_team_free_throws_attempted` | numeric | Away side: team-only (not attributed to a player): free throws attempted. |
| `road_team_field_goals_made_total` | numeric | Away side: team-only (not attributed to a player): field goals made. |
| `road_team_field_goals_attempted_total` | numeric | Away side: team-only (not attributed to a player): field goals attempted. |
| `road_team_accuracy_made` | numeric | Away side: team-only (not attributed to a player): shots made (accuracy numerator). |
| `road_team_accuracy_attempted` | numeric | Away side: team-only (not attributed to a player): shots attempted (accuracy denominator). |
| `road_team_total_rebounds` | numeric | Away side: team-only (not attributed to a player): total rebounds. |
| `road_team_defensive_rebounds` | numeric | Away side: team-only (not attributed to a player): defensive rebounds. |
| `road_team_offensive_rebounds` | numeric | Away side: team-only (not attributed to a player): offensive rebounds. |
| `road_team_assistances` | numeric | Away side: team-only (not attributed to a player): assists. |
| `road_team_steals` | numeric | Away side: team-only (not attributed to a player): steals. |
| `road_team_turnovers` | numeric | Away side: team-only (not attributed to a player): turnovers. |
| `road_team_blocks_favour` | numeric | Away side: team-only (not attributed to a player): blocks made. |
| `road_team_blocks_against` | numeric | Away side: team-only (not attributed to a player): shots blocked by the opponent. |
| `road_team_fouls_commited` | numeric | Away side: team-only (not attributed to a player): personal fouls committed. |
| `road_team_fouls_received` | numeric | Away side: team-only (not attributed to a player): fouls drawn. |
| `road_team_plus_minus` | numeric | Away side: team-only (not attributed to a player): plus/minus. |
| `road_total_time_played` | numeric | Away side: totals: seconds played. |
| `road_total_valuation` | numeric | Away side: totals: performance index rating (PIR). |
| `road_total_points` | numeric | Away side: totals: points. |
| `road_total_field_goals_made2` | numeric | Away side: totals: two-point field goals made. |
| `road_total_field_goals_attempted2` | numeric | Away side: totals: two-point field goals attempted. |
| `road_total_field_goals_made3` | numeric | Away side: totals: three-point field goals made. |
| `road_total_field_goals_attempted3` | numeric | Away side: totals: three-point field goals attempted. |
| `road_total_free_throws_made` | numeric | Away side: totals: free throws made. |
| `road_total_free_throws_attempted` | numeric | Away side: totals: free throws attempted. |
| `road_total_field_goals_made_total` | numeric | Away side: totals: field goals made. |
| `road_total_field_goals_attempted_total` | numeric | Away side: totals: field goals attempted. |
| `road_total_accuracy_made` | numeric | Away side: totals: shots made (accuracy numerator). |
| `road_total_accuracy_attempted` | numeric | Away side: totals: shots attempted (accuracy denominator). |
| `road_total_total_rebounds` | numeric | Away side: totals: total rebounds. |
| `road_total_defensive_rebounds` | numeric | Away side: totals: defensive rebounds. |
| `road_total_offensive_rebounds` | numeric | Away side: totals: offensive rebounds. |
| `road_total_assistances` | numeric | Away side: totals: assists. |
| `road_total_steals` | numeric | Away side: totals: steals. |
| `road_total_turnovers` | numeric | Away side: totals: turnovers. |
| `road_total_blocks_favour` | numeric | Away side: totals: blocks made. |
| `road_total_blocks_against` | numeric | Away side: totals: shots blocked by the opponent. |
| `road_total_fouls_commited` | numeric | Away side: totals: personal fouls committed. |
| `road_total_fouls_received` | numeric | Away side: totals: fouls drawn. |
| `road_total_plus_minus` | numeric | Away side: totals: plus/minus. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#euroleague_game_stats-example}

```python
euroleague_game_stats(competition_code='E', game_code=1, season_code='E2025')
```

_Last validated n/a._

## euroleague_games

Games of a season

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

People (players, coaches) in a season

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

## euroleague_rounds

Rounds of a season

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

Seasons of a competition

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
