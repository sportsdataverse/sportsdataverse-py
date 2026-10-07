---
title: SOCCER — UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)
sidebar_label: UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)
description: "SOCCER — UEFA front-end APIs (comp/match/standings/matchstats.uefa.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 12
toc_max_heading_level: 2
---
# SOCCER — UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)

`sportsdataverse.soccer` — 7 endpoints.

## uefa_competitions

UEFA competitions by id (Champions League 1, Europa League 3, Conference League 2019).

**Endpoint URL:** `GET https://comp.uefa.com/v2/competitions`

**Valid URL:** [https://comp.uefa.com/v2/competitions?competitionIds=1%2C3%2C2019](https://comp.uefa.com/v2/competitions?competitionIds=1%2C3%2C2019)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionIds` | `competition_ids` |  |  | `Y` | Required. Comma-separated competition ids. |

### Returns {#uefa_competitions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `age` | character | Player age (in years). |
| `code` | character | Fielder detail type code. |
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `region` | character | Region label. |
| `sex` | character |  |
| `sports_type` | character |  |
| `team_category` | character |  |
| `type` | character | Type discriminator for the record. |
| `images_full_logo` | character |  |
| `meta_data_name` | character |  |
| `translations_name_en` | character |  |
| `translations_name_fr` | character |  |
| `translations_name_de` | character |  |
| `translations_name_es` | character |  |
| `translations_name_pt` | character |  |
| `translations_name_it` | character |  |
| `translations_name_ru` | character |  |
| `translations_name_zh` | character |  |
| `translations_name_ar` | character |  |
| `translations_prequalifying_name_en` | character |  |
| `translations_prequalifying_name_fr` | character |  |
| `translations_prequalifying_name_de` | character |  |
| `translations_prequalifying_name_es` | character |  |
| `translations_prequalifying_name_pt` | character |  |
| `translations_prequalifying_name_it` | character |  |
| `translations_prequalifying_name_ru` | character |  |
| `translations_prequalifying_name_zh` | character |  |
| `translations_prequalifying_name_ar` | character |  |
| `translations_qualifying_name_en` | character |  |
| `translations_qualifying_name_fr` | character |  |
| `translations_qualifying_name_de` | character |  |
| `translations_qualifying_name_es` | character |  |
| `translations_qualifying_name_pt` | character |  |
| `translations_qualifying_name_it` | character |  |
| `translations_qualifying_name_ru` | character |  |
| `translations_qualifying_name_zh` | character |  |
| `translations_qualifying_name_ar` | character |  |
| `translations_tournament_name_en` | character |  |
| `translations_tournament_name_fr` | character |  |
| `translations_tournament_name_de` | character |  |
| `translations_tournament_name_es` | character |  |
| `translations_tournament_name_pt` | character |  |
| `translations_tournament_name_it` | character |  |
| `translations_tournament_name_ru` | character |  |
| `translations_tournament_name_zh` | character |  |
| `translations_tournament_name_ar` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_competitions-example}

```python
uefa_competitions(competition_ids='1,3,2019')
```

_Last validated n/a._

## uefa_livescore

Matches live right now across UEFA competitions.

**Endpoint URL:** `GET https://match.uefa.com/v5/livescore`

**Valid URL:** [https://match.uefa.com/v5/livescore](https://match.uefa.com/v5/livescore)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#uefa_livescore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `hash` | character |  |
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `lineup_status` | character |  |
| `status` | character | Status label. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_livescore-example}

```python
uefa_livescore()
```

_Last validated n/a._

## uefa_matches

Matches of a UEFA competition season.

**Endpoint URL:** `GET https://match.uefa.com/v5/matches`

**Valid URL:** [https://match.uefa.com/v5/matches?competitionId=1&seasonYear=2026&limit=3&offset=0](https://match.uefa.com/v5/matches?competitionId=1&seasonYear=2026&limit=3&offset=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Required. 1 = Champions League, 3 = Europa League, 2019 = Conference League (see /v2/competitions). |
| `seasonYear` | `season_year` |  |  | `Y` | The season's end year (2026 = 2025-26). |
| `limit` | `limit` |  |  | `Y` | Page size. |
| `offset` | `offset` |  |  | `Y` | Page offset. |

### Returns {#uefa_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `behind_closed_doors` | logical |  |
| `competition_phase` | character | Competition phase: TOURNAMENT (main draw) or QUALIFYING. |
| `full_time_at` | character |  |
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `lineup_status` | character |  |
| `match_attendance` | integer |  |
| `referees` | character |  |
| `season_year` | character | Season year string ('YYYY-YY' format). |
| `session_number` | integer |  |
| `status` | character | Status label. |
| `type` | character | Type discriminator for the record. |
| `away_team_association_id` | character |  |
| `away_team_association_logo_url` | character |  |
| `away_team_big_logo_url` | character |  |
| `away_team_confederation_type` | character |  |
| `away_team_country_code` | character | Away club: club: ISO country code. |
| `away_team_id` | character | Away club: club: provider identifier for the entity (Utf8 join key). |
| `away_team_id_provider` | character |  |
| `away_team_international_name` | character |  |
| `away_team_is_place_holder` | logical |  |
| `away_team_logo_url` | character |  |
| `away_team_medium_logo_url` | character |  |
| `away_team_organization_id` | character |  |
| `away_team_team_code` | character | Away team code. |
| `away_team_team_type_detail` | character |  |
| `away_team_translations_country_name_en` | character |  |
| `away_team_translations_country_name_fr` | character |  |
| `away_team_translations_country_name_de` | character |  |
| `away_team_translations_country_name_es` | character |  |
| `away_team_translations_country_name_pt` | character |  |
| `away_team_translations_country_name_it` | character |  |
| `away_team_translations_country_name_ru` | character |  |
| `away_team_translations_country_name_zh` | character |  |
| `away_team_translations_country_name_ar` | character |  |
| `away_team_translations_display_name_en` | character |  |
| `away_team_translations_display_name_fr` | character |  |
| `away_team_translations_display_name_de` | character |  |
| `away_team_translations_display_name_es` | character |  |
| `away_team_translations_display_name_pt` | character |  |
| `away_team_translations_display_name_it` | character |  |
| `away_team_translations_display_name_ru` | character |  |
| `away_team_translations_display_name_zh` | character |  |
| `away_team_translations_display_name_ar` | character |  |
| `away_team_translations_display_official_name_en` | character |  |
| `away_team_translations_display_official_name_fr` | character |  |
| `away_team_translations_display_official_name_de` | character |  |
| `away_team_translations_display_official_name_es` | character |  |
| `away_team_translations_display_official_name_pt` | character |  |
| `away_team_translations_display_official_name_it` | character |  |
| `away_team_translations_display_official_name_ru` | character |  |
| `away_team_translations_display_official_name_zh` | character |  |
| `away_team_translations_display_official_name_ar` | character |  |
| `away_team_translations_display_team_code_en` | character |  |
| `away_team_translations_display_team_code_fr` | character |  |
| `away_team_translations_display_team_code_de` | character |  |
| `away_team_translations_display_team_code_es` | character |  |
| `away_team_translations_display_team_code_pt` | character |  |
| `away_team_translations_display_team_code_it` | character |  |
| `away_team_translations_display_team_code_ru` | character |  |
| `away_team_translations_display_team_code_zh` | character |  |
| `away_team_translations_display_team_code_ar` | character |  |
| `away_team_translations_short_name_en` | character |  |
| `away_team_translations_short_name_fr` | character |  |
| `away_team_translations_short_name_de` | character |  |
| `away_team_translations_short_name_es` | character |  |
| `away_team_translations_short_name_pt` | character |  |
| `away_team_translations_short_name_it` | character |  |
| `away_team_translations_short_name_ru` | character |  |
| `away_team_translations_short_name_zh` | character |  |
| `away_team_translations_short_name_ar` | character |  |
| `away_team_type_is_national` | logical |  |
| `away_team_type_team` | character |  |
| `competition_age` | character |  |
| `competition_code` | character |  |
| `competition_id` | character | Competition: provider identifier for the entity (Utf8 join key). |
| `competition_images_full_logo` | character |  |
| `competition_meta_data_name` | character |  |
| `competition_region` | character |  |
| `competition_sex` | character |  |
| `competition_sports_type` | character |  |
| `competition_team_category` | character |  |
| `competition_translations_name_en` | character |  |
| `competition_translations_name_fr` | character |  |
| `competition_translations_name_de` | character |  |
| `competition_translations_name_es` | character |  |
| `competition_translations_name_pt` | character |  |
| `competition_translations_name_it` | character |  |
| `competition_translations_name_ru` | character |  |
| `competition_translations_name_zh` | character |  |
| `competition_translations_name_ar` | character |  |
| `competition_translations_prequalifying_name_en` | character |  |
| `competition_translations_prequalifying_name_fr` | character |  |
| `competition_translations_prequalifying_name_de` | character |  |
| `competition_translations_prequalifying_name_es` | character |  |
| `competition_translations_prequalifying_name_pt` | character |  |
| `competition_translations_prequalifying_name_it` | character |  |
| `competition_translations_prequalifying_name_ru` | character |  |
| `competition_translations_prequalifying_name_zh` | character |  |
| `competition_translations_prequalifying_name_ar` | character |  |
| `competition_translations_qualifying_name_en` | character |  |
| `competition_translations_qualifying_name_fr` | character |  |
| `competition_translations_qualifying_name_de` | character |  |
| `competition_translations_qualifying_name_es` | character |  |
| `competition_translations_qualifying_name_pt` | character |  |
| `competition_translations_qualifying_name_it` | character |  |
| `competition_translations_qualifying_name_ru` | character |  |
| `competition_translations_qualifying_name_zh` | character |  |
| `competition_translations_qualifying_name_ar` | character |  |
| `competition_translations_tournament_name_en` | character |  |
| `competition_translations_tournament_name_fr` | character |  |
| `competition_translations_tournament_name_de` | character |  |
| `competition_translations_tournament_name_es` | character |  |
| `competition_translations_tournament_name_pt` | character |  |
| `competition_translations_tournament_name_it` | character |  |
| `competition_translations_tournament_name_ru` | character |  |
| `competition_translations_tournament_name_zh` | character |  |
| `competition_translations_tournament_name_ar` | character |  |
| `competition_type` | character | Competition: type discriminator for the record. |
| `home_team_association_id` | character |  |
| `home_team_association_logo_url` | character |  |
| `home_team_big_logo_url` | character |  |
| `home_team_confederation_type` | character |  |
| `home_team_country_code` | character | Home club: club: ISO country code. |
| `home_team_id` | character | Home club: club: provider identifier for the entity (Utf8 join key). |
| `home_team_id_provider` | character |  |
| `home_team_international_name` | character |  |
| `home_team_is_place_holder` | logical |  |
| `home_team_logo_url` | character |  |
| `home_team_medium_logo_url` | character |  |
| `home_team_organization_id` | character |  |
| `home_team_team_code` | character | Home team code. |
| `home_team_team_type_detail` | character |  |
| `home_team_translations_country_name_en` | character |  |
| `home_team_translations_country_name_fr` | character |  |
| `home_team_translations_country_name_de` | character |  |
| `home_team_translations_country_name_es` | character |  |
| `home_team_translations_country_name_pt` | character |  |
| `home_team_translations_country_name_it` | character |  |
| `home_team_translations_country_name_ru` | character |  |
| `home_team_translations_country_name_zh` | character |  |
| `home_team_translations_country_name_ar` | character |  |
| `home_team_translations_display_name_en` | character |  |
| `home_team_translations_display_name_fr` | character |  |
| `home_team_translations_display_name_de` | character |  |
| `home_team_translations_display_name_es` | character |  |
| `home_team_translations_display_name_pt` | character |  |
| `home_team_translations_display_name_it` | character |  |
| `home_team_translations_display_name_ru` | character |  |
| `home_team_translations_display_name_zh` | character |  |
| `home_team_translations_display_name_ar` | character |  |
| `home_team_translations_display_official_name_en` | character |  |
| `home_team_translations_display_official_name_fr` | character |  |
| `home_team_translations_display_official_name_de` | character |  |
| `home_team_translations_display_official_name_es` | character |  |
| `home_team_translations_display_official_name_pt` | character |  |
| `home_team_translations_display_official_name_it` | character |  |
| `home_team_translations_display_official_name_ru` | character |  |
| `home_team_translations_display_official_name_zh` | character |  |
| `home_team_translations_display_official_name_ar` | character |  |
| `home_team_translations_display_team_code_en` | character |  |
| `home_team_translations_display_team_code_fr` | character |  |
| `home_team_translations_display_team_code_de` | character |  |
| `home_team_translations_display_team_code_es` | character |  |
| `home_team_translations_display_team_code_pt` | character |  |
| `home_team_translations_display_team_code_it` | character |  |
| `home_team_translations_display_team_code_ru` | character |  |
| `home_team_translations_display_team_code_zh` | character |  |
| `home_team_translations_display_team_code_ar` | character |  |
| `home_team_translations_short_name_en` | character |  |
| `home_team_translations_short_name_fr` | character |  |
| `home_team_translations_short_name_de` | character |  |
| `home_team_translations_short_name_es` | character |  |
| `home_team_translations_short_name_pt` | character |  |
| `home_team_translations_short_name_it` | character |  |
| `home_team_translations_short_name_ru` | character |  |
| `home_team_translations_short_name_zh` | character |  |
| `home_team_translations_short_name_ar` | character |  |
| `home_team_type_is_national` | logical |  |
| `home_team_type_team` | character |  |
| `kick_off_time_date` | character |  |
| `kick_off_time_date_time` | character |  |
| `kick_off_time_utc_offset_in_hours` | integer |  |
| `matchday_competition_id` | character |  |
| `matchday_date_from` | character |  |
| `matchday_date_to` | character |  |
| `matchday_format` | character |  |
| `matchday_id` | character |  |
| `matchday_long_name` | character |  |
| `matchday_name` | character |  |
| `matchday_phase` | character |  |
| `matchday_round_id` | character |  |
| `matchday_season_year` | character |  |
| `matchday_sequence_number` | character |  |
| `matchday_translations_long_name_en` | character |  |
| `matchday_translations_long_name_fr` | character |  |
| `matchday_translations_long_name_de` | character |  |
| `matchday_translations_long_name_es` | character |  |
| `matchday_translations_long_name_pt` | character |  |
| `matchday_translations_long_name_it` | character |  |
| `matchday_translations_long_name_ru` | character |  |
| `matchday_translations_long_name_zh` | character |  |
| `matchday_translations_long_name_ar` | character |  |
| `matchday_translations_name_en` | character |  |
| `matchday_translations_name_fr` | character |  |
| `matchday_translations_name_de` | character |  |
| `matchday_translations_name_es` | character |  |
| `matchday_translations_name_pt` | character |  |
| `matchday_translations_name_it` | character |  |
| `matchday_translations_name_ru` | character |  |
| `matchday_translations_name_zh` | character |  |
| `matchday_translations_name_ar` | character |  |
| `matchday_type` | character |  |
| `player_events_penalty_scorers` | character |  |
| `player_events_scorers` | character |  |
| `player_of_the_match_player_age` | character |  |
| `player_of_the_match_player_birth_date` | character |  |
| `player_of_the_match_player_club_id` | character |  |
| `player_of_the_match_player_club_jersey_number` | character |  |
| `player_of_the_match_player_club_shirt_name` | character |  |
| `player_of_the_match_player_country_code` | character |  |
| `player_of_the_match_player_detailed_field_position` | character |  |
| `player_of_the_match_player_field_position` | character |  |
| `player_of_the_match_player_gender` | character |  |
| `player_of_the_match_player_id` | character |  |
| `player_of_the_match_player_image_url` | character |  |
| `player_of_the_match_player_international_name` | character |  |
| `player_of_the_match_player_national_field_position` | character |  |
| `player_of_the_match_player_national_jersey_number` | character |  |
| `player_of_the_match_player_national_shirt_name` | character |  |
| `player_of_the_match_player_national_team_id` | character |  |
| `player_of_the_match_player_translations_country_name_en` | character |  |
| `player_of_the_match_player_translations_country_name_fr` | character |  |
| `player_of_the_match_player_translations_country_name_de` | character |  |
| `player_of_the_match_player_translations_country_name_es` | character |  |
| `player_of_the_match_player_translations_country_name_pt` | character |  |
| `player_of_the_match_player_translations_country_name_it` | character |  |
| `player_of_the_match_player_translations_country_name_ru` | character |  |
| `player_of_the_match_player_translations_country_name_zh` | character |  |
| `player_of_the_match_player_translations_country_name_ar` | character |  |
| `player_of_the_match_player_translations_field_position_en` | character |  |
| `player_of_the_match_player_translations_field_position_fr` | character |  |
| `player_of_the_match_player_translations_field_position_de` | character |  |
| `player_of_the_match_player_translations_field_position_es` | character |  |
| `player_of_the_match_player_translations_field_position_pt` | character |  |
| `player_of_the_match_player_translations_field_position_it` | character |  |
| `player_of_the_match_player_translations_field_position_ru` | character |  |
| `player_of_the_match_player_translations_field_position_zh` | character |  |
| `player_of_the_match_player_translations_field_position_ar` | character |  |
| `player_of_the_match_player_translations_last_name_en` | character |  |
| `player_of_the_match_player_translations_last_name_fr` | character |  |
| `player_of_the_match_player_translations_last_name_de` | character |  |
| `player_of_the_match_player_translations_last_name_es` | character |  |
| `player_of_the_match_player_translations_last_name_pt` | character |  |
| `player_of_the_match_player_translations_last_name_it` | character |  |
| `player_of_the_match_player_translations_last_name_ru` | character |  |
| `player_of_the_match_player_translations_last_name_zh` | character |  |
| `player_of_the_match_player_translations_last_name_ar` | character |  |
| `player_of_the_match_player_translations_name_en` | character |  |
| `player_of_the_match_player_translations_name_fr` | character |  |
| `player_of_the_match_player_translations_name_de` | character |  |
| `player_of_the_match_player_translations_name_es` | character |  |
| `player_of_the_match_player_translations_name_pt` | character |  |
| `player_of_the_match_player_translations_name_it` | character |  |
| `player_of_the_match_player_translations_name_ru` | character |  |
| `player_of_the_match_player_translations_name_zh` | character |  |
| `player_of_the_match_player_translations_name_ar` | character |  |
| `player_of_the_match_player_translations_national_field_position_en` | character |  |
| `player_of_the_match_player_translations_national_field_position_fr` | character |  |
| `player_of_the_match_player_translations_national_field_position_de` | character |  |
| `player_of_the_match_player_translations_national_field_position_es` | character |  |
| `player_of_the_match_player_translations_national_field_position_pt` | character |  |
| `player_of_the_match_player_translations_national_field_position_it` | character |  |
| `player_of_the_match_player_translations_national_field_position_ru` | character |  |
| `player_of_the_match_player_translations_national_field_position_zh` | character |  |
| `player_of_the_match_player_translations_national_field_position_ar` | character |  |
| `player_of_the_match_player_translations_short_name_en` | character |  |
| `player_of_the_match_player_translations_short_name_fr` | character |  |
| `player_of_the_match_player_translations_short_name_de` | character |  |
| `player_of_the_match_player_translations_short_name_es` | character |  |
| `player_of_the_match_player_translations_short_name_pt` | character |  |
| `player_of_the_match_player_translations_short_name_it` | character |  |
| `player_of_the_match_player_translations_short_name_ru` | character |  |
| `player_of_the_match_player_translations_short_name_zh` | character |  |
| `player_of_the_match_player_translations_short_name_ar` | character |  |
| `player_of_the_match_team_id` | character |  |
| `round_active` | logical |  |
| `round_bench_gk_count` | integer |  |
| `round_bench_players_count` | integer |  |
| `round_bench_staff_count` | integer |  |
| `round_coefficient_winner_bonus` | numeric |  |
| `round_competition_id` | character |  |
| `round_date_from` | character |  |
| `round_date_to` | character |  |
| `round_field_players_count` | integer |  |
| `round_group_count` | integer |  |
| `round_id` | character | Composite id of the round. |
| `round_meta_data_name` | character |  |
| `round_meta_data_type` | character |  |
| `round_mode` | character |  |
| `round_mode_detail` | character |  |
| `round_order_in_competition` | integer |  |
| `round_phase` | character |  |
| `round_season_year` | character |  |
| `round_secondary_type` | character |  |
| `round_stadium_name_type` | character |  |
| `round_status` | character |  |
| `round_substitution_count` | integer |  |
| `round_team_count` | integer |  |
| `round_teams` | character |  |
| `round_translations_abbreviation_en` | character |  |
| `round_translations_abbreviation_fr` | character |  |
| `round_translations_abbreviation_de` | character |  |
| `round_translations_abbreviation_es` | character |  |
| `round_translations_abbreviation_pt` | character |  |
| `round_translations_abbreviation_it` | character |  |
| `round_translations_abbreviation_ru` | character |  |
| `round_translations_abbreviation_zh` | character |  |
| `round_translations_abbreviation_ar` | character |  |
| `round_translations_name_en` | character |  |
| `round_translations_name_fr` | character |  |
| `round_translations_name_de` | character |  |
| `round_translations_name_es` | character |  |
| `round_translations_name_pt` | character |  |
| `round_translations_name_it` | character |  |
| `round_translations_name_ru` | character |  |
| `round_translations_name_zh` | character |  |
| `round_translations_name_ar` | character |  |
| `round_translations_short_name_en` | character |  |
| `round_translations_short_name_fr` | character |  |
| `round_translations_short_name_de` | character |  |
| `round_translations_short_name_es` | character |  |
| `round_translations_short_name_pt` | character |  |
| `round_translations_short_name_it` | character |  |
| `round_translations_short_name_ru` | character |  |
| `round_translations_short_name_zh` | character |  |
| `round_translations_short_name_ar` | character |  |
| `score_penalty_away` | integer |  |
| `score_penalty_home` | integer |  |
| `score_regular_away` | integer |  |
| `score_regular_home` | integer |  |
| `score_total_away` | integer |  |
| `score_total_home` | integer |  |
| `stadium_address` | character | Stadium: street address of the stadium. |
| `stadium_capacity` | integer | Stadium: seating capacity of the stadium. |
| `stadium_city_country_code` | character |  |
| `stadium_city_id` | character |  |
| `stadium_city_translations_name_en` | character |  |
| `stadium_city_translations_name_fr` | character |  |
| `stadium_city_translations_name_de` | character |  |
| `stadium_city_translations_name_es` | character |  |
| `stadium_city_translations_name_pt` | character |  |
| `stadium_city_translations_name_it` | character |  |
| `stadium_city_translations_name_ru` | character |  |
| `stadium_city_translations_name_zh` | character |  |
| `stadium_city_translations_name_ar` | character |  |
| `stadium_country_code` | character | Stadium: ISO country code. |
| `stadium_geolocation_latitude` | numeric |  |
| `stadium_geolocation_longitude` | numeric |  |
| `stadium_id` | character | Stadium: provider identifier for the entity (Utf8 join key). |
| `stadium_images_medium_wide` | character |  |
| `stadium_images_large_ultra_wide` | character |  |
| `stadium_opening_date` | character |  |
| `stadium_pitch_length` | integer |  |
| `stadium_pitch_width` | integer |  |
| `stadium_translations_media_name_en` | character |  |
| `stadium_translations_media_name_fr` | character |  |
| `stadium_translations_media_name_de` | character |  |
| `stadium_translations_media_name_es` | character |  |
| `stadium_translations_media_name_pt` | character |  |
| `stadium_translations_media_name_it` | character |  |
| `stadium_translations_media_name_ru` | character |  |
| `stadium_translations_media_name_zh` | character |  |
| `stadium_translations_media_name_ar` | character |  |
| `stadium_translations_name_en` | character |  |
| `stadium_translations_name_fr` | character |  |
| `stadium_translations_name_de` | character |  |
| `stadium_translations_name_es` | character |  |
| `stadium_translations_name_pt` | character |  |
| `stadium_translations_name_it` | character |  |
| `stadium_translations_name_ru` | character |  |
| `stadium_translations_name_zh` | character |  |
| `stadium_translations_name_ar` | character |  |
| `stadium_translations_official_name_en` | character |  |
| `stadium_translations_official_name_fr` | character |  |
| `stadium_translations_official_name_de` | character |  |
| `stadium_translations_official_name_es` | character |  |
| `stadium_translations_official_name_pt` | character |  |
| `stadium_translations_official_name_it` | character |  |
| `stadium_translations_official_name_ru` | character |  |
| `stadium_translations_official_name_zh` | character |  |
| `stadium_translations_official_name_ar` | character |  |
| `stadium_translations_special_events_name_en` | character |  |
| `stadium_translations_special_events_name_fr` | character |  |
| `stadium_translations_special_events_name_de` | character |  |
| `stadium_translations_special_events_name_es` | character |  |
| `stadium_translations_special_events_name_pt` | character |  |
| `stadium_translations_special_events_name_it` | character |  |
| `stadium_translations_special_events_name_ru` | character |  |
| `stadium_translations_special_events_name_zh` | character |  |
| `stadium_translations_special_events_name_ar` | character |  |
| `stadium_translations_sponsor_name_en` | character |  |
| `stadium_translations_sponsor_name_fr` | character |  |
| `stadium_translations_sponsor_name_de` | character |  |
| `stadium_translations_sponsor_name_es` | character |  |
| `stadium_translations_sponsor_name_pt` | character |  |
| `stadium_translations_sponsor_name_it` | character |  |
| `stadium_translations_sponsor_name_ru` | character |  |
| `stadium_translations_sponsor_name_zh` | character |  |
| `stadium_translations_sponsor_name_ar` | character |  |
| `winner_match_reason` | character |  |
| `winner_match_team_association_id` | character |  |
| `winner_match_team_association_logo_url` | character |  |
| `winner_match_team_big_logo_url` | character |  |
| `winner_match_team_confederation_type` | character |  |
| `winner_match_team_country_code` | character |  |
| `winner_match_team_id` | character |  |
| `winner_match_team_id_provider` | character |  |
| `winner_match_team_international_name` | character |  |
| `winner_match_team_is_place_holder` | logical |  |
| `winner_match_team_logo_url` | character |  |
| `winner_match_team_medium_logo_url` | character |  |
| `winner_match_team_organization_id` | character |  |
| `winner_match_team_team_code` | character |  |
| `winner_match_team_team_type_detail` | character |  |
| `winner_match_team_translations_country_name_en` | character |  |
| `winner_match_team_translations_country_name_fr` | character |  |
| `winner_match_team_translations_country_name_de` | character |  |
| `winner_match_team_translations_country_name_es` | character |  |
| `winner_match_team_translations_country_name_pt` | character |  |
| `winner_match_team_translations_country_name_it` | character |  |
| `winner_match_team_translations_country_name_ru` | character |  |
| `winner_match_team_translations_country_name_zh` | character |  |
| `winner_match_team_translations_country_name_ar` | character |  |
| `winner_match_team_translations_display_name_en` | character |  |
| `winner_match_team_translations_display_name_fr` | character |  |
| `winner_match_team_translations_display_name_de` | character |  |
| `winner_match_team_translations_display_name_es` | character |  |
| `winner_match_team_translations_display_name_pt` | character |  |
| `winner_match_team_translations_display_name_it` | character |  |
| `winner_match_team_translations_display_name_ru` | character |  |
| `winner_match_team_translations_display_name_zh` | character |  |
| `winner_match_team_translations_display_name_ar` | character |  |
| `winner_match_team_translations_display_official_name_en` | character |  |
| `winner_match_team_translations_display_official_name_fr` | character |  |
| `winner_match_team_translations_display_official_name_de` | character |  |
| `winner_match_team_translations_display_official_name_es` | character |  |
| `winner_match_team_translations_display_official_name_pt` | character |  |
| `winner_match_team_translations_display_official_name_it` | character |  |
| `winner_match_team_translations_display_official_name_ru` | character |  |
| `winner_match_team_translations_display_official_name_zh` | character |  |
| `winner_match_team_translations_display_official_name_ar` | character |  |
| `winner_match_team_translations_display_team_code_en` | character |  |
| `winner_match_team_translations_display_team_code_fr` | character |  |
| `winner_match_team_translations_display_team_code_de` | character |  |
| `winner_match_team_translations_display_team_code_es` | character |  |
| `winner_match_team_translations_display_team_code_pt` | character |  |
| `winner_match_team_translations_display_team_code_it` | character |  |
| `winner_match_team_translations_display_team_code_ru` | character |  |
| `winner_match_team_translations_display_team_code_zh` | character |  |
| `winner_match_team_translations_display_team_code_ar` | character |  |
| `winner_match_team_translations_short_name_en` | character |  |
| `winner_match_team_translations_short_name_fr` | character |  |
| `winner_match_team_translations_short_name_de` | character |  |
| `winner_match_team_translations_short_name_es` | character |  |
| `winner_match_team_translations_short_name_pt` | character |  |
| `winner_match_team_translations_short_name_it` | character |  |
| `winner_match_team_translations_short_name_ru` | character |  |
| `winner_match_team_translations_short_name_zh` | character |  |
| `winner_match_team_translations_short_name_ar` | character |  |
| `winner_match_team_type_is_national` | logical |  |
| `winner_match_team_type_team` | character |  |
| `winner_match_translations_reason_text_ru` | character |  |
| `winner_match_translations_reason_text_de` | character |  |
| `winner_match_translations_reason_text_pt` | character |  |
| `winner_match_translations_reason_text_es` | character |  |
| `winner_match_translations_reason_text_it` | character |  |
| `winner_match_translations_reason_text_en` | character |  |
| `winner_match_translations_reason_text_zh` | character |  |
| `winner_match_translations_reason_text_ar` | character |  |
| `winner_match_translations_reason_text_fr` | character |  |
| `winner_match_translations_reason_text_abbr_ru` | character |  |
| `winner_match_translations_reason_text_abbr_de` | character |  |
| `winner_match_translations_reason_text_abbr_pt` | character |  |
| `winner_match_translations_reason_text_abbr_es` | character |  |
| `winner_match_translations_reason_text_abbr_it` | character |  |
| `winner_match_translations_reason_text_abbr_en` | character |  |
| `winner_match_translations_reason_text_abbr_zh` | character |  |
| `winner_match_translations_reason_text_abbr_ar` | character |  |
| `winner_match_translations_reason_text_abbr_fr` | character |  |
| `related_matches` | character |  |
| `condition_humidity` | integer |  |
| `condition_pitch_condition` | character |  |
| `condition_temperature` | integer |  |
| `condition_translations_pitch_condition_name_en` | character |  |
| `condition_translations_pitch_condition_name_fr` | character |  |
| `condition_translations_pitch_condition_name_de` | character |  |
| `condition_translations_pitch_condition_name_es` | character |  |
| `condition_translations_pitch_condition_name_pt` | character |  |
| `condition_translations_pitch_condition_name_it` | character |  |
| `condition_translations_pitch_condition_name_ru` | character |  |
| `condition_translations_pitch_condition_name_zh` | character |  |
| `condition_translations_pitch_condition_name_ar` | character |  |
| `condition_translations_weather_condition_name_en` | character |  |
| `condition_translations_weather_condition_name_fr` | character |  |
| `condition_translations_weather_condition_name_de` | character |  |
| `condition_translations_weather_condition_name_es` | character |  |
| `condition_translations_weather_condition_name_pt` | character |  |
| `condition_translations_weather_condition_name_it` | character |  |
| `condition_translations_weather_condition_name_ru` | character |  |
| `condition_translations_weather_condition_name_zh` | character |  |
| `condition_translations_weather_condition_name_ar` | character |  |
| `condition_weather_condition` | character |  |
| `condition_wind_speed` | integer |  |
| `leg_date_time_from` | character |  |
| `leg_date_time_to` | character |  |
| `leg_number` | integer | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `leg_translations_name_en` | character |  |
| `leg_translations_name_fr` | character |  |
| `leg_translations_name_de` | character |  |
| `leg_translations_name_es` | character |  |
| `leg_translations_name_pt` | character |  |
| `leg_translations_name_it` | character |  |
| `leg_translations_name_ru` | character |  |
| `leg_translations_name_zh` | character |  |
| `leg_translations_name_ar` | character |  |
| `player_of_the_match_player_translations_first_name_en` | character |  |
| `player_of_the_match_player_translations_first_name_fr` | character |  |
| `player_of_the_match_player_translations_first_name_de` | character |  |
| `player_of_the_match_player_translations_first_name_es` | character |  |
| `player_of_the_match_player_translations_first_name_pt` | character |  |
| `player_of_the_match_player_translations_first_name_it` | character |  |
| `player_of_the_match_player_translations_first_name_ru` | character |  |
| `player_of_the_match_player_translations_first_name_zh` | character |  |
| `player_of_the_match_player_translations_first_name_ar` | character |  |
| `score_aggregate_away` | integer |  |
| `score_aggregate_home` | integer |  |
| `winner_aggregate_reason` | character |  |
| `winner_aggregate_team_association_id` | character |  |
| `winner_aggregate_team_association_logo_url` | character |  |
| `winner_aggregate_team_big_logo_url` | character |  |
| `winner_aggregate_team_confederation_type` | character |  |
| `winner_aggregate_team_country_code` | character |  |
| `winner_aggregate_team_id` | character |  |
| `winner_aggregate_team_id_provider` | character |  |
| `winner_aggregate_team_international_name` | character |  |
| `winner_aggregate_team_is_place_holder` | logical |  |
| `winner_aggregate_team_logo_url` | character |  |
| `winner_aggregate_team_medium_logo_url` | character |  |
| `winner_aggregate_team_organization_id` | character |  |
| `winner_aggregate_team_team_code` | character |  |
| `winner_aggregate_team_team_type_detail` | character |  |
| `winner_aggregate_team_translations_country_name_en` | character |  |
| `winner_aggregate_team_translations_country_name_fr` | character |  |
| `winner_aggregate_team_translations_country_name_de` | character |  |
| `winner_aggregate_team_translations_country_name_es` | character |  |
| `winner_aggregate_team_translations_country_name_pt` | character |  |
| `winner_aggregate_team_translations_country_name_it` | character |  |
| `winner_aggregate_team_translations_country_name_ru` | character |  |
| `winner_aggregate_team_translations_country_name_zh` | character |  |
| `winner_aggregate_team_translations_country_name_ar` | character |  |
| `winner_aggregate_team_translations_display_name_en` | character |  |
| `winner_aggregate_team_translations_display_name_fr` | character |  |
| `winner_aggregate_team_translations_display_name_de` | character |  |
| `winner_aggregate_team_translations_display_name_es` | character |  |
| `winner_aggregate_team_translations_display_name_pt` | character |  |
| `winner_aggregate_team_translations_display_name_it` | character |  |
| `winner_aggregate_team_translations_display_name_ru` | character |  |
| `winner_aggregate_team_translations_display_name_zh` | character |  |
| `winner_aggregate_team_translations_display_name_ar` | character |  |
| `winner_aggregate_team_translations_display_official_name_en` | character |  |
| `winner_aggregate_team_translations_display_official_name_fr` | character |  |
| `winner_aggregate_team_translations_display_official_name_de` | character |  |
| `winner_aggregate_team_translations_display_official_name_es` | character |  |
| `winner_aggregate_team_translations_display_official_name_pt` | character |  |
| `winner_aggregate_team_translations_display_official_name_it` | character |  |
| `winner_aggregate_team_translations_display_official_name_ru` | character |  |
| `winner_aggregate_team_translations_display_official_name_zh` | character |  |
| `winner_aggregate_team_translations_display_official_name_ar` | character |  |
| `winner_aggregate_team_translations_display_team_code_en` | character |  |
| `winner_aggregate_team_translations_display_team_code_fr` | character |  |
| `winner_aggregate_team_translations_display_team_code_de` | character |  |
| `winner_aggregate_team_translations_display_team_code_es` | character |  |
| `winner_aggregate_team_translations_display_team_code_pt` | character |  |
| `winner_aggregate_team_translations_display_team_code_it` | character |  |
| `winner_aggregate_team_translations_display_team_code_ru` | character |  |
| `winner_aggregate_team_translations_display_team_code_zh` | character |  |
| `winner_aggregate_team_translations_display_team_code_ar` | character |  |
| `winner_aggregate_team_translations_short_name_en` | character |  |
| `winner_aggregate_team_translations_short_name_fr` | character |  |
| `winner_aggregate_team_translations_short_name_de` | character |  |
| `winner_aggregate_team_translations_short_name_es` | character |  |
| `winner_aggregate_team_translations_short_name_pt` | character |  |
| `winner_aggregate_team_translations_short_name_it` | character |  |
| `winner_aggregate_team_translations_short_name_ru` | character |  |
| `winner_aggregate_team_translations_short_name_zh` | character |  |
| `winner_aggregate_team_translations_short_name_ar` | character |  |
| `winner_aggregate_team_type_is_national` | logical |  |
| `winner_aggregate_team_type_team` | character |  |
| `winner_aggregate_translations_reason_text_abbr_ru` | character |  |
| `winner_aggregate_translations_reason_text_abbr_de` | character |  |
| `winner_aggregate_translations_reason_text_abbr_pt` | character |  |
| `winner_aggregate_translations_reason_text_abbr_es` | character |  |
| `winner_aggregate_translations_reason_text_abbr_it` | character |  |
| `winner_aggregate_translations_reason_text_abbr_en` | character |  |
| `winner_aggregate_translations_reason_text_abbr_zh` | character |  |
| `winner_aggregate_translations_reason_text_abbr_ar` | character |  |
| `winner_aggregate_translations_reason_text_abbr_fr` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_matches-example}

```python
uefa_matches(competition_id='1', limit='3', offset='0', season_year='2026')
```

_Last validated n/a._

## uefa_players

Players registered in a UEFA competition season.

**Endpoint URL:** `GET https://comp.uefa.com/v2/players`

**Valid URL:** [https://comp.uefa.com/v2/players?competitionId=1&seasonYear=2026&limit=3&offset=0](https://comp.uefa.com/v2/players?competitionId=1&seasonYear=2026&limit=3&offset=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Required. 1 = Champions League, 3 = Europa League, 2019 = Conference League (see /v2/competitions). |
| `seasonYear` | `season_year` |  |  | `Y` | The season's end year (2026 = 2025-26). |
| `limit` | `limit` |  |  | `Y` | Page size. |
| `offset` | `offset` |  |  | `Y` | Page offset. |

### Returns {#uefa_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `age` | character | Player age (in years). |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `club_jersey_number` | character |  |
| `club_shirt_name` | character |  |
| `country_code` | character | ISO country code. |
| `detailed_field_position` | character |  |
| `gender` | character | League gender designation. |
| `height` | integer | Player height (string e.g. '6-2' or inches). |
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `image_url` | character | Player headshot URL. |
| `international_name` | character |  |
| `national_jersey_number` | character |  |
| `national_shirt_name` | character |  |
| `national_team_id` | character |  |
| `weight` | integer | Player weight in pounds. |
| `translations_country_name_en` | character |  |
| `translations_country_name_fr` | character |  |
| `translations_country_name_de` | character |  |
| `translations_country_name_es` | character |  |
| `translations_country_name_pt` | character |  |
| `translations_country_name_it` | character |  |
| `translations_country_name_ru` | character |  |
| `translations_country_name_zh` | character |  |
| `translations_country_name_ar` | character |  |
| `translations_first_name_en` | character |  |
| `translations_first_name_fr` | character |  |
| `translations_first_name_de` | character |  |
| `translations_first_name_es` | character |  |
| `translations_first_name_pt` | character |  |
| `translations_first_name_it` | character |  |
| `translations_first_name_ru` | character |  |
| `translations_first_name_zh` | character |  |
| `translations_first_name_ar` | character |  |
| `translations_last_name_en` | character |  |
| `translations_last_name_fr` | character |  |
| `translations_last_name_de` | character |  |
| `translations_last_name_es` | character |  |
| `translations_last_name_pt` | character |  |
| `translations_last_name_it` | character |  |
| `translations_last_name_ru` | character |  |
| `translations_last_name_zh` | character |  |
| `translations_last_name_ar` | character |  |
| `translations_name_en` | character |  |
| `translations_name_fr` | character |  |
| `translations_name_de` | character |  |
| `translations_name_es` | character |  |
| `translations_name_pt` | character |  |
| `translations_name_it` | character |  |
| `translations_name_ru` | character |  |
| `translations_name_zh` | character |  |
| `translations_name_ar` | character |  |
| `translations_short_name_en` | character |  |
| `translations_short_name_fr` | character |  |
| `translations_short_name_de` | character |  |
| `translations_short_name_es` | character |  |
| `translations_short_name_pt` | character |  |
| `translations_short_name_it` | character |  |
| `translations_short_name_ru` | character |  |
| `translations_short_name_zh` | character |  |
| `translations_short_name_ar` | character |  |
| `club_id` | character | Provider identifier for the entity (Utf8 join key). |
| `field_position` | character | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `national_field_position` | character |  |
| `translations_field_position_en` | character |  |
| `translations_field_position_fr` | character |  |
| `translations_field_position_de` | character |  |
| `translations_field_position_es` | character |  |
| `translations_field_position_pt` | character |  |
| `translations_field_position_it` | character |  |
| `translations_field_position_ru` | character |  |
| `translations_field_position_zh` | character |  |
| `translations_field_position_ar` | character |  |
| `translations_national_field_position_en` | character |  |
| `translations_national_field_position_fr` | character |  |
| `translations_national_field_position_de` | character |  |
| `translations_national_field_position_es` | character |  |
| `translations_national_field_position_pt` | character |  |
| `translations_national_field_position_it` | character |  |
| `translations_national_field_position_ru` | character |  |
| `translations_national_field_position_zh` | character |  |
| `translations_national_field_position_ar` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_players-example}

```python
uefa_players(competition_id='1', limit='3', offset='0', season_year='2026')
```

_Last validated n/a._

## uefa_standings

Group / league-phase standings of a UEFA competition season.

**Endpoint URL:** `GET https://standings.uefa.com/v1/standings`

**Valid URL:** [https://standings.uefa.com/v1/standings?competitionId=1&seasonYear=2026](https://standings.uefa.com/v1/standings?competitionId=1&seasonYear=2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Required. 1 = Champions League, 3 = Europa League, 2019 = Conference League (see /v2/competitions). |
| `seasonYear` | `season_year` |  |  | `Y` | The season's end year (2026 = 2025-26). |

### Returns {#uefa_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `items` | character |  |
| `qualification_labels` | character |  |
| `status` | character | Status label. |
| `group_competition_id` | character |  |
| `group_id` | character | ESPN group id. |
| `group_meta_data_group_name` | character |  |
| `group_meta_data_group_short_name` | character |  |
| `group_order` | integer |  |
| `group_phase` | character |  |
| `group_round_id` | character |  |
| `group_season_year` | character |  |
| `group_teams` | character |  |
| `group_teams_qualified_number` | integer |  |
| `group_translations_name_en` | character |  |
| `group_translations_name_fr` | character |  |
| `group_translations_name_de` | character |  |
| `group_translations_name_es` | character |  |
| `group_translations_name_pt` | character |  |
| `group_translations_name_it` | character |  |
| `group_translations_name_ru` | character |  |
| `group_translations_name_zh` | character |  |
| `group_translations_name_ar` | character |  |
| `group_translations_short_name_en` | character |  |
| `group_translations_short_name_fr` | character |  |
| `group_translations_short_name_de` | character |  |
| `group_translations_short_name_es` | character |  |
| `group_translations_short_name_pt` | character |  |
| `group_translations_short_name_it` | character |  |
| `group_translations_short_name_ru` | character |  |
| `group_translations_short_name_zh` | character |  |
| `group_translations_short_name_ar` | character |  |
| `group_type` | character |  |
| `round_active` | logical |  |
| `round_bench_gk_count` | integer |  |
| `round_bench_players_count` | integer |  |
| `round_bench_staff_count` | integer |  |
| `round_coefficient_winner_bonus` | numeric |  |
| `round_competition_id` | character |  |
| `round_date_from` | character |  |
| `round_date_to` | character |  |
| `round_field_players_count` | integer |  |
| `round_group_count` | integer |  |
| `round_id` | character | Composite id of the round. |
| `round_meta_data_name` | character |  |
| `round_meta_data_type` | character |  |
| `round_mode` | character |  |
| `round_mode_detail` | character |  |
| `round_order_in_competition` | integer |  |
| `round_phase` | character |  |
| `round_season_year` | character |  |
| `round_secondary_type` | character |  |
| `round_stadium_name_type` | character |  |
| `round_standings_ranking_formula_id` | character |  |
| `round_status` | character |  |
| `round_substitution_count` | integer |  |
| `round_team_count` | integer |  |
| `round_teams` | character |  |
| `round_translations_abbreviation_en` | character |  |
| `round_translations_abbreviation_fr` | character |  |
| `round_translations_abbreviation_de` | character |  |
| `round_translations_abbreviation_es` | character |  |
| `round_translations_abbreviation_pt` | character |  |
| `round_translations_abbreviation_it` | character |  |
| `round_translations_abbreviation_ru` | character |  |
| `round_translations_abbreviation_zh` | character |  |
| `round_translations_abbreviation_ar` | character |  |
| `round_translations_name_en` | character |  |
| `round_translations_name_fr` | character |  |
| `round_translations_name_de` | character |  |
| `round_translations_name_es` | character |  |
| `round_translations_name_pt` | character |  |
| `round_translations_name_it` | character |  |
| `round_translations_name_ru` | character |  |
| `round_translations_name_zh` | character |  |
| `round_translations_name_ar` | character |  |
| `round_translations_short_name_en` | character |  |
| `round_translations_short_name_fr` | character |  |
| `round_translations_short_name_de` | character |  |
| `round_translations_short_name_es` | character |  |
| `round_translations_short_name_pt` | character |  |
| `round_translations_short_name_it` | character |  |
| `round_translations_short_name_ru` | character |  |
| `round_translations_short_name_zh` | character |  |
| `round_translations_short_name_ar` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_standings-example}

```python
uefa_standings(competition_id='1', season_year='2026')
```

_Last validated n/a._

## uefa_team_statistics

Per-team match statistics of one UEFA match (one row per team).

**Endpoint URL:** `GET https://matchstats.uefa.com/v1/team-statistics/{match_id}`

**Valid URL:** [https://matchstats.uefa.com/v1/team-statistics/2047742](https://matchstats.uefa.com/v1/team-statistics/2047742)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `match_id` | `match_id` |  | `Y` |  | Match id (from /v5/matches). |

### Returns {#uefa_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_provider` | character |  |
| `statistics` | character | The team's full statistic list for the match, JSON-encoded (one {name, value} object per statistic). |
| `team_id` | character | Club: provider identifier for the entity (Utf8 join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_team_statistics-example}

```python
uefa_team_statistics(match_id='2047742')
```

_Last validated n/a._

## uefa_teams

Teams entered in a UEFA competition season.

**Endpoint URL:** `GET https://comp.uefa.com/v2/teams`

**Valid URL:** [https://comp.uefa.com/v2/teams?competitionId=1&seasonYear=2026&limit=3&offset=0](https://comp.uefa.com/v2/teams?competitionId=1&seasonYear=2026&limit=3&offset=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `competitionId` | `competition_id` |  |  | `Y` | Required. 1 = Champions League, 3 = Europa League, 2019 = Conference League (see /v2/competitions). |
| `seasonYear` | `season_year` |  |  | `Y` | The season's end year (2026 = 2025-26). |
| `limit` | `limit` |  |  | `Y` | Page size. |
| `offset` | `offset` |  |  | `Y` | Page offset. |

### Returns {#uefa_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `association_id` | character |  |
| `association_logo_url` | character |  |
| `big_logo_url` | character |  |
| `confederation_type` | character |  |
| `country_code` | character | ISO country code. |
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `id_provider` | character |  |
| `international_name` | character |  |
| `is_place_holder` | logical |  |
| `logo_url` | character | NBA CDN primary logo URL. |
| `medium_logo_url` | character |  |
| `organization_id` | character |  |
| `team_code` | character | Internal team code. |
| `team_type_detail` | character |  |
| `type_is_national` | logical |  |
| `type_team` | character |  |
| `translations_country_name_en` | character |  |
| `translations_country_name_fr` | character |  |
| `translations_country_name_de` | character |  |
| `translations_country_name_es` | character |  |
| `translations_country_name_pt` | character |  |
| `translations_country_name_it` | character |  |
| `translations_country_name_ru` | character |  |
| `translations_country_name_zh` | character |  |
| `translations_country_name_ar` | character |  |
| `translations_display_name_en` | character |  |
| `translations_display_name_fr` | character |  |
| `translations_display_name_de` | character |  |
| `translations_display_name_es` | character |  |
| `translations_display_name_pt` | character |  |
| `translations_display_name_it` | character |  |
| `translations_display_name_ru` | character |  |
| `translations_display_name_zh` | character |  |
| `translations_display_name_ar` | character |  |
| `translations_display_official_name_en` | character |  |
| `translations_display_official_name_fr` | character |  |
| `translations_display_official_name_de` | character |  |
| `translations_display_official_name_es` | character |  |
| `translations_display_official_name_pt` | character |  |
| `translations_display_official_name_it` | character |  |
| `translations_display_official_name_ru` | character |  |
| `translations_display_official_name_zh` | character |  |
| `translations_display_official_name_ar` | character |  |
| `translations_display_team_code_en` | character |  |
| `translations_display_team_code_fr` | character |  |
| `translations_display_team_code_de` | character |  |
| `translations_display_team_code_es` | character |  |
| `translations_display_team_code_pt` | character |  |
| `translations_display_team_code_it` | character |  |
| `translations_display_team_code_ru` | character |  |
| `translations_display_team_code_zh` | character |  |
| `translations_display_team_code_ar` | character |  |
| `translations_short_name_en` | character |  |
| `translations_short_name_fr` | character |  |
| `translations_short_name_de` | character |  |
| `translations_short_name_es` | character |  |
| `translations_short_name_pt` | character |  |
| `translations_short_name_it` | character |  |
| `translations_short_name_ru` | character |  |
| `translations_short_name_zh` | character |  |
| `translations_short_name_ar` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#uefa_teams-example}

```python
uefa_teams(competition_id='1', limit='3', offset='0', season_year='2026')
```

_Last validated n/a._
