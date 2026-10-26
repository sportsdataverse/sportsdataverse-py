---
title: Soccer (all) — UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)
sidebar_label: UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)
description: "Soccer (all) — UEFA front-end APIs (comp/match/standings/matchstats.uefa.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 12
toc_max_heading_level: 2
---
# Soccer (all) — UEFA front-end APIs (comp/match/standings/matchstats.uefa.com)

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
| `age` | String | Age category of the competition (e.g. 'ADULT', 'U21'). |
| `code` | String | UEFA competition code (e.g. 'UCL', 'EURO', 'UECL'). |
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `region` | String | UEFA region of the competition (e.g. 'CONTINENTAL'). |
| `sex` | String |  |
| `sports_type` | String |  |
| `team_category` | String |  |
| `type` | String | Type discriminator for the record. |
| `images_full_logo` | String |  |
| `meta_data_name` | String |  |
| `translations_name_en` | String |  |
| `translations_name_fr` | String |  |
| `translations_name_de` | String |  |
| `translations_name_es` | String |  |
| `translations_name_pt` | String |  |
| `translations_name_it` | String |  |
| `translations_name_ru` | String |  |
| `translations_name_zh` | String |  |
| `translations_name_ar` | String |  |
| `translations_prequalifying_name_en` | String |  |
| `translations_prequalifying_name_fr` | String |  |
| `translations_prequalifying_name_de` | String |  |
| `translations_prequalifying_name_es` | String |  |
| `translations_prequalifying_name_pt` | String |  |
| `translations_prequalifying_name_it` | String |  |
| `translations_prequalifying_name_ru` | String |  |
| `translations_prequalifying_name_zh` | String |  |
| `translations_prequalifying_name_ar` | String |  |
| `translations_qualifying_name_en` | String |  |
| `translations_qualifying_name_fr` | String |  |
| `translations_qualifying_name_de` | String |  |
| `translations_qualifying_name_es` | String |  |
| `translations_qualifying_name_pt` | String |  |
| `translations_qualifying_name_it` | String |  |
| `translations_qualifying_name_ru` | String |  |
| `translations_qualifying_name_zh` | String |  |
| `translations_qualifying_name_ar` | String |  |
| `translations_tournament_name_en` | String |  |
| `translations_tournament_name_fr` | String |  |
| `translations_tournament_name_de` | String |  |
| `translations_tournament_name_es` | String |  |
| `translations_tournament_name_pt` | String |  |
| `translations_tournament_name_it` | String |  |
| `translations_tournament_name_ru` | String |  |
| `translations_tournament_name_zh` | String |  |
| `translations_tournament_name_ar` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_competitions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_competitions-example}

```python
uefa_competitions(competition_ids='1,3,2019')
```

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
| `hash` | String |  |
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `lineup_status` | String |  |
| `status` | String | UEFA match status (e.g. 'UPCOMING', 'LIVE', 'FINISHED'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_livescore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_livescore-example}

```python
uefa_livescore()
```

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
| `behind_closed_doors` | Boolean |  |
| `competition_phase` | String | Competition phase: TOURNAMENT (main draw) or QUALIFYING. |
| `full_time_at` | String |  |
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `lineup_status` | String |  |
| `match_attendance` | Int64 |  |
| `referees` | String |  |
| `season_year` | String | Season year of the match as a string (e.g. '2026'). |
| `session_number` | Int64 |  |
| `status` | String | UEFA match status (e.g. 'UPCOMING', 'LIVE', 'FINISHED'). |
| `type` | String | Type discriminator for the record. |
| `away_team_association_id` | String |  |
| `away_team_association_logo_url` | String |  |
| `away_team_big_logo_url` | String |  |
| `away_team_confederation_type` | String |  |
| `away_team_country_code` | String | Away club: club: ISO country code. |
| `away_team_id` | String | Away club: club: provider identifier for the entity (Utf8 join key). |
| `away_team_id_provider` | String |  |
| `away_team_international_name` | String |  |
| `away_team_is_place_holder` | Boolean |  |
| `away_team_logo_url` | String |  |
| `away_team_medium_logo_url` | String |  |
| `away_team_organization_id` | String |  |
| `away_team_team_code` | String | Away team's three-letter UEFA code (e.g. 'PSG'); null when the team is not yet known. |
| `away_team_team_type_detail` | String |  |
| `away_team_translations_country_name_en` | String |  |
| `away_team_translations_country_name_fr` | String |  |
| `away_team_translations_country_name_de` | String |  |
| `away_team_translations_country_name_es` | String |  |
| `away_team_translations_country_name_pt` | String |  |
| `away_team_translations_country_name_it` | String |  |
| `away_team_translations_country_name_ru` | String |  |
| `away_team_translations_country_name_zh` | String |  |
| `away_team_translations_country_name_ar` | String |  |
| `away_team_translations_display_name_en` | String |  |
| `away_team_translations_display_name_fr` | String |  |
| `away_team_translations_display_name_de` | String |  |
| `away_team_translations_display_name_es` | String |  |
| `away_team_translations_display_name_pt` | String |  |
| `away_team_translations_display_name_it` | String |  |
| `away_team_translations_display_name_ru` | String |  |
| `away_team_translations_display_name_zh` | String |  |
| `away_team_translations_display_name_ar` | String |  |
| `away_team_translations_display_official_name_en` | String |  |
| `away_team_translations_display_official_name_fr` | String |  |
| `away_team_translations_display_official_name_de` | String |  |
| `away_team_translations_display_official_name_es` | String |  |
| `away_team_translations_display_official_name_pt` | String |  |
| `away_team_translations_display_official_name_it` | String |  |
| `away_team_translations_display_official_name_ru` | String |  |
| `away_team_translations_display_official_name_zh` | String |  |
| `away_team_translations_display_official_name_ar` | String |  |
| `away_team_translations_display_team_code_en` | String |  |
| `away_team_translations_display_team_code_fr` | String |  |
| `away_team_translations_display_team_code_de` | String |  |
| `away_team_translations_display_team_code_es` | String |  |
| `away_team_translations_display_team_code_pt` | String |  |
| `away_team_translations_display_team_code_it` | String |  |
| `away_team_translations_display_team_code_ru` | String |  |
| `away_team_translations_display_team_code_zh` | String |  |
| `away_team_translations_display_team_code_ar` | String |  |
| `away_team_translations_short_name_en` | String |  |
| `away_team_translations_short_name_fr` | String |  |
| `away_team_translations_short_name_de` | String |  |
| `away_team_translations_short_name_es` | String |  |
| `away_team_translations_short_name_pt` | String |  |
| `away_team_translations_short_name_it` | String |  |
| `away_team_translations_short_name_ru` | String |  |
| `away_team_translations_short_name_zh` | String |  |
| `away_team_translations_short_name_ar` | String |  |
| `away_team_type_is_national` | Boolean |  |
| `away_team_type_team` | String |  |
| `competition_age` | String |  |
| `competition_code` | String |  |
| `competition_id` | String | Competition: provider identifier for the entity (Utf8 join key). |
| `competition_images_full_logo` | String |  |
| `competition_meta_data_name` | String |  |
| `competition_region` | String |  |
| `competition_sex` | String |  |
| `competition_sports_type` | String |  |
| `competition_team_category` | String |  |
| `competition_translations_name_en` | String |  |
| `competition_translations_name_fr` | String |  |
| `competition_translations_name_de` | String |  |
| `competition_translations_name_es` | String |  |
| `competition_translations_name_pt` | String |  |
| `competition_translations_name_it` | String |  |
| `competition_translations_name_ru` | String |  |
| `competition_translations_name_zh` | String |  |
| `competition_translations_name_ar` | String |  |
| `competition_translations_prequalifying_name_en` | String |  |
| `competition_translations_prequalifying_name_fr` | String |  |
| `competition_translations_prequalifying_name_de` | String |  |
| `competition_translations_prequalifying_name_es` | String |  |
| `competition_translations_prequalifying_name_pt` | String |  |
| `competition_translations_prequalifying_name_it` | String |  |
| `competition_translations_prequalifying_name_ru` | String |  |
| `competition_translations_prequalifying_name_zh` | String |  |
| `competition_translations_prequalifying_name_ar` | String |  |
| `competition_translations_qualifying_name_en` | String |  |
| `competition_translations_qualifying_name_fr` | String |  |
| `competition_translations_qualifying_name_de` | String |  |
| `competition_translations_qualifying_name_es` | String |  |
| `competition_translations_qualifying_name_pt` | String |  |
| `competition_translations_qualifying_name_it` | String |  |
| `competition_translations_qualifying_name_ru` | String |  |
| `competition_translations_qualifying_name_zh` | String |  |
| `competition_translations_qualifying_name_ar` | String |  |
| `competition_translations_tournament_name_en` | String |  |
| `competition_translations_tournament_name_fr` | String |  |
| `competition_translations_tournament_name_de` | String |  |
| `competition_translations_tournament_name_es` | String |  |
| `competition_translations_tournament_name_pt` | String |  |
| `competition_translations_tournament_name_it` | String |  |
| `competition_translations_tournament_name_ru` | String |  |
| `competition_translations_tournament_name_zh` | String |  |
| `competition_translations_tournament_name_ar` | String |  |
| `competition_type` | String | Competition: type discriminator for the record. |
| `home_team_association_id` | String |  |
| `home_team_association_logo_url` | String |  |
| `home_team_big_logo_url` | String |  |
| `home_team_confederation_type` | String |  |
| `home_team_country_code` | String | Home club: club: ISO country code. |
| `home_team_id` | String | Home club: club: provider identifier for the entity (Utf8 join key). |
| `home_team_id_provider` | String |  |
| `home_team_international_name` | String |  |
| `home_team_is_place_holder` | Boolean |  |
| `home_team_logo_url` | String |  |
| `home_team_medium_logo_url` | String |  |
| `home_team_organization_id` | String |  |
| `home_team_team_code` | String | Home team's three-letter UEFA code (e.g. 'ARS'); null when the team is not yet known. |
| `home_team_team_type_detail` | String |  |
| `home_team_translations_country_name_en` | String |  |
| `home_team_translations_country_name_fr` | String |  |
| `home_team_translations_country_name_de` | String |  |
| `home_team_translations_country_name_es` | String |  |
| `home_team_translations_country_name_pt` | String |  |
| `home_team_translations_country_name_it` | String |  |
| `home_team_translations_country_name_ru` | String |  |
| `home_team_translations_country_name_zh` | String |  |
| `home_team_translations_country_name_ar` | String |  |
| `home_team_translations_display_name_en` | String |  |
| `home_team_translations_display_name_fr` | String |  |
| `home_team_translations_display_name_de` | String |  |
| `home_team_translations_display_name_es` | String |  |
| `home_team_translations_display_name_pt` | String |  |
| `home_team_translations_display_name_it` | String |  |
| `home_team_translations_display_name_ru` | String |  |
| `home_team_translations_display_name_zh` | String |  |
| `home_team_translations_display_name_ar` | String |  |
| `home_team_translations_display_official_name_en` | String |  |
| `home_team_translations_display_official_name_fr` | String |  |
| `home_team_translations_display_official_name_de` | String |  |
| `home_team_translations_display_official_name_es` | String |  |
| `home_team_translations_display_official_name_pt` | String |  |
| `home_team_translations_display_official_name_it` | String |  |
| `home_team_translations_display_official_name_ru` | String |  |
| `home_team_translations_display_official_name_zh` | String |  |
| `home_team_translations_display_official_name_ar` | String |  |
| `home_team_translations_display_team_code_en` | String |  |
| `home_team_translations_display_team_code_fr` | String |  |
| `home_team_translations_display_team_code_de` | String |  |
| `home_team_translations_display_team_code_es` | String |  |
| `home_team_translations_display_team_code_pt` | String |  |
| `home_team_translations_display_team_code_it` | String |  |
| `home_team_translations_display_team_code_ru` | String |  |
| `home_team_translations_display_team_code_zh` | String |  |
| `home_team_translations_display_team_code_ar` | String |  |
| `home_team_translations_short_name_en` | String |  |
| `home_team_translations_short_name_fr` | String |  |
| `home_team_translations_short_name_de` | String |  |
| `home_team_translations_short_name_es` | String |  |
| `home_team_translations_short_name_pt` | String |  |
| `home_team_translations_short_name_it` | String |  |
| `home_team_translations_short_name_ru` | String |  |
| `home_team_translations_short_name_zh` | String |  |
| `home_team_translations_short_name_ar` | String |  |
| `home_team_type_is_national` | Boolean |  |
| `home_team_type_team` | String |  |
| `kick_off_time_date` | String |  |
| `kick_off_time_date_time` | String |  |
| `kick_off_time_utc_offset_in_hours` | Int64 |  |
| `matchday_competition_id` | String |  |
| `matchday_date_from` | String |  |
| `matchday_date_to` | String |  |
| `matchday_format` | String |  |
| `matchday_id` | String |  |
| `matchday_long_name` | String |  |
| `matchday_name` | String |  |
| `matchday_phase` | String |  |
| `matchday_round_id` | String |  |
| `matchday_season_year` | String |  |
| `matchday_sequence_number` | String |  |
| `matchday_translations_long_name_en` | String |  |
| `matchday_translations_long_name_fr` | String |  |
| `matchday_translations_long_name_de` | String |  |
| `matchday_translations_long_name_es` | String |  |
| `matchday_translations_long_name_pt` | String |  |
| `matchday_translations_long_name_it` | String |  |
| `matchday_translations_long_name_ru` | String |  |
| `matchday_translations_long_name_zh` | String |  |
| `matchday_translations_long_name_ar` | String |  |
| `matchday_translations_name_en` | String |  |
| `matchday_translations_name_fr` | String |  |
| `matchday_translations_name_de` | String |  |
| `matchday_translations_name_es` | String |  |
| `matchday_translations_name_pt` | String |  |
| `matchday_translations_name_it` | String |  |
| `matchday_translations_name_ru` | String |  |
| `matchday_translations_name_zh` | String |  |
| `matchday_translations_name_ar` | String |  |
| `matchday_type` | String |  |
| `player_events_penalty_scorers` | String |  |
| `player_events_scorers` | String |  |
| `player_of_the_match_player_age` | String |  |
| `player_of_the_match_player_birth_date` | String |  |
| `player_of_the_match_player_club_id` | String |  |
| `player_of_the_match_player_club_jersey_number` | String |  |
| `player_of_the_match_player_club_shirt_name` | String |  |
| `player_of_the_match_player_country_code` | String |  |
| `player_of_the_match_player_detailed_field_position` | String |  |
| `player_of_the_match_player_field_position` | String |  |
| `player_of_the_match_player_gender` | String |  |
| `player_of_the_match_player_id` | String |  |
| `player_of_the_match_player_image_url` | String |  |
| `player_of_the_match_player_international_name` | String |  |
| `player_of_the_match_player_national_field_position` | String |  |
| `player_of_the_match_player_national_jersey_number` | String |  |
| `player_of_the_match_player_national_shirt_name` | String |  |
| `player_of_the_match_player_national_team_id` | String |  |
| `player_of_the_match_player_translations_country_name_en` | String |  |
| `player_of_the_match_player_translations_country_name_fr` | String |  |
| `player_of_the_match_player_translations_country_name_de` | String |  |
| `player_of_the_match_player_translations_country_name_es` | String |  |
| `player_of_the_match_player_translations_country_name_pt` | String |  |
| `player_of_the_match_player_translations_country_name_it` | String |  |
| `player_of_the_match_player_translations_country_name_ru` | String |  |
| `player_of_the_match_player_translations_country_name_zh` | String |  |
| `player_of_the_match_player_translations_country_name_ar` | String |  |
| `player_of_the_match_player_translations_field_position_en` | String |  |
| `player_of_the_match_player_translations_field_position_fr` | String |  |
| `player_of_the_match_player_translations_field_position_de` | String |  |
| `player_of_the_match_player_translations_field_position_es` | String |  |
| `player_of_the_match_player_translations_field_position_pt` | String |  |
| `player_of_the_match_player_translations_field_position_it` | String |  |
| `player_of_the_match_player_translations_field_position_ru` | String |  |
| `player_of_the_match_player_translations_field_position_zh` | String |  |
| `player_of_the_match_player_translations_field_position_ar` | String |  |
| `player_of_the_match_player_translations_last_name_en` | String |  |
| `player_of_the_match_player_translations_last_name_fr` | String |  |
| `player_of_the_match_player_translations_last_name_de` | String |  |
| `player_of_the_match_player_translations_last_name_es` | String |  |
| `player_of_the_match_player_translations_last_name_pt` | String |  |
| `player_of_the_match_player_translations_last_name_it` | String |  |
| `player_of_the_match_player_translations_last_name_ru` | String |  |
| `player_of_the_match_player_translations_last_name_zh` | String |  |
| `player_of_the_match_player_translations_last_name_ar` | String |  |
| `player_of_the_match_player_translations_name_en` | String |  |
| `player_of_the_match_player_translations_name_fr` | String |  |
| `player_of_the_match_player_translations_name_de` | String |  |
| `player_of_the_match_player_translations_name_es` | String |  |
| `player_of_the_match_player_translations_name_pt` | String |  |
| `player_of_the_match_player_translations_name_it` | String |  |
| `player_of_the_match_player_translations_name_ru` | String |  |
| `player_of_the_match_player_translations_name_zh` | String |  |
| `player_of_the_match_player_translations_name_ar` | String |  |
| `player_of_the_match_player_translations_national_field_position_en` | String |  |
| `player_of_the_match_player_translations_national_field_position_fr` | String |  |
| `player_of_the_match_player_translations_national_field_position_de` | String |  |
| `player_of_the_match_player_translations_national_field_position_es` | String |  |
| `player_of_the_match_player_translations_national_field_position_pt` | String |  |
| `player_of_the_match_player_translations_national_field_position_it` | String |  |
| `player_of_the_match_player_translations_national_field_position_ru` | String |  |
| `player_of_the_match_player_translations_national_field_position_zh` | String |  |
| `player_of_the_match_player_translations_national_field_position_ar` | String |  |
| `player_of_the_match_player_translations_short_name_en` | String |  |
| `player_of_the_match_player_translations_short_name_fr` | String |  |
| `player_of_the_match_player_translations_short_name_de` | String |  |
| `player_of_the_match_player_translations_short_name_es` | String |  |
| `player_of_the_match_player_translations_short_name_pt` | String |  |
| `player_of_the_match_player_translations_short_name_it` | String |  |
| `player_of_the_match_player_translations_short_name_ru` | String |  |
| `player_of_the_match_player_translations_short_name_zh` | String |  |
| `player_of_the_match_player_translations_short_name_ar` | String |  |
| `player_of_the_match_team_id` | String |  |
| `round_active` | Boolean |  |
| `round_bench_gk_count` | Int64 |  |
| `round_bench_players_count` | Int64 |  |
| `round_bench_staff_count` | Int64 |  |
| `round_coefficient_winner_bonus` | Float64 |  |
| `round_competition_id` | String |  |
| `round_date_from` | String |  |
| `round_date_to` | String |  |
| `round_field_players_count` | Int64 |  |
| `round_group_count` | Int64 |  |
| `round_id` | String | Composite id of the round. |
| `round_meta_data_name` | String |  |
| `round_meta_data_type` | String |  |
| `round_mode` | String |  |
| `round_mode_detail` | String |  |
| `round_order_in_competition` | Int64 |  |
| `round_phase` | String |  |
| `round_season_year` | String |  |
| `round_secondary_type` | String |  |
| `round_stadium_name_type` | String |  |
| `round_status` | String |  |
| `round_substitution_count` | Int64 |  |
| `round_team_count` | Int64 |  |
| `round_teams` | String |  |
| `round_translations_abbreviation_en` | String |  |
| `round_translations_abbreviation_fr` | String |  |
| `round_translations_abbreviation_de` | String |  |
| `round_translations_abbreviation_es` | String |  |
| `round_translations_abbreviation_pt` | String |  |
| `round_translations_abbreviation_it` | String |  |
| `round_translations_abbreviation_ru` | String |  |
| `round_translations_abbreviation_zh` | String |  |
| `round_translations_abbreviation_ar` | String |  |
| `round_translations_name_en` | String |  |
| `round_translations_name_fr` | String |  |
| `round_translations_name_de` | String |  |
| `round_translations_name_es` | String |  |
| `round_translations_name_pt` | String |  |
| `round_translations_name_it` | String |  |
| `round_translations_name_ru` | String |  |
| `round_translations_name_zh` | String |  |
| `round_translations_name_ar` | String |  |
| `round_translations_short_name_en` | String |  |
| `round_translations_short_name_fr` | String |  |
| `round_translations_short_name_de` | String |  |
| `round_translations_short_name_es` | String |  |
| `round_translations_short_name_pt` | String |  |
| `round_translations_short_name_it` | String |  |
| `round_translations_short_name_ru` | String |  |
| `round_translations_short_name_zh` | String |  |
| `round_translations_short_name_ar` | String |  |
| `score_penalty_away` | Int64 |  |
| `score_penalty_home` | Int64 |  |
| `score_regular_away` | Int64 |  |
| `score_regular_home` | Int64 |  |
| `score_total_away` | Int64 |  |
| `score_total_home` | Int64 |  |
| `stadium_address` | String | Stadium: street address of the stadium. |
| `stadium_capacity` | Int64 | Stadium: seating capacity of the stadium. |
| `stadium_city_country_code` | String |  |
| `stadium_city_id` | String |  |
| `stadium_city_translations_name_en` | String |  |
| `stadium_city_translations_name_fr` | String |  |
| `stadium_city_translations_name_de` | String |  |
| `stadium_city_translations_name_es` | String |  |
| `stadium_city_translations_name_pt` | String |  |
| `stadium_city_translations_name_it` | String |  |
| `stadium_city_translations_name_ru` | String |  |
| `stadium_city_translations_name_zh` | String |  |
| `stadium_city_translations_name_ar` | String |  |
| `stadium_country_code` | String | Stadium: ISO country code. |
| `stadium_geolocation_latitude` | Float64 |  |
| `stadium_geolocation_longitude` | Float64 |  |
| `stadium_id` | String | Stadium: provider identifier for the entity (Utf8 join key). |
| `stadium_images_medium_wide` | String |  |
| `stadium_images_large_ultra_wide` | String |  |
| `stadium_opening_date` | String |  |
| `stadium_pitch_length` | Int64 |  |
| `stadium_pitch_width` | Int64 |  |
| `stadium_translations_media_name_en` | String |  |
| `stadium_translations_media_name_fr` | String |  |
| `stadium_translations_media_name_de` | String |  |
| `stadium_translations_media_name_es` | String |  |
| `stadium_translations_media_name_pt` | String |  |
| `stadium_translations_media_name_it` | String |  |
| `stadium_translations_media_name_ru` | String |  |
| `stadium_translations_media_name_zh` | String |  |
| `stadium_translations_media_name_ar` | String |  |
| `stadium_translations_name_en` | String |  |
| `stadium_translations_name_fr` | String |  |
| `stadium_translations_name_de` | String |  |
| `stadium_translations_name_es` | String |  |
| `stadium_translations_name_pt` | String |  |
| `stadium_translations_name_it` | String |  |
| `stadium_translations_name_ru` | String |  |
| `stadium_translations_name_zh` | String |  |
| `stadium_translations_name_ar` | String |  |
| `stadium_translations_official_name_en` | String |  |
| `stadium_translations_official_name_fr` | String |  |
| `stadium_translations_official_name_de` | String |  |
| `stadium_translations_official_name_es` | String |  |
| `stadium_translations_official_name_pt` | String |  |
| `stadium_translations_official_name_it` | String |  |
| `stadium_translations_official_name_ru` | String |  |
| `stadium_translations_official_name_zh` | String |  |
| `stadium_translations_official_name_ar` | String |  |
| `stadium_translations_special_events_name_en` | String |  |
| `stadium_translations_special_events_name_fr` | String |  |
| `stadium_translations_special_events_name_de` | String |  |
| `stadium_translations_special_events_name_es` | String |  |
| `stadium_translations_special_events_name_pt` | String |  |
| `stadium_translations_special_events_name_it` | String |  |
| `stadium_translations_special_events_name_ru` | String |  |
| `stadium_translations_special_events_name_zh` | String |  |
| `stadium_translations_special_events_name_ar` | String |  |
| `stadium_translations_sponsor_name_en` | String |  |
| `stadium_translations_sponsor_name_fr` | String |  |
| `stadium_translations_sponsor_name_de` | String |  |
| `stadium_translations_sponsor_name_es` | String |  |
| `stadium_translations_sponsor_name_pt` | String |  |
| `stadium_translations_sponsor_name_it` | String |  |
| `stadium_translations_sponsor_name_ru` | String |  |
| `stadium_translations_sponsor_name_zh` | String |  |
| `stadium_translations_sponsor_name_ar` | String |  |
| `winner_match_reason` | String |  |
| `winner_match_team_association_id` | String |  |
| `winner_match_team_association_logo_url` | String |  |
| `winner_match_team_big_logo_url` | String |  |
| `winner_match_team_confederation_type` | String |  |
| `winner_match_team_country_code` | String |  |
| `winner_match_team_id` | String |  |
| `winner_match_team_id_provider` | String |  |
| `winner_match_team_international_name` | String |  |
| `winner_match_team_is_place_holder` | Boolean |  |
| `winner_match_team_logo_url` | String |  |
| `winner_match_team_medium_logo_url` | String |  |
| `winner_match_team_organization_id` | String |  |
| `winner_match_team_team_code` | String |  |
| `winner_match_team_team_type_detail` | String |  |
| `winner_match_team_translations_country_name_en` | String |  |
| `winner_match_team_translations_country_name_fr` | String |  |
| `winner_match_team_translations_country_name_de` | String |  |
| `winner_match_team_translations_country_name_es` | String |  |
| `winner_match_team_translations_country_name_pt` | String |  |
| `winner_match_team_translations_country_name_it` | String |  |
| `winner_match_team_translations_country_name_ru` | String |  |
| `winner_match_team_translations_country_name_zh` | String |  |
| `winner_match_team_translations_country_name_ar` | String |  |
| `winner_match_team_translations_display_name_en` | String |  |
| `winner_match_team_translations_display_name_fr` | String |  |
| `winner_match_team_translations_display_name_de` | String |  |
| `winner_match_team_translations_display_name_es` | String |  |
| `winner_match_team_translations_display_name_pt` | String |  |
| `winner_match_team_translations_display_name_it` | String |  |
| `winner_match_team_translations_display_name_ru` | String |  |
| `winner_match_team_translations_display_name_zh` | String |  |
| `winner_match_team_translations_display_name_ar` | String |  |
| `winner_match_team_translations_display_official_name_en` | String |  |
| `winner_match_team_translations_display_official_name_fr` | String |  |
| `winner_match_team_translations_display_official_name_de` | String |  |
| `winner_match_team_translations_display_official_name_es` | String |  |
| `winner_match_team_translations_display_official_name_pt` | String |  |
| `winner_match_team_translations_display_official_name_it` | String |  |
| `winner_match_team_translations_display_official_name_ru` | String |  |
| `winner_match_team_translations_display_official_name_zh` | String |  |
| `winner_match_team_translations_display_official_name_ar` | String |  |
| `winner_match_team_translations_display_team_code_en` | String |  |
| `winner_match_team_translations_display_team_code_fr` | String |  |
| `winner_match_team_translations_display_team_code_de` | String |  |
| `winner_match_team_translations_display_team_code_es` | String |  |
| `winner_match_team_translations_display_team_code_pt` | String |  |
| `winner_match_team_translations_display_team_code_it` | String |  |
| `winner_match_team_translations_display_team_code_ru` | String |  |
| `winner_match_team_translations_display_team_code_zh` | String |  |
| `winner_match_team_translations_display_team_code_ar` | String |  |
| `winner_match_team_translations_short_name_en` | String |  |
| `winner_match_team_translations_short_name_fr` | String |  |
| `winner_match_team_translations_short_name_de` | String |  |
| `winner_match_team_translations_short_name_es` | String |  |
| `winner_match_team_translations_short_name_pt` | String |  |
| `winner_match_team_translations_short_name_it` | String |  |
| `winner_match_team_translations_short_name_ru` | String |  |
| `winner_match_team_translations_short_name_zh` | String |  |
| `winner_match_team_translations_short_name_ar` | String |  |
| `winner_match_team_type_is_national` | Boolean |  |
| `winner_match_team_type_team` | String |  |
| `winner_match_translations_reason_text_ru` | String |  |
| `winner_match_translations_reason_text_de` | String |  |
| `winner_match_translations_reason_text_pt` | String |  |
| `winner_match_translations_reason_text_es` | String |  |
| `winner_match_translations_reason_text_it` | String |  |
| `winner_match_translations_reason_text_en` | String |  |
| `winner_match_translations_reason_text_zh` | String |  |
| `winner_match_translations_reason_text_ar` | String |  |
| `winner_match_translations_reason_text_fr` | String |  |
| `winner_match_translations_reason_text_abbr_ru` | String |  |
| `winner_match_translations_reason_text_abbr_de` | String |  |
| `winner_match_translations_reason_text_abbr_pt` | String |  |
| `winner_match_translations_reason_text_abbr_es` | String |  |
| `winner_match_translations_reason_text_abbr_it` | String |  |
| `winner_match_translations_reason_text_abbr_en` | String |  |
| `winner_match_translations_reason_text_abbr_zh` | String |  |
| `winner_match_translations_reason_text_abbr_ar` | String |  |
| `winner_match_translations_reason_text_abbr_fr` | String |  |
| `related_matches` | String |  |
| `condition_humidity` | Int64 |  |
| `condition_pitch_condition` | String |  |
| `condition_temperature` | Int64 |  |
| `condition_translations_pitch_condition_name_en` | String |  |
| `condition_translations_pitch_condition_name_fr` | String |  |
| `condition_translations_pitch_condition_name_de` | String |  |
| `condition_translations_pitch_condition_name_es` | String |  |
| `condition_translations_pitch_condition_name_pt` | String |  |
| `condition_translations_pitch_condition_name_it` | String |  |
| `condition_translations_pitch_condition_name_ru` | String |  |
| `condition_translations_pitch_condition_name_zh` | String |  |
| `condition_translations_pitch_condition_name_ar` | String |  |
| `condition_translations_weather_condition_name_en` | String |  |
| `condition_translations_weather_condition_name_fr` | String |  |
| `condition_translations_weather_condition_name_de` | String |  |
| `condition_translations_weather_condition_name_es` | String |  |
| `condition_translations_weather_condition_name_pt` | String |  |
| `condition_translations_weather_condition_name_it` | String |  |
| `condition_translations_weather_condition_name_ru` | String |  |
| `condition_translations_weather_condition_name_zh` | String |  |
| `condition_translations_weather_condition_name_ar` | String |  |
| `condition_weather_condition` | String |  |
| `condition_wind_speed` | Int64 |  |
| `leg_date_time_from` | String |  |
| `leg_date_time_to` | String |  |
| `leg_number` | Int64 | Ordinal of this leg within a multi-leg tie, counting from 1. |
| `leg_translations_name_en` | String |  |
| `leg_translations_name_fr` | String |  |
| `leg_translations_name_de` | String |  |
| `leg_translations_name_es` | String |  |
| `leg_translations_name_pt` | String |  |
| `leg_translations_name_it` | String |  |
| `leg_translations_name_ru` | String |  |
| `leg_translations_name_zh` | String |  |
| `leg_translations_name_ar` | String |  |
| `player_of_the_match_player_translations_first_name_en` | String |  |
| `player_of_the_match_player_translations_first_name_fr` | String |  |
| `player_of_the_match_player_translations_first_name_de` | String |  |
| `player_of_the_match_player_translations_first_name_es` | String |  |
| `player_of_the_match_player_translations_first_name_pt` | String |  |
| `player_of_the_match_player_translations_first_name_it` | String |  |
| `player_of_the_match_player_translations_first_name_ru` | String |  |
| `player_of_the_match_player_translations_first_name_zh` | String |  |
| `player_of_the_match_player_translations_first_name_ar` | String |  |
| `score_aggregate_away` | Int64 |  |
| `score_aggregate_home` | Int64 |  |
| `winner_aggregate_reason` | String |  |
| `winner_aggregate_team_association_id` | String |  |
| `winner_aggregate_team_association_logo_url` | String |  |
| `winner_aggregate_team_big_logo_url` | String |  |
| `winner_aggregate_team_confederation_type` | String |  |
| `winner_aggregate_team_country_code` | String |  |
| `winner_aggregate_team_id` | String |  |
| `winner_aggregate_team_id_provider` | String |  |
| `winner_aggregate_team_international_name` | String |  |
| `winner_aggregate_team_is_place_holder` | Boolean |  |
| `winner_aggregate_team_logo_url` | String |  |
| `winner_aggregate_team_medium_logo_url` | String |  |
| `winner_aggregate_team_organization_id` | String |  |
| `winner_aggregate_team_team_code` | String |  |
| `winner_aggregate_team_team_type_detail` | String |  |
| `winner_aggregate_team_translations_country_name_en` | String |  |
| `winner_aggregate_team_translations_country_name_fr` | String |  |
| `winner_aggregate_team_translations_country_name_de` | String |  |
| `winner_aggregate_team_translations_country_name_es` | String |  |
| `winner_aggregate_team_translations_country_name_pt` | String |  |
| `winner_aggregate_team_translations_country_name_it` | String |  |
| `winner_aggregate_team_translations_country_name_ru` | String |  |
| `winner_aggregate_team_translations_country_name_zh` | String |  |
| `winner_aggregate_team_translations_country_name_ar` | String |  |
| `winner_aggregate_team_translations_display_name_en` | String |  |
| `winner_aggregate_team_translations_display_name_fr` | String |  |
| `winner_aggregate_team_translations_display_name_de` | String |  |
| `winner_aggregate_team_translations_display_name_es` | String |  |
| `winner_aggregate_team_translations_display_name_pt` | String |  |
| `winner_aggregate_team_translations_display_name_it` | String |  |
| `winner_aggregate_team_translations_display_name_ru` | String |  |
| `winner_aggregate_team_translations_display_name_zh` | String |  |
| `winner_aggregate_team_translations_display_name_ar` | String |  |
| `winner_aggregate_team_translations_display_official_name_en` | String |  |
| `winner_aggregate_team_translations_display_official_name_fr` | String |  |
| `winner_aggregate_team_translations_display_official_name_de` | String |  |
| `winner_aggregate_team_translations_display_official_name_es` | String |  |
| `winner_aggregate_team_translations_display_official_name_pt` | String |  |
| `winner_aggregate_team_translations_display_official_name_it` | String |  |
| `winner_aggregate_team_translations_display_official_name_ru` | String |  |
| `winner_aggregate_team_translations_display_official_name_zh` | String |  |
| `winner_aggregate_team_translations_display_official_name_ar` | String |  |
| `winner_aggregate_team_translations_display_team_code_en` | String |  |
| `winner_aggregate_team_translations_display_team_code_fr` | String |  |
| `winner_aggregate_team_translations_display_team_code_de` | String |  |
| `winner_aggregate_team_translations_display_team_code_es` | String |  |
| `winner_aggregate_team_translations_display_team_code_pt` | String |  |
| `winner_aggregate_team_translations_display_team_code_it` | String |  |
| `winner_aggregate_team_translations_display_team_code_ru` | String |  |
| `winner_aggregate_team_translations_display_team_code_zh` | String |  |
| `winner_aggregate_team_translations_display_team_code_ar` | String |  |
| `winner_aggregate_team_translations_short_name_en` | String |  |
| `winner_aggregate_team_translations_short_name_fr` | String |  |
| `winner_aggregate_team_translations_short_name_de` | String |  |
| `winner_aggregate_team_translations_short_name_es` | String |  |
| `winner_aggregate_team_translations_short_name_pt` | String |  |
| `winner_aggregate_team_translations_short_name_it` | String |  |
| `winner_aggregate_team_translations_short_name_ru` | String |  |
| `winner_aggregate_team_translations_short_name_zh` | String |  |
| `winner_aggregate_team_translations_short_name_ar` | String |  |
| `winner_aggregate_team_type_is_national` | Boolean |  |
| `winner_aggregate_team_type_team` | String |  |
| `winner_aggregate_translations_reason_text_abbr_ru` | String |  |
| `winner_aggregate_translations_reason_text_abbr_de` | String |  |
| `winner_aggregate_translations_reason_text_abbr_pt` | String |  |
| `winner_aggregate_translations_reason_text_abbr_es` | String |  |
| `winner_aggregate_translations_reason_text_abbr_it` | String |  |
| `winner_aggregate_translations_reason_text_abbr_en` | String |  |
| `winner_aggregate_translations_reason_text_abbr_zh` | String |  |
| `winner_aggregate_translations_reason_text_abbr_ar` | String |  |
| `winner_aggregate_translations_reason_text_abbr_fr` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_matches-example}

```python
uefa_matches(competition_id='1', limit='3', offset='0', season_year='2026')
```

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
| `age` | String | Player's age in years at capture (string), where the source lists it. |
| `birth_date` | String | Player's date of birth (YYYY-MM-DD), where the source lists it. |
| `club_jersey_number` | String |  |
| `club_shirt_name` | String |  |
| `country_code` | String | ISO country code. |
| `detailed_field_position` | String |  |
| `gender` | String | Player's gender as the source lists it (e.g. 'MALE'). |
| `height` | Int64 | Player's height as the source encodes it (PFF uses feet and inches without a separator, 602 = 6'02"; others use inches or centimetres). |
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `image_url` | String | URL of the player's headshot on the source CDN. |
| `international_name` | String |  |
| `national_jersey_number` | String |  |
| `national_shirt_name` | String |  |
| `national_team_id` | String |  |
| `weight` | Int64 | Player's weight as the source lists it (pounds for US sources). |
| `translations_country_name_en` | String |  |
| `translations_country_name_fr` | String |  |
| `translations_country_name_de` | String |  |
| `translations_country_name_es` | String |  |
| `translations_country_name_pt` | String |  |
| `translations_country_name_it` | String |  |
| `translations_country_name_ru` | String |  |
| `translations_country_name_zh` | String |  |
| `translations_country_name_ar` | String |  |
| `translations_first_name_en` | String |  |
| `translations_first_name_fr` | String |  |
| `translations_first_name_de` | String |  |
| `translations_first_name_es` | String |  |
| `translations_first_name_pt` | String |  |
| `translations_first_name_it` | String |  |
| `translations_first_name_ru` | String |  |
| `translations_first_name_zh` | String |  |
| `translations_first_name_ar` | String |  |
| `translations_last_name_en` | String |  |
| `translations_last_name_fr` | String |  |
| `translations_last_name_de` | String |  |
| `translations_last_name_es` | String |  |
| `translations_last_name_pt` | String |  |
| `translations_last_name_it` | String |  |
| `translations_last_name_ru` | String |  |
| `translations_last_name_zh` | String |  |
| `translations_last_name_ar` | String |  |
| `translations_name_en` | String |  |
| `translations_name_fr` | String |  |
| `translations_name_de` | String |  |
| `translations_name_es` | String |  |
| `translations_name_pt` | String |  |
| `translations_name_it` | String |  |
| `translations_name_ru` | String |  |
| `translations_name_zh` | String |  |
| `translations_name_ar` | String |  |
| `translations_short_name_en` | String |  |
| `translations_short_name_fr` | String |  |
| `translations_short_name_de` | String |  |
| `translations_short_name_es` | String |  |
| `translations_short_name_pt` | String |  |
| `translations_short_name_it` | String |  |
| `translations_short_name_ru` | String |  |
| `translations_short_name_zh` | String |  |
| `translations_short_name_ar` | String |  |
| `club_id` | String | Provider identifier for the entity (Utf8 join key). |
| `field_position` | String | Ball spot expressed on Yahoo's 0-100 field scale, measured toward the offense's target goal line. |
| `national_field_position` | String |  |
| `translations_field_position_en` | String |  |
| `translations_field_position_fr` | String |  |
| `translations_field_position_de` | String |  |
| `translations_field_position_es` | String |  |
| `translations_field_position_pt` | String |  |
| `translations_field_position_it` | String |  |
| `translations_field_position_ru` | String |  |
| `translations_field_position_zh` | String |  |
| `translations_field_position_ar` | String |  |
| `translations_national_field_position_en` | String |  |
| `translations_national_field_position_fr` | String |  |
| `translations_national_field_position_de` | String |  |
| `translations_national_field_position_es` | String |  |
| `translations_national_field_position_pt` | String |  |
| `translations_national_field_position_it` | String |  |
| `translations_national_field_position_ru` | String |  |
| `translations_national_field_position_zh` | String |  |
| `translations_national_field_position_ar` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_players-example}

```python
uefa_players(competition_id='1', limit='3', offset='0', season_year='2026')
```

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
| `items` | String |  |
| `qualification_labels` | String |  |
| `status` | String | UEFA standings status (e.g. 'FINISHED', 'OFFICIAL'). |
| `group_competition_id` | String |  |
| `group_id` | String | UEFA group id the standings row belongs to; null for a league-phase table. |
| `group_meta_data_group_name` | String |  |
| `group_meta_data_group_short_name` | String |  |
| `group_order` | Int64 |  |
| `group_phase` | String |  |
| `group_round_id` | String |  |
| `group_season_year` | String |  |
| `group_teams` | String |  |
| `group_teams_qualified_number` | Int64 |  |
| `group_translations_name_en` | String |  |
| `group_translations_name_fr` | String |  |
| `group_translations_name_de` | String |  |
| `group_translations_name_es` | String |  |
| `group_translations_name_pt` | String |  |
| `group_translations_name_it` | String |  |
| `group_translations_name_ru` | String |  |
| `group_translations_name_zh` | String |  |
| `group_translations_name_ar` | String |  |
| `group_translations_short_name_en` | String |  |
| `group_translations_short_name_fr` | String |  |
| `group_translations_short_name_de` | String |  |
| `group_translations_short_name_es` | String |  |
| `group_translations_short_name_pt` | String |  |
| `group_translations_short_name_it` | String |  |
| `group_translations_short_name_ru` | String |  |
| `group_translations_short_name_zh` | String |  |
| `group_translations_short_name_ar` | String |  |
| `group_type` | String |  |
| `round_active` | Boolean |  |
| `round_bench_gk_count` | Int64 |  |
| `round_bench_players_count` | Int64 |  |
| `round_bench_staff_count` | Int64 |  |
| `round_coefficient_winner_bonus` | Float64 |  |
| `round_competition_id` | String |  |
| `round_date_from` | String |  |
| `round_date_to` | String |  |
| `round_field_players_count` | Int64 |  |
| `round_group_count` | Int64 |  |
| `round_id` | String | Composite id of the round. |
| `round_meta_data_name` | String |  |
| `round_meta_data_type` | String |  |
| `round_mode` | String |  |
| `round_mode_detail` | String |  |
| `round_order_in_competition` | Int64 |  |
| `round_phase` | String |  |
| `round_season_year` | String |  |
| `round_secondary_type` | String |  |
| `round_stadium_name_type` | String |  |
| `round_standings_ranking_formula_id` | String |  |
| `round_status` | String |  |
| `round_substitution_count` | Int64 |  |
| `round_team_count` | Int64 |  |
| `round_teams` | String |  |
| `round_translations_abbreviation_en` | String |  |
| `round_translations_abbreviation_fr` | String |  |
| `round_translations_abbreviation_de` | String |  |
| `round_translations_abbreviation_es` | String |  |
| `round_translations_abbreviation_pt` | String |  |
| `round_translations_abbreviation_it` | String |  |
| `round_translations_abbreviation_ru` | String |  |
| `round_translations_abbreviation_zh` | String |  |
| `round_translations_abbreviation_ar` | String |  |
| `round_translations_name_en` | String |  |
| `round_translations_name_fr` | String |  |
| `round_translations_name_de` | String |  |
| `round_translations_name_es` | String |  |
| `round_translations_name_pt` | String |  |
| `round_translations_name_it` | String |  |
| `round_translations_name_ru` | String |  |
| `round_translations_name_zh` | String |  |
| `round_translations_name_ar` | String |  |
| `round_translations_short_name_en` | String |  |
| `round_translations_short_name_fr` | String |  |
| `round_translations_short_name_de` | String |  |
| `round_translations_short_name_es` | String |  |
| `round_translations_short_name_pt` | String |  |
| `round_translations_short_name_it` | String |  |
| `round_translations_short_name_ru` | String |  |
| `round_translations_short_name_zh` | String |  |
| `round_translations_short_name_ar` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_standings-example}

```python
uefa_standings(competition_id='1', season_year='2026')
```

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
| `id_provider` | String |  |
| `statistics` | String | The team's full statistic list for the match, JSON-encoded (one {name, value} object per statistic). |
| `team_id` | String | Club: provider identifier for the entity (Utf8 join key). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_team_statistics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_team_statistics-example}

```python
uefa_team_statistics(match_id='2047742')
```

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
| `association_id` | String |  |
| `association_logo_url` | String |  |
| `big_logo_url` | String |  |
| `confederation_type` | String |  |
| `country_code` | String | ISO country code. |
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `id_provider` | String |  |
| `international_name` | String |  |
| `is_place_holder` | Boolean |  |
| `logo_url` | String | URL of the team's logo on the source CDN. |
| `medium_logo_url` | String |  |
| `organization_id` | String |  |
| `team_code` | String | Team's three-letter UEFA code (e.g. 'PSG'). |
| `team_type_detail` | String |  |
| `type_is_national` | Boolean |  |
| `type_team` | String |  |
| `translations_country_name_en` | String |  |
| `translations_country_name_fr` | String |  |
| `translations_country_name_de` | String |  |
| `translations_country_name_es` | String |  |
| `translations_country_name_pt` | String |  |
| `translations_country_name_it` | String |  |
| `translations_country_name_ru` | String |  |
| `translations_country_name_zh` | String |  |
| `translations_country_name_ar` | String |  |
| `translations_display_name_en` | String |  |
| `translations_display_name_fr` | String |  |
| `translations_display_name_de` | String |  |
| `translations_display_name_es` | String |  |
| `translations_display_name_pt` | String |  |
| `translations_display_name_it` | String |  |
| `translations_display_name_ru` | String |  |
| `translations_display_name_zh` | String |  |
| `translations_display_name_ar` | String |  |
| `translations_display_official_name_en` | String |  |
| `translations_display_official_name_fr` | String |  |
| `translations_display_official_name_de` | String |  |
| `translations_display_official_name_es` | String |  |
| `translations_display_official_name_pt` | String |  |
| `translations_display_official_name_it` | String |  |
| `translations_display_official_name_ru` | String |  |
| `translations_display_official_name_zh` | String |  |
| `translations_display_official_name_ar` | String |  |
| `translations_display_team_code_en` | String |  |
| `translations_display_team_code_fr` | String |  |
| `translations_display_team_code_de` | String |  |
| `translations_display_team_code_es` | String |  |
| `translations_display_team_code_pt` | String |  |
| `translations_display_team_code_it` | String |  |
| `translations_display_team_code_ru` | String |  |
| `translations_display_team_code_zh` | String |  |
| `translations_display_team_code_ar` | String |  |
| `translations_short_name_en` | String |  |
| `translations_short_name_fr` | String |  |
| `translations_short_name_de` | String |  |
| `translations_short_name_es` | String |  |
| `translations_short_name_pt` | String |  |
| `translations_short_name_it` | String |  |
| `translations_short_name_ru` | String |  |
| `translations_short_name_zh` | String |  |
| `translations_short_name_ar` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#uefa_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. The UEFA host answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. The UEFA host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The fetch failed (a non-2xx answer or a connection failure after retries, e.g. a 403 from the edge, 429 or 5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#uefa_teams-example}

```python
uefa_teams(competition_id='1', limit='3', offset='0', season_year='2026')
```
