# Soccer (all) — FotMob data API (fotmob.com, unofficial)

> Soccer (all) — FotMob data API (fotmob.com, unofficial) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.soccer` — 14 endpoints.

## fotmob_all_leagues

Every league FotMob covers, grouped as popular / international / by country.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/allLeagues`

**Valid URL:** [https://www.fotmob.com/api/data/allLeagues](https://www.fotmob.com/api/data/allLeagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#fotmob_all_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `popular` | String |  |
| `international` | String |  |
| `countries` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_all_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_all_leagues-example}

```python
fotmob_all_leagues()
```

## fotmob_audio_matches

Matches with audio commentary available, with their languages.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/audio-matches`

**Valid URL:** [https://www.fotmob.com/api/data/audio-matches](https://www.fotmob.com/api/data/audio-matches)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#fotmob_audio_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `langs` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_audio_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_audio_matches-example}

```python
fotmob_audio_matches()
```

## fotmob_leagues

League page: details, tabs, current table, fixtures, stats, transfers (one wide row).

**Endpoint URL:** `GET https://www.fotmob.com/api/data/leagues`

**Valid URL:** [https://www.fotmob.com/api/data/leagues?id=47&tab=overview&type=league&timeZone=UTC](https://www.fotmob.com/api/data/leagues?id=47&tab=overview&type=league&timeZone=UTC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League, team or player id (numeric string). |
| `tab` | `tab` |  |  | `Y` | Page tab, e.g. overview, table, fixtures, stats. |
| `type` | `type` |  |  | `Y` | league or team. |
| `timeZone` | `time_zone` |  |  | `Y` | IANA zone for kickoff times. |

### Returns {#fotmob_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tabs` | String |  |
| `all_available_seasons` | String |  |
| `seostr` | String |  |
| `qa_data` | String |  |
| `table` | String |  |
| `playoff` | String | Whether the league entry is a playoff competition; null when Fotmob ships no flag. |
| `seasons` | String | Nested list (stringified) of the seasons the source publishes for the league. |
| `details_id` | String |  |
| `details_type` | String |  |
| `details_name` | String |  |
| `details_selected_season` | String |  |
| `details_latest_season` | String |  |
| `details_short_name` | String |  |
| `details_country` | String |  |
| `details_gender` | String |  |
| `details_national_team_id` | String |  |
| `details_faq_jsonld` | String |  |
| `details_breadcrumb_jsonld` | String |  |
| `details_can_sync_calendar` | Boolean |  |
| `details_has_fixture_difficulty` | Boolean |  |
| `details_league_color` | String |  |
| `details_data_provider` | String |  |
| `details_seopath` | String |  |
| `transfers_type` | String |  |
| `transfers_data` | String |  |
| `overview_season` | String |  |
| `overview_selected_season` | String |  |
| `overview_table` | String |  |
| `overview_top_players` | String |  |
| `overview_has_totw` | Boolean |  |
| `overview_league_overview_matches` | String |  |
| `overview_has_ongoing_match` | Boolean |  |
| `overview_shot_map` | String |  |
| `overview_matches` | String |  |
| `stats_players` | String |  |
| `stats_teams` | String |  |
| `stats_season_stat_links` | String |  |
| `stats_seasons_with_links` | String |  |
| `fixtures_first_unplayed_match` | String |  |
| `fixtures_all_matches` | String |  |
| `fixtures_has_ongoing_match` | Boolean |  |
| `fixtures_fixture_info` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_leagues-example}

```python
fotmob_leagues(id='47', tab='overview', time_zone='UTC', type='league')
```

## fotmob_match_details

Match page: general info, header, lineups, events, stats (one wide row).

**Endpoint URL:** `GET https://www.fotmob.com/api/data/matchDetails`

**Valid URL:** [https://www.fotmob.com/api/data/matchDetails?matchId=4813647](https://www.fotmob.com/api/data/matchDetails?matchId=4813647)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `matchId` | `match_id` |  |  | `Y` | Required. Match id (from /matches). |

### Returns {#fotmob_match_details-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nav` | String |  |
| `ongoing` | Boolean | Whether the match was in progress when the payload was fetched. |
| `has_pending_var` | Boolean |  |
| `general_match_id` | String |  |
| `general_match_name` | String |  |
| `general_match_round` | String |  |
| `general_team_colors` | String |  |
| `general_league_id` | String |  |
| `general_league_name` | String |  |
| `general_league_round_name` | String |  |
| `general_parent_league_id` | String |  |
| `general_country_code` | String |  |
| `general_home_team` | String |  |
| `general_away_team` | String |  |
| `general_coverage_level` | String |  |
| `general_match_time_utc` | String |  |
| `general_match_time_utc_date` | String |  |
| `general_started` | Boolean |  |
| `general_finished` | Boolean |  |
| `general_gender` | String |  |
| `header_teams` | String |  |
| `header_status` | String |  |
| `header_events` | String |  |
| `content_match_facts` | String |  |
| `content_liveticker` | String |  |
| `content_superlive` | String |  |
| `content_buzz` | String |  |
| `content_stats` | String |  |
| `content_player_stats` | String |  |
| `content_shotmap` | String |  |
| `content_weather` | String |  |
| `content_lineup` | String |  |
| `content_has_playoff` | Boolean |  |
| `content_table` | String |  |
| `content_h2h` | String |  |
| `content_momentum` | String |  |
| `content_highlight_stories` | String |  |
| `content_heatmap_url` | String |  |
| `seo_path` | String |  |
| `seo_event_jsonld` | String |  |
| `seo_breadcrumb_jsonld` | String |  |
| `seo_faq_jsonld` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_match_details-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_match_details-example}

```python
fotmob_match_details(match_id='4813647')
```

## fotmob_matches

All matches on a date, one row per league with that league's matches nested.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/matches`

**Valid URL:** [https://www.fotmob.com/api/data/matches?date=20260301&timezone=UTC](https://www.fotmob.com/api/data/matches?date=20260301&timezone=UTC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | YYYYMMDD. |
| `timezone` | `timezone` |  |  | `Y` | IANA zone. |

### Returns {#fotmob_matches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `ccode` | String |  |
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `primary_id` | String |  |
| `name` | String | Display name. |
| `matches` | String |  |
| `internal_rank` | Int64 |  |
| `simple_league` | Boolean |  |
| `local_rank` | Int64 |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_matches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_matches-example}

```python
fotmob_matches(date='20260301', timezone='UTC')
```

## fotmob_player_data

Player page: bio, primary team, career history, recent matches, stats (one wide row).

**Endpoint URL:** `GET https://www.fotmob.com/api/data/playerData`

**Valid URL:** [https://www.fotmob.com/api/data/playerData?id=1077894](https://www.fotmob.com/api/data/playerData?id=1077894)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League, team or player id (numeric string). |

### Returns {#fotmob_player_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `name` | String | Display name. |
| `is_coach` | Boolean |  |
| `is_captain` | Boolean |  |
| `gender` | String | Player's gender as Fotmob ships it (e.g. 'male'). |
| `injury_information` | String |  |
| `international_duty` | String |  |
| `player_information` | String |  |
| `recent_matches` | String |  |
| `matches_url` | String |  |
| `coach_stats` | String |  |
| `stat_seasons` | String |  |
| `status` | String | Player's status as Fotmob ships it (e.g. 'active'). |
| `data_provider` | String |  |
| `birth_date_utc_time` | String |  |
| `birth_date_timezone` | String |  |
| `contract_end_utc_time` | String |  |
| `contract_end_timezone` | String |  |
| `primary_team_team_id` | String |  |
| `primary_team_team_name` | String |  |
| `primary_team_on_loan` | Boolean |  |
| `primary_team_team_colors` | String |  |
| `position_description_positions` | String |  |
| `position_description_primary_position` | String |  |
| `position_description_non_primary_positions` | String |  |
| `main_league_league_id` | String |  |
| `main_league_league_name` | String |  |
| `main_league_season` | String |  |
| `main_league_stats` | String |  |
| `trophies_player_trophies` | String |  |
| `trophies_coach_trophies` | String |  |
| `match_filters_teams` | String |  |
| `match_filters_leagues` | String |  |
| `career_history_show_footnote` | Boolean |  |
| `career_history_career_items` | String |  |
| `career_history_full_career` | Boolean |  |
| `traits_key` | String |  |
| `traits_title` | String |  |
| `traits_items` | String |  |
| `meta_seopath` | String |  |
| `meta_pageurl` | String |  |
| `meta_faq_jsonld` | String |  |
| `meta_person_jsonld` | String |  |
| `meta_breadcrumb_jsonld` | String |  |
| `first_season_stats_section_order` | String |  |
| `first_season_stats_shotmap` | String |  |
| `first_season_stats_stats_section` | String |  |
| `first_season_stats_top_stat_card` | String |  |
| `first_season_stats_heatmap` | String |  |
| `first_season_stats_keeper_shotmap` | String |  |
| `first_season_stats_shotmap_team_colors` | String |  |
| `market_values_values` | String |  |
| `related_links_data_teammates` | String |  |
| `related_links_data_mens_national_team` | String |  |
| `related_links_data_womens_national_team` | String |  |
| `next_match_match_id` | String |  |
| `next_match_home_id` | String |  |
| `next_match_away_id` | String |  |
| `next_match_home_name` | String |  |
| `next_match_away_name` | String |  |
| `next_match_match_date` | String |  |
| `next_match_status_id` | String |  |
| `next_match_league_id` | String |  |
| `next_match_parent_league_id` | String |  |
| `next_match_league_name` | String |  |
| `next_match_status` | String |  |
| `next_match_stadium` | String |  |
| `next_match_match_url` | String |  |
| `initial_matches_page_matches` | String |  |
| `initial_matches_page_previous` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_player_data-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_player_data-example}

```python
fotmob_player_data(id='1077894')
```

## fotmob_search_suggest

Search suggestions for a term, one row per result group (players, teams, leagues).

**Endpoint URL:** `GET https://www.fotmob.com/api/data/search/suggest`

**Valid URL:** [https://www.fotmob.com/api/data/search/suggest?term=haaland&lang=en](https://www.fotmob.com/api/data/search/suggest?term=haaland&lang=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `term` | `term` |  |  | `Y` | Required. Search text. |
| `lang` | `lang` |  |  | `Y` | Language code. |

### Returns {#fotmob_search_suggest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `suggestions` | String |  |
| `title_key` | String |  |
| `title_value` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_search_suggest-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_search_suggest-example}

```python
fotmob_search_suggest(lang='en', term='haaland')
```

## fotmob_table

A data.fotmob.com league table file: legend, filters and the all/home/away/xg tables.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/table`

**Valid URL:** [https://www.fotmob.com/api/data/table?url=http%3A%2F%2Fdata.fotmob.com%2Ftables.ext.47.fot.gz](https://www.fotmob.com/api/data/table?url=http%3A%2F%2Fdata.fotmob.com%2Ftables.ext.47.fot.gz)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `url` | `url` |  |  | `Y` | Required. The data.fotmob.com table file to return, e.g. http://data.fotmob.com/tables.ext.47.fot.gz. |

### Returns {#fotmob_table-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `ccode` | String |  |
| `league_id` | String | Fotmob league id of the table, as a string. |
| `page_url` | String |  |
| `league_name` | String | Fotmob league name of the table (e.g. 'Premier League'). |
| `legend` | String |  |
| `ongoing` | String | Stringified list of the team's ongoing matches at capture; empty when none. |
| `table_filter_types` | String |  |
| `composite` | Boolean |  |
| `table_all` | String |  |
| `table_home` | String |  |
| `table_away` | String |  |
| `table_xg` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_table-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_table-example}

```python
fotmob_table(url='http://data.fotmob.com/tables.ext.47.fot.gz')
```

## fotmob_teams

Team page: details, squad, table, fixtures, stats, transfers, history (one wide row).

**Endpoint URL:** `GET https://www.fotmob.com/api/data/teams`

**Valid URL:** [https://www.fotmob.com/api/data/teams?id=8650&tab=overview&type=team&timeZone=UTC](https://www.fotmob.com/api/data/teams?id=8650&tab=overview&type=team&timeZone=UTC)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League, team or player id (numeric string). |
| `tab` | `tab` |  |  | `Y` | Page tab, e.g. overview, table, fixtures, stats. |
| `type` | `type` |  |  | `Y` | league or team. |
| `timeZone` | `time_zone` |  |  | `Y` | IANA zone for kickoff times. |

### Returns {#fotmob_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `tabs` | String |  |
| `all_available_seasons` | String |  |
| `seostr` | String |  |
| `qa_data` | String |  |
| `table` | String |  |
| `details_id` | String |  |
| `details_type` | String |  |
| `details_name` | String |  |
| `details_latest_season` | String |  |
| `details_short_name` | String |  |
| `details_country` | String |  |
| `details_gender` | String |  |
| `details_national_team_id` | String |  |
| `details_faq_jsonld` | String |  |
| `details_sports_team_jsonld` | String |  |
| `details_breadcrumb_jsonld` | String |  |
| `details_can_sync_calendar` | Boolean |  |
| `details_has_fixture_difficulty` | Boolean |  |
| `details_primary_league_id` | String |  |
| `details_primary_league_name` | String |  |
| `details_team_color` | String |  |
| `details_seopath` | String |  |
| `transfers_type` | String |  |
| `transfers_data` | String |  |
| `transfers_all_transfers` | String |  |
| `transfers_all_rumours` | String |  |
| `transfers_max_fee` | Int64 |  |
| `transfers_our_team_id` | String |  |
| `overview_season` | String |  |
| `overview_selected_season` | String |  |
| `overview_table` | String |  |
| `overview_top_players` | String |  |
| `overview_venue` | String |  |
| `overview_coach_history` | String |  |
| `overview_overview_fixtures` | String |  |
| `overview_next_match` | String |  |
| `overview_last_match` | String |  |
| `overview_team_form` | String |  |
| `overview_has_ongoing_match` | Boolean |  |
| `overview_previous_fixtures_url` | String |  |
| `overview_team_colors` | String |  |
| `overview_last_lineup_stats` | String |  |
| `overview_news_summary` | String |  |
| `overview_featured_article` | String |  |
| `overview_fixture_difficulty` | String |  |
| `overview_squad` | String |  |
| `overview_transfers` | String |  |
| `stats_team_id` | String |  |
| `stats_primary_league_id` | String |  |
| `stats_primary_season_id` | String |  |
| `stats_players` | String |  |
| `stats_teams` | String |  |
| `stats_tournament_id` | String |  |
| `stats_tournament_seasons` | String |  |
| `fixtures_all_fixtures` | String |  |
| `fixtures_primary_tournament_id` | String |  |
| `fixtures_previous_fixtures_url` | String |  |
| `fixtures_has_ongoing_match` | Boolean |  |
| `squad_squad` | String |  |
| `squad_is_national_team` | Boolean |  |
| `history_trophy_list` | String |  |
| `history_historical_table_data` | String |  |
| `history_team_color_map` | String |  |
| `history_tables` | String |  |
| `history_coach_history` | String |  |
| `history_team_colors` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_teams-example}

```python
fotmob_teams(id='8650', tab='overview', time_zone='UTC', type='team')
```

## fotmob_tlnews

News timeline for a league or team, one row per story.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/tlnews`

**Valid URL:** [https://www.fotmob.com/api/data/tlnews?id=47&type=league&startIndex=0](https://www.fotmob.com/api/data/tlnews?id=47&type=league&startIndex=0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  |  | `Y` | Required. League, team or player id (numeric string). |
| `type` | `type` |  |  | `Y` | Required. league or team. |
| `startIndex` | `start_index` |  |  | `Y` | Required. Paging offset for the news timeline. |

### Returns {#fotmob_tlnews-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `image_url` | String | URL of the article's lead image. |
| `title` | String | Display title. |
| `gmt_time` | String |  |
| `source_str` | String |  |
| `source_icon_url` | String |  |
| `language` | String | Language code of the article (e.g. 'en'). |
| `page_url` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_tlnews-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_tlnews-example}

```python
fotmob_tlnews(id='47', start_index='0', type='league')
```

## fotmob_top_transfers

Top transfers across FotMob, one row per transfer.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/top-transfers`

**Valid URL:** [https://www.fotmob.com/api/data/top-transfers](https://www.fotmob.com/api/data/top-transfers)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#fotmob_top_transfers-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `name` | String | Display name. |
| `player_id` | String | Fotmob player id of the transferred player, as a string. |
| `transfer_date` | String | Timestamp the transfer was recorded (ISO-8601 UTC). |
| `transfer_text` | String |  |
| `from_club` | String |  |
| `from_club_full_name` | String |  |
| `from_club_id` | String |  |
| `to_club` | String |  |
| `to_club_full_name` | String |  |
| `to_club_id` | String |  |
| `amount_euro_estimated` | Int64 |  |
| `contract_extension` | Boolean |  |
| `on_loan` | Boolean |  |
| `from_date` | String |  |
| `to_date` | String |  |
| `market_value` | Int64 |  |
| `position_label` | String |  |
| `position_key` | String |  |
| `from_club_colors_light_mode` | String |  |
| `from_club_colors_dark_mode` | String |  |
| `to_club_colors_light_mode` | String |  |
| `to_club_colors_dark_mode` | String |  |
| `fee_fee_text` | String |  |
| `fee_localized_fee_text` | String |  |
| `transfer_type_text` | String |  |
| `transfer_type_localization_key` | String |  |
| `fee_value` | Int64 |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_top_transfers-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_top_transfers-example}

```python
fotmob_top_transfers()
```

## fotmob_totw_rounds

Team-of-the-week rounds for a league season, one row per round.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/team-of-the-week/rounds`

**Valid URL:** [https://www.fotmob.com/api/data/team-of-the-week/rounds?leagueId=47&season=2025%2F2026](https://www.fotmob.com/api/data/team-of-the-week/rounds?leagueId=47&season=2025%2F2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `leagueId` | `league_id` |  |  | `Y` | Required. League id. |
| `season` | `season` |  |  | `Y` | e.g. 2025/2026. |

### Returns {#fotmob_totw_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `localized_key` | String |  |
| `round_id` | String | Composite id of the round. |
| `link` | String | Fotmob API URL of the round's team-of-the-week payload. |
| `is_completed` | Boolean |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_totw_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_totw_rounds-example}

```python
fotmob_totw_rounds(league_id='47', season='2025/2026')
```

## fotmob_trending_news

Trending news stories (host-root route), one row per story.

**Endpoint URL:** `GET https://www.fotmob.com/api/trendingnews`

**Valid URL:** [https://www.fotmob.com/api/trendingnews?lang=en](https://www.fotmob.com/api/trendingnews?lang=en)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `lang` | `lang` |  |  | `Y` | Language code. |

### Returns {#fotmob_trending_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `image_url` | String | URL of the article's lead image. |
| `title` | String | Display title. |
| `gmt_time` | String |  |
| `source_str` | String |  |
| `source_icon_url` | String |  |
| `page_url` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_trending_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_trending_news-example}

```python
fotmob_trending_news(lang='en')
```

## fotmob_tvlistings

TV listings for a country, one row per broadcast with the match id in `id`.

**Endpoint URL:** `GET https://www.fotmob.com/api/data/tvlistings`

**Valid URL:** [https://www.fotmob.com/api/data/tvlistings?countryCode=GB](https://www.fotmob.com/api/data/tvlistings?countryCode=GB)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `countryCode` | `country_code` |  |  | `Y` | Required. ISO country for TV listings. |

### Returns {#fotmob_tvlistings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | String | Provider identifier for the entity (Utf8 join key). |
| `start_time` | String | Broadcast start as an ASP.NET-style '/Date(<ms>)/' string (milliseconds since the Unix epoch). |
| `end_time` | String | Broadcast end as an ASP.NET-style '/Date(<ms>)/' string; a large negative value is Fotmob's 'unset' sentinel. |
| `qualifiers` | String | Stringified list of listing qualifiers (e.g. ['Live']). |
| `station_id` | String |  |
| `match_id` | String |  |
| `league_id` | String | Fotmob league id of the listing's competition, as a string. |
| `parent_league_id` | String |  |
| `bet365_match_id` | String |  |
| `external_id` | String | Provider-side identifier for the media item, matching the play id it accompanies. |
| `affiliates` | String |  |
| `tags` | String | Content tags, JSON-encoded. |
| `station_call_sign` | String |  |
| `station_station_id` | String |  |
| `station_affiliate_id` | String |  |
| `station_affiliate_call_sign` | String |  |
| `station_name` | String |  |
| `station_type` | String |  |
| `station_blocked_country_codes` | String |  |
| `program_root_id` | String |  |
| `program_teams` | String |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Raises {#fotmob_tvlistings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. FotMob answered HTTP 404 (route withdrawn). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fotmob_tvlistings-example}

```python
fotmob_tvlistings(country_code='GB')
```
