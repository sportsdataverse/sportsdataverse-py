# SOCCER — FotMob data API (fotmob.com, unofficial)

> SOCCER — FotMob data API (fotmob.com, unofficial) — endpoint reference in sdv-py, the SportsDataverse Python package.

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
| `popular` | character |  |
| `international` | character |  |
| `countries` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_all_leagues-example}

```python
fotmob_all_leagues()
```

_Last validated n/a._

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
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `langs` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_audio_matches-example}

```python
fotmob_audio_matches()
```

_Last validated n/a._

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
| `tabs` | character |  |
| `all_available_seasons` | character |  |
| `seostr` | character |  |
| `qa_data` | character |  |
| `table` | character |  |
| `playoff` | character | Whether the league entry is a playoff competition; null when Fotmob ships no flag. |
| `seasons` | character | Nested list (stringified) of the seasons the source publishes for the league. |
| `details_id` | character |  |
| `details_type` | character |  |
| `details_name` | character |  |
| `details_selected_season` | character |  |
| `details_latest_season` | character |  |
| `details_short_name` | character |  |
| `details_country` | character |  |
| `details_gender` | character |  |
| `details_national_team_id` | character |  |
| `details_faq_jsonld` | character |  |
| `details_breadcrumb_jsonld` | character |  |
| `details_can_sync_calendar` | logical |  |
| `details_has_fixture_difficulty` | logical |  |
| `details_league_color` | character |  |
| `details_data_provider` | character |  |
| `details_seopath` | character |  |
| `transfers_type` | character |  |
| `transfers_data` | character |  |
| `overview_season` | character |  |
| `overview_selected_season` | character |  |
| `overview_table` | character |  |
| `overview_top_players` | character |  |
| `overview_has_totw` | logical |  |
| `overview_league_overview_matches` | character |  |
| `overview_has_ongoing_match` | logical |  |
| `overview_shot_map` | character |  |
| `overview_matches` | character |  |
| `stats_players` | character |  |
| `stats_teams` | character |  |
| `stats_season_stat_links` | character |  |
| `stats_seasons_with_links` | character |  |
| `fixtures_first_unplayed_match` | character |  |
| `fixtures_all_matches` | character |  |
| `fixtures_has_ongoing_match` | logical |  |
| `fixtures_fixture_info` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_leagues-example}

```python
fotmob_leagues(id='47', tab='overview', time_zone='UTC', type='league')
```

_Last validated n/a._

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
| `nav` | character |  |
| `ongoing` | logical | Whether the match was in progress when the payload was fetched. |
| `has_pending_var` | logical |  |
| `general_match_id` | character |  |
| `general_match_name` | character |  |
| `general_match_round` | character |  |
| `general_team_colors` | character |  |
| `general_league_id` | character |  |
| `general_league_name` | character |  |
| `general_league_round_name` | character |  |
| `general_parent_league_id` | character |  |
| `general_country_code` | character |  |
| `general_home_team` | character |  |
| `general_away_team` | character |  |
| `general_coverage_level` | character |  |
| `general_match_time_utc` | character |  |
| `general_match_time_utc_date` | character |  |
| `general_started` | logical |  |
| `general_finished` | logical |  |
| `general_gender` | character |  |
| `header_teams` | character |  |
| `header_status` | character |  |
| `header_events` | character |  |
| `content_match_facts` | character |  |
| `content_liveticker` | character |  |
| `content_superlive` | character |  |
| `content_buzz` | character |  |
| `content_stats` | character |  |
| `content_player_stats` | character |  |
| `content_shotmap` | character |  |
| `content_weather` | character |  |
| `content_lineup` | character |  |
| `content_has_playoff` | logical |  |
| `content_table` | character |  |
| `content_h2h` | character |  |
| `content_momentum` | character |  |
| `content_highlight_stories` | character |  |
| `content_heatmap_url` | character |  |
| `seo_path` | character |  |
| `seo_event_jsonld` | character |  |
| `seo_breadcrumb_jsonld` | character |  |
| `seo_faq_jsonld` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_match_details-example}

```python
fotmob_match_details(match_id='4813647')
```

_Last validated n/a._

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
| `ccode` | character |  |
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `primary_id` | character |  |
| `name` | character | Display name. |
| `matches` | character |  |
| `internal_rank` | integer |  |
| `simple_league` | logical |  |
| `local_rank` | integer |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_matches-example}

```python
fotmob_matches(date='20260301', timezone='UTC')
```

_Last validated n/a._

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
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `name` | character | Display name. |
| `is_coach` | logical |  |
| `is_captain` | logical |  |
| `gender` | character | Player's gender as Fotmob ships it (e.g. 'male'). |
| `injury_information` | character |  |
| `international_duty` | character |  |
| `player_information` | character |  |
| `recent_matches` | character |  |
| `matches_url` | character |  |
| `coach_stats` | character |  |
| `stat_seasons` | character |  |
| `status` | character | Player's status as Fotmob ships it (e.g. 'active'). |
| `data_provider` | character |  |
| `birth_date_utc_time` | character |  |
| `birth_date_timezone` | character |  |
| `contract_end_utc_time` | character |  |
| `contract_end_timezone` | character |  |
| `primary_team_team_id` | character |  |
| `primary_team_team_name` | character |  |
| `primary_team_on_loan` | logical |  |
| `primary_team_team_colors` | character |  |
| `position_description_positions` | character |  |
| `position_description_primary_position` | character |  |
| `position_description_non_primary_positions` | character |  |
| `main_league_league_id` | character |  |
| `main_league_league_name` | character |  |
| `main_league_season` | character |  |
| `main_league_stats` | character |  |
| `trophies_player_trophies` | character |  |
| `trophies_coach_trophies` | character |  |
| `match_filters_teams` | character |  |
| `match_filters_leagues` | character |  |
| `career_history_show_footnote` | logical |  |
| `career_history_career_items` | character |  |
| `career_history_full_career` | logical |  |
| `traits_key` | character |  |
| `traits_title` | character |  |
| `traits_items` | character |  |
| `meta_seopath` | character |  |
| `meta_pageurl` | character |  |
| `meta_faq_jsonld` | character |  |
| `meta_person_jsonld` | character |  |
| `meta_breadcrumb_jsonld` | character |  |
| `first_season_stats_section_order` | character |  |
| `first_season_stats_shotmap` | character |  |
| `first_season_stats_stats_section` | character |  |
| `first_season_stats_top_stat_card` | character |  |
| `first_season_stats_heatmap` | character |  |
| `first_season_stats_keeper_shotmap` | character |  |
| `first_season_stats_shotmap_team_colors` | character |  |
| `market_values_values` | character |  |
| `related_links_data_teammates` | character |  |
| `related_links_data_mens_national_team` | character |  |
| `related_links_data_womens_national_team` | character |  |
| `next_match_match_id` | character |  |
| `next_match_home_id` | character |  |
| `next_match_away_id` | character |  |
| `next_match_home_name` | character |  |
| `next_match_away_name` | character |  |
| `next_match_match_date` | character |  |
| `next_match_status_id` | character |  |
| `next_match_league_id` | character |  |
| `next_match_parent_league_id` | character |  |
| `next_match_league_name` | character |  |
| `next_match_status` | character |  |
| `next_match_stadium` | character |  |
| `next_match_match_url` | character |  |
| `initial_matches_page_matches` | character |  |
| `initial_matches_page_previous` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_player_data-example}

```python
fotmob_player_data(id='1077894')
```

_Last validated n/a._

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
| `suggestions` | character |  |
| `title_key` | character |  |
| `title_value` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_search_suggest-example}

```python
fotmob_search_suggest(lang='en', term='haaland')
```

_Last validated n/a._

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
| `ccode` | character |  |
| `league_id` | character | Fotmob league id of the table, as a string. |
| `page_url` | character |  |
| `league_name` | character | Fotmob league name of the table (e.g. 'Premier League'). |
| `legend` | character |  |
| `ongoing` | character | Stringified list of the team's ongoing matches at capture; empty when none. |
| `table_filter_types` | character |  |
| `composite` | logical |  |
| `table_all` | character |  |
| `table_home` | character |  |
| `table_away` | character |  |
| `table_xg` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_table-example}

```python
fotmob_table(url='http://data.fotmob.com/tables.ext.47.fot.gz')
```

_Last validated n/a._

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
| `tabs` | character |  |
| `all_available_seasons` | character |  |
| `seostr` | character |  |
| `qa_data` | character |  |
| `table` | character |  |
| `details_id` | character |  |
| `details_type` | character |  |
| `details_name` | character |  |
| `details_latest_season` | character |  |
| `details_short_name` | character |  |
| `details_country` | character |  |
| `details_gender` | character |  |
| `details_national_team_id` | character |  |
| `details_faq_jsonld` | character |  |
| `details_sports_team_jsonld` | character |  |
| `details_breadcrumb_jsonld` | character |  |
| `details_can_sync_calendar` | logical |  |
| `details_has_fixture_difficulty` | logical |  |
| `details_primary_league_id` | character |  |
| `details_primary_league_name` | character |  |
| `details_team_color` | character |  |
| `details_seopath` | character |  |
| `transfers_type` | character |  |
| `transfers_data` | character |  |
| `transfers_all_transfers` | character |  |
| `transfers_all_rumours` | character |  |
| `transfers_max_fee` | integer |  |
| `transfers_our_team_id` | character |  |
| `overview_season` | character |  |
| `overview_selected_season` | character |  |
| `overview_table` | character |  |
| `overview_top_players` | character |  |
| `overview_venue` | character |  |
| `overview_coach_history` | character |  |
| `overview_overview_fixtures` | character |  |
| `overview_next_match` | character |  |
| `overview_last_match` | character |  |
| `overview_team_form` | character |  |
| `overview_has_ongoing_match` | logical |  |
| `overview_previous_fixtures_url` | character |  |
| `overview_team_colors` | character |  |
| `overview_last_lineup_stats` | character |  |
| `overview_news_summary` | character |  |
| `overview_featured_article` | character |  |
| `overview_fixture_difficulty` | character |  |
| `overview_squad` | character |  |
| `overview_transfers` | character |  |
| `stats_team_id` | character |  |
| `stats_primary_league_id` | character |  |
| `stats_primary_season_id` | character |  |
| `stats_players` | character |  |
| `stats_teams` | character |  |
| `stats_tournament_id` | character |  |
| `stats_tournament_seasons` | character |  |
| `fixtures_all_fixtures` | character |  |
| `fixtures_primary_tournament_id` | character |  |
| `fixtures_previous_fixtures_url` | character |  |
| `fixtures_has_ongoing_match` | logical |  |
| `squad_squad` | character |  |
| `squad_is_national_team` | logical |  |
| `history_trophy_list` | character |  |
| `history_historical_table_data` | character |  |
| `history_team_color_map` | character |  |
| `history_tables` | character |  |
| `history_coach_history` | character |  |
| `history_team_colors` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_teams-example}

```python
fotmob_teams(id='8650', tab='overview', time_zone='UTC', type='team')
```

_Last validated n/a._

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
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `image_url` | character | URL of the article's lead image. |
| `title` | character | Display title. |
| `gmt_time` | character |  |
| `source_str` | character |  |
| `source_icon_url` | character |  |
| `language` | character | Language code of the article (e.g. 'en'). |
| `page_url` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_tlnews-example}

```python
fotmob_tlnews(id='47', start_index='0', type='league')
```

_Last validated n/a._

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
| `name` | character | Display name. |
| `player_id` | character | Fotmob player id of the transferred player, as a string. |
| `transfer_date` | character | Timestamp the transfer was recorded (ISO-8601 UTC). |
| `transfer_text` | character |  |
| `from_club` | character |  |
| `from_club_full_name` | character |  |
| `from_club_id` | character |  |
| `to_club` | character |  |
| `to_club_full_name` | character |  |
| `to_club_id` | character |  |
| `amount_euro_estimated` | integer |  |
| `contract_extension` | logical |  |
| `on_loan` | logical |  |
| `from_date` | character |  |
| `to_date` | character |  |
| `market_value` | integer |  |
| `position_label` | character |  |
| `position_key` | character |  |
| `from_club_colors_light_mode` | character |  |
| `from_club_colors_dark_mode` | character |  |
| `to_club_colors_light_mode` | character |  |
| `to_club_colors_dark_mode` | character |  |
| `fee_fee_text` | character |  |
| `fee_localized_fee_text` | character |  |
| `transfer_type_text` | character |  |
| `transfer_type_localization_key` | character |  |
| `fee_value` | integer |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_top_transfers-example}

```python
fotmob_top_transfers()
```

_Last validated n/a._

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
| `localized_key` | character |  |
| `round_id` | character | Composite id of the round. |
| `link` | character | Fotmob API URL of the round's team-of-the-week payload. |
| `is_completed` | logical |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_totw_rounds-example}

```python
fotmob_totw_rounds(league_id='47', season='2025/2026')
```

_Last validated n/a._

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
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `image_url` | character | URL of the article's lead image. |
| `title` | character | Display title. |
| `gmt_time` | character |  |
| `source_str` | character |  |
| `source_icon_url` | character |  |
| `page_url` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_trending_news-example}

```python
fotmob_trending_news(lang='en')
```

_Last validated n/a._

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
| `id` | character | Provider identifier for the entity (Utf8 join key). |
| `start_time` | character | Broadcast start as an ASP.NET-style '/Date(<ms>)/' string (milliseconds since the Unix epoch). |
| `end_time` | character | Broadcast end as an ASP.NET-style '/Date(<ms>)/' string; a large negative value is Fotmob's 'unset' sentinel. |
| `qualifiers` | character | Stringified list of listing qualifiers (e.g. ['Live']). |
| `station_id` | character |  |
| `match_id` | character |  |
| `league_id` | character | Fotmob league id of the listing's competition, as a string. |
| `parent_league_id` | character |  |
| `bet365_match_id` | character |  |
| `external_id` | character | Provider-side identifier for the media item, matching the play id it accompanies. |
| `affiliates` | character |  |
| `tags` | character | Content tags, JSON-encoded. |
| `station_call_sign` | character |  |
| `station_station_id` | character |  |
| `station_affiliate_id` | character |  |
| `station_affiliate_call_sign` | character |  |
| `station_name` | character |  |
| `station_type` | character |  |
| `station_blocked_country_codes` | character |  |
| `program_root_id` | character |  |
| `program_teams` | character |  |

**`return_parsed=False`** — the decoded JSON body (a page object, a one-list envelope, an id-keyed map or a list; `{}` or `null` for an unknown id).

### Example {#fotmob_tvlistings-example}

```python
fotmob_tvlistings(country_code='GB')
```

_Last validated n/a._
