---
title: TheSportsDB — TheSportsDB API v1 (thesportsdb.com; free test key by default, $THESPORTSDB_API_KEY to use your own)
sidebar_label: TheSportsDB API v1 (thesportsdb.com; free test key by default, $THESPORTSDB_API_KEY to use your own)
description: "TheSportsDB — TheSportsDB API v1 (thesportsdb.com; free test key by default, $THESPORTSDB_API_KEY to use your own) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# TheSportsDB — TheSportsDB API v1 (thesportsdb.com; free test key by default, $THESPORTSDB_API_KEY to use your own)

`sportsdataverse.providers.thesportsdb` — 12 endpoints.

## thesportsdb_event

One event.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/lookupevent.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/lookupevent.php?id=2267073](https://www.thesportsdb.com/api/v1/json/123/lookupevent.php?id=2267073)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. Event id (idEvent from /eventsseason.php). |

### Returns {#thesportsdb_event-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_event` | String |  |
| `id_ap_ifootball` | String |  |
| `str_timestamp` | String |  |
| `str_event` | String |  |
| `str_event_alternate` | String |  |
| `str_filename` | String |  |
| `str_sport` | String |  |
| `id_league` | String |  |
| `str_league` | String |  |
| `str_league_badge` | String |  |
| `str_season` | String |  |
| `str_description_en` | String |  |
| `str_home_team` | String |  |
| `str_away_team` | String |  |
| `int_home_score` | String |  |
| `int_home_score_extra` | String |  |
| `int_away_score_extra` | String |  |
| `int_round` | String |  |
| `int_away_score` | String |  |
| `int_spectators` | String |  |
| `str_official` | String |  |
| `str_weather` | String |  |
| `date_event` | String |  |
| `date_event_local` | String |  |
| `str_time` | String |  |
| `str_time_local` | String |  |
| `str_group` | String |  |
| `id_home_team` | String |  |
| `str_home_team_badge` | String |  |
| `id_away_team` | String |  |
| `str_away_team_badge` | String |  |
| `int_score` | String |  |
| `int_score_votes` | String |  |
| `str_result` | String |  |
| `id_venue` | String |  |
| `str_venue` | String |  |
| `str_country` | String |  |
| `str_city` | String |  |
| `str_poster` | String |  |
| `str_square` | String |  |
| `str_fanart` | String |  |
| `str_thumb` | String |  |
| `str_banner` | String |  |
| `str_map` | String |  |
| `str_tweet1` | String |  |
| `str_video` | String |  |
| `str_status` | String |  |
| `str_postponed` | String |  |
| `str_locked` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_event-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_event-example}

```python
thesportsdb_event(id='2267073')
```

## thesportsdb_league

One league.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/lookupleague.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/lookupleague.php?id=4328](https://www.thesportsdb.com/api/v1/json/123/lookupleague.php?id=4328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. League id (idLeague from /all_leagues.php). |

### Returns {#thesportsdb_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_league` | String |  |
| `id_ap_ifootball` | String |  |
| `id_ap_ifootballv3` | String |  |
| `str_sport` | String |  |
| `str_league` | String |  |
| `str_league_alternate` | String |  |
| `int_division` | String |  |
| `id_cup` | String |  |
| `str_current_season` | String |  |
| `int_formed_year` | String |  |
| `date_first_event` | String |  |
| `str_gender` | String |  |
| `str_country` | String |  |
| `str_website` | String |  |
| `str_facebook` | String |  |
| `str_instagram` | String |  |
| `str_twitter` | String |  |
| `str_youtube` | String |  |
| `str_rss` | String |  |
| `str_description_en` | String |  |
| `str_description_de` | String |  |
| `str_description_fr` | String |  |
| `str_description_it` | String |  |
| `str_description_cn` | String |  |
| `str_description_jp` | String |  |
| `str_description_ru` | String |  |
| `str_description_es` | String |  |
| `str_description_pt` | String |  |
| `str_description_se` | String |  |
| `str_description_nl` | String |  |
| `str_description_hu` | String |  |
| `str_description_no` | String |  |
| `str_description_pl` | String |  |
| `str_description_il` | String |  |
| `str_tv_rights` | String |  |
| `str_fanart1` | String |  |
| `str_fanart2` | String |  |
| `str_fanart3` | String |  |
| `str_fanart4` | String |  |
| `str_banner` | String |  |
| `str_badge` | String |  |
| `str_logo` | String |  |
| `str_poster` | String |  |
| `str_trophy` | String |  |
| `str_naming` | String |  |
| `str_complete` | String |  |
| `str_locked` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_league-example}

```python
thesportsdb_league(id='4328')
```

## thesportsdb_league_next_events

Next 15 events of a league.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/eventsnextleague.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/eventsnextleague.php?id=4328](https://www.thesportsdb.com/api/v1/json/123/eventsnextleague.php?id=4328)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. League id (idLeague from /all_leagues.php). |

### Returns {#thesportsdb_league_next_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_event` | String |  |
| `id_ap_ifootball` | String |  |
| `str_timestamp` | String |  |
| `str_event` | String |  |
| `str_event_alternate` | String |  |
| `str_filename` | String |  |
| `str_sport` | String |  |
| `id_league` | String |  |
| `str_league` | String |  |
| `str_league_badge` | String |  |
| `str_season` | String |  |
| `str_description_en` | String |  |
| `str_home_team` | String |  |
| `str_away_team` | String |  |
| `int_home_score` | String |  |
| `int_home_score_extra` | String |  |
| `int_away_score_extra` | String |  |
| `int_round` | String |  |
| `int_away_score` | String |  |
| `int_spectators` | String |  |
| `str_official` | String |  |
| `str_weather` | String |  |
| `date_event` | String |  |
| `date_event_local` | String |  |
| `str_time` | String |  |
| `str_time_local` | String |  |
| `str_group` | String |  |
| `id_home_team` | String |  |
| `str_home_team_badge` | String |  |
| `id_away_team` | String |  |
| `str_away_team_badge` | String |  |
| `int_score` | String |  |
| `int_score_votes` | String |  |
| `str_result` | String |  |
| `id_venue` | String |  |
| `str_venue` | String |  |
| `str_country` | String |  |
| `str_city` | String |  |
| `str_poster` | String |  |
| `str_square` | String |  |
| `str_fanart` | String |  |
| `str_thumb` | String |  |
| `str_banner` | String |  |
| `str_map` | String |  |
| `str_tweet1` | String |  |
| `str_video` | String |  |
| `str_status` | String |  |
| `str_postponed` | String |  |
| `str_locked` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_league_next_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_league_next_events-example}

```python
thesportsdb_league_next_events(id='4328')
```

## thesportsdb_league_teams

Teams in a league (by league name).

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/search_all_teams.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/search_all_teams.php?l=English+Premier+League](https://www.thesportsdb.com/api/v1/json/123/search_all_teams.php?l=English+Premier+League)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `l` | `league_name` |  | `Y` |  | Required. League name as spelled in strLeague. |

### Returns {#thesportsdb_league_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_team` | String |  |
| `id_espn` | String |  |
| `id_ap_ifootball` | String |  |
| `int_loved` | String |  |
| `str_team` | String |  |
| `str_team_alternate` | String |  |
| `str_team_short` | String |  |
| `int_formed_year` | String |  |
| `str_sport` | String |  |
| `str_league` | String |  |
| `id_league` | String |  |
| `str_league2` | String |  |
| `id_league2` | String |  |
| `str_league3` | String |  |
| `id_league3` | String |  |
| `str_league4` | String |  |
| `id_league4` | String |  |
| `str_league5` | String |  |
| `id_league5` | String |  |
| `str_league6` | String |  |
| `id_league6` | String |  |
| `str_league7` | String |  |
| `id_league7` | String |  |
| `str_division` | String |  |
| `id_venue` | String |  |
| `str_stadium` | String |  |
| `str_keywords` | String |  |
| `str_rss` | String |  |
| `str_location` | String |  |
| `str_website` | String |  |
| `str_facebook` | String |  |
| `str_twitter` | String |  |
| `str_instagram` | String |  |
| `str_description_en` | String |  |
| `str_description_de` | String |  |
| `str_description_fr` | String |  |
| `str_description_cn` | String |  |
| `str_description_it` | String |  |
| `str_description_jp` | String |  |
| `str_description_ru` | String |  |
| `str_description_es` | String |  |
| `str_description_pt` | String |  |
| `str_description_se` | String |  |
| `str_description_nl` | String |  |
| `str_description_hu` | String |  |
| `str_description_no` | String |  |
| `str_description_il` | String |  |
| `str_description_pl` | String |  |
| `str_colour1` | String |  |
| `str_colour2` | String |  |
| `str_colour3` | String |  |
| `str_gender` | String |  |
| `str_country` | String |  |
| `str_badge` | String |  |
| `str_logo` | String |  |
| `str_fanart1` | String |  |
| `str_fanart2` | String |  |
| `str_fanart3` | String |  |
| `str_fanart4` | String |  |
| `str_banner` | String |  |
| `str_equipment` | String |  |
| `str_youtube` | String |  |
| `str_locked` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_league_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_league_teams-example}

```python
thesportsdb_league_teams(league_name='English Premier League')
```

## thesportsdb_leagues

All leagues.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/all_leagues.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/all_leagues.php](https://www.thesportsdb.com/api/v1/json/123/all_leagues.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#thesportsdb_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_league` | String |  |
| `str_league` | String |  |
| `str_sport` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_leagues-example}

```python
thesportsdb_leagues()
```

## thesportsdb_player

One player.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/lookupplayer.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/lookupplayer.php?id=34145937](https://www.thesportsdb.com/api/v1/json/123/lookupplayer.php?id=34145937)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. Player id (idPlayer from /lookup_all_players.php). |

### Returns {#thesportsdb_player-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | String |  |
| `id_team` | String |  |
| `id_team2` | String |  |
| `id_team_national` | String |  |
| `id_ap_ifootball` | String |  |
| `id_player_manager` | String |  |
| `id_wikidata` | String |  |
| `id_transfer_mkt` | String |  |
| `id_espn` | String |  |
| `id_google` | String |  |
| `str_nationality` | String |  |
| `str_player` | String |  |
| `str_player_alternate` | String |  |
| `str_team` | String |  |
| `str_team2` | String |  |
| `str_sport` | String |  |
| `int_soccer_xml_team_id` | String |  |
| `date_born` | String |  |
| `date_died` | String |  |
| `str_number` | String |  |
| `date_signed` | String |  |
| `str_signing` | String |  |
| `str_wage` | String |  |
| `str_outfitter` | String |  |
| `str_kit` | String |  |
| `str_agent` | String |  |
| `str_birth_location` | String |  |
| `str_death_location` | String |  |
| `str_ethnicity` | String |  |
| `str_status` | String |  |
| `str_description_en` | String |  |
| `str_description_de` | String |  |
| `str_description_fr` | String |  |
| `str_description_cn` | String |  |
| `str_description_it` | String |  |
| `str_description_jp` | String |  |
| `str_description_ru` | String |  |
| `str_description_es` | String |  |
| `str_description_pt` | String |  |
| `str_description_se` | String |  |
| `str_description_nl` | String |  |
| `str_description_hu` | String |  |
| `str_description_no` | String |  |
| `str_description_il` | String |  |
| `str_description_pl` | String |  |
| `str_gender` | String |  |
| `str_side` | String |  |
| `str_position` | String |  |
| `str_college` | String |  |
| `str_facebook` | String |  |
| `str_website` | String |  |
| `str_twitter` | String |  |
| `str_instagram` | String |  |
| `str_youtube` | String |  |
| `str_height` | String |  |
| `str_weight` | String |  |
| `int_loved` | String |  |
| `str_thumb` | String |  |
| `str_poster` | String |  |
| `str_cutout` | String |  |
| `str_cartoon` | String |  |
| `str_render` | String |  |
| `str_banner` | String |  |
| `str_fanart1` | String |  |
| `str_fanart2` | String |  |
| `str_fanart3` | String |  |
| `str_fanart4` | String |  |
| `str_creative_commons` | String |  |
| `str_creative_commons_attribution` | String |  |
| `str_locked` | String |  |
| `str_last_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_player-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_player-example}

```python
thesportsdb_player(id='34145937')
```

## thesportsdb_player_search

Search players by name.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/searchplayers.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/searchplayers.php?p=Danny+Welbeck](https://www.thesportsdb.com/api/v1/json/123/searchplayers.php?p=Danny+Welbeck)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `p` | `player_name` |  | `Y` |  | Required. Player name (substring search). |

### Returns {#thesportsdb_player_search-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | String |  |
| `id_team` | String |  |
| `str_player` | String |  |
| `str_team` | String |  |
| `str_sport` | String |  |
| `str_thumb` | String |  |
| `str_cutout` | String |  |
| `str_nationality` | String |  |
| `date_born` | String |  |
| `str_status` | String |  |
| `str_gender` | String |  |
| `str_position` | String |  |
| `relevance` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_player_search-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_player_search-example}

```python
thesportsdb_player_search(player_name='Danny Welbeck')
```

## thesportsdb_season_events

Events of a league-season.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/eventsseason.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/eventsseason.php?id=4328&s=2025-2026](https://www.thesportsdb.com/api/v1/json/123/eventsseason.php?id=4328&s=2025-2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. League id (idLeague from /all_leagues.php). |
| `s` | `season` |  |  | `Y` | Season label, e.g. 2025-2026; omitted, the API answers the current season. |

### Returns {#thesportsdb_season_events-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_event` | String |  |
| `str_event` | String |  |
| `str_event_alternate` | String |  |
| `str_season` | String |  |
| `id_league` | String |  |
| `str_league` | String |  |
| `str_league_badge` | String |  |
| `str_sport` | String |  |
| `str_home_team` | String |  |
| `str_away_team` | String |  |
| `id_home_team` | String |  |
| `id_away_team` | String |  |
| `int_round` | String |  |
| `int_home_score` | String |  |
| `int_away_score` | String |  |
| `str_timestamp` | String |  |
| `date_event` | String |  |
| `date_event_local` | String |  |
| `str_time` | String |  |
| `str_time_local` | String |  |
| `str_home_team_badge` | String |  |
| `str_away_team_badge` | String |  |
| `str_venue` | String |  |
| `str_country` | String |  |
| `str_thumb` | String |  |
| `str_poster` | String |  |
| `str_video` | String |  |
| `str_postponed` | String |  |
| `str_filename` | String |  |
| `str_status` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_season_events-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_season_events-example}

```python
thesportsdb_season_events(id='4328', season='2025-2026')
```

## thesportsdb_sports

All sports.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/all_sports.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/all_sports.php](https://www.thesportsdb.com/api/v1/json/123/all_sports.php)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#thesportsdb_sports-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_sport` | String |  |
| `str_sport` | String |  |
| `str_format` | String |  |
| `str_sport_thumb` | String |  |
| `str_sport_thumb_bw` | String |  |
| `str_sport_icon_green` | String |  |
| `str_sport_description` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_sports-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_sports-example}

```python
thesportsdb_sports()
```

## thesportsdb_table

League table.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/lookuptable.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/lookuptable.php?l=4328&s=2025-2026](https://www.thesportsdb.com/api/v1/json/123/lookuptable.php?l=4328&s=2025-2026)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `l` | `league_id` |  | `Y` |  | Required. League id (idLeague from /all_leagues.php). |
| `s` | `season` |  |  | `Y` | Season label, e.g. 2025-2026; omitted, the API answers the current season. |

### Returns {#thesportsdb_table-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_standing` | String |  |
| `int_rank` | String |  |
| `id_team` | String |  |
| `str_team` | String |  |
| `str_badge` | String |  |
| `id_league` | String |  |
| `str_league` | String |  |
| `str_season` | String |  |
| `str_group` | String |  |
| `str_form` | String |  |
| `str_description` | String |  |
| `int_played` | String |  |
| `int_win` | String |  |
| `int_loss` | String |  |
| `int_draw` | String |  |
| `int_goals_for` | String |  |
| `int_goals_against` | String |  |
| `int_goal_difference` | String |  |
| `int_points` | String |  |
| `date_updated` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_table-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_table-example}

```python
thesportsdb_table(league_id='4328', season='2025-2026')
```

## thesportsdb_team

One team.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/lookupteam.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/lookupteam.php?id=133604](https://www.thesportsdb.com/api/v1/json/123/lookupteam.php?id=133604)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. Team id (idTeam from /search_all_teams.php). |

### Returns {#thesportsdb_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_team` | String |  |
| `id_espn` | String |  |
| `id_ap_ifootball` | String |  |
| `int_loved` | String |  |
| `str_team` | String |  |
| `str_team_alternate` | String |  |
| `str_team_short` | String |  |
| `int_formed_year` | String |  |
| `str_sport` | String |  |
| `str_league` | String |  |
| `id_league` | String |  |
| `str_league2` | String |  |
| `id_league2` | String |  |
| `str_league3` | String |  |
| `id_league3` | String |  |
| `str_league4` | String |  |
| `id_league4` | String |  |
| `str_league5` | String |  |
| `id_league5` | String |  |
| `str_league6` | String |  |
| `id_league6` | String |  |
| `str_league7` | String |  |
| `id_league7` | String |  |
| `str_division` | String |  |
| `id_venue` | String |  |
| `str_stadium` | String |  |
| `str_keywords` | String |  |
| `str_rss` | String |  |
| `str_location` | String |  |
| `str_website` | String |  |
| `str_facebook` | String |  |
| `str_twitter` | String |  |
| `str_instagram` | String |  |
| `str_description_en` | String |  |
| `str_description_de` | String |  |
| `str_description_fr` | String |  |
| `str_description_cn` | String |  |
| `str_description_it` | String |  |
| `str_description_jp` | String |  |
| `str_description_ru` | String |  |
| `str_description_es` | String |  |
| `str_description_pt` | String |  |
| `str_description_se` | String |  |
| `str_description_nl` | String |  |
| `str_description_hu` | String |  |
| `str_description_no` | String |  |
| `str_description_il` | String |  |
| `str_description_pl` | String |  |
| `str_colour1` | String |  |
| `str_colour2` | String |  |
| `str_colour3` | String |  |
| `str_gender` | String |  |
| `str_country` | String |  |
| `str_badge` | String |  |
| `str_logo` | String |  |
| `str_fanart1` | String |  |
| `str_fanart2` | String |  |
| `str_fanart3` | String |  |
| `str_fanart4` | String |  |
| `str_banner` | String |  |
| `str_equipment` | String |  |
| `str_youtube` | String |  |
| `str_locked` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_team-example}

```python
thesportsdb_team(id='133604')
```

## thesportsdb_team_players

Players of a team.

**Endpoint URL:** `GET https://www.thesportsdb.com/api/v1/json/123/lookup_all_players.php`

**Valid URL:** [https://www.thesportsdb.com/api/v1/json/123/lookup_all_players.php?id=133604](https://www.thesportsdb.com/api/v1/json/123/lookup_all_players.php?id=133604)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Required. Team id (idTeam from /search_all_teams.php). |

### Returns {#thesportsdb_team_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id_player` | String |  |
| `id_team` | String |  |
| `id_team2` | String |  |
| `id_team_national` | String |  |
| `id_ap_ifootball` | String |  |
| `id_player_manager` | String |  |
| `id_wikidata` | String |  |
| `id_transfer_mkt` | String |  |
| `id_espn` | String |  |
| `id_google` | String |  |
| `str_nationality` | String |  |
| `str_player` | String |  |
| `str_player_alternate` | String |  |
| `str_team` | String |  |
| `str_team2` | String |  |
| `str_sport` | String |  |
| `int_soccer_xml_team_id` | String |  |
| `date_born` | String |  |
| `date_died` | String |  |
| `str_number` | String |  |
| `date_signed` | String |  |
| `str_signing` | String |  |
| `str_wage` | String |  |
| `str_outfitter` | String |  |
| `str_kit` | String |  |
| `str_agent` | String |  |
| `str_birth_location` | String |  |
| `str_death_location` | String |  |
| `str_ethnicity` | String |  |
| `str_status` | String |  |
| `str_description_en` | String |  |
| `str_description_de` | String |  |
| `str_description_fr` | String |  |
| `str_description_cn` | String |  |
| `str_description_it` | String |  |
| `str_description_jp` | String |  |
| `str_description_ru` | String |  |
| `str_description_es` | String |  |
| `str_description_pt` | String |  |
| `str_description_se` | String |  |
| `str_description_nl` | String |  |
| `str_description_hu` | String |  |
| `str_description_no` | String |  |
| `str_description_il` | String |  |
| `str_description_pl` | String |  |
| `str_gender` | String |  |
| `str_side` | String |  |
| `str_position` | String |  |
| `str_college` | String |  |
| `str_facebook` | String |  |
| `str_website` | String |  |
| `str_twitter` | String |  |
| `str_instagram` | String |  |
| `str_youtube` | String |  |
| `str_height` | String |  |
| `str_weight` | String |  |
| `int_loved` | String |  |
| `str_thumb` | String |  |
| `str_poster` | String |  |
| `str_cutout` | String |  |
| `str_cartoon` | String |  |
| `str_render` | String |  |
| `str_banner` | String |  |
| `str_fanart1` | String |  |
| `str_fanart2` | String |  |
| `str_fanart3` | String |  |
| `str_fanart4` | String |  |
| `str_creative_commons` | String |  |
| `str_creative_commons_attribution` | String |  |
| `str_locked` | String |  |
| `str_last_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#thesportsdb_team_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. TheSportsDB answered 404 (unknown id or route). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#thesportsdb_team_players-example}

```python
thesportsdb_team_players(id='133604')
```
