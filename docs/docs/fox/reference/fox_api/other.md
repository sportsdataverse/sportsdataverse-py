---
title: "FOX — Fox Sports API (api.foxsports.com) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "FOX — Fox Sports API (api.foxsports.com) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# FOX — Fox Sports API (api.foxsports.com) — Other

## fox_api_scoreboard

GET /bifrost/v1/{sport}/scoreboard/main -- Fox Sports API scoreboard.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/scoreboard/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/scoreboard/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/scoreboard/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `groupId` | `group_id` |  |  | `Y` | Conference / group id filter. |

### Returns {#fox_api_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `parameters_season_type` | character |  |
| `parameters_week` | character |  |
| `subtitle` | character |  |
| `title` | character |  |
| `uri` | character |  |
| `web_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_scoreboard-example}

```python
fox_api_scoreboard(sport='nfl')
```

## fox_api_scorechip

GET /bifrost/v1/{sport}/scorechip/{chip_id} -- one game's score chip (this route 400s if api-version is sent).

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/scorechip/{chip_id}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/scorechip/nfl11195?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq](https://api.foxsports.com/bifrost/v1/nfl/scorechip/nfl11195?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `chip_id` | `chip_id` |  | `Y` |  | Score-chip id: the league slug plus the numeric game id, e.g. ``nfl11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |

### Returns {#fox_api_scorechip-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `alt_sort_key` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_away_uri` | character |  |
| `entity_link_layout_tokens_event_uri` | character |  |
| `entity_link_layout_tokens_home_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `event_headline` | character |  |
| `event_status` | integer |  |
| `event_time` | character |  |
| `favorite_entities` | character |  |
| `id` | character |  |
| `importance` | integer |  |
| `is_tba` | logical |  |
| `league` | character |  |
| `lower_team_alternate_logo_url` | character |  |
| `lower_team_has_possession` | logical |  |
| `lower_team_image_alt_text` | character |  |
| `lower_team_image_type` | character |  |
| `lower_team_is_loser` | logical |  |
| `lower_team_logo_url` | character |  |
| `lower_team_long_name` | character |  |
| `lower_team_name` | character |  |
| `lower_team_record` | character |  |
| `lower_team_score` | integer |  |
| `lower_team_stacked_name_bottom` | character |  |
| `lower_team_stacked_name_top` | character |  |
| `lower_team_uri` | character |  |
| `odds_line` | character |  |
| `over_under_line` | character |  |
| `sort_key` | character |  |
| `status_line` | character |  |
| `template` | character |  |
| `upper_team_alternate_logo_url` | character |  |
| `upper_team_has_possession` | logical |  |
| `upper_team_image_alt_text` | character |  |
| `upper_team_image_type` | character |  |
| `upper_team_is_loser` | logical |  |
| `upper_team_logo_url` | character |  |
| `upper_team_long_name` | character |  |
| `upper_team_name` | character |  |
| `upper_team_record` | character |  |
| `upper_team_score` | integer |  |
| `upper_team_stacked_name_bottom` | character |  |
| `upper_team_stacked_name_top` | character |  |
| `upper_team_uri` | character |  |
| `uri` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_scorechip-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_scorechip-example}

```python
fox_api_scorechip(sport='nfl', chip_id='nfl11195')
```

## fox_api_topevents_scoreboard_segment

GET /bifrost/v1/topevents/scoreboard/segment/{segment} -- Fox Sports API topevents scoreboard segment.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/{segment}`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `segment` | `segment` |  | `Y` |  | Top-events scoreboard segment id (``0`` / ``1`` ... as listed by ``topevents/scoreboard/main``); NOT a league ``<season>-<week>-<type>`` id. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_topevents_scoreboard_segment-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `segment_id` | character |  |
| `section_id` | character |  |
| `section_title` | character |  |
| `game_id` | character |  |
| `chip_id` | character |  |
| `league` | character |  |
| `date` | character |  |
| `event_status` | integer |  |
| `status` | character |  |
| `tv_station` | character |  |
| `headline` | character |  |
| `odds_line` | character |  |
| `over_under_line` | character |  |
| `home_team` | character |  |
| `home_team_id` | character |  |
| `home_score` | integer |  |
| `home_record` | character |  |
| `away_team` | character |  |
| `away_team_id` | character |  |
| `away_score` | integer |  |
| `away_record` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_topevents_scoreboard_segment-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_topevents_scoreboard_segment-example}

```python
fox_api_topevents_scoreboard_segment(segment='1')
```

## fox_api_explore_browse

GET /bifrost/v1/explore/browse/{section}/main -- Fox Sports API explore browse.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/explore/browse/{section}/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/explore/browse/sports/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/explore/browse/sports/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `section` | `section` |  | `Y` |  | Browse section: ``sports``, ``players``, ``shows``, ``personalities`` or ``topics``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_explore_browse-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `fox_id` | character |  |
| `abbreviation` | character |  |
| `name` | character |  |
| `content_uri` | character |  |
| `content_type` | character |  |
| `web_url` | character |  |
| `color` | character |  |
| `logo_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_explore_browse-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_explore_browse-example}

```python
fox_api_explore_browse(section='sports')
```

## fox_api_explore_odds

GET /bifrost/v1/explore/odds/main -- Fox Sports API explore odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/explore/odds/main`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/explore/odds/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/explore/odds/main?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_explore_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `external_id` | character | Provider-side identifier for the media item, matching the play id it accompanies. |
| `external_uri` | character |  |
| `isa_value` | character |  |
| `path` | character |  |
| `short_title` | character |  |
| `slug` | character |  |
| `spark_id` | character |  |
| `tag_type` | character |  |
| `title` | character |  |
| `type` | character |  |
| `uuidv5` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_explore_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_explore_odds-example}

```python
fox_api_explore_odds()
```

## fox_api_search_content

GET /bifrost/v1/search/content -- Fox Sports API search content.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/content`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/content?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes](https://api.foxsports.com/bifrost/v1/search/content?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `text` | `text` |  |  | `Y` | Search text. |

### Returns {#fox_api_search_content-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `type` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `title` | character |  |
| `subtitle` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `web_url` | character |  |
| `analytics_name` | character |  |
| `image_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_search_content-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_search_content-example}

```python
fox_api_search_content(text='mahomes')
```

## fox_api_search_entities

GET /bifrost/v1/search/entities -- Fox Sports API search entities.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/entities`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/entities?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes](https://api.foxsports.com/bifrost/v1/search/entities?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1&text=mahomes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `text` | `text` |  |  | `Y` | Search text. |

### Returns {#fox_api_search_entities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `type` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `title` | character |  |
| `subtitle` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `web_url` | character |  |
| `analytics_name` | character |  |
| `image_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_search_entities-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_search_entities-example}

```python
fox_api_search_entities(text='mahomes')
```

## fox_api_search_popular

GET /bifrost/v1/search/popular -- Fox Sports API search popular.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/search/popular`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/search/popular?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/search/popular?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_search_popular-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | character |  |
| `type` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `title` | character |  |
| `subtitle` | character |  |
| `content_type` | character |  |
| `content_uri` | character |  |
| `web_url` | character |  |
| `analytics_name` | character |  |
| `image_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_search_popular-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_search_popular-example}

```python
fox_api_search_popular()
```

## fox_api_trending_articles

GET /bifrost/v1/general/trending/articles -- Fox Sports API trending articles.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/general/trending/articles`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/general/trending/articles?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4](https://api.foxsports.com/bifrost/v1/general/trending/articles?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `duration` | `duration` |  |  | `Y` | Trending look-back window. |
| `tags` | `tags` |  |  | `Y` | Comma-separated tag filter. |

### Returns {#fox_api_trending_articles-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `spark_id` | character |  |
| `title` | character |  |
| `description` | character |  |
| `content_type` | character |  |
| `component_type` | character |  |
| `publication_date` | character |  |
| `last_published_date` | character |  |
| `canonical_url` | character |  |
| `thumbnail_url` | character |  |
| `playback_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_trending_articles-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_trending_articles-example}

```python
fox_api_trending_articles()
```

## fox_api_trending_videos

GET /bifrost/v1/general/trending/videos -- Fox Sports API trending videos.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/general/trending/videos`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/general/trending/videos?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4&maxItems=12](https://api.foxsports.com/bifrost/v1/general/trending/videos?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&api-version=1.1&duration=4&maxItems=12)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |
| `duration` | `duration` |  |  | `Y` | Trending look-back window. |
| `maxItems` | `max_items` |  |  | `Y` | Maximum items to return. |

### Returns {#fox_api_trending_videos-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character |  |
| `spark_id` | character |  |
| `title` | character |  |
| `description` | character |  |
| `content_type` | character |  |
| `component_type` | character |  |
| `publication_date` | character |  |
| `last_published_date` | character |  |
| `canonical_url` | character |  |
| `thumbnail_url` | character |  |
| `playback_url` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_trending_videos-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_trending_videos-example}

```python
fox_api_trending_videos()
```

## fox_api_foxpolls

GET /foxpolls/v1/polls -- Fox Sports API foxpolls.

**Endpoint URL:** `GET https://api.foxsports.com/foxpolls/v1/polls`

**Valid URL:** [https://api.foxsports.com/foxpolls/v1/polls?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&includeAnswers=true](https://api.foxsports.com/foxpolls/v1/polls?apikey=SuNgfBgmTGS2xozZbnV6FcjGGRQrR8cg&includeAnswers=true)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports feed-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `associatedEntityIds` | `associated_entity_ids` |  |  | `Y` | Comma-separated Fox entity ids the polls are associated with. |
| `includeAnswers` | `include_answers` |  |  | `Y` | Include poll answer options. |

### Returns {#fox_api_foxpolls-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `poll_id` | integer |  |
| `uri` | character |  |
| `title` | character |  |
| `type` | character |  |
| `background_image_url` | character |  |
| `background_image_version` | integer |  |
| `background_image_type` | character |  |
| `background_image_alt_text` | character |  |
| `background_image_additional_urls` | character |  |
| `min_submissions_needed` | integer |  |
| `max_submissions_allowed` | integer |  |
| `max_votes_per_user` | integer |  |
| `cta_text` | character |  |
| `cta_url` | character |  |
| `results_display_type` | character |  |
| `results_display_text` | character |  |
| `results_commentary` | character |  |
| `results_displayed_at` | character |  |
| `authorization_type` | character |  |
| `starts_at` | character |  |
| `ends_at` | character |  |
| `locks_at` | character |  |
| `answers` | character |  |
| `votes` | integer |  |
| `min_votes_to_display_post_lock` | integer |  |
| `ads_enabled` | logical |  |
| `background_image_additional_urls_resized_url` | character |  |
| `background_image` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_foxpolls-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_foxpolls-example}

```python
fox_api_foxpolls()
```
