# ESPN content (news) — ESPN content API (content.core.api.espn.com/v1, news)

> ESPN content (news) — ESPN content API (content.core.api.espn.com/v1, news) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.providers.espn_content` — 3 endpoints.

## espn_content_league_news

Headlines for one league (limit, offset).

**Endpoint URL:** `GET https://content.core.api.espn.com/v1/sports/{sport_slug}/{league_slug}/news`

**Valid URL:** [https://content.core.api.espn.com/v1/sports/football/nfl/news](https://content.core.api.espn.com/v1/sports/football/nfl/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport_slug` | `sport_slug` |  | `Y` |  | ESPN sport slug (football, basketball, soccer, ...). |
| `league_slug` | `league_slug` |  | `Y` |  | ESPN league slug (nfl, college-football, nba, mens-college-basketball, eng.1, ...). |
| `limit` | `limit` |  |  | `Y` | Page size (default 10; the body echoes resultsLimit). |
| `offset` | `offset` |  |  | `Y` | Page offset (body echoes resultsOffset). |

### Returns {#espn_content_league_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 |  |
| `now_id` | String |  |
| `content_key` | String |  |
| `data_source_identifier` | String |  |
| `publishedkey` | String |  |
| `type` | String |  |
| `feed_display_type` | String |  |
| `headline` | String |  |
| `description` | String |  |
| `title` | String |  |
| `link_text` | String |  |
| `categorized` | String |  |
| `originally_posted` | String |  |
| `last_modified` | String |  |
| `published` | String |  |
| `root` | String |  |
| `section` | String |  |
| `images` | String |  |
| `related` | String |  |
| `categories` | String |  |
| `keywords` | String |  |
| `story` | String |  |
| `allow_amp` | Boolean |  |
| `allow_ads` | Boolean |  |
| `allow_comments` | Boolean |  |
| `allow_commerce` | Boolean |  |
| `allow_content_reactions` | Boolean |  |
| `allow_search` | Boolean |  |
| `byline` | String |  |
| `is_live_blog` | Boolean |  |
| `premium` | Boolean |  |
| `links_api_self_href` | String |  |
| `links_app_sportscenter_href` | String |  |
| `links_mobile_href` | String |  |
| `links_web_href` | String |  |
| `video` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_content_league_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Content.core answered 404 (unknown story id, sport or league slug). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_content_league_news-example}

```python
espn_content_league_news(league_slug='nfl', sport_slug='football')
```

## espn_content_news

Cross-sport headlines (limit, offset).

**Endpoint URL:** `GET https://content.core.api.espn.com/v1/sports/news`

**Valid URL:** [https://content.core.api.espn.com/v1/sports/news](https://content.core.api.espn.com/v1/sports/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Page size (default 10; the body echoes resultsLimit). |
| `offset` | `offset` |  |  | `Y` | Page offset (body echoes resultsOffset). |

### Returns {#espn_content_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the article. |
| `now_id` | String | ESPN 'now' feed id. |
| `content_key` | String | Internal content key. |
| `data_source_identifier` | String | Source-system identifier. |
| `publishedkey` | String |  |
| `type` | String | Article type (Story, Media, HeadlineNews, etc.). |
| `feed_display_type` | String |  |
| `headline` | String | Article headline. |
| `description` | String | Article summary/description. |
| `title` | String |  |
| `link_text` | String |  |
| `categorized` | String |  |
| `originally_posted` | String |  |
| `last_modified` | String | Last-modified timestamp (ISO 8601). |
| `published` | String | Publish timestamp (ISO 8601). |
| `root` | String |  |
| `section` | String |  |
| `images` | String | Article images (list, stringified). |
| `categories` | String | Article categories (list, stringified). |
| `keywords` | String |  |
| `story` | String |  |
| `allow_amp` | Boolean |  |
| `allow_ads` | Boolean |  |
| `allow_comments` | Boolean |  |
| `allow_commerce` | Boolean |  |
| `allow_content_reactions` | Boolean |  |
| `allow_search` | Boolean |  |
| `byline` | String | Author byline string as published by ESPN. |
| `is_live_blog` | Boolean |  |
| `premium` | Boolean | Whether the article is premium/paywalled. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_web_href` | String | Web article URL. |
| `video` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_content_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Content.core answered 404 (unknown story id, sport or league slug). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_content_news-example}

```python
espn_content_news()
```

## espn_content_story

One story (same envelope, one headline).

**Endpoint URL:** `GET https://content.core.api.espn.com/v1/sports/news/{id}`

**Valid URL:** [https://content.core.api.espn.com/v1/sports/news/49778940](https://content.core.api.espn.com/v1/sports/news/49778940)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `id` | `id` |  | `Y` |  | Story id (headlines[].id; links.api.self.href points here). |

### Returns {#espn_content_story-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 |  |
| `now_id` | String |  |
| `content_key` | String |  |
| `data_source_identifier` | String |  |
| `publishedkey` | String |  |
| `type` | String |  |
| `feed_display_type` | String |  |
| `headline` | String |  |
| `description` | String |  |
| `title` | String |  |
| `link_text` | String |  |
| `categorized` | String |  |
| `originally_posted` | String |  |
| `last_modified` | String |  |
| `published` | String |  |
| `root` | String |  |
| `section` | String |  |
| `images` | String |  |
| `categories` | String |  |
| `keywords` | String |  |
| `story` | String |  |
| `allow_amp` | Boolean |  |
| `allow_ads` | Boolean |  |
| `allow_comments` | Boolean |  |
| `allow_commerce` | Boolean |  |
| `allow_content_reactions` | Boolean |  |
| `allow_search` | Boolean |  |
| `byline` | String |  |
| `is_live_blog` | Boolean |  |
| `premium` | Boolean |  |
| `links_api_self_href` | String |  |
| `links_app_sportscenter_href` | String |  |
| `links_mobile_href` | String |  |
| `links_web_href` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_content_story-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. Content.core answered 404 (unknown story id, sport or league slug). |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_content_story-example}

```python
espn_content_story(id='49778940')
```
