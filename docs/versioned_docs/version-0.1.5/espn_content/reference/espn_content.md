---
title: ESPN content (news) — ESPN content API (content.core.api.espn.com/v1, news)
sidebar_label: ESPN content API (content.core.api.espn.com/v1, news)
description: "ESPN content (news) — ESPN content API (content.core.api.espn.com/v1, news) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# ESPN content (news) — ESPN content API (content.core.api.espn.com/v1, news)

`sportsdataverse.espn_content` — 3 endpoints.

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
| `id` | character |  |
| `now_id` | character |  |
| `content_key` | character |  |
| `data_source_identifier` | character |  |
| `publishedkey` | character |  |
| `type` | character |  |
| `feed_display_type` | character |  |
| `headline` | character |  |
| `description` | character |  |
| `title` | character |  |
| `link_text` | character |  |
| `categorized` | character |  |
| `originally_posted` | character |  |
| `last_modified` | character |  |
| `published` | character |  |
| `root` | character |  |
| `section` | character |  |
| `images` | character |  |
| `related` | character |  |
| `categories` | character |  |
| `keywords` | character |  |
| `story` | character |  |
| `allow_amp` | logical |  |
| `allow_ads` | logical |  |
| `allow_comments` | logical |  |
| `allow_commerce` | logical |  |
| `allow_content_reactions` | logical |  |
| `allow_search` | logical |  |
| `byline` | character |  |
| `is_live_blog` | logical |  |
| `premium` | logical |  |
| `links_api_self_href` | character |  |
| `links_app_sportscenter_href` | character |  |
| `links_mobile_href` | character |  |
| `links_web_href` | character |  |
| `video` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_content_league_news-example}

```python
espn_content_league_news(league_slug='nfl', sport_slug='football')
```

_Last validated n/a._

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
| `id` | character | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `publishedkey` | character |  |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `feed_display_type` | character |  |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `title` | character |  |
| `link_text` | character |  |
| `categorized` | character |  |
| `originally_posted` | character |  |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `root` | character |  |
| `section` | character |  |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `keywords` | character |  |
| `story` | character |  |
| `allow_amp` | logical |  |
| `allow_ads` | logical |  |
| `allow_comments` | logical |  |
| `allow_commerce` | logical |  |
| `allow_content_reactions` | logical |  |
| `allow_search` | logical |  |
| `byline` | character | Author byline string as published by ESPN. |
| `is_live_blog` | logical |  |
| `premium` | logical | Whether the article is premium/paywalled. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_web_href` | character | Web article URL. |
| `video` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_content_news-example}

```python
espn_content_news()
```

_Last validated n/a._

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
| `id` | character |  |
| `now_id` | character |  |
| `content_key` | character |  |
| `data_source_identifier` | character |  |
| `publishedkey` | character |  |
| `type` | character |  |
| `feed_display_type` | character |  |
| `headline` | character |  |
| `description` | character |  |
| `title` | character |  |
| `link_text` | character |  |
| `categorized` | character |  |
| `originally_posted` | character |  |
| `last_modified` | character |  |
| `published` | character |  |
| `root` | character |  |
| `section` | character |  |
| `images` | character |  |
| `categories` | character |  |
| `keywords` | character |  |
| `story` | character |  |
| `allow_amp` | logical |  |
| `allow_ads` | logical |  |
| `allow_comments` | logical |  |
| `allow_commerce` | logical |  |
| `allow_content_reactions` | logical |  |
| `allow_search` | logical |  |
| `byline` | character |  |
| `is_live_blog` | logical |  |
| `premium` | logical |  |
| `links_api_self_href` | character |  |
| `links_app_sportscenter_href` | character |  |
| `links_mobile_href` | character |  |
| `links_web_href` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_content_story-example}

```python
espn_content_story(id='49778940')
```

_Last validated n/a._
