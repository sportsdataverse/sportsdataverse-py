# CFB — ESPN site API (v2) — Team

> CFB — ESPN site API (v2) — Team — function reference in sdv-py, the SportsDataverse Python package.

## espn_cfb_teams_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?limit=1000](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_teams_site-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | character | Secondary team color as a hex string (no leading '#'). |
| `team_color` | character | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | character | Full team display name (location + nickname). |
| `team_id` | character | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | logical | Whether the team is currently active. |
| `team_is_all_star` | logical | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | character | Team location / city (e.g. "Boston"). |
| `team_logos` | character | Pipe-delimited logo image URLs. |
| `team_name` | character | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | character | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | character | Abbreviated display name for compact UIs. |
| `team_slug` | character | URL slug used in ESPN web paths. |
| `team_uid` | character | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_teams_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_teams_site-example}

```python
espn_cfb_teams_site()
```

_Last validated n/a._

## espn_cfb_team

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Team abbreviation. |
| `team_alternate_color` | character | Alternate team color. |
| `team_color` | character | Primary team color. |
| `team_display_name` | character | Full team display name. |
| `team_franchise_$ref` | character |  |
| `team_franchise_abbreviation` | character |  |
| `team_franchise_color` | character |  |
| `team_franchise_display_name` | character |  |
| `team_franchise_id` | character |  |
| `team_franchise_is_active` | logical |  |
| `team_franchise_location` | character |  |
| `team_franchise_name` | character |  |
| `team_franchise_short_display_name` | character |  |
| `team_franchise_slug` | character |  |
| `team_franchise_team_$ref` | character |  |
| `team_franchise_uid` | character |  |
| `team_franchise_venue_$ref` | character |  |
| `team_franchise_venue_address_city` | character |  |
| `team_franchise_venue_address_state` | character |  |
| `team_franchise_venue_full_name` | character |  |
| `team_franchise_venue_grass` | logical |  |
| `team_franchise_venue_guid` | character |  |
| `team_franchise_venue_id` | character |  |
| `team_franchise_venue_images` | character |  |
| `team_franchise_venue_indoor` | logical |  |
| `team_franchise_venue_short_name` | character |  |
| `team_groups_id` | character |  |
| `team_groups_is_conference` | logical |  |
| `team_groups_parent_id` | character |  |
| `team_id` | character | ESPN team id. |
| `team_is_active` | logical |  |
| `team_links` | character |  |
| `team_location` | character | Team location / school name. |
| `team_logos` | character |  |
| `team_name` | character | Team nickname. |
| `team_next_event` | character |  |
| `team_record_items` | character |  |
| `team_short_display_name` | character | Short team display name. |
| `team_slug` | character | Team slug for the stat row. |
| `team_standing_summary` | character |  |
| `team_uid` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team-example}

```python
espn_cfb_team(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/roster?limit=500](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/roster?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `id` | character | Id. |
| `uid` | character | Uid. |
| `guid` | character | Guid. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `full_name` | character | Full name. |
| `display_name` | character | Display name. |
| `short_name` | character | Short name. |
| `weight` | double | Weight. |
| `display_weight` | character | Display weight. |
| `height` | double | Height. |
| `display_height` | character | Display height. |
| `links` | character | Links. |
| `slug` | character | Slug. |
| `jersey` | character | Jersey. |
| `injuries` | character | Injuries. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_place_display_text` | character |  |
| `birth_country_alternate_id` | character | Alternate identifier for the athlete's country of birth used in ESPN's country-flag reference system. |
| `birth_country_abbreviation` | character |  |
| `college_id` | character | College id. |
| `college_guid` | character | College guid. |
| `college_mascot` | character | College mascot. |
| `college_name` | character | College name. |
| `college_short_name` | character | College short name. |
| `college_abbrev` | character | College abbrev. |
| `college_logos` | character | College logos. |
| `headshot_href` | character | Headshot href. |
| `headshot_alt` | character | Headshot alt. |
| `flag_href` | character | URL of the SVG or PNG flag image representing the athlete's country of birth. |
| `flag_alt` | character | Alt-text string for the athlete's country-of-birth flag image, typically the full country name. |
| `flag_rel` | character | Relationship descriptor for the athlete's country-of-birth flag link (e.g., "flag"). |
| `position_id` | character | Position id. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation. |
| `position_leaf` | logical | Position leaf. |
| `position_parent_id` | character | ESPN id of the parent position. |
| `position_parent_name` | character |  |
| `position_parent_display_name` | character |  |
| `position_parent_abbreviation` | character |  |
| `position_parent_leaf` | logical |  |
| `experience_years` | integer | Experience years. |
| `experience_display_value` | character |  |
| `experience_abbreviation` | character |  |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_roster-example}

```python
espn_cfb_team_roster(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_schedule

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_cfb_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric event identifier. |
| `date` | character | Event timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `time_valid` | logical | Whether the event time is confirmed. |
| `competitions` | character | Competition detail (list of dicts, stringified): competitors, venue, status. |
| `links` | character | Related links (list, stringified). |
| `season_year` | integer | Four-digit season year. |
| `season_display_name` | character | Human-readable season label (e.g. '2024-25'). |
| `season_type_id` | character | ESPN numeric identifier for the season type. |
| `season_type_type` | integer | Season type numeric code. |
| `season_type_name` | character | Season type name (e.g. Regular Season). |
| `season_type_abbreviation` | character | Season type abbreviation. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_schedule-example}

```python
espn_cfb_team_schedule(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_record-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_record-example}

```python
espn_cfb_team_record(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_depthcharts

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_depthcharts-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: depthchart, season, status, team, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_depthcharts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_depthcharts-example}

```python
espn_cfb_team_depthcharts(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_injuries-example}

```python
espn_cfb_team_injuries(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_transactions-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_transactions-example}

```python
espn_cfb_team_transactions(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_history

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_history-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, mbb, wbb; 400 in cfb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_history-example}

```python
espn_cfb_team_history(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_team_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | character | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | character | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | character | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_news-example}

```python
espn_cfb_team_news(team_id='4')
```

_Last validated n/a._

## espn_cfb_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_cfb_team_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_team_leaders-example}

```python
espn_cfb_team_leaders(team_id='4')
```

_Last validated n/a._
