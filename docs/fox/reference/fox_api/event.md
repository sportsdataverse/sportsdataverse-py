# FOX — Fox Sports API (api.foxsports.com) — Event

> FOX — Fox Sports API (api.foxsports.com) — Event — function reference in sdv-py, the SportsDataverse Python package.

## fox_api_event_data

GET /bifrost/v1/{sport}/event/{event_id}/data -- Fox Sports API event data.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/data`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/data?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/data?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_data-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `columns` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `is_cutoff` | logical |  |
| `is_faded` | logical |  |
| `selected` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_event_data-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_event_data-example}

```python
fox_api_event_data(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_matchup

GET /bifrost/v1/{sport}/event/{event_id}/matchup -- Fox Sports API event matchup.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/matchup`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/matchup?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/matchup?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_matchup-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_image_alt_text` | character |  |
| `entity_image_alt_url` | character |  |
| `entity_image_type` | character |  |
| `entity_image_url` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `text` | character |  |
| `title` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_event_matchup-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_event_matchup-example}

```python
fox_api_event_matchup(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_odds

GET /bifrost/v1/{sport}/event/{event_id}/odds -- Fox Sports API event odds.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/odds`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/odds?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/odds?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `odds1` | character |  |
| `odds2` | character |  |
| `point` | integer |  |
| `text` | character |  |
| `text1` | character |  |
| `text2` | character |  |
| `timestamp` | character |  |
| `annotation_color` | character |  |
| `annotation_text` | character |  |
| `annotation_type` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_event_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_event_odds-example}

```python
fox_api_event_odds(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_recap

GET /bifrost/v1/{sport}/event/{event_id}/recap -- Fox Sports API event recap.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/recap`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/recap?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/recap?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_recap-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `entity_image_alt_text` | character |  |
| `entity_image_alt_url` | character |  |
| `entity_image_type` | character |  |
| `entity_image_url` | character |  |
| `entity_link_alternate_image_url` | character |  |
| `entity_link_analytics_name` | character |  |
| `entity_link_analytics_sport` | character |  |
| `entity_link_color` | character |  |
| `entity_link_content_type` | character |  |
| `entity_link_content_uri` | character |  |
| `entity_link_image_alt_text` | character |  |
| `entity_link_image_type` | character |  |
| `entity_link_image_url` | character |  |
| `entity_link_layout_path` | character |  |
| `entity_link_layout_tokens_content_uri` | character |  |
| `entity_link_layout_tokens_id` | character |  |
| `entity_link_title` | character |  |
| `entity_link_type` | character |  |
| `entity_link_web_url` | character |  |
| `text` | character |  |
| `title` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_event_recap-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_event_recap-example}

```python
fox_api_event_recap(sport='nfl', event_id='11195')
```

_Last validated n/a._

## fox_api_event_standings

GET /bifrost/v1/{sport}/event/{event_id}/standings -- Fox Sports API event standings.

**Endpoint URL:** `GET https://api.foxsports.com/bifrost/v1/{sport}/event/{event_id}/standings`

**Valid URL:** [https://api.foxsports.com/bifrost/v1/nfl/event/11195/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1](https://api.foxsports.com/bifrost/v1/nfl/event/11195/standings?apikey=jE7yBJVRNAwdDesMgTzTXUUSx1It41Fq&api-version=1.1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sport` | `sport` |  | `Y` |  | Fox sport/league slug as used in foxsports.com URLs, e.g. ``nfl``, ``cfb``, ``nba``, ``cbk``, ``wcbk``, ``mlb``, ``nhl``. |
| `event_id` | `event_id` |  | `Y` |  | Numeric Fox game (event) id, e.g. ``11195``. |
| `apikey` | `apikey` |  |  | `Y` | Public Fox Sports data-tier key shipped in the foxsports.com web bundle (not a secret); override only if Fox rotates it. |
| `api-version` | `api_version` |  |  | `Y` | Fox API version (``api-version`` query key). |

### Returns {#fox_api_event_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `section` | character |  |
| `nfc_north` | character |  |
| `v1` | character |  |
| `w_l_t` | character |  |
| `div` | character |  |
| `strk` | character |  |
| `entity_id` | character | Composite Yahoo id this editorial row was keyed under, surfaced from the collection map key (e.g., "ncaaf.g.202509200023" for a game, "ncaaf.t.29" for a team); always carried as Utf8. |
| `nfc_south` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#fox_api_event_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#fox_api_event_standings-example}

```python
fox_api_event_standings(sport='nfl', event_id='11195')
```

_Last validated n/a._
