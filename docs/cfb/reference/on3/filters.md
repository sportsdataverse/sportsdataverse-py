# CFB — On3 Recruit Database (api.on3.com) — Filters

> CFB — On3 Recruit Database (api.on3.com) — Filters — function reference in sdv-py, the SportsDataverse Python package.

## on3_filters_conferences

GET /rdb/v1/filters/conferences

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/conferences`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/conferences](https://api.on3.com/public/rdb/v1/filters/conferences)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_conferences-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_conferences-example}

```python
on3_filters_conferences()
```

_Last validated n/a._

## on3_filters_draft_rounds

GET /rdb/v1/filters/draft-rounds

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/draft-rounds`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/draft-rounds](https://api.on3.com/public/rdb/v1/filters/draft-rounds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_draft_rounds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `round` | integer | Draft round number. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_draft_rounds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_draft_rounds-example}

```python
on3_filters_draft_rounds()
```

_Last validated n/a._

## on3_filters_positions

GET /rdb/v1/filters/positions

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/positions`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/positions](https://api.on3.com/public/rdb/v1/filters/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `positionType` | `position_type` |  |  | `Y` | positionType query parameter. |

### Returns {#on3_filters_positions-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_positions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_positions-example}

```python
on3_filters_positions()
```

_Last validated n/a._

## on3_filters_sports

GET /rdb/v1/filters/sports

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/sports`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/sports](https://api.on3.com/public/rdb/v1/filters/sports)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_sports-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_sports-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_sports-example}

```python
on3_filters_sports()
```

_Last validated n/a._

## on3_filters_status

GET /rdb/v1/filters/status

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/status`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/status](https://api.on3.com/public/rdb/v1/filters/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `value` | character | Filter value as On3 lists it. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_status-example}

```python
on3_filters_status()
```

_Last validated n/a._

## on3_filters_teams

GET /rdb/v1/filters/teams

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/teams`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/teams](https://api.on3.com/public/rdb/v1/filters/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `groupBy` | `group_by` |  |  | `Y` | groupBy query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |

### Returns {#on3_filters_teams-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_teams-example}

```python
on3_filters_teams()
```

_Last validated n/a._

## on3_filters_years

GET /rdb/v1/filters/years

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/filters/years`

**Valid URL:** [https://api.on3.com/public/rdb/v1/filters/years](https://api.on3.com/public/rdb/v1/filters/years)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#on3_filters_years-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the row has nested objects whose flattened column names depend on which are null in the data; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_filters_years-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_filters_years-example}

```python
on3_filters_years()
```

_Last validated n/a._
