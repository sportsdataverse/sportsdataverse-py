# College football — On3 Recruit Database (api.on3.com) — Draft

> College football — On3 Recruit Database (api.on3.com) — Draft — function reference in sdv-py, the SportsDataverse Python package.

## on3_draft_organization_rank

Draft organization rank.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/draft-organization-rank`

**Valid URL:** [https://api.on3.com/public/rdb/v1/draft-organization-rank](https://api.on3.com/public/rdb/v1/draft-organization-rank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_draft_organization_rank-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_draft_organization_rank-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_draft_organization_rank-example}

```python
on3_draft_organization_rank()
```

## on3_draft_pick_organization_rank

Draft pick organization rank.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/draft-pick-organization-rank`

**Valid URL:** [https://api.on3.com/public/rdb/v1/draft-pick-organization-rank](https://api.on3.com/public/rdb/v1/draft-pick-organization-rank)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_draft_pick_organization_rank-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_draft_pick_organization_rank-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_draft_pick_organization_rank-example}

```python
on3_draft_pick_organization_rank()
```

## on3_drafts

Drafts.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts](https://api.on3.com/public/rdb/v1/drafts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `round` | `round` |  |  | `Y` | round query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_drafts-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: its committed capture has 0 rows, so the parser emits no columns; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_drafts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_drafts-example}

```python
on3_drafts()
```

## on3_drafts_by_stars

Drafts by stars.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts-by-stars`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts-by-stars](https://api.on3.com/public/rdb/v1/drafts-by-stars)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `yearSpan` | `year_span` |  |  | `Y` | yearSpan query parameter. |

### Returns {#on3_drafts_by_stars-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `blue_chip_percent` | Float64 | Percent of the drafted group who were blue-chip (four- or five-star) recruits. |
| `population_percent` | Float64 | Percent of the overall recruit population holding this star rating. |
| `talent_ratio` | Float64 | Ratio of the star tier's draft share to its population share (On3's talent ratio). |
| `five_stars` | Int64 | Number of drafted players who were five-star recruits. |
| `four_stars` | Int64 | Number of drafted players who were four-star recruits. |
| `three_stars` | Int64 | Number of drafted players who were three-star recruits. |
| `zero_stars` | Int64 | Number of drafted players who were unrated (zero-star) recruits. |
| `total` | Int64 | Total drafted players counted in the row. |
| `state_key` | Int64 | On3 numeric key of the state. |
| `state_name` | String | Name of the state the row aggregates (e.g. Alabama). |
| `state_abbreviation` | String | Two-letter state abbreviation. |
| `state_country_key` | Int64 | On3 numeric key of the state's country. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_drafts_by_stars-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_drafts_by_stars-example}

```python
on3_drafts_by_stars()
```

## on3_drafts_by_stars_summary

Drafts by stars summary.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts-by-stars-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts-by-stars-summary](https://api.on3.com/public/rdb/v1/drafts-by-stars-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `sportKey` | `sport_key` |  |  | `Y` | sportKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_drafts_by_stars_summary-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_drafts_by_stars_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_drafts_by_stars_summary-example}

```python
on3_drafts_by_stars_summary()
```

## on3_drafts_players

Drafts players.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/drafts/{org_key}/players`

**Valid URL:** [https://api.on3.com/public/rdb/v1/drafts/1867/players](https://api.on3.com/public/rdb/v1/drafts/1867/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `org_key` | `org_key` |  | `Y` |  | org_key path parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |

### Returns {#on3_drafts_players-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_drafts_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_drafts_players-example}

```python
on3_drafts_players(org_key=1867)
```
