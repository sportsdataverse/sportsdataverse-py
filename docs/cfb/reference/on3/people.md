# College football — On3 Recruit Database (api.on3.com) — People

> College football — On3 Recruit Database (api.on3.com) — People — function reference in sdv-py, the SportsDataverse Python package.

## on3_people_combine_measurements

People combine measurements.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/combine-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/combine-measurements](https://api.on3.com/public/rdb/v1/people/89617/combine-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_combine_measurements-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: its committed capture has 0 rows, so the parser emits no columns; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_combine_measurements-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_combine_measurements-example}

```python
on3_people_combine_measurements(person_key=89617)
```

## on3_people_latest_valuation

People latest valuation.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/latest-valuation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/latest-valuation](https://api.on3.com/public/rdb/v1/people/89617/latest-valuation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_latest_valuation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nil_status` | String | Status of the athlete's On3 NIL valuation (e.g. active, inactive). |
| `valuation` | Int64 | Athlete's latest On3 NIL valuation in dollars. |
| `valuation_change` | Int64 | Change in the NIL valuation since the previous update, in dollars. |
| `followers` | Int64 | Total social-media followers counted toward the valuation. |
| `rank` | Int64 | Overall rank of the player's NIL valuation across On3's NIL 100. |
| `last_updated` | Int64 | Unix timestamp (seconds) of the last update to the NIL valuation. |
| `whisper` | Float64 | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `whisper_change` | Float64 | Change in the whisper valuation since the previous update, in US dollars. |
| `social_valuations` | String | Per-platform breakdown of the social components of the valuation (stringified list). |
| `group_rank` | Int64 | Rank of the NIL valuation within its group (sport or position). |
| `group_name` | String | Name of the group the NIL valuation is ranked within (usually null). |
| `tags` | String | JSON-encoded list of NIL tags on the valuation (e.g. Influencer). |
| `roster_value` | String | Nested roster-value object of the NIL valuation (usually null). |
| `nil_value` | String | On3 NIL valuation in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_latest_valuation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_latest_valuation-example}

```python
on3_people_latest_valuation(person_key=89617)
```

## on3_people_measurements

People measurements.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/measurements](https://api.on3.com/public/rdb/v1/people/89617/measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_measurements-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_measurements` | String | JSON-encoded list of the player's measurement records (type, value, verification). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_measurements-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_measurements-example}

```python
on3_people_measurements(person_key=89617)
```

## on3_people_measurements_averages

People measurements averages.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/measurements/averages`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/measurements/averages](https://api.on3.com/public/rdb/v1/people/89617/measurements/averages)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_people_measurements_averages-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `measurement_key` | Int64 | On3 key for the measurement category being averaged. |
| `measurement_name` | String | Name of the measurement category (e.g. height, 40-yard dash). |
| `current_person_measurement` | Float64 | Athlete's current value for the measurement. |
| `current_measurement_verified` | String | Whether the athlete's current measurement is verified by On3. |
| `top300_difference` | Float64 | Difference between the athlete's value and the On3 Top300 average. |
| `top300_average` | Float64 | Average value of the measurement among On3 Top300-ranked players. |
| `combine_drafted_average` | Float64 | Average combine value of the measurement among drafted players. |
| `combine_drafted_difference` | Float64 | Difference between the athlete's value and the drafted-player combine average. |
| `sort` | String | Display sort order of the measurement row on the On3 profile. |
| `measurement_record` | String | Nested On3 record for the athlete's underlying measurement (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_measurements_averages-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_measurements_averages-example}

```python
on3_people_measurements_averages(person_key=89617)
```

## on3_people_person_connections

People person connections.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/person-connections`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/person-connections](https://api.on3.com/public/rdb/v1/people/89617/person-connections)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_people_person_connections-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: its committed capture has 0 rows, so the parser emits no columns; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_person_connections-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_person_connections-example}

```python
on3_people_person_connections(person_key=89617)
```

## on3_people_social

People social.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/social`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/social](https://api.on3.com/public/rdb/v1/people/89617/social)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_social-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | String | Type label of the row (vocabulary depends on the endpoint). |
| `handle` | String | Athlete's account handle on the social platform. |
| `handshake` | Boolean | On3 RDB handshake field on the social-account record (platform link/verification metadata). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_social-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_social-example}

```python
on3_people_social(person_key=89617)
```

## on3_people_social_post_summary

People social post summary.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/social-post-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/social-post-summary](https://api.on3.com/public/rdb/v1/people/89617/social-post-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_social_post_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `social_type` | String | Social platform the post summary covers (e.g. Twitter/X, Instagram). |
| `type` | String | Type label of the row (vocabulary depends on the endpoint). |
| `followers` | Int64 | Athlete's follower count on the platform. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_social_post_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_social_post_summary-example}

```python
on3_people_social_post_summary(person_key=89617)
```

## on3_people_track_and_field_measurements

People track and field measurements.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/track-and-field-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/track-and-field-measurements](https://api.on3.com/public/rdb/v1/people/89617/track-and-field-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_track_and_field_measurements-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_track_and_field_measurements-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_track_and_field_measurements-example}

```python
on3_people_track_and_field_measurements(person_key=89617)
```

## on3_people_valuation_growth

People valuation growth.

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/valuation-growth`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/valuation-growth](https://api.on3.com/public/rdb/v1/people/89617/valuation-growth)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_valuation_growth-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nil_status` | String | Status of the athlete's On3 NIL valuation at the snapshot (e.g. active, inactive). |
| `valuation` | Int64 | Athlete's On3 NIL valuation in dollars at the snapshot. |
| `valuation_change` | Float64 | Change in the NIL valuation versus the previous snapshot, in dollars. |
| `date` | String | Date of the On3 NIL valuation snapshot. |
| `date_unix` | Int64 | Unix timestamp of the valuation snapshot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#on3_people_valuation_growth-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#on3_people_valuation_growth-example}

```python
on3_people_valuation_growth(person_key=89617)
```
