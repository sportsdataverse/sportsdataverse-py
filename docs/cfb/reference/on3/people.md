# CFB — On3 Recruit Database (api.on3.com) — People

> CFB — On3 Recruit Database (api.on3.com) — People — function reference in sdv-py, the SportsDataverse Python package.

## on3_people_combine_measurements

GET /rdb/v1/people/{personKey}/combine-measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/combine-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/combine-measurements](https://api.on3.com/public/rdb/v1/people/89617/combine-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_combine_measurements-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: its committed capture has 0 rows, so the parser emits no columns; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_combine_measurements-example}

```python
on3_people_combine_measurements(person_key=89617)
```

_Last validated n/a._

## on3_people_latest_valuation

GET /rdb/v1/people/{personKey}/latest-valuation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/latest-valuation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/latest-valuation](https://api.on3.com/public/rdb/v1/people/89617/latest-valuation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_latest_valuation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nil_status` | character | Status of the athlete's On3 NIL valuation (e.g. active, inactive). |
| `valuation` | integer | Athlete's latest On3 NIL valuation in dollars. |
| `valuation_change` | integer | Change in the NIL valuation since the previous update, in dollars. |
| `followers` | integer | Total social-media followers counted toward the valuation. |
| `rank` | integer | Overall rank of the player's NIL valuation across On3's NIL 100. |
| `last_updated` | integer | Unix timestamp (seconds) of the last update to the NIL valuation. |
| `whisper` | numeric | On3 whisper valuation (reported deal value) behind the NIL valuation, in US dollars. |
| `whisper_change` | numeric | Change in the whisper valuation since the previous update, in US dollars. |
| `social_valuations` | character | Per-platform breakdown of the social components of the valuation (stringified list). |
| `group_rank` | integer | Rank of the NIL valuation within its group (sport or position). |
| `group_name` | character | Name of the group the NIL valuation is ranked within (usually null). |
| `tags` | character | JSON-encoded list of NIL tags on the valuation (e.g. Influencer). |
| `roster_value` | character | Nested roster-value object of the NIL valuation (usually null). |
| `nil_value` | character | On3 NIL valuation in US dollars. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_latest_valuation-example}

```python
on3_people_latest_valuation(person_key=89617)
```

_Last validated n/a._

## on3_people_measurements

GET /rdb/v1/people/{personKey}/measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/measurements](https://api.on3.com/public/rdb/v1/people/89617/measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_measurements-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `player_measurements` | character | JSON-encoded list of the player's measurement records (type, value, verification). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_measurements-example}

```python
on3_people_measurements(person_key=89617)
```

_Last validated n/a._

## on3_people_measurements_averages

GET /rdb/v1/people/{personKey}/measurements/averages

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
| `measurement_key` | integer | On3 key for the measurement category being averaged. |
| `measurement_name` | character | Name of the measurement category (e.g. height, 40-yard dash). |
| `current_person_measurement` | numeric | Athlete's current value for the measurement. |
| `current_measurement_verified` | character | Whether the athlete's current measurement is verified by On3. |
| `top300_difference` | numeric | Difference between the athlete's value and the On3 Top300 average. |
| `top300_average` | numeric | Average value of the measurement among On3 Top300-ranked players. |
| `combine_drafted_average` | numeric | Average combine value of the measurement among drafted players. |
| `combine_drafted_difference` | numeric | Difference between the athlete's value and the drafted-player combine average. |
| `sort` | character | Display sort order of the measurement row on the On3 profile. |
| `measurement_record` | character | Nested On3 record for the athlete's underlying measurement (stringified). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_measurements_averages-example}

```python
on3_people_measurements_averages(person_key=89617)
```

_Last validated n/a._

## on3_people_person_connections

GET /rdb/v1/people/{personKey}/person-connections

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

### Example {#on3_people_person_connections-example}

```python
on3_people_person_connections(person_key=89617)
```

_Last validated n/a._

## on3_people_social

GET /rdb/v1/people/{personKey}/social

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/social`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/social](https://api.on3.com/public/rdb/v1/people/89617/social)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_social-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | character | Type label of the row (vocabulary depends on the endpoint). |
| `handle` | character | Athlete's account handle on the social platform. |
| `handshake` | logical | On3 RDB handshake field on the social-account record (platform link/verification metadata). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_social-example}

```python
on3_people_social(person_key=89617)
```

_Last validated n/a._

## on3_people_social_post_summary

GET /rdb/v1/people/{personKey}/social-post-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/social-post-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/social-post-summary](https://api.on3.com/public/rdb/v1/people/89617/social-post-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_social_post_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `social_type` | character | Social platform the post summary covers (e.g. Twitter/X, Instagram). |
| `type` | character | Type label of the row (vocabulary depends on the endpoint). |
| `followers` | integer | Athlete's follower count on the platform. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_social_post_summary-example}

```python
on3_people_social_post_summary(person_key=89617)
```

_Last validated n/a._

## on3_people_track_and_field_measurements

GET /rdb/v1/people/{personKey}/track-and-field-measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/track-and-field-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/track-and-field-measurements](https://api.on3.com/public/rdb/v1/people/89617/track-and-field-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_track_and_field_measurements-returns}

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows, and the response type is only heuristically mapped (x-source: call-site); names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_track_and_field_measurements-example}

```python
on3_people_track_and_field_measurements(person_key=89617)
```

_Last validated n/a._

## on3_people_valuation_growth

GET /rdb/v1/people/{personKey}/valuation-growth

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/valuation-growth`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/valuation-growth](https://api.on3.com/public/rdb/v1/people/89617/valuation-growth)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_valuation_growth-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `nil_status` | character | Status of the athlete's On3 NIL valuation at the snapshot (e.g. active, inactive). |
| `valuation` | integer | Athlete's On3 NIL valuation in dollars at the snapshot. |
| `valuation_change` | numeric | Change in the NIL valuation versus the previous snapshot, in dollars. |
| `date` | character | Date of the On3 NIL valuation snapshot. |
| `date_unix` | integer | Unix timestamp of the valuation snapshot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_valuation_growth-example}

```python
on3_people_valuation_growth(person_key=89617)
```

_Last validated n/a._
