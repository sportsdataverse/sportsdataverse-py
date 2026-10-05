---
title: "CFB — On3 Recruit Database (api.on3.com) — People"
sidebar_label: "People"
sidebar_position: 3
description: "CFB — On3 Recruit Database (api.on3.com) — People — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — People

## on3_people_combine_measurements

GET /rdb/v1/people/{personKey}/combine-measurements

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/people/{person_key}/combine-measurements`

**Valid URL:** [https://api.on3.com/public/rdb/v1/people/89617/combine-measurements](https://api.on3.com/public/rdb/v1/people/89617/combine-measurements)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `person_key` | `person_key` |  | `Y` |  | person_key path parameter. |

### Returns {#on3_people_combine_measurements-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `measurement_type_key` | integer | On3 key for the measurement category. |
| `measurement_type` | character | Measurement category (e.g. height, weight, 40-yard dash) per On3's measurement taxonomy. |
| `value` | numeric | Metric value. |
| `is_verified` | logical | Whether the player profile is verified. |

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
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `last_updated` | integer | Timestamp ESPN last refreshed the power index. |
| `social_valuations` | character | Per-platform breakdown of the social components of the valuation (stringified list). |
| `group_rank` | integer | League/season rank for group. |
| `group_name` | character | Group name (conference / division). |

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
| `key` | integer | On3 RDB key for the measurement record. |
| `measurement_type` | character | Measurement category (e.g. height, weight, 40-yard dash) per On3's measurement taxonomy. |
| `measurement_type_key` | integer | On3 key for the measurement category. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | numeric | Metric value. |
| `delta` | numeric | Change in the measured value versus the player's previous measurement of the same type. |
| `person_key` | integer | On3 person key of the measured athlete. |
| `verified` | logical | Whether On3 verified the measurement. |
| `verified_by_user_key` | integer | On3 user key of the staffer who verified the measurement. |
| `elite` | logical | Whether On3 flags the result as elite for this measurement type. |
| `event_key` | integer | On3 key of the camp or combine event where the measurement was taken. |
| `event_name` | character | Event name (e.g. 'All-Star Workout Day: Home Run Derby'). |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `age_measurement_occurred` | numeric | Athlete's age when the measurement was taken. |
| `top300_average` | numeric | Average value of this measurement among On3 Top300-ranked players. |
| `top_average_change_percent` | numeric | Percent difference between the athlete's value and the Top300 average. |
| `drafted_average` | numeric | Average value of this measurement among drafted players at the combine. |
| `record` | character | Team win-loss record for the season. |
| `draft_change_percent` | numeric | Percent difference between the athlete's value and the drafted-player average. |
| `date_added` | integer | Date the measurement record was added to the On3 database. |
| `date_modified` | integer | Date and time that injury information was updated |
| `date_occurred` | integer | Date the measurement was actually taken. |
| `is_current` | logical | Whether this is the athlete's current (most recent) measurement of the type. |
| `person_sport_org_key` | integer | On3 player-sport-organization (PSO) key the measurement is attached to. |
| `organization` | character | Organization. |

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
| `current_measurement_verified` | logical | Whether the athlete's current measurement is verified by On3. |
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `connection` | character | Relationship type linking the two people (e.g. sibling, parent, teammate) per On3. |
| `connected_player` | character | Nested On3 person object for the connected player (stringified). |
| `connected_roster_rating` | character | Nested On3 roster rating object for the connected player (stringified). |
| `connected_rating` | character | Nested On3 recruiting rating object for the connected player (stringified). |
| `connected_college_organization` | character | Nested On3 organization object for the connected player's college (stringified). |
| `connected_draft` | character | Nested On3 draft record for the connected player, when drafted (stringified). |

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
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
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
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the measurement record. |
| `measurement_type` | character | Measurement category (e.g. height, weight, 40-yard dash) per On3's measurement taxonomy. |
| `measurement_type_key` | integer | On3 key for the measurement category. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `value` | numeric | Metric value. |
| `delta` | numeric | Change in the measured value versus the player's previous measurement of the same type. |
| `person_key` | integer | On3 person key of the measured athlete. |
| `verified` | logical | Whether On3 verified the measurement. |
| `verified_by_user_key` | integer | On3 user key of the staffer who verified the measurement. |
| `elite` | logical | Whether On3 flags the result as elite for this measurement type. |
| `event_key` | integer | On3 key of the camp or combine event where the measurement was taken. |
| `event_name` | character | Event name (e.g. 'All-Star Workout Day: Home Run Derby'). |
| `event` | character | Binary flag indicating the row is a counted game event (excludes end markers). |
| `age_measurement_occurred` | numeric | Athlete's age when the measurement was taken. |
| `top300_average` | numeric | Average value of this measurement among On3 Top300-ranked players. |
| `top_average_change_percent` | numeric | Percent difference between the athlete's value and the Top300 average. |
| `drafted_average` | numeric | Average value of this measurement among drafted players at the combine. |
| `record` | character | Team win-loss record for the season. |
| `draft_change_percent` | numeric | Percent difference between the athlete's value and the drafted-player average. |
| `date_added` | integer | Date the measurement record was added to the On3 database. |
| `date_modified` | integer | Date and time that injury information was updated |
| `date_occurred` | integer | Date the measurement was actually taken. |
| `is_current` | logical | Whether this is the athlete's current (most recent) measurement of the type. |
| `person_sport_org_key` | integer | On3 player-sport-organization (PSO) key the measurement is attached to. |
| `organization` | character | Organization. |

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
| `valuation_change` | integer | Change in the NIL valuation versus the previous snapshot, in dollars. |
| `date` | character | Date of the On3 NIL valuation snapshot. |
| `date_unix` | integer | Unix timestamp of the valuation snapshot. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_people_valuation_growth-example}

```python
on3_people_valuation_growth(person_key=89617)
```

_Last validated n/a._
