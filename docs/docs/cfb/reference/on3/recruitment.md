---
title: "CFB — On3 Recruit Database (api.on3.com) — Recruitment"
sidebar_label: "Recruitment"
sidebar_position: 5
description: "CFB — On3 Recruit Database (api.on3.com) — Recruitment — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# CFB — On3 Recruit Database (api.on3.com) — Recruitment

## on3_recruitment_primary_recruitment_evaluation

GET /rdb/v1/recruitment/{recruitmentKey}/primary-recruitment-evaluation

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitment/{recruitment_key}/primary-recruitment-evaluation`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitment/270036/primary-recruitment-evaluation](https://api.on3.com/public/rdb/v1/recruitment/270036/primary-recruitment-evaluation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `recruitment_key` | `recruitment_key` |  | `Y` |  | recruitment_key path parameter. |

### Returns {#on3_recruitment_primary_recruitment_evaluation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the scouting evaluation. |
| `recruitment_key` | integer | On3 recruitment key the evaluation is attached to. |
| `author_key` | integer | On3 user key of the evaluation's author. |
| `author_name` | character | Name of the On3 scout who wrote the evaluation. |
| `author_title` | character | Job title of the On3 scout who wrote the evaluation. |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character | Full text of the scouting evaluation. |
| `primary` | logical | Whether this is the primary (featured) evaluation for the recruitment. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer | Unix timestamp of the evaluation's last update. |
| `date_added` | character | Date the evaluation was added. |
| `date_updated` | character | Date the evaluation was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitment_primary_recruitment_evaluation-example}

```python
on3_recruitment_primary_recruitment_evaluation(recruitment_key=270036)
```

_Last validated n/a._

## on3_recruitment_recruitment_evaluations

GET /rdb/v1/recruitment/{recruitmentKey}/recruitment-evaluations

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitment/{recruitment_key}/recruitment-evaluations`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitment/270036/recruitment-evaluations](https://api.on3.com/public/rdb/v1/recruitment/270036/recruitment-evaluations)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `recruitment_key` | `recruitment_key` |  | `Y` |  | recruitment_key path parameter. |

### Returns {#on3_recruitment_recruitment_evaluations-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the scouting evaluation. |
| `recruitment_key` | integer | On3 recruitment key the evaluation is attached to. |
| `author_key` | integer | On3 user key of the evaluation's author. |
| `author_name` | character | Name of the On3 scout who wrote the evaluation. |
| `author_title` | character | Job title of the On3 scout who wrote the evaluation. |
| `title` | character | Specific role title for the assignment. |
| `premium` | logical | Whether the article is premium content. |
| `body` | character | Full text of the scouting evaluation. |
| `primary` | logical | Whether this is the primary (featured) evaluation for the recruitment. |
| `category` | character | CFBD stats category name (e.g. passing, rushing, defensive). |
| `date_updated_unix` | integer | Unix timestamp of the evaluation's last update. |
| `date_added` | character | Date the evaluation was added. |
| `date_updated` | character | Date the evaluation was last updated. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitment_recruitment_evaluations-example}

```python
on3_recruitment_recruitment_evaluations(recruitment_key=270036)
```

_Last validated n/a._

## on3_recruitments_latest_rpm_picks

Latest RPM (prediction) picks feed — paged {list,pagination}

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks](https://api.on3.com/public/rdb/v1/recruitments/latest-rpm-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `orgKey` | `org_key` |  |  | `Y` | orgKey query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `page` | `page` |  |  | `Y` | page query parameter. |
| `pageSize` | `page_size` |  |  | `Y` | pageSize query parameter. |

### Returns {#on3_recruitments_latest_rpm_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pick` | character | Pick number of the NFL draftee within the round they were picked in. |
| `player` | character | Player name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_latest_rpm_picks-example}

```python
on3_recruitments_latest_rpm_picks()
```

_Last validated n/a._

## on3_recruitments_profile

GET /rdb/v1/recruitments/{recKey}/profile

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/profile`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/profile](https://api.on3.com/public/rdb/v1/recruitments/270036/profile)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns {#on3_recruitments_profile-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `rating` | character | Overall SP+ rating (Bill Connelly methodology, in points per game). |
| `class_year` | integer | Recruiting class year of the recruitment. |
| `committed_status` | character | Nested commitment status of the recruitment (stringified). |
| `high_school` | character | High school |
| `high_school_org` | character | Nested On3 organization object for the recruit's high school (stringified). |
| `home_town` | character | Player home town. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_profile-example}

```python
on3_recruitments_profile(rec_key=270036)
```

_Last validated n/a._

## on3_recruitments_rpm_picks

GET /rdb/v1/recruitments/{recKey}/rpm-picks

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/rpm-picks`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-picks](https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns {#on3_recruitments_rpm_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `key` | integer | On3 RDB key for the RPM pick. |
| `organization` | character | Organization. |
| `date_added` | character | Date the expert logged the RPM pick. |
| `expert` | character | Nested On3 record for the expert who logged the pick (stringified). |
| `confidence` | numeric | Expert's confidence level in the prediction. |
| `description` | character | ESPN's description of the stat. |
| `article_link` | character | Link to the On3 article accompanying the pick, when any. |
| `premium` | logical | Whether the article is premium content. |
| `correct` | logical | Whether the pick proved correct. |
| `days_correct` | numeric | Number of days the pick stood as correct. |
| `flipped_from_organization` | character | Organization the expert's pick flipped from, when the prediction changed (stringified). |
| `previous_confidence` | numeric | Expert's confidence level on their previous pick for this recruitment. |
| `previous_date_added` | character | Date the expert's previous pick was logged. |
| `type` | character | Record-type category (e.g. `total`, `home`, `road`). |
| `top_teams` | character | Teams currently leading for the recruit per the pick, as a stringified list. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_rpm_picks-example}

```python
on3_recruitments_rpm_picks(rec_key=270036)
```

_Last validated n/a._

## on3_recruitments_rpm_summary

GET /rdb/v1/recruitments/{recKey}/rpm-summary

**Endpoint URL:** `GET https://api.on3.com/public/rdb/v1/recruitments/{rec_key}/rpm-summary`

**Valid URL:** [https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-summary](https://api.on3.com/public/rdb/v1/recruitments/270036/rpm-summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `rec_key` | `rec_key` |  | `Y` |  | rec_key path parameter. |

### Returns {#on3_recruitments_rpm_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `predictions` | character | Per-team RPM prediction percentages for the recruitment, as a stringified list. |
| `locked` | logical | Whether the RPM prediction for the recruitment is locked (no longer updating). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#on3_recruitments_rpm_summary-example}

```python
on3_recruitments_rpm_summary(rec_key=270036)
```

_Last validated n/a._
