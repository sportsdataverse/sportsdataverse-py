---
title: "CFB — On3 Recruit Database (api.on3.com) — Recruitment"
sidebar_label: "Recruitment"
sidebar_position: 6
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

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

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

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

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
| `pick_key` | integer |  |
| `pick_organization` | character |  |
| `pick_year` | integer |  |
| `pick_date_added` | character |  |
| `pick_expert` | character |  |
| `pick_confidence` | numeric |  |
| `pick_article_link` | character |  |
| `pick_premium` | logical |  |
| `pick_correct` | character |  |
| `pick_days_correct` | numeric |  |
| `pick_flipped_from_organization` | character |  |
| `pick_previous_confidence` | character |  |
| `pick_previous_date_added` | character |  |
| `pick_type` | character |  |
| `pick_expert_accuracy` | numeric |  |
| `pick_top_teams` | character |  |
| `player_key` | integer |  |
| `player_recruitment_key` | integer |  |
| `player_name` | character | Full name of player |
| `player_slug` | character | URL-safe player identifier. |
| `player_high_school_name` | character |  |
| `player_high_school` | character |  |
| `player_home_town_name` | character |  |
| `player_early_enrollee` | logical |  |
| `player_early_signee` | logical |  |
| `player_default_asset_url` | character |  |
| `player_class_year` | character |  |
| `player_athlete_verified` | logical |  |
| `player_prospect_verified` | logical |  |
| `player_default_asset_key` | integer |  |
| `player_default_asset_domain_override` | character |  |
| `player_default_asset_domain` | character |  |
| `player_default_asset_source_override` | character |  |
| `player_default_asset_source` | character |  |
| `player_default_asset_title` | character |  |
| `player_default_asset_description` | character |  |
| `player_default_asset_caption` | character |  |
| `player_default_asset_category` | character |  |
| `player_default_asset_alt_text` | character |  |
| `player_default_asset_height` | integer |  |
| `player_default_asset_width` | integer |  |
| `player_default_asset_asset_type` | character |  |
| `player_default_asset_file_system` | character |  |
| `player_default_asset_path` | character |  |
| `player_default_asset_type` | character |  |
| `player_default_asset_thumbnail` | character |  |
| `player_default_asset_duration` | integer |  |
| `player_default_asset_mime_type` | character |  |
| `player_position_abbreviation` | character |  |
| `player_height` | character | Participant height (e.g. "6' 5\""). |
| `player_weight` | integer | Participant weight in pounds. |
| `player_rating` | character |  |
| `player_roster_rating` | character |  |
| `player_commit_status_type` | character |  |
| `player_commit_status_short_term_signee` | logical |  |
| `player_commit_status_date` | character |  |
| `player_commit_status_committed_asset_key` | integer |  |
| `player_commit_status_committed_asset_url` | character |  |
| `player_commit_status_committed_asset_slug` | character |  |
| `player_commit_status_committed_asset_full_name` | character |  |
| `player_commit_status_committed_asset_res_key` | integer |  |
| `player_commit_status_committed_asset_res_domain_override` | character |  |
| `player_commit_status_committed_asset_res_domain` | character |  |
| `player_commit_status_committed_asset_res_source_override` | character |  |
| `player_commit_status_committed_asset_res_source` | character |  |
| `player_commit_status_committed_asset_res_title` | character |  |
| `player_commit_status_committed_asset_res_description` | character |  |
| `player_commit_status_committed_asset_res_caption` | character |  |
| `player_commit_status_committed_asset_res_category` | character |  |
| `player_commit_status_committed_asset_res_alt_text` | character |  |
| `player_commit_status_committed_asset_res_height` | integer |  |
| `player_commit_status_committed_asset_res_width` | integer |  |
| `player_commit_status_committed_asset_res_asset_type` | character |  |
| `player_commit_status_committed_asset_res_file_system` | character |  |
| `player_commit_status_committed_asset_res_path` | character |  |
| `player_commit_status_committed_asset_res_type` | character |  |
| `player_commit_status_committed_asset_res_thumbnail` | character |  |
| `player_commit_status_committed_asset_res_duration` | integer |  |
| `player_commit_status_committed_asset_res_mime_type` | character |  |
| `player_commit_status_transferred_asset_key` | integer |  |
| `player_commit_status_transferred_asset_url` | character |  |
| `player_commit_status_transferred_asset_slug` | character |  |
| `player_commit_status_transferred_asset_full_name` | character |  |
| `player_commit_status_transferred_asset_res_key` | integer |  |
| `player_commit_status_transferred_asset_res_domain_override` | character |  |
| `player_commit_status_transferred_asset_res_domain` | character |  |
| `player_commit_status_transferred_asset_res_source_override` | character |  |
| `player_commit_status_transferred_asset_res_source` | character |  |
| `player_commit_status_transferred_asset_res_title` | character |  |
| `player_commit_status_transferred_asset_res_description` | character |  |
| `player_commit_status_transferred_asset_res_caption` | character |  |
| `player_commit_status_transferred_asset_res_category` | character |  |
| `player_commit_status_transferred_asset_res_alt_text` | character |  |
| `player_commit_status_transferred_asset_res_height` | integer |  |
| `player_commit_status_transferred_asset_res_width` | integer |  |
| `player_commit_status_transferred_asset_res_asset_type` | character |  |
| `player_commit_status_transferred_asset_res_file_system` | character |  |
| `player_commit_status_transferred_asset_res_path` | character |  |
| `player_commit_status_transferred_asset_res_type` | character |  |
| `player_commit_status_transferred_asset_res_thumbnail` | character |  |
| `player_commit_status_transferred_asset_res_duration` | integer |  |
| `player_commit_status_transferred_asset_res_mime_type` | character |  |
| `player_commit_status_committed_organization_key` | integer |  |
| `player_commit_status_committed_organization_full_name` | character |  |
| `player_commit_status_committed_organization_name` | character |  |
| `player_commit_status_committed_organization_mascot` | character |  |
| `player_commit_status_committed_organization_abbreviation` | character |  |
| `player_commit_status_committed_organization_asset_url` | character |  |
| `player_commit_status_committed_organization_asset_key` | integer |  |
| `player_commit_status_committed_organization_asset_domain_override` | character |  |
| `player_commit_status_committed_organization_asset_domain` | character |  |
| `player_commit_status_committed_organization_asset_source_override` | character |  |
| `player_commit_status_committed_organization_asset_source` | character |  |
| `player_commit_status_committed_organization_asset_title` | character |  |
| `player_commit_status_committed_organization_asset_description` | character |  |
| `player_commit_status_committed_organization_asset_caption` | character |  |
| `player_commit_status_committed_organization_asset_category` | character |  |
| `player_commit_status_committed_organization_asset_alt_text` | character |  |
| `player_commit_status_committed_organization_asset_height` | integer |  |
| `player_commit_status_committed_organization_asset_width` | integer |  |
| `player_commit_status_committed_organization_asset_asset_type` | character |  |
| `player_commit_status_committed_organization_asset_file_system` | character |  |
| `player_commit_status_committed_organization_asset_path` | character |  |
| `player_commit_status_committed_organization_asset_type` | character |  |
| `player_commit_status_committed_organization_asset_thumbnail` | character |  |
| `player_commit_status_committed_organization_asset_duration` | integer |  |
| `player_commit_status_committed_organization_asset_mime_type` | character |  |
| `player_commit_status_committed_organization_slug` | character |  |
| `player_commit_status_committed_organization_primary_color` | character |  |
| `player_commit_status_class_rank` | character |  |
| `player_commit_status_transfer_entered` | character |  |
| `player_commit_status_recruitment_year` | character |  |
| `player_commit_status_decommitted_asset` | character |  |
| `player_commit_status_transfer` | logical |  |
| `player_commit_status_expected_to_transfer` | logical |  |
| `player_commit_status_recruitment_key` | integer |  |
| `player_commit_status_withdrawn_transfer` | logical |  |
| `player_commit_status_withdrawn_transfer_date` | character |  |
| `player_predictions` | character |  |
| `player_nil_status` | character |  |
| `player_nil_value` | integer |  |
| `player_sport` | character |  |
| `pick_organization_asset_url_key` | numeric |  |
| `pick_organization_asset_url_url` | character |  |
| `pick_organization_asset_url_slug` | character |  |
| `pick_organization_asset_url_full_name` | character |  |
| `pick_organization_full_name` | character |  |
| `pick_organization_key` | numeric |  |
| `pick_organization_name` | character |  |
| `pick_organization_mascot` | character |  |
| `pick_organization_abbreviation` | character |  |
| `pick_organization_asset_key` | numeric |  |
| `pick_organization_asset_domain_override` | character |  |
| `pick_organization_asset_domain` | character |  |
| `pick_organization_asset_source_override` | character |  |
| `pick_organization_asset_source` | character |  |
| `pick_organization_asset_title` | character |  |
| `pick_organization_asset_description` | character |  |
| `pick_organization_asset_caption` | character |  |
| `pick_organization_asset_category` | character |  |
| `pick_organization_asset_alt_text` | character |  |
| `pick_organization_asset_height` | numeric |  |
| `pick_organization_asset_width` | numeric |  |
| `pick_organization_asset_asset_type` | character |  |
| `pick_organization_asset_file_system` | character |  |
| `pick_organization_asset_path` | character |  |
| `pick_organization_asset_type` | character |  |
| `pick_organization_asset_thumbnail` | character |  |
| `pick_organization_asset_duration` | numeric |  |
| `pick_organization_asset_mime_type` | character |  |
| `pick_organization_slug` | character |  |
| `pick_organization_primary_color` | character |  |
| `pick_expert_key` | numeric |  |
| `pick_expert_name` | character |  |
| `pick_expert_nice_name` | character |  |
| `pick_expert_twitter_handle` | character |  |
| `pick_expert_instagram_handle` | character |  |
| `pick_expert_youtube_url` | character |  |
| `pick_expert_bio` | character |  |
| `pick_expert_job_title` | character |  |
| `pick_expert_site_affiliation` | character |  |
| `pick_expert_profile_picture` | character |  |
| `pick_expert_profile_picture_response_key` | numeric |  |
| `pick_expert_profile_picture_response_domain_override` | character |  |
| `pick_expert_profile_picture_response_domain` | character |  |
| `pick_expert_profile_picture_response_source_override` | character |  |
| `pick_expert_profile_picture_response_source` | character |  |
| `pick_expert_profile_picture_response_title` | character |  |
| `pick_expert_profile_picture_response_description` | character |  |
| `pick_expert_profile_picture_response_caption` | character |  |
| `pick_expert_profile_picture_response_category` | character |  |
| `pick_expert_profile_picture_response_alt_text` | character |  |
| `pick_expert_profile_picture_response_height` | character |  |
| `pick_expert_profile_picture_response_width` | character |  |
| `pick_expert_profile_picture_response_asset_type` | character |  |
| `pick_expert_profile_picture_response_file_system` | character |  |
| `pick_expert_profile_picture_response_path` | character |  |
| `pick_expert_profile_picture_response_type` | character |  |
| `pick_expert_profile_picture_response_thumbnail` | character |  |
| `pick_expert_profile_picture_response_duration` | numeric |  |
| `pick_expert_profile_picture_response_mime_type` | character |  |
| `player_high_school_key` | numeric |  |
| `player_high_school_full_name` | character |  |
| `player_high_school_name_2` | character |  |
| `player_high_school_known_as` | character |  |
| `player_high_school_mascot` | character |  |
| `player_high_school_abbreviation` | character |  |
| `player_high_school_asset_url` | character |  |
| `player_high_school_default_asset_key` | numeric |  |
| `player_high_school_default_asset_domain_override` | character |  |
| `player_high_school_default_asset_domain` | character |  |
| `player_high_school_default_asset_source_override` | character |  |
| `player_high_school_default_asset_source` | character |  |
| `player_high_school_default_asset_title` | character |  |
| `player_high_school_default_asset_description` | character |  |
| `player_high_school_default_asset_caption` | character |  |
| `player_high_school_default_asset_category` | character |  |
| `player_high_school_default_asset_alt_text` | character |  |
| `player_high_school_default_asset_height` | numeric |  |
| `player_high_school_default_asset_width` | numeric |  |
| `player_high_school_default_asset_asset_type` | character |  |
| `player_high_school_default_asset_file_system` | character |  |
| `player_high_school_default_asset_path` | character |  |
| `player_high_school_default_asset_type` | character |  |
| `player_high_school_default_asset_thumbnail` | character |  |
| `player_high_school_default_asset_duration` | numeric |  |
| `player_high_school_default_asset_mime_type` | character |  |
| `player_high_school_slug` | character |  |
| `player_high_school_primary_color` | character |  |
| `player_high_school_org_type` | character |  |
| `player_high_school_org_type_enum` | character |  |
| `player_high_school_division` | character |  |
| `player_high_school_site_keys` | character |  |
| `player_high_school_url_slug` | character |  |
| `player_rating_key` | numeric |  |
| `player_rating_rating` | numeric |  |
| `player_rating_stars` | numeric |  |
| `player_rating_national_rank` | numeric |  |
| `player_rating_position_rank` | numeric |  |
| `player_rating_state_rank` | numeric |  |
| `player_rating_position_abbr` | character |  |
| `player_rating_state_abbr` | character |  |
| `player_rating_five_star_plus` | character |  |

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
| `class_year` | integer | Recruiting class year of the recruitment. |
| `high_school` | character | High school |
| `home_town` | character | Player home town. |
| `rating_key` | integer |  |
| `rating_rating` | numeric |  |
| `rating_stars` | integer |  |
| `rating_national_rank` | integer |  |
| `rating_position_rank` | integer |  |
| `rating_state_rank` | integer |  |
| `rating_position_abbr` | character |  |
| `rating_state_abbr` | character |  |
| `rating_five_star_plus` | logical |  |
| `committed_status_type` | character |  |
| `committed_status_short_term_signee` | logical |  |
| `committed_status_date` | character |  |
| `committed_status_committed_asset_key` | integer |  |
| `committed_status_committed_asset_url` | character |  |
| `committed_status_committed_asset_slug` | character |  |
| `committed_status_committed_asset_full_name` | character |  |
| `committed_status_committed_asset_res_key` | integer |  |
| `committed_status_committed_asset_res_domain_override` | character |  |
| `committed_status_committed_asset_res_domain` | character |  |
| `committed_status_committed_asset_res_source_override` | character |  |
| `committed_status_committed_asset_res_source` | character |  |
| `committed_status_committed_asset_res_title` | character |  |
| `committed_status_committed_asset_res_description` | character |  |
| `committed_status_committed_asset_res_caption` | character |  |
| `committed_status_committed_asset_res_category` | character |  |
| `committed_status_committed_asset_res_alt_text` | character |  |
| `committed_status_committed_asset_res_height` | integer |  |
| `committed_status_committed_asset_res_width` | integer |  |
| `committed_status_committed_asset_res_asset_type` | character |  |
| `committed_status_committed_asset_res_file_system` | character |  |
| `committed_status_committed_asset_res_path` | character |  |
| `committed_status_committed_asset_res_type` | character |  |
| `committed_status_committed_asset_res_thumbnail` | character |  |
| `committed_status_committed_asset_res_duration` | integer |  |
| `committed_status_committed_asset_res_mime_type` | character |  |
| `committed_status_transferred_asset` | character |  |
| `committed_status_transferred_asset_res` | character |  |
| `committed_status_committed_organization_key` | integer |  |
| `committed_status_committed_organization_full_name` | character |  |
| `committed_status_committed_organization_name` | character |  |
| `committed_status_committed_organization_mascot` | character |  |
| `committed_status_committed_organization_abbreviation` | character |  |
| `committed_status_committed_organization_asset_url` | character |  |
| `committed_status_committed_organization_asset` | character |  |
| `committed_status_committed_organization_slug` | character |  |
| `committed_status_committed_organization_primary_color` | character |  |
| `committed_status_class_rank` | character |  |
| `committed_status_transfer_entered` | character |  |
| `committed_status_recruitment_year` | character |  |
| `committed_status_decommitted_asset` | character |  |
| `committed_status_transfer` | logical |  |
| `committed_status_expected_to_transfer` | logical |  |
| `committed_status_recruitment_key` | integer |  |
| `committed_status_withdrawn_transfer` | logical |  |
| `committed_status_withdrawn_transfer_date` | character |  |
| `high_school_org_key` | integer |  |
| `high_school_org_full_name` | character |  |
| `high_school_org_name` | character |  |
| `high_school_org_known_as` | character |  |
| `high_school_org_mascot` | character |  |
| `high_school_org_abbreviation` | character |  |
| `high_school_org_asset_url` | character |  |
| `high_school_org_default_asset_key` | integer |  |
| `high_school_org_default_asset_domain_override` | character |  |
| `high_school_org_default_asset_domain` | character |  |
| `high_school_org_default_asset_source_override` | character |  |
| `high_school_org_default_asset_source` | character |  |
| `high_school_org_default_asset_title` | character |  |
| `high_school_org_default_asset_description` | character |  |
| `high_school_org_default_asset_caption` | character |  |
| `high_school_org_default_asset_category` | character |  |
| `high_school_org_default_asset_alt_text` | character |  |
| `high_school_org_default_asset_height` | integer |  |
| `high_school_org_default_asset_width` | integer |  |
| `high_school_org_default_asset_asset_type` | character |  |
| `high_school_org_default_asset_file_system` | character |  |
| `high_school_org_default_asset_path` | character |  |
| `high_school_org_default_asset_type` | character |  |
| `high_school_org_default_asset_thumbnail` | character |  |
| `high_school_org_default_asset_duration` | integer |  |
| `high_school_org_default_asset_mime_type` | character |  |
| `high_school_org_slug` | character |  |
| `high_school_org_primary_color` | character |  |
| `high_school_org_org_type` | character |  |
| `high_school_org_org_type_enum` | character |  |
| `high_school_org_division` | character |  |
| `high_school_org_site_keys` | character |  |
| `high_school_org_url_slug` | character |  |

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

**`return_parsed=True`** (default) — the output of `parse_on3_rdb`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no committed capture with rows; names derived from the OpenAPI response type matched parse_on3_rdb's output on only 7 of the 9 checkable endpoints, so none are published.

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
