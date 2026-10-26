---
title: "NHL — NHL Records API — Draft"
sidebar_label: "Draft"
sidebar_position: 3
description: "NHL — NHL Records API — Draft — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Records API — Draft

## nhl_records_draft

Retrieve NHL Entry Draft picks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft/{draft_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft](https://records.nhl.com/site/api/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `draft_id` | `draft_id` |  |  | `Y` | draft_id path parameter. |

### Returns {#nhl_records_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `age_in_days` | String | Player age in days. |
| `age_in_days_for_year` | String | Player age in days for the draft year. |
| `age_in_years` | String | Player age in years. |
| `amateur_club_name` | String | Amateur club the player played for. |
| `amateur_league` | String | Amateur league the player played in. |
| `birth_date` | String | Player birth date. |
| `birth_place` | String | Player birth place. |
| `country_code` | String | Player country code. |
| `cs_player_id` | Int64 | Central Scouting player identifier. |
| `draft_date` | String | Date the player was drafted. |
| `draft_master_id` | Int64 | Draft master record identifier. |
| `draft_year` | Int64 | Draft year the lottery applies to. |
| `drafted_by_team_id` | Int64 | Identifier of the drafting team. |
| `first_name` | String | Player first name. |
| `height` | String | Player height in inches. |
| `last_name` | String | Player last name. |
| `notes` | String | Notes flag for the pick. |
| `overall_pick_number` | Int64 | Overall pick number in the draft. |
| `pick_in_round` | Int64 | Pick number within the round. |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `position` | String | Player position. |
| `removed_outright` | String | Removed-outright indicator. |
| `removed_outright_why` | String | Reason the pick was removed outright. |
| `round_number` | Int64 | Draft round number. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `supplemental_draft` | String | Supplemental draft indicator. |
| `team_pick_history` | String | History of the team's picks at this slot. |
| `tri_code` | String | Team three-letter code. |
| `weight` | String | Player weight in pounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_draft-example}

```python
nhl_records_draft()
```

## nhl_records_draft_by_team

All draft picks made by a single team.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft/byTeam/{team_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft/byTeam/10](https://records.nhl.com/site/api/draft/byTeam/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#nhl_records_draft_by_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `age_in_days` | Int64 | Player age in days. |
| `age_in_days_for_year` | Int64 | Player age in days for the draft year. |
| `age_in_years` | Int64 | Player age in years. |
| `amateur_club_name` | String | Amateur club the player played for. |
| `amateur_league` | String | Amateur league the player played in. |
| `birth_date` | String | Player birth date. |
| `birth_place` | String | Player birth place. |
| `country_code` | String | Player country code. |
| `cs_player_id` | Int64 | Central Scouting player identifier. |
| `draft_date` | String | Date the player was drafted. |
| `draft_master_id` | Int64 | Draft master record identifier. |
| `draft_year` | Int64 | Draft year the lottery applies to. |
| `drafted_by_team_id` | Int64 | Identifier of the drafting team. |
| `first_name` | String | Player first name. |
| `height` | Float64 | Player height in inches. |
| `last_name` | String | Player last name. |
| `notes` | String | Notes flag for the pick. |
| `overall_pick_number` | Int64 | Overall pick number in the draft. |
| `pick_in_round` | Int64 | Pick number within the round. |
| `player_id` | Int64 | Unique player identifier. |
| `player_name` | String | Player name. |
| `position` | String | Player position. |
| `removed_outright` | String | Removed-outright indicator. |
| `removed_outright_why` | String | Reason the pick was removed outright. |
| `round_number` | Int64 | Draft round number. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `supplemental_draft` | String | Supplemental draft indicator. |
| `team_pick_history` | String | History of the team's picks at this slot. |
| `tri_code` | String | Team three-letter code. |
| `weight` | Float64 | Player weight in pounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_draft_by_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_draft_by_team-example}

```python
nhl_records_draft_by_team(team_id=10)
```

## nhl_records_draft_prospect

Draft prospect records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft-prospect/{prospect_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft-prospect](https://records.nhl.com/site/api/draft-prospect)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `prospect_id` | `prospect_id` |  |  | `Y` | prospect_id path parameter. |

### Returns {#nhl_records_draft_prospect-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `birth_city` | String | Birth city. |
| `birth_country3code` | String | Prospect birth country three-letter code. |
| `birth_date` | String | Player birth date. |
| `birth_state_prov_code` | String | Prospect birth state/province code. |
| `category_id` | Int64 | Prospect category identifier. |
| `created_on` | String | Date the trophy record was created. |
| `cs_player_id` | Int64 | Central Scouting player identifier. |
| `draft_status_code` | String | Draft eligibility status code. |
| `ep_player_id` | Int64 | EliteProspects player identifier. |
| `first_name` | String | Player first name. |
| `headshot_id` | Int64 | Headshot image identifier. |
| `height` | Int64 | Player height in inches. |
| `hometown` | String | Prospect hometown. |
| `last_club_name` | String | Most recent club name. |
| `last_league_abbr` | String | Most recent league abbreviation. |
| `last_name` | String | Player last name. |
| `nationality_code` | String | Nationality code of the official. |
| `news_articles` | String | Associated news articles. |
| `playerid` | Int64 | Unique player identifier. |
| `position_desc` | String | Player position description. |
| `profile` | String | Prospect profile text. |
| `quotes` | String | Quotes about the prospect. |
| `scouting_report` | String | Scouting report text. |
| `shoots_catches` | String | Handedness (shoots/catches). |
| `stats_text` | String | Statistical summary text. |
| `video` | String | Associated video content. |
| `weight` | Int64 | Player weight in pounds. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_draft_prospect-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_draft_prospect-example}

```python
nhl_records_draft_prospect()
```

## nhl_records_draft_lottery_odds

Draft lottery odds (current year or filtered by season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft-lottery-odds`

**Valid URL:** [https://records.nhl.com/site/api/draft-lottery-odds](https://records.nhl.com/site/api/draft-lottery-odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_draft_lottery_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `draft_year` | Int64 | Draft year the lottery applies to. |
| `format_content` | String | Description of the lottery format. |
| `odds_content` | String | Description of the lottery odds. |
| `result_notes` | String | Notes on the lottery results. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_draft_lottery_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_draft_lottery_odds-example}

```python
nhl_records_draft_lottery_odds()
```
