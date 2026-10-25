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
| `id` | integer | Unique player identifier. |
| `age_in_days` | character | Player age in days. |
| `age_in_days_for_year` | character | Player age in days for the draft year. |
| `age_in_years` | character | Player age in years. |
| `amateur_club_name` | character | Amateur club the player played for. |
| `amateur_league` | character | Amateur league the player played in. |
| `birth_date` | character | Player birth date. |
| `birth_place` | character | Player birth place. |
| `country_code` | character | Player country code. |
| `cs_player_id` | integer | Central Scouting player identifier. |
| `draft_date` | character | Date the player was drafted. |
| `draft_master_id` | integer | Draft master record identifier. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `drafted_by_team_id` | integer | Identifier of the drafting team. |
| `first_name` | character | Player first name. |
| `height` | character | Player height in inches. |
| `last_name` | character | Player last name. |
| `notes` | character | Notes flag for the pick. |
| `overall_pick_number` | integer | Overall pick number in the draft. |
| `pick_in_round` | integer | Pick number within the round. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `position` | character | Player position. |
| `removed_outright` | character | Removed-outright indicator. |
| `removed_outright_why` | character | Reason the pick was removed outright. |
| `round_number` | integer | Draft round number. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `supplemental_draft` | character | Supplemental draft indicator. |
| `team_pick_history` | character | History of the team's picks at this slot. |
| `tri_code` | character | Team three-letter code. |
| `weight` | character | Player weight in pounds. |

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

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `age_in_days` | integer | Player age in days. |
| `age_in_days_for_year` | integer | Player age in days for the draft year. |
| `age_in_years` | integer | Player age in years. |
| `amateur_club_name` | character | Amateur club the player played for. |
| `amateur_league` | character | Amateur league the player played in. |
| `birth_date` | character | Player birth date. |
| `birth_place` | character | Player birth place. |
| `country_code` | character | Player country code. |
| `cs_player_id` | integer | Central Scouting player identifier. |
| `draft_date` | character | Date the player was drafted. |
| `draft_master_id` | integer | Draft master record identifier. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `drafted_by_team_id` | integer | Identifier of the drafting team. |
| `first_name` | character | Player first name. |
| `height` | double | Player height in inches. |
| `last_name` | character | Player last name. |
| `notes` | character | Notes flag for the pick. |
| `overall_pick_number` | integer | Overall pick number in the draft. |
| `pick_in_round` | integer | Pick number within the round. |
| `player_id` | integer | Unique player identifier. |
| `player_name` | character | Player name. |
| `position` | character | Player position. |
| `removed_outright` | character | Removed-outright indicator. |
| `removed_outright_why` | character | Reason the pick was removed outright. |
| `round_number` | integer | Draft round number. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `supplemental_draft` | character | Supplemental draft indicator. |
| `team_pick_history` | character | History of the team's picks at this slot. |
| `tri_code` | character | Team three-letter code. |
| `weight` | double | Player weight in pounds. |

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

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `birth_city` | character | Birth city. |
| `birth_country3code` | character | Prospect birth country three-letter code. |
| `birth_date` | character | Player birth date. |
| `birth_state_prov_code` | character | Prospect birth state/province code. |
| `category_id` | integer | Prospect category identifier. |
| `created_on` | character | Date the trophy record was created. |
| `cs_player_id` | integer | Central Scouting player identifier. |
| `draft_status_code` | character | Draft eligibility status code. |
| `ep_player_id` | integer | EliteProspects player identifier. |
| `first_name` | character | Player first name. |
| `headshot_id` | integer | Headshot image identifier. |
| `height` | integer | Player height in inches. |
| `hometown` | character | Prospect hometown. |
| `last_club_name` | character | Most recent club name. |
| `last_league_abbr` | character | Most recent league abbreviation. |
| `last_name` | character | Player last name. |
| `nationality_code` | character | Nationality code of the official. |
| `news_articles` | character | Associated news articles. |
| `playerid` | integer | Unique player identifier. |
| `position_desc` | character | Player position description. |
| `profile` | character | Prospect profile text. |
| `quotes` | character | Quotes about the prospect. |
| `scouting_report` | character | Scouting report text. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `stats_text` | character | Statistical summary text. |
| `video` | character | Associated video content. |
| `weight` | integer | Player weight in pounds. |

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

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `format_content` | character | Description of the lottery format. |
| `odds_content` | character | Description of the lottery odds. |
| `result_notes` | character | Notes on the lottery results. |

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

_Last validated n/a._
