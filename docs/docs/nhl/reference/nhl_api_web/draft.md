---
title: "NHL — NHL Web API — Draft"
sidebar_label: "Draft"
sidebar_position: 2
description: "NHL — NHL Web API — Draft — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Web API — Draft

## nhl_draft_picks

Pull NHL draft picks for a year (and optionally one round).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/{year}/{round_}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/2024/all](https://api-web.nhle.com/v1/draft/picks/2024/all)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `round_` | `round_` |  |  | `Y` | round_ path parameter. |

### Returns {#nhl_draft_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `round` | integer | Shootout round number. |
| `pick_in_round` | integer | Pick number within the round. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `team_id` | integer | Unique team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo_light` | character | URL to the team logo (light variant). |
| `team_logo_dark` | character | URL to the team logo (dark variant). |
| `team_pick_history` | character | History of the team's picks at this slot. |
| `position_code` | character | Player position code. |
| `country_code` | character | Player country code. |
| `height` | integer | Player height in inches. |
| `weight` | integer | Player weight in pounds. |
| `amateur_league` | character | Amateur league the player played in. |
| `amateur_club_name` | character | Amateur club the player played for. |
| `team_name_default` | character | Team name (default locale). |
| `team_name_fr` | character | Team name (French locale). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `display_abbrev_default` | character | Short display abbreviation for the selected player's nationality or amateur league affiliation shown in the NHL draft picks listing. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |
| `team_common_name_fr` | character | Team common name (French localization). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_draft_picks-example}

```python
nhl_draft_picks(year=2024)
```

_Last validated n/a._

## nhl_draft_rankings

Pull NHL Central Scouting rankings for a draft year.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/{year}/{category}`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/2024/1](https://api-web.nhle.com/v1/draft/rankings/2024/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `category` | `category` |  |  | `Y` | category path parameter. |

### Returns {#nhl_draft_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `draft_year` | integer | Draft year the lottery applies to. |
| `category_id` | integer | Prospect category identifier. |
| `category_key` | character | Machine-readable slug identifying the scouting or ranking category (e.g., 'north-american-skater', 'international-skater') that the prospect belongs to in the NHL draft rankings. |
| `last_name` | character | Player last name. |
| `first_name` | character | Player first name. |
| `position_code` | character | Player position code. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `height_in_inches` | integer | Height in inches. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `last_amateur_club` | character | Prospect's most recent amateur club. |
| `last_amateur_league` | character | Prospect's most recent amateur league. |
| `birth_date` | character | Player birth date. |
| `birth_city` | character | Birth city. |
| `birth_state_province` | character | Birth state or province of the player. |
| `birth_country` | character | Player birth country. |
| `midterm_rank` | double | Prospect's midterm draft ranking. |
| `final_rank` | double | Prospect's final draft ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_draft_rankings-example}

```python
nhl_draft_rankings(year=2024)
```

_Last validated n/a._

## nhl_draft_picks_now

Pull the current / most recent draft pick set.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/picks/now](https://api-web.nhle.com/v1/draft/picks/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_draft_picks_now-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `round` | integer | Shootout round number. |
| `pick_in_round` | integer | Pick number within the round. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `team_id` | integer | Unique team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo_light` | character | URL to the team logo (light variant). |
| `team_logo_dark` | character | URL to the team logo (dark variant). |
| `team_pick_history` | character | History of the team's picks at this slot. |
| `position_code` | character | Player position code. |
| `country_code` | character | Player country code. |
| `height` | integer | Player height in inches. |
| `weight` | integer | Player weight in pounds. |
| `amateur_league` | character | Amateur league the player played in. |
| `amateur_club_name` | character | Amateur club the player played for. |
| `team_name_default` | character | Team name (default locale). |
| `team_name_fr` | character | Team name (French locale). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `display_abbrev_default` | character | Default-language display abbreviation for the team that currently holds this draft pick. |
| `first_name_default` | character | Player first name (default language). |
| `last_name_default` | character | Player last name (default language). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_draft_picks_now-example}

```python
nhl_draft_picks_now()
```

_Last validated n/a._

## nhl_draft_rankings_now

Pull the current Central Scouting rankings.

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft/rankings/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft/rankings/now](https://api-web.nhle.com/v1/draft/rankings/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_draft_rankings_now-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `draft_year` | integer | Draft year the lottery applies to. |
| `category_id` | integer | Prospect category identifier. |
| `category_key` | character | Short identifier string for the scouting category or ranking list under which the prospect is evaluated (e.g., 'NA-SKATER', 'GOALIE'). |
| `last_name` | character | Player last name. |
| `first_name` | character | Player first name. |
| `position_code` | character | Player position code. |
| `shoots_catches` | character | Handedness (shoots/catches). |
| `height_in_inches` | integer | Height in inches. |
| `weight_in_pounds` | integer | Weight in pounds. |
| `last_amateur_club` | character | Prospect's most recent amateur club. |
| `last_amateur_league` | character | Prospect's most recent amateur league. |
| `birth_date` | character | Player birth date. |
| `birth_city` | character | Birth city. |
| `birth_state_province` | character | Birth state or province of the player. |
| `birth_country` | character | Player birth country. |
| `midterm_rank` | double | Prospect's midterm draft ranking. |
| `final_rank` | double | Prospect's final draft ranking. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_draft_rankings_now-example}

```python
nhl_draft_rankings_now()
```

_Last validated n/a._

## nhl_draft_tracker_picks_now

Pull the live draft-tracker pick list (during the draft itself).

**Endpoint URL:** `GET https://api-web.nhle.com/v1/draft-tracker/picks/now`

**Valid URL:** [https://api-web.nhle.com/v1/draft-tracker/picks/now](https://api-web.nhle.com/v1/draft-tracker/picks/now)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_draft_tracker_picks_now-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `pick_in_round` | integer | Pick number within the round. |
| `overall_pick` | integer | Overall pick number in the draft. |
| `team_id` | integer | Unique team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_logo_light` | character | URL to the team logo (light variant). |
| `team_logo_dark` | character | URL to the team logo (dark variant). |
| `state` | character | Pick state (e.g., on the clock, complete). |
| `position_code` | character | Player position code. |
| `team_full_name_default` | character | Team full name (default language). |
| `team_full_name_fr` | character | Team full name (French). |
| `team_common_name_default` | character | Team common name (default language). |
| `team_place_name_with_preposition_default` | character | Team place name with preposition (default). |
| `team_place_name_with_preposition_fr` | character | Team place name with preposition (French). |
| `last_name_default` | character | Player last name (default language). |
| `first_name_default` | character | Player first name (default language). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_draft_tracker_picks_now-example}

```python
nhl_draft_tracker_picks_now()
```

_Last validated n/a._
