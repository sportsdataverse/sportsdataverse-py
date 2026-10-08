---
title: "MLB — MLB Stats API — All"
sidebar_label: "All"
sidebar_position: 1
description: "MLB — MLB Stats API — All — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — All

## mlb_all_star_ballot

View All-Star Ballots per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarBallot`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarBallot?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarBallot?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_all_star_ballot-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `middle_name` | character | Player middle name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `name_matrilineal` | character | Maternal family name. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `last_played_date` | character | Date of last MLB game played. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `birth_state_province` | character | State or province of birth. |
| `draft_year` | double | Year the player was drafted. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_all_star_ballot-example}

```python
mlb_all_star_ballot(league_id='103', season='2023')
```

_Last validated n/a._

## mlb_all_star_write_ins

View All-Star Write-ins per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarWriteIns`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarWriteIns?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_all_star_write_ins-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_state_province` | character | State or province of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `primary_number` | character | Player uniform number. |
| `draft_year` | double | Year the player was drafted. |
| `middle_name` | character | Player middle name. |
| `name_matrilineal` | character | Maternal family name. |
| `last_played_date` | character | Date of last MLB game played. |
| `nick_name` | character | Player nickname. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_all_star_write_ins-example}

```python
mlb_all_star_write_ins(league_id='103', season='2023')
```

_Last validated n/a._

## mlb_all_star_final_vote

View All-Star Final Vote per league.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/league/{league_id}/allStarFinalVote`

**Valid URL:** [https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote?season=2023](https://statsapi.mlb.com/api/v1/league/103/allStarFinalVote?season=2023)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league_id` | `league_id` |  | `Y` |  | league_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_all_star_final_vote-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `full_name` | character | Player's full name. |
| `link` | character | API link to the game feed. |
| `first_name` | character | Player first name. |
| `last_name` | character | Player last name. |
| `primary_number` | character | Player uniform number. |
| `birth_date` | character | Date of birth (YYYY-MM-DD). |
| `current_age` | integer | Current age in years. |
| `birth_city` | character | City of birth. |
| `birth_country` | character | Country of birth. |
| `height` | character | Height (feet and inches). |
| `weight` | integer | Weight in pounds. |
| `active` | logical | Whether the player is currently active. |
| `use_name` | character | Preferred first name. |
| `use_last_name` | character | Preferred last name. |
| `boxscore_name` | character | Name as shown in box scores. |
| `nick_name` | character | Player nickname. |
| `gender` | character | Player gender. |
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `mlb_debut_date` | character | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | character | Name in first-last order. |
| `name_slug` | character | URL-friendly name slug. |
| `first_last_name` | character | First and last name. |
| `last_first_name` | character | Name in last, first order. |
| `last_init_name` | character | Last name with first initial. |
| `init_last_name` | character | First initial with last name. |
| `full_fml_name` | character | Full name (first-middle-last). |
| `full_lfm_name` | character | Full name (last-first-middle). |
| `strike_zone_top` | double | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | double | Bottom of the player's strike zone (feet). |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `name_matrilineal` | character | Maternal family name. |
| `birth_state_province` | character | State or province of birth. |
| `name_title` | character | Name title. |
| `name_suffix` | character | Name suffix (e.g. Jr., Sr., III). |
| `middle_name` | character | Player middle name. |
| `draft_year` | double | Year the player was drafted. |
| `last_played_date` | character | Date of last MLB game played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_all_star_final_vote-example}

```python
mlb_all_star_final_vote(league_id='103', season='2023')
```

_Last validated n/a._
