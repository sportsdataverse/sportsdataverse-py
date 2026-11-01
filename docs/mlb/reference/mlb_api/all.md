# MLB — MLB Stats API — All

> MLB — MLB Stats API — All — function reference in sdv-py, the SportsDataverse Python package.

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
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `primary_number` | String | Player uniform number. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `middle_name` | String | Player middle name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `nick_name` | String | Player nickname. |
| `gender` | String | Player gender. |
| `name_matrilineal` | String | Maternal family name. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `last_played_date` | String | Date of last MLB game played. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `primary_position_code` | String | Primary position code. |
| `primary_position_name` | String | Primary fielding position name. |
| `primary_position_type` | String | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | String | Primary position abbreviation. |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |
| `birth_state_province` | String | State or province of birth. |
| `draft_year` | Float64 | Year the player was drafted. |
| `name_title` | String | Name title. |
| `name_suffix` | String | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_all_star_ballot-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_all_star_ballot-example}

```python
mlb_all_star_ballot(league_id='103', season='2023')
```

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
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_state_province` | String | State or province of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `gender` | String | Player gender. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |
| `primary_number` | String | Player uniform number. |
| `draft_year` | Float64 | Year the player was drafted. |
| `middle_name` | String | Player middle name. |
| `name_matrilineal` | String | Maternal family name. |
| `last_played_date` | String | Date of last MLB game played. |
| `nick_name` | String | Player nickname. |
| `name_title` | String | Name title. |
| `name_suffix` | String | Name suffix (e.g. Jr., Sr., III). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_all_star_write_ins-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_all_star_write_ins-example}

```python
mlb_all_star_write_ins(league_id='103', season='2023')
```

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
| `id` | Int64 | Id. |
| `full_name` | String | Player's full name. |
| `link` | String | API link to the game feed. |
| `first_name` | String | Player first name. |
| `last_name` | String | Player last name. |
| `primary_number` | String | Player uniform number. |
| `birth_date` | String | Date of birth (YYYY-MM-DD). |
| `current_age` | Int64 | Current age in years. |
| `birth_city` | String | City of birth. |
| `birth_country` | String | Country of birth. |
| `height` | String | Height (feet and inches). |
| `weight` | Int64 | Weight in pounds. |
| `active` | Boolean | Whether the player is currently active. |
| `use_name` | String | Preferred first name. |
| `use_last_name` | String | Preferred last name. |
| `boxscore_name` | String | Name as shown in box scores. |
| `nick_name` | String | Player nickname. |
| `gender` | String | Player gender. |
| `is_player` | Boolean | Whether the person is a player. |
| `is_verified` | Boolean | Whether the player profile is verified. |
| `pronunciation` | String | Phonetic name pronunciation. |
| `mlb_debut_date` | String | MLB debut date (YYYY-MM-DD). |
| `name_first_last` | String | Name in first-last order. |
| `name_slug` | String | URL-friendly name slug. |
| `first_last_name` | String | First and last name. |
| `last_first_name` | String | Name in last, first order. |
| `last_init_name` | String | Last name with first initial. |
| `init_last_name` | String | First initial with last name. |
| `full_fml_name` | String | Full name (first-middle-last). |
| `full_lfm_name` | String | Full name (last-first-middle). |
| `strike_zone_top` | Float64 | Top of the player's strike zone (feet). |
| `strike_zone_bottom` | Float64 | Bottom of the player's strike zone (feet). |
| `primary_position_code` | String | Primary position code. |
| `primary_position_name` | String | Primary fielding position name. |
| `primary_position_type` | String | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | String | Primary position abbreviation. |
| `bat_side_code` | String | Batting side code (L/R/S). |
| `bat_side_description` | String | Batting side description. |
| `pitch_hand_code` | String | Throwing hand code (L/R). |
| `pitch_hand_description` | String | Throwing hand description. |
| `name_matrilineal` | String | Maternal family name. |
| `birth_state_province` | String | State or province of birth. |
| `name_title` | String | Name title. |
| `name_suffix` | String | Name suffix (e.g. Jr., Sr., III). |
| `middle_name` | String | Player middle name. |
| `draft_year` | Float64 | Year the player was drafted. |
| `last_played_date` | String | Date of last MLB game played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_all_star_final_vote-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_all_star_final_vote-example}

```python
mlb_all_star_final_vote(league_id='103', season='2023')
```
