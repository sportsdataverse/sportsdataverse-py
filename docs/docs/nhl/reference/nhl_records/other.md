---
title: "NHL — NHL Records API — Other"
sidebar_label: "Other"
sidebar_position: 6
description: "NHL — NHL Records API — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NHL — NHL Records API — Other

## nhl_records_awards

List all NHL award / trophy records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details`

**Valid URL:** [https://records.nhl.com/site/api/award-details](https://records.nhl.com/site/api/award-details)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `awarded_posthumously` | Boolean | Whether the award was given posthumously. |
| `coach_id` | Int64 | ESPN coach id parsed from the `$ref` URL. |
| `created_on` | String | Date the trophy record was created. |
| `detail_summary` | String | Detail summary flag. |
| `full_name` | String | Player full name. |
| `general_manager_id` | Int64 | General manager identifier, if applicable. |
| `image_url` | String | Player headshot URL. |
| `is_rookie` | Boolean | Whether the player is a rookie. |
| `player_id` | Int64 | Unique player identifier. |
| `player_image_caption` | String | Player image caption flag. |
| `player_image_url` | String | URL to the player image. |
| `season_id` | Int64 | Season identifier. |
| `status` | String | Status string (e.g. captain markers). |
| `summary` | String | Record summary string (e.g. "25-15-10"). |
| `team_id` | Int64 | Unique team identifier. |
| `trophy_category_id` | Int64 | Trophy category identifier. |
| `trophy_id` | Int64 | Trophy identifier. |
| `value` | String | Leader stat numeric value. |
| `vote_count` | Float64 | Number of votes received. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_awards-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_awards-example}

```python
nhl_records_awards()
```

## nhl_records_awards_by_franchise

List award records for a single franchise.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details/{franchise_id}`

**Valid URL:** [https://records.nhl.com/site/api/award-details/1](https://records.nhl.com/site/api/award-details/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#nhl_records_awards_by_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_awards_by_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_awards_by_franchise-example}

```python
nhl_records_awards_by_franchise(franchise_id=1)
```

## nhl_records_awards_trophy_season

Retrieve the trophy winner for a specific season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details/trophy/{trophy_id}/season/{season_id}`

**Valid URL:** [https://records.nhl.com/site/api/award-details/trophy/1/season/X](https://records.nhl.com/site/api/award-details/trophy/1/season/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `trophy_id` | `trophy_id` |  | `Y` |  | trophy_id path parameter. |
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |

### Returns {#nhl_records_awards_trophy_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `awarded_posthumously` | Boolean | Whether the award was given posthumously. |
| `coach_id` | Int64 | ESPN coach id parsed from the `$ref` URL. |
| `created_on` | String | Date the trophy record was created. |
| `detail_summary` | String | Detail summary flag. |
| `full_name` | String | Player full name. |
| `general_manager_id` | Int64 | General manager identifier, if applicable. |
| `image_url` | String | Player headshot URL. |
| `is_rookie` | Boolean | Whether the player is a rookie. |
| `player_id` | Int64 | Unique player identifier. |
| `player_image_caption` | String | Player image caption flag. |
| `player_image_url` | String | URL to the player image. |
| `season_id` | Int64 | Season identifier. |
| `status` | String | Status string (e.g. captain markers). |
| `summary` | String | Record summary string (e.g. "25-15-10"). |
| `team_id` | Int64 | Unique team identifier. |
| `trophy_category_id` | Int64 | Trophy category identifier. |
| `trophy_id` | Int64 | Trophy identifier. |
| `value` | String | Leader stat numeric value. |
| `vote_count` | Int64 | Number of votes received. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_awards_trophy_season-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_awards_trophy_season-example}

```python
nhl_records_awards_trophy_season(trophy_id=1, season_id='X')
```

## nhl_records_coaches

List NHL head coaches.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach`

**Valid URL:** [https://records.nhl.com/site/api/coach](https://records.nhl.com/site/api/coach)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_coaches-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `bio` | String | Long-form biographical narrative for the coach, as provided by the NHL api-web endpoint. |
| `birth_city` | String | Birth city. |
| `birth_country3code` | String | Prospect birth country three-letter code. |
| `birth_date` | String | Player birth date. |
| `birth_state_province_code` | String | Two-letter state or province code of the coach's birth location (e.g., 'ON' for Ontario, 'MI' for Michigan). |
| `brief_description` | String | Brief description of the trophy. |
| `date_of_death` | String | Date of death, if applicable. |
| `deceased` | Boolean | Whether the player is deceased. |
| `description` | String | Full text description of the event. |
| `featured_image` | String | URL of the coach's featured promotional or profile image on the NHL platform. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `history` | String |  |
| `hockey_hof_link` | String | URL to the coach's Hockey Hall of Fame profile page, if they are an inductee. |
| `in_hockey_hof` | Boolean | Whether the player is in the Hockey Hall of Fame. |
| `in_iihf_hockey_hof` | Boolean | Boolean flag indicating whether the coach is inducted into the IIHF Hockey Hall of Fame. |
| `in_us_hockey_hof` | Boolean | Whether the player is in the US Hockey Hall of Fame. |
| `instagram` | String | Instagram handle or profile URL for the coach's official social media presence. |
| `is_active` | Boolean | Whether the team is active. |
| `last_name` | String | Player last name. |
| `nationality_code` | String | Nationality code of the official. |
| `player_id` | Int64 | Unique player identifier. |
| `stanley_cup` | Float64 | Number of Stanley Cup championships won by the coach as a head coach or assistant coach. |
| `team_id` | Int64 | Unique team identifier. |
| `top100_player_link` | String | URL to the coach's NHL Top 100 players recognition page, if applicable. |
| `twitter` | String | Twitter/X handle or profile URL for the coach's official social media presence. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_coaches-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_coaches-example}

```python
nhl_records_coaches()
```

## nhl_records_coach

Retrieve one coach by their numeric ID.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach/X](https://records.nhl.com/site/api/coach/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#nhl_records_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `bio` | String | Free-text biographical summary of the coach's career and background. |
| `birth_city` | String | Birth city. |
| `birth_country3code` | String | Prospect birth country three-letter code. |
| `birth_date` | String | Player birth date. |
| `birth_state_province_code` | String | Two-letter state or province code indicating the coach's place of birth. |
| `brief_description` | String | Brief description of the trophy. |
| `date_of_death` | String | Date of death, if applicable. |
| `deceased` | Boolean | Whether the player is deceased. |
| `description` | String | Full text description of the event. |
| `featured_image` | String | URL of the featured promotional image associated with the coach's NHL profile. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `history` | String |  |
| `hockey_hof_link` | String | URL to the coach's page on the Hockey Hall of Fame website, if inducted. |
| `in_hockey_hof` | Boolean | Whether the player is in the Hockey Hall of Fame. |
| `in_iihf_hockey_hof` | Boolean | Boolean flag indicating whether the coach is inducted into the IIHF Hockey Hall of Fame. |
| `in_us_hockey_hof` | Boolean | Whether the player is in the US Hockey Hall of Fame. |
| `instagram` | String | Instagram profile handle or URL associated with the coach. |
| `is_active` | Boolean | Whether the team is active. |
| `last_name` | String | Player last name. |
| `nationality_code` | String | Nationality code of the official. |
| `player_id` | Int64 | Unique player identifier. |
| `stanley_cup` | Int64 | Number of Stanley Cup championships won by the coach as a head coach. |
| `team_id` | Int64 | Unique team identifier. |
| `top100_player_link` | String | URL to the coach's entry on the NHL's Top 100 Players list, if applicable. |
| `twitter` | String | Twitter (X) handle or URL associated with the coach. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_coach-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_coach-example}

```python
nhl_records_coach(coach_id='X')
```

## nhl_records_all_time_record_vs_franchise

All-time head-to-head records between every franchise pairing.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-time-record-vs-franchise`

**Valid URL:** [https://records.nhl.com/site/api/all-time-record-vs-franchise](https://records.nhl.com/site/api/all-time-record-vs-franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_all_time_record_vs_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_franchise` | Int64 | Indicator of whether the franchise is active. |
| `active_opponent_franchise` | Int64 | Flag indicating whether the opponent franchise is currently active in the NHL (1 = active, 0 = relocated or dissolved). |
| `franchise_name` | String | Franchise name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `home_games_played` | Int64 | Total number of home games the franchise has played all-time against this opponent franchise. |
| `home_goals_against` | Float64 | Total goals allowed by the franchise in all-time home games against this opponent. |
| `home_goals_for` | Float64 | Total goals scored by the franchise in all-time home games against this opponent. |
| `home_last_meeting_season_id` | Int64 | NHL season identifier for the most recent home game played against this opponent franchise. |
| `home_losses` | Int64 | Losses at home. |
| `home_ot_losses` | Int64 | Home overtime losses. |
| `home_points` | Int64 |  |
| `home_ties` | Int64 | Ties at home. |
| `home_wins` | Int64 | Wins at home. |
| `opponent_franchise_id` | Int64 | NHL records identifier for the opposing franchise in this all-time head-to-head record. |
| `opponent_franchise_name` | String | Full name of the opposing franchise in this all-time head-to-head record. |
| `opponent_team_id` | Int64 | Opponent team identifier. |
| `road_games_played` | Int64 | Total number of road games the franchise has played all-time against this opponent franchise. |
| `road_goals_against` | Int64 | Total goals allowed by the franchise in all-time road games against this opponent. |
| `road_goals_for` | Int64 | Total goals scored by the franchise in all-time road games against this opponent. |
| `road_last_meeting_season_id` | Int64 | NHL season identifier for the most recent road game played against this opponent franchise. |
| `road_losses` | Int64 | Losses on the road. |
| `road_ot_losses` | Int64 | Road overtime losses. |
| `road_points` | Int64 | Total standings points earned by the franchise in all-time road games against this opponent. |
| `road_ties` | Int64 | Ties on the road. |
| `road_wins` | Int64 | Wins on the road. |
| `team_franchise_id` | Int64 | Team franchise identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `total_games_played` | Int64 | Total number of games played all-time between this franchise and the opponent franchise across home and road venues. |
| `total_goals_against` | Int64 | Total goals allowed by the franchise in all-time games against this opponent across home and road. |
| `total_goals_for` | Int64 | Total goals scored by the franchise in all-time games against this opponent across home and road. |
| `total_last_meeting_season_id` | Int64 | NHL season identifier for the most recent game played between the two franchises in any venue. |
| `total_losses` | Int64 | Total losses to date (goalie). |
| `total_ot_losses` | Int64 | Total number of overtime losses accumulated by the franchise all-time against this opponent. |
| `total_points` | Int64 | Total standings points earned by the franchise across all all-time games against this opponent. |
| `total_ties` | Int64 |  |
| `total_wins` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_all_time_record_vs_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_all_time_record_vs_franchise-example}

```python
nhl_records_all_time_record_vs_franchise()
```

## nhl_records_skater_career_stats

Skater career statistics (all-time, regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-statistics`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-statistics](https://records.nhl.com/site/api/skater-career-statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_skater_career_stats-returns}

**`return_parsed=True`** (default) — the output of `parse_nhl_records`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: records.nhl.com answers 404 for this route with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_skater_career_stats-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_skater_career_stats-example}

```python
nhl_records_skater_career_stats()
```

## nhl_records_skater_career_leaders

All-time skater career leaderboards.

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-leaders`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-leaders](https://records.nhl.com/site/api/skater-career-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_skater_career_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_nhl_records`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: records.nhl.com answers 404 for this route with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_skater_career_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_skater_career_leaders-example}

```python
nhl_records_skater_career_leaders()
```

## nhl_records_consecutive_100pt_seasons

Skaters with the most consecutive 100-point seasons.

**Endpoint URL:** `GET https://records.nhl.com/site/api/consecutive-100-point-seasons`

**Valid URL:** [https://records.nhl.com/site/api/consecutive-100-point-seasons](https://records.nhl.com/site/api/consecutive-100-point-seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_consecutive_100pt_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_player` | Boolean | Indicator of whether the player is active. |
| `active_streak` | Boolean | Indicator of whether the streak is active. |
| `consecutive100_point_seasons` | Int64 | Number of consecutive NHL regular seasons in which the player reached 100 or more points. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `last_name` | String | Player last name. |
| `player_id` | Int64 | Unique player identifier. |
| `position_code` | String | Player position code. |
| `seasons_played` | Int64 | Number of seasons played. |
| `streak_end_season` | Int64 | The last season of the consecutive 100-point streak, encoded as an eight-digit season ID. |
| `streak_start_season` | Int64 | The first season of the consecutive 100-point streak, encoded as an eight-digit season ID. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `team_names` | String | Team names. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_consecutive_100pt_seasons-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_consecutive_100pt_seasons-example}

```python
nhl_records_consecutive_100pt_seasons()
```

## nhl_records_expansion_draft_picks

Expansion draft picks (e.g. Vegas 2017, Seattle 2021).

**Endpoint URL:** `GET https://records.nhl.com/site/api/expansion-draft-picks`

**Valid URL:** [https://records.nhl.com/site/api/expansion-draft-picks](https://records.nhl.com/site/api/expansion-draft-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_expansion_draft_picks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active` | Boolean | Whether athlete is currently active. |
| `draft_picks` | String | JSON-serialized list of players selected by this franchise in the NHL expansion draft. |
| `season_id` | Int64 | Season identifier. |
| `team_id` | Int64 | Unique team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_expansion_draft_picks-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_expansion_draft_picks-example}

```python
nhl_records_expansion_draft_picks()
```

## nhl_records_attendance

NHL arena attendance records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/attendance`

**Valid URL:** [https://records.nhl.com/site/api/attendance](https://records.nhl.com/site/api/attendance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_attendance-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `playoff_attendance` | Float64 | Total playoff attendance. |
| `regular_attendance` | Float64 | Total regular-season attendance. |
| `season_id` | Int64 | Season identifier. |
| `total_attendance` | Float64 | Total attendance for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_attendance-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_attendance-example}

```python
nhl_records_attendance()
```

## nhl_records_hof_players

Hockey Hall of Fame player inductees.

**Endpoint URL:** `GET https://records.nhl.com/site/api/hof/players`

**Valid URL:** [https://records.nhl.com/site/api/hof/players](https://records.nhl.com/site/api/hof/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_hof_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `date_inducted` | String | Date the inductee entered the Hall of Fame. |
| `induction_cat_id` | Int64 | Induction category identifier. |
| `misc_full_name` | String | Full name of the inductee. |
| `office_id` | Int64 | Office/category identifier. |
| `official_id` | Int64 | ESPN official id (echoed from arg). |
| `player_id` | Int64 | Unique player identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_hof_players-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_hof_players-example}

```python
nhl_records_hof_players()
```

## nhl_records_hof_players_by_office

Hall of Fame players for a specific induction office/category.

**Endpoint URL:** `GET https://records.nhl.com/site/api/hof/players/{office_id}`

**Valid URL:** [https://records.nhl.com/site/api/hof/players/X](https://records.nhl.com/site/api/hof/players/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `office_id` | `office_id` |  | `Y` |  | office_id path parameter. |

### Returns {#nhl_records_hof_players_by_office-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `date_inducted` | String | Date the inductee entered the Hall of Fame. |
| `induction_cat_id` | Int64 | Induction category identifier. |
| `misc_full_name` | String | Full name of the inductee. |
| `office_id` | Int64 | Office/category identifier. |
| `official_id` | Int64 | ESPN official id (echoed from arg). |
| `player_id` | Int64 | Unique player identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_hof_players_by_office-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_hof_players_by_office-example}

```python
nhl_records_hof_players_by_office(office_id='X')
```

## nhl_records_gm_career

General Manager career records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/general-manager/{gm_id}`

**Valid URL:** [https://records.nhl.com/site/api/general-manager-career-records](https://records.nhl.com/site/api/general-manager-career-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gm_id` | `gm_id` |  |  | `Y` | gm_id path parameter. |

### Returns {#nhl_records_gm_career-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_gm` | Boolean | Indicates whether the general manager is currently active in an NHL front-office role. |
| `end_date` | String | Season end date. |
| `end_season_id` | Int64 | Season identifier (e.g., 20232024) for the last season the GM held the position. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player full name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games` | Int64 | Games played. |
| `gm_of_the_year` | Int64 | Number of times the general manager won the NHL GM of the Year Award during their career. |
| `home_games` | Int64 |  |
| `home_losses` | Int64 | Losses at home. |
| `home_ot_losses` | Float64 | Home overtime losses. |
| `home_ties` | Float64 | Ties at home. |
| `home_wins` | Int64 | Wins at home. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `losses_in_ot` | Int64 | Number of games the GM's team lost in overtime during their tenure. |
| `losses_in_ot_plus_shootout` | Int64 | Combined total of overtime and shootout losses recorded during the GM's tenure. |
| `losses_in_shootout` | Float64 | Number of games the GM's team lost in the shootout portion of a tied game. |
| `overtime_losses` | Int64 | Total overtime losses. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `road_games` | Int64 | Total number of away games played by the GM's team across their tenure. |
| `road_losses` | Int64 | Losses on the road. |
| `road_ot_losses` | Float64 | Road overtime losses. |
| `road_ties` | Float64 | Ties on the road. |
| `road_wins` | Int64 | Wins on the road. |
| `seasons` | Int64 | Total number of NHL seasons the general manager has served in the role. |
| `stanley_cup_final_appearances` | Int64 | Number of times the GM's team reached the Stanley Cup Final during their tenure. |
| `stanley_cups` | Int64 | Number of Stanley Cup championships won by the GM's franchise during their tenure. |
| `start_date` | String | Season start date. |
| `start_season_id` | Int64 | Season identifier (e.g., 20052006) for the first season the GM held the position. |
| `team_abbrevs` | String | Team abbreviation(s). |
| `ties` | Int64 | Total ties. |
| `ties_in_ot` | Int64 | Number of overtime ties recorded under legacy rules during the GM's tenure. |
| `win_pctg` | Float64 | Career winning percentage for the GM, calculated as wins divided by total games decided. |
| `wins` | Int64 | Wins. |
| `wins_in_ot` | Int64 | Number of games the GM's team won in overtime during their tenure. |
| `wins_in_ot_plus_shootout` | Int64 | Combined total of overtime and shootout wins recorded during the GM's tenure. |
| `wins_in_shootout` | Float64 | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_gm_career-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_gm_career-example}

```python
nhl_records_gm_career()
```

## nhl_records_gm_franchise

General Manager records scoped to franchise stints.

**Endpoint URL:** `GET https://records.nhl.com/site/api/general-manager-franchise-records`

**Valid URL:** [https://records.nhl.com/site/api/general-manager-franchise-records](https://records.nhl.com/site/api/general-manager-franchise-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_gm_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `active_gm` | Boolean | Indicates whether the general manager is currently active with the franchise. |
| `end_date` | String | Season end date. |
| `end_season_id` | Int64 | NHL season identifier for the last season the GM held the role with this franchise. |
| `first_name` | String | Player first name. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `franchise_name` | String | Franchise name. |
| `full_name` | String | Player full name. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games` | Int64 | Games played. |
| `gm_of_the_year` | Int64 | Number of NHL General Manager of the Year awards won during this franchise tenure. |
| `home_games` | Int64 |  |
| `home_losses` | Int64 | Losses at home. |
| `home_ot_losses` | Float64 | Home overtime losses. |
| `home_ties` | Float64 | Ties at home. |
| `home_wins` | Int64 | Wins at home. |
| `last_name` | String | Player last name. |
| `losses` | Int64 | Losses. |
| `losses_in_ot` | Int64 | Number of regular-season overtime losses recorded by the franchise under this GM. |
| `losses_in_ot_plus_shootout` | Int64 | Combined overtime and shootout losses for the franchise during this GM's tenure. |
| `losses_in_shootout` | Float64 | Number of regular-season shootout losses recorded by the franchise under this GM. |
| `overtime_losses` | Int64 | Total overtime losses. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `road_games` | Int64 | Total regular-season away games played by the franchise during this GM's tenure. |
| `road_losses` | Int64 | Losses on the road. |
| `road_ot_losses` | Float64 | Road overtime losses. |
| `road_ties` | Float64 | Ties on the road. |
| `road_wins` | Int64 | Wins on the road. |
| `seasons` | Int64 | Number of NHL seasons the GM held the role with this franchise. |
| `stanley_cup_final_appearances` | Int64 | Number of Stanley Cup Final appearances by the franchise during this GM's tenure. |
| `stanley_cups` | Int64 | Number of Stanley Cup championships won by the franchise under this GM. |
| `start_date` | String | Season start date. |
| `start_season_id` | Int64 | NHL season identifier for the first season the GM held the role with this franchise. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `ties` | Int64 | Total ties. |
| `ties_in_ot` | Int64 | Number of overtime ties recorded by the franchise under this GM (pre-shootout era). |
| `win_pctg` | Float64 | Overall win percentage for the franchise across all regular-season games during this GM's tenure. |
| `wins` | Int64 | Wins. |
| `wins_in_ot` | Int64 | Number of regular-season overtime wins recorded by the franchise under this GM. |
| `wins_in_ot_plus_shootout` | Int64 | Combined overtime and shootout wins for the franchise during this GM's tenure. |
| `wins_in_shootout` | Float64 | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_gm_franchise-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_gm_franchise-example}

```python
nhl_records_gm_franchise()
```

## nhl_records_home_team_record

League-wide home-team win/loss record by season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/home-team-record`

**Valid URL:** [https://records.nhl.com/site/api/home-team-record](https://records.nhl.com/site/api/home-team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_home_team_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_per_game` | Float64 | Goals against per game. |
| `goals_per_game` | Float64 | Average number of goals the team scored per home game over the recorded period. |
| `losses` | Int64 | Losses. |
| `overtime_losses` | Float64 | Total overtime losses. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `season_id` | Int64 | Season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `ties` | Float64 | Total ties. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_home_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_home_team_record-example}

```python
nhl_records_home_team_record()
```

## nhl_records_away_team_record

League-wide away-team win/loss record by season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/away-team-record`

**Valid URL:** [https://records.nhl.com/site/api/away-team-record](https://records.nhl.com/site/api/away-team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_away_team_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Unique player identifier. |
| `franchise_id` | Int64 | Unique franchise identifier. |
| `game_type_id` | Int64 | Game type identifier (regular/playoffs). |
| `games_played` | Int64 | Games played. |
| `goals` | Int64 | Goals scored. |
| `goals_against` | Int64 | Goals against. |
| `goals_against_per_game` | Float64 | Goals against per game. |
| `goals_per_game` | Float64 | Average number of goals the team scored per road game over the recorded period. |
| `losses` | Int64 | Losses. |
| `overtime_losses` | Float64 | Total overtime losses. |
| `point_pctg` | Float64 | Points percentage. |
| `points` | Int64 | Total points (goals + assists). |
| `season_id` | Int64 | Season identifier. |
| `team_id` | Int64 | Unique team identifier. |
| `team_name` | String | Team name. |
| `ties` | Float64 | Total ties. |
| `wins` | Int64 | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#nhl_records_away_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#nhl_records_away_team_record-example}

```python
nhl_records_away_team_record()
```
