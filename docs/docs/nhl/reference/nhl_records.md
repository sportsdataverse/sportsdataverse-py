---
title: NHL — NHL Records API
sidebar_label: NHL Records API
sidebar_position: 13
---
# NHL — NHL Records API

`sportsdataverse.nhl` — 44 endpoints.

## `nhl_records_awards`

List all NHL award / trophy records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details`

**Valid URL:** [https://records.nhl.com/site/api/award-details](https://records.nhl.com/site/api/award-details)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_awards()
```

_Last validated n/a._

## `nhl_records_awards_by_franchise`

List award records for a single franchise.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details/{franchise_id}`

**Valid URL:** [https://records.nhl.com/site/api/award-details/1](https://records.nhl.com/site/api/award-details/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_awards_by_franchise(franchise_id=1)
```

_Last validated n/a._

## `nhl_records_awards_trophy_season`

Retrieve the trophy winner for a specific season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/award-details/trophy/{trophy_id}/season/{season_id}`

**Valid URL:** [https://records.nhl.com/site/api/award-details/trophy/1/season/X](https://records.nhl.com/site/api/award-details/trophy/1/season/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `trophy_id` | `trophy_id` |  | `Y` |  | trophy_id path parameter. |
| `season_id` | `season_id` |  | `Y` |  | season_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_awards_trophy_season(trophy_id=1, season_id='X')
```

_Last validated n/a._

## `nhl_records_coaches`

List NHL head coaches.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach`

**Valid URL:** [https://records.nhl.com/site/api/coach](https://records.nhl.com/site/api/coach)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `bio` | character | Long-form biographical narrative for the coach, as provided by the NHL api-web endpoint. |
| `birth_city` | character | Birth city. |
| `birth_country3code` | character | Prospect birth country three-letter code. |
| `birth_date` | character | Player birth date. |
| `birth_state_province_code` | character | Two-letter state or province code of the coach's birth location (e.g., 'ON' for Ontario, 'MI' for Michigan). |
| `brief_description` | character | Brief description of the trophy. |
| `date_of_death` | character | Date of death, if applicable. |
| `deceased` | logical | Whether the player is deceased. |
| `description` | character | Full text description of the event. |
| `featured_image` | character | URL of the coach's featured promotional or profile image on the NHL platform. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `history` | character | ESPN's long-form history text for the award. |
| `hockey_hof_link` | character | URL to the coach's Hockey Hall of Fame profile page, if they are an inductee. |
| `in_hockey_hof` | logical | Whether the player is in the Hockey Hall of Fame. |
| `in_iihf_hockey_hof` | logical | Boolean flag indicating whether the coach is inducted into the IIHF Hockey Hall of Fame. |
| `in_us_hockey_hof` | logical | Whether the player is in the US Hockey Hall of Fame. |
| `instagram` | character | Instagram handle or profile URL for the coach's official social media presence. |
| `is_active` | logical | Whether the team is active. |
| `last_name` | character | Player last name. |
| `nationality_code` | character | Nationality code of the official. |
| `player_id` | double | Unique player identifier. |
| `stanley_cup` | double | Number of Stanley Cup championships won by the coach as a head coach or assistant coach. |
| `team_id` | character | Unique team identifier. |
| `top100_player_link` | character | URL to the coach's NHL Top 100 players recognition page, if applicable. |
| `twitter` | character | Twitter/X handle or profile URL for the coach's official social media presence. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_coaches()
```

_Last validated n/a._

## `nhl_records_coach`

Retrieve one coach by their numeric ID.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach/X](https://records.nhl.com/site/api/coach/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_coach(coach_id='X')
```

_Last validated n/a._

## `nhl_records_coach_career`

Coach career-records (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-career-records/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach-career-records](https://records.nhl.com/site/api/coach-career-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  |  | `Y` | coach_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_coach_career()
```

_Last validated n/a._

## `nhl_records_coach_career_with_playoffs`

Coach career records inclusive of regular season + playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs`

**Valid URL:** [https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs](https://records.nhl.com/site/api/coach-career-records-regular-plus-playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_coach_career_with_playoffs()
```

_Last validated n/a._

## `nhl_records_coach_franchise`

Coach records scoped to individual franchise stints.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-franchise-records/{coach_id}`

**Valid URL:** [https://records.nhl.com/site/api/coach-franchise-records](https://records.nhl.com/site/api/coach-franchise-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  |  | `Y` | coach_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_coach_franchise()
```

_Last validated n/a._

## `nhl_records_coach_stanley_cup`

Coach Stanley Cup Final win streak and consecutive-cup records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/coach-stanley-cup-streak`

**Valid URL:** [https://records.nhl.com/site/api/coach-stanley-cup-streak](https://records.nhl.com/site/api/coach-stanley-cup-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_coach_stanley_cup()
```

_Last validated n/a._

## `nhl_records_franchises`

List all NHL franchises (historical and active).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise`

**Valid URL:** [https://records.nhl.com/site/api/franchise](https://records.nhl.com/site/api/franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `first_season_id` | integer | Season identifier of the first season. |
| `full_name` | character | Player full name. |
| `last_season_id` | double | Season ID of the franchise's last season. |
| `most_recent_team_id` | integer | Most recent team identifier. |
| `team_abbrev` | character | Team abbreviation. |
| `team_common_name` | character | Team common (nickname) name. |
| `team_place_name` | character | Team place (city/location) name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_franchises()
```

_Last validated n/a._

## `nhl_records_franchise_detail`

Franchise detail records (extended metadata per franchise).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-detail`

**Valid URL:** [https://records.nhl.com/site/api/franchise-detail](https://records.nhl.com/site/api/franchise-detail)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_franchise_detail()
```

_Last validated n/a._

## `nhl_records_franchise_team_totals`

All-time team totals per franchise (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-team-totals`

**Valid URL:** [https://records.nhl.com/site/api/franchise-team-totals](https://records.nhl.com/site/api/franchise-team-totals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_franchise` | integer | Indicator of whether the franchise is active. |
| `active_team` | logical | Indicator of whether the team is active. |
| `cups` | integer | Number of Stanley Cup championships. |
| `first_season_id` | integer | Season identifier of the first season. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `game_win_pctg` | double | Game-winning percentage. |
| `games_played` | double | Games played. |
| `goals_against` | double | Goals against. |
| `goals_for` | double | Goals for. |
| `home_losses` | double | Losses at home. |
| `home_overtime_losses` | double | Overtime losses at home. |
| `home_ties` | double | Ties at home. |
| `home_wins` | double | Wins at home. |
| `last_season_id` | double | Season ID of the franchise's last season. |
| `losses` | double | Losses. |
| `overtime_losses` | double | Total overtime losses. |
| `penalty_minutes` | double | Penalty minutes. |
| `playoff_seasons` | double | Number of playoff seasons. |
| `point_pctg` | double | Points percentage. |
| `points` | double | Total points (goals + assists). |
| `road_losses` | double | Losses on the road. |
| `road_overtime_losses` | double | Overtime losses on the road. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | double | Wins on the road. |
| `series_losses` | integer | Playoff series losses. |
| `series_played` | double | Playoff series played. |
| `series_win_pctg` | double | Playoff series win percentage. |
| `series_wins` | integer | Playoff series wins. |
| `shootout_losses` | double | Shootout losses. |
| `shootout_wins` | double | Shootout wins. |
| `shutouts` | double | Shutouts recorded. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `tri_code` | character | Team three-letter code. |
| `wins` | double | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_franchise_team_totals()
```

_Last validated n/a._

## `nhl_records_franchise_season_results`

Season-by-season results for each franchise.

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-season-results`

**Valid URL:** [https://records.nhl.com/site/api/franchise-season-results](https://records.nhl.com/site/api/franchise-season-results)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_franchise_season_results()
```

_Last validated n/a._

## `nhl_records_franchise_playoff_appearances`

Franchise playoff appearance counts and streak information.

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-playoff-appearances`

**Valid URL:** [https://records.nhl.com/site/api/franchise-playoff-appearances](https://records.nhl.com/site/api/franchise-playoff-appearances)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_franchise_playoff_appearances()
```

_Last validated n/a._

## `nhl_records_franchise_totals`

League-wide franchise totals (all-time aggregate per franchise).

**Endpoint URL:** `GET https://records.nhl.com/site/api/franchise-totals`

**Valid URL:** [https://records.nhl.com/site/api/franchise-totals](https://records.nhl.com/site/api/franchise-totals)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_franchise_totals()
```

_Last validated n/a._

## `nhl_records_all_time_record_vs_franchise`

All-time head-to-head records between every franchise pairing.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-time-record-vs-franchise`

**Valid URL:** [https://records.nhl.com/site/api/all-time-record-vs-franchise](https://records.nhl.com/site/api/all-time-record-vs-franchise)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_all_time_record_vs_franchise()
```

_Last validated n/a._

## `nhl_records_skater_career_stats`

Skater career statistics (all-time, regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-statistics`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-statistics](https://records.nhl.com/site/api/skater-career-statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_skater_career_stats()
```

_Last validated n/a._

## `nhl_records_skater_career_leaders`

All-time skater career leaderboards.

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-leaders`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-leaders](https://records.nhl.com/site/api/skater-career-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_skater_career_leaders()
```

_Last validated n/a._

## `nhl_records_consecutive_100pt_seasons`

Skaters with the most consecutive 100-point seasons.

**Endpoint URL:** `GET https://records.nhl.com/site/api/consecutive-100-point-seasons`

**Valid URL:** [https://records.nhl.com/site/api/consecutive-100-point-seasons](https://records.nhl.com/site/api/consecutive-100-point-seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_consecutive_100pt_seasons()
```

_Last validated n/a._

## `nhl_records_goalie_career_stats`

Goaltender career statistics (regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-career-stats](https://records.nhl.com/site/api/goalie-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_career_stats()
```

_Last validated n/a._

## `nhl_records_goalie_career_stats_with_playoffs`

Goaltender career stats inclusive of regular season and playoffs.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs`

**Valid URL:** [https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs](https://records.nhl.com/site/api/goalie_career_stats_incl_playoffs)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_career_stats_with_playoffs()
```

_Last validated n/a._

## `nhl_records_goalie_season_stats`

Goaltender single-season statistics.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-season-stats`

**Valid URL:** [https://records.nhl.com/site/api/goalie-season-stats](https://records.nhl.com/site/api/goalie-season-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_season_stats()
```

_Last validated n/a._

## `nhl_records_goalie_win_streak`

Goaltenders with the longest consecutive-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-streak](https://records.nhl.com/site/api/goalie-win-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_win_streak()
```

_Last validated n/a._

## `nhl_records_goalie_shutout_streak`

Goaltenders with the longest consecutive-shutout streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-shutout-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-shutout-streak](https://records.nhl.com/site/api/goalie-shutout-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_shutout_streak()
```

_Last validated n/a._

## `nhl_records_goalie_win_plateaus`

Goaltenders who reached each win plateau (100, 200, 300 …).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-win-plateaus`

**Valid URL:** [https://records.nhl.com/site/api/goalie-win-plateaus](https://records.nhl.com/site/api/goalie-win-plateaus)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_win_plateaus()
```

_Last validated n/a._

## `nhl_records_goalie_playoff_streak`

Goaltender consecutive playoff-win streaks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-playoff-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-playoff-streak](https://records.nhl.com/site/api/goalie-playoff-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_playoff_streak()
```

_Last validated n/a._

## `nhl_records_goalie_undefeated_streak`

Goaltender longest undefeated streaks (wins + ties).

**Endpoint URL:** `GET https://records.nhl.com/site/api/goalie-undefeated-streak`

**Valid URL:** [https://records.nhl.com/site/api/goalie-undefeated-streak](https://records.nhl.com/site/api/goalie-undefeated-streak)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_goalie_undefeated_streak()
```

_Last validated n/a._

## `nhl_records_draft`

Retrieve NHL Entry Draft picks.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft/{draft_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft](https://records.nhl.com/site/api/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `draft_id` | `draft_id` |  |  | `Y` | draft_id path parameter. |

### Returns

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
| `cs_player_id` | character | Central Scouting player identifier. |
| `draft_date` | character | Date the player was drafted. |
| `draft_master_id` | integer | Draft master record identifier. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `drafted_by_team_id` | character | Identifier of the drafting team. |
| `first_name` | character | Player first name. |
| `height` | character | Player height in inches. |
| `last_name` | character | Player last name. |
| `notes` | character | Notes flag for the pick. |
| `overall_pick_number` | integer | Overall pick number in the draft. |
| `pick_in_round` | integer | Pick number within the round. |
| `player_id` | character | Unique player identifier. |
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

### Example

```python
nhl_records_draft()
```

_Last validated n/a._

## `nhl_records_draft_by_team`

All draft picks made by a single team.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft/byTeam/{team_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft/byTeam/10](https://records.nhl.com/site/api/draft/byTeam/10)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_draft_by_team(team_id=10)
```

_Last validated n/a._

## `nhl_records_draft_prospect`

Draft prospect records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft-prospect/{prospect_id}`

**Valid URL:** [https://records.nhl.com/site/api/draft-prospect](https://records.nhl.com/site/api/draft-prospect)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `prospect_id` | `prospect_id` |  |  | `Y` | prospect_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_draft_prospect()
```

_Last validated n/a._

## `nhl_records_draft_lottery_odds`

Draft lottery odds (current year or filtered by season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/draft-lottery-odds`

**Valid URL:** [https://records.nhl.com/site/api/draft-lottery-odds](https://records.nhl.com/site/api/draft-lottery-odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_draft_lottery_odds()
```

_Last validated n/a._

## `nhl_records_expansion_draft_picks`

Expansion draft picks (e.g. Vegas 2017, Seattle 2021).

**Endpoint URL:** `GET https://records.nhl.com/site/api/expansion-draft-picks`

**Valid URL:** [https://records.nhl.com/site/api/expansion-draft-picks](https://records.nhl.com/site/api/expansion-draft-picks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_expansion_draft_picks()
```

_Last validated n/a._

## `nhl_records_allstar_skater_career`

All-Star Game career statistics for skaters.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-skater-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-skater-career-stats](https://records.nhl.com/site/api/all-star-skater-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_allstar_skater_career()
```

_Last validated n/a._

## `nhl_records_allstar_goalie_career`

All-Star Game career statistics for goaltenders.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-goaltender-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-goaltender-career-stats](https://records.nhl.com/site/api/all-star-goaltender-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_allstar_goalie_career()
```

_Last validated n/a._

## `nhl_records_allstar_coach_career`

All-Star Game career records for coaches.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-coach-career-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-coach-career-stats](https://records.nhl.com/site/api/all-star-coach-career-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_allstar_coach_career()
```

_Last validated n/a._

## `nhl_records_allstar_skater_game`

All-Star Game single-game scoring records for skaters.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-skater-game-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-skater-game-stats](https://records.nhl.com/site/api/all-star-skater-game-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_allstar_skater_game()
```

_Last validated n/a._

## `nhl_records_allstar_goalie_game`

All-Star Game single-game stats for goaltenders.

**Endpoint URL:** `GET https://records.nhl.com/site/api/all-star-goaltender-game-stats`

**Valid URL:** [https://records.nhl.com/site/api/all-star-goaltender-game-stats](https://records.nhl.com/site/api/all-star-goaltender-game-stats)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_allstar_goalie_game()
```

_Last validated n/a._

## `nhl_records_attendance`

NHL arena attendance records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/attendance`

**Valid URL:** [https://records.nhl.com/site/api/attendance](https://records.nhl.com/site/api/attendance)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `playoff_attendance` | double | Total playoff attendance. |
| `regular_attendance` | double | Total regular-season attendance. |
| `season_id` | integer | Season identifier. |
| `total_attendance` | double | Total attendance for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_attendance()
```

_Last validated n/a._

## `nhl_records_hof_players`

Hockey Hall of Fame player inductees.

**Endpoint URL:** `GET https://records.nhl.com/site/api/hof/players`

**Valid URL:** [https://records.nhl.com/site/api/hof/players](https://records.nhl.com/site/api/hof/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_hof_players()
```

_Last validated n/a._

## `nhl_records_hof_players_by_office`

Hall of Fame players for a specific induction office/category.

**Endpoint URL:** `GET https://records.nhl.com/site/api/hof/players/{office_id}`

**Valid URL:** [https://records.nhl.com/site/api/hof/players/X](https://records.nhl.com/site/api/hof/players/X)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `office_id` | `office_id` |  | `Y` |  | office_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_hof_players_by_office(office_id='X')
```

_Last validated n/a._

## `nhl_records_gm_career`

General Manager career records.

**Endpoint URL:** `GET https://records.nhl.com/site/api/general-manager/{gm_id}`

**Valid URL:** [https://records.nhl.com/site/api/general-manager](https://records.nhl.com/site/api/general-manager)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gm_id` | `gm_id` |  |  | `Y` | gm_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_gm_career()
```

_Last validated n/a._

## `nhl_records_gm_franchise`

General Manager records scoped to franchise stints.

**Endpoint URL:** `GET https://records.nhl.com/site/api/general-manager-franchise-records`

**Valid URL:** [https://records.nhl.com/site/api/general-manager-franchise-records](https://records.nhl.com/site/api/general-manager-franchise-records)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_gm_franchise()
```

_Last validated n/a._

## `nhl_records_home_team_record`

League-wide home-team win/loss record by season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/home-team-record`

**Valid URL:** [https://records.nhl.com/site/api/home-team-record](https://records.nhl.com/site/api/home-team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_home_team_record()
```

_Last validated n/a._

## `nhl_records_away_team_record`

League-wide away-team win/loss record by season.

**Endpoint URL:** `GET https://records.nhl.com/site/api/away-team-record`

**Valid URL:** [https://records.nhl.com/site/api/away-team-record](https://records.nhl.com/site/api/away-team-record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_away_team_record()
```

_Last validated n/a._
