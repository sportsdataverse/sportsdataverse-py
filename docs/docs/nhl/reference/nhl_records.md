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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `awarded_posthumously` | logical | Whether the award was given posthumously. |
| `coach_id` | double | ESPN coach id parsed from the `$ref` URL. |
| `created_on` | character | Date the trophy record was created. |
| `detail_summary` | character | Detail summary flag. |
| `full_name` | character | Player full name. |
| `general_manager_id` | double | General manager identifier, if applicable. |
| `image_url` | character | Player headshot URL. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `player_id` | double | Unique player identifier. |
| `player_image_caption` | character | Player image caption flag. |
| `player_image_url` | character | URL to the player image. |
| `season_id` | integer | Season identifier. |
| `status` | character | Status string (e.g. captain markers). |
| `summary` | character | Record summary string (e.g. "25-15-10"). |
| `team_id` | integer | Unique team identifier. |
| `trophy_category_id` | integer | Trophy category identifier. |
| `trophy_id` | integer | Trophy identifier. |
| `value` | character | Leader stat numeric value. |
| `vote_count` | double | Number of votes received. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `awarded_posthumously` | logical | Whether the award was given posthumously. |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `created_on` | character | Date the trophy record was created. |
| `detail_summary` | character | Detail summary flag. |
| `full_name` | character | Player full name. |
| `general_manager_id` | integer | General manager identifier, if applicable. |
| `image_url` | character | Player headshot URL. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `player_id` | character | Unique player identifier. |
| `player_image_caption` | character | Player image caption flag. |
| `player_image_url` | character | URL to the player image. |
| `season_id` | integer | Season identifier. |
| `status` | character | Status string (e.g. captain markers). |
| `summary` | character | Record summary string (e.g. "25-15-10"). |
| `team_id` | integer | Unique team identifier. |
| `trophy_category_id` | integer | Trophy category identifier. |
| `trophy_id` | integer | Trophy identifier. |
| `value` | character | Leader stat numeric value. |
| `vote_count` | integer | Number of votes received. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `bio` | character |  |
| `birth_city` | character | Birth city. |
| `birth_country3code` | character | Prospect birth country three-letter code. |
| `birth_date` | character | Player birth date. |
| `birth_state_province_code` | character |  |
| `brief_description` | character | Brief description of the trophy. |
| `date_of_death` | character | Date of death, if applicable. |
| `deceased` | logical | Whether the player is deceased. |
| `description` | character | Full text description of the event. |
| `featured_image` | character |  |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `history` | character | ESPN's long-form history text for the award. |
| `hockey_hof_link` | character |  |
| `in_hockey_hof` | logical | Whether the player is in the Hockey Hall of Fame. |
| `in_iihf_hockey_hof` | logical |  |
| `in_us_hockey_hof` | logical | Whether the player is in the US Hockey Hall of Fame. |
| `instagram` | character |  |
| `is_active` | logical | Whether the team is active. |
| `last_name` | character | Player last name. |
| `nationality_code` | character | Nationality code of the official. |
| `player_id` | integer | Unique player identifier. |
| `stanley_cup` | integer |  |
| `team_id` | character | Unique team identifier. |
| `top100_player_link` | character |  |
| `twitter` | character |  |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_coach` | logical |  |
| `coach_name` | character |  |
| `end_season` | integer |  |
| `first_name` | character | Player first name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_win_pctg` | double |  |
| `home_wins` | integer | Wins at home. |
| `jack_adams` | integer |  |
| `last_coached_date` | character |  |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer |  |
| `losses_in_ot_plus_shootout` | integer |  |
| `losses_in_shootout` | double |  |
| `ot_losses` | double | Overtime losses. |
| `road_games` | integer |  |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_win_pctg` | double |  |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer |  |
| `stanley_cup_final_appearances` | integer |  |
| `stanley_cups` | integer |  |
| `start_season` | integer |  |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | double | Total ties. |
| `ties_in_ot` | integer |  |
| `win_pctg` | double |  |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer |  |
| `wins_in_ot_plus_shootout` | integer |  |
| `wins_in_shootout` | double | Wins in shootout. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_coach` | logical |  |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `coach_name` | character |  |
| `end_season` | integer |  |
| `games` | integer | Games played. |
| `losses` | integer | Losses. |
| `ot_losses` | double | Overtime losses. |
| `seasons` | integer |  |
| `start_season` | integer |  |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | double | Total ties. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_coach` | logical |  |
| `coach_name` | character |  |
| `end_season` | integer |  |
| `first_coached_date` | character |  |
| `first_name` | character | Player first name. |
| `franchise_id` | integer | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_win_pctg` | double |  |
| `home_wins` | integer | Wins at home. |
| `jack_adams` | integer |  |
| `last_coached_date` | character |  |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer |  |
| `losses_in_ot_plus_shootout` | integer |  |
| `losses_in_shootout` | double |  |
| `ot_losses` | double | Overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_games` | integer |  |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_win_pctg` | double |  |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer |  |
| `stanley_cup_final_appearances` | integer |  |
| `stanley_cups` | integer |  |
| `start_season` | integer |  |
| `team_abbrev` | character | Team abbreviation. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `ties_in_ot` | integer |  |
| `win_pctg` | double |  |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer |  |
| `wins_in_ot_plus_shootout` | integer |  |
| `wins_in_shootout` | double | Wins in shootout. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_coach` | logical |  |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `coach_name` | character |  |
| `franchise_id` | double | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `longest_streak` | integer |  |
| `longest_streak_description` | character |  |
| `seasons_won` | character |  |
| `stanley_cups` | integer |  |
| `team_abbrevs` | character | Team abbreviation(s). |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active` | logical | Whether athlete is currently active. |
| `captain_history` | character | Franchise captain history text. |
| `coaching_history` | character | Franchise coaching history text. |
| `date_awarded` | character | Date the franchise was awarded. |
| `directory_url` | character | Franchise directory URL. |
| `first_season_id` | integer | Season identifier of the first season. |
| `general_manager_history` | character | Franchise general manager history text. |
| `hero_image_url` | character | Franchise hero image URL. |
| `most_recent_team_id` | integer | Most recent team identifier. |
| `retired_numbers_summary` | character | Summary of retired jersey numbers. |
| `team_abbrev` | character | Team abbreviation. |
| `team_full_name` | character | Full team name. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `conference_abbrev` | character | Conference abbreviation. |
| `conference_name` | character | Conference name. |
| `conference_sequence` | integer | Team's seeding position within the conference. |
| `decision` | character | Goalie decision (W/L/O). |
| `division_abbrev` | character | Division abbreviation. |
| `division_name` | character | Division name. |
| `division_sequence` | integer | Team's seeding position within the division. |
| `final_playoff_round` | integer | Final playoff round reached. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `home_losses` | integer | Losses at home. |
| `home_overtime_losses` | character | Overtime losses at home. |
| `home_ties` | integer | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `in_playoffs` | logical | Whether the season reached the playoffs. |
| `league_sequence` | integer | Team's seeding position within the league. |
| `losses` | integer | Losses. |
| `overtime_losses` | character | Total overtime losses. |
| `penalty_minutes` | integer | Penalty minutes. |
| `playoff_round` | double | Playoff round identifier. |
| `points` | integer | Total points (goals + assists). |
| `road_losses` | integer | Losses on the road. |
| `road_overtime_losses` | character | Overtime losses on the road. |
| `road_ties` | integer | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `season_id` | integer | Season identifier. |
| `series_abbrev` | character | Playoff series abbreviation. |
| `series_title` | character | Playoff series title. |
| `shutouts` | integer | Shutouts recorded. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | integer | Total ties. |
| `tri_code` | character | Team three-letter code. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `first_season_id` | integer | Season identifier of the first season. |
| `franchise_id` | integer | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `playoff_seasons` | integer | Number of playoff seasons. |
| `stanley_cup_appearances` | integer | Number of Stanley Cup Final appearances. |
| `stanley_cup_wins` | integer | Number of Stanley Cup championships. |
| `years` | integer | Number of years the franchise existed. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_franchise` | integer | Indicator of whether the franchise is active. |
| `cups` | integer | Number of Stanley Cup championships. |
| `first_season_id` | integer | Season identifier of the first season. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `game_win_pctg` | double | Game-winning percentage. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_for` | integer | Goals for. |
| `home_losses` | integer | Losses at home. |
| `home_overtime_losses` | double | Overtime losses at home. |
| `home_ties` | double | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `last_season_id` | double | Season ID of the franchise's last season. |
| `losses` | integer | Losses. |
| `overtime_losses` | double | Total overtime losses. |
| `penalty_minutes` | integer | Penalty minutes. |
| `playoff_seasons` | double | Number of playoff seasons. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_losses` | integer | Losses on the road. |
| `road_overtime_losses` | double | Overtime losses on the road. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `series_losses` | double | Playoff series losses. |
| `series_played` | double | Playoff series played. |
| `series_win_pctg` | double | Playoff series win percentage. |
| `series_wins` | double | Playoff series wins. |
| `shootout_losses` | integer | Shootout losses. |
| `shootout_wins` | integer | Shootout wins. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_franchise` | integer | Indicator of whether the franchise is active. |
| `active_opponent_franchise` | integer |  |
| `franchise_name` | character | Franchise name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `home_games_played` | integer |  |
| `home_goals_against` | double |  |
| `home_goals_for` | double |  |
| `home_last_meeting_season_id` | integer |  |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | integer | Home overtime losses. |
| `home_points` | integer | Home team total points scored in the game so far. |
| `home_ties` | integer | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `opponent_franchise_id` | integer |  |
| `opponent_franchise_name` | character |  |
| `opponent_team_id` | integer | Opponent team identifier. |
| `road_games_played` | integer |  |
| `road_goals_against` | integer |  |
| `road_goals_for` | integer |  |
| `road_last_meeting_season_id` | integer |  |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | integer | Road overtime losses. |
| `road_points` | integer |  |
| `road_ties` | integer | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `team_franchise_id` | integer | Team franchise identifier. |
| `team_id` | integer | Unique team identifier. |
| `total_games_played` | integer |  |
| `total_goals_against` | integer |  |
| `total_goals_for` | integer |  |
| `total_last_meeting_season_id` | integer |  |
| `total_losses` | integer | Total losses to date (goalie). |
| `total_ot_losses` | integer |  |
| `total_points` | integer |  |
| `total_ties` | integer | Total ties. |
| `total_wins` | integer | Total wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `consecutive100_point_seasons` | integer |  |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `seasons_played` | integer | Number of seasons played. |
| `streak_end_season` | integer |  |
| `streak_start_season` | integer |  |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `first_season_for_game_type` | integer | First season ID for the game type. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_seven_games_played` | character | Game seven games played. |
| `game_seven_losses` | character | Game seven losses. |
| `game_seven_wins` | character | Game seven wins. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `last_season_for_game_type` | integer | Last season ID for the game type. |
| `losses` | integer | Losses. |
| `overtime_games_played` | integer | Overtime games played. |
| `overtime_goals_against` | integer | Overtime goals against. |
| `overtime_goals_against_average` | character | Overtime goals against average. |
| `overtime_losses` | character | Total overtime losses. |
| `overtime_save_pctg` | double | Overtime save percentage. |
| `overtime_shots_against` | integer | Overtime shots against. |
| `overtime_ties` | integer | Overtime ties. |
| `overtime_time_on_ice` | double | Overtime time on ice (seconds). |
| `overtime_wins` | integer | Overtime wins. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `save_pctg` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `seasons_played` | integer | Number of seasons played. |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `time_on_ice_min_sec` | character | Total time on ice (MM:SS). |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | integer | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `overtime_losses` | character | Total overtime losses. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `save_pctg` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `time_on_ice_min_sec` | character | Total time on ice (MM:SS). |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_seven_games_played` | character | Game seven games played. |
| `game_seven_losses` | character | Game seven losses. |
| `game_seven_wins` | character | Game seven wins. |
| `game_type` | integer | Game type the row belongs to. |
| `games_played` | integer | Games played. |
| `games_started` | integer | Games started (goalies). |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `number_of_games_in_season` | integer | Number of games in the season. |
| `overtime_games_played` | integer | Overtime games played. |
| `overtime_goals_against` | integer | Overtime goals against. |
| `overtime_losses` | character | Total overtime losses. |
| `overtime_ties` | integer | Overtime ties. |
| `overtime_wins` | integer | Overtime wins. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `rookie_flag` | logical | Indicator of whether the player was a rookie. |
| `save_pctg` | double | Save percentage. |
| `saves` | integer | Saves made. |
| `season_id` | integer | Season identifier. |
| `shots_against` | integer | Shots faced. |
| `shutouts` | integer | Shutouts recorded. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `time_on_ice_min_sec` | character | Total time on ice (MM:SS). |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `end_date` | character | Season end date. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `rookie` | logical | Whether the player is a rookie. |
| `season_id` | integer | Season identifier. |
| `start_date` | character | Season start date. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `win_streak` | integer |  |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `duration_min_sec` | character | Streak duration (MM:SS). |
| `duration_seconds` | integer | Streak duration in seconds. |
| `end_date` | character | Season end date. |
| `first_name` | character | Player first name. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `saves` | character | Saves made. |
| `season_id` | integer | Season identifier. |
| `start_date` | character | Season start date. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `first_name` | character | Player first name. |
| `forty_win_seasons` | integer |  |
| `franchise_id` | double | Unique franchise identifier. |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `seasons_played` | integer | Number of seasons played. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |
| `thirty_win_seasons` | integer |  |
| `twenty_win_seasons` | integer |  |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `consecutive_playoff_seasons` | integer |  |
| `end_season` | integer |  |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `playoff_seasons` | integer | Number of playoff seasons. |
| `stanley_cup_wins` | integer | Number of Stanley Cup championships. |
| `start_season` | integer |  |
| `team_abbrevs` | character | Team abbreviation(s). |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `end_date` | character | Season end date. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `season_id` | integer | Season identifier. |
| `start_date` | character | Season start date. |
| `team_abbrev` | character | Team abbreviation. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `undefeated_streak` | integer |  |

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
| `cs_player_id` | character | Central Scouting player identifier. |
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
| `weight` | double | Player weight in pounds. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `draft_year` | integer | Draft year the lottery applies to. |
| `format_content` | character | Description of the lottery format. |
| `odds_content` | character | Description of the lottery odds. |
| `result_notes` | character | Notes on the lottery results. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active` | logical | Whether athlete is currently active. |
| `draft_picks` | character |  |
| `season_id` | integer | Season identifier. |
| `team_id` | integer | Unique team identifier. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer |  |
| `assists` | integer | Assists. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `nhl_team_id` | integer |  |
| `penalties` | double | Penalty count. |
| `penalty_minutes` | double | Penalty minutes. |
| `player_id` | integer | Unique player identifier. |
| `points` | integer | Total points (goals + assists). |
| `position` | character | Player position. |
| `power_play_goals` | integer | Power-play goals. |
| `season_id` | integer | Season identifier. |
| `short_handed_goals` | integer | Short-handed goals. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer |  |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `games_played` | integer | Games played. |
| `goals_against` | integer | Goals against. |
| `goals_against_average` | double | Goals against average. |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `nhl_team_id` | integer |  |
| `ot_losses` | integer | Overtime losses. |
| `player_id` | integer | Unique player identifier. |
| `save_percentage` | double | Save percentage (goalies). |
| `season_id` | integer | Season identifier. |
| `shots_against` | integer | Shots faced. |
| `team_losses` | integer | Team losses. |
| `team_wins` | integer | Team wins. |
| `ties` | integer | Total ties. |
| `time_on_ice` | integer | Time on ice in seconds. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer |  |
| `coach_id` | integer | ESPN coach id parsed from the `$ref` URL. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `games_coached` | integer |  |
| `is_active` | logical | Whether the team is active. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `ot_losses` | integer | Overtime losses. |
| `season_id` | integer | Season identifier. |
| `ties` | integer | Total ties. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer |  |
| `all_star_team_score` | integer |  |
| `arena_name` | character | Arena name. |
| `assists` | integer | Assists. |
| `city` | character | City where the venue is located. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `game_date` | character | Game date. |
| `game_id` | integer | Unique game identifier. |
| `game_name` | character | Full event name. |
| `goals` | integer | Goals scored. |
| `home_road` | character |  |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `mvp` | character | Mvp. |
| `nhl_team_id` | integer |  |
| `opponent_score` | integer | Opponent score. |
| `opponent_team_id` | integer | Opponent team identifier. |
| `penalties` | double | Penalty count. |
| `penalty_minutes` | double | Penalty minutes. |
| `player_id` | integer | Unique player identifier. |
| `points` | integer | Total points (goals + assists). |
| `position` | character | Player position. |
| `power_play_goals` | integer | Power-play goals. |
| `season_id` | integer | Season identifier. |
| `short_handed_goals` | integer | Short-handed goals. |
| `state_province_code` | character | State or province code of the official. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `all_star_team_id` | integer |  |
| `all_star_team_score` | integer |  |
| `arena_name` | character | Arena name. |
| `city` | character | City where the venue is located. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `game_date` | character | Game date. |
| `game_id` | integer | Unique game identifier. |
| `game_name` | character | Full event name. |
| `goals_against` | integer | Goals against. |
| `home_road` | character |  |
| `is_active` | logical | Whether the team is active. |
| `is_rookie` | logical | Whether the player is a rookie. |
| `last_name` | character | Player last name. |
| `mvp` | character | Mvp. |
| `nhl_team_id` | integer |  |
| `opponent_score` | integer | Opponent score. |
| `opponent_team_id` | integer | Opponent team identifier. |
| `player_id` | integer | Unique player identifier. |
| `save_percentage` | double | Save percentage (goalies). |
| `saves` | integer | Saves made. |
| `season_id` | integer | Season identifier. |
| `shots_against` | integer | Shots faced. |
| `state_province_code` | character | State or province code of the official. |
| `time_on_ice` | integer | Time on ice in seconds. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `date_inducted` | character | Date the inductee entered the Hall of Fame. |
| `induction_cat_id` | integer | Induction category identifier. |
| `misc_full_name` | character | Full name of the inductee. |
| `office_id` | integer | Office/category identifier. |
| `official_id` | character | ESPN official id (echoed from arg). |
| `player_id` | integer | Unique player identifier. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `date_inducted` | character | Date the inductee entered the Hall of Fame. |
| `induction_cat_id` | integer | Induction category identifier. |
| `misc_full_name` | character | Full name of the inductee. |
| `office_id` | integer | Office/category identifier. |
| `official_id` | character | ESPN official id (echoed from arg). |
| `player_id` | character | Unique player identifier. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_gm` | logical |  |
| `end_date` | character | Season end date. |
| `end_season_id` | integer |  |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `gm_of_the_year` | integer |  |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer |  |
| `losses_in_ot_plus_shootout` | integer |  |
| `losses_in_shootout` | double |  |
| `overtime_losses` | integer | Total overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_games` | integer |  |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer |  |
| `stanley_cup_final_appearances` | integer |  |
| `stanley_cups` | integer |  |
| `start_date` | character | Season start date. |
| `start_season_id` | integer |  |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | integer | Total ties. |
| `ties_in_ot` | integer |  |
| `win_pctg` | double |  |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer |  |
| `wins_in_ot_plus_shootout` | integer |  |
| `wins_in_shootout` | double | Wins in shootout. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `active_gm` | logical |  |
| `end_date` | character | Season end date. |
| `end_season_id` | integer |  |
| `first_name` | character | Player first name. |
| `franchise_id` | integer | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `full_name` | character | Player full name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `gm_of_the_year` | integer |  |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer |  |
| `losses_in_ot_plus_shootout` | integer |  |
| `losses_in_shootout` | double |  |
| `overtime_losses` | integer | Total overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_games` | integer |  |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer |  |
| `stanley_cup_final_appearances` | integer |  |
| `stanley_cups` | integer |  |
| `start_date` | character | Season start date. |
| `start_season_id` | integer |  |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | integer | Total ties. |
| `ties_in_ot` | integer |  |
| `win_pctg` | double |  |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer |  |
| `wins_in_ot_plus_shootout` | integer |  |
| `wins_in_shootout` | double | Wins in shootout. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `goals_against_per_game` | double | Goals against per game. |
| `goals_per_game` | double |  |
| `losses` | integer | Losses. |
| `overtime_losses` | double | Total overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `season_id` | integer | Season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `wins` | integer | Wins. |

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | integer | Unique player identifier. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `goals_against_per_game` | double | Goals against per game. |
| `goals_per_game` | double |  |
| `losses` | integer | Losses. |
| `overtime_losses` | double | Total overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `season_id` | integer | Season identifier. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | double | Total ties. |
| `wins` | integer | Wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
nhl_records_away_team_record()
```

_Last validated n/a._
