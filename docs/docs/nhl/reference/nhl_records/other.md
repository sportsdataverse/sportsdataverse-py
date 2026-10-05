---
title: "NHL — NHL Records API — Other"
sidebar_label: "Other"
sidebar_position: 5
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

### Example {#nhl_records_awards-example}

```python
nhl_records_awards()
```

_Last validated n/a._

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

### Example {#nhl_records_awards_by_franchise-example}

```python
nhl_records_awards_by_franchise(franchise_id=1)
```

_Last validated n/a._

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

### Example {#nhl_records_awards_trophy_season-example}

```python
nhl_records_awards_trophy_season(trophy_id=1, season_id='X')
```

_Last validated n/a._

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

### Example {#nhl_records_coaches-example}

```python
nhl_records_coaches()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `bio` | character | Free-text biographical summary of the coach's career and background. |
| `birth_city` | character | Birth city. |
| `birth_country3code` | character | Prospect birth country three-letter code. |
| `birth_date` | character | Player birth date. |
| `birth_state_province_code` | character | Two-letter state or province code indicating the coach's place of birth. |
| `brief_description` | character | Brief description of the trophy. |
| `date_of_death` | character | Date of death, if applicable. |
| `deceased` | logical | Whether the player is deceased. |
| `description` | character | Full text description of the event. |
| `featured_image` | character | URL of the featured promotional image associated with the coach's NHL profile. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `history` | character | ESPN's long-form history text for the award. |
| `hockey_hof_link` | character | URL to the coach's page on the Hockey Hall of Fame website, if inducted. |
| `in_hockey_hof` | logical | Whether the player is in the Hockey Hall of Fame. |
| `in_iihf_hockey_hof` | logical | Boolean flag indicating whether the coach is inducted into the IIHF Hockey Hall of Fame. |
| `in_us_hockey_hof` | logical | Whether the player is in the US Hockey Hall of Fame. |
| `instagram` | character | Instagram profile handle or URL associated with the coach. |
| `is_active` | logical | Whether the team is active. |
| `last_name` | character | Player last name. |
| `nationality_code` | character | Nationality code of the official. |
| `player_id` | integer | Unique player identifier. |
| `stanley_cup` | integer | Number of Stanley Cup championships won by the coach as a head coach. |
| `team_id` | character | Unique team identifier. |
| `top100_player_link` | character | URL to the coach's entry on the NHL's Top 100 Players list, if applicable. |
| `twitter` | character | Twitter (X) handle or URL associated with the coach. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_coach-example}

```python
nhl_records_coach(coach_id='X')
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_franchise` | integer | Indicator of whether the franchise is active. |
| `active_opponent_franchise` | integer | Flag indicating whether the opponent franchise is currently active in the NHL (1 = active, 0 = relocated or dissolved). |
| `franchise_name` | character | Franchise name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `home_games_played` | integer | Total number of home games the franchise has played all-time against this opponent franchise. |
| `home_goals_against` | double | Total goals allowed by the franchise in all-time home games against this opponent. |
| `home_goals_for` | double | Total goals scored by the franchise in all-time home games against this opponent. |
| `home_last_meeting_season_id` | integer | NHL season identifier for the most recent home game played against this opponent franchise. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | integer | Home overtime losses. |
| `home_points` | integer | Home team total points scored in the game so far. |
| `home_ties` | integer | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `opponent_franchise_id` | integer | NHL records identifier for the opposing franchise in this all-time head-to-head record. |
| `opponent_franchise_name` | character | Full name of the opposing franchise in this all-time head-to-head record. |
| `opponent_team_id` | integer | Opponent team identifier. |
| `road_games_played` | integer | Total number of road games the franchise has played all-time against this opponent franchise. |
| `road_goals_against` | integer | Total goals allowed by the franchise in all-time road games against this opponent. |
| `road_goals_for` | integer | Total goals scored by the franchise in all-time road games against this opponent. |
| `road_last_meeting_season_id` | integer | NHL season identifier for the most recent road game played against this opponent franchise. |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | integer | Road overtime losses. |
| `road_points` | integer | Total standings points earned by the franchise in all-time road games against this opponent. |
| `road_ties` | integer | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `team_franchise_id` | integer | Team franchise identifier. |
| `team_id` | integer | Unique team identifier. |
| `total_games_played` | integer | Total number of games played all-time between this franchise and the opponent franchise across home and road venues. |
| `total_goals_against` | integer | Total goals allowed by the franchise in all-time games against this opponent across home and road. |
| `total_goals_for` | integer | Total goals scored by the franchise in all-time games against this opponent across home and road. |
| `total_last_meeting_season_id` | integer | NHL season identifier for the most recent game played between the two franchises in any venue. |
| `total_losses` | integer | Total losses to date (goalie). |
| `total_ot_losses` | integer | Total number of overtime losses accumulated by the franchise all-time against this opponent. |
| `total_points` | integer | Total standings points earned by the franchise across all all-time games against this opponent. |
| `total_ties` | integer | Total ties. |
| `total_wins` | integer | Total wins. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_all_time_record_vs_franchise-example}

```python
nhl_records_all_time_record_vs_franchise()
```

_Last validated n/a._

## nhl_records_skater_career_stats

Skater career statistics (all-time, regular season).

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-statistics`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-statistics](https://records.nhl.com/site/api/skater-career-statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_skater_career_stats-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_skater_career_stats-example}

```python
nhl_records_skater_career_stats()
```

_Last validated n/a._

## nhl_records_skater_career_leaders

All-time skater career leaderboards.

**Endpoint URL:** `GET https://records.nhl.com/site/api/skater-career-leaders`

**Valid URL:** [https://records.nhl.com/site/api/skater-career-leaders](https://records.nhl.com/site/api/skater-career-leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#nhl_records_skater_career_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_nhl_records`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_skater_career_leaders-example}

```python
nhl_records_skater_career_leaders()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_player` | logical | Indicator of whether the player is active. |
| `active_streak` | logical | Indicator of whether the streak is active. |
| `consecutive100_point_seasons` | integer | Number of consecutive NHL regular seasons in which the player reached 100 or more points. |
| `first_name` | character | Player first name. |
| `franchise_id` | double | Unique franchise identifier. |
| `last_name` | character | Player last name. |
| `player_id` | integer | Unique player identifier. |
| `position_code` | character | Player position code. |
| `seasons_played` | integer | Number of seasons played. |
| `streak_end_season` | integer | The last season of the consecutive 100-point streak, encoded as an eight-digit season ID. |
| `streak_start_season` | integer | The first season of the consecutive 100-point streak, encoded as an eight-digit season ID. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `team_names` | character | Team names. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_consecutive_100pt_seasons-example}

```python
nhl_records_consecutive_100pt_seasons()
```

_Last validated n/a._

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

### Example {#nhl_records_draft_lottery_odds-example}

```python
nhl_records_draft_lottery_odds()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active` | logical | Whether athlete is currently active. |
| `draft_picks` | character | JSON-serialized list of players selected by this franchise in the NHL expansion draft. |
| `season_id` | integer | Season identifier. |
| `team_id` | integer | Unique team identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_expansion_draft_picks-example}

```python
nhl_records_expansion_draft_picks()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `playoff_attendance` | double | Total playoff attendance. |
| `regular_attendance` | double | Total regular-season attendance. |
| `season_id` | integer | Season identifier. |
| `total_attendance` | double | Total attendance for the season. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_attendance-example}

```python
nhl_records_attendance()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `date_inducted` | character | Date the inductee entered the Hall of Fame. |
| `induction_cat_id` | integer | Induction category identifier. |
| `misc_full_name` | character | Full name of the inductee. |
| `office_id` | integer | Office/category identifier. |
| `official_id` | character | ESPN official id (echoed from arg). |
| `player_id` | integer | Unique player identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_hof_players-example}

```python
nhl_records_hof_players()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `date_inducted` | character | Date the inductee entered the Hall of Fame. |
| `induction_cat_id` | integer | Induction category identifier. |
| `misc_full_name` | character | Full name of the inductee. |
| `office_id` | integer | Office/category identifier. |
| `official_id` | character | ESPN official id (echoed from arg). |
| `player_id` | character | Unique player identifier. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_hof_players_by_office-example}

```python
nhl_records_hof_players_by_office(office_id='X')
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_gm` | logical | Indicates whether the general manager is currently active in an NHL front-office role. |
| `end_date` | character | Season end date. |
| `end_season_id` | integer | Season identifier (e.g., 20232024) for the last season the GM held the position. |
| `first_name` | character | Player first name. |
| `full_name` | character | Player full name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `gm_of_the_year` | integer | Number of times the general manager won the NHL GM of the Year Award during their career. |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer | Number of games the GM's team lost in overtime during their tenure. |
| `losses_in_ot_plus_shootout` | integer | Combined total of overtime and shootout losses recorded during the GM's tenure. |
| `losses_in_shootout` | double | Number of games the GM's team lost in the shootout portion of a tied game. |
| `overtime_losses` | integer | Total overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_games` | integer | Total number of away games played by the GM's team across their tenure. |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer | Total number of NHL seasons the general manager has served in the role. |
| `stanley_cup_final_appearances` | integer | Number of times the GM's team reached the Stanley Cup Final during their tenure. |
| `stanley_cups` | integer | Number of Stanley Cup championships won by the GM's franchise during their tenure. |
| `start_date` | character | Season start date. |
| `start_season_id` | integer | Season identifier (e.g., 20052006) for the first season the GM held the position. |
| `team_abbrevs` | character | Team abbreviation(s). |
| `ties` | integer | Total ties. |
| `ties_in_ot` | integer | Number of overtime ties recorded under legacy rules during the GM's tenure. |
| `win_pctg` | double | Career winning percentage for the GM, calculated as wins divided by total games decided. |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer | Number of games the GM's team won in overtime during their tenure. |
| `wins_in_ot_plus_shootout` | integer | Combined total of overtime and shootout wins recorded during the GM's tenure. |
| `wins_in_shootout` | double | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_gm_career-example}

```python
nhl_records_gm_career()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `active_gm` | logical | Indicates whether the general manager is currently active with the franchise. |
| `end_date` | character | Season end date. |
| `end_season_id` | integer | NHL season identifier for the last season the GM held the role with this franchise. |
| `first_name` | character | Player first name. |
| `franchise_id` | integer | Unique franchise identifier. |
| `franchise_name` | character | Franchise name. |
| `full_name` | character | Player full name. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games` | integer | Games played. |
| `gm_of_the_year` | integer | Number of NHL General Manager of the Year awards won during this franchise tenure. |
| `home_games` | integer | Total home games. |
| `home_losses` | integer | Losses at home. |
| `home_ot_losses` | double | Home overtime losses. |
| `home_ties` | double | Ties at home. |
| `home_wins` | integer | Wins at home. |
| `last_name` | character | Player last name. |
| `losses` | integer | Losses. |
| `losses_in_ot` | integer | Number of regular-season overtime losses recorded by the franchise under this GM. |
| `losses_in_ot_plus_shootout` | integer | Combined overtime and shootout losses for the franchise during this GM's tenure. |
| `losses_in_shootout` | double | Number of regular-season shootout losses recorded by the franchise under this GM. |
| `overtime_losses` | integer | Total overtime losses. |
| `point_pctg` | double | Points percentage. |
| `points` | integer | Total points (goals + assists). |
| `road_games` | integer | Total regular-season away games played by the franchise during this GM's tenure. |
| `road_losses` | integer | Losses on the road. |
| `road_ot_losses` | double | Road overtime losses. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | integer | Wins on the road. |
| `seasons` | integer | Number of NHL seasons the GM held the role with this franchise. |
| `stanley_cup_final_appearances` | integer | Number of Stanley Cup Final appearances by the franchise during this GM's tenure. |
| `stanley_cups` | integer | Number of Stanley Cup championships won by the franchise under this GM. |
| `start_date` | character | Season start date. |
| `start_season_id` | integer | NHL season identifier for the first season the GM held the role with this franchise. |
| `team_id` | integer | Unique team identifier. |
| `team_name` | character | Team name. |
| `ties` | integer | Total ties. |
| `ties_in_ot` | integer | Number of overtime ties recorded by the franchise under this GM (pre-shootout era). |
| `win_pctg` | double | Overall win percentage for the franchise across all regular-season games during this GM's tenure. |
| `wins` | integer | Wins. |
| `wins_in_ot` | integer | Number of regular-season overtime wins recorded by the franchise under this GM. |
| `wins_in_ot_plus_shootout` | integer | Combined overtime and shootout wins for the franchise during this GM's tenure. |
| `wins_in_shootout` | double | Wins in shootout. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#nhl_records_gm_franchise-example}

```python
nhl_records_gm_franchise()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `goals_against_per_game` | double | Goals against per game. |
| `goals_per_game` | double | Average number of goals the team scored per home game over the recorded period. |
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

### Example {#nhl_records_home_team_record-example}

```python
nhl_records_home_team_record()
```

_Last validated n/a._

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
| `id` | integer | Unique player identifier. |
| `franchise_id` | integer | Unique franchise identifier. |
| `game_type_id` | integer | Game type identifier (regular/playoffs). |
| `games_played` | integer | Games played. |
| `goals` | integer | Goals scored. |
| `goals_against` | integer | Goals against. |
| `goals_against_per_game` | double | Goals against per game. |
| `goals_per_game` | double | Average number of goals the team scored per road game over the recorded period. |
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

### Example {#nhl_records_away_team_record-example}

```python
nhl_records_away_team_record()
```

_Last validated n/a._
