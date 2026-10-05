---
title: "MLB — MLB Stats API — Home"
sidebar_label: "Home"
sidebar_position: 4
description: "MLB — MLB Stats API — Home — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Home

## mlb_home_run_derby

View a home run derby object based on gamePk.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101](https://statsapi.mlb.com/api/v1/homeRunDerby/511101)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_home_run_derby-returns}

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
| `birth_state_province` | character | State or province of birth. |
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
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `stats` | character | Stats. |
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
| `current_team_spring_league_id` | double | The MLB Stats API numeric identifier for the spring training league of the player's current team. |
| `current_team_spring_league_name` | character | The full name of the spring training league (e.g., 'Cactus League') for the player's current team. |
| `current_team_spring_league_link` | character | The MLB Stats API relative URL linking to the spring training league resource for the player's current team. |
| `current_team_spring_league_abbreviation` | character | The abbreviation for the Cactus League or Grapefruit League in which the player's current team participates during spring training. |
| `current_team_all_star_status` | character | The All-Star designation status of the player's current team (e.g., which league's All-Star pool the team belongs to). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer | The MLB season year for which the player's current team metadata is reported. |
| `current_team_venue_id` | integer | The MLB Stats API numeric identifier for the regular-season home ballpark of the player's current team. |
| `current_team_venue_name` | character | The official name of the regular-season home ballpark for the player's current team. |
| `current_team_venue_link` | character | The MLB Stats API relative URL linking to the regular-season venue resource for the player's current team. |
| `current_team_spring_venue_id` | double | The MLB Stats API numeric identifier for the spring training ballpark used by the player's current team. |
| `current_team_spring_venue_link` | character | The MLB Stats API relative URL linking to the spring training venue resource for the player's current team. |
| `current_team_team_code` | character | The three-letter internal team code used by MLB in legacy data systems and some API references. |
| `current_team_file_code` | character | The lowercase alphabetic file code used by MLB for identifying the team in media and data assets. |
| `current_team_abbreviation` | character | The standard two- or three-letter abbreviation for the player's current MLB team (e.g., 'NYY', 'LAD'). |
| `current_team_team_name` | character | The full official name of the player's current team, including both city and nickname. |
| `current_team_location_name` | character | The city or metropolitan area name associated with the player's current team. |
| `current_team_first_year_of_play` | character | The calendar year in which the player's current franchise first played MLB games. |
| `current_team_league_id` | integer | The MLB Stats API numeric identifier for the league (American League or National League) of the player's current team. |
| `current_team_league_name` | character | The full name of the league (e.g., 'American League') in which the player's current team competes. |
| `current_team_league_link` | character | The MLB Stats API relative URL linking to the league resource for the player's current team. |
| `current_team_division_id` | double | The MLB Stats API numeric identifier for the division in which the player's current team competes. |
| `current_team_division_name` | character | The full name of the division in which the player's current team competes (e.g., 'American League East'). |
| `current_team_division_link` | character | The MLB Stats API relative URL linking to the division resource for the player's current team. |
| `current_team_sport_id` | integer | The MLB Stats API numeric identifier for the sport classification (e.g., 1 for MLB) of the player's current team. |
| `current_team_sport_link` | character | The MLB Stats API relative URL linking to the sport resource associated with the player's current team. |
| `current_team_sport_name` | character | The name of the sport classification for the player's current team (e.g., 'Major League Baseball'). |
| `current_team_short_name` | character | A shortened display name for the player's current team, often used in space-constrained UI contexts. |
| `current_team_franchise_name` | character | The historical franchise name for the player's current team, which may differ from the current team name for relocated clubs. |
| `current_team_club_name` | character | The short club nickname for the player's current team, typically the city-less portion of the franchise name. |
| `current_team_active` | logical | Boolean flag indicating whether the player's current team is an active MLB franchise. |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |
| `current_team_parent_org_name` | character | The name of the parent major-league organization for the player's current team. |
| `current_team_parent_org_id` | double | The MLB Stats API numeric identifier for the parent organization (major-league affiliate) of the player's current team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_home_run_derby-example}

```python
mlb_home_run_derby(game_pk=511101)
```

_Last validated n/a._

## mlb_home_run_derby_bracket

View a home run derby object based on bracket.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/bracket`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101/bracket](https://statsapi.mlb.com/api/v1/homeRunDerby/511101/bracket)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_home_run_derby_bracket-returns}

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
| `birth_state_province` | character | State or province of birth. |
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
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `stats` | character | Stats. |
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
| `current_team_spring_league_id` | double | MLB Stats API identifier for the spring training league of the participant's current team. |
| `current_team_spring_league_name` | character | Full name of the spring training league the participant's team belongs to. |
| `current_team_spring_league_link` | character | Relative URL path to the spring training league resource in the MLB Stats API. |
| `current_team_spring_league_abbreviation` | character | Abbreviation for the spring training league the participant's team belongs to (e.g., Cactus, Grapefruit). |
| `current_team_all_star_status` | character | All-Star game league affiliation of the participant's current team (e.g., American League, National League). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer | MLB season year for which the participant's current team metadata snapshot applies. |
| `current_team_venue_id` | integer | MLB Stats API identifier for the participant's current team's regular-season home ballpark. |
| `current_team_venue_name` | character | Name of the participant's current team's regular-season home ballpark. |
| `current_team_venue_link` | character | Relative URL path to the current team's home venue resource in the MLB Stats API. |
| `current_team_spring_venue_id` | double | MLB Stats API identifier for the spring training ballpark used by the participant's team. |
| `current_team_spring_venue_link` | character | Relative URL path to the spring training venue resource in the MLB Stats API. |
| `current_team_team_code` | character | Short internal code used by MLB to identify the participant's current team in system contexts. |
| `current_team_file_code` | character | Lowercase file-system-safe code used by MLB to identify the participant's current team. |
| `current_team_abbreviation` | character | Standard two- or three-letter abbreviation for the Home Run Derby participant's current MLB team. |
| `current_team_team_name` | character | The nickname portion of the participant's current team name (e.g., Red Sox, Braves). |
| `current_team_location_name` | character | City or metropolitan area name associated with the participant's current team. |
| `current_team_first_year_of_play` | character | Year in which the participant's current franchise first played as an MLB team. |
| `current_team_league_id` | integer | MLB Stats API identifier for the league (American or National) of the participant's current team. |
| `current_team_league_name` | character | Full name of the league the participant's current team belongs to (e.g., National League). |
| `current_team_league_link` | character | Relative URL path to the league resource for the participant's current team in the MLB Stats API. |
| `current_team_division_id` | double | MLB Stats API identifier for the division the participant's current team belongs to. |
| `current_team_division_name` | character | Full name of the division the participant's current team belongs to (e.g., AL East). |
| `current_team_division_link` | character | Relative URL path to the participant's current team's division resource in the MLB Stats API. |
| `current_team_sport_id` | integer | MLB Stats API identifier for the sport classification of the participant's current team (MLB = 1). |
| `current_team_sport_link` | character | Relative URL path to the sport resource for the participant's current team in the MLB Stats API. |
| `current_team_sport_name` | character | Full sport classification name for the participant's current team (e.g., Major League Baseball). |
| `current_team_short_name` | character | Shortened display name of the participant's current team for space-constrained contexts. |
| `current_team_franchise_name` | character | Historical franchise name for the participant's team, persisting across relocations. |
| `current_team_club_name` | character | Informal nickname portion of the participant's current team name (e.g., Yankees, Dodgers). |
| `current_team_active` | logical | Boolean flag indicating whether the participant's current franchise is an active MLB organization. |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |
| `current_team_parent_org_name` | character | Name of the parent MLB organization for the participant's current team. |
| `current_team_parent_org_id` | double | MLB Stats API identifier for the parent MLB organization of the participant's current team, relevant for minor league affiliates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_home_run_derby_bracket-example}

```python
mlb_home_run_derby_bracket(game_pk=511101)
```

_Last validated n/a._

## mlb_home_run_derby_pool

View a home run derby object based on pool.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/homeRunDerby/{game_pk}/pool`

**Valid URL:** [https://statsapi.mlb.com/api/v1/homeRunDerby/511101/pool](https://statsapi.mlb.com/api/v1/homeRunDerby/511101/pool)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  | `Y` |  | game_pk path parameter. |
| `fields` | `fields` |  |  | `Y` | fields query parameter. |

### Returns {#mlb_home_run_derby_pool-returns}

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
| `birth_state_province` | character | State or province of birth. |
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
| `is_player` | logical | Whether the person is a player. |
| `is_verified` | logical | Whether the player profile is verified. |
| `draft_year` | double | Year the player was drafted. |
| `pronunciation` | character | Phonetic name pronunciation. |
| `stats` | character | Stats. |
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
| `current_team_spring_league_id` | double | The MLB Stats API numeric identifier for the spring training league of the pool participant's current team. |
| `current_team_spring_league_name` | character | The full name of the spring training league for the pool participant's current team. |
| `current_team_spring_league_link` | character | The MLB Stats API relative URL linking to the spring training league resource for the pool participant's current team. |
| `current_team_spring_league_abbreviation` | character | The abbreviation for the spring training league in which the pool participant's current team plays during spring training. |
| `current_team_all_star_status` | character | The All-Star designation status of the pool participant's current team (e.g., which league's All-Star pool the team belongs to). |
| `current_team_id` | integer | Current team MLBAM ID. |
| `current_team_name` | character | Current team name. |
| `current_team_link` | character | API link to the current team. |
| `current_team_season` | integer | The MLB season year for which the pool participant's current team metadata is reported. |
| `current_team_venue_id` | integer | The MLB Stats API numeric identifier for the regular-season home ballpark of the pool participant's current team. |
| `current_team_venue_name` | character | The official name of the regular-season home ballpark for the pool participant's current team. |
| `current_team_venue_link` | character | The MLB Stats API relative URL linking to the regular-season venue resource for the pool participant's current team. |
| `current_team_spring_venue_id` | double | The MLB Stats API numeric identifier for the spring training ballpark used by the pool participant's current team. |
| `current_team_spring_venue_link` | character | The MLB Stats API relative URL linking to the spring training venue resource for the pool participant's current team. |
| `current_team_team_code` | character | The three-letter internal team code used by MLB for the pool participant's team in legacy data systems. |
| `current_team_file_code` | character | The lowercase alphabetic file code used by MLB for identifying the pool participant's team in media and data assets. |
| `current_team_abbreviation` | character | The standard two- or three-letter abbreviation for the Home Run Derby pool participant's current MLB team. |
| `current_team_team_name` | character | The full official name of the pool participant's current team, including both city and nickname. |
| `current_team_location_name` | character | The city or metropolitan area name associated with the pool participant's current team. |
| `current_team_first_year_of_play` | character | The calendar year in which the pool participant's current franchise first played MLB games. |
| `current_team_league_id` | integer | The MLB Stats API numeric identifier for the league of the pool participant's current team. |
| `current_team_league_name` | character | The full name of the league (e.g., 'National League') in which the pool participant's current team competes. |
| `current_team_league_link` | character | The MLB Stats API relative URL linking to the league resource for the pool participant's current team. |
| `current_team_division_id` | double | The MLB Stats API numeric identifier for the division in which the pool participant's current team competes. |
| `current_team_division_name` | character | The full name of the division in which the pool participant's current team competes (e.g., 'National League West'). |
| `current_team_division_link` | character | The MLB Stats API relative URL linking to the division resource for the pool participant's current team. |
| `current_team_sport_id` | integer | The MLB Stats API numeric identifier for the sport classification of the pool participant's current team. |
| `current_team_sport_link` | character | The MLB Stats API relative URL linking to the sport resource associated with the pool participant's current team. |
| `current_team_sport_name` | character | The name of the sport classification for the pool participant's current team (e.g., 'Major League Baseball'). |
| `current_team_short_name` | character | A shortened display name for the pool participant's current team, often used in space-constrained UI contexts. |
| `current_team_franchise_name` | character | The historical franchise name for the pool participant's current team, which may differ from the current team name for relocated clubs. |
| `current_team_club_name` | character | The short club nickname for the pool participant's current team, typically the city-less portion of the franchise name. |
| `current_team_active` | logical | Boolean flag indicating whether the pool participant's current team is an active MLB franchise. |
| `primary_position_code` | character | Primary position code. |
| `primary_position_name` | character | Primary fielding position name. |
| `primary_position_type` | character | Primary position type (e.g. Infielder). |
| `primary_position_abbreviation` | character | Primary position abbreviation. |
| `bat_side_code` | character | Batting side code (L/R/S). |
| `bat_side_description` | character | Batting side description. |
| `pitch_hand_code` | character | Throwing hand code (L/R). |
| `pitch_hand_description` | character | Throwing hand description. |
| `last_played_date` | character | Date of last MLB game played. |
| `name_matrilineal` | character | Maternal family name. |
| `current_team_parent_org_name` | character | The name of the parent major-league organization for the pool participant's current team. |
| `current_team_parent_org_id` | double | The MLB Stats API numeric identifier for the parent organization of the pool participant's current team. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_home_run_derby_pool-example}

```python
mlb_home_run_derby_pool(game_pk=511101)
```

_Last validated n/a._
