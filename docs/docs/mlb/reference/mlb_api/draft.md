---
title: "MLB — MLB Stats API — Draft"
sidebar_label: "Draft"
sidebar_position: 2
description: "MLB — MLB Stats API — Draft — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Stats API — Draft

## mlb_draft

GET /api/v1/draft/{year} — draft results for a year (optionally one round).

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2024?limit=100](https://statsapi.mlb.com/api/v1/draft/2024?limit=100)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `round` | `round_` |  |  | `Y` | round query parameter. |
| `teamId` | `team_id` |  |  | `Y` | teamId query parameter. |
| `playerId` | `player_id` |  |  | `Y` | playerId query parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#mlb_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_api_list`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_draft-example}

```python
mlb_draft(year=2024)
```

_Last validated n/a._

## mlb_draft_latest

View latest player drafted, endpoint best used when draft is currently open.

**Endpoint URL:** `GET https://statsapi.mlb.com/api/v1/draft/{year}/latest`

**Valid URL:** [https://statsapi.mlb.com/api/v1/draft/2023/latest](https://statsapi.mlb.com/api/v1/draft/2023/latest)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#mlb_draft_latest-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `number` | integer | Jersey number. |
| `next_up` | character | Indicates whether this draft slot is the next pick to be made in the current draft. |
| `pick_pick_round` | character | Draft round in which this pick was made (e.g., '1', '2', 'CB-A' for competitive balance). |
| `pick_pick_number` | integer | Overall pick number of this selection counting sequentially across all rounds of the draft. |
| `pick_display_pick_number` | integer | The formatted overall pick number displayed publicly for this draft selection. |
| `pick_round_pick_number` | integer | Pick number within the specific draft round (i.e., the Nth pick in that round). |
| `pick_signing_bonus` | character | Reported or slotted signing bonus amount associated with this draft pick. |
| `pick_home_city` | character | City of the draftee's listed home address at the time of the draft. |
| `pick_home_state` | character | State or province of the draftee's listed home address at the time of the draft. |
| `pick_home_country` | character | Country of the draftee's listed home address at the time of the draft. |
| `pick_school_name` | character | Name of the high school or college the draftee attended before being drafted. |
| `pick_school_school_class` | character | Academic class or level of the draftee at their school (e.g., High School, Junior, Senior). |
| `pick_school_city` | character | City of the high school or college the draftee attended before being drafted. |
| `pick_school_country` | character | Country of the school the draftee attended. |
| `pick_school_state` | character | State of the school the draftee attended. |
| `pick_headshot_link` | character | URL to the headshot image of the drafted player on the MLB Stats API CDN. |
| `pick_person_id` | integer | Unique MLB Stats API (MLBAM) identifier for the drafted player. |
| `pick_person_full_name` | character | Player's complete display name as used throughout the MLB Stats API. |
| `pick_person_link` | character | Relative URL path to the player's resource in the MLB Stats API. |
| `pick_person_first_name` | character | The player's legal or preferred first name. |
| `pick_person_last_name` | character | The player's legal or preferred last name. |
| `pick_person_birth_date` | character | Date of birth of the drafted player in ISO 8601 format. |
| `pick_person_current_age` | integer | Age of the drafted player in years at the time of the data retrieval. |
| `pick_person_birth_city` | character | City where the drafted player was born. |
| `pick_person_birth_state_province` | character | State or province where the drafted player was born. |
| `pick_person_birth_country` | character | Country where the drafted player was born. |
| `pick_person_height` | character | Player's height in feet-and-inches notation (e.g., 6' 2"). |
| `pick_person_weight` | integer | Player's weight in pounds as recorded by the MLB Stats API. |
| `pick_person_active` | logical | Boolean flag indicating whether the drafted player is currently on an active MLB roster. |
| `pick_person_primary_position_code` | character | Numeric or short code identifying the player's primary fielding position. |
| `pick_person_primary_position_name` | character | Full name of the player's primary fielding position (e.g., Shortstop, Center Field). |
| `pick_person_primary_position_type` | character | Broad classification of the player's position role (e.g., Pitcher, Infielder, Outfielder). |
| `pick_person_primary_position_abbreviation` | character | Short abbreviation for the player's primary fielding position (e.g., SS, CF, SP). |
| `pick_person_use_name` | character | The first name or nickname the player prefers to use publicly. |
| `pick_person_use_last_name` | character | The last name the player prefers to use publicly, which may differ from the legal last name. |
| `pick_person_middle_name` | character | The player's middle name as recorded by the MLB Stats API. |
| `pick_person_boxscore_name` | character | Abbreviated name format used for the player on official MLB box scores. |
| `pick_person_gender` | character | Recorded gender of the drafted player. |
| `pick_person_is_player` | logical | Boolean flag indicating whether this person is classified as an active player in the MLB Stats API. |
| `pick_person_is_verified` | logical | Boolean flag indicating whether the player's profile has been verified by MLB. |
| `pick_person_draft_year` | integer | The MLB draft year in which this player was originally selected. |
| `pick_person_bat_side_code` | character | Single-letter code for the player's batting handedness (e.g., R, L, S for switch). |
| `pick_person_bat_side_description` | character | Full description of the player's batting side (e.g., Right, Left, Switch). |
| `pick_person_pitch_hand_code` | character | Single-letter code for the player's pitching handedness (e.g., R, L, S). |
| `pick_person_pitch_hand_description` | character | Full description of the player's pitching hand (e.g., Right, Left, Switch). |
| `pick_person_name_first_last` | character | Player's display name in first-last format, typically matching the broadcast name. |
| `pick_person_name_slug` | character | URL-safe slug derived from the player's name for use in web links. |
| `pick_person_first_last_name` | character | Player's name formatted as first name followed by last name. |
| `pick_person_last_first_name` | character | Player's name formatted as last name followed by first name. |
| `pick_person_last_init_name` | character | Player's name formatted as last name followed by first initial. |
| `pick_person_init_last_name` | character | Player's name formatted as first initial followed by last name (e.g., J. Smith). |
| `pick_person_full_fml_name` | character | Player's full name in first-middle-last order as recorded by the MLB Stats API. |
| `pick_person_full_lfm_name` | character | Player's full name in last-first-middle order as recorded by the MLB Stats API. |
| `pick_person_strike_zone_top` | double | Upper boundary of the player's personalized strike zone in feet from the ground. |
| `pick_person_strike_zone_bottom` | double | Lower boundary of the player's personalized strike zone in feet from the ground. |
| `pick_person_xref_ids` | character | Serialized cross-reference identifiers linking the player to external data systems. |
| `pick_team_spring_league_id` | integer | MLB Stats API identifier for the team's spring training league. |
| `pick_team_spring_league_name` | character | Full name of the spring training league the team belongs to. |
| `pick_team_spring_league_link` | character | Relative URL path to the spring training league resource in the MLB Stats API. |
| `pick_team_spring_league_abbreviation` | character | Abbreviation for the spring training league the team participates in (e.g., Cactus, Grapefruit). |
| `pick_team_all_star_status` | character | All-Star game affiliation status of the team (e.g., American League, National League). |
| `pick_team_id` | integer | Unique MLB Stats API identifier for the team that made this draft pick. |
| `pick_team_name` | character | Full official name of the MLB team that made this pick (e.g., New York Yankees). |
| `pick_team_link` | character | Relative URL path to the team resource in the MLB Stats API. |
| `pick_team_season` | integer | MLB season year for which this team's metadata snapshot applies. |
| `pick_team_venue_id` | integer | MLB Stats API identifier for the team's regular-season home ballpark. |
| `pick_team_venue_name` | character | Name of the team's regular-season home ballpark (e.g., Yankee Stadium). |
| `pick_team_venue_link` | character | Relative URL path to the team's regular-season home venue in the MLB Stats API. |
| `pick_team_spring_venue_id` | integer | MLB Stats API identifier for the team's spring training ballpark. |
| `pick_team_spring_venue_link` | character | Relative URL path to the spring training venue resource in the MLB Stats API. |
| `pick_team_team_code` | character | Short internal code used by MLB to identify the team in system contexts. |
| `pick_team_file_code` | character | Lowercase file-system-safe code used internally by MLB to identify the team. |
| `pick_team_abbreviation` | character | Standard two- or three-letter abbreviation for the MLB team that made this pick. |
| `pick_team_team_name` | character | The nickname portion of the team's full name (e.g., Yankees, Dodgers). |
| `pick_team_location_name` | character | Geographic location name (city/metro) associated with the team (e.g., New York). |
| `pick_team_first_year_of_play` | character | Year in which the selecting franchise first played as an MLB team. |
| `pick_team_league_id` | integer | MLB Stats API identifier for the league (American or National) of the selecting team. |
| `pick_team_league_name` | character | Full name of the league the selecting team belongs to (e.g., American League). |
| `pick_team_league_link` | character | Relative URL path to the league resource in the MLB Stats API. |
| `pick_team_division_id` | integer | MLB Stats API identifier for the division the selecting team belongs to. |
| `pick_team_division_name` | character | Full name of the division the selecting team belongs to (e.g., AL East). |
| `pick_team_division_link` | character | Relative URL path to the division resource in the MLB Stats API. |
| `pick_team_sport_id` | integer | MLB Stats API identifier for the sport classification (MLB = 1). |
| `pick_team_sport_link` | character | Relative URL path to the sport resource in the MLB Stats API. |
| `pick_team_sport_name` | character | Full name of the sport classification for the team (e.g., Major League Baseball). |
| `pick_team_short_name` | character | Shortened version of the team name used in space-constrained display contexts. |
| `pick_team_franchise_name` | character | Historical franchise name that persists across any team relocations or renames. |
| `pick_team_club_name` | character | Informal club or nickname portion of the team's full name (e.g., Yankees, Red Sox). |
| `pick_team_active` | logical | Boolean flag indicating whether the selecting MLB franchise is currently active. |
| `pick_draft_type_code` | character | Short code identifying the type of draft (e.g., amateur, Rule 5) for this pick. |
| `pick_draft_type_description` | character | Human-readable description of the draft type associated with this pick. |
| `pick_is_drafted` | logical | Boolean flag indicating whether this draft slot has been filled with an actual selection. |
| `pick_is_pass` | logical | Boolean flag indicating whether the selecting team passed on this pick rather than making a selection. |
| `pick_year` | character | MLB draft year for which this pick record applies. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_draft_latest-example}

```python
mlb_draft_latest(year=2023)
```

_Last validated n/a._
