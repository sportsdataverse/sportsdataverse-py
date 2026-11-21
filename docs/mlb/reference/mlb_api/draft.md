# MLB — MLB Stats API — Draft

> MLB — MLB Stats API — Draft — function reference in sdv-py, the SportsDataverse Python package.

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

### Raises {#mlb_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_draft-example}

```python
mlb_draft(year=2024)
```

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
| `number` | Int64 | Jersey number. |
| `next_up` | String | Indicates whether this draft slot is the next pick to be made in the current draft. |
| `pick_pick_round` | String | Draft round in which this pick was made (e.g., '1', '2', 'CB-A' for competitive balance). |
| `pick_pick_number` | Int64 | Overall pick number of this selection counting sequentially across all rounds of the draft. |
| `pick_display_pick_number` | Int64 | The formatted overall pick number displayed publicly for this draft selection. |
| `pick_round_pick_number` | Int64 | Pick number within the specific draft round (i.e., the Nth pick in that round). |
| `pick_signing_bonus` | String | Reported or slotted signing bonus amount associated with this draft pick. |
| `pick_home_city` | String | City of the draftee's listed home address at the time of the draft. |
| `pick_home_state` | String | State or province of the draftee's listed home address at the time of the draft. |
| `pick_home_country` | String | Country of the draftee's listed home address at the time of the draft. |
| `pick_school_name` | String | Name of the high school or college the draftee attended before being drafted. |
| `pick_school_school_class` | String | Academic class or level of the draftee at their school (e.g., High School, Junior, Senior). |
| `pick_school_city` | String | City of the high school or college the draftee attended before being drafted. |
| `pick_school_country` | String | Country of the school the draftee attended. |
| `pick_school_state` | String | State of the school the draftee attended. |
| `pick_headshot_link` | String | URL to the headshot image of the drafted player on the MLB Stats API CDN. |
| `pick_person_id` | Int64 | Unique MLB Stats API (MLBAM) identifier for the drafted player. |
| `pick_person_full_name` | String | Player's complete display name as used throughout the MLB Stats API. |
| `pick_person_link` | String | Relative URL path to the player's resource in the MLB Stats API. |
| `pick_person_first_name` | String | The player's legal or preferred first name. |
| `pick_person_last_name` | String | The player's legal or preferred last name. |
| `pick_person_birth_date` | String | Date of birth of the drafted player in ISO 8601 format. |
| `pick_person_current_age` | Int64 | Age of the drafted player in years at the time of the data retrieval. |
| `pick_person_birth_city` | String | City where the drafted player was born. |
| `pick_person_birth_state_province` | String | State or province where the drafted player was born. |
| `pick_person_birth_country` | String | Country where the drafted player was born. |
| `pick_person_height` | String | Player's height in feet-and-inches notation (e.g., 6' 2"). |
| `pick_person_weight` | Int64 | Player's weight in pounds as recorded by the MLB Stats API. |
| `pick_person_active` | Boolean | Boolean flag indicating whether the drafted player is currently on an active MLB roster. |
| `pick_person_primary_position_code` | String | Numeric or short code identifying the player's primary fielding position. |
| `pick_person_primary_position_name` | String | Full name of the player's primary fielding position (e.g., Shortstop, Center Field). |
| `pick_person_primary_position_type` | String | Broad classification of the player's position role (e.g., Pitcher, Infielder, Outfielder). |
| `pick_person_primary_position_abbreviation` | String | Short abbreviation for the player's primary fielding position (e.g., SS, CF, SP). |
| `pick_person_use_name` | String | The first name or nickname the player prefers to use publicly. |
| `pick_person_use_last_name` | String | The last name the player prefers to use publicly, which may differ from the legal last name. |
| `pick_person_middle_name` | String | The player's middle name as recorded by the MLB Stats API. |
| `pick_person_boxscore_name` | String | Abbreviated name format used for the player on official MLB box scores. |
| `pick_person_gender` | String | Recorded gender of the drafted player. |
| `pick_person_is_player` | Boolean | Boolean flag indicating whether this person is classified as an active player in the MLB Stats API. |
| `pick_person_is_verified` | Boolean | Boolean flag indicating whether the player's profile has been verified by MLB. |
| `pick_person_draft_year` | Int64 | The MLB draft year in which this player was originally selected. |
| `pick_person_bat_side_code` | String | Single-letter code for the player's batting handedness (e.g., R, L, S for switch). |
| `pick_person_bat_side_description` | String | Full description of the player's batting side (e.g., Right, Left, Switch). |
| `pick_person_pitch_hand_code` | String | Single-letter code for the player's pitching handedness (e.g., R, L, S). |
| `pick_person_pitch_hand_description` | String | Full description of the player's pitching hand (e.g., Right, Left, Switch). |
| `pick_person_name_first_last` | String | Player's display name in first-last format, typically matching the broadcast name. |
| `pick_person_name_slug` | String | URL-safe slug derived from the player's name for use in web links. |
| `pick_person_first_last_name` | String | Player's name formatted as first name followed by last name. |
| `pick_person_last_first_name` | String | Player's name formatted as last name followed by first name. |
| `pick_person_last_init_name` | String | Player's name formatted as last name followed by first initial. |
| `pick_person_init_last_name` | String | Player's name formatted as first initial followed by last name (e.g., J. Smith). |
| `pick_person_full_fml_name` | String | Player's full name in first-middle-last order as recorded by the MLB Stats API. |
| `pick_person_full_lfm_name` | String | Player's full name in last-first-middle order as recorded by the MLB Stats API. |
| `pick_person_strike_zone_top` | Float64 | Upper boundary of the player's personalized strike zone in feet from the ground. |
| `pick_person_strike_zone_bottom` | Float64 | Lower boundary of the player's personalized strike zone in feet from the ground. |
| `pick_person_xref_ids` | String | Serialized cross-reference identifiers linking the player to external data systems. |
| `pick_team_spring_league_id` | Int64 | MLB Stats API identifier for the team's spring training league. |
| `pick_team_spring_league_name` | String | Full name of the spring training league the team belongs to. |
| `pick_team_spring_league_link` | String | Relative URL path to the spring training league resource in the MLB Stats API. |
| `pick_team_spring_league_abbreviation` | String | Abbreviation for the spring training league the team participates in (e.g., Cactus, Grapefruit). |
| `pick_team_all_star_status` | String | All-Star game affiliation status of the team (e.g., American League, National League). |
| `pick_team_id` | Int64 | Unique MLB Stats API identifier for the team that made this draft pick. |
| `pick_team_name` | String | Full official name of the MLB team that made this pick (e.g., New York Yankees). |
| `pick_team_link` | String | Relative URL path to the team resource in the MLB Stats API. |
| `pick_team_season` | Int64 | MLB season year for which this team's metadata snapshot applies. |
| `pick_team_venue_id` | Int64 | MLB Stats API identifier for the team's regular-season home ballpark. |
| `pick_team_venue_name` | String | Name of the team's regular-season home ballpark (e.g., Yankee Stadium). |
| `pick_team_venue_link` | String | Relative URL path to the team's regular-season home venue in the MLB Stats API. |
| `pick_team_spring_venue_id` | Int64 | MLB Stats API identifier for the team's spring training ballpark. |
| `pick_team_spring_venue_link` | String | Relative URL path to the spring training venue resource in the MLB Stats API. |
| `pick_team_team_code` | String | Short internal code used by MLB to identify the team in system contexts. |
| `pick_team_file_code` | String | Lowercase file-system-safe code used internally by MLB to identify the team. |
| `pick_team_abbreviation` | String | Standard two- or three-letter abbreviation for the MLB team that made this pick. |
| `pick_team_team_name` | String | The nickname portion of the team's full name (e.g., Yankees, Dodgers). |
| `pick_team_location_name` | String | Geographic location name (city/metro) associated with the team (e.g., New York). |
| `pick_team_first_year_of_play` | String | Year in which the selecting franchise first played as an MLB team. |
| `pick_team_league_id` | Int64 | MLB Stats API identifier for the league (American or National) of the selecting team. |
| `pick_team_league_name` | String | Full name of the league the selecting team belongs to (e.g., American League). |
| `pick_team_league_link` | String | Relative URL path to the league resource in the MLB Stats API. |
| `pick_team_division_id` | Int64 | MLB Stats API identifier for the division the selecting team belongs to. |
| `pick_team_division_name` | String | Full name of the division the selecting team belongs to (e.g., AL East). |
| `pick_team_division_link` | String | Relative URL path to the division resource in the MLB Stats API. |
| `pick_team_sport_id` | Int64 | MLB Stats API identifier for the sport classification (MLB = 1). |
| `pick_team_sport_link` | String | Relative URL path to the sport resource in the MLB Stats API. |
| `pick_team_sport_name` | String | Full name of the sport classification for the team (e.g., Major League Baseball). |
| `pick_team_short_name` | String | Shortened version of the team name used in space-constrained display contexts. |
| `pick_team_franchise_name` | String | Historical franchise name that persists across any team relocations or renames. |
| `pick_team_club_name` | String | Informal club or nickname portion of the team's full name (e.g., Yankees, Red Sox). |
| `pick_team_active` | Boolean | Boolean flag indicating whether the selecting MLB franchise is currently active. |
| `pick_draft_type_code` | String | Short code identifying the type of draft (e.g., amateur, Rule 5) for this pick. |
| `pick_draft_type_description` | String | Human-readable description of the draft type associated with this pick. |
| `pick_is_drafted` | Boolean | Boolean flag indicating whether this draft slot has been filled with an actual selection. |
| `pick_is_pass` | Boolean | Boolean flag indicating whether the selecting team passed on this pick rather than making a selection. |
| `pick_year` | String | MLB draft year for which this pick record applies. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_draft_latest-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_draft_latest-example}

```python
mlb_draft_latest(year=2023)
```
