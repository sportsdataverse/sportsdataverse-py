# WNBA dataset loaders — Other

> WNBA dataset loaders — Other — function reference in sdv-py, the SportsDataverse Python package.

## load_wnba_pbp

Release: [espn_wnba_pbp](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_pbp) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_pbp/play_by_play_{season}.parquet`
### Returns {#load_wnba_pbp-returns}

| col_name | type | description |
|---|---|---|
| `game_play_number` | Int32 | Game play number |
| `id` | Int64 | Unique play identification number |
| `sequence_number` | Int32 | Sequence number representing a shot-possession (V3 PBP). |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_text` | String | Play type text, passed through verbatim from ESPN. ESPN labels the free-throw play type "MadeFreeThrow" for made AND missed free throws; filter makes vs. misses with scoring_play, not type_text. |
| `text` | String | Text description of the play / record. |
| `away_score` | Int32 | Away team's score after the play. Rebuilt from the scoring plays in feed order when that reaches the official final (score_source 'rebuilt'), otherwise ESPN's per-play value (score_source 'espn'). Seasons published before the repair carry ESPN's per-play value. |
| `home_score` | Int32 | Home team's score after the play. Rebuilt from the scoring plays in feed order when that reaches the official final (score_source 'rebuilt'), otherwise ESPN's per-play value (score_source 'espn'). Seasons published before the repair carry ESPN's per-play value. |
| `period_number` | Int32 | Numeric period (1-4 for quarters; 5+ for OT). |
| `period_display_value` | String | Period display label (e.g. '1st Quarter', 'OT'). |
| `clock_display_value` | String | Game clock display string (e.g. '8:32'). |
| `scoring_play` | Boolean | TRUE if the play resulted in points scored. |
| `score_value` | Int32 | Point value of the attempt (1 / 2 / 3), carried even on misses (a missed free throw still shows 1); use scoring_play to identify points actually scored. |
| `team_id` | Int32 | Unique team identifier. |
| `athlete_id_1` | Int32 | Primary athlete identifier (e.g. shooter). |
| `athlete_id_2` | Int32 | Secondary athlete identifier (e.g. assister / fouler). |
| `athlete_id_3` | Int32 | Athlete id 3. |
| `wallclock` | String | Wallclock. |
| `shooting_play` | Boolean | TRUE if the play was a shooting attempt. |
| `coordinate_x_raw` | Float64 | X coordinate as returned by the API before any adjustment. |
| `coordinate_y_raw` | Float64 | Y coordinate as returned by the API before any adjustment. |
| `points_attempted` | Int32 | Point value at stake on the play's shot attempt (1, 2, or 3); 0 for non-shooting plays. |
| `short_description` | String | Abbreviated ESPN text description of the play. |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `home_team_id` | Int32 | Unique identifier for the home team. |
| `home_team_name` | String | Home team name. |
| `home_team_mascot` | String | Home team mascot. |
| `home_team_abbrev` | String | Home team three-letter abbreviation. |
| `home_team_name_alt` | String | Alternate versions of the home team abbreviation |
| `away_team_id` | Int32 | Unique identifier for the away team. |
| `away_team_name` | String | Away team name. |
| `away_team_mascot` | String | Away team mascot. |
| `away_team_abbrev` | String | Away team three-letter abbreviation. |
| `away_team_name_alt` | String | Alternate versions of the away team abbreviation |
| `game_spread` | Float64 | Game spread in (-X Team) format. There are almost none, I would recommend not trusting any of these three columns |
| `home_favorite` | Boolean | Logical (TRUE/FALSE) indicating whether the home team is favored |
| `game_spread_available` | Boolean | Logical (TRUE/FALSE) indicating whether the spread was available from ESPN. Basically, I would just not recommend using any of the spread information, I think I defaulted a lot of them to -2.5 for the home team. Most games probably do not have spread information. This column should really be listed first |
| `home_team_spread` | Float64 | The game spread with respect to the home team |
| `qtr` | Int32 | Quarter of the game |
| `time` | String | Time left within the period |
| `clock_minutes` | Int32 | Clock minutes split from seconds for developer convenience |
| `clock_seconds` | Float64 | Clock seconds split from minutes for developer convenience |
| `home_timeout_called` | Boolean | True when the play is a timeout charged to the home team. |
| `away_timeout_called` | Boolean | True when the play is a timeout charged to the away team. |
| `half` | Int32 | Half of the game |
| `game_half` | Int32 | Half of the game |
| `lag_qtr` | Int32 | A lag column on the quarter |
| `lead_qtr` | Int32 | A lead column on the quarter |
| `lag_half` | Int32 | A lag column on the half |
| `lead_half` | Int32 | A lead column on the half |
| `start_quarter_seconds_remaining` | Float64 | Quarter seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_half_seconds_remaining` | Float64 | Game half seconds remaining at the start of the play (these are more or less code artifacts from other sports, but may eventually be used more seriously) |
| `start_game_seconds_remaining` | Float64 | Game seconds remaining at the start of the play (''') |
| `end_quarter_seconds_remaining` | Float64 | Quarter seconds remaining at the end of the play (''') |
| `end_half_seconds_remaining` | Float64 | Game half seconds remaining at the end of the play (''') |
| `end_game_seconds_remaining` | Float64 | Game seconds remaining at the end of the play (''') |
| `period` | Int32 | Period of the game (1-4 quarters; 5+ for OT). |
| `coordinate_x` | Float64 | X coordinate on the court (half-court layout). |
| `coordinate_y` | Float64 | Y coordinate on the court (half-court layout). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `athlete_name_1` | String | Display name of the primary athlete on the play (e.g. the shooter, rebounder, or fouler), per ESPN participant order. |
| `athlete_name_2` | String | Display name of the secondary athlete on the play (e.g. the assister or fouled player), when present. |
| `athlete_name_3` | String | Display name of the third athlete listed on the play, when present. |
| `type_abbreviation` | String | Play type abbreviation |
| `score_source` | String | Where this game's home_score/away_score come from: 'rebuilt' (summed from the scoring plays and equal to the official final on the last play) or 'espn' (ESPN's per-play values, kept when the rebuild misses the final or the game has no final). Absent from seasons published before the repair. Select it by name: it is the play-by-play builder's last column, but a release can carry columns after it. |

```python
load_wnba_pbp(seasons=2024)
```

## load_wnba_schedule

Release: [espn_wnba_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_schedules/wnba_schedule_{season}.parquet`
### Returns {#load_wnba_schedule-returns}

| col_name | type | description |
|---|---|---|
| `id` | Int32 | Unique play identification number |
| `uid` | String | ESPN UID string. |
| `date` | String | Date in YYYY-MM-DD format. |
| `attendance` | Float64 | Reported attendance. |
| `time_valid` | Boolean | Time valid. |
| `neutral_site` | Boolean | Neutral site. |
| `conference_competition` | Boolean | Conference competition. |
| `play_by_play_available` | Boolean |  |
| `recent` | Boolean | Recent. |
| `start_date` | String | Start date (YYYY-MM-DD). |
| `broadcast` | String | Broadcast information string. |
| `highlights` | String |  |
| `notes_type` | String | Notes type. |
| `notes_headline` | String | Notes headline. |
| `broadcast_market` | String | Broadcast market label (e.g. 'national', 'home'). |
| `broadcast_name` | String | Broadcast name. |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_abbreviation` | String | Play type abbreviation |
| `venue_id` | Int32 | Unique venue identifier. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state / region. |
| `venue_indoor` | Boolean | TRUE if the venue is indoors. |
| `status_clock` | Float64 | Status clock. |
| `status_display_clock` | String | Status display clock. |
| `status_period` | Float64 | Status period. |
| `status_type_id` | Int32 | ESPN's numeric game-status code, passed through unchanged. ESPN publishes no reference for these codes, so read status_type_name rather than hard-coding numbers. |
| `status_type_name` | String | ESPN's game-status enum (STATUS_FINAL, STATUS_SCHEDULED, STATUS_POSTPONED, STATUS_CANCELED, ...), passed through unchanged. ESPN does not document the full set, and no value certifies an ordinary final as distinct from a forfeit or an awarded result. |
| `status_type_state` | String | ESPN's coarse game state, passed through unchanged: pre, in or post. post covers every game that is over, whatever the reason. |
| `status_type_completed` | Boolean | ESPN's completed flag, passed through unchanged: true once ESPN considers the game over. It does not separate an ordinary final from a forfeited, protested, replayed or awarded result; ESPN documents no such distinction. |
| `status_type_description` | String | ESPN's display text for the game status (for example "Final" or "Postponed"), passed through unchanged. |
| `status_type_detail` | String | ESPN's longer status text, passed through unchanged; an overtime final reads like "Final/OT". |
| `status_type_short_detail` | String | ESPN's short status text for scoreboards, passed through unchanged. |
| `format_regulation_periods` | Float64 | Format regulation periods. |
| `home_id` | Int32 | Unique identifier for home. |
| `home_uid` | String | Home team's uid. |
| `home_location` | String | Home team's location. |
| `home_name` | String | Home name. |
| `home_abbreviation` | String | Home team's abbreviation. |
| `home_display_name` | String | Home display name. |
| `home_short_display_name` | String | Home short display name. |
| `home_color` | String | Color code (hex) for home. |
| `home_alternate_color` | String | Color code (hex) for home alternate. |
| `home_is_active` | Boolean | Home team's is active. |
| `home_venue_id` | Int32 | Unique identifier for home venue. |
| `home_logo` | String | Home team logo URL. |
| `home_score` | Int32 | Home team's points as ESPN reports them: once the game is final, the full-game total including every overtime period. ESPN corrects a score in place, so this is the value ESPN showed when the release was last built. Not a final score until status_type_completed is true. |
| `home_winner` | Boolean | Home team's winner. |
| `home_linescores` | String | Stringified list of the home team's period-by-period scores from the ESPN schedule feed. |
| `home_records` | String | Stringified list of the home team's record summaries (overall/home/away) from the ESPN schedule feed. |
| `away_id` | Int32 | Unique identifier for away. |
| `away_uid` | String | Away team's uid. |
| `away_location` | String | Away team's location. |
| `away_name` | String | Away name. |
| `away_abbreviation` | String | Away team's abbreviation. |
| `away_display_name` | String | Away display name. |
| `away_short_display_name` | String | Away short display name. |
| `away_color` | String | Color code (hex) for away. |
| `away_alternate_color` | String | Color code (hex) for away alternate. |
| `away_is_active` | Boolean | Away team's is active. |
| `away_venue_id` | Int32 | Unique identifier for away venue. |
| `away_logo` | String | Away team logo URL. |
| `away_score` | Int32 | Away team's points as ESPN reports them: once the game is final, the full-game total including every overtime period. ESPN corrects a score in place, so this is the value ESPN showed when the release was last built. Not a final score until status_type_completed is true. |
| `away_winner` | Boolean | Away team's winner. |
| `away_linescores` | String | Stringified list of the away team's period-by-period scores from the ESPN schedule feed. |
| `away_records` | String | Stringified list of the away team's record summaries (overall/home/away) from the ESPN schedule feed. |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `status_type_alt_detail` | String | Status type alt detail. |
| `game_json` | Boolean |  |
| `game_json_url` | String |  |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `PBP` | Boolean |  |
| `team_box` | Boolean | Team box. |
| `player_box` | Boolean | Player box. |

```python
load_wnba_schedule(seasons=2024)
```

## load_wnba_draft

Release: [espn_wnba_draft](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_draft) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_draft/draft_{season}.parquet`
### Returns {#load_wnba_draft-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `round` | Int32 | Tournament / playoff round. |
| `round_display_name` | String | Human-readable label for the draft round, read from the ESPN round object's displayName falling back to its name; null whenever ESPN ships the modern flat picks array with no round objects, which is the case for every published season. |
| `pick` | Int32 | Pick. |
| `overall_pick` | Int32 | Overall pick. |
| `pick_traded` | String | ESPN's pick-level traded flag stringified as TRUE or FALSE, marking selections made with a pick that had changed hands (17 of the 45 published 2026 picks are TRUE). |
| `pick_notes` | String | Free-text annotation ESPN attaches to a pick, taken from notes and falling back to note; empty for every pick published so far. |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_first_name` | String |  |
| `athlete_last_name` | String |  |
| `athlete_full_name` | String |  |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_height` | String |  |
| `athlete_weight` | String |  |
| `athlete_position_abbreviation` | String | Athlete position abbreviation (G / F / C). |
| `athlete_position_name` | String | Athlete position ('Guard', 'Forward', 'Center'). |
| `athlete_headshot_href` | String | Athlete headshot image URL. |
| `college_id` | Int32 | Unique identifier for college. |
| `college_name` | String | College name. |
| `college_short_name` | String |  |
| `college_abbreviation` | String | Short code for the drafted player's school, read from the athlete's ESPN college block; null throughout the published data because ESPN ships no college block on these picks. |
| `team_id` | Int32 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |

```python
load_wnba_draft(seasons=2026)
```

## load_wnba_game_rosters

Release: [espn_wnba_game_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_game_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_game_rosters/game_rosters_{season}.parquet`
### Returns {#load_wnba_game_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_id` | Int32 | Unique game identifier. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `home_away` | String | Game venue label ('home' or 'away'). |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `athlete_uid` | String | ESPN athlete UID (universal identifier). |
| `athlete_guid` | String | ESPN athlete GUID. |
| `athlete_display_name` | String | Athlete display name (full). |
| `athlete_short_name` | String | Athlete short display name. |
| `athlete_first_name` | String |  |
| `athlete_last_name` | String |  |
| `athlete_jersey` | String | Athlete jersey number. |
| `athlete_position` | String | Athlete position. |
| `athlete_headshot` | String | Direct link to the player's ESPN headshot image, always of the form https://a.espncdn.com/i/headshots/wnba/players/full/{athlete_id}.png, and null for the few players ESPN has no photo for. |
| `starter` | Boolean | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `did_not_play` | Boolean | TRUE if the player did not appear in the game. |
| `active` | Boolean | TRUE if the row represents an active record (player / team / season). |
| `ejected` | Boolean | TRUE if the player was ejected from the game. |
| `reason` | String | Reason. |

```python
load_wnba_game_rosters(seasons=2024)
```

## load_wnba_officials

Release: [espn_wnba_officials](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_officials) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_officials/officials_{season}.parquet`

:::caution[Coverage]
Published seasons: every season from the floor except 2022 and 2023, which the release has no asset for; a request for one is skipped with a warning.
:::

### Returns {#load_wnba_officials-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `game_id` | Int32 | Unique game identifier. |
| `official_id` | Int32 | Unique official / referee identifier. |
| `official_uid` | String | ESPN's global uid string for the official, carried straight through from the officials payload; the Core v2 items ESPN serves omit it, so it is null in every published season. |
| `official_full_name` | String | The official's full name, taken from ESPN fullName and falling back to displayName; it equals first plus last name on every published row. |
| `official_display_name` | String | ESPN's display rendering of the official's name, which is byte-identical to official_full_name on every published row and therefore adds nothing. |
| `official_first_name` | String | Given name of the official as ESPN splits it out, the leading token of official_full_name. |
| `official_last_name` | String | Family name of the official as ESPN splits it out, the trailing token of official_full_name, with hyphenated surnames kept intact. |
| `official_order` | Int32 | ESPN's 1-based position of the official within that game's crew; most games run 1 through 3 for a three-person crew and 40 of 573 games in 2024-2025 add a fourth. |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_display_name` | String | Position display name. |

```python
load_wnba_officials(seasons=2024)
```

## load_wnba_rosters

Release: [espn_wnba_rosters](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_rosters) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_rosters/rosters_{season}.parquet`
### Returns {#load_wnba_rosters-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `team_id` | Int32 | Unique team identifier. |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `athlete_id` | Int32 | Unique athlete identifier (ESPN). |
| `uid` | String | ESPN UID string. |
| `guid` | String | Stable cross-league team GUID. |
| `full_name` | String | Player's full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short display name. |
| `first_name` | String | Player's first name. |
| `last_name` | String | Player's last name. |
| `jersey` | String | Jersey number worn by the player. |
| `position_abbreviation` | String | Position abbreviation ('G' / 'F' / 'C'). |
| `position_name` | String | Listed roster position ('Guard', 'Forward', 'Center'). |
| `position_id` | String | Unique position identifier. |
| `height` | String | Player height (string e.g. '6-2' or inches). |
| `weight` | String | Player weight in pounds. |
| `age` | String | Player age (in years). |
| `date_of_birth` | String | Date of birth (YYYY-MM-DD). |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `experience_years` | String | Experience years. |
| `experience_display_value` | String | Experience display value. |
| `headshot_href` | String | Headshot image URL. |
| `headshot_alt` | String | Alternative-text label for the headshot. |
| `link_web` | String | Web link / URL. |
| `status_id` | String | Status identifier. |
| `status_name` | String | Status label. |
| `status_type` | String | Status type. |

```python
load_wnba_rosters(seasons=2024)
```

## load_wnba_shots

Release: [espn_wnba_shots](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_shots) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_shots/shots_{season}.parquet`
### Returns {#load_wnba_shots-returns}

| col_name | type | description |
|---|---|---|
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `period_number` | Int32 | Numeric period (1-4 for quarters; 5+ for OT). |
| `clock_display_value` | String | Game clock display string (e.g. '8:32'). |
| `team_id` | Int32 | Unique team identifier. |
| `athlete_id_1` | Int32 | Primary athlete identifier (e.g. shooter). |
| `athlete_id_2` | Int32 | Secondary athlete identifier (e.g. assister / fouler). |
| `type_id` | Int32 | Type identifier (numeric). |
| `type_text` | String | Display text for the type field. |
| `scoring_play` | Boolean | TRUE if the play resulted in points scored. |
| `score_value` | Int32 | Point value of the play (2 / 3 / 1). |
| `coordinate_x` | Float64 | X coordinate on the court (half-court layout). |
| `coordinate_y` | Float64 | Y coordinate on the court (half-court layout). |
| `coordinate_x_raw` | Float64 | X coordinate as returned by the API before any adjustment. |
| `coordinate_y_raw` | Float64 | Y coordinate as returned by the API before any adjustment. |
| `athlete_name_1` | String | Display name of the shooter, per ESPN participant order. |
| `athlete_name_2` | String | Display name of the secondary athlete on the shot (typically the assister), when present. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_mascot` | String | ESPN mascot (nickname) of the shooting team. |
| `team_abbrev` | String | Abbreviation for team. |

```python
load_wnba_shots(seasons=2024)
```

## load_wnba_standings

Release: [espn_wnba_standings](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_wnba_standings) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_wnba_standings/standings_{season}.parquet`

:::caution[Coverage]
Published seasons: every season from the floor except 2022 and 2023, which the release has no asset for; a request for one is skipped with a warning.
:::

### Returns {#load_wnba_standings-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `group_id` | String | ESPN group id. |
| `group_name` | String |  |
| `group_abbreviation` | String |  |
| `group_short_name` | String | Short label of the standings group node the team sits under, read from ESPN shortName; the WNBA conference nodes ship only name and abbreviation, so it is null on every published row. |
| `team_id` | Int32 | Unique team identifier. |
| `team_uid` | String | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `team_slug` | String | URL-safe team identifier (e.g. 'lasvegas-aces' / 'aces'). |
| `team_location` | String | Team city or location string. |
| `team_name` | String | Full team display name (e.g. 'Las Vegas Aces'). |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'LAS'). |
| `team_display_name` | String | Full team display name. |
| `team_short_display_name` | String | Short team display name (e.g. 'Aces'). |
| `team_color` | String | Team primary color (hex without leading '#'). |
| `team_alternate_color` | String | Team alternate color (hex without leading '#'). |
| `team_logo` | String | Team logo image URL. |
| `stat_name` | String | Internal stat key. |
| `stat_display_name` | String | Stat display name. |
| `stat_short_display_name` | String |  |
| `stat_description` | String | ESPN's long-form explanation of the standings stat, such as Clinched Best League Record for clincher or Record last 10 games for lasttengames. |
| `stat_abbreviation` | String | ESPN's abbreviation for the standings stat, which can differ from stat_short_display_name (playoffSeed is SEED here but POS there) and is null on the record-split rows such as Home and vs. Conf. |
| `stat_type` | String | Stat type code (e.g. "win", "loss"). |
| `display_value` | String | Display-formatted value. |
| `value` | Float64 | Numeric or string value field. |

```python
load_wnba_standings(seasons=2024)
```

## load_wnba_schedule_crosswalk

Release: [wnba_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_crosswalk/wnba_schedule_crosswalk_{season}.parquet`

:::caution[Coverage]
wnba_game_id stays String, the WNBA Stats game id's dtype; the wnba_*_team_id bridge ids are Int64, as WNBA Stats ships team_id.
:::

### Returns {#load_wnba_schedule_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | String | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `home_espn_team_id` | Int32 | ESPN home team id (NA for bart-only rows). |
| `away_espn_team_id` | Int32 | ESPN away team id (NA for bart-only rows). |
| `espn_game_id` | Int32 | ESPN game id (NA for bart-only rows). |
| `wnba_game_id` | String | Game identifier on stats.wnba.com matched to the ESPN game. |
| `wnba_game_code` | String | stats.wnba.com game code (date/matchup slug) for the matched game. |
| `wnba_home_team_id` | Int64 | Home team identifier on stats.wnba.com for the matched game. |
| `wnba_away_team_id` | Int64 | Away team identifier on stats.wnba.com for the matched game. |
| `fox_game_id` | String | Fox Sports game id (string), filled when the crosswalk is built with fox=True from Fox's per-date scoreboard, matched on the US-Eastern date and the two teams in either home/away order; null otherwise and where Fox lists no matching game. |
| `fox_home_team_id` | String | Fox Sports team id of the side Fox calls home in fox_game_id, filled with fox=True; at a neutral site it can be the reverse of home_espn_team_id. Null when fox_game_id is. |
| `fox_away_team_id` | String | Fox Sports team id of the side Fox calls away in fox_game_id, filled with fox=True; at a neutral site it can be the reverse of away_espn_team_id. Null when fox_game_id is. |
| `yahoo_game_id` | String | Yahoo Sports game id, wnba.g.<home Yahoo team id, 3 digits>, filled when built with yahoo=True and checked against the ids Yahoo lists, so a neutral-site game carries the side Yahoo calls home; null otherwise and where Yahoo lists no such game (a non-Division-I home team). |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | Float64 | Jaro-Winkler score or 1 for exact (NA if none). |

```python
load_wnba_schedule_crosswalk(seasons=2026)
```

## load_wnba_groups

Release: [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_groups.parquet`

:::caution[Coverage]
One season-less file: one row per group lineage (the league, subdivisions, conferences, divisions) with the first and last season it had members. group_id is SDV's own id (e.g. wnba:east) and names a lineage: a rename that keeps continuity keeps the id, a new body gets a new one, and notes records each call. Seasons are the calendar year.
:::

### Returns {#load_wnba_groups-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wnba"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `first_season` | Int32 | First season in which the group had at least one member (calendar year). |
| `last_season` | Int32 | Last season in which the group had at least one member (calendar year). |
| `notes` | String | Builder notes on the group: the lineage decisions behind its group_id and any source caveats. |

```python
load_wnba_groups()
```

## load_wnba_group_seasons

Release: [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_group_seasons.parquet`

:::caution[Coverage]
One season-less file: one row per group per season it existed, with its name, short name, abbreviation and parent group AS OF that season (never today's label applied to the past) and its member count. season is the calendar year.
:::

### Returns {#load_wnba_group_seasons-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wnba"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `season` | Int32 | Season the row describes (calendar year). |
| `level` | String | Hierarchy level of the group: "league", "subdivision", "conference" or "division". |
| `name` | String | Full name of the group as of that season -- the label in use then, not today's name. |
| `short_name` | String | Short display name of the group as of that season. |
| `abbreviation` | String | Abbreviation of the group as of that season. |
| `parent_group_id` | String | group_id one level up as of that season (division -> conference -> subdivision -> league); null at the top level or where no higher group applied that season. |
| `n_teams` | Int32 | Number of member teams in the group that season. |

```python
load_wnba_group_seasons()
```

## load_wnba_group_aliases

Release: [wnba_groups](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wnba_groups) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wnba_groups/wnba_group_aliases.parquet`

:::caution[Coverage]
One season-less file: every name, abbreviation, slug and source id that a source (espn, sdv, wnba_stats) uses for a group, each with the seasons it is valid for (valid_from / valid_to, inclusive; null = unbounded). Match a source's conference or division label here to reach group_id.
:::

### Returns {#load_wnba_group_aliases-returns}

| col_name | type | description |
|---|---|---|
| `league` | String | League code of the table ("wnba"); the prefix of every group_id in it. |
| `group_id` | String | SDV group id, {league}:{slug}. It names a lineage: renames that keep continuity keep the id, and a new body (a new conference, or a merger the sources treat as new) gets a new one. |
| `source` | String | Source that uses this label or id (in this table: espn, sdv, wnba_stats); "sdv" marks SDV's own labels. |
| `source_id` | String | The source's own id for the group (ESPN group id, NCAA conf_id, CFBD id, MLB division id) when it has one; null otherwise. |
| `name_kind` | String | Kind of label in value: "name", "short_name", "abbreviation", "slug" or "code". |
| `value` | String | The label exactly as the source writes it; match a source's conference or division label against it to reach group_id. |
| `valid_from` | Int32 | First season the alias is valid for, inclusive (calendar year); null = unbounded. |
| `valid_to` | Int32 | Last season the alias is valid for, inclusive (calendar year); null = unbounded (still in use). |

```python
load_wnba_group_aliases()
```
