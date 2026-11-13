---
title: "WBB dataset loaders — Schedule"
sidebar_label: "Schedule"
sidebar_position: 3
description: "WBB dataset loaders — Schedule — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WBB dataset loaders — Schedule

## load_wbb_schedule

Release: [espn_womens_college_basketball_schedules](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/espn_womens_college_basketball_schedules) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_schedules/wbb_schedule_{season}.parquet` · [dataset page](https://github.com/sportsdataverse/wehoop-wbb-data/blob/main/docs/datasets/schedules.md)
### Returns {#load_wbb_schedule-returns}

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
| `home_conference_id` | Int32 | Unique identifier for home conference. |
| `home_score` | Int32 | Home team's points as ESPN reports them: once the game is final, the full-game total including every overtime period. ESPN corrects a score in place, so this is the value ESPN showed when the release was last built. Not a final score until status_type_completed is true. |
| `home_winner` | Boolean | Home team's winner. |
| `home_current_rank` | Float64 | Poll ranking ESPN listed for the home team at game time (unranked teams carry a sentinel value). |
| `home_linescores` | String | Period-by-period scores for the home team as a delimited string from ESPN's schedule feed. |
| `home_records` | String | Record strings (overall and split records) for the home team from ESPN's schedule feed. |
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
| `away_conference_id` | Int32 | Unique identifier for away conference. |
| `away_score` | Int32 | Away team's points as ESPN reports them: once the game is final, the full-game total including every overtime period. ESPN corrects a score in place, so this is the value ESPN showed when the release was last built. Not a final score until status_type_completed is true. |
| `away_winner` | Boolean | Away team's winner. |
| `away_current_rank` | Float64 | Poll ranking ESPN listed for the away team at game time (unranked teams carry a sentinel value). |
| `away_linescores` | String | Period-by-period scores for the away team as a delimited string from ESPN's schedule feed. |
| `away_records` | String | Record strings (overall and split records) for the away team from ESPN's schedule feed. |
| `game_id` | Int32 | Unique game identifier. |
| `season` | Int32 | Season as a 4-digit starting year (integer). A 'YYYY-YY' string is not accepted. |
| `season_type` | Int32 | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `status_type_alt_detail` | String | Status type alt detail. |
| `tournament_id` | Int32 | ESPN tournament identifier. |
| `groups_id` | Int32 | Unique identifier for groups. |
| `groups_name` | String | Groups name. |
| `groups_short_name` | String | Groups short name. |
| `groups_is_conference` | Boolean | Groups is conference. |
| `game_json` | Boolean |  |
| `game_json_url` | String |  |
| `has_game_json` | Boolean | Whether the raw game JSON payload exists in the raw store for this game. |
| `game_json_raw_url` | String | raw.githubusercontent.com URL of the game's raw JSON payload in wehoop-wbb-raw. |
| `game_rosters_json_url` | String | raw.githubusercontent.com URL of the game's rosters JSON payload in wehoop-wbb-raw. |
| `has_game_rosters_json` | Boolean | Whether the rosters JSON payload exists in the raw store for this game. |
| `officials_json_url` | String | raw.githubusercontent.com URL of the game's officials JSON payload in wehoop-wbb-raw. |
| `has_officials_json` | Boolean | Whether the officials JSON payload exists in the raw store for this game. |
| `game_date_time` | Datetime(time_unit='us', time_zone='America/New_York') | Game start date/time (ISO 8601). |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `PBP` | Boolean |  |
| `team_box` | Boolean | Team box. |
| `player_box` | Boolean | Player box. |
| `in_shots` | Boolean | Whether the game is represented in the built shots dataset. |
| `in_game_rosters` | Boolean | Whether the game is represented in the built game-rosters dataset. |
| `in_officials` | Boolean | Whether the game is represented in the built officials dataset. |

```python
load_wbb_schedule(seasons=2024)
```

## load_wbb_schedule_crosswalk

Release: [wbb_crosswalk](https://github.com/sportsdataverse/sportsdataverse-data/releases/tag/wbb_crosswalk) · asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/wbb_crosswalk/wbb_schedule_crosswalk_{season}.parquet` · [dataset page](https://github.com/sportsdataverse/wehoop-wbb-data/blob/main/docs/datasets/schedule_crosswalk.md)
### Returns {#load_wbb_schedule_crosswalk-returns}

| col_name | type | description |
|---|---|---|
| `season` | Int32 | Season as its 4-digit ending year (integer; 2026 = 2025-26), the year in the asset name. |
| `game_date` | Date | Game date (YYYY-MM-DD). |
| `home_espn_team_id` | Int32 | ESPN home team id (NA for bart-only rows). |
| `away_espn_team_id` | Int32 | ESPN away team id (NA for bart-only rows). |
| `espn_game_id` | Int32 | ESPN game id (NA for bart-only rows). |
| `bart_muid` | String | Torvik muid (NA for espn-only rows). |
| `bart_team1` | String | Torvik team1 name (NA for espn-only rows). |
| `bart_team2` | String | Torvik team2 name (NA for espn-only rows). |
| `bart_winner` | String | Torvik winner name (NA for espn-only rows). |
| `fox_game_id` | String | Fox Sports game id (string), filled when the crosswalk is built with fox=True from Fox's per-date scoreboard, matched on the US-Eastern date and the two teams in either home/away order; null otherwise and where Fox lists no matching game. |
| `yahoo_game_id` | String | Yahoo Sports game id, ncaaw.g.<YYYYMMDD US-Eastern date><home Yahoo team id, 4 digits>, filled when built with yahoo=True and checked against the ids Yahoo lists, so a neutral-site game carries the side Yahoo calls home; null otherwise and where Yahoo lists no such game (a non-Division-I home team). |
| `ncaa_contest_id` | String | stats.ncaa.org contest id (string, the contest_id of the load_ncaa_wbb_* releases), matched on the game date and the two teams' ESPN ids through the NCAA team crosswalk, then on one team's only game that day; null when no stats.ncaa.org game matches uniquely (a non-Division-I opponent), and on every row of a season before 2010 (2009-10, the first stats.ncaa.org season; season is the ending year). |
| `match_method` | String | Combination of matched sources, e.g. "fox+bart" / "fox_only" / "bart_only" / "espn_only". |
| `match_confidence` | Float64 | Jaro-Winkler score or 1 for exact (NA if none). |

```python
load_wbb_schedule_crosswalk(seasons=2026)
```
