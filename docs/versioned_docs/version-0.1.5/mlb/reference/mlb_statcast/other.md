---
title: "MLB — MLB Statcast (Baseball Savant) — Other"
sidebar_label: "Other"
sidebar_position: 3
description: "MLB — MLB Statcast (Baseball Savant) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — MLB Statcast (Baseball Savant) — Other

## mlb_statcast_gamefeed

GET /gf — Savant per-game JSON feed (pitch-by-pitch tracking).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/gf`

**Valid URL:** [https://baseballsavant.mlb.com/gf](https://baseballsavant.mlb.com/gf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  |  | `Y` | game_pk query parameter. |
| `at_bat_number` | `at_bat_number` |  |  | `Y` | at_bat_number query parameter. |

### Returns {#mlb_statcast_gamefeed-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `type` | character | Record/pitch type. |
| `year` | character | Season year. |
| `sport_id` | character | Sport id. |
| `play_id` | character | Statcast play UUID. |
| `inning` | character | Inning. |
| `half_inning` | character | Half inning. |
| `ab_number` | character | Ab number. |
| `cap_index` | character | Cap index. |
| `outs` | character | Outs. |
| `batter` | integer | MLBAM id of the batter. |
| `stand` | character | Batter stance side (R/L). |
| `batter_name` | character | Batter name. |
| `pitcher` | integer | MLBAM id of the pitcher. |
| `p_throws` | character | Pitcher throwing hand (R/L). |
| `pitcher_name` | character | Pitcher name. |
| `catcher` | character | Catcher. |
| `catcher_name` | character | Catcher name. |
| `team_batting` | character | Team batting. |
| `team_fielding` | character | Team fielding. |
| `team_batting_id` | character | Team batting id. |
| `team_fielding_id` | character | Team fielding id. |
| `result` | character | Result. |
| `des` | character | Des. |
| `events` | character | Events. |
| `strikes` | character | Strikes. |
| `balls` | character | Balls. |
| `pre_strikes` | character | Pre strikes. |
| `pre_balls` | character | Pre balls. |
| `call` | character | Call. |
| `call_name` | character | Call name. |
| `pitch_type` | character | Pitch type code. |
| `pitch_name` | character | Pitch type name. |
| `description` | character | Description. |
| `result_code` | character | Result code. |
| `pitch_call` | character | Pitch call. |
| `is_strike_swinging` | character | Is strike swinging. |
| `balls_and_strikes` | character | Balls and strikes. |
| `start_speed` | character | Start speed. |
| `end_speed` | character | End speed. |
| `sz_top` | character | Sz top. |
| `sz_bot` | character | Sz bot. |
| `extension` | character | Release extension (ft). |
| `plate_time` | character | Plate time. |
| `zone` | character | Zone. |
| `spin_rate` | character | Spin rate (rpm). |
| `break_x` | character | Break x. |
| `induced_break_z` | character | Induced break z. |
| `break_z` | character | Break z. |
| `px` | character | Px. |
| `pz` | character | Pz. |
| `pfx_x` | character | Horizontal movement (in, pitcher perspective). |
| `pfx_z` | character | Induced vertical movement (in). |
| `is_bip_out` | character | Is bip out. |
| `pitch_number` | character | Pitch number. |
| `plate_x` | character | Plate x. |
| `plate_z` | character | Plate z. |
| `hit_speed` | character | Hit speed. |
| `hit_distance` | character | Hit distance. |
| `xba` | character | Expected batting average. |
| `hit_angle` | character | Hit angle. |
| `is_barrel` | character | Is barrel. |
| `hc_x` | character | Hc x. |
| `hc_y` | character | Hc y. |
| `launch_speed` | character | Exit velocity of the batted ball (mph). |
| `launch_angle` | character | Launch angle (deg). |
| `game_total_pitches` | character | Game total pitches. |
| `game_pk` | integer | MLBAM game id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_gamefeed-example}

```python
mlb_statcast_gamefeed()
```

_Last validated n/a._

## mlb_statcast_schedule

GET /schedule — Savant schedule feed (one row per game).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/schedule`

**Valid URL:** [https://baseballsavant.mlb.com/schedule](https://baseballsavant.mlb.com/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | date query parameter. |

### Returns {#mlb_statcast_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_pk` | integer | MLBAM game id. |
| `game_guid` | character | Game GUID. |
| `link` | character | Stats API resource link. |
| `game_type` | character | Game type code (R/F/D/L/W/S/E/A). |
| `season` | character | Season year. |
| `game_date` | character | Game date/time (ISO 8601, UTC offset). |
| `official_date` | character | Official game date (YYYY-MM-DD). |
| `game_number` | integer | Game number (1, or 2 for the nightcap of a doubleheader). |
| `public_facing` | logical | Public facing. |
| `double_header` | character | Doubleheader flag (Y/N/S). |
| `gameday_type` | character | Gameday type. |
| `tiebreaker` | character | Tiebreaker. |
| `calendar_event_id` | character | Calendar event id. |
| `season_display` | character | Season display. |
| `day_night` | character | Day or night game. |
| `scheduled_innings` | integer | Scheduled innings (usually 9). |
| `reverse_home_away_status` | logical | Reverse home away status. |
| `inning_break_length` | integer | Inning break length. |
| `games_in_series` | integer | Total games in the series. |
| `series_game_number` | integer | Game number within the series. |
| `series_description` | character | Series description. |
| `record_source` | character | Record source. |
| `if_necessary` | character | If necessary. |
| `if_necessary_description` | character | If necessary description. |
| `status_abstract_game_state` | character | Status abstract game state. |
| `status_coded_game_state` | character | Status coded game state. |
| `status_detailed_state` | character | Status detailed state. |
| `status_status_code` | character | Status status code. |
| `status_start_time_tbd` | logical | Status start time tbd. |
| `status_abstract_game_code` | character | Status abstract game code. |
| `teams_away_team_spring_league_id` | integer | Away team team spring league id. |
| `teams_away_team_spring_league_name` | character | Away team team spring league name. |
| `teams_away_team_spring_league_link` | character | Away team team spring league link. |
| `teams_away_team_spring_league_abbreviation` | character | Away team team spring league abbreviation. |
| `teams_away_team_all_star_status` | character | Away team team all star status. |
| `teams_away_team_id` | integer | Away team team id. |
| `teams_away_team_name` | character | Away team team name. |
| `teams_away_team_link` | character | Away team team link. |
| `teams_away_team_season` | integer | Away team team season. |
| `teams_away_team_venue_id` | integer | Away team team venue id. |
| `teams_away_team_venue_name` | character | Away team team venue name. |
| `teams_away_team_venue_link` | character | Away team team venue link. |
| `teams_away_team_spring_venue_id` | integer | Away team team spring venue id. |
| `teams_away_team_spring_venue_link` | character | Away team team spring venue link. |
| `teams_away_team_team_code` | character | Away team team team code. |
| `teams_away_team_file_code` | character | Away team team file code. |
| `teams_away_team_abbreviation` | character | Away team team abbreviation. |
| `teams_away_team_team_name` | character | Away team team team name. |
| `teams_away_team_location_name` | character | Away team team location name. |
| `teams_away_team_first_year_of_play` | character | Away team team first year of play. |
| `teams_away_team_league_id` | integer | Away team team league id. |
| `teams_away_team_league_name` | character | Away team team league name. |
| `teams_away_team_league_link` | character | Away team team league link. |
| `teams_away_team_division_id` | integer | Away team team division id. |
| `teams_away_team_division_name` | character | Away team team division name. |
| `teams_away_team_division_link` | character | Away team team division link. |
| `teams_away_team_sport_id` | integer | Away team team sport id. |
| `teams_away_team_sport_link` | character | Away team team sport link. |
| `teams_away_team_sport_name` | character | Away team team sport name. |
| `teams_away_team_short_name` | character | Away team team short name. |
| `teams_away_team_franchise_name` | character | Away team team franchise name. |
| `teams_away_team_club_name` | character | Away team team club name. |
| `teams_away_team_active` | logical | Away team team active. |
| `teams_away_league_record_wins` | integer | Away team league record wins. |
| `teams_away_league_record_losses` | integer | Away team league record losses. |
| `teams_away_league_record_ties` | integer | Away team league record ties. |
| `teams_away_league_record_pct` | character | Away team league record rate. |
| `teams_away_probable_pitcher_id` | integer | Away team probable pitcher id. |
| `teams_away_probable_pitcher_full_name` | character | Away team probable pitcher full name. |
| `teams_away_probable_pitcher_link` | character | Away team probable pitcher link. |
| `teams_away_probable_pitcher_first_name` | character | Away team probable pitcher first name. |
| `teams_away_probable_pitcher_last_name` | character | Away team probable pitcher last name. |
| `teams_away_probable_pitcher_primary_number` | character | Away team probable pitcher primary number. |
| `teams_away_probable_pitcher_birth_date` | character | Away team probable pitcher birth date. |
| `teams_away_probable_pitcher_current_age` | integer | Away team probable pitcher current age. |
| `teams_away_probable_pitcher_birth_city` | character | Away team probable pitcher birth city. |
| `teams_away_probable_pitcher_birth_state_province` | character | Away team probable pitcher birth state province. |
| `teams_away_probable_pitcher_birth_country` | character | Away team probable pitcher birth country. |
| `teams_away_probable_pitcher_height` | character | Away team probable pitcher height. |
| `teams_away_probable_pitcher_weight` | integer | Away team probable pitcher weight. |
| `teams_away_probable_pitcher_active` | logical | Away team probable pitcher active. |
| `teams_away_probable_pitcher_primary_position_code` | character | Away team probable pitcher primary position code. |
| `teams_away_probable_pitcher_primary_position_name` | character | Away team probable pitcher primary position name. |
| `teams_away_probable_pitcher_primary_position_type` | character | Away team probable pitcher primary position type. |
| `teams_away_probable_pitcher_primary_position_abbreviation` | character | Away team probable pitcher primary position abbreviation. |
| `teams_away_probable_pitcher_use_name` | character | Away team probable pitcher use name. |
| `teams_away_probable_pitcher_use_last_name` | character | Away team probable pitcher use last name. |
| `teams_away_probable_pitcher_middle_name` | character | Away team probable pitcher middle name. |
| `teams_away_probable_pitcher_boxscore_name` | character | Away team probable pitcher boxscore name. |
| `teams_away_probable_pitcher_gender` | character | Away team probable pitcher gender. |
| `teams_away_probable_pitcher_is_player` | logical | Away team probable pitcher is player. |
| `teams_away_probable_pitcher_is_verified` | logical | Away team probable pitcher is verified. |
| `teams_away_probable_pitcher_draft_year` | integer | Away team probable pitcher draft year. |
| `teams_away_probable_pitcher_mlb_debut_date` | character | Away team probable pitcher mlb debut date. |
| `teams_away_probable_pitcher_bat_side_code` | character | Away team probable pitcher bat side code. |
| `teams_away_probable_pitcher_bat_side_description` | character | Away team probable pitcher bat side description. |
| `teams_away_probable_pitcher_pitch_hand_code` | character | Away team probable pitcher pitch hand code. |
| `teams_away_probable_pitcher_pitch_hand_description` | character | Away team probable pitcher pitch hand description. |
| `teams_away_probable_pitcher_name_first_last` | character | Away team probable pitcher name first last. |
| `teams_away_probable_pitcher_name_slug` | character | Away team probable pitcher name slug. |
| `teams_away_probable_pitcher_first_last_name` | character | Away team probable pitcher first last name. |
| `teams_away_probable_pitcher_last_first_name` | character | Away team probable pitcher last first name. |
| `teams_away_probable_pitcher_last_init_name` | character | Away team probable pitcher last init name. |
| `teams_away_probable_pitcher_init_last_name` | character | Away team probable pitcher init last name. |
| `teams_away_probable_pitcher_full_fml_name` | character | Away team probable pitcher full fml name. |
| `teams_away_probable_pitcher_full_lfm_name` | character | Away team probable pitcher full lfm name. |
| `teams_away_probable_pitcher_strike_zone_top` | numeric | Away team probable pitcher strike zone top. |
| `teams_away_probable_pitcher_strike_zone_bottom` | numeric | Away team probable pitcher strike zone bottom. |
| `teams_away_split_squad` | logical | Away team split squad. |
| `teams_away_series_number` | integer | Away team series number. |
| `teams_away_spring_league_id` | integer | Away team spring league id. |
| `teams_away_spring_league_name` | character | Away team spring league name. |
| `teams_away_spring_league_link` | character | Away team spring league link. |
| `teams_away_spring_league_abbreviation` | character | Away team spring league abbreviation. |
| `teams_home_team_spring_league_id` | integer | Home team team spring league id. |
| `teams_home_team_spring_league_name` | character | Home team team spring league name. |
| `teams_home_team_spring_league_link` | character | Home team team spring league link. |
| `teams_home_team_spring_league_abbreviation` | character | Home team team spring league abbreviation. |
| `teams_home_team_all_star_status` | character | Home team team all star status. |
| `teams_home_team_id` | integer | Home team team id. |
| `teams_home_team_name` | character | Home team team name. |
| `teams_home_team_link` | character | Home team team link. |
| `teams_home_team_season` | integer | Home team team season. |
| `teams_home_team_venue_id` | integer | Home team team venue id. |
| `teams_home_team_venue_name` | character | Home team team venue name. |
| `teams_home_team_venue_link` | character | Home team team venue link. |
| `teams_home_team_spring_venue_id` | integer | Home team team spring venue id. |
| `teams_home_team_spring_venue_link` | character | Home team team spring venue link. |
| `teams_home_team_team_code` | character | Home team team team code. |
| `teams_home_team_file_code` | character | Home team team file code. |
| `teams_home_team_abbreviation` | character | Home team team abbreviation. |
| `teams_home_team_team_name` | character | Home team team team name. |
| `teams_home_team_location_name` | character | Home team team location name. |
| `teams_home_team_first_year_of_play` | character | Home team team first year of play. |
| `teams_home_team_league_id` | integer | Home team team league id. |
| `teams_home_team_league_name` | character | Home team team league name. |
| `teams_home_team_league_link` | character | Home team team league link. |
| `teams_home_team_division_id` | integer | Home team team division id. |
| `teams_home_team_division_name` | character | Home team team division name. |
| `teams_home_team_division_link` | character | Home team team division link. |
| `teams_home_team_sport_id` | integer | Home team team sport id. |
| `teams_home_team_sport_link` | character | Home team team sport link. |
| `teams_home_team_sport_name` | character | Home team team sport name. |
| `teams_home_team_short_name` | character | Home team team short name. |
| `teams_home_team_franchise_name` | character | Home team team franchise name. |
| `teams_home_team_club_name` | character | Home team team club name. |
| `teams_home_team_active` | logical | Home team team active. |
| `teams_home_league_record_wins` | integer | Home team league record wins. |
| `teams_home_league_record_losses` | integer | Home team league record losses. |
| `teams_home_league_record_ties` | integer | Home team league record ties. |
| `teams_home_league_record_pct` | character | Home team league record rate. |
| `teams_home_probable_pitcher_id` | integer | Home team probable pitcher id. |
| `teams_home_probable_pitcher_full_name` | character | Home team probable pitcher full name. |
| `teams_home_probable_pitcher_link` | character | Home team probable pitcher link. |
| `teams_home_probable_pitcher_first_name` | character | Home team probable pitcher first name. |
| `teams_home_probable_pitcher_last_name` | character | Home team probable pitcher last name. |
| `teams_home_probable_pitcher_primary_number` | character | Home team probable pitcher primary number. |
| `teams_home_probable_pitcher_birth_date` | character | Home team probable pitcher birth date. |
| `teams_home_probable_pitcher_current_age` | integer | Home team probable pitcher current age. |
| `teams_home_probable_pitcher_birth_city` | character | Home team probable pitcher birth city. |
| `teams_home_probable_pitcher_birth_state_province` | character | Home team probable pitcher birth state province. |
| `teams_home_probable_pitcher_birth_country` | character | Home team probable pitcher birth country. |
| `teams_home_probable_pitcher_height` | character | Home team probable pitcher height. |
| `teams_home_probable_pitcher_weight` | integer | Home team probable pitcher weight. |
| `teams_home_probable_pitcher_active` | logical | Home team probable pitcher active. |
| `teams_home_probable_pitcher_primary_position_code` | character | Home team probable pitcher primary position code. |
| `teams_home_probable_pitcher_primary_position_name` | character | Home team probable pitcher primary position name. |
| `teams_home_probable_pitcher_primary_position_type` | character | Home team probable pitcher primary position type. |
| `teams_home_probable_pitcher_primary_position_abbreviation` | character | Home team probable pitcher primary position abbreviation. |
| `teams_home_probable_pitcher_use_name` | character | Home team probable pitcher use name. |
| `teams_home_probable_pitcher_use_last_name` | character | Home team probable pitcher use last name. |
| `teams_home_probable_pitcher_middle_name` | character | Home team probable pitcher middle name. |
| `teams_home_probable_pitcher_boxscore_name` | character | Home team probable pitcher boxscore name. |
| `teams_home_probable_pitcher_nick_name` | character | Home team probable pitcher nick name. |
| `teams_home_probable_pitcher_gender` | character | Home team probable pitcher gender. |
| `teams_home_probable_pitcher_is_player` | logical | Home team probable pitcher is player. |
| `teams_home_probable_pitcher_is_verified` | logical | Home team probable pitcher is verified. |
| `teams_home_probable_pitcher_draft_year` | integer | Home team probable pitcher draft year. |
| `teams_home_probable_pitcher_mlb_debut_date` | character | Home team probable pitcher mlb debut date. |
| `teams_home_probable_pitcher_bat_side_code` | character | Home team probable pitcher bat side code. |
| `teams_home_probable_pitcher_bat_side_description` | character | Home team probable pitcher bat side description. |
| `teams_home_probable_pitcher_pitch_hand_code` | character | Home team probable pitcher pitch hand code. |
| `teams_home_probable_pitcher_pitch_hand_description` | character | Home team probable pitcher pitch hand description. |
| `teams_home_probable_pitcher_name_first_last` | character | Home team probable pitcher name first last. |
| `teams_home_probable_pitcher_name_slug` | character | Home team probable pitcher name slug. |
| `teams_home_probable_pitcher_first_last_name` | character | Home team probable pitcher first last name. |
| `teams_home_probable_pitcher_last_first_name` | character | Home team probable pitcher last first name. |
| `teams_home_probable_pitcher_last_init_name` | character | Home team probable pitcher last init name. |
| `teams_home_probable_pitcher_init_last_name` | character | Home team probable pitcher init last name. |
| `teams_home_probable_pitcher_full_fml_name` | character | Home team probable pitcher full fml name. |
| `teams_home_probable_pitcher_full_lfm_name` | character | Home team probable pitcher full lfm name. |
| `teams_home_probable_pitcher_strike_zone_top` | numeric | Home team probable pitcher strike zone top. |
| `teams_home_probable_pitcher_strike_zone_bottom` | numeric | Home team probable pitcher strike zone bottom. |
| `teams_home_split_squad` | logical | Home team split squad. |
| `teams_home_series_number` | integer | Home team series number. |
| `teams_home_spring_league_id` | integer | Home team spring league id. |
| `teams_home_spring_league_name` | character | Home team spring league name. |
| `teams_home_spring_league_link` | character | Home team spring league link. |
| `teams_home_spring_league_abbreviation` | character | Home team spring league abbreviation. |
| `linescore_scheduled_innings` | integer | Linescore scheduled innings. |
| `linescore_innings` | character | Linescore innings. |
| `linescore_defense_team_id` | integer | Linescore defense team id. |
| `linescore_defense_team_name` | character | Linescore defense team name. |
| `linescore_defense_team_link` | character | Linescore defense team link. |
| `linescore_offense_team_id` | integer | Linescore offense team id. |
| `linescore_offense_team_name` | character | Linescore offense team name. |
| `linescore_offense_team_link` | character | Linescore offense team link. |
| `venue_id` | integer | MLBAM venue id. |
| `venue_name` | character | Ballpark name. |
| `venue_link` | character | Venue link. |
| `content_link` | character | Content link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#mlb_statcast_schedule-example}

```python
mlb_statcast_schedule()
```

_Last validated n/a._
