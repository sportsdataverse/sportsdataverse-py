# MLB — MLB Statcast (Baseball Savant) — Other

> MLB — MLB Statcast (Baseball Savant) — Other — function reference in sdv-py, the SportsDataverse Python package.

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
| `type` | String | Record/pitch type. |
| `year` | String | Season year. |
| `sport_id` | String | Sport id. |
| `play_id` | String | Statcast play UUID. |
| `inning` | String | Inning. |
| `half_inning` | String | Half inning. |
| `ab_number` | String | Ab number. |
| `cap_index` | String | Cap index. |
| `outs` | String | Outs. |
| `batter` | Int64 | MLBAM id of the batter. |
| `stand` | String | Batter stance side (R/L). |
| `batter_name` | String | Batter name. |
| `pitcher` | Int64 | MLBAM id of the pitcher. |
| `p_throws` | String | Pitcher throwing hand (R/L). |
| `pitcher_name` | String | Pitcher name. |
| `catcher` | String | Catcher. |
| `catcher_name` | String | Catcher name. |
| `team_batting` | String | Team batting. |
| `team_fielding` | String | Team fielding. |
| `team_batting_id` | String | Team batting id. |
| `team_fielding_id` | String | Team fielding id. |
| `result` | String | Result. |
| `des` | String | Des. |
| `events` | String | Events. |
| `strikes` | String | Strikes. |
| `balls` | String | Balls. |
| `pre_strikes` | String | Pre strikes. |
| `pre_balls` | String | Pre balls. |
| `call` | String | Call. |
| `call_name` | String | Call name. |
| `pitch_type` | String | Pitch type code. |
| `pitch_name` | String | Pitch type name. |
| `description` | String | Description. |
| `result_code` | String | Result code. |
| `pitch_call` | String | Pitch call. |
| `is_strike_swinging` | String | Is strike swinging. |
| `balls_and_strikes` | String | Balls and strikes. |
| `start_speed` | String | Start speed. |
| `end_speed` | String | End speed. |
| `sz_top` | String | Sz top. |
| `sz_bot` | String | Sz bot. |
| `extension` | String | Release extension (ft). |
| `plate_time` | String | Plate time. |
| `zone` | String | Zone. |
| `spin_rate` | String | Spin rate (rpm). |
| `break_x` | String | Break x. |
| `induced_break_z` | String | Induced break z. |
| `break_z` | String | Break z. |
| `px` | String | Px. |
| `pz` | String | Pz. |
| `pfx_x` | String | Horizontal movement (in, pitcher perspective). |
| `pfx_z` | String | Induced vertical movement (in). |
| `is_bip_out` | String | Is bip out. |
| `pitch_number` | String | Pitch number. |
| `plate_x` | String | Plate x. |
| `plate_z` | String | Plate z. |
| `hit_speed` | String | Hit speed. |
| `hit_distance` | String | Hit distance. |
| `xba` | String | Expected batting average. |
| `hit_angle` | String | Hit angle. |
| `is_barrel` | String | Is barrel. |
| `hc_x` | String | Hc x. |
| `hc_y` | String | Hc y. |
| `launch_speed` | String | Exit velocity of the batted ball (mph). |
| `launch_angle` | String | Launch angle (deg). |
| `game_total_pitches` | String | Game total pitches. |
| `game_pk` | Int64 | MLBAM game id. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_gamefeed-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_gamefeed-example}

```python
mlb_statcast_gamefeed()
```

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
| `game_pk` | Int64 | MLBAM game id. |
| `game_guid` | String | Game GUID. |
| `link` | String | Stats API resource link. |
| `game_type` | String | Game type code (R/F/D/L/W/S/E/A). |
| `season` | String | Season year. |
| `game_date` | String | Game date/time (ISO 8601, UTC offset). |
| `official_date` | String | Official game date (YYYY-MM-DD). |
| `game_number` | Int64 | Game number (1, or 2 for the nightcap of a doubleheader). |
| `public_facing` | Boolean | Public facing. |
| `double_header` | String | Doubleheader flag (Y/N/S). |
| `gameday_type` | String | Gameday type. |
| `tiebreaker` | String | Tiebreaker. |
| `calendar_event_id` | String | Calendar event id. |
| `season_display` | String | Season display. |
| `day_night` | String | Day or night game. |
| `scheduled_innings` | Int64 | Scheduled innings (usually 9). |
| `reverse_home_away_status` | Boolean | Reverse home away status. |
| `inning_break_length` | Int64 | Inning break length. |
| `games_in_series` | Int64 | Total games in the series. |
| `series_game_number` | Int64 | Game number within the series. |
| `series_description` | String | Series description. |
| `record_source` | String | Record source. |
| `if_necessary` | String | If necessary. |
| `if_necessary_description` | String | If necessary description. |
| `status_abstract_game_state` | String | Status abstract game state. |
| `status_coded_game_state` | String | Status coded game state. |
| `status_detailed_state` | String | Status detailed state. |
| `status_status_code` | String | Status status code. |
| `status_start_time_tbd` | Boolean | Status start time tbd. |
| `status_abstract_game_code` | String | Status abstract game code. |
| `teams_away_team_spring_league_id` | Int64 | Away team team spring league id. |
| `teams_away_team_spring_league_name` | String | Away team team spring league name. |
| `teams_away_team_spring_league_link` | String | Away team team spring league link. |
| `teams_away_team_spring_league_abbreviation` | String | Away team team spring league abbreviation. |
| `teams_away_team_all_star_status` | String | Away team team all star status. |
| `teams_away_team_id` | Int64 | Away team team id. |
| `teams_away_team_name` | String | Away team team name. |
| `teams_away_team_link` | String | Away team team link. |
| `teams_away_team_season` | Int64 | Away team team season. |
| `teams_away_team_venue_id` | Int64 | Away team team venue id. |
| `teams_away_team_venue_name` | String | Away team team venue name. |
| `teams_away_team_venue_link` | String | Away team team venue link. |
| `teams_away_team_spring_venue_id` | Int64 | Away team team spring venue id. |
| `teams_away_team_spring_venue_link` | String | Away team team spring venue link. |
| `teams_away_team_team_code` | String | Away team team team code. |
| `teams_away_team_file_code` | String | Away team team file code. |
| `teams_away_team_abbreviation` | String | Away team team abbreviation. |
| `teams_away_team_team_name` | String | Away team team team name. |
| `teams_away_team_location_name` | String | Away team team location name. |
| `teams_away_team_first_year_of_play` | String | Away team team first year of play. |
| `teams_away_team_league_id` | Int64 | Away team team league id. |
| `teams_away_team_league_name` | String | Away team team league name. |
| `teams_away_team_league_link` | String | Away team team league link. |
| `teams_away_team_division_id` | Int64 | Away team team division id. |
| `teams_away_team_division_name` | String | Away team team division name. |
| `teams_away_team_division_link` | String | Away team team division link. |
| `teams_away_team_sport_id` | Int64 | Away team team sport id. |
| `teams_away_team_sport_link` | String | Away team team sport link. |
| `teams_away_team_sport_name` | String | Away team team sport name. |
| `teams_away_team_short_name` | String | Away team team short name. |
| `teams_away_team_franchise_name` | String | Away team team franchise name. |
| `teams_away_team_club_name` | String | Away team team club name. |
| `teams_away_team_active` | Boolean | Away team team active. |
| `teams_away_league_record_wins` | Int64 | Away team league record wins. |
| `teams_away_league_record_losses` | Int64 | Away team league record losses. |
| `teams_away_league_record_ties` | Int64 | Away team league record ties. |
| `teams_away_league_record_pct` | String | Away team league record rate. |
| `teams_away_probable_pitcher_id` | Int64 | Away team probable pitcher id. |
| `teams_away_probable_pitcher_full_name` | String | Away team probable pitcher full name. |
| `teams_away_probable_pitcher_link` | String | Away team probable pitcher link. |
| `teams_away_probable_pitcher_first_name` | String | Away team probable pitcher first name. |
| `teams_away_probable_pitcher_last_name` | String | Away team probable pitcher last name. |
| `teams_away_probable_pitcher_primary_number` | String | Away team probable pitcher primary number. |
| `teams_away_probable_pitcher_birth_date` | String | Away team probable pitcher birth date. |
| `teams_away_probable_pitcher_current_age` | Int64 | Away team probable pitcher current age. |
| `teams_away_probable_pitcher_birth_city` | String | Away team probable pitcher birth city. |
| `teams_away_probable_pitcher_birth_state_province` | String | Away team probable pitcher birth state province. |
| `teams_away_probable_pitcher_birth_country` | String | Away team probable pitcher birth country. |
| `teams_away_probable_pitcher_height` | String | Away team probable pitcher height. |
| `teams_away_probable_pitcher_weight` | Int64 | Away team probable pitcher weight. |
| `teams_away_probable_pitcher_active` | Boolean | Away team probable pitcher active. |
| `teams_away_probable_pitcher_primary_position_code` | String | Away team probable pitcher primary position code. |
| `teams_away_probable_pitcher_primary_position_name` | String | Away team probable pitcher primary position name. |
| `teams_away_probable_pitcher_primary_position_type` | String | Away team probable pitcher primary position type. |
| `teams_away_probable_pitcher_primary_position_abbreviation` | String | Away team probable pitcher primary position abbreviation. |
| `teams_away_probable_pitcher_use_name` | String | Away team probable pitcher use name. |
| `teams_away_probable_pitcher_use_last_name` | String | Away team probable pitcher use last name. |
| `teams_away_probable_pitcher_middle_name` | String | Away team probable pitcher middle name. |
| `teams_away_probable_pitcher_boxscore_name` | String | Away team probable pitcher boxscore name. |
| `teams_away_probable_pitcher_gender` | String | Away team probable pitcher gender. |
| `teams_away_probable_pitcher_is_player` | Boolean | Away team probable pitcher is player. |
| `teams_away_probable_pitcher_is_verified` | Boolean | Away team probable pitcher is verified. |
| `teams_away_probable_pitcher_draft_year` | Int64 | Away team probable pitcher draft year. |
| `teams_away_probable_pitcher_mlb_debut_date` | String | Away team probable pitcher mlb debut date. |
| `teams_away_probable_pitcher_bat_side_code` | String | Away team probable pitcher bat side code. |
| `teams_away_probable_pitcher_bat_side_description` | String | Away team probable pitcher bat side description. |
| `teams_away_probable_pitcher_pitch_hand_code` | String | Away team probable pitcher pitch hand code. |
| `teams_away_probable_pitcher_pitch_hand_description` | String | Away team probable pitcher pitch hand description. |
| `teams_away_probable_pitcher_name_first_last` | String | Away team probable pitcher name first last. |
| `teams_away_probable_pitcher_name_slug` | String | Away team probable pitcher name slug. |
| `teams_away_probable_pitcher_first_last_name` | String | Away team probable pitcher first last name. |
| `teams_away_probable_pitcher_last_first_name` | String | Away team probable pitcher last first name. |
| `teams_away_probable_pitcher_last_init_name` | String | Away team probable pitcher last init name. |
| `teams_away_probable_pitcher_init_last_name` | String | Away team probable pitcher init last name. |
| `teams_away_probable_pitcher_full_fml_name` | String | Away team probable pitcher full fml name. |
| `teams_away_probable_pitcher_full_lfm_name` | String | Away team probable pitcher full lfm name. |
| `teams_away_probable_pitcher_strike_zone_top` | Float64 | Away team probable pitcher strike zone top. |
| `teams_away_probable_pitcher_strike_zone_bottom` | Float64 | Away team probable pitcher strike zone bottom. |
| `teams_away_split_squad` | Boolean | Away team split squad. |
| `teams_away_series_number` | Int64 | Away team series number. |
| `teams_away_spring_league_id` | Int64 | Away team spring league id. |
| `teams_away_spring_league_name` | String | Away team spring league name. |
| `teams_away_spring_league_link` | String | Away team spring league link. |
| `teams_away_spring_league_abbreviation` | String | Away team spring league abbreviation. |
| `teams_home_team_spring_league_id` | Int64 | Home team team spring league id. |
| `teams_home_team_spring_league_name` | String | Home team team spring league name. |
| `teams_home_team_spring_league_link` | String | Home team team spring league link. |
| `teams_home_team_spring_league_abbreviation` | String | Home team team spring league abbreviation. |
| `teams_home_team_all_star_status` | String | Home team team all star status. |
| `teams_home_team_id` | Int64 | Home team team id. |
| `teams_home_team_name` | String | Home team team name. |
| `teams_home_team_link` | String | Home team team link. |
| `teams_home_team_season` | Int64 | Home team team season. |
| `teams_home_team_venue_id` | Int64 | Home team team venue id. |
| `teams_home_team_venue_name` | String | Home team team venue name. |
| `teams_home_team_venue_link` | String | Home team team venue link. |
| `teams_home_team_spring_venue_id` | Int64 | Home team team spring venue id. |
| `teams_home_team_spring_venue_link` | String | Home team team spring venue link. |
| `teams_home_team_team_code` | String | Home team team team code. |
| `teams_home_team_file_code` | String | Home team team file code. |
| `teams_home_team_abbreviation` | String | Home team team abbreviation. |
| `teams_home_team_team_name` | String | Home team team team name. |
| `teams_home_team_location_name` | String | Home team team location name. |
| `teams_home_team_first_year_of_play` | String | Home team team first year of play. |
| `teams_home_team_league_id` | Int64 | Home team team league id. |
| `teams_home_team_league_name` | String | Home team team league name. |
| `teams_home_team_league_link` | String | Home team team league link. |
| `teams_home_team_division_id` | Int64 | Home team team division id. |
| `teams_home_team_division_name` | String | Home team team division name. |
| `teams_home_team_division_link` | String | Home team team division link. |
| `teams_home_team_sport_id` | Int64 | Home team team sport id. |
| `teams_home_team_sport_link` | String | Home team team sport link. |
| `teams_home_team_sport_name` | String | Home team team sport name. |
| `teams_home_team_short_name` | String | Home team team short name. |
| `teams_home_team_franchise_name` | String | Home team team franchise name. |
| `teams_home_team_club_name` | String | Home team team club name. |
| `teams_home_team_active` | Boolean | Home team team active. |
| `teams_home_league_record_wins` | Int64 | Home team league record wins. |
| `teams_home_league_record_losses` | Int64 | Home team league record losses. |
| `teams_home_league_record_ties` | Int64 | Home team league record ties. |
| `teams_home_league_record_pct` | String | Home team league record rate. |
| `teams_home_probable_pitcher_id` | Int64 | Home team probable pitcher id. |
| `teams_home_probable_pitcher_full_name` | String | Home team probable pitcher full name. |
| `teams_home_probable_pitcher_link` | String | Home team probable pitcher link. |
| `teams_home_probable_pitcher_first_name` | String | Home team probable pitcher first name. |
| `teams_home_probable_pitcher_last_name` | String | Home team probable pitcher last name. |
| `teams_home_probable_pitcher_primary_number` | String | Home team probable pitcher primary number. |
| `teams_home_probable_pitcher_birth_date` | String | Home team probable pitcher birth date. |
| `teams_home_probable_pitcher_current_age` | Int64 | Home team probable pitcher current age. |
| `teams_home_probable_pitcher_birth_city` | String | Home team probable pitcher birth city. |
| `teams_home_probable_pitcher_birth_state_province` | String | Home team probable pitcher birth state province. |
| `teams_home_probable_pitcher_birth_country` | String | Home team probable pitcher birth country. |
| `teams_home_probable_pitcher_height` | String | Home team probable pitcher height. |
| `teams_home_probable_pitcher_weight` | Int64 | Home team probable pitcher weight. |
| `teams_home_probable_pitcher_active` | Boolean | Home team probable pitcher active. |
| `teams_home_probable_pitcher_primary_position_code` | String | Home team probable pitcher primary position code. |
| `teams_home_probable_pitcher_primary_position_name` | String | Home team probable pitcher primary position name. |
| `teams_home_probable_pitcher_primary_position_type` | String | Home team probable pitcher primary position type. |
| `teams_home_probable_pitcher_primary_position_abbreviation` | String | Home team probable pitcher primary position abbreviation. |
| `teams_home_probable_pitcher_use_name` | String | Home team probable pitcher use name. |
| `teams_home_probable_pitcher_use_last_name` | String | Home team probable pitcher use last name. |
| `teams_home_probable_pitcher_middle_name` | String | Home team probable pitcher middle name. |
| `teams_home_probable_pitcher_boxscore_name` | String | Home team probable pitcher boxscore name. |
| `teams_home_probable_pitcher_nick_name` | String | Home team probable pitcher nick name. |
| `teams_home_probable_pitcher_gender` | String | Home team probable pitcher gender. |
| `teams_home_probable_pitcher_is_player` | Boolean | Home team probable pitcher is player. |
| `teams_home_probable_pitcher_is_verified` | Boolean | Home team probable pitcher is verified. |
| `teams_home_probable_pitcher_draft_year` | Int64 | Home team probable pitcher draft year. |
| `teams_home_probable_pitcher_mlb_debut_date` | String | Home team probable pitcher mlb debut date. |
| `teams_home_probable_pitcher_bat_side_code` | String | Home team probable pitcher bat side code. |
| `teams_home_probable_pitcher_bat_side_description` | String | Home team probable pitcher bat side description. |
| `teams_home_probable_pitcher_pitch_hand_code` | String | Home team probable pitcher pitch hand code. |
| `teams_home_probable_pitcher_pitch_hand_description` | String | Home team probable pitcher pitch hand description. |
| `teams_home_probable_pitcher_name_first_last` | String | Home team probable pitcher name first last. |
| `teams_home_probable_pitcher_name_slug` | String | Home team probable pitcher name slug. |
| `teams_home_probable_pitcher_first_last_name` | String | Home team probable pitcher first last name. |
| `teams_home_probable_pitcher_last_first_name` | String | Home team probable pitcher last first name. |
| `teams_home_probable_pitcher_last_init_name` | String | Home team probable pitcher last init name. |
| `teams_home_probable_pitcher_init_last_name` | String | Home team probable pitcher init last name. |
| `teams_home_probable_pitcher_full_fml_name` | String | Home team probable pitcher full fml name. |
| `teams_home_probable_pitcher_full_lfm_name` | String | Home team probable pitcher full lfm name. |
| `teams_home_probable_pitcher_strike_zone_top` | Float64 | Home team probable pitcher strike zone top. |
| `teams_home_probable_pitcher_strike_zone_bottom` | Float64 | Home team probable pitcher strike zone bottom. |
| `teams_home_split_squad` | Boolean | Home team split squad. |
| `teams_home_series_number` | Int64 | Home team series number. |
| `teams_home_spring_league_id` | Int64 | Home team spring league id. |
| `teams_home_spring_league_name` | String | Home team spring league name. |
| `teams_home_spring_league_link` | String | Home team spring league link. |
| `teams_home_spring_league_abbreviation` | String | Home team spring league abbreviation. |
| `linescore_scheduled_innings` | Int64 | Linescore scheduled innings. |
| `linescore_innings` | String | Linescore innings. |
| `linescore_defense_team_id` | Int64 | Linescore defense team id. |
| `linescore_defense_team_name` | String | Linescore defense team name. |
| `linescore_defense_team_link` | String | Linescore defense team link. |
| `linescore_offense_team_id` | Int64 | Linescore offense team id. |
| `linescore_offense_team_name` | String | Linescore offense team name. |
| `linescore_offense_team_link` | String | Linescore offense team link. |
| `venue_id` | Int64 | MLBAM venue id. |
| `venue_name` | String | Ballpark name. |
| `venue_link` | String | Venue link. |
| `content_link` | String | Content link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#mlb_statcast_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#mlb_statcast_schedule-example}

```python
mlb_statcast_schedule()
```
