---
title: NFL — ESPN CDN API (cdn.espn.com)
sidebar_label: ESPN CDN API (cdn.espn.com)
description: "NFL — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 24
toc_max_heading_level: 2
---
# NFL — ESPN CDN API (cdn.espn.com)

`sportsdataverse.nfl` — 4 endpoints.

## espn_nfl_cdn_playbyplay

One game's espn.com play-by-play page data. The gamepackageJSON block is a Site v2 summary (header, boxscore, plays or drives, win probability, ...), so the parsed result is the same dict of frames that parse_summary returns.

**Endpoint URL:** `GET https://cdn.espn.com/core/nfl/playbyplay`

**Valid URL:** [https://cdn.espn.com/core/nfl/playbyplay?xhr=1&gameId=401671881](https://cdn.espn.com/core/nfl/playbyplay?xhr=1&gameId=401671881)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | ESPN game (event) id. |

### Returns {#espn_nfl_cdn_playbyplay-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**boxscore_player**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `athlete_id` | character | Athlete id. |
| `athlete_display_name` | character | Athlete display name. |
| `athlete_short_name` | character | Athlete short name. |
| `athlete_jersey` | character | Athlete jersey. |
| `athlete_position` | character | Athlete position. |
| `starter` | character | Starter. |
| `active` | character | Active. |
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `completions/passing_attempts` | character | Pass completion ratio for the player in the box score, expressed as completions divided by pass attempts. |
| `passing_yards` | character | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `yards_per_pass_attempt` | character | Average passing yards gained per pass attempt by the player in the box score. |
| `passing_touchdowns` | character | Number of touchdown passes thrown by the player in the box score. |
| `interceptions` | character | The number of interceptions thrown. |
| `sacks_sack_yards_lost` | character | Combined sack statistics for the player, including sack count and total yards lost by the opposing offense. |
| `adj_qbr` | character | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `qb_rating` | character | Traditional passer rating for the quarterback in the box score, calculated from completions, yards, touchdowns, and interceptions. |
| `rushing_attempts` | character |  |
| `rushing_yards` | character | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `yards_per_rush_attempt` | character |  |
| `rushing_touchdowns` | character | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | character | Longest single rushing gain recorded by the player in the game. |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | character | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | character | Longest single reception recorded by the player in the game. |
| `receiving_targets` | character | Number of passing targets directed at the receiver during the game. |
| `fumbles` | character | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | character |  |
| `fumbles_recovered` | character |  |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | character | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | character | The Number of times sacked. |
| `tackles_for_loss` | character |  |
| `passes_defended` | character | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `qb_hits` | character | Number of times the player made contact with the opposing quarterback after or during a pass attempt. |
| `defensive_touchdowns` | character | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `interception_yards` | character |  |
| `interception_touchdowns` | character | Number of touchdowns scored by the player on interception return plays. |
| `kick_returns` | character |  |
| `kick_return_yards` | character |  |
| `yards_per_kick_return` | character | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | character | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | character | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | character |  |
| `punt_return_yards` | character |  |
| `yards_per_punt_return` | character | Average yards gained per punt return by the player in the box score. |
| `long_punt_return` | character | Longest single punt return yardage recorded by the player in the game. |
| `punt_return_touchdowns` | character | Number of touchdowns scored on punt returns by the player in the box score. |
| `punts` | character | Total number of punts executed by the player in the box score. |
| `punt_yards` | character | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | character | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | character | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | character | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | character | Distance of the longest individual punt executed by the player in the game. |
| `field_goals_made/field_goal_attempts` | character | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | character |  |
| `long_field_goal_made` | character | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | character | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | character | Total points contributed by the player through field goals and extra points in the game. |

**boxscore_team**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `home_away` | character | Home away. |
| `display_order` | integer | Display order. |
| `stat_name` | character | Stat name. |
| `stat_label` | character | Stat label. |
| `stat_display_value` | character | Stat display value. |
| `stat_value` | character | Stat value. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | double | Home win percentage. |
| `tie_percentage` | double | Tie percentage. |
| `play_id` | character | Play id. |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `category_name` | character | Category name. |
| `category_display_name` | character | Category display name. |
| `athlete_id` | character | Athlete id. |
| `athlete_display_name` | character | Athlete display name. |
| `athlete_position` | character | Athlete position. |
| `value` | double | Value. |
| `display_value` | character | Display value. |
| `main_stat` | character | Main stat. |
| `summary` | character | Summary. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | integer | Attendance. |
| `venue_id` | character | Venue id. |
| `venue_guid` | character | Venue guid. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
| `venue_address_zip_code` | character | Postal zip code of the venue where the game was played. |
| `venue_address_country` | character | Country of the venue where the game was played. |
| `venue_grass` | logical | Venue grass. |

**officials**

| col_name | type | description |
|---|---|---|
| `full_name` | character | Full name. |
| `display_name` | character | Display name. |
| `order` | integer | Order. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Position id. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `week` | integer | Season week. |
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_slug` | character | League slug. |
| `league_is_tournament` | logical | League is tournament. |
| `league_links` | character | League links. |
| `league_logos` | character | League logos. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `losses` | character | Losses. |
| `points_against` | character |  |
| `points_for` | character |  |
| `ties` | character |  |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |
| `overall` | character |  |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |
| `regulation_clock` | double | Regulation clock. |
| `overtime_display_name` | character | Overtime display name. |
| `overtime_slug` | character | Overtime slug. |
| `overtime_clock` | double | Overtime clock. |
| `sudden_death_periods` | integer | Number of sudden-death overtime periods defined in the game format rules. |
| `sudden_death_clock` | double | Clock duration or time limit for sudden-death overtime periods as defined in the game format. |

**article**

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `now_id` | character | Now id. |
| `content_key` | character | Content key. |
| `data_source_identifier` | character | Data source identifier. |
| `publishedkey` | character | Publishedkey. |
| `type` | character | Type. |
| `game_id` | character | Game id. |
| `headline` | character | Headline. |
| `description` | character | Description. |
| `link_text` | character | Link text. |
| `categorized` | character | Categorized. |
| `originally_posted` | character | Originally posted. |
| `last_modified` | character | Last modified. |
| `published` | character | Published. |
| `section` | character | Section. |
| `source` | character | Source. |
| `images` | character | Images. |
| `video` | character | Video. |
| `categories` | character | Categories. |
| `keywords` | character | Keywords. |
| `story` | character | Story. |
| `premium` | logical | Premium. |
| `is_live_blog` | logical | Is live blog. |
| `allow_comments` | logical | Allow comments. |
| `allow_search` | logical | Allow search. |
| `allow_content_reactions` | logical | Allow content reactions. |
| `links_web_href` | character | Links web href. |
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**injuries**

| col_name | type | description |
|---|---|---|
| `injuries` | character | Injuries. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |

**news**

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `now_id` | character | Now id. |
| `content_key` | character | Content key. |
| `data_source_identifier` | character | Data source identifier. |
| `type` | character | Type. |
| `headline` | character | Headline. |
| `description` | character | Description. |
| `last_modified` | character | Last modified. |
| `published` | character | Published. |
| `images` | character | Images. |
| `categories` | character | Categories. |
| `premium` | logical | Premium. |
| `byline` | character | Byline. |
| `links_web_href` | character | Links web href. |
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `description` | character | Description. |
| `yards` | integer | The number of receiving yards |
| `is_score` | logical |  |
| `offensive_plays` | integer |  |
| `result` | character | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `short_display_result` | character |  |
| `display_result` | character |  |
| `plays` | character |  |
| `team_id` | character | Team id. |
| `team_name` | character | Full display name of the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character |  |
| `team_logos` | character | Team logos. |
| `start_period_type` | character |  |
| `start_period_number` | integer | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | character | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | integer |  |
| `start_text` | character |  |
| `end_period_type` | character |  |
| `end_period_number` | integer | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | character | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_text` | character |  |
| `time_elapsed_display_value` | character | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | character |  |
| `drive_sequence` | integer | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `priority` | logical |  |
| `modified` | character |  |
| `wallclock` | character | Wallclock. |
| `team_participants` | character | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | logical |  |
| `stat_yardage` | integer |  |
| `is_turnover` | logical |  |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character |  |
| `period_number` | integer | Period number. |
| `clock_display_value` | character | Clock display value. |
| `start_down` | integer |  |
| `start_distance` | integer |  |
| `start_yard_line` | integer |  |
| `start_yards_to_endzone` | integer |  |
| `start_team_id` | character |  |
| `end_down` | integer |  |
| `end_distance` | integer |  |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_yards_to_endzone` | integer |  |
| `end_down_distance_text` | character |  |
| `end_short_down_distance_text` | character |  |
| `end_possession_text` | character |  |
| `end_team_id` | character |  |
| `start_down_distance_text` | character |  |
| `start_short_down_distance_text` | character |  |
| `start_possession_text` | character |  |
| `scoring_type_name` | character |  |
| `scoring_type_display_name` | character |  |
| `scoring_type_abbreviation` | character |  |
| `point_after_attempt_id` | double |  |
| `point_after_attempt_text` | character |  |
| `point_after_attempt_abbreviation` | character |  |
| `point_after_attempt_value` | double |  |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character |  |
| `period_number` | integer | Period number. |
| `clock_value` | double |  |
| `clock_display_value` | character | Clock display value. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |
| `scoring_type_name` | character |  |
| `scoring_type_display_name` | character |  |
| `scoring_type_abbreviation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_cdn_playbyplay-example}

```python
espn_nfl_cdn_playbyplay(game_id='401671881')
```

_Last validated n/a._

## espn_nfl_cdn_boxscore

One game's espn.com box-score page data, parsed like a Site v2 summary. For football the drives and scoring plays are only on the playbyplay page.

**Endpoint URL:** `GET https://cdn.espn.com/core/nfl/boxscore`

**Valid URL:** [https://cdn.espn.com/core/nfl/boxscore?xhr=1&gameId=401671881](https://cdn.espn.com/core/nfl/boxscore?xhr=1&gameId=401671881)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | ESPN game (event) id. |

### Returns {#espn_nfl_cdn_boxscore-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**boxscore_player**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `athlete_id` | character | Athlete id. |
| `athlete_display_name` | character | Athlete display name. |
| `athlete_short_name` | character | Athlete short name. |
| `athlete_jersey` | character | Athlete jersey. |
| `athlete_position` | character | Athlete position. |
| `starter` | character | Starter. |
| `active` | character | Active. |
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `completions/passing_attempts` | character | Pass completion ratio for the player in the box score, expressed as completions divided by pass attempts. |
| `passing_yards` | character | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `yards_per_pass_attempt` | character | Average passing yards gained per pass attempt by the player in the box score. |
| `passing_touchdowns` | character | Number of touchdown passes thrown by the player in the box score. |
| `interceptions` | character | The number of interceptions thrown. |
| `sacks_sack_yards_lost` | character | Combined sack statistics for the player, including sack count and total yards lost by the opposing offense. |
| `adj_qbr` | character | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `qb_rating` | character | Traditional passer rating for the quarterback in the box score, calculated from completions, yards, touchdowns, and interceptions. |
| `rushing_attempts` | character |  |
| `rushing_yards` | character | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `yards_per_rush_attempt` | character |  |
| `rushing_touchdowns` | character | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | character | Longest single rushing gain recorded by the player in the game. |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | character | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | character | Longest single reception recorded by the player in the game. |
| `receiving_targets` | character | Number of passing targets directed at the receiver during the game. |
| `fumbles` | character | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | character |  |
| `fumbles_recovered` | character |  |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | character | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | character | The Number of times sacked. |
| `tackles_for_loss` | character |  |
| `passes_defended` | character | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `qb_hits` | character | Number of times the player made contact with the opposing quarterback after or during a pass attempt. |
| `defensive_touchdowns` | character | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `interception_yards` | character |  |
| `interception_touchdowns` | character | Number of touchdowns scored by the player on interception return plays. |
| `kick_returns` | character |  |
| `kick_return_yards` | character |  |
| `yards_per_kick_return` | character | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | character | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | character | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | character |  |
| `punt_return_yards` | character |  |
| `yards_per_punt_return` | character | Average yards gained per punt return by the player in the box score. |
| `long_punt_return` | character | Longest single punt return yardage recorded by the player in the game. |
| `punt_return_touchdowns` | character | Number of touchdowns scored on punt returns by the player in the box score. |
| `punts` | character | Total number of punts executed by the player in the box score. |
| `punt_yards` | character | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | character | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | character | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | character | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | character | Distance of the longest individual punt executed by the player in the game. |
| `field_goals_made/field_goal_attempts` | character | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | character |  |
| `long_field_goal_made` | character | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | character | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | character | Total points contributed by the player through field goals and extra points in the game. |

**boxscore_team**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `home_away` | character | Home away. |
| `display_order` | integer | Display order. |
| `stat_name` | character | Stat name. |
| `stat_label` | character | Stat label. |
| `stat_display_value` | character | Stat display value. |
| `stat_value` | character | Stat value. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | double | Home win percentage. |
| `tie_percentage` | double | Tie percentage. |
| `play_id` | character | Play id. |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Team id. |
| `team_abbreviation` | character | Team abbreviation. |
| `category_name` | character | Category name. |
| `category_display_name` | character | Category display name. |
| `athlete_id` | character | Athlete id. |
| `athlete_display_name` | character | Athlete display name. |
| `athlete_position` | character | Athlete position. |
| `value` | double | Value. |
| `display_value` | character | Display value. |
| `main_stat` | character | Main stat. |
| `summary` | character | Summary. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | integer | Attendance. |
| `venue_id` | character | Venue id. |
| `venue_guid` | character | Venue guid. |
| `venue_full_name` | character | Venue full name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
| `venue_address_zip_code` | character | Postal zip code of the venue where the game was played. |
| `venue_address_country` | character | Country of the venue where the game was played. |
| `venue_grass` | logical | Venue grass. |

**officials**

| col_name | type | description |
|---|---|---|
| `full_name` | character | Full name. |
| `display_name` | character | Display name. |
| `order` | integer | Order. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Position id. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `week` | integer | Season week. |
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_slug` | character | League slug. |
| `league_is_tournament` | logical | League is tournament. |
| `league_links` | character | League links. |
| `league_logos` | character | League logos. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `losses` | character | Losses. |
| `points_against` | character |  |
| `points_for` | character |  |
| `ties` | character |  |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |
| `overall` | character |  |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |
| `regulation_clock` | double | Regulation clock. |
| `overtime_display_name` | character | Overtime display name. |
| `overtime_slug` | character | Overtime slug. |
| `overtime_clock` | double | Overtime clock. |
| `sudden_death_periods` | integer | Number of sudden-death overtime periods defined in the game format rules. |
| `sudden_death_clock` | double | Clock duration or time limit for sudden-death overtime periods as defined in the game format. |

**article**

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `now_id` | character | Now id. |
| `content_key` | character | Content key. |
| `data_source_identifier` | character | Data source identifier. |
| `publishedkey` | character | Publishedkey. |
| `type` | character | Type. |
| `game_id` | character | Game id. |
| `headline` | character | Headline. |
| `description` | character | Description. |
| `link_text` | character | Link text. |
| `categorized` | character | Categorized. |
| `originally_posted` | character | Originally posted. |
| `last_modified` | character | Last modified. |
| `published` | character | Published. |
| `section` | character | Section. |
| `source` | character | Source. |
| `images` | character | Images. |
| `video` | character | Video. |
| `categories` | character | Categories. |
| `keywords` | character | Keywords. |
| `story` | character | Story. |
| `premium` | logical | Premium. |
| `is_live_blog` | logical | Is live blog. |
| `allow_comments` | logical | Allow comments. |
| `allow_search` | logical | Allow search. |
| `allow_content_reactions` | logical | Allow content reactions. |
| `links_web_href` | character | Links web href. |
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**injuries**

| col_name | type | description |
|---|---|---|
| `injuries` | character | Injuries. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |

**news**

| col_name | type | description |
|---|---|---|
| `id` | integer | Id. |
| `now_id` | character | Now id. |
| `content_key` | character | Content key. |
| `data_source_identifier` | character | Data source identifier. |
| `type` | character | Type. |
| `headline` | character | Headline. |
| `description` | character | Description. |
| `last_modified` | character | Last modified. |
| `published` | character | Published. |
| `images` | character | Images. |
| `categories` | character | Categories. |
| `premium` | logical | Premium. |
| `byline` | character | Byline. |
| `links_web_href` | character | Links web href. |
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `description` | character | Description. |
| `yards` | integer | The number of receiving yards |
| `is_score` | logical |  |
| `offensive_plays` | integer |  |
| `result` | character | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `short_display_result` | character |  |
| `display_result` | character |  |
| `plays` | character |  |
| `team_id` | character | Team id. |
| `team_name` | character | Full display name of the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character |  |
| `team_logos` | character | Team logos. |
| `start_period_type` | character |  |
| `start_period_number` | integer | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | character | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | integer |  |
| `start_text` | character |  |
| `end_period_type` | character |  |
| `end_period_number` | integer | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | character | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_text` | character |  |
| `time_elapsed_display_value` | character | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | character |  |
| `drive_sequence` | integer | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `priority` | logical |  |
| `modified` | character |  |
| `wallclock` | character | Wallclock. |
| `team_participants` | character | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | logical |  |
| `stat_yardage` | integer |  |
| `is_turnover` | logical |  |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character |  |
| `period_number` | integer | Period number. |
| `clock_display_value` | character | Clock display value. |
| `start_down` | integer |  |
| `start_distance` | integer |  |
| `start_yard_line` | integer |  |
| `start_yards_to_endzone` | integer |  |
| `start_team_id` | character |  |
| `end_down` | integer |  |
| `end_distance` | integer |  |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_yards_to_endzone` | integer |  |
| `end_down_distance_text` | character |  |
| `end_short_down_distance_text` | character |  |
| `end_possession_text` | character |  |
| `end_team_id` | character |  |
| `start_down_distance_text` | character |  |
| `start_short_down_distance_text` | character |  |
| `start_possession_text` | character |  |
| `scoring_type_name` | character |  |
| `scoring_type_display_name` | character |  |
| `scoring_type_abbreviation` | character |  |
| `point_after_attempt_id` | double |  |
| `point_after_attempt_text` | character |  |
| `point_after_attempt_abbreviation` | character |  |
| `point_after_attempt_value` | double |  |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character |  |
| `period_number` | integer | Period number. |
| `clock_value` | double |  |
| `clock_display_value` | character | Clock display value. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |
| `scoring_type_name` | character |  |
| `scoring_type_display_name` | character |  |
| `scoring_type_abbreviation` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_cdn_boxscore-example}

```python
espn_nfl_cdn_boxscore(game_id='401671881')
```

_Last validated n/a._

## espn_nfl_cdn_schedule

espn.com schedule page data, one row per game: up to 7 days starting at `date` (mbb and wbb: that day only; cfb and nfl: one week).

**Endpoint URL:** `GET https://cdn.espn.com/core/nfl/schedule`

**Valid URL:** [https://cdn.espn.com/core/nfl/schedule?xhr=1&week=5&year=2024&seasontype=2](https://cdn.espn.com/core/nfl/schedule?xhr=1&week=5&year=2024&seasontype=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_nfl_cdn_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character |  |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | integer | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | character |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `status_clock` | double | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | character |  |
| `status_period` | integer | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical |  |
| `attendance` | integer |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_city` | character |  |
| `venue_state` | character |  |
| `venue_indoor` | logical |  |
| `broadcast` | character |  |
| `note` | character | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_location` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_logo` | character |  |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical |  |
| `home_rank` | character |  |
| `away_id` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_location` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_logo` | character |  |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical |  |
| `away_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_cdn_schedule-example}

```python
espn_nfl_cdn_schedule(season=2024, week=5, season_type=2)
```

_Last validated n/a._

## espn_nfl_cdn_scoreboard

espn.com scoreboard page data for one day (one week for football), one row per game. The page's sbData block is a Site v2 scoreboard payload.

**Endpoint URL:** `GET https://cdn.espn.com/core/nfl/scoreboard`

**Valid URL:** [https://cdn.espn.com/core/nfl/scoreboard?xhr=1&week=5&year=2024&seasontype=2](https://cdn.espn.com/core/nfl/scoreboard?xhr=1&week=5&year=2024&seasontype=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_nfl_cdn_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character |  |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | integer | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | character |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `status_clock` | double | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | character |  |
| `status_period` | integer | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical |  |
| `attendance` | integer |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_city` | character |  |
| `venue_state` | character |  |
| `venue_indoor` | logical |  |
| `broadcast` | character |  |
| `note` | character | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_location` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_logo` | character |  |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical |  |
| `home_rank` | character |  |
| `away_id` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_location` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_logo` | character |  |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical |  |
| `away_rank` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_cdn_scoreboard-example}

```python
espn_nfl_cdn_scoreboard(season=2024, week=5, season_type=2)
```

_Last validated n/a._
