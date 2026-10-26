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
| `team_id` | Int64 | Team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_location` | String | Team location. |
| `athlete_id` | Int64 | Athlete id. |
| `athlete_display_name` | String | Athlete display name. |
| `athlete_short_name` | String | Athlete short name. |
| `athlete_jersey` | String | Athlete jersey. |
| `athlete_position` | String | Athlete position. |
| `starter` | String | Starter. |
| `active` | String | Active. |
| `did_not_play` | String | Did not play. |
| `ejected` | String | Ejected. |
| `reason` | String | Reason. |
| `completions/passing_attempts` | String | Pass completion ratio for the player in the box score, expressed as completions divided by pass attempts. |
| `passing_yards` | String | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `yards_per_pass_attempt` | String | Average passing yards gained per pass attempt by the player in the box score. |
| `passing_touchdowns` | String | Number of touchdown passes thrown by the player in the box score. |
| `interceptions` | String | The number of interceptions thrown. |
| `sacks_sack_yards_lost` | String | Combined sack statistics for the player, including sack count and total yards lost by the opposing offense. |
| `adj_qbr` | String | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `qb_rating` | String | Traditional passer rating for the quarterback in the box score, calculated from completions, yards, touchdowns, and interceptions. |
| `rushing_attempts` | String |  |
| `rushing_yards` | String | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `yards_per_rush_attempt` | String |  |
| `rushing_touchdowns` | String | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | String | Longest single rushing gain recorded by the player in the game. |
| `receptions` | String | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | String | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | String | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | String | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | String | Longest single reception recorded by the player in the game. |
| `receiving_targets` | String | Number of passing targets directed at the receiver during the game. |
| `fumbles` | String | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | String |  |
| `fumbles_recovered` | String |  |
| `total_tackles` | String | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | String | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | String | The Number of times sacked. |
| `tackles_for_loss` | String |  |
| `passes_defended` | String | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `qb_hits` | String | Number of times the player made contact with the opposing quarterback after or during a pass attempt. |
| `defensive_touchdowns` | String | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `interception_yards` | String |  |
| `interception_touchdowns` | String | Number of touchdowns scored by the player on interception return plays. |
| `kick_returns` | String |  |
| `kick_return_yards` | String |  |
| `yards_per_kick_return` | String | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | String | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | String | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | String |  |
| `punt_return_yards` | String |  |
| `yards_per_punt_return` | String | Average yards gained per punt return by the player in the box score. |
| `long_punt_return` | String | Longest single punt return yardage recorded by the player in the game. |
| `punt_return_touchdowns` | String | Number of touchdowns scored on punt returns by the player in the box score. |
| `punts` | String | Total number of punts executed by the player in the box score. |
| `punt_yards` | String | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | String | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | String | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | String | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | String | Distance of the longest individual punt executed by the player in the game. |
| `field_goals_made/field_goal_attempts` | String | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | String |  |
| `long_field_goal_made` | String | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | String | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | String | Total points contributed by the player through field goals and extra points in the game. |

**boxscore_team**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `home_away` | String | Home away. |
| `display_order` | Int64 | Display order. |
| `stat_name` | String | Stat name. |
| `stat_label` | String | Stat label. |
| `stat_display_value` | String | Stat display value. |
| `stat_value` | String | Stat value. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | Float64 | Home win percentage. |
| `tie_percentage` | Float64 | Tie percentage. |
| `play_id` | Int64 | Play id. |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `category_name` | String | Category name. |
| `category_display_name` | String | Category display name. |
| `athlete_id` | Int64 | Athlete id. |
| `athlete_display_name` | String | Athlete display name. |
| `athlete_position` | String | Athlete position. |
| `value` | Float64 | Value. |
| `display_value` | String | Display value. |
| `main_stat` | String | Main stat. |
| `summary` | String | Summary. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | Int64 | Attendance. |
| `venue_id` | Int64 | Venue id. |
| `venue_guid` | String | Venue guid. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state. |
| `venue_address_zip_code` | String | Postal zip code of the venue where the game was played. |
| `venue_address_country` | String | Country of the venue where the game was played. |
| `venue_grass` | Boolean | Venue grass. |

**officials**

| col_name | type | description |
|---|---|---|
| `full_name` | String | Full name. |
| `display_name` | String | Display name. |
| `order` | Int64 | Order. |
| `position_name` | String | Position name. |
| `position_display_name` | String | Position display name. |
| `position_id` | Int64 | Position id. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `uid` | String | Uid. |
| `time_valid` | Boolean | Time valid. |
| `competitions` | String | Competitions. |
| `links` | String | Links. |
| `week` | Int64 | Season week. |
| `game_note` | String | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | Int64 | Season year. |
| `season_current` | Boolean | Season current. |
| `season_type` | Int64 | Season type. |
| `league_id` | Int64 | League id. |
| `league_uid` | String | League uid. |
| `league_name` | String | League name. |
| `league_abbreviation` | String | League abbreviation. |
| `league_slug` | String | League slug. |
| `league_is_tournament` | Boolean | League is tournament. |
| `league_links` | String | League links. |
| `league_logos` | String | League logos. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | String | Group header. |
| `conference_header` | String | Conference header. |
| `division_header` | String | Division header. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_location` | String | Team location. |
| `losses` | String | Losses. |
| `points_against` | String |  |
| `points_for` | String |  |
| `ties` | String |  |
| `win_percent` | String | Win percent. |
| `wins` | String | Wins. |
| `overall` | String |  |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | Int64 | Regulation periods. |
| `regulation_display_name` | String | Regulation display name. |
| `regulation_slug` | String | Regulation slug. |
| `regulation_clock` | Float64 | Regulation clock. |
| `overtime_display_name` | String | Overtime display name. |
| `overtime_slug` | String | Overtime slug. |
| `overtime_clock` | Float64 | Overtime clock. |
| `sudden_death_periods` | Int64 | Number of sudden-death overtime periods defined in the game format rules. |
| `sudden_death_clock` | Float64 | Clock duration or time limit for sudden-death overtime periods as defined in the game format. |

**article**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `now_id` | String | Now id. |
| `content_key` | String | Content key. |
| `data_source_identifier` | String | Data source identifier. |
| `publishedkey` | String | Publishedkey. |
| `type` | String | Type. |
| `game_id` | Int64 | Game id. |
| `headline` | String | Headline. |
| `description` | String | Description. |
| `link_text` | String | Link text. |
| `categorized` | String | Categorized. |
| `originally_posted` | String | Originally posted. |
| `last_modified` | String | Last modified. |
| `published` | String | Published. |
| `section` | String | Section. |
| `source` | String | Source. |
| `images` | String | Images. |
| `video` | String | Video. |
| `categories` | String | Categories. |
| `keywords` | String | Keywords. |
| `story` | String | Story. |
| `premium` | Boolean | Premium. |
| `is_live_blog` | Boolean | Is live blog. |
| `allow_comments` | Boolean | Allow comments. |
| `allow_search` | Boolean | Allow search. |
| `allow_content_reactions` | Boolean | Allow content reactions. |
| `links_web_href` | String | Links web href. |
| `links_mobile_href` | String | Links mobile href. |
| `links_api_self_href` | String | Links api self href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |

**injuries**

| col_name | type | description |
|---|---|---|
| `injuries` | String | Injuries. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_display_name` | String | Team display name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_links` | String | Team links. |
| `team_logo` | String | Team logo. |
| `team_logos` | String | Team logos. |

**news**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `now_id` | String | Now id. |
| `content_key` | String | Content key. |
| `data_source_identifier` | String | Data source identifier. |
| `type` | String | Type. |
| `headline` | String | Headline. |
| `description` | String | Description. |
| `last_modified` | String | Last modified. |
| `published` | String | Published. |
| `images` | String | Images. |
| `categories` | String | Categories. |
| `premium` | Boolean | Premium. |
| `byline` | String | Byline. |
| `links_web_href` | String | Links web href. |
| `links_mobile_href` | String | Links mobile href. |
| `links_api_self_href` | String | Links api self href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `description` | String | Description. |
| `yards` | Int64 | The number of receiving yards |
| `is_score` | Boolean |  |
| `offensive_plays` | Int64 |  |
| `result` | String | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `short_display_result` | String |  |
| `display_result` | String |  |
| `plays` | String |  |
| `team_id` | Int64 | Team id. |
| `team_name` | String | Full display name of the team. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_short_display_name` | String |  |
| `team_logos` | String | Team logos. |
| `start_period_type` | String |  |
| `start_period_number` | Int64 | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | String | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | Int64 |  |
| `start_text` | String |  |
| `end_period_type` | String |  |
| `end_period_number` | Int64 | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | String | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | Int64 | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_text` | String |  |
| `time_elapsed_display_value` | String | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | Int64 |  |
| `drive_sequence` | Int64 | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | Int64 | Id. |
| `sequence_number` | String | Sequence number. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `scoring_play` | Boolean | Scoring play. |
| `priority` | Boolean |  |
| `modified` | String |  |
| `wallclock` | String | Wallclock. |
| `team_participants` | String | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | Boolean |  |
| `stat_yardage` | Int64 |  |
| `is_turnover` | Boolean |  |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_abbreviation` | String |  |
| `period_number` | Int64 | Period number. |
| `clock_display_value` | String | Clock display value. |
| `start_down` | Int64 |  |
| `start_distance` | Int64 |  |
| `start_yard_line` | Int64 |  |
| `start_yards_to_endzone` | Int64 |  |
| `start_team_id` | Int64 |  |
| `end_down` | Int64 |  |
| `end_distance` | Int64 |  |
| `end_yard_line` | Int64 | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_yards_to_endzone` | Int64 |  |
| `end_down_distance_text` | String |  |
| `end_short_down_distance_text` | String |  |
| `end_possession_text` | String |  |
| `end_team_id` | Int64 |  |
| `start_down_distance_text` | String |  |
| `start_short_down_distance_text` | String |  |
| `start_possession_text` | String |  |
| `scoring_type_name` | String |  |
| `scoring_type_display_name` | String |  |
| `scoring_type_abbreviation` | String |  |
| `point_after_attempt_id` | Int64 |  |
| `point_after_attempt_text` | String |  |
| `point_after_attempt_abbreviation` | String |  |
| `point_after_attempt_value` | Float64 |  |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_abbreviation` | String |  |
| `period_number` | Int64 | Period number. |
| `clock_value` | Float64 |  |
| `clock_display_value` | String | Clock display value. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_display_name` | String | Team display name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_links` | String | Team links. |
| `team_logo` | String | Team logo. |
| `team_logos` | String | Team logos. |
| `scoring_type_name` | String |  |
| `scoring_type_display_name` | String |  |
| `scoring_type_abbreviation` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_cdn_playbyplay-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_cdn_playbyplay-example}

```python
espn_nfl_cdn_playbyplay(game_id='401671881')
```

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
| `team_id` | Int64 | Team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_location` | String | Team location. |
| `athlete_id` | Int64 | Athlete id. |
| `athlete_display_name` | String | Athlete display name. |
| `athlete_short_name` | String | Athlete short name. |
| `athlete_jersey` | String | Athlete jersey. |
| `athlete_position` | String | Athlete position. |
| `starter` | String | Starter. |
| `active` | String | Active. |
| `did_not_play` | String | Did not play. |
| `ejected` | String | Ejected. |
| `reason` | String | Reason. |
| `completions/passing_attempts` | String | Pass completion ratio for the player in the box score, expressed as completions divided by pass attempts. |
| `passing_yards` | String | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `yards_per_pass_attempt` | String | Average passing yards gained per pass attempt by the player in the box score. |
| `passing_touchdowns` | String | Number of touchdown passes thrown by the player in the box score. |
| `interceptions` | String | The number of interceptions thrown. |
| `sacks_sack_yards_lost` | String | Combined sack statistics for the player, including sack count and total yards lost by the opposing offense. |
| `adj_qbr` | String | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `qb_rating` | String | Traditional passer rating for the quarterback in the box score, calculated from completions, yards, touchdowns, and interceptions. |
| `rushing_attempts` | String |  |
| `rushing_yards` | String | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `yards_per_rush_attempt` | String |  |
| `rushing_touchdowns` | String | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | String | Longest single rushing gain recorded by the player in the game. |
| `receptions` | String | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | String | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | String | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | String | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | String | Longest single reception recorded by the player in the game. |
| `receiving_targets` | String | Number of passing targets directed at the receiver during the game. |
| `fumbles` | String | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | String |  |
| `fumbles_recovered` | String |  |
| `total_tackles` | String | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | String | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | String | The Number of times sacked. |
| `tackles_for_loss` | String |  |
| `passes_defended` | String | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `qb_hits` | String | Number of times the player made contact with the opposing quarterback after or during a pass attempt. |
| `defensive_touchdowns` | String | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `interception_yards` | String |  |
| `interception_touchdowns` | String | Number of touchdowns scored by the player on interception return plays. |
| `kick_returns` | String |  |
| `kick_return_yards` | String |  |
| `yards_per_kick_return` | String | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | String | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | String | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | String |  |
| `punt_return_yards` | String |  |
| `yards_per_punt_return` | String | Average yards gained per punt return by the player in the box score. |
| `long_punt_return` | String | Longest single punt return yardage recorded by the player in the game. |
| `punt_return_touchdowns` | String | Number of touchdowns scored on punt returns by the player in the box score. |
| `punts` | String | Total number of punts executed by the player in the box score. |
| `punt_yards` | String | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | String | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | String | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | String | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | String | Distance of the longest individual punt executed by the player in the game. |
| `field_goals_made/field_goal_attempts` | String | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | String |  |
| `long_field_goal_made` | String | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | String | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | String | Total points contributed by the player through field goals and extra points in the game. |

**boxscore_team**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `home_away` | String | Home away. |
| `display_order` | Int64 | Display order. |
| `stat_name` | String | Stat name. |
| `stat_label` | String | Stat label. |
| `stat_display_value` | String | Stat display value. |
| `stat_value` | String | Stat value. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | Float64 | Home win percentage. |
| `tie_percentage` | Float64 | Tie percentage. |
| `play_id` | Int64 | Play id. |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | Team id. |
| `team_abbreviation` | String | Team abbreviation. |
| `category_name` | String | Category name. |
| `category_display_name` | String | Category display name. |
| `athlete_id` | Int64 | Athlete id. |
| `athlete_display_name` | String | Athlete display name. |
| `athlete_position` | String | Athlete position. |
| `value` | Float64 | Value. |
| `display_value` | String | Display value. |
| `main_stat` | String | Main stat. |
| `summary` | String | Summary. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | Int64 | Attendance. |
| `venue_id` | Int64 | Venue id. |
| `venue_guid` | String | Venue guid. |
| `venue_full_name` | String | Venue full name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state. |
| `venue_address_zip_code` | String | Postal zip code of the venue where the game was played. |
| `venue_address_country` | String | Country of the venue where the game was played. |
| `venue_grass` | Boolean | Venue grass. |

**officials**

| col_name | type | description |
|---|---|---|
| `full_name` | String | Full name. |
| `display_name` | String | Display name. |
| `order` | Int64 | Order. |
| `position_name` | String | Position name. |
| `position_display_name` | String | Position display name. |
| `position_id` | Int64 | Position id. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `uid` | String | Uid. |
| `time_valid` | Boolean | Time valid. |
| `competitions` | String | Competitions. |
| `links` | String | Links. |
| `week` | Int64 | Season week. |
| `game_note` | String | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | Int64 | Season year. |
| `season_current` | Boolean | Season current. |
| `season_type` | Int64 | Season type. |
| `league_id` | Int64 | League id. |
| `league_uid` | String | League uid. |
| `league_name` | String | League name. |
| `league_abbreviation` | String | League abbreviation. |
| `league_slug` | String | League slug. |
| `league_is_tournament` | Boolean | League is tournament. |
| `league_links` | String | League links. |
| `league_logos` | String | League logos. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | String | Group header. |
| `conference_header` | String | Conference header. |
| `division_header` | String | Division header. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_location` | String | Team location. |
| `losses` | String | Losses. |
| `points_against` | String |  |
| `points_for` | String |  |
| `ties` | String |  |
| `win_percent` | String | Win percent. |
| `wins` | String | Wins. |
| `overall` | String |  |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | Int64 | Regulation periods. |
| `regulation_display_name` | String | Regulation display name. |
| `regulation_slug` | String | Regulation slug. |
| `regulation_clock` | Float64 | Regulation clock. |
| `overtime_display_name` | String | Overtime display name. |
| `overtime_slug` | String | Overtime slug. |
| `overtime_clock` | Float64 | Overtime clock. |
| `sudden_death_periods` | Int64 | Number of sudden-death overtime periods defined in the game format rules. |
| `sudden_death_clock` | Float64 | Clock duration or time limit for sudden-death overtime periods as defined in the game format. |

**article**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `now_id` | String | Now id. |
| `content_key` | String | Content key. |
| `data_source_identifier` | String | Data source identifier. |
| `publishedkey` | String | Publishedkey. |
| `type` | String | Type. |
| `game_id` | Int64 | Game id. |
| `headline` | String | Headline. |
| `description` | String | Description. |
| `link_text` | String | Link text. |
| `categorized` | String | Categorized. |
| `originally_posted` | String | Originally posted. |
| `last_modified` | String | Last modified. |
| `published` | String | Published. |
| `section` | String | Section. |
| `source` | String | Source. |
| `images` | String | Images. |
| `video` | String | Video. |
| `categories` | String | Categories. |
| `keywords` | String | Keywords. |
| `story` | String | Story. |
| `premium` | Boolean | Premium. |
| `is_live_blog` | Boolean | Is live blog. |
| `allow_comments` | Boolean | Allow comments. |
| `allow_search` | Boolean | Allow search. |
| `allow_content_reactions` | Boolean | Allow content reactions. |
| `links_web_href` | String | Links web href. |
| `links_mobile_href` | String | Links mobile href. |
| `links_api_self_href` | String | Links api self href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |

**injuries**

| col_name | type | description |
|---|---|---|
| `injuries` | String | Injuries. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_display_name` | String | Team display name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_links` | String | Team links. |
| `team_logo` | String | Team logo. |
| `team_logos` | String | Team logos. |

**news**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `now_id` | String | Now id. |
| `content_key` | String | Content key. |
| `data_source_identifier` | String | Data source identifier. |
| `type` | String | Type. |
| `headline` | String | Headline. |
| `description` | String | Description. |
| `last_modified` | String | Last modified. |
| `published` | String | Published. |
| `images` | String | Images. |
| `categories` | String | Categories. |
| `premium` | Boolean | Premium. |
| `byline` | String | Byline. |
| `links_web_href` | String | Links web href. |
| `links_mobile_href` | String | Links mobile href. |
| `links_api_self_href` | String | Links api self href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `description` | String | Description. |
| `yards` | Int64 | The number of receiving yards |
| `is_score` | Boolean |  |
| `offensive_plays` | Int64 |  |
| `result` | String | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `short_display_result` | String |  |
| `display_result` | String |  |
| `plays` | String |  |
| `team_id` | Int64 | Team id. |
| `team_name` | String | Full display name of the team. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_short_display_name` | String |  |
| `team_logos` | String | Team logos. |
| `start_period_type` | String |  |
| `start_period_number` | Int64 | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | String | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | Int64 |  |
| `start_text` | String |  |
| `end_period_type` | String |  |
| `end_period_number` | Int64 | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | String | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | Int64 | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_text` | String |  |
| `time_elapsed_display_value` | String | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | Int64 |  |
| `drive_sequence` | Int64 | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | Int64 | Id. |
| `sequence_number` | String | Sequence number. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `scoring_play` | Boolean | Scoring play. |
| `priority` | Boolean |  |
| `modified` | String |  |
| `wallclock` | String | Wallclock. |
| `team_participants` | String | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | Boolean |  |
| `stat_yardage` | Int64 |  |
| `is_turnover` | Boolean |  |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_abbreviation` | String |  |
| `period_number` | Int64 | Period number. |
| `clock_display_value` | String | Clock display value. |
| `start_down` | Int64 |  |
| `start_distance` | Int64 |  |
| `start_yard_line` | Int64 |  |
| `start_yards_to_endzone` | Int64 |  |
| `start_team_id` | Int64 |  |
| `end_down` | Int64 |  |
| `end_distance` | Int64 |  |
| `end_yard_line` | Int64 | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_yards_to_endzone` | Int64 |  |
| `end_down_distance_text` | String |  |
| `end_short_down_distance_text` | String |  |
| `end_possession_text` | String |  |
| `end_team_id` | Int64 |  |
| `start_down_distance_text` | String |  |
| `start_short_down_distance_text` | String |  |
| `start_possession_text` | String |  |
| `scoring_type_name` | String |  |
| `scoring_type_display_name` | String |  |
| `scoring_type_abbreviation` | String |  |
| `point_after_attempt_id` | Int64 |  |
| `point_after_attempt_text` | String |  |
| `point_after_attempt_abbreviation` | String |  |
| `point_after_attempt_value` | Float64 |  |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_abbreviation` | String |  |
| `period_number` | Int64 | Period number. |
| `clock_value` | Float64 |  |
| `clock_display_value` | String | Clock display value. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_display_name` | String | Team display name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_links` | String | Team links. |
| `team_logo` | String | Team logo. |
| `team_logos` | String | Team logos. |
| `scoring_type_name` | String |  |
| `scoring_type_display_name` | String |  |
| `scoring_type_abbreviation` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_cdn_boxscore-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_cdn_boxscore-example}

```python
espn_nfl_cdn_boxscore(game_id='401671881')
```

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
| `game_id` | Int64 | ESPN event id. |
| `uid` | String |  |
| `date` | String | Match start timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | Int64 | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | Int64 | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | String | ESPN slug of the event's season type (e.g. 'regular-season', 'post-season'; soccer competitions use their own, such as 'final' or 'torneo-apertura'). |
| `status_type_id` | Int64 |  |
| `status_type_name` | String |  |
| `status_type_state` | String |  |
| `status_type_completed` | Boolean |  |
| `status_type_description` | String |  |
| `status_type_detail` | String |  |
| `status_type_short_detail` | String |  |
| `status_clock` | Float64 | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | String |  |
| `status_period` | Int64 | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | Boolean | Whether the match is played at a neutral venue. |
| `conference_competition` | Boolean |  |
| `attendance` | Int64 |  |
| `venue_id` | Int64 |  |
| `venue_full_name` | String |  |
| `venue_city` | String |  |
| `venue_state` | String |  |
| `venue_indoor` | Boolean |  |
| `broadcast` | String | Every network name of the competition in feed order, deduped and ", "-joined (an entry with no names gives its market); "" when the game lists no broadcast. |
| `note` | String | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | Int64 |  |
| `home_name` | String |  |
| `home_abbreviation` | String |  |
| `home_display_name` | String |  |
| `home_location` | String |  |
| `home_color` | String |  |
| `home_alternate_color` | String |  |
| `home_logo` | String |  |
| `home_score` | String | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | Boolean |  |
| `home_rank` | String |  |
| `away_id` | Int64 |  |
| `away_name` | String |  |
| `away_abbreviation` | String |  |
| `away_display_name` | String |  |
| `away_location` | String |  |
| `away_color` | String |  |
| `away_alternate_color` | String |  |
| `away_logo` | String |  |
| `away_score` | String | Away team's score. For cricket, the innings string. |
| `away_winner` | Boolean |  |
| `away_rank` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_cdn_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_cdn_schedule-example}

```python
espn_nfl_cdn_schedule(season=2024, week=5, season_type=2)
```

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
| `game_id` | Int64 | ESPN event id. |
| `uid` | String |  |
| `date` | String | Match start timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | Int64 | Integer season year ESPN assigns the event (e.g. 2025 for the 2025-26 season). |
| `season_type` | Int64 | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | String | ESPN slug of the event's season type (e.g. 'regular-season', 'post-season'; soccer competitions use their own, such as 'final' or 'torneo-apertura'). |
| `status_type_id` | Int64 |  |
| `status_type_name` | String |  |
| `status_type_state` | String |  |
| `status_type_completed` | Boolean |  |
| `status_type_description` | String |  |
| `status_type_detail` | String |  |
| `status_type_short_detail` | String |  |
| `status_clock` | Float64 | Game clock in seconds as ESPN reports it: time remaining in the period for clock sports, elapsed seconds for soccer (e.g. 5400.0 at full time); 0.0 once a game has ended. |
| `status_display_clock` | String |  |
| `status_period` | Int64 | Current or final period number (quarter, half, inning or period, depending on the sport). |
| `neutral_site` | Boolean | Whether the match is played at a neutral venue. |
| `conference_competition` | Boolean |  |
| `attendance` | Int64 |  |
| `venue_id` | Int64 |  |
| `venue_full_name` | String |  |
| `venue_city` | String |  |
| `venue_state` | String |  |
| `venue_indoor` | Boolean |  |
| `broadcast` | String | Every network name of the competition in feed order, deduped and ", "-joined (an entry with no names gives its market); "" when the game lists no broadcast. |
| `note` | String | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | Int64 |  |
| `home_name` | String |  |
| `home_abbreviation` | String |  |
| `home_display_name` | String |  |
| `home_location` | String |  |
| `home_color` | String |  |
| `home_alternate_color` | String |  |
| `home_logo` | String |  |
| `home_score` | String | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | Boolean |  |
| `home_rank` | String |  |
| `away_id` | Int64 |  |
| `away_name` | String |  |
| `away_abbreviation` | String |  |
| `away_display_name` | String |  |
| `away_location` | String |  |
| `away_color` | String |  |
| `away_alternate_color` | String |  |
| `away_logo` | String |  |
| `away_score` | String | Away team's score. For cricket, the innings string. |
| `away_winner` | Boolean |  |
| `away_rank` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_nfl_cdn_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_nfl_cdn_scoreboard-example}

```python
espn_nfl_cdn_scoreboard(season=2024, week=5, season_type=2)
```
