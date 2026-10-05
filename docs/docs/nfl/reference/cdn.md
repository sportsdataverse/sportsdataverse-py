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

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `yards_per_rush_attempt` | character | Team yards per rush attempt. |
| `rushing_touchdowns` | character | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | character | Longest single rushing gain recorded by the player in the game. |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | character | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | character | Longest single reception recorded by the player in the game. |
| `receiving_targets` | character | Number of passing targets directed at the receiver during the game. |
| `fumbles` | character | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | character | Fumbles lost. |
| `fumbles_recovered` | character | Team fumbles recovered. |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | character | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | character | The Number of times sacked. |
| `tackles_for_loss` | character | Team tackles for a loss. |
| `passes_defended` | character | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `qb_hits` | character | Number of times the player made contact with the opposing quarterback after or during a pass attempt. |
| `defensive_touchdowns` | character | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `interception_yards` | character | Interception yards. |
| `interception_touchdowns` | character | Number of touchdowns scored by the player on interception return plays. |
| `kick_returns` | character | Number of kick returns. |
| `kick_return_yards` | character | Team kick return yards. |
| `yards_per_kick_return` | character | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | character | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | character | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | character | Number of punt returns. |
| `punt_return_yards` | character | Team punt return yards. |
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
| `field_goal_pct` | character | Field goal percentage (0-1). |
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
| `points_against` | character | Points allowed. |
| `points_for` | character | Goals/points scored. |
| `ties` | character | Number of ties in the series. |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |
| `overall` | character | Overall draft pick number. |

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
| `is_score` | logical | `TRUE` if the drive resulted in a score. |
| `offensive_plays` | integer | Number of offensive plays on the drive. |
| `result` | character | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `short_display_result` | character | Short drive-result label. |
| `display_result` | character | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `plays` | character | Total qualifying passing plays included in the WEPA calculation. |
| `team_id` | character | Team id. |
| `team_name` | character | Full display name of the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_logos` | character | Team logos. |
| `start_period_type` | character | Period type at the start of the drive (e.g. `quarter`). |
| `start_period_number` | integer | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | character | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | integer | Yard line at the start of the play. |
| `start_text` | character | Field-position text at the start of the drive. |
| `end_period_type` | character | Period type at the end of the drive (e.g. `quarter`). |
| `end_period_number` | integer | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | character | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_text` | character | Field-position text at the end of the drive. |
| `time_elapsed_display_value` | character | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | character | CFBD drive identifier the play belongs to. |
| `drive_sequence` | integer | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Wallclock. |
| `team_participants` | character | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | logical | `TRUE` if the play was a penalty. |
| `stat_yardage` | integer | Yards gained or lost on the play. |
| `is_turnover` | logical | `TRUE` if the play was a turnover. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `clock_display_value` | character | Clock display value. |
| `start_down` | integer | Down at the start of the play. |
| `start_distance` | integer | Yards to go at the start of the play. |
| `start_yard_line` | integer | Yard line at the start of the play. |
| `start_yards_to_endzone` | integer | Yards to the end zone at the start of the play. |
| `start_team_id` | character | ESPN team id in possession at the start of the play. |
| `end_down` | integer | Down at the end of the play. |
| `end_distance` | integer | Yards to go at the end of the play. |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_yards_to_endzone` | integer | Yards to the end zone at the end of the play. |
| `end_down_distance_text` | character | Down-and-distance text at the end of the play. |
| `end_short_down_distance_text` | character | Short down-and-distance text at the end of the play. |
| `end_possession_text` | character | Field-position text at the end of the play. |
| `end_team_id` | character | ESPN team id in possession at the end of the play. |
| `start_down_distance_text` | character | Down-and-distance text at the start of the play. |
| `start_short_down_distance_text` | character | Short down-and-distance text at the start of the play. |
| `start_possession_text` | character | Field-position text at the start of the play. |
| `scoring_type_name` | character | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | character | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | character | Scoring-type abbreviation (e.g. `TD`, `FG`). |
| `point_after_attempt_id` | double | Point-after-attempt id on a scoring play. |
| `point_after_attempt_text` | character | Point-after-attempt text (e.g. `Extra Point Good`). |
| `point_after_attempt_abbreviation` | character | Point-after-attempt abbreviation. |
| `point_after_attempt_value` | double | Points added by the point-after attempt. |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `clock_value` | double | Clock value in seconds. |
| `clock_display_value` | character | Clock display value. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |
| `scoring_type_name` | character | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | character | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | character | Scoring-type abbreviation (e.g. `TD`, `FG`). |

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

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).
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
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `yards_per_rush_attempt` | character | Team yards per rush attempt. |
| `rushing_touchdowns` | character | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | character | Longest single rushing gain recorded by the player in the game. |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | character | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | character | Longest single reception recorded by the player in the game. |
| `receiving_targets` | character | Number of passing targets directed at the receiver during the game. |
| `fumbles` | character | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | character | Fumbles lost. |
| `fumbles_recovered` | character | Team fumbles recovered. |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | character | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | character | The Number of times sacked. |
| `tackles_for_loss` | character | Team tackles for a loss. |
| `passes_defended` | character | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `qb_hits` | character | Number of times the player made contact with the opposing quarterback after or during a pass attempt. |
| `defensive_touchdowns` | character | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `interception_yards` | character | Interception yards. |
| `interception_touchdowns` | character | Number of touchdowns scored by the player on interception return plays. |
| `kick_returns` | character | Number of kick returns. |
| `kick_return_yards` | character | Team kick return yards. |
| `yards_per_kick_return` | character | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | character | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | character | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | character | Number of punt returns. |
| `punt_return_yards` | character | Team punt return yards. |
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
| `field_goal_pct` | character | Field goal percentage (0-1). |
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
| `points_against` | character | Points allowed. |
| `points_for` | character | Goals/points scored. |
| `ties` | character | Number of ties in the series. |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |
| `overall` | character | Overall draft pick number. |

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
| `is_score` | logical | `TRUE` if the drive resulted in a score. |
| `offensive_plays` | integer | Number of offensive plays on the drive. |
| `result` | character | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `short_display_result` | character | Short drive-result label. |
| `display_result` | character | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `plays` | character | Total qualifying passing plays included in the WEPA calculation. |
| `team_id` | character | Team id. |
| `team_name` | character | Full display name of the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character | Short team display name; `team_detail = TRUE` only. |
| `team_logos` | character | Team logos. |
| `start_period_type` | character | Period type at the start of the drive (e.g. `quarter`). |
| `start_period_number` | integer | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | character | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | integer | Yard line at the start of the play. |
| `start_text` | character | Field-position text at the start of the drive. |
| `end_period_type` | character | Period type at the end of the drive (e.g. `quarter`). |
| `end_period_number` | integer | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | character | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_text` | character | Field-position text at the end of the drive. |
| `time_elapsed_display_value` | character | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | character | CFBD drive identifier the play belongs to. |
| `drive_sequence` | integer | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `priority` | logical | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Wallclock. |
| `team_participants` | character | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | logical | `TRUE` if the play was a penalty. |
| `stat_yardage` | integer | Yards gained or lost on the play. |
| `is_turnover` | logical | `TRUE` if the play was a turnover. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `clock_display_value` | character | Clock display value. |
| `start_down` | integer | Down at the start of the play. |
| `start_distance` | integer | Yards to go at the start of the play. |
| `start_yard_line` | integer | Yard line at the start of the play. |
| `start_yards_to_endzone` | integer | Yards to the end zone at the start of the play. |
| `start_team_id` | character | ESPN team id in possession at the start of the play. |
| `end_down` | integer | Down at the end of the play. |
| `end_distance` | integer | Yards to go at the end of the play. |
| `end_yard_line` | integer | String indicating the yardline at the end of the given play consisting of team half and yard line number. |
| `end_yards_to_endzone` | integer | Yards to the end zone at the end of the play. |
| `end_down_distance_text` | character | Down-and-distance text at the end of the play. |
| `end_short_down_distance_text` | character | Short down-and-distance text at the end of the play. |
| `end_possession_text` | character | Field-position text at the end of the play. |
| `end_team_id` | character | ESPN team id in possession at the end of the play. |
| `start_down_distance_text` | character | Down-and-distance text at the start of the play. |
| `start_short_down_distance_text` | character | Short down-and-distance text at the start of the play. |
| `start_possession_text` | character | Field-position text at the start of the play. |
| `scoring_type_name` | character | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | character | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | character | Scoring-type abbreviation (e.g. `TD`, `FG`). |
| `point_after_attempt_id` | double | Point-after-attempt id on a scoring play. |
| `point_after_attempt_text` | character | Point-after-attempt text (e.g. `Extra Point Good`). |
| `point_after_attempt_abbreviation` | character | Point-after-attempt abbreviation. |
| `point_after_attempt_value` | double | Points added by the point-after attempt. |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `clock_value` | double | Clock value in seconds. |
| `clock_display_value` | character | Clock display value. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_display_name` | character | Team display name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_links` | character | Team links. |
| `team_logo` | character | Team logo. |
| `team_logos` | character | Team logos. |
| `scoring_type_name` | character | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | character | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | character | Scoring-type abbreviation (e.g. `TD`, `FG`). |

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
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | REG or POST indicating if the timeframe belongs to regular or post season. |
| `season_slug` | character | Season slug. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current period. |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical | Conference competition. |
| `attendance` | integer | Reported attendance at the game. |
| `venue_id` | character | Referencing venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | City where the venue is located. |
| `venue_state` | character | State (or province/country) where the venue is located. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast network short name. |
| `note` | character | Injury status and description. |
| `home_id` | character | Home team referencing id. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team's location. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | character | Home team rank (if ranked). |
| `away_id` | character | Away team referencing id. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team's location. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | character | Away team rank (if ranked). |

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
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | REG or POST indicating if the timeframe belongs to regular or post season. |
| `season_slug` | character | Season slug. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Current period. |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | logical | Conference competition. |
| `attendance` | integer | Reported attendance at the game. |
| `venue_id` | character | Referencing venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | City where the venue is located. |
| `venue_state` | character | State (or province/country) where the venue is located. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast network short name. |
| `note` | character | Injury status and description. |
| `home_id` | character | Home team referencing id. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team's location. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | character | Home team rank (if ranked). |
| `away_id` | character | Away team referencing id. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team's location. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | character | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_cdn_scoreboard-example}

```python
espn_nfl_cdn_scoreboard(season=2024, week=5, season_type=2)
```

_Last validated n/a._
