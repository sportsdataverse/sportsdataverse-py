---
title: CFB — ESPN CDN API (cdn.espn.com)
sidebar_label: ESPN CDN API (cdn.espn.com)
description: "CFB — ESPN CDN API (cdn.espn.com) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 24
toc_max_heading_level: 2
---
# CFB — ESPN CDN API (cdn.espn.com)

`sportsdataverse.cfb` — 5 endpoints.

## espn_cfb_cdn_playbyplay

One game's espn.com play-by-play page data. The gamepackageJSON block is a Site v2 summary (header, boxscore, plays or drives, win probability, ...), so the parsed result is the same dict of frames that parse_summary returns.

**Endpoint URL:** `GET https://cdn.espn.com/core/college-football/playbyplay`

**Valid URL:** [https://cdn.espn.com/core/college-football/playbyplay?xhr=1&gameId=401705127](https://cdn.espn.com/core/college-football/playbyplay?xhr=1&gameId=401705127)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | ESPN game (event) id. |

### Returns {#espn_cfb_cdn_playbyplay-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `interceptions` | character | Passing interceptions. |
| `adj_qbr` | character | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Team rushing yards. |
| `yards_per_rush_attempt` | character | Team yards per rush attempt. |
| `rushing_touchdowns` | character | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | character | Longest single rushing gain recorded by the player in the game. |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | character | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | character | Longest single reception recorded by the player in the game. |
| `fumbles` | character | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | character | Fumbles lost. |
| `fumbles_recovered` | character | Team fumbles recovered. |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | character | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | character | Team sacks. |
| `tackles_for_loss` | character | Team tackles for a loss. |
| `passes_defended` | character | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `hurries` | character | Number of times the player pressured the opposing passer into an early or errant throw. |
| `defensive_touchdowns` | character | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
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
| `field_goals_made/field_goal_attempts` | character | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | character | Field goal percentage (0-1). |
| `long_field_goal_made` | character | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | character | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | character | Total points contributed by the player through field goals and extra points in the game. |
| `punts` | character | Total number of punts executed by the player in the box score. |
| `punt_yards` | character | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | character | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | character | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | character | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | character | Distance of the longest individual punt executed by the player in the game. |

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

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `week` | integer | Game week of the season. |
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_midsize_name` | character | Medium-length display name for the league or competition as shown in the game header. |
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
| `overall` | character | Overall draft pick number. |
| `vs. conf.` | character | Team's record against conference opponents, shown as part of the standings snapshot in the box score. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Broadcast station / network name (e.g. `ESPN+`). |
| `station_key` | character | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | character | Broadcast language code. |
| `region` | character | Broadcast region code. |
| `is_national` | logical | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `media_call_letters` | character | Broadcast call letters for the outlet. |
| `media_name` | character | ESPN media name for the outlet. |
| `media_short_name` | character | Short ESPN media name for the outlet. |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |
| `regulation_clock` | double | Regulation clock. |
| `overtime_display_name` | character | Overtime display name. |
| `overtime_slug` | character | Overtime slug. |
| `sudden_death_periods` | integer | Number of sudden-death overtime periods defined in the game format rules. |

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
| `links_web_href` | character | Links web href. |
| `links_web_self_href` | character | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | character | Data-source-identified URL for the web page of the associated article content. |
| `links_api_self_href` | character | Links api self href. |
| `links_api_artwork_href` | character | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | character | URL for the article's page on ESPN's SportsCenter platform. |
| `byline` | character | Byline. |
| `links_mobile_href` | character | Links mobile href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `description` | character | Description. |
| `yards` | integer | Total yards gained on the drive. |
| `is_score` | logical | `TRUE` if the drive resulted in a score. |
| `offensive_plays` | integer | Number of offensive plays on the drive. |
| `result` | character | Drive result code (e.g. `PUNT`, `TD`). |
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
| `end_yard_line` | integer | Yard line at the end of the play. |
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
| `end_yard_line` | integer | Yard line at the end of the play. |
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
| `media_id` | character | ESPN media id for the outlet. |

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

### Example {#espn_cfb_cdn_playbyplay-example}

```python
espn_cfb_cdn_playbyplay(game_id='401705127')
```

_Last validated n/a._

## espn_cfb_cdn_boxscore

One game's espn.com box-score page data, parsed like a Site v2 summary. For football the drives and scoring plays are only on the playbyplay page.

**Endpoint URL:** `GET https://cdn.espn.com/core/college-football/boxscore`

**Valid URL:** [https://cdn.espn.com/core/college-football/boxscore?xhr=1&gameId=401705127](https://cdn.espn.com/core/college-football/boxscore?xhr=1&gameId=401705127)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `gameId` | `game_id` |  | `Y` |  | ESPN game (event) id. |

### Returns {#espn_cfb_cdn_boxscore-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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
| `interceptions` | character | Passing interceptions. |
| `adj_qbr` | character | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `rushing_attempts` | character | Team rushing attempts. |
| `rushing_yards` | character | Team rushing yards. |
| `yards_per_rush_attempt` | character | Team yards per rush attempt. |
| `rushing_touchdowns` | character | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | character | Longest single rushing gain recorded by the player in the game. |
| `receptions` | character | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receiving_yards` | character | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `yards_per_reception` | character | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | character | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | character | Longest single reception recorded by the player in the game. |
| `fumbles` | character | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | character | Fumbles lost. |
| `fumbles_recovered` | character | Team fumbles recovered. |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | character | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | character | Team sacks. |
| `tackles_for_loss` | character | Team tackles for a loss. |
| `passes_defended` | character | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `hurries` | character | Number of times the player pressured the opposing passer into an early or errant throw. |
| `defensive_touchdowns` | character | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
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
| `field_goals_made/field_goal_attempts` | character | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | character | Field goal percentage (0-1). |
| `long_field_goal_made` | character | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | character | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | character | Total points contributed by the player through field goals and extra points in the game. |
| `punts` | character | Total number of punts executed by the player in the box score. |
| `punt_yards` | character | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | character | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | character | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | character | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | character | Distance of the longest individual punt executed by the player in the game. |

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

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `week` | integer | Game week of the season. |
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_midsize_name` | character | Medium-length display name for the league or competition as shown in the game header. |
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
| `overall` | character | Overall draft pick number. |
| `vs. conf.` | character | Team's record against conference opponents, shown as part of the standings snapshot in the box score. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Broadcast station / network name (e.g. `ESPN+`). |
| `station_key` | character | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | character | Broadcast language code. |
| `region` | character | Broadcast region code. |
| `is_national` | logical | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Geographic market type (e.g. `National`). |
| `media_call_letters` | character | Broadcast call letters for the outlet. |
| `media_name` | character | ESPN media name for the outlet. |
| `media_short_name` | character | Short ESPN media name for the outlet. |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |
| `regulation_clock` | double | Regulation clock. |
| `overtime_display_name` | character | Overtime display name. |
| `overtime_slug` | character | Overtime slug. |
| `sudden_death_periods` | integer | Number of sudden-death overtime periods defined in the game format rules. |

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
| `links_web_href` | character | Links web href. |
| `links_web_self_href` | character | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | character | Data-source-identified URL for the web page of the associated article content. |
| `links_api_self_href` | character | Links api self href. |
| `links_api_artwork_href` | character | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | character | URL for the article's page on ESPN's SportsCenter platform. |
| `byline` | character | Byline. |
| `links_mobile_href` | character | Links mobile href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `description` | character | Description. |
| `yards` | integer | Total yards gained on the drive. |
| `is_score` | logical | `TRUE` if the drive resulted in a score. |
| `offensive_plays` | integer | Number of offensive plays on the drive. |
| `result` | character | Drive result code (e.g. `PUNT`, `TD`). |
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
| `end_yard_line` | integer | Yard line at the end of the play. |
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
| `end_yard_line` | integer | Yard line at the end of the play. |
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
| `media_id` | character | ESPN media id for the outlet. |

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

### Example {#espn_cfb_cdn_boxscore-example}

```python
espn_cfb_cdn_boxscore(game_id='401705127')
```

_Last validated n/a._

## espn_cfb_cdn_schedule

espn.com schedule page data, one row per game: up to 7 days starting at `date` (mbb and wbb: that day only; cfb and nfl: one week).

**Endpoint URL:** `GET https://cdn.espn.com/core/college-football/schedule`

**Valid URL:** [https://cdn.espn.com/core/college-football/schedule?xhr=1&date=20250115](https://cdn.espn.com/core/college-football/schedule?xhr=1&date=20250115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_cfb_cdn_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
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
| `home_rank` | integer | Home team rank (if ranked). |
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
| `away_rank` | integer | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_cdn_schedule-example}

```python
espn_cfb_cdn_schedule(date='20250115')
```

_Last validated n/a._

## espn_cfb_cdn_scoreboard

espn.com scoreboard page data for one day (one week for football), one row per game. The page's sbData block is a Site v2 scoreboard payload.

**Endpoint URL:** `GET https://cdn.espn.com/core/college-football/scoreboard`

**Valid URL:** [https://cdn.espn.com/core/college-football/scoreboard?xhr=1&date=20250115](https://cdn.espn.com/core/college-football/scoreboard?xhr=1&date=20250115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `date` | `date` |  |  | `Y` | Single date (YYYYMMDD). Ignored by cfb and nfl, which are week-oriented. Defaults to today. |
| `week` | `week` |  |  | `Y` | Week number (cfb and nfl). |
| `year` | `season` |  |  | `Y` | Season year that `week` belongs to (cfb and nfl). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase for `week`: 1=preseason, 2=regular season, 3=postseason (cfb and nfl). |

### Returns {#espn_cfb_cdn_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | ESPN global unique identifier. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | ESPN season type (2 = regular, 3 = postseason). |
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
| `home_rank` | integer | Home team rank (if ranked). |
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
| `away_rank` | integer | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_cdn_scoreboard-example}

```python
espn_cfb_cdn_scoreboard(date='20250115')
```

_Last validated n/a._

## espn_cfb_cdn_rankings

espn.com poll rankings page data: every poll (AP, Coaches, FCS, Division II and III) for one week, one row per ranked or vote-receiving team.

**Endpoint URL:** `GET https://cdn.espn.com/core/college-football/rankings`

**Valid URL:** [https://cdn.espn.com/core/college-football/rankings?xhr=1&week=5&year=2024&seasontype=2](https://cdn.espn.com/core/college-football/rankings?xhr=1&week=5&year=2024&seasontype=2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `week` | `week` |  |  | `Y` | Poll week. Defaults to the current week. |
| `year` | `season` |  |  | `Y` | Season year. Defaults to the current season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |

### Returns {#espn_cfb_cdn_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `poll_id` | integer | ESPN poll id, e.g. 1 = AP Top 25, 2 = AFCA Coaches Poll, 20 = FCS Coaches Poll, 11 / 12 = AFCA Division II / III Coaches Poll. |
| `poll_name` | character | Full poll name, e.g. 'AP Top 25', 'AFCA Coaches Poll', 'FCS Coaches Poll'. |
| `poll_short_name` | character | Short poll label, e.g. 'AP Poll'. |
| `ranked` | logical | TRUE for the poll's ranked teams; FALSE for teams that only received votes, whose rows carry just the team name and points. |
| `team_id` | character | ESPN team id as a string (the dtype of scoreboard home_id / away_id), read from team_url. Null on vote-receiving rows and on teams ESPN does not link (most Division II and III entries), so join on it only where present. |
| `team_display_name` | character | Team name as the poll page shows it. |
| `trend` | character | Movement since the previous poll as ESPN prints it, e.g. '+3', '-2', or '-' for no change. |
| `formatted_record` | character | Team's win-loss record at the poll date, e.g. '4-0'. |
| `first_place_votes` | integer | First-place votes received. Null on vote-receiving rows. |
| `rank` | integer | Position in the poll (1 = top). Null on vote-receiving rows. |
| `previous_rank` | integer | Position in the previous week's poll. Null on vote-receiving rows. |
| `team_abbreviation` | character | Short team code ESPN displays, e.g. 'TEX'. |
| `team_url` | character | espn.com team page URL. Absent for teams ESPN does not link. |
| `team_logo` | character | URL of the team logo on ESPN's CDN. |
| `points` | integer | Poll points received. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_cfb_cdn_rankings-example}

```python
espn_cfb_cdn_rankings(season=2024, week=5, season_type=2)
```

_Last validated n/a._
