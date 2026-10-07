---
title: NFL — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
description: "NFL — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 20
toc_max_heading_level: 2
---
# NFL — ESPN site API (v2)

`sportsdataverse.nfl` — 24 endpoints.

## espn_nfl_scoreboard

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=20240115&limit=500](https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=20240115&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_scoreboard-returns}

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

### Example {#espn_nfl_scoreboard-example}

```python
espn_nfl_scoreboard(dates='20240115')
```

_Last validated n/a._

## espn_nfl_summary

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary](https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns {#espn_nfl_summary-returns}

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

### Example {#espn_nfl_summary-example}

```python
espn_nfl_summary()
```

_Last validated n/a._

## espn_nfl_calendar

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/calendar](https://site.api.espn.com/apis/site/v2/sports/football/nfl/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_calendar-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_calendar-example}

```python
espn_nfl_calendar()
```

_Last validated n/a._

## espn_nfl_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/football/nfl/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_news-example}

```python
espn_nfl_news()
```

_Last validated n/a._

## espn_nfl_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries](https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_injuries-example}

```python
espn_nfl_injuries()
```

_Last validated n/a._

## espn_nfl_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/transactions?limit=500](https://site.api.espn.com/apis/site/v2/sports/football/nfl/transactions?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | character |  |
| `description` | character |  |
| `team_abbreviation` | character |  |
| `team_alternate_color` | character |  |
| `team_color` | character |  |
| `team_display_name` | character |  |
| `team_id` | character |  |
| `team_links` | character |  |
| `team_location` | character |  |
| `team_logos` | character |  |
| `team_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_transactions-example}

```python
espn_nfl_transactions()
```

_Last validated n/a._

## espn_nfl_conferences

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/groups](https://site.api.espn.com/apis/site/v2/sports/football/nfl/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | character |  |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `abbreviation` | character |  |
| `short_name` | character | Player short name (i.e. "F.Last") |
| `is_conference` | logical |  |
| `parent_group_id` | character |  |
| `depth` | integer |  |
| `children_count` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_conferences-example}

```python
espn_nfl_conferences()
```

_Last validated n/a._

## espn_nfl_statistics_league

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/statistics](https://site.api.espn.com/apis/site/v2/sports/football/nfl/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_statistics_league-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_statistics_league-example}

```python
espn_nfl_statistics_league()
```

_Last validated n/a._

## espn_nfl_draft

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/draft](https://site.api.espn.com/apis/site/v2/sports/football/nfl/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nfl_draft-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_draft-example}

```python
espn_nfl_draft()
```

_Last validated n/a._

## espn_nfl_teams_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams?limit=1000](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_teams_site-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | character | Secondary team color as a hex string (no leading '#'). |
| `team_color` | character | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | character | Full team display name (location + nickname). |
| `team_id` | character | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | logical | Whether the team is currently active. |
| `team_is_all_star` | logical | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | character | Team location / city (e.g. "Boston"). |
| `team_logos` | character | Pipe-delimited logo image URLs. |
| `team_name` | character | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | character | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | character | Abbreviated display name for compact UIs. |
| `team_slug` | character | URL slug used in ESPN web paths. |
| `team_uid` | character | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_teams_site-example}

```python
espn_nfl_teams_site()
```

_Last validated n/a._

## espn_nfl_team

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | character |  |
| `team_alternate_color` | character |  |
| `team_color` | character |  |
| `team_display_name` | character |  |
| `team_franchise_$ref` | character |  |
| `team_franchise_abbreviation` | character |  |
| `team_franchise_color` | character |  |
| `team_franchise_display_name` | character |  |
| `team_franchise_id` | character |  |
| `team_franchise_is_active` | logical |  |
| `team_franchise_location` | character |  |
| `team_franchise_name` | character |  |
| `team_franchise_short_display_name` | character |  |
| `team_franchise_slug` | character |  |
| `team_franchise_team_$ref` | character |  |
| `team_franchise_uid` | character |  |
| `team_franchise_venue_$ref` | character |  |
| `team_franchise_venue_address_city` | character |  |
| `team_franchise_venue_address_state` | character |  |
| `team_franchise_venue_full_name` | character |  |
| `team_franchise_venue_grass` | logical |  |
| `team_franchise_venue_guid` | character |  |
| `team_franchise_venue_id` | character |  |
| `team_franchise_venue_images` | character |  |
| `team_franchise_venue_indoor` | logical |  |
| `team_franchise_venue_short_name` | character |  |
| `team_groups_id` | character |  |
| `team_groups_is_conference` | logical |  |
| `team_groups_parent_id` | character |  |
| `team_id` | character |  |
| `team_is_active` | logical |  |
| `team_links` | character |  |
| `team_location` | character |  |
| `team_logos` | character |  |
| `team_name` | character |  |
| `team_next_event` | character |  |
| `team_record_items` | character |  |
| `team_short_display_name` | character |  |
| `team_slug` | character |  |
| `team_standing_summary` | character |  |
| `team_uid` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team-example}

```python
espn_nfl_team(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/roster?limit=500](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/roster?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | character | Postion group of player as listed by NFL |
| `id` | character | Id. |
| `uid` | character | Uid. |
| `guid` | character | Guid. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `full_name` | character | Full name. |
| `display_name` | character | Display name. |
| `short_name` | character | Short name. |
| `weight` | double | Weight. |
| `display_weight` | character | Display weight. |
| `height` | double | Height. |
| `display_height` | character | Display height. |
| `links` | character | Links. |
| `slug` | character | Slug. |
| `jersey` | character | Jersey. |
| `injuries` | character | Injuries. |
| `contracts` | character | Contracts. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `college_id` | character | College id. |
| `college_guid` | character | College guid. |
| `college_mascot` | character | College mascot. |
| `college_name` | character | College name. |
| `college_short_name` | character | College short name. |
| `college_abbrev` | character | College abbrev. |
| `college_logos` | character | College logos. |
| `headshot_href` | character | Headshot href. |
| `headshot_alt` | character | Headshot alt. |
| `position_id` | character | Position id. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation. |
| `position_leaf` | logical | Position leaf. |
| `position_parent_id` | character |  |
| `position_parent_name` | character |  |
| `position_parent_display_name` | character |  |
| `position_parent_abbreviation` | character |  |
| `position_parent_leaf` | logical |  |
| `experience_years` | integer | Experience years. |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `age` | double | Age. |
| `date_of_birth` | character | Date of birth. |
| `debut_year` | double | Debut year. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_roster-example}

```python
espn_nfl_team_roster(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_schedule

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_nfl_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric event identifier. |
| `date` | character | Event timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `time_valid` | logical | Whether the event time is confirmed. |
| `competitions` | character | Competition detail (list of dicts, stringified): competitors, venue, status. |
| `links` | character | Related links (list, stringified). |
| `season_year` | integer | Four-digit season year. |
| `season_display_name` | character | Human-readable season label (e.g. '2024-25'). |
| `season_type_id` | character | ESPN numeric identifier for the season type. |
| `season_type_type` | integer | Season type numeric code. |
| `season_type_name` | character | Season type name (e.g. Regular Season). |
| `season_type_abbreviation` | character | Season type abbreviation. |
| `week_number` | integer |  |
| `week_text` | character | Human-readable label for the week or scheduling block in which the event falls (e.g., 'Week 3', 'Bowl Week'), as returned by the ESPN schedule API. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_schedule-example}

```python
espn_nfl_team_schedule(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_record-example}

```python
espn_nfl_team_record(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_depthcharts

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_depthcharts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_depthcharts-example}

```python
espn_nfl_team_depthcharts(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_injuries-example}

```python
espn_nfl_team_injuries(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_transactions-example}

```python
espn_nfl_team_transactions(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_history

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_history-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_history-example}

```python
espn_nfl_team_history(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nfl_team_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_news-example}

```python
espn_nfl_team_news(team_id='4')
```

_Last validated n/a._

## espn_nfl_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nfl_team_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_team_leaders-example}

```python
espn_nfl_team_leaders(team_id='4')
```

_Last validated n/a._

## espn_nfl_player_info

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nfl_player_info-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_player_info-example}

```python
espn_nfl_player_info(athlete_id='4239')
```

_Last validated n/a._

## espn_nfl_player_bio

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nfl_player_bio-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_player_bio-example}

```python
espn_nfl_player_bio(athlete_id='4239')
```

_Last validated n/a._

## espn_nfl_player_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/football/nfl/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_nfl_player_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | integer | ESPN numeric identifier for the article. |
| `now_id` | character | ESPN 'now' feed id. |
| `content_key` | character | Internal content key. |
| `data_source_identifier` | character | Source-system identifier. |
| `type` | character | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | character | Article headline. |
| `description` | character | Article summary/description. |
| `last_modified` | character | Last-modified timestamp (ISO 8601). |
| `published` | character | Publish timestamp (ISO 8601). |
| `images` | character | Article images (list, stringified). |
| `categories` | character | Article categories (list, stringified). |
| `premium` | logical | Whether the article is premium/paywalled. |
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_player_news-example}

```python
espn_nfl_player_news(athlete_id='4239')
```

_Last validated n/a._

## espn_nfl_standings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/football/nfl/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/football/nfl/standings](https://site.api.espn.com/apis/v2/sports/football/nfl/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns {#espn_nfl_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | character | Group name. |
| `group_abbreviation` | character | Group abbreviation. |
| `team_id` | character | Team id. |
| `team_name` | character | Team name. |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_location` | character | Team location. |
| `team_logo` | character | Team logo. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `games_behind` | double | Games behind. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `ties` | double | Number of matches the team has drawn. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `division_losses` | double | Number of games the team has lost against opponents within their own division. |
| `division_record` | double | The team's composite win-loss-tie record against division opponents, serialized as a numeric value. |
| `division_ties` | double | Number of games the team has tied against opponents within their own division. |
| `division_wins` | double | Number of games the team has won against opponents within their own division. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nfl_standings-example}

```python
espn_nfl_standings()
```

_Last validated n/a._
