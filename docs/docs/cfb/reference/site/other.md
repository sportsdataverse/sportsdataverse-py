---
title: "College football — ESPN site API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 2
description: "College football — ESPN site API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# College football — ESPN site API (v2) — Other

## espn_cfb_scoreboard

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115&limit=500](https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN event id. |
| `uid` | String | ESPN global unique identifier. |
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
| `attendance` | Int64 | Reported attendance at the game. |
| `venue_id` | Int64 | Referencing venue id. |
| `venue_full_name` | String |  |
| `venue_city` | String | City where the venue is located. |
| `venue_state` | String | State (or province/country) where the venue is located. |
| `venue_indoor` | Boolean | Whether the home venue is indoors. |
| `broadcast` | String | Every network name of the competition in feed order, deduped and ", "-joined (an entry with no names gives its market); "" when the game lists no broadcast. |
| `note` | String | Event note text from the competition (e.g. a series or game label such as 'World Series - Game 1', or a shootout result); an empty string when there is none. |
| `home_id` | Int64 | Home team referencing id. |
| `home_name` | String |  |
| `home_abbreviation` | String |  |
| `home_display_name` | String |  |
| `home_location` | String |  |
| `home_color` | String |  |
| `home_alternate_color` | String |  |
| `home_logo` | String |  |
| `home_score` | String | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | Boolean |  |
| `home_rank` | Int64 |  |
| `away_id` | Int64 | Away team referencing id. |
| `away_name` | String |  |
| `away_abbreviation` | String |  |
| `away_display_name` | String |  |
| `away_location` | String |  |
| `away_color` | String |  |
| `away_alternate_color` | String |  |
| `away_logo` | String |  |
| `away_score` | String | Away team's score. For cricket, the innings string. |
| `away_winner` | Boolean |  |
| `away_rank` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_scoreboard-example}

```python
espn_cfb_scoreboard(dates='20240115')
```

## espn_cfb_summary

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary](https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns {#espn_cfb_summary-returns}

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
| `passing_yards` | String |  |
| `yards_per_pass_attempt` | String | Average passing yards gained per pass attempt by the player in the box score. |
| `passing_touchdowns` | String | Number of touchdown passes thrown by the player in the box score. |
| `interceptions` | String | Passing interceptions. |
| `adj_qbr` | String | ESPN's Adjusted Quarterback Rating for the player, measuring overall passing efficiency on a 0-100 scale. |
| `rushing_attempts` | String | Team rushing attempts. |
| `rushing_yards` | String | Team rushing yards. |
| `yards_per_rush_attempt` | String | Team yards per rush attempt. |
| `rushing_touchdowns` | String | Number of rushing touchdowns scored by the player in the box score. |
| `long_rushing` | String | Longest single rushing gain recorded by the player in the game. |
| `receptions` | String |  |
| `receiving_yards` | String |  |
| `yards_per_reception` | String | Average receiving yards gained per reception by the player in the box score. |
| `receiving_touchdowns` | String | Number of receiving touchdowns scored by the player in the box score. |
| `long_reception` | String | Longest single reception recorded by the player in the game. |
| `fumbles` | String | Number of fumbles committed by the player in the box score. |
| `fumbles_lost` | String | Fumbles lost. |
| `fumbles_recovered` | String | Team fumbles recovered. |
| `total_tackles` | String | Total tackles recorded by the player, including both solo and assisted tackles. |
| `solo_tackles` | String | Number of unassisted tackles recorded by the player in the box score. |
| `sacks` | String | Team sacks. |
| `tackles_for_loss` | String | Team tackles for a loss. |
| `passes_defended` | String | Number of pass plays disrupted or broken up by the defensive player in the game. |
| `hurries` | String | Number of times the player pressured the opposing passer into an early or errant throw. |
| `defensive_touchdowns` | String | Number of touchdowns scored by the player via defensive plays such as interception or fumble returns. |
| `kick_returns` | String | Number of kick returns. |
| `kick_return_yards` | String | Team kick return yards. |
| `yards_per_kick_return` | String | Average yards gained per kick return by the player in the box score. |
| `long_kick_return` | String | Longest single kick return yardage recorded by the player in the game. |
| `kick_return_touchdowns` | String | Number of touchdowns scored on kick returns by the player in the box score. |
| `punt_returns` | String | Number of punt returns. |
| `punt_return_yards` | String | Team punt return yards. |
| `yards_per_punt_return` | String | Average yards gained per punt return by the player in the box score. |
| `long_punt_return` | String | Longest single punt return yardage recorded by the player in the game. |
| `punt_return_touchdowns` | String | Number of touchdowns scored on punt returns by the player in the box score. |
| `field_goals_made/field_goal_attempts` | String | Field goal conversion ratio for the player, expressed as field goals made divided by attempts. |
| `field_goal_pct` | String |  |
| `long_field_goal_made` | String | Distance of the longest successful field goal kicked by the player in the game. |
| `extra_points_made/extra_point_attempts` | String | Extra point conversion ratio for the player, expressed as extra points made divided by attempts. |
| `total_kicking_points` | String | Total points contributed by the player through field goals and extra points in the game. |
| `punts` | String | Total number of punts executed by the player in the box score. |
| `punt_yards` | String | Total yardage of all punts executed by the player in the game. |
| `gross_avg_punt_yards` | String | Average gross punt distance before accounting for returns, recorded for the player in the game. |
| `touchbacks` | String | Number of punts or kick-offs by the player that resulted in the opposing team starting from their own end zone. |
| `punts_inside20` | String | Number of punts by the player that were downed or stopped inside the opposing team's 20-yard line. |
| `long_punt` | String | Distance of the longest individual punt executed by the player in the game. |

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

**header**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `uid` | String | Uid. |
| `time_valid` | Boolean | Time valid. |
| `competitions` | String | Competitions. |
| `links` | String | Links. |
| `week` | Int64 | Game week of the season. |
| `game_note` | String | Optional editorial note or context annotation attached to the game in the header. |
| `season_year` | Int64 | Season year. |
| `season_current` | Boolean | Season current. |
| `season_type` | Int64 | Season type. |
| `league_id` | Int64 | League id. |
| `league_uid` | String | League uid. |
| `league_name` | String | League name. |
| `league_abbreviation` | String | League abbreviation. |
| `league_midsize_name` | String | Medium-length display name for the league or competition as shown in the game header. |
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
| `overall` | String | Overall draft pick number. |
| `vs. conf.` | String | Team's record against conference opponents, shown as part of the standings snapshot in the box score. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | String | Broadcast station / network name (e.g. `ESPN+`). |
| `station_key` | String | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | String | Broadcast language code. |
| `region` | String | Broadcast region code. |
| `is_national` | Boolean | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | Int64 | Type id. |
| `type_short_name` | String |  |
| `type_long_name` | String |  |
| `type_slug` | String | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | Int64 | ESPN futures-market identifier. |
| `market_type` | String | Geographic market type (e.g. `National`). |
| `media_call_letters` | String | Broadcast call letters for the outlet. |
| `media_name` | String | ESPN media name for the outlet. |
| `media_short_name` | String | Short ESPN media name for the outlet. |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | Int64 | Regulation periods. |
| `regulation_display_name` | String | Regulation display name. |
| `regulation_slug` | String | Regulation slug. |
| `regulation_clock` | Float64 | Regulation clock. |
| `overtime_display_name` | String | Overtime display name. |
| `overtime_slug` | String | Overtime slug. |
| `sudden_death_periods` | Int64 | Number of sudden-death overtime periods defined in the game format rules. |

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
| `links_web_href` | String | Links web href. |
| `links_web_self_href` | String | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | String | Data-source-identified URL for the web page of the associated article content. |
| `links_api_self_href` | String | Links api self href. |
| `links_api_artwork_href` | String | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | String | URL for the article's page on ESPN's SportsCenter platform. |
| `byline` | String | Byline. |
| `links_mobile_href` | String | Links mobile href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `description` | String | Description. |
| `yards` | Int64 | Total yards gained on the drive. |
| `is_score` | Boolean | `TRUE` if the drive resulted in a score. |
| `offensive_plays` | Int64 | Number of offensive plays on the drive. |
| `result` | String | Drive result code (e.g. `PUNT`, `TD`). |
| `short_display_result` | String | Short drive-result label. |
| `display_result` | String | Drive-result label (e.g. `Punt`, `Touchdown`). |
| `plays` | String | Total qualifying passing plays included in the WEPA calculation. |
| `team_id` | Int64 | Team id. |
| `team_name` | String | Full display name of the team. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_short_display_name` | String | Short team display name. |
| `team_logos` | String | Team logos. |
| `start_period_type` | String | Period type at the start of the drive (e.g. `quarter`). |
| `start_period_number` | Int64 | Period or quarter number in which the drive or sequence began. |
| `start_clock_display_value` | String | Game clock time displayed at the start of the drive or scoring sequence. |
| `start_yard_line` | Int64 | Yard line at the start of the play. |
| `start_text` | String | Field-position text at the start of the drive. |
| `end_period_type` | String | Period type at the end of the drive (e.g. `quarter`). |
| `end_period_number` | Int64 | Period or quarter number in which the drive or sequence ended. |
| `end_clock_display_value` | String | Game clock time displayed at the end of the drive or scoring sequence. |
| `end_yard_line` | Int64 | Yard line at the end of the play. |
| `end_text` | String | Field-position text at the end of the drive. |
| `time_elapsed_display_value` | String | Human-readable duration of time elapsed during the drive or scoring sequence. |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | Int64 | CFBD drive identifier the play belongs to. |
| `drive_sequence` | Int64 | Sequential position of the drive within the game's broadcast or play-by-play listing. |
| `id` | Int64 | Id. |
| `sequence_number` | String | Sequence number. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `scoring_play` | Boolean | Scoring play. |
| `priority` | Boolean | `TRUE` if ESPN flags the play as a priority highlight. |
| `modified` | String | ISO timestamp the play record was last modified. |
| `wallclock` | String | Wallclock. |
| `team_participants` | String | Teams or participants associated with a specific drive or sequence in the broadcast record. |
| `is_penalty` | Boolean | `TRUE` if the play was a penalty. |
| `stat_yardage` | Int64 | Yards gained or lost on the play. |
| `is_turnover` | Boolean | `TRUE` if the play was a turnover. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_abbreviation` | String | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | Int64 | Period number. |
| `clock_display_value` | String | Clock display value. |
| `start_down` | Int64 | Down at the start of the play. |
| `start_distance` | Int64 | Yards to go at the start of the play. |
| `start_yard_line` | Int64 | Yard line at the start of the play. |
| `start_yards_to_endzone` | Int64 | Yards to the end zone at the start of the play. |
| `start_team_id` | Int64 | ESPN team id in possession at the start of the play. |
| `end_down` | Int64 | Down at the end of the play. |
| `end_distance` | Int64 | Yards to go at the end of the play. |
| `end_yard_line` | Int64 | Yard line at the end of the play. |
| `end_yards_to_endzone` | Int64 | Yards to the end zone at the end of the play. |
| `end_down_distance_text` | String | Down-and-distance text at the end of the play. |
| `end_short_down_distance_text` | String | Short down-and-distance text at the end of the play. |
| `end_possession_text` | String | Field-position text at the end of the play. |
| `end_team_id` | Int64 | ESPN team id in possession at the end of the play. |
| `start_down_distance_text` | String | Down-and-distance text at the start of the play. |
| `start_short_down_distance_text` | String | Short down-and-distance text at the start of the play. |
| `start_possession_text` | String | Field-position text at the start of the play. |
| `scoring_type_name` | String | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | String | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | String | Scoring-type abbreviation (e.g. `TD`, `FG`). |
| `point_after_attempt_id` | Int64 | Point-after-attempt id on a scoring play. |
| `point_after_attempt_text` | String | Point-after-attempt text (e.g. `Extra Point Good`). |
| `point_after_attempt_abbreviation` | String | Point-after-attempt abbreviation. |
| `point_after_attempt_value` | Float64 | Points added by the point-after attempt. |
| `media_id` | Int64 | ESPN media id for the outlet. |

**scoring_plays**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_abbreviation` | String | Play-type abbreviation (e.g. `RUSH`, `TD`). |
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
| `scoring_type_name` | String | Scoring-type key on a scoring play (e.g. `touchdown`). |
| `scoring_type_display_name` | String | Human-readable scoring-type name. |
| `scoring_type_abbreviation` | String | Scoring-type abbreviation (e.g. `TD`, `FG`). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_summary-example}

```python
espn_cfb_summary()
```

## espn_cfb_calendar_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_calendar_site-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_calendar_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_calendar_site-example}

```python
espn_cfb_calendar_site()
```

## espn_cfb_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/football/college-football/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the article. |
| `now_id` | String | ESPN 'now' feed id. |
| `content_key` | String | Internal content key. |
| `data_source_identifier` | String | Source-system identifier. |
| `type` | String | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | String | Article headline. |
| `description` | String | Article summary/description. |
| `last_modified` | String | Last-modified timestamp (ISO 8601). |
| `published` | String | Publish timestamp (ISO 8601). |
| `images` | String | Article images (list, stringified). |
| `categories` | String | Article categories (list, stringified). |
| `premium` | Boolean | Whether the article is premium/paywalled. |
| `byline` | String | Author byline string as published by ESPN. |
| `links_web_href` | String | Web article URL. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |
| `links_web_self_href` | String | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | String | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | String | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | String | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_news-example}

```python
espn_cfb_news()
```

## espn_cfb_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries](https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_injuries-example}

```python
espn_cfb_injuries()
```

## espn_cfb_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions?limit=500](https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_cfb_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Date of the poll release. |
| `description` | String | ESPN's description of the stat. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_alternate_color` | String | Alternate team color. |
| `team_color` | String | Primary team color. |
| `team_display_name` | String | Full team display name. |
| `team_id` | Int64 | ESPN team id. |
| `team_links` | String |  |
| `team_location` | String | Team location / school name. |
| `team_logos` | String |  |
| `team_name` | String | Team nickname. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_transactions-example}

```python
espn_cfb_transactions()
```

## espn_cfb_conferences

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups](https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | Int64 | ESPN group (conference) id for the season. |
| `name` | String | Position name (e.g. `Quarterback`). |
| `abbreviation` | String | Metric abbreviation. |
| `short_name` | String | Ranking source short name (e.g. `AP Poll`). |
| `is_conference` | Boolean | `TRUE` for an actual conference, `FALSE` for a division roll-up. |
| `parent_group_id` | Int64 | `group_id` of the parent node (`NA` at the root). |
| `depth` | Int64 |  |
| `children_count` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_conferences-example}

```python
espn_cfb_conferences()
```

## espn_cfb_statistics_league

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics](https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_statistics_league-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: league, season, status, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_statistics_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_statistics_league-example}

```python
espn_cfb_statistics_league()
```

## espn_cfb_draft

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft](https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_draft-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, nhl, wnba, 2026-10-07); its rows sit under keys it does not read (top level: breakingNews, broadcasts, displayName, picks, positions, rounds, shortDisplayName, status).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_draft-example}

```python
espn_cfb_draft()
```

## espn_cfb_player_info

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_info-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_player_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_player_info-example}

```python
espn_cfb_player_info(athlete_id='4239')
```

## espn_cfb_player_bio

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_bio-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_player_bio-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_player_bio-example}

```python
espn_cfb_player_bio(athlete_id='4239')
```

## espn_cfb_player_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_cfb_player_news-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the article. |
| `now_id` | String | ESPN 'now' feed id. |
| `content_key` | String | Internal content key. |
| `data_source_identifier` | String | Source-system identifier. |
| `type` | String | Article type (Story, Media, HeadlineNews, etc.). |
| `headline` | String | Article headline. |
| `description` | String | Article summary/description. |
| `last_modified` | String | Last-modified timestamp (ISO 8601). |
| `published` | String | Publish timestamp (ISO 8601). |
| `images` | String | Article images (list, stringified). |
| `categories` | String | Article categories (list, stringified). |
| `premium` | Boolean | Whether the article is premium/paywalled. |
| `byline` | String | Author byline string as published by ESPN. |
| `links_web_href` | String | Web article URL. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |
| `links_web_self_href` | String | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | String | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | String | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | String | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_player_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_player_news-example}

```python
espn_cfb_player_news(athlete_id='4239')
```

## espn_cfb_standings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/football/college-football/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/football/college-football/standings](https://site.api.espn.com/apis/v2/sports/football/college-football/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns {#espn_cfb_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_name` | String | Group name. |
| `group_abbreviation` | String | Group abbreviation. |
| `team_id` | Int64 | Team id. |
| `team_name` | String | Team name. |
| `team_abbreviation` | String | Team abbreviation. |
| `team_display_name` | String | Team display name. |
| `team_location` | String | Team location. |
| `team_logo` | String | Team logo. |
| `games_behind` | Float64 | Games behind. |
| `league_win_percent` | Float64 | League win percent. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `wins` | Float64 | Wins. |
| `division_losses` | Float64 | Number of games the team has lost against opponents within their own division. |
| `division_ties` | Float64 | Number of games the team has tied against opponents within their own division. |
| `division_wins` | Float64 | Number of games the team has won against opponents within their own division. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `away` | String | Away team name. |
| `vs. conf.` | String | Vs. conf.. |
| `vs ap top 25` | String | The team's win-loss record against opponents ranked in the AP Top 25 poll. |
| `vs usa ranked teams` | String | The team's win-loss record against opponents ranked in the USA Today Coaches Poll. |
| `vs division` | Float64 | The team's win-loss(-tie) record against opponents within their own division, serialized as a numeric value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_standings-example}

```python
espn_cfb_standings()
```

## espn_cfb_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings](https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_cfb_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `poll_id` | Int64 | ESPN poll id, e.g. 1 = AP Top 25, 2 = Coaches Poll, 20 = FCS Coaches Poll, 10624 = USCHO Men's Poll. |
| `poll_name` | String | Full poll name, e.g. 'AP Top 25', 'AFCA Coaches Poll', "USCHO Women's Poll". |
| `poll_short_name` | String | Short poll label, e.g. 'AP Poll'. |
| `poll_type` | String | ESPN poll type code, e.g. 'ap', 'usa' (coaches), 'fcs', 'USCHOMENSPOLL'. |
| `season` | Int64 | Season year of the poll (ESPN's ending year for a season that spans two calendar years, e.g. 2026 for 2025-26). |
| `season_type` | Int64 | Season phase of the poll: 1 = preseason, 2 = regular season, 3 = postseason. |
| `week` | Int64 | Poll week within season_type (the week ESPN's Core v2 rankings URL uses). |
| `week_display` | String | Poll week as ESPN labels it, e.g. 'Week 6'. |
| `poll_date` | String | Date the poll was released (ISO 8601, UTC). |
| `ranked` | Boolean | TRUE for the poll's ranked teams; FALSE for teams that only received votes. |
| `team_id` | Int64 | ESPN team id (Int64, like scoreboard home_id / away_id). |
| `rank` | Int64 | Position in the poll (1 = top). Null on vote-receiving rows. |
| `previous_rank` | Int64 | Position in the previous poll; 0 when the team was unranked then. |
| `points` | Float64 | Poll points received (0 for polls ESPN ships without points, such as USCHO). |
| `first_place_votes` | Int64 | First-place votes received. Null when the poll does not report them. |
| `trend` | String | Movement since the previous poll as ESPN prints it, e.g. '+3', '-2', or '-' for no change. |
| `record_summary` | String | Team's win-loss record at the poll date, e.g. '5-0'. |
| `team_uid` | String | ESPN universal team id, e.g. 's:20~l:23~t:251'. |
| `team_location` | String | Team location (school name), e.g. 'Texas'. |
| `team_name` | String | Team mascot name, e.g. 'Longhorns'. |
| `team_nickname` | String | Short team name ESPN displays, e.g. 'Texas'. |
| `team_abbreviation` | String | Short team code ESPN displays, e.g. 'TEX'. |
| `team_color` | String | Team primary color as a hex string without '#'. Null for teams ESPN ships without one. |
| `team_logo` | String | URL of the team logo on ESPN's CDN. |
| `last_updated` | String | When ESPN last updated this poll entry (ISO 8601, UTC). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_cfb_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_cfb_rankings-example}

```python
espn_cfb_rankings()
```
