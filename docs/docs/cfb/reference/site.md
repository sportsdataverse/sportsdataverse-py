---
title: CFB — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# CFB — ESPN site API (v2)

`sportsdataverse.cfb` — 29 endpoints.

## `espn_cfb_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=20240115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character |  |
| `date` | character |  |
| `name` | character |  |
| `short_name` | character |  |
| `season_year` | integer |  |
| `season_type` | integer |  |
| `season_slug` | character |  |
| `status_type_id` | character |  |
| `status_type_name` | character |  |
| `status_type_state` | character |  |
| `status_type_completed` | logical |  |
| `status_type_description` | character |  |
| `status_type_detail` | character |  |
| `status_type_short_detail` | character |  |
| `status_clock` | double |  |
| `status_display_clock` | character |  |
| `status_period` | integer |  |
| `neutral_site` | logical |  |
| `conference_competition` | logical |  |
| `attendance` | integer |  |
| `venue_id` | character |  |
| `venue_full_name` | character |  |
| `venue_city` | character |  |
| `venue_state` | character |  |
| `venue_indoor` | logical |  |
| `broadcast` | character |  |
| `note` | character |  |
| `home_id` | character |  |
| `home_name` | character |  |
| `home_abbreviation` | character |  |
| `home_display_name` | character |  |
| `home_location` | character |  |
| `home_color` | character |  |
| `home_alternate_color` | character |  |
| `home_logo` | character |  |
| `home_score` | character |  |
| `home_winner` | logical |  |
| `home_rank` | integer |  |
| `away_id` | character |  |
| `away_name` | character |  |
| `away_abbreviation` | character |  |
| `away_display_name` | character |  |
| `away_location` | character |  |
| `away_color` | character |  |
| `away_alternate_color` | character |  |
| `away_logo` | character |  |
| `away_score` | character |  |
| `away_winner` | logical |  |
| `away_rank` | integer |  |

### Example

```python
espn_cfb_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_cfb_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary](https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns

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
| `completions/passing_attempts` | character |  |
| `passing_yards` | character |  |
| `yards_per_pass_attempt` | character |  |
| `passing_touchdowns` | character |  |
| `interceptions` | character |  |
| `adj_qbr` | character |  |
| `rushing_attempts` | character |  |
| `rushing_yards` | character |  |
| `yards_per_rush_attempt` | character |  |
| `rushing_touchdowns` | character |  |
| `long_rushing` | character |  |
| `receptions` | character |  |
| `receiving_yards` | character |  |
| `yards_per_reception` | character |  |
| `receiving_touchdowns` | character |  |
| `long_reception` | character |  |
| `fumbles` | character |  |
| `fumbles_lost` | character |  |
| `fumbles_recovered` | character |  |
| `total_tackles` | character |  |
| `solo_tackles` | character |  |
| `sacks` | character |  |
| `tackles_for_loss` | character |  |
| `passes_defended` | character |  |
| `hurries` | character |  |
| `defensive_touchdowns` | character |  |
| `kick_returns` | character |  |
| `kick_return_yards` | character |  |
| `yards_per_kick_return` | character |  |
| `long_kick_return` | character |  |
| `kick_return_touchdowns` | character |  |
| `punt_returns` | character |  |
| `punt_return_yards` | character |  |
| `yards_per_punt_return` | character |  |
| `long_punt_return` | character |  |
| `punt_return_touchdowns` | character |  |
| `field_goals_made/field_goal_attempts` | character |  |
| `field_goal_pct` | character |  |
| `long_field_goal_made` | character |  |
| `extra_points_made/extra_point_attempts` | character |  |
| `total_kicking_points` | character |  |
| `punts` | character |  |
| `punt_yards` | character |  |
| `gross_avg_punt_yards` | character |  |
| `touchbacks` | character |  |
| `punts_inside20` | character |  |
| `long_punt` | character |  |

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
| `venue_address_zip_code` | character |  |
| `venue_address_country` | character |  |
| `venue_grass` | logical | Venue grass. |

**header**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `uid` | character | Uid. |
| `time_valid` | logical | Time valid. |
| `competitions` | character | Competitions. |
| `links` | character | Links. |
| `week` | integer |  |
| `game_note` | character |  |
| `season_year` | integer | Season year. |
| `season_current` | logical | Season current. |
| `season_type` | integer | Season type. |
| `league_id` | character | League id. |
| `league_uid` | character | League uid. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | League abbreviation. |
| `league_midsize_name` | character |  |
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
| `overall` | character |  |
| `vs. conf.` | character |  |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character |  |
| `station_key` | character |  |
| `lang` | character |  |
| `region` | character |  |
| `is_national` | logical |  |
| `type_id` | character | Type id. |
| `type_short_name` | character |  |
| `type_long_name` | character |  |
| `type_slug` | character |  |
| `market_id` | character |  |
| `market_type` | character |  |
| `media_call_letters` | character |  |
| `media_name` | character |  |
| `media_short_name` | character |  |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | integer | Regulation periods. |
| `regulation_display_name` | character | Regulation display name. |
| `regulation_slug` | character | Regulation slug. |
| `regulation_clock` | double | Regulation clock. |
| `overtime_display_name` | character | Overtime display name. |
| `overtime_slug` | character | Overtime slug. |
| `sudden_death_periods` | integer |  |

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
| `links_web_self_href` | character |  |
| `links_web_self_dsi_href` | character |  |
| `links_api_self_href` | character | Links api self href. |
| `links_api_artwork_href` | character |  |
| `links_sportscenter_href` | character |  |
| `byline` | character | Byline. |
| `links_mobile_href` | character | Links mobile href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |

**drives**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `description` | character | Description. |
| `yards` | integer |  |
| `is_score` | logical |  |
| `offensive_plays` | integer |  |
| `result` | character |  |
| `short_display_result` | character |  |
| `display_result` | character |  |
| `plays` | character |  |
| `team_id` | character | Team id. |
| `team_name` | character |  |
| `team_abbreviation` | character | Team abbreviation. |
| `team_display_name` | character | Team display name. |
| `team_short_display_name` | character |  |
| `team_logos` | character | Team logos. |
| `start_period_type` | character |  |
| `start_period_number` | integer |  |
| `start_clock_display_value` | character |  |
| `start_yard_line` | integer |  |
| `start_text` | character |  |
| `end_period_type` | character |  |
| `end_period_number` | integer |  |
| `end_clock_display_value` | character |  |
| `end_yard_line` | integer |  |
| `end_text` | character |  |
| `time_elapsed_display_value` | character |  |

**drive_plays**

| col_name | type | description |
|---|---|---|
| `drive_id` | character |  |
| `drive_sequence` | integer |  |
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `priority` | logical |  |
| `modified` | character |  |
| `wallclock` | character | Wallclock. |
| `team_participants` | character |  |
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
| `end_yard_line` | integer |  |
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
| `media_id` | character |  |

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

### Example

```python
espn_cfb_summary()
```

_Last validated n/a._

## `espn_cfb_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_calendar()
```

_Last validated n/a._

## `espn_cfb_calendar_offseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/offseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/offseason](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/offseason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_calendar_offseason()
```

_Last validated n/a._

## `espn_cfb_calendar_regular_season`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/regular-season`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/regular-season](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/regular-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_calendar_regular_season()
```

_Last validated n/a._

## `espn_cfb_calendar_postseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/postseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/postseason](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/postseason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_calendar_postseason()
```

_Last validated n/a._

## `espn_cfb_calendar_ondays`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/ondays`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/ondays](https://site.api.espn.com/apis/site/v2/sports/football/college-football/calendar/ondays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_calendar_ondays()
```

_Last validated n/a._

## `espn_cfb_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_news()
```

_Last validated n/a._

## `espn_cfb_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries](https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_injuries()
```

_Last validated n/a._

## `espn_cfb_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions](https://site.api.espn.com/apis/site/v2/sports/football/college-football/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_transactions()
```

_Last validated n/a._

## `espn_cfb_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups](https://site.api.espn.com/apis/site/v2/sports/football/college-football/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_conferences()
```

_Last validated n/a._

## `espn_cfb_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics](https://site.api.espn.com/apis/site/v2/sports/football/college-football/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_statistics_league()
```

_Last validated n/a._

## `espn_cfb_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft](https://site.api.espn.com/apis/site/v2/sports/football/college-football/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_draft()
```

_Last validated n/a._

## `espn_cfb_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

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

### Example

```python
espn_cfb_teams_site()
```

_Last validated n/a._

## `espn_cfb_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `position_group` | character |  |
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
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_place_display_text` | character |  |
| `birth_country_alternate_id` | character |  |
| `birth_country_abbreviation` | character |  |
| `college_id` | character | College id. |
| `college_guid` | character | College guid. |
| `college_mascot` | character | College mascot. |
| `college_name` | character | College name. |
| `college_short_name` | character | College short name. |
| `college_abbrev` | character | College abbrev. |
| `college_logos` | character | College logos. |
| `headshot_href` | character | Headshot href. |
| `headshot_alt` | character | Headshot alt. |
| `flag_href` | character |  |
| `flag_alt` | character |  |
| `flag_rel` | character |  |
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
| `experience_display_value` | character |  |
| `experience_abbreviation` | character |  |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |

### Example

```python
espn_cfb_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_record(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_history(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_news(team_id='4')
```

_Last validated n/a._

## `espn_cfb_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_cfb_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/football/college-football/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_cfb_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/football/college-football/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/football/college-football/standings](https://site.api.espn.com/apis/v2/sports/football/college-football/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns

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
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `wins` | double | Wins. |
| `division_losses` | double |  |
| `division_ties` | double |  |
| `division_wins` | double |  |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `away` | character |  |
| `vs. conf.` | character | Vs. conf.. |
| `vs ap top 25` | character |  |
| `vs usa ranked teams` | character |  |
| `vs division` | double |  |

### Example

```python
espn_cfb_standings()
```

_Last validated n/a._

## `espn_cfb_rankings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings](https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_cfb_rankings()
```

_Last validated n/a._
