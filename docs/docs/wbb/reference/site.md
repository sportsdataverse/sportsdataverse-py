---
title: WBB — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
---
# WBB — ESPN site API (v2)

`sportsdataverse.wbb` — 29 endpoints.

## `espn_wbb_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/scoreboard?dates=20240115)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `dates` | `dates` |  |  | `Y` |
| `week` | `week` |  |  | `Y` |
| `seasontype` | `season_type` |  |  | `Y` |
| `groups` | `groups` |  |  | `Y` |
| `limit` | `limit` |  |  | `Y` |

### Returns

| col_name | type | description |
|---|---|---|
| `game_id` | integer | ESPN event id. |
| `season` | integer | Four-digit season year. |
| `game_date` | character | ISO 8601 kickoff timestamp (UTC). |

### Example

```python
espn_wbb_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_wbb_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/summary)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `event` | `event_id` |  |  | `Y` |

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
| `starter` | logical | Starter. |
| `active` | logical | Active. |
| `did_not_play` | logical | Did not play. |
| `ejected` | logical | Ejected. |
| `reason` | character | Reason. |
| `minutes` | character | Minutes. |
| `points` | character | Points. |
| `field_goals_made_field_goals_attempted` | character | Field goals made field goals attempted. |
| `three_point_field_goals_made_three_point_field_goals_attempted` | character | Three point field goals made three point field goals attempted. |
| `free_throws_made_free_throws_attempted` | character | Free throws made free throws attempted. |
| `rebounds` | character | Rebounds. |
| `assists` | character | Assists. |
| `turnovers` | character | Turnovers. |
| `steals` | character | Steals. |
| `blocks` | character | Blocks. |
| `offensive_rebounds` | character | Offensive rebounds. |
| `defensive_rebounds` | character | Defensive rebounds. |
| `fouls` | character | Fouls. |
| `plus_minus` | character | Plus minus. |

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

**plays**

| col_name | type | description |
|---|---|---|
| `id` | character | Id. |
| `sequence_number` | character | Sequence number. |
| `text` | character | Text. |
| `away_score` | integer | Away score. |
| `home_score` | integer | Home score. |
| `scoring_play` | logical | Scoring play. |
| `score_value` | integer | Score value. |
| `participants` | character | Participants. |
| `wallclock` | character | Wallclock. |
| `shooting_play` | logical | Shooting play. |
| `points_attempted` | integer | Points attempted. |
| `short_description` | character | Short description. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `clock_display_value` | character | Clock display value. |
| `team_id` | character | Team id. |
| `coordinate_x` | integer | Coordinate x. |
| `coordinate_y` | integer | Coordinate y. |

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
| `venue_short_name` | character | Venue short name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
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

**season_series**

| col_name | type | description |
|---|---|---|
| `type` | character | Type. |
| `title` | character | Title. |
| `description` | character | Description. |
| `summary` | character | Summary. |
| `completed` | logical | Completed. |
| `total_competitions` | integer | Total competitions. |
| `series_label` | character | Series label. |
| `series_score` | character | Series score. |
| `short_summary` | character | Short summary. |
| `events` | character | Events. |

**against_the_spread**

| col_name | type | description |
|---|---|---|

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `games_behind` | character | Games behind. |
| `losses` | character | Losses. |
| `streak` | character | Streak. |
| `win_percent` | character | Win percent. |
| `wins` | character | Wins. |

**broadcasts**

| col_name | type | description |
|---|---|---|

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

**pickcenter**

| col_name | type | description |
|---|---|---|

**odds**

| col_name | type | description |
|---|---|---|

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

**drive_plays**

| col_name | type | description |
|---|---|---|

**scoring_plays**

| col_name | type | description |
|---|---|---|

### Example

```python
espn_wbb_summary()
```

_Last validated n/a._

## `espn_wbb_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_calendar()
```

_Last validated n/a._

## `espn_wbb_calendar_offseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/offseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/offseason](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/offseason)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_calendar_offseason()
```

_Last validated n/a._

## `espn_wbb_calendar_regular_season`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/regular-season`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/regular-season](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/regular-season)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_calendar_regular_season()
```

_Last validated n/a._

## `espn_wbb_calendar_postseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/postseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/postseason](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/postseason)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_calendar_postseason()
```

_Last validated n/a._

## `espn_wbb_calendar_ondays`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/ondays`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/ondays](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/calendar/ondays)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_calendar_ondays()
```

_Last validated n/a._

## `espn_wbb_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/news](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/news)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_news()
```

_Last validated n/a._

## `espn_wbb_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/injuries](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/injuries)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_injuries()
```

_Last validated n/a._

## `espn_wbb_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/transactions](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/transactions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_transactions()
```

_Last validated n/a._

## `espn_wbb_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/groups](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/groups)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_conferences()
```

_Last validated n/a._

## `espn_wbb_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/statistics](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/statistics)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_statistics_league()
```

_Last validated n/a._

## `espn_wbb_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/draft](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/draft)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_draft()
```

_Last validated n/a._

## `espn_wbb_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

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
espn_wbb_teams_site()
```

_Last validated n/a._

## `espn_wbb_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

| col_name | type | description |
|---|---|---|
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
| `age` | integer | Age. |
| `date_of_birth` | character | Date of birth. |
| `debut_year` | double | Debut year. |
| `links` | character | Links. |
| `slug` | character | Slug. |
| `jersey` | character | Jersey. |
| `injuries` | character | Injuries. |
| `teams` | character | Teams. |
| `contracts` | character | Contracts. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
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
| `experience_years` | integer | Experience years. |
| `contract_bird_status` | integer | Contract bird status. |
| `contract_base_year_compensation_active` | logical | Contract base year compensation active. |
| `contract_poison_pill_provision_active` | logical | Contract poison pill provision active. |
| `contract_incoming_trade_value` | integer | Contract incoming trade value. |
| `contract_outgoing_trade_value` | integer | Contract outgoing trade value. |
| `contract_minimum_salary_exception` | logical | Contract minimum salary exception. |
| `contract_option_type` | integer | Contract option type. |
| `contract_salary` | integer | Contract salary. |
| `contract_salary_remaining` | integer | Contract salary remaining. |
| `contract_years_remaining` | integer | Contract years remaining. |
| `contract_season_year` | integer | Contract season year. |
| `contract_season_start_date` | character | Contract season start date. |
| `contract_season_end_date` | character | Contract season end date. |
| `contract_trade_kicker_active` | logical | Contract trade kicker active. |
| `contract_trade_kicker_percentage` | double | Contract trade kicker percentage. |
| `contract_trade_kicker_value` | integer | Contract trade kicker value. |
| `contract_trade_kicker_trade_value` | integer | Contract trade kicker trade value. |
| `contract_trade_restriction` | logical | Contract trade restriction. |
| `contract_unsigned_foreign_pick` | logical | Contract unsigned foreign pick. |
| `contract_active` | logical | Contract active. |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `citizenship` | character | Citizenship. |
| `birth_place_state` | character | Birth place state. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |

### Example

```python
espn_wbb_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_record(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_history(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/news)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_news(team_id='4')
```

_Last validated n/a._

## `espn_wbb_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_wbb_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_wbb_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_wbb_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_wbb_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/basketball/womens-college-basketball/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/basketball/womens-college-basketball/standings](https://site.api.espn.com/apis/v2/sports/basketball/womens-college-basketball/standings)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `season` | `season` |  |  | `Y` |
| `group` | `group` |  |  | `Y` |
| `type` | `standings_type` |  |  | `Y` |

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
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `games_ahead` | double | Games ahead. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |
| `last ten games` | character | Last ten games. |

### Example

```python
espn_wbb_standings()
```

_Last validated n/a._

## `espn_wbb_rankings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/rankings`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/rankings](https://site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/rankings)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_wbb_rankings()
```

_Last validated n/a._
