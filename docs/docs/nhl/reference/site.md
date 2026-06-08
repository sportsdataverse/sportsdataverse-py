---
title: NHL — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# NHL — ESPN site API (v2)

`sportsdataverse.nhl` — 28 endpoints.

## `espn_nhl_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard?dates=20240115)

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
| `uid` | character | ESPN UID string. |
| `date` | character | Date in YYYY-MM-DD format. |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | Season type (1=pre-season, 2=regular season, 3=postseason, 4=off-season for ESPN; or string label for WNBA Stats). |
| `season_slug` | character | Season slug. |
| `status_type_id` | character | Unique identifier for status type. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status type state. |
| `status_type_completed` | logical | Status type completed. |
| `status_type_description` | character | Status type description. |
| `status_type_detail` | character | Status type detail. |
| `status_type_short_detail` | character | Status type short detail. |
| `status_clock` | double | Status clock. |
| `status_display_clock` | character | Status display clock. |
| `status_period` | integer | Status period. |
| `neutral_site` | logical | Neutral site. |
| `conference_competition` | character | Conference competition. |
| `attendance` | integer | Reported attendance. |
| `venue_id` | character | Unique venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / region. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast information string. |
| `note` | character |  |
| `home_id` | character | Unique identifier for home. |
| `home_name` | character | Home name. |
| `home_abbreviation` | character | Home team's abbreviation. |
| `home_display_name` | character | Home display name. |
| `home_location` | character | Home team's location. |
| `home_color` | character | Color code (hex) for home. |
| `home_alternate_color` | character | Color code (hex) for home alternate. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team score at the time of the play. |
| `home_winner` | logical | Home team's winner. |
| `home_rank` | integer |  |
| `away_id` | character | Unique identifier for away. |
| `away_name` | character | Away name. |
| `away_abbreviation` | character | Away team's abbreviation. |
| `away_display_name` | character | Away display name. |
| `away_location` | character | Away team's location. |
| `away_color` | character | Color code (hex) for away. |
| `away_alternate_color` | character | Color code (hex) for away alternate. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team score at the time of the play. |
| `away_winner` | logical | Away team's winner. |
| `away_rank` | integer |  |

### Example

```python
espn_nhl_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_nhl_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary)

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
| `blocked_shots` | character |  |
| `hits` | character | Hits. |
| `takeaways` | character |  |
| `plus_minus` | character | Plus minus. |
| `time_on_ice` | character |  |
| `power_play_time_on_ice` | character |  |
| `short_handed_time_on_ice` | character |  |
| `even_strength_time_on_ice` | character |  |
| `shifts` | character |  |
| `goals` | character |  |
| `ytd_goals` | character |  |
| `assists` | character | Assists. |
| `shots_total` | character |  |
| `shots_missed` | character |  |
| `shootout_goals` | character |  |
| `faceoffs_won` | character |  |
| `faceoffs_lost` | character |  |
| `faceoff_percent` | character |  |
| `giveaways` | character |  |
| `penalties` | character | Total number of penalties. |
| `penalty_minutes` | character |  |
| `goals_against` | character |  |
| `shots_against` | character |  |
| `shootout_saves` | character |  |
| `shootout_shots_against` | character |  |
| `saves` | character | Saves. |
| `save_pct` | character |  |
| `even_strength_saves` | character |  |
| `power_play_saves` | character |  |
| `short_handed_saves` | character |  |

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
| `modified` | character | ISO timestamp the play record was last modified. |
| `wallclock` | character | Wallclock. |
| `shooting_play` | logical | Shooting play. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_abbreviation` | character | Play-type abbreviation (e.g. `RUSH`, `TD`). |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `clock_display_value` | character | Clock display value. |
| `participants` | character | Participants. |
| `team_id` | character | Team id. |
| `strength_id` | character |  |
| `strength_text` | character |  |
| `strength_abbreviation` | character |  |
| `coordinate_x` | double | Coordinate x. |
| `coordinate_y` | double | Coordinate y. |
| `shot_info_id` | character |  |
| `shot_info_text` | character |  |
| `shot_info_abbreviation` | character |  |

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
| `venue_address_country` | character |  |
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
| `game_note` | character |  |
| `standings` | character |  |
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
| `round` | character | Tournament / playoff round. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `ot_losses` | character |  |
| `losses` | character | Losses. |
| `points` | character | Points. |
| `wins` | character | Wins. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Broadcast station / network name (e.g. `ESPN+`). |
| `lang` | character | Broadcast language code. |
| `region` | character | Region label. |
| `is_national` | logical |  |
| `type_id` | character | Type id. |
| `type_short_name` | character | Type short name. |
| `type_long_name` | character | Type long name. |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
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

### Example

```python
espn_nhl_summary()
```

_Last validated n/a._

## `espn_nhl_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_calendar()
```

_Last validated n/a._

## `espn_nhl_calendar_offseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/offseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/offseason](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/offseason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_calendar_offseason()
```

_Last validated n/a._

## `espn_nhl_calendar_regular_season`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/regular-season`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/regular-season](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/regular-season)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_calendar_regular_season()
```

_Last validated n/a._

## `espn_nhl_calendar_postseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/postseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/postseason](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/postseason)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_calendar_postseason()
```

_Last validated n/a._

## `espn_nhl_calendar_ondays`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/ondays`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/ondays](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/calendar/ondays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_calendar_ondays()
```

_Last validated n/a._

## `espn_nhl_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/news](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_news()
```

_Last validated n/a._

## `espn_nhl_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_injuries()
```

_Last validated n/a._

## `espn_nhl_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/transactions](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_transactions()
```

_Last validated n/a._

## `espn_nhl_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/groups](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_conferences()
```

_Last validated n/a._

## `espn_nhl_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/statistics](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_statistics_league()
```

_Last validated n/a._

## `espn_nhl_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/draft](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_draft()
```

_Last validated n/a._

## `espn_nhl_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams)

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
espn_nhl_teams_site()
```

_Last validated n/a._

## `espn_nhl_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `id` | character | Id. |
| `uid` | character | Uid. |
| `guid` | character | Guid. |
| `alternate_id` | character |  |
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
| `links` | character | Links. |
| `slug` | character | Slug. |
| `jersey` | character | Jersey. |
| `injuries` | character | Injuries. |
| `teams` | character | Teams. |
| `contracts` | character | Contracts. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_country` | character | Birth place country. |
| `birth_place_display_text` | character |  |
| `birth_country_abbreviation` | character |  |
| `headshot_href` | character | Headshot href. |
| `headshot_alt` | character | Headshot alt. |
| `hand_type` | character | Hand type. |
| `hand_abbreviation` | character | Hand abbreviation. |
| `hand_display_value` | character | Hand display value. |
| `position_id` | character | Position id. |
| `position_name` | character | Position name. |
| `position_display_name` | character | Position display name. |
| `position_abbreviation` | character | Position abbreviation. |
| `position_leaf` | logical | Position leaf. |
| `position_parent_id` | character | ESPN id of the parent position; `position_detail = TRUE` only. |
| `position_parent_name` | character |  |
| `position_parent_display_name` | character |  |
| `position_parent_abbreviation` | character |  |
| `position_parent_leaf` | logical |  |
| `experience_years` | integer | Experience years. |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `debut_year` | double | Debut year. |
| `college_id` | character | College id. |
| `college_guid` | character | College guid. |
| `college_mascot` | character | College mascot. |
| `college_name` | character | College name. |
| `college_short_name` | character | College short name. |
| `college_abbrev` | character | College abbrev. |
| `college_logos` | character | College logos. |
| `birth_place_state` | character | Birth place state. |

### Example

```python
espn_nhl_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_record(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_history(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_news(team_id='4')
```

_Last validated n/a._

## `espn_nhl_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_nhl_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_nhl_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_nhl_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_nhl_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_nhl_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings](https://site.api.espn.com/apis/v2/sports/hockey/nhl/standings)

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
| `ot_losses` | double |  |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `games_behind` | double | Games behind. |
| `games_played` | double | Games played. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `wins` | double | Wins. |
| `overtime_losses` | double |  |
| `overtime_wins` | double |  |
| `points_diff` | double |  |
| `reg_losses` | double |  |
| `reg_wins` | double |  |
| `rot_losses` | double |  |
| `rot_wins` | double |  |
| `shootout_losses` | double |  |
| `shootout_wins` | double |  |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `last ten games` | character | Last ten games. |
| `vs. div.` | character | Vs. div.. |

### Example

```python
espn_nhl_standings()
```

_Last validated n/a._
