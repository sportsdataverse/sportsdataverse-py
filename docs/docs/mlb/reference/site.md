---
title: MLB — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# MLB — ESPN site API (v2)

`sportsdataverse.mlb` — 28 endpoints.

## `espn_mlb_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=20240115)

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
| `home_rank` | character |  |
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
| `away_rank` | character |  |

### Example

```python
espn_mlb_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_mlb_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary)

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
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `hits_at_bats` | character |  |
| `at_bats` | character |  |
| `runs` | character |  |
| `hits` | character |  |
| `rb_is` | character |  |
| `home_runs` | character |  |
| `walks` | character |  |
| `strikeouts` | character |  |
| `pitches` | character |  |
| `avg` | character |  |
| `on_base_pct` | character |  |
| `slug_avg` | character |  |
| `full_innings.part_innings` | character |  |
| `earned_runs` | character |  |
| `pitches_strikes` | character |  |
| `era` | character |  |

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
| `wallclock` | character | Wallclock. |
| `at_bat_id` | character |  |
| `summary_type` | character |  |
| `outs` | integer |  |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_type` | character |  |
| `period_type` | character |  |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `team_id` | character | Team id. |
| `pitch_count_balls` | integer |  |
| `pitch_count_strikes` | integer |  |
| `result_count_balls` | integer |  |
| `result_count_strikes` | integer |  |
| `participants` | character | Participants. |
| `bat_order` | double |  |
| `type_alternative_text` | character |  |
| `bats_type` | character |  |
| `bats_abbreviation` | character |  |
| `bats_display_value` | character |  |
| `at_bat_pitch_number` | double |  |
| `pitch_velocity` | double |  |
| `trajectory` | character |  |
| `type_abbreviation` | character |  |
| `pitch_coordinate_x` | double |  |
| `pitch_coordinate_y` | double |  |
| `pitch_type_id` | character |  |
| `pitch_type_text` | character |  |
| `pitch_type_abbreviation` | character |  |
| `hit_coordinate_x` | double |  |
| `hit_coordinate_y` | double |  |
| `alternative_play` | character |  |
| `alternative_type_id` | character |  |
| `alternative_type_text` | character |  |
| `alternative_type_abbreviation` | character |  |
| `alternative_type_alternative_text` | character |  |
| `alternative_type_type` | character |  |
| `on_first_athlete_id` | character |  |
| `on_second_athlete_id` | character |  |
| `on_third_athlete_id` | character |  |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | double | Home win percentage. |
| `tie_percentage` | double | Tie percentage. |
| `play_id` | character | Play id. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | integer | Attendance. |
| `venue_id` | character | Venue id. |
| `venue_full_name` | character | Venue full name. |
| `venue_short_name` | character | Venue short name. |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state. |
| `venue_address_zip_code` | character |  |

**officials**

| col_name | type | description |
|---|---|---|
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

**season_series**

| col_name | type | description |
|---|---|---|
| `type` | character | Type. |
| `title` | character | Title. |
| `description` | character | Description. |
| `summary` | character | Summary. |
| `completed` | logical | Completed. |
| `total_competitions` | integer | Total competitions. |
| `series_score` | character | Series score. |
| `events` | character | Events. |
| `series_label` | character | Series label. |
| `short_summary` | character | Short summary. |
| `round` | character |  |

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
| `links_web_href` | character | Links web href. |
| `links_mobile_href` | character | Links mobile href. |
| `links_api_self_href` | character | Links api self href. |
| `links_app_sportscenter_href` | character | Links app sportscenter href. |
| `links_web_self_href` | character |  |
| `links_web_self_dsi_href` | character |  |
| `links_api_artwork_href` | character |  |
| `links_sportscenter_href` | character |  |

### Example

```python
espn_mlb_summary()
```

_Last validated n/a._

## `espn_mlb_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_calendar()
```

_Last validated n/a._

## `espn_mlb_calendar_offseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/offseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/offseason](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/offseason)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_calendar_offseason()
```

_Last validated n/a._

## `espn_mlb_calendar_regular_season`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/regular-season`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/regular-season](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/regular-season)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_calendar_regular_season()
```

_Last validated n/a._

## `espn_mlb_calendar_postseason`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/postseason`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/postseason](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/postseason)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_calendar_postseason()
```

_Last validated n/a._

## `espn_mlb_calendar_ondays`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/ondays`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/ondays](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar/ondays)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_calendar_ondays()
```

_Last validated n/a._

## `espn_mlb_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_news()
```

_Last validated n/a._

## `espn_mlb_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_injuries()
```

_Last validated n/a._

## `espn_mlb_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_transactions()
```

_Last validated n/a._

## `espn_mlb_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_conferences()
```

_Last validated n/a._

## `espn_mlb_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_statistics_league()
```

_Last validated n/a._

## `espn_mlb_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_draft()
```

_Last validated n/a._

## `espn_mlb_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams)

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
espn_mlb_teams_site()
```

_Last validated n/a._

## `espn_mlb_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

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
| `nickname` | character |  |
| `short_name` | character | Short name. |
| `weight` | double | Weight. |
| `display_weight` | character | Display weight. |
| `height` | double | Height. |
| `display_height` | character | Display height. |
| `age` | integer | Age. |
| `date_of_birth` | character | Date of birth. |
| `debut_year` | integer | Debut year. |
| `links` | character | Links. |
| `slug` | character | Slug. |
| `jersey` | character | Jersey. |
| `positions` | character |  |
| `injuries` | character | Injuries. |
| `teams` | character | Teams. |
| `contracts` | character | Contracts. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_place_display_text` | character |  |
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
| `bats_type` | character |  |
| `bats_abbreviation` | character |  |
| `bats_display_value` | character |  |
| `throws_type` | character |  |
| `throws_abbreviation` | character |  |
| `throws_display_value` | character |  |

### Example

```python
espn_mlb_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `season` | `season` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_record(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_history(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/news)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |
| `limit` | `limit` |  |  | `Y` |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_news(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `team_id` | `team_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_mlb_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_mlb_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_mlb_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable |
|---|---|:---:|:---:|:---:|
| `athlete_id` | `athlete_id` |  | `Y` |  |

### Returns

Raw JSON `Dict` (no parser registered).

### Example

```python
espn_mlb_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_mlb_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings](https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings)

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
| `ot_losses` | double |  |
| `ot_wins` | double |  |
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `games_played` | double |  |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `ties` | double |  |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `division_games_behind` | double |  |
| `division_percent` | double |  |
| `division_tied` | double |  |
| `home_losses` | double |  |
| `home_ties` | double |  |
| `home_wins` | double |  |
| `magic_number_division` | double |  |
| `magic_number_wildcard` | double |  |
| `playoff_percent` | double |  |
| `road_losses` | double |  |
| `road_ties` | double |  |
| `road_wins` | double |  |
| `wild_card_percent` | double |  |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `intradivision` | character |  |
| `intraleague` | character |  |
| `last ten games` | character | Last ten games. |

### Example

```python
espn_mlb_standings()
```

_Last validated n/a._
