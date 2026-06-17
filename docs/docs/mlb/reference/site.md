---
title: MLB — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# MLB — ESPN site API (v2)

`sportsdataverse.mlb` — 24 endpoints.

## `espn_mlb_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=20240115)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season (football). |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | ESPN UID string. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | Season-type id. |
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
| `attendance` | integer | Reported game attendance. |
| `venue_id` | character | MLBAM venue ID. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state / province. |
| `venue_indoor` | logical | Whether the home venue is indoors. |
| `broadcast` | character | Broadcast information string. |
| `note` | character | Game note or headline. |
| `home_id` | character | Unique identifier for home. |
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
| `away_id` | character | Unique identifier for away. |
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

### Example

```python
espn_mlb_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_mlb_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns

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
| `starter` | logical | Starter. |
| `active` | logical | Active. |
| `did_not_play` | character | Did not play. |
| `ejected` | character | Ejected. |
| `reason` | character | Reason. |
| `hits_at_bats` | character | Hitting performance ratio expressed as hits divided by at-bats for the player in the box score. |
| `at_bats` | character | At bats. |
| `runs` | character | Runs scored. |
| `hits` | character | Hits. |
| `rb_is` | character | Runs batted in and other secondary hitting statistics for the player in the box score. |
| `home_runs` | character | Home runs. |
| `walks` | character | Number of base-on-balls (walks) recorded by the player in the box score. |
| `strikeouts` | character | Number of strikeouts recorded by the player, either as a batter or pitcher, in the box score. |
| `pitches` | character | Total number of pitches thrown or faced by the player in the box score. |
| `avg` | character | Batting average. |
| `on_base_pct` | character | Percentage of plate appearances in which the player reached base safely in the game. |
| `slug_avg` | character | Slugging average reflecting the total bases per at-bat recorded by the player in the box score. |
| `full_innings.part_innings` | character | Innings pitched expressed as full innings and fractional partial innings for the pitcher in the box score. |
| `earned_runs` | character | Earned runs allowed. |
| `pitches_strikes` | character | Combined count of pitches thrown and strikes recorded for the pitcher in the box score. |
| `era` | character | Earned run average. |

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
| `at_bat_id` | character | Identifier of the at-bat the play belongs to. |
| `summary_type` | character | Play summary type. |
| `outs` | integer | Outs in the inning after the play. |
| `type_id` | character | Type id. |
| `type_text` | character | Type text. |
| `type_type` | character | Play type category. |
| `period_type` | character | Period type ('inning'). |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `team_id` | character | Team id. |
| `pitch_count_balls` | integer | Balls in the count when the pitch was thrown. |
| `pitch_count_strikes` | integer | Strikes in the count when the pitch was thrown. |
| `result_count_balls` | integer | Balls in the count after the pitch. |
| `result_count_strikes` | integer | Strikes in the count after the pitch. |
| `participants` | character | Participants. |
| `bat_order` | double | Spot in the batting order (1-9; NA if not applicable). |
| `type_alternative_text` | character | Alternative play type text. |
| `bats_type` | character | Bats type. |
| `bats_abbreviation` | character | Bats abbreviation. |
| `bats_display_value` | character | Bats display value. |
| `at_bat_pitch_number` | double | Pitch number within the at-bat. |
| `pitch_velocity` | double | Pitch velocity (mph). |
| `trajectory` | character | Batted-ball trajectory. |
| `type_abbreviation` | character | Play type abbreviation. |
| `pitch_coordinate_x` | double | Pitch location x-coordinate. |
| `pitch_coordinate_y` | double | Pitch location y-coordinate. |
| `pitch_type_id` | character | Pitch type identifier. |
| `pitch_type_text` | character | Pitch type description (e.g. 'Four-seam FB'). |
| `pitch_type_abbreviation` | character | Pitch type abbreviation. |
| `hit_coordinate_x` | double | Batted-ball location x-coordinate. |
| `hit_coordinate_y` | double | Batted-ball location y-coordinate. |
| `alternative_play` | character | Alternative play flag. |
| `alternative_type_id` | character | Alternative play type id. |
| `alternative_type_text` | character | Alternative play type text. |
| `alternative_type_abbreviation` | character | Alternative play type abbreviation. |
| `alternative_type_alternative_text` | character | Alternative type alternative text. |
| `alternative_type_type` | character | Alternative play type category. |
| `on_first_athlete_id` | character | Athlete id of the runner on first base. |
| `on_second_athlete_id` | character | Athlete id of the runner on second base. |
| `on_third_athlete_id` | character | Athlete id of the runner on third base. |

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
| `venue_address_zip_code` | character | Postal zip code of the venue where the game was played. |

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
| `round` | character | Draft round number. |

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
| `station` | character | Station full name (e.g. "FanDuel Sports Network Detroit"). |
| `station_key` | character | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | character | Broadcast language (e.g. "en"). |
| `region` | character | Region label. |
| `is_national` | logical | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
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
| `links_web_self_href` | character | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | character | Data-source-identified URL for the web page of the associated article content. |
| `links_api_artwork_href` | character | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | character | URL for the article's page on ESPN's SportsCenter platform. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_summary()
```

_Last validated n/a._

## `espn_mlb_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_calendar()
```

_Last validated n/a._

## `espn_mlb_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

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
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | character | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | character | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | character | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_news()
```

_Last validated n/a._

## `espn_mlb_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_injuries()
```

_Last validated n/a._

## `espn_mlb_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_transactions()
```

_Last validated n/a._

## `espn_mlb_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_groups`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_conferences()
```

_Last validated n/a._

## `espn_mlb_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_statistics_league()
```

_Last validated n/a._

## `espn_mlb_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_draft()
```

_Last validated n/a._

## `espn_mlb_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

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

### Example

```python
espn_mlb_teams_site()
```

_Last validated n/a._

## `espn_mlb_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group of the recruits (e.g. Offensive Line, Defensive Back). |
| `id` | character | Id. |
| `uid` | character | Uid. |
| `guid` | character | Guid. |
| `first_name` | character | First name. |
| `last_name` | character | Last name. |
| `full_name` | character | Full name. |
| `display_name` | character | Display name. |
| `nickname` | character | Team nickname. |
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
| `positions` | character | Positions. |
| `injuries` | character | Injuries. |
| `teams` | character | Teams. |
| `contracts` | character | Contracts. |
| `alternate_ids_sdr` | character | Alternate ids sdr. |
| `birth_place_city` | character | Birth place city. |
| `birth_place_state` | character | Birth place state. |
| `birth_place_country` | character | Birth place country. |
| `birth_place_display_text` | character | Birth place display text. |
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
| `position_parent_id` | character | ESPN id of the parent position; `position_detail = TRUE` only. |
| `position_parent_name` | character | Parent position name. |
| `position_parent_display_name` | character | Parent position display name. |
| `position_parent_abbreviation` | character | Parent position abbreviation. |
| `position_parent_leaf` | logical | Whether parent position is leaf. |
| `experience_years` | integer | Experience years. |
| `status_id` | character | Status id. |
| `status_name` | character | Status name. |
| `status_type` | character | Status type. |
| `status_abbreviation` | character | Status abbreviation. |
| `bats_type` | character | Bats type. |
| `bats_abbreviation` | character | Bats abbreviation. |
| `bats_display_value` | character | Bats display value. |
| `throws_type` | character | Throws type. |
| `throws_abbreviation` | character | Throws abbreviation. |
| `throws_display_value` | character | Throws display value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns

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
| `week_number` | double | Week number. |
| `week_text` | character | Human-readable label for the week or scheduling block in which the event falls (e.g., 'Week 3', 'Bowl Week'), as returned by the ESPN schedule API. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_record(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_history(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

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
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | character | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | character | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | character | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_news(team_id='4')
```

_Last validated n/a._

## `espn_mlb_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_mlb_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_mlb_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_mlb_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

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
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |
| `links_web_self_href` | character | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | character | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | character | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | character | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_mlb_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings](https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns

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
| `ot_losses` | double | Overtime losses. |
| `ot_wins` | double | Overtime wins. |
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `games_played` | double | Matches played. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `ties` | double | Number of matches the team has drawn. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `division_games_behind` | double | Number of games the team trails the division leader in the standings, expressed as a decimal (e.g., 0.5 for half a game back). |
| `division_percent` | double | The team's winning percentage in division games, calculated as division wins divided by total division games played. |
| `division_tied` | double | Number of games the team has tied against opponents within their own division. |
| `home_losses` | double | Home team's losses. |
| `home_ties` | double | Total home ties. |
| `home_wins` | double | Home team's wins. |
| `magic_number_division` | double | Combination of wins needed by the team (or losses needed by the division leader) for the team to clinch a division title. |
| `magic_number_wildcard` | double | Combination of wins needed by the team (or losses needed by the next wildcard team) for the team to clinch a wildcard playoff berth. |
| `playoff_percent` | double | Estimated or model-derived probability that the team will qualify for the playoffs, expressed as a decimal between 0 and 1. |
| `road_losses` | double | Road losses. |
| `road_ties` | double | Ties on the road. |
| `road_wins` | double | Road wins. |
| `wild_card_percent` | double | The team's winning percentage in games that count toward wildcard standings positioning. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `intradivision` | character | Intradivision. |
| `intraleague` | character | Intraleague. |
| `last ten games` | character | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_mlb_standings()
```

_Last validated n/a._
