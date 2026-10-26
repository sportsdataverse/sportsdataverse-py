---
title: MLB — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
description: "MLB — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 20
toc_max_heading_level: 2
---
# MLB — ESPN site API (v2)

`sportsdataverse.mlb` — 24 endpoints.

## espn_mlb_scoreboard

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=20240115&limit=500](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard?dates=20240115&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN event id. |
| `uid` | String | ESPN UID string. |
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
| `conference_competition` | Boolean | Conference competition. |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_full_name` | String |  |
| `venue_city` | String | Venue city. |
| `venue_state` | String | Venue state / province. |
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

### Raises {#espn_mlb_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_scoreboard-example}

```python
espn_mlb_scoreboard(dates='20240115')
```

## espn_mlb_summary

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns {#espn_mlb_summary-returns}

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
| `starter` | Boolean | Starter. |
| `active` | Boolean | Active. |
| `did_not_play` | String | Did not play. |
| `ejected` | String | Ejected. |
| `reason` | String | Reason. |
| `hits_at_bats` | String | Hitting performance ratio expressed as hits divided by at-bats for the player in the box score. |
| `at_bats` | String | At bats. |
| `runs` | String | Runs scored. |
| `hits` | String | Hits. |
| `rb_is` | String | Runs batted in and other secondary hitting statistics for the player in the box score. |
| `home_runs` | String | Home runs. |
| `walks` | String | Number of base-on-balls (walks) recorded by the player in the box score. |
| `strikeouts` | String | Number of strikeouts recorded by the player, either as a batter or pitcher, in the box score. |
| `pitches` | String | Total number of pitches thrown or faced by the player in the box score. |
| `avg` | String | Batting average. |
| `on_base_pct` | String | Percentage of plate appearances in which the player reached base safely in the game. |
| `slug_avg` | String | Slugging average reflecting the total bases per at-bat recorded by the player in the box score. |
| `full_innings.part_innings` | String | Innings pitched expressed as full innings and fractional partial innings for the pitcher in the box score. |
| `earned_runs` | String | Earned runs allowed. |
| `pitches_strikes` | String | Combined count of pitches thrown and strikes recorded for the pitcher in the box score. |
| `era` | String | Earned run average. |

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

**plays**

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `sequence_number` | String | Sequence number. |
| `text` | String | Text. |
| `away_score` | Int64 | Away score. |
| `home_score` | Int64 | Home score. |
| `scoring_play` | Boolean | Scoring play. |
| `score_value` | Int64 | Score value. |
| `wallclock` | String | Wallclock. |
| `at_bat_id` | Int64 | Identifier of the at-bat the play belongs to. |
| `summary_type` | String | Play summary type. |
| `outs` | Int64 | Outs in the inning after the play. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `type_type` | String | Play type category. |
| `period_type` | String | Period type ('inning'). |
| `period_number` | Int64 | Period number. |
| `period_display_value` | String | Period display value. |
| `team_id` | Int64 | Team id. |
| `pitch_count_balls` | Int64 | Balls in the count when the pitch was thrown. |
| `pitch_count_strikes` | Int64 | Strikes in the count when the pitch was thrown. |
| `result_count_balls` | Int64 | Balls in the count after the pitch. |
| `result_count_strikes` | Int64 | Strikes in the count after the pitch. |
| `participants` | String | Participants. |
| `bat_order` | Float64 | Spot in the batting order (1-9; NA if not applicable). |
| `type_alternative_text` | String | Alternative play type text. |
| `bats_type` | String | Bats type. |
| `bats_abbreviation` | String | Bats abbreviation. |
| `bats_display_value` | String | Bats display value. |
| `at_bat_pitch_number` | Float64 | Pitch number within the at-bat. |
| `pitch_velocity` | Float64 | Pitch velocity (mph). |
| `trajectory` | String | Batted-ball trajectory. |
| `type_abbreviation` | String | Play type abbreviation. |
| `pitch_coordinate_x` | Float64 | Pitch location x-coordinate. |
| `pitch_coordinate_y` | Float64 | Pitch location y-coordinate. |
| `pitch_type_id` | Int64 | Pitch type identifier. |
| `pitch_type_text` | String | Pitch type description (e.g. 'Four-seam FB'). |
| `pitch_type_abbreviation` | String | Pitch type abbreviation. |
| `hit_coordinate_x` | Float64 | Batted-ball location x-coordinate. |
| `hit_coordinate_y` | Float64 | Batted-ball location y-coordinate. |
| `alternative_play` | String | Alternative play flag. |
| `alternative_type_id` | Int64 | Alternative play type id. |
| `alternative_type_text` | String | Alternative play type text. |
| `alternative_type_abbreviation` | String | Alternative play type abbreviation. |
| `alternative_type_alternative_text` | String | Alternative type alternative text. |
| `alternative_type_type` | String | Alternative play type category. |
| `on_first_athlete_id` | Int64 | Athlete id of the runner on first base. |
| `on_second_athlete_id` | Int64 | Athlete id of the runner on second base. |
| `on_third_athlete_id` | Int64 | Athlete id of the runner on third base. |

**winprobability**

| col_name | type | description |
|---|---|---|
| `home_win_percentage` | Float64 | Home win percentage. |
| `tie_percentage` | Float64 | Tie percentage. |
| `play_id` | Int64 | Play id. |

**game_info**

| col_name | type | description |
|---|---|---|
| `attendance` | Int64 | Attendance. |
| `venue_id` | Int64 | Venue id. |
| `venue_full_name` | String | Venue full name. |
| `venue_short_name` | String | Venue short name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state. |
| `venue_address_zip_code` | String | Postal zip code of the venue where the game was played. |

**officials**

| col_name | type | description |
|---|---|---|
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

**season_series**

| col_name | type | description |
|---|---|---|
| `type` | String | Type. |
| `title` | String | Title. |
| `description` | String | Description. |
| `summary` | String | Summary. |
| `completed` | Boolean | Completed. |
| `total_competitions` | Int64 | Total competitions. |
| `series_score` | String | Series score. |
| `events` | String | Events. |
| `series_label` | String | Series label. |
| `short_summary` | String | Short summary. |
| `round` | String | Draft round number. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | String | Group header. |
| `conference_header` | String | Conference header. |
| `division_header` | String | Division header. |
| `team_id` | Int64 | Team id. |
| `team_uid` | String | Team uid. |
| `team_location` | String | Team location. |
| `games_behind` | String | Games behind. |
| `losses` | String | Losses. |
| `streak` | String | Streak. |
| `win_percent` | String | Win percent. |
| `wins` | String | Wins. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | String |  |
| `station_key` | String | Machine-readable key identifying the broadcasting station airing the game. |
| `lang` | String |  |
| `region` | String | Region label. |
| `is_national` | Boolean | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | Int64 | Type id. |
| `type_short_name` | String |  |
| `type_long_name` | String |  |
| `type_slug` | String |  |
| `market_id` | Int64 | ESPN futures-market identifier. |
| `market_type` | String | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `media_call_letters` | String |  |
| `media_name` | String |  |
| `media_short_name` | String |  |

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | Int64 | Regulation periods. |
| `regulation_display_name` | String | Regulation display name. |
| `regulation_slug` | String | Regulation slug. |

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
| `links_web_href` | String | Links web href. |
| `links_mobile_href` | String | Links mobile href. |
| `links_api_self_href` | String | Links api self href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |
| `links_web_self_href` | String | URL for the canonical web page of the associated article or editorial content. |
| `links_web_self_dsi_href` | String | Data-source-identified URL for the web page of the associated article content. |
| `links_api_artwork_href` | String | API endpoint URL for artwork or imagery associated with the article. |
| `links_sportscenter_href` | String | URL for the article's page on ESPN's SportsCenter platform. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_summary-example}

```python
espn_mlb_summary()
```

## espn_mlb_calendar_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_calendar_site-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_calendar_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_calendar_site-example}

```python
espn_mlb_calendar_site()
```

## espn_mlb_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_news-returns}

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
| `links_web_href` | String | Web article URL. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |
| `links_web_self_href` | String | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | String | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | String | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | String | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_news-example}

```python
espn_mlb_news()
```

## espn_mlb_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_injuries-example}

```python
espn_mlb_injuries()
```

## espn_mlb_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions?limit=500](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/transactions?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String | Date in YYYY-MM-DD format. |
| `description` | String | Long-form description text. |
| `team_abbreviation` | String | Short team abbreviation (e.g. 'NYY'). |
| `team_alternate_color` | String | Team alternate color (hex). |
| `team_color` | String | Team primary color (hex, no leading '#'). |
| `team_display_name` | String | Full team display name (e.g. 'New York Yankees'). |
| `team_id` | Int64 | Unique ESPN team identifier. |
| `team_links` | String |  |
| `team_location` | String | Team city / location. |
| `team_logos` | String |  |
| `team_name` | String | Team name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_transactions-example}

```python
espn_mlb_transactions()
```

## espn_mlb_conferences

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | Int64 | ESPN group id. |
| `name` | String | Display name. |
| `abbreviation` | String | Short abbreviation. |
| `short_name` | String | Short display name. |
| `is_conference` | Boolean | Whether this group is a conference. |
| `parent_group_id` | Int64 | Unique identifier for parent group. |
| `depth` | Int64 |  |
| `children_count` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_conferences-example}

```python
espn_mlb_conferences()
```

## espn_mlb_statistics_league

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_statistics_league-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: league, season, status, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_statistics_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_statistics_league-example}

```python
espn_mlb_statistics_league()
```

## espn_mlb_draft

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_draft-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, nhl, wnba, 2026-10-07); its rows sit under keys it does not read (top level: breakingNews, broadcasts, displayName, picks, positions, rounds, shortDisplayName, status).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_draft-example}

```python
espn_mlb_draft()
```

## espn_mlb_teams_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams?limit=1000](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_teams_site-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. "BOS"). |
| `team_alternate_color` | String | Secondary team color as a hex string (no leading '#'). |
| `team_color` | String | Primary team color as a hex string (no leading '#'). |
| `team_display_name` | String | Full team display name (location + nickname). |
| `team_id` | Int64 | ESPN team id (stable join key across ESPN endpoints). |
| `team_is_active` | Boolean | Whether the team is currently active. |
| `team_is_all_star` | Boolean | Whether the entry is an all-star squad rather than a franchise. |
| `team_location` | String | Team location / city (e.g. "Boston"). |
| `team_logos` | String | Pipe-delimited logo image URLs. |
| `team_name` | String | Team nickname/mascot (e.g. "Celtics"). |
| `team_nickname` | String | Team nickname as ESPN labels it (often equals team_name). |
| `team_short_display_name` | String | Abbreviated display name for compact UIs. |
| `team_slug` | String | URL slug used in ESPN web paths. |
| `team_uid` | String | ESPN global UID (encodes sport/league/team). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_teams_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_teams_site-example}

```python
espn_mlb_teams_site()
```

## espn_mlb_team

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String | Short team abbreviation (e.g. 'NYY'). |
| `team_alternate_color` | String | Team alternate color (hex). |
| `team_color` | String | Team primary color (hex, no leading '#'). |
| `team_display_name` | String | Full team display name (e.g. 'New York Yankees'). |
| `team_franchise_$ref` | String |  |
| `team_franchise_abbreviation` | String |  |
| `team_franchise_color` | String |  |
| `team_franchise_display_name` | String |  |
| `team_franchise_id` | Int64 |  |
| `team_franchise_is_active` | Boolean |  |
| `team_franchise_location` | String |  |
| `team_franchise_name` | String | Franchise name. |
| `team_franchise_short_display_name` | String |  |
| `team_franchise_slug` | String |  |
| `team_franchise_team_$ref` | String |  |
| `team_franchise_uid` | String |  |
| `team_franchise_venue_$ref` | String |  |
| `team_franchise_venue_address_city` | String |  |
| `team_franchise_venue_address_state` | String |  |
| `team_franchise_venue_full_name` | String |  |
| `team_franchise_venue_grass` | Boolean |  |
| `team_franchise_venue_guid` | String |  |
| `team_franchise_venue_id` | Int64 |  |
| `team_franchise_venue_images` | String |  |
| `team_franchise_venue_indoor` | Boolean |  |
| `team_franchise_venue_short_name` | String |  |
| `team_groups_id` | Int64 |  |
| `team_groups_is_conference` | Boolean |  |
| `team_groups_parent_id` | Int64 |  |
| `team_id` | Int64 | Unique ESPN team identifier. |
| `team_is_active` | Boolean | Team is active. |
| `team_links` | String |  |
| `team_location` | String | Team city / location. |
| `team_logos` | String |  |
| `team_name` | String | Team name. |
| `team_next_event` | String |  |
| `team_record_items` | String |  |
| `team_short_display_name` | String | Short team display name. |
| `team_slug` | String | URL-safe team identifier. |
| `team_standing_summary` | String |  |
| `team_uid` | String | ESPN universal team identifier (UID). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team-example}

```python
espn_mlb_team(team_id='4')
```

## espn_mlb_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/roster?limit=500](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/roster?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `position_group` | String |  |
| `id` | Int64 | Id. |
| `uid` | String | Uid. |
| `guid` | String | Guid. |
| `first_name` | String | First name. |
| `last_name` | String | Last name. |
| `full_name` | String | Full name. |
| `display_name` | String | Display name. |
| `nickname` | String | Team nickname. |
| `short_name` | String | Short name. |
| `weight` | Float64 | Weight. |
| `display_weight` | String | Display weight. |
| `height` | Float64 | Height. |
| `display_height` | String | Display height. |
| `age` | Int64 | Age. |
| `date_of_birth` | String | Date of birth. |
| `debut_year` | Int64 | Debut year. |
| `links` | String | Links. |
| `slug` | String | Slug. |
| `jersey` | String | Jersey. |
| `positions` | String | Positions. |
| `injuries` | String | Injuries. |
| `teams` | String | Teams. |
| `contracts` | String | Contracts. |
| `alternate_ids_sdr` | String | Alternate ids sdr. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_state` | String | Birth place state. |
| `birth_place_country` | String | Birth place country. |
| `birth_place_display_text` | String |  |
| `college_id` | Int64 | College id. |
| `college_guid` | String | College guid. |
| `college_mascot` | String | College mascot. |
| `college_name` | String | College name. |
| `college_short_name` | String | College short name. |
| `college_abbrev` | String | College abbrev. |
| `college_logos` | String | College logos. |
| `headshot_href` | String | Headshot href. |
| `headshot_alt` | String | Headshot alt. |
| `position_id` | Int64 | Position id. |
| `position_name` | String | Position name. |
| `position_display_name` | String | Position display name. |
| `position_abbreviation` | String | Position abbreviation. |
| `position_leaf` | Boolean | Position leaf. |
| `position_parent_id` | Int64 |  |
| `position_parent_name` | String |  |
| `position_parent_display_name` | String |  |
| `position_parent_abbreviation` | String |  |
| `position_parent_leaf` | Boolean |  |
| `experience_years` | Int64 | Experience years. |
| `status_id` | Int64 | Status id. |
| `status_name` | String | Status name. |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `bats_type` | String | Bats type. |
| `bats_abbreviation` | String | Bats abbreviation. |
| `bats_display_value` | String | Bats display value. |
| `throws_type` | String | Throws type. |
| `throws_abbreviation` | String | Throws abbreviation. |
| `throws_display_value` | String | Throws display value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_roster-example}

```python
espn_mlb_team_roster(team_id='4')
```

## espn_mlb_team_schedule

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_mlb_team_schedule-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric event identifier. |
| `date` | String | Event timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `time_valid` | Boolean | Whether the event time is confirmed. |
| `competitions` | String | Competition detail (list of dicts, stringified): competitors, venue, status. |
| `links` | String | Related links (list, stringified). |
| `season_year` | Int64 | Four-digit season year. |
| `season_display_name` | String | Human-readable season label (e.g. '2024-25'). |
| `season_type_id` | Int64 | ESPN numeric identifier for the season type. |
| `season_type_type` | Int64 | Season type numeric code. |
| `season_type_name` | String | Season type name (e.g. Regular Season). |
| `season_type_abbreviation` | String | Season type abbreviation. |
| `week_number` | Float64 |  |
| `week_text` | String | Human-readable label for the week or scheduling block in which the event falls (e.g., 'Week 3', 'Bowl Week'), as returned by the ESPN schedule API. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_schedule-example}

```python
espn_mlb_team_schedule(team_id='4')
```

## espn_mlb_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_record-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_record-example}

```python
espn_mlb_team_record(team_id='4')
```

## espn_mlb_team_depthcharts

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_depthcharts-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: depthchart, season, status, team, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_depthcharts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_depthcharts-example}

```python
espn_mlb_team_depthcharts(team_id='4')
```

## espn_mlb_team_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_injuries-example}

```python
espn_mlb_team_injuries(team_id='4')
```

## espn_mlb_team_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_transactions-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_transactions-example}

```python
espn_mlb_team_transactions(team_id='4')
```

## espn_mlb_team_history

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_history-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, mbb, wbb; 400 in cfb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_history-example}

```python
espn_mlb_team_history(team_id='4')
```

## espn_mlb_team_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_team_news-returns}

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
| `links_web_href` | String | Web article URL. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |
| `links_web_self_href` | String | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | String | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | String | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | String | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_news-example}

```python
espn_mlb_team_news(team_id='4')
```

## espn_mlb_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_team_leaders-example}

```python
espn_mlb_team_leaders(team_id='4')
```

## espn_mlb_player_info

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_mlb_player_info-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_info-example}

```python
espn_mlb_player_info(athlete_id='4239')
```

## espn_mlb_player_bio

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_mlb_player_bio-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_bio-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_bio-example}

```python
espn_mlb_player_bio(athlete_id='4239')
```

## espn_mlb_player_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_mlb_player_news-returns}

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
| `links_web_href` | String | Web article URL. |
| `links_mobile_href` | String | Mobile article URL. |
| `links_api_self_href` | String | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | String | SportsCenter app deep link. |
| `links_web_self_href` | String | Primary canonical web URL for this news article on ESPN.com. |
| `links_web_self_dsi_href` | String | Alternate canonical web URL for this news article using ESPN's DSI routing. |
| `links_api_artwork_href` | String | ESPN API URL for the artwork image associated with this news article. |
| `links_sportscenter_href` | String | Deep-link URL to this news article within the ESPN SportsCenter app or web experience. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_player_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_player_news-example}

```python
espn_mlb_player_news(athlete_id='4239')
```

## espn_mlb_standings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings](https://site.api.espn.com/apis/v2/sports/baseball/mlb/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns {#espn_mlb_standings-returns}

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
| `ot_losses` | Float64 |  |
| `ot_wins` | Float64 |  |
| `avg_points_against` | Float64 | Avg points against. |
| `avg_points_for` | Float64 | Avg points for. |
| `clincher` | Float64 | Clincher. |
| `differential` | Float64 | Differential. |
| `division_win_percent` | Float64 | Division win percent. |
| `games_behind` | Float64 | Games behind. |
| `games_played` | Float64 | Matches played. |
| `league_win_percent` | Float64 | League win percent. |
| `losses` | Float64 | Losses. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points` | Float64 | Points. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `ties` | Float64 | Number of matches the team has drawn. |
| `win_percent` | Float64 | Win percent. |
| `wins` | Float64 | Wins. |
| `division_games_behind` | Float64 | Number of games the team trails the division leader in the standings, expressed as a decimal (e.g., 0.5 for half a game back). |
| `division_percent` | Float64 | The team's winning percentage in division games, calculated as division wins divided by total division games played. |
| `division_tied` | Float64 | Number of games the team has tied against opponents within their own division. |
| `home_losses` | Float64 |  |
| `home_ties` | Float64 |  |
| `home_wins` | Float64 |  |
| `magic_number_division` | Float64 | Combination of wins needed by the team (or losses needed by the division leader) for the team to clinch a division title. |
| `magic_number_wildcard` | Float64 | Combination of wins needed by the team (or losses needed by the next wildcard team) for the team to clinch a wildcard playoff berth. |
| `playoff_percent` | Float64 | Estimated or model-derived probability that the team will qualify for the playoffs, expressed as a decimal between 0 and 1. |
| `road_losses` | Float64 |  |
| `road_ties` | Float64 |  |
| `road_wins` | Float64 |  |
| `wild_card_percent` | Float64 | The team's winning percentage in games that count toward wildcard standings positioning. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `road` | String | Road. |
| `intradivision` | String | Intradivision. |
| `intraleague` | String | Intraleague. |
| `last ten games` | String | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_standings-example}

```python
espn_mlb_standings()
```
