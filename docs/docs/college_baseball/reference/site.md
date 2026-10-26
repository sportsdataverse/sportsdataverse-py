---
title: College baseball — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
description: "College baseball — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 20
toc_max_heading_level: 2
---
# College baseball — ESPN site API (v2)

`sportsdataverse.baseball.college_baseball` — 25 endpoints.

## espn_college_baseball_scoreboard

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard?dates=20240115&limit=500](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/scoreboard?dates=20240115&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_baseball_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `game_id` | Int64 | ESPN event id. |
| `uid` | String |  |
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
| `attendance` | Int64 |  |
| `venue_id` | Int64 |  |
| `venue_full_name` | String |  |
| `venue_city` | String |  |
| `venue_state` | String |  |
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

### Raises {#espn_college_baseball_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_scoreboard-example}

```python
espn_college_baseball_scoreboard(dates='20240115')
```

## espn_college_baseball_summary

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/summary](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns {#espn_college_baseball_summary-returns}

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
| `did_not_play` | Boolean | Did not play. |
| `ejected` | Boolean | Ejected. |
| `reason` | String | Reason. |
| `minutes` | String | Minutes. |
| `points` | String | Points. |
| `field_goals_made_field_goals_attempted` | String | Field goals made field goals attempted. |
| `three_point_field_goals_made_three_point_field_goals_attempted` | String | Three point field goals made three point field goals attempted. |
| `free_throws_made_free_throws_attempted` | String | Free throws made free throws attempted. |
| `rebounds` | String | Rebounds. |
| `assists` | String | Assists. |
| `turnovers` | String | Turnovers. |
| `steals` | String | Steals. |
| `blocks` | String | Blocks. |
| `offensive_rebounds` | String | Offensive rebounds. |
| `defensive_rebounds` | String | Defensive rebounds. |
| `fouls` | String | Fouls. |
| `plus_minus` | String | Plus minus. |

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
| `participants` | String | Participants. |
| `wallclock` | String | Wallclock. |
| `shooting_play` | Boolean | Shooting play. |
| `points_attempted` | Int64 | Points attempted. |
| `short_description` | String | Short description. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `period_number` | Int64 | Period number. |
| `period_display_value` | String | Period display value. |
| `clock_display_value` | String | Clock display value. |
| `team_id` | Int64 | Team id. |
| `coordinate_x` | Int64 | Coordinate x. |
| `coordinate_y` | Int64 | Coordinate y. |

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
| `venue_short_name` | String | Venue short name. |
| `venue_address_city` | String | Venue address city. |
| `venue_address_state` | String | Venue address state. |
| `venue_grass` | Boolean | Venue grass. |

**officials**

| col_name | type | description |
|---|---|---|
| `full_name` | String | Full name. |
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
| `season_year` | Int64 | Season year. |
| `season_current` | Boolean | Season current. |
| `season_type` | Int64 | Season type. |
| `league_id` | Int64 | League id. |
| `league_uid` | String | League uid. |
| `league_name` | String | League name. |
| `league_abbreviation` | String | League abbreviation. |
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
| `series_label` | String | Series label. |
| `series_score` | String | Series score. |
| `short_summary` | String | Short summary. |
| `events` | String | Events. |

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

**format**

| col_name | type | description |
|---|---|---|
| `regulation_periods` | Int64 | Regulation periods. |
| `regulation_display_name` | String | Regulation display name. |
| `regulation_slug` | String | Regulation slug. |
| `regulation_clock` | Float64 | Regulation clock. |
| `overtime_display_name` | String | Overtime display name. |
| `overtime_slug` | String | Overtime slug. |
| `overtime_clock` | Float64 | Overtime clock. |

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
| `byline` | String | Byline. |
| `links_web_href` | String | Links web href. |
| `links_mobile_href` | String | Links mobile href. |
| `links_api_self_href` | String | Links api self href. |
| `links_app_sportscenter_href` | String | Links app sportscenter href. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_summary-example}

```python
espn_college_baseball_summary()
```

## espn_college_baseball_calendar

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/calendar](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_baseball_calendar-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_calendar-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_calendar-example}

```python
espn_college_baseball_calendar()
```

## espn_college_baseball_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_baseball_news-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_news-example}

```python
espn_college_baseball_news()
```

## espn_college_baseball_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_baseball_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_injuries-example}

```python
espn_college_baseball_injuries()
```

## espn_college_baseball_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/transactions?limit=500](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/transactions?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_baseball_transactions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `date` | String |  |
| `description` | String |  |
| `team_abbreviation` | String |  |
| `team_alternate_color` | String |  |
| `team_color` | String |  |
| `team_display_name` | String |  |
| `team_id` | Int64 |  |
| `team_links` | String |  |
| `team_location` | String |  |
| `team_logos` | String |  |
| `team_name` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_transactions-example}

```python
espn_college_baseball_transactions()
```

## espn_college_baseball_conferences

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/groups](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_baseball_conferences-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group_id` | Int64 |  |
| `name` | String |  |
| `abbreviation` | String |  |
| `short_name` | String |  |
| `is_conference` | Boolean |  |
| `parent_group_id` | Int64 |  |
| `depth` | Int64 |  |
| `children_count` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_conferences-example}

```python
espn_college_baseball_conferences()
```

## espn_college_baseball_statistics_league

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/statistics](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_baseball_statistics_league-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: league, season, status, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_statistics_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_statistics_league-example}

```python
espn_college_baseball_statistics_league()
```

## espn_college_baseball_draft

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/draft](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_baseball_draft-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, nhl, wnba, 2026-10-07); its rows sit under keys it does not read (top level: breakingNews, broadcasts, displayName, picks, positions, rounds, shortDisplayName, status).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_draft-example}

```python
espn_college_baseball_draft()
```

## espn_college_baseball_teams_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams?limit=1000](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_baseball_teams_site-returns}

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

### Raises {#espn_college_baseball_teams_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_teams_site-example}

```python
espn_college_baseball_teams_site()
```

## espn_college_baseball_team

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_abbreviation` | String |  |
| `team_alternate_color` | String |  |
| `team_color` | String |  |
| `team_display_name` | String |  |
| `team_franchise_$ref` | String |  |
| `team_franchise_abbreviation` | String |  |
| `team_franchise_color` | String |  |
| `team_franchise_display_name` | String |  |
| `team_franchise_id` | Int64 |  |
| `team_franchise_is_active` | Boolean |  |
| `team_franchise_location` | String |  |
| `team_franchise_name` | String |  |
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
| `team_id` | Int64 |  |
| `team_is_active` | Boolean |  |
| `team_links` | String |  |
| `team_location` | String |  |
| `team_logos` | String |  |
| `team_name` | String |  |
| `team_next_event` | String |  |
| `team_record_items` | String |  |
| `team_short_display_name` | String |  |
| `team_slug` | String |  |
| `team_standing_summary` | String |  |
| `team_uid` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team-example}

```python
espn_college_baseball_team(team_id='4')
```

## espn_college_baseball_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/roster?limit=500](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/roster?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_baseball_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | Id. |
| `uid` | String | Uid. |
| `guid` | String | Guid. |
| `first_name` | String | First name. |
| `last_name` | String | Last name. |
| `full_name` | String | Full name. |
| `display_name` | String | Display name. |
| `short_name` | String | Short name. |
| `weight` | Float64 | Weight. |
| `display_weight` | String | Display weight. |
| `height` | Float64 | Height. |
| `display_height` | String | Display height. |
| `age` | Int64 | Age. |
| `date_of_birth` | String | Date of birth. |
| `debut_year` | Float64 | Debut year. |
| `links` | String | Links. |
| `slug` | String | Slug. |
| `jersey` | String | Jersey. |
| `injuries` | String | Injuries. |
| `teams` | String | Teams. |
| `contracts` | String | Contracts. |
| `alternate_ids_sdr` | String | Alternate ids sdr. |
| `birth_place_city` | String | Birth place city. |
| `birth_place_country` | String | Birth place country. |
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
| `experience_years` | Int64 | Experience years. |
| `contract_bird_status` | Int64 | Contract bird status. |
| `contract_base_year_compensation_active` | Boolean | Contract base year compensation active. |
| `contract_poison_pill_provision_active` | Boolean | Contract poison pill provision active. |
| `contract_incoming_trade_value` | Int64 | Contract incoming trade value. |
| `contract_outgoing_trade_value` | Int64 | Contract outgoing trade value. |
| `contract_minimum_salary_exception` | Boolean | Contract minimum salary exception. |
| `contract_option_type` | Int64 | Contract option type. |
| `contract_salary` | Int64 | Contract salary. |
| `contract_salary_remaining` | Int64 | Contract salary remaining. |
| `contract_years_remaining` | Int64 | Contract years remaining. |
| `contract_season_year` | Int64 | Contract season year. |
| `contract_season_start_date` | String | Contract season start date. |
| `contract_season_end_date` | String | Contract season end date. |
| `contract_trade_kicker_active` | Boolean | Contract trade kicker active. |
| `contract_trade_kicker_percentage` | Float64 | Contract trade kicker percentage. |
| `contract_trade_kicker_value` | Int64 | Contract trade kicker value. |
| `contract_trade_kicker_trade_value` | Int64 | Contract trade kicker trade value. |
| `contract_trade_restriction` | Boolean | Contract trade restriction. |
| `contract_unsigned_foreign_pick` | Boolean | Contract unsigned foreign pick. |
| `contract_active` | Boolean | Contract active. |
| `status_id` | Int64 | Status id. |
| `status_name` | String | Status name. |
| `status_type` | String | Status type. |
| `status_abbreviation` | String | Status abbreviation. |
| `citizenship` | String | Citizenship. |
| `birth_place_state` | String | Birth place state. |
| `hand_type` | String | Hand type. |
| `hand_abbreviation` | String | Hand abbreviation. |
| `hand_display_value` | String | Hand display value. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_roster-example}

```python
espn_college_baseball_team_roster(team_id='4')
```

## espn_college_baseball_team_schedule

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_college_baseball_team_schedule-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_schedule-example}

```python
espn_college_baseball_team_schedule(team_id='4')
```

## espn_college_baseball_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team_record-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_record-example}

```python
espn_college_baseball_team_record(team_id='4')
```

## espn_college_baseball_team_depthcharts

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team_depthcharts-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: depthchart, season, status, team, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_depthcharts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_depthcharts-example}

```python
espn_college_baseball_team_depthcharts(team_id='4')
```

## espn_college_baseball_team_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_injuries-example}

```python
espn_college_baseball_team_injuries(team_id='4')
```

## espn_college_baseball_team_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team_transactions-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_transactions-example}

```python
espn_college_baseball_team_transactions(team_id='4')
```

## espn_college_baseball_team_history

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team_history-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, mbb, wbb; 400 in cfb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_history-example}

```python
espn_college_baseball_team_history(team_id='4')
```

## espn_college_baseball_team_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_baseball_team_news-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_news-example}

```python
espn_college_baseball_team_news(team_id='4')
```

## espn_college_baseball_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_baseball_team_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_team_leaders-example}

```python
espn_college_baseball_team_leaders(team_id='4')
```

## espn_college_baseball_player_info

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_college_baseball_player_info-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_player_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_player_info-example}

```python
espn_college_baseball_player_info(athlete_id='4239')
```

## espn_college_baseball_player_bio

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_college_baseball_player_bio-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_player_bio-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_player_bio-example}

```python
espn_college_baseball_player_bio(athlete_id='4239')
```

## espn_college_baseball_player_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_college_baseball_player_news-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_player_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_player_news-example}

```python
espn_college_baseball_player_news(athlete_id='4239')
```

## espn_college_baseball_standings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/baseball/college-baseball/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/baseball/college-baseball/standings](https://site.api.espn.com/apis/v2/sports/baseball/college-baseball/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns {#espn_college_baseball_standings-returns}

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
| `avg_points_against` | Float64 | Avg points against. |
| `avg_points_for` | Float64 | Avg points for. |
| `clincher` | Float64 | Clincher. |
| `differential` | Float64 | Differential. |
| `division_win_percent` | Float64 | Division win percent. |
| `games_behind` | Float64 | Games behind. |
| `league_win_percent` | Float64 | League win percent. |
| `losses` | Float64 | Losses. |
| `playoff_seed` | Float64 | Playoff seed. |
| `point_differential` | Float64 | Point differential. |
| `points` | Float64 | Points. |
| `points_against` | Float64 | Points against. |
| `points_for` | Float64 | Points for. |
| `streak` | Float64 | Streak. |
| `win_percent` | Float64 | Win percent. |
| `wins` | Float64 | Wins. |
| `games_ahead` | Float64 | Games ahead. |
| `overall` | String | Overall. |
| `home` | String | Home. |
| `road` | String | Road. |
| `vs. div.` | String | Vs. div.. |
| `vs. conf.` | String | Vs. conf.. |
| `last ten games` | String | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_college_baseball_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_standings-example}

```python
espn_college_baseball_standings()
```

## espn_college_baseball_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/rankings`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/rankings](https://site.api.espn.com/apis/site/v2/sports/baseball/college-baseball/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_baseball_rankings-returns}

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

### Raises {#espn_college_baseball_rankings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_college_baseball_rankings-example}

```python
espn_college_baseball_rankings()
```
