---
title: NHL — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# NHL — ESPN site API (v2)

`sportsdataverse.nhl` — 24 endpoints.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `game_id` | character | ESPN event id. |
| `uid` | character | Competitor uid string. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_year` | integer | Season end year. |
| `season_type` | integer | Season type code (echoed from arg). |
| `season_slug` | character | Season type slug. |
| `status_type_id` | character | Status type identifier. |
| `status_type_name` | character | Status type name. |
| `status_type_state` | character | Status state (pre/in/post). |
| `status_type_completed` | logical | Whether the game is complete. |
| `status_type_description` | character | Status description. |
| `status_type_detail` | character | Status detail text. |
| `status_type_short_detail` | character | Short status detail. |
| `status_clock` | double | Game clock in seconds. |
| `status_display_clock` | character | Display clock string. |
| `status_period` | integer | Current period. |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |
| `conference_competition` | character | Whether it is a conference competition. |
| `attendance` | integer | Game attendance. |
| `venue_id` | character | Venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_city` | character | Venue city. |
| `venue_state` | character | Venue state. |
| `venue_indoor` | logical | Whether the venue is indoors. |
| `broadcast` | character | Broadcast network(s). |
| `note` | character | Game note or headline. |
| `home_id` | character | Home team ESPN identifier. |
| `home_name` | character | Home team display name. |
| `home_abbreviation` | character | Home team abbreviation. |
| `home_display_name` | character | Home team display name. |
| `home_location` | character | Home team city. |
| `home_color` | character | Home team primary color hex. |
| `home_alternate_color` | character | Home team alternate color hex. |
| `home_logo` | character | Home team logo URL. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `home_winner` | logical | Whether the home team won. |
| `home_rank` | integer | Home team rank (if ranked). |
| `away_id` | character | Away team ESPN identifier. |
| `away_name` | character | Away team display name. |
| `away_abbreviation` | character | Away team abbreviation. |
| `away_display_name` | character | Away team display name. |
| `away_location` | character | Away team city. |
| `away_color` | character | Away team primary color hex. |
| `away_alternate_color` | character | Away team alternate color hex. |
| `away_logo` | character | Away team logo URL. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `away_winner` | logical | Whether the away team won. |
| `away_rank` | integer | Away team rank (if ranked). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `blocked_shots` | character | Blocked shots. |
| `hits` | character | Hits. |
| `takeaways` | character | Takeaways. |
| `plus_minus` | character | Plus minus. |
| `time_on_ice` | character | Time on ice in seconds. |
| `power_play_time_on_ice` | character | Total time the player spent on the ice during power-play situations in the game. |
| `short_handed_time_on_ice` | character | Total time the player spent on the ice while the team was at a numerical disadvantage (shorthanded) in the game. |
| `even_strength_time_on_ice` | character | Total time the player spent on the ice during even-strength situations in the game. |
| `shifts` | character | Number of shifts. |
| `goals` | character | Goals scored. |
| `ytd_goals` | character | Year-to-date goal total for the player entering or through this game. |
| `assists` | character | Assists. |
| `shots_total` | character | Shots on goal. |
| `shots_missed` | character | Number of shots taken by the player that did not result in a goal or save attempt in the game. |
| `shootout_goals` | character | Shootout goals. |
| `faceoffs_won` | character | Faceoffs won in the season. |
| `faceoffs_lost` | character | Faceoffs lost in the season. |
| `faceoff_percent` | character | Faceoff win percentage. |
| `giveaways` | character | Giveaways. |
| `penalties` | character | Penalty count. |
| `penalty_minutes` | character | Penalty minutes. |
| `goals_against` | character | Goals against. |
| `shots_against` | character | Shots faced. |
| `shootout_saves` | character | Shootout saves made. |
| `shootout_shots_against` | character | Number of shootout attempts faced by the goaltender in the game's shootout. |
| `saves` | character | Saves made. |
| `save_pct` | character | Save percentage. |
| `even_strength_saves` | character | Number of saves made by the goaltender during even-strength play in the game. |
| `power_play_saves` | character | Number of saves made by the goaltender during power-play situations in the game. |
| `short_handed_saves` | character | Number of saves made by the goaltender while the team was shorthanded in the game. |

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
| `type_abbreviation` | character | Play type abbreviation. |
| `period_number` | integer | Period number. |
| `period_display_value` | character | Period display value. |
| `clock_display_value` | character | Clock display value. |
| `participants` | character | Participants. |
| `team_id` | character | Team id. |
| `strength_id` | character | Strength situation id (e.g. even strength, PP, SH). |
| `strength_text` | character | Strength situation (e.g. "Even Strength", "Power Play"). |
| `strength_abbreviation` | character | Abbreviated label for the strength-of-play situation at the time of the scoring event (e.g., even strength, power play). |
| `coordinate_x` | double | Coordinate x. |
| `coordinate_y` | double | Coordinate y. |
| `shot_info_id` | character | Shot type identifier. |
| `shot_info_text` | character | Shot type text (e.g. "Wrist Shot"). |
| `shot_info_abbreviation` | character | Abbreviated description of the shot type associated with a scoring event in the box score. |

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
| `game_note` | character | Optional editorial note or context annotation attached to the game in the header. |
| `standings` | character | Condensed standings reference or link included in the game header for the relevant league. |
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
| `round` | character | Shootout round number. |

**standings**

| col_name | type | description |
|---|---|---|
| `group_header` | character | Group header. |
| `conference_header` | character | Conference header. |
| `division_header` | character | Division header. |
| `team_id` | character | Team id. |
| `team_uid` | character | Team uid. |
| `team_location` | character | Team location. |
| `ot_losses` | character | Overtime losses. |
| `losses` | character | Losses. |
| `points` | character | Points. |
| `wins` | character | Wins. |

**broadcasts**

| col_name | type | description |
|---|---|---|
| `station` | character | Station full name (e.g. "FanDuel Sports Network Detroit"). |
| `lang` | character | Broadcast language (e.g. "en"). |
| `region` | character | Broadcast region (e.g. "us"). |
| `is_national` | logical | Boolean flag indicating whether the broadcast is a nationally distributed feed. |
| `type_id` | character | Type id. |
| `type_short_name` | character | Broadcast type short name (e.g. "TV"). |
| `type_long_name` | character | Broadcast type long name (e.g. "Television"). |
| `type_slug` | character | Broadcast type slug. |
| `market_id` | character | Market identifier. |
| `market_type` | character | Market type. |
| `media_call_letters` | character | Media outlet call letters. |
| `media_name` | character | Media outlet full name. |
| `media_short_name` | character | Media outlet short name. |

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_nhl_calendar()
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
| `byline` | character | Author byline string as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_groups`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `position_group` | character | Position group name (e.g. Centers). |
| `id` | character | Id. |
| `uid` | character | Uid. |
| `guid` | character | Guid. |
| `alternate_id` | character | Alternate player identifier. |
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
| `birth_place_display_text` | character | Birth place display text. |
| `birth_country_abbreviation` | character | Birth country abbreviation. |
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
| `position_parent_id` | character | Parent position identifier. |
| `position_parent_name` | character | Parent position name. |
| `position_parent_display_name` | character | Parent position display name. |
| `position_parent_abbreviation` | character | Parent position abbreviation. |
| `position_parent_leaf` | logical | Whether parent position is leaf. |
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_team_schedule`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `id` | character | ESPN numeric identifier for the athlete. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `injuries` | character | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `byline` | character | Author byline string as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `byline` | character | Author byline string as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

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
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `games_behind` | double | Games behind. |
| `games_played` | double | Matches played. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `wins` | double | Wins. |
| `overtime_losses` | double | Total overtime losses. |
| `overtime_wins` | double | Overtime wins. |
| `points_diff` | double | Difference between total points scored for and against the team across all games played. |
| `reg_losses` | double | Number of losses the team has suffered in regulation time (excluding overtime and shootout losses). |
| `reg_wins` | double | Number of wins the team has earned in regulation time (excluding overtime and shootout wins). |
| `rot_losses` | double | Number of losses the team has suffered in overtime or the shootout (non-regulation losses). |
| `rot_wins` | double | Number of wins the team has earned in overtime or the shootout (non-regulation wins). |
| `shootout_losses` | double | Shootout losses. |
| `shootout_wins` | double | Shootout wins. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `last ten games` | character | Last ten games. |
| `vs. div.` | character | Vs. div.. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_nhl_standings()
```

_Last validated n/a._
