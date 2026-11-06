# FIFA World Cup — ESPN site API (v2)

> FIFA World Cup — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.soccer.wc` — 24 endpoints.

## espn_wc_scoreboard

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard?dates=20240115&limit=500](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard?dates=20240115&limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `week` | `week` |  |  | `Y` | Week number within the season. |
| `seasontype` | `season_type` |  |  | `Y` | Season phase: 1=preseason, 2=regular season, 3=postseason. |
| `groups` | `groups` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wc_scoreboard-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `event_id` | String | ESPN event id for the match. |
| `date` | String | Match start timestamp (ISO 8601, UTC). |
| `name` | String | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | String | Abbreviated event name (e.g. 'TA @ TB'). |
| `season_type` | Int64 | ESPN season-type id of the event's season: 1 preseason, 2 regular season, 3 postseason, 4 offseason for the US leagues; soccer competitions carry their own competition-specific ids (e.g. 13481). |
| `season_slug` | String | ESPN slug of the event's season type (e.g. 'regular-season', 'post-season'; soccer competitions use their own, such as 'final' or 'torneo-apertura'). |
| `tournament` | String | Short tournament the match belongs to, derived from the season-type slug: 'apertura' (July-December) or 'clausura' (January-May) for split-season leagues such as Liga MX, null for single-tournament leagues. |
| `home_team` | String | Home team display name. |
| `home_team_id` | String | Home team ESPN id. |
| `home_score` | String | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `away_team` | String | Away team display name. |
| `away_team_id` | String | Away team ESPN id. |
| `away_score` | String | Away team's score. For cricket, the innings string. |
| `status` | String | Status type name (e.g. STATUS_FINAL, STATUS_SCHEDULED, STATUS_IN_PROGRESS). |
| `venue` | String | Full name of the venue where the match was played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_scoreboard-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_scoreboard-example}

```python
espn_wc_scoreboard(dates='20240115')
```

## espn_wc_summary

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/summary](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns {#espn_wc_summary-returns}

**`return_parsed=True`** (default) — a dict of `polars.DataFrame`s keyed by summary section (one table per documented key below); pass `return_as_pandas=True` for a dict of `pandas.DataFrame`s (same keys).

**header**

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `uid` | String |  |
| `time_valid` | Boolean |  |
| `season_year` | Int64 |  |
| `season_type` | Int64 | ESPN season-phase numeric code (1=pre-season, 2=regular season, 3=postseason, 4=off-season). |
| `season_slug` | String |  |
| `tournament` | String | Short tournament the match belongs to, derived from the header season-type name: 'apertura' (July-December) or 'clausura' (January-May) for split-season leagues such as Liga MX, null for single-tournament leagues; soccer summaries only. |
| `league_id` | String | ESPN numeric identifier for the league or competition. |
| `league_name` | String |  |
| `league_abbreviation` | String | Abbreviation for the league or competition the game belongs to. |
| `competition_id` | String | ESPN numeric identifier for the primary competition (game) in the header. |
| `competition_date` | String | Date and time of the competition as recorded in the game header. |
| `neutral_site` | Boolean |  |
| `status_name` | String |  |
| `status_description` | String |  |
| `is_final` | Boolean | Boolean flag indicating whether the game has reached a final or completed status. |

**lineups**

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `home_away` | String |  |
| `athlete` | String | Reference or identifier string for the athlete associated with this row in the box score. |
| `athlete_id` | String |  |
| `position` | String |  |
| `starter` | Boolean |  |
| `jersey` | String |  |
| `formation_place` | String | Starting formation position or role assigned to the player within the team lineup for the match. |
| `subbed_in` | Boolean | Indicates whether the player entered the game as a substitute, or the minute of substitution. |
| `subbed_out` | Boolean | Indicates whether the player was substituted out of the game, or the minute of substitution. |

**key_events**

| col_name | type | description |
|---|---|---|
| `id` | String |  |
| `type` | String |  |
| `type_id` | String |  |
| `type_slug` | String |  |
| `text` | String |  |
| `short_text` | String |  |
| `clock` | String |  |
| `clock_value` | Float64 |  |
| `period` | Int64 |  |
| `team_id` | String |  |
| `team_name` | String | Full display name of the team. |
| `scoring_play` | Boolean |  |
| `field_position_x` | Float64 | Distance of the event from the goal line the event's team attacks, as a fraction of half the pitch (penalty spot = 0.23); 0 together with a 0 y means ESPN recorded no location. |
| `field_position_y` | Float64 | Position of the event across the pitch from 0 to 1, where values below 0.5 are the attacking team's left; 0 together with a 0 x means ESPN recorded no location. |
| `field_position2_x` | Float64 | Distance of the event's end location from the goal line the team attacks, as a fraction of half the pitch, in the same frame as field_position_x; 0 when not recorded. |
| `field_position2_y` | Float64 | Position of the event's end location across the pitch from 0 to 1, in the same frame as field_position_y; 0 when not recorded. |
| `goal_position_x` | Float64 | Horizontal position at which a shot crossed the goal frame; 0 when not recorded. |
| `goal_position_y` | Float64 | Vertical position at which a shot crossed the goal frame; 0 when not recorded. |
| `source_id` | String |  |
| `source_description` | String |  |
| `athlete_id` | String |  |
| `athlete_name` | String |  |
| `wallclock` | String |  |

**team_stats**

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `team_name` | String | Full display name of the team. |
| `team_abbreviation` | String |  |
| `home_away` | String |  |
| `fouls_committed` | String | Total number of fouls committed by the team during the match. |
| `yellow_cards` | String | Total number of yellow cards received by the team during the match. |
| `red_cards` | String | Total number of red cards received by the team during the match. |
| `offsides` | String | Total number of offside calls against the team during the match. |
| `won_corners` | String | Total number of corner kicks earned by the team during the match. |
| `saves` | String |  |
| `possession_pct` | String | Percentage of total ball possession time held by the team during the match. |
| `total_shots` | String | Total number of shot attempts taken by the team during the match. |
| `shots_on_target` | String | Number of the team's shot attempts that were on target, requiring a save or resulting in a goal. |
| `shot_pct` | String | Percentage of the team's shot attempts that were on target during the match. |
| `penalty_kick_goals` | String | Number of goals scored by the team from penalty kicks during the match. |
| `penalty_kick_shots` | String | Number of penalty kick attempts taken by the team during the match. |
| `accurate_passes` | String | Number of passes completed successfully by the team during the match. |
| `total_passes` | String | Total number of passes attempted by the team during the match. |
| `pass_pct` | String | Percentage of passes attempted by the team that were completed successfully. |
| `accurate_crosses` | String | Number of crosses into the opponent's area completed successfully by the team. |
| `total_crosses` | String | Total number of crossing attempts made by the team during the match. |
| `cross_pct` | String | Percentage of the team's crossing attempts that were accurate during the match. |
| `total_long_balls` | String | Total number of long-ball passes attempted by the team during the match. |
| `accurate_long_balls` | String | Number of long-ball passes completed successfully by the team during the match. |
| `longball_pct` | String | Percentage of long-ball attempts by the team that were accurate during the match. |
| `blocked_shots` | String |  |
| `effective_tackles` | String | Number of tackles by the team that successfully dispossessed the opposing player. |
| `total_tackles` | String | Total tackles recorded by the player, including both solo and assisted tackles. |
| `tackle_pct` | String | Percentage of the team's tackle attempts that were effective in winning possession. |
| `interceptions` | String |  |
| `effective_clearance` | String | Number of defensive clearances that successfully removed the ball from the danger area for the team. |
| `total_clearance` | String | Total number of defensive clearances made by the team during the match. |

**commentary**

| col_name | type | description |
|---|---|---|
| `sequence` | Int64 |  |
| `time_display` | String | Human-readable game clock time at which a match event occurred. |
| `time_value` | Float64 | Numeric game clock value at which a match event occurred. |
| `text` | String |  |
| `play_id` | String |  |
| `play_type` | String |  |
| `play_type_id` | String |  |
| `play_type_slug` | String | ESPN's lowercase slug for the play type of the linked play, e.g. foul or goal. |
| `play_text` | String |  |
| `play_short_text` | String | ESPN's short label for the play linked to this commentary item, e.g. a player name followed by the event type. |
| `period` | Int64 |  |
| `clock` | String |  |
| `clock_value` | Float64 |  |
| `scoring_play` | Boolean |  |
| `team_name` | String | Full display name of the team. |
| `athlete_id` | String |  |
| `athlete_name` | String |  |
| `field_position_x` | Float64 | Distance of the event from the goal line the event's team attacks, as a fraction of half the pitch (penalty spot = 0.23); 0 together with a 0 y means ESPN recorded no location. |
| `field_position_y` | Float64 | Position of the event across the pitch from 0 to 1, where values below 0.5 are the attacking team's left; 0 together with a 0 x means ESPN recorded no location. |
| `field_position2_x` | Float64 | Distance of the event's end location from the goal line the team attacks, as a fraction of half the pitch, in the same frame as field_position_x; 0 when not recorded. |
| `field_position2_y` | Float64 | Position of the event's end location across the pitch from 0 to 1, in the same frame as field_position_y; 0 when not recorded. |
| `goal_position_x` | Float64 | Horizontal position at which a shot crossed the goal frame; 0 when not recorded. |
| `goal_position_y` | Float64 | Vertical position at which a shot crossed the goal frame; 0 when not recorded. |
| `wallclock` | String |  |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `team_name` | String | Full display name of the team. |
| `category` | String |  |
| `category_slug` | String | Machine-readable slug identifying the statistical category the player entry belongs to in the box score. |
| `athlete_id` | String |  |
| `athlete` | String | Reference or identifier string for the athlete associated with this row in the box score. |
| `athlete_position` | String |  |
| `value` | String |  |
| `main_stat_label` | String | Human-readable label for the primary statistic displayed for the player in the box score. |
| `main_stat_value` | String | Value of the primary statistic displayed for the player in the box score. |
| `summary` | String |  |

**standings**

| col_name | type | description |
|---|---|---|
| `group` | String |  |
| `team` | String |  |
| `team_id` | String |  |
| `team_uid` | String |  |
| `games_played` | Float64 |  |
| `losses` | Float64 |  |
| `point_differential` | Float64 |  |
| `points` | Float64 |  |
| `ties` | Float64 |  |
| `wins` | Float64 |  |
| `rank` | Float64 |  |
| `overall` | String |  |

**head_to_head**

| col_name | type | description |
|---|---|---|
| `event_id` | String |  |
| `game_date` | String |  |
| `at_vs` | String |  |
| `score` | String |  |
| `home_team_id` | String |  |
| `away_team_id` | String |  |
| `home_team_score` | String |  |
| `away_team_score` | String |  |
| `home_aggregate_score` | String | Aggregate score for the home team across both legs of the two-legged tie. |
| `away_aggregate_score` | String | Aggregate score for the away team across both legs of the two-legged tie. |
| `home_shootout_score` | String | Number of penalties scored by the home team in a shootout to decide the tie. |
| `away_shootout_score` | String | Number of penalties scored by the away team in a shootout to decide the tie. |
| `game_result` | String |  |
| `match_note` | String | Optional editorial note or special circumstance annotation attached to this match in the series. |
| `competition_name` | String | Full name of the competition or tournament to which this series match belongs. |
| `round_name` | String | Name or label for the round or stage of the competition in which this series match is played. |
| `league_name` | String |  |
| `league_abbreviation` | String | Abbreviation for the league or competition the game belongs to. |
| `opponent` | Int64 |  |
| `perspective_team_id` | String | ESPN identifier for the team whose perspective is used to orient the series or match result display. |

**last_five**

| col_name | type | description |
|---|---|---|
| `team_id` | String |  |
| `team_name` | String | Full display name of the team. |
| `display_order` | Int64 | Numeric order in which the team or entry is displayed within the box score table. |
| `event_id` | String |  |
| `game_date` | String |  |
| `at_vs` | String |  |
| `score` | String |  |
| `home_team_id` | String |  |
| `away_team_id` | String |  |
| `home_team_score` | String |  |
| `away_team_score` | String |  |
| `game_result` | String |  |
| `competition_name` | String | Full name of the competition or tournament to which this series match belongs. |
| `league_name` | String |  |
| `league_abbreviation` | String | Abbreviation for the league or competition the game belongs to. |
| `opponent` | Int64 |  |

**game_info**

| col_name | type | description |
|---|---|---|
| `venue_id` | String |  |
| `venue_full_name` | String |  |
| `venue_short_name` | String | Abbreviated or shortened display name for the venue where the game was played. |
| `venue_city` | String |  |
| `venue_country` | String | Country name for the venue where the game was played. |
| `attendance` | Int64 |  |
| `officials` | String |  |

**shootout**

| col_name | type | description |
|---|---|---|
| `team_entry_id` | String | ESPN identifier for the team entry associated with this shootout or penalty record. |
| `team_name` | String | Full display name of the team. |
| `shot_id` | String | ESPN identifier for an individual shootout or penalty kick attempt. |
| `player_id` | String |  |
| `player` | String |  |
| `shot_number` | Int64 | Sequential number of the penalty kick or shootout attempt within the series. |
| `did_score` | Boolean | Boolean flag indicating whether the penalty kick or shootout attempt resulted in a goal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_summary-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_summary-example}

```python
espn_wc_summary()
```

## espn_wc_calendar

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/calendar](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wc_calendar-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_calendar-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_calendar-example}

```python
espn_wc_calendar()
```

## espn_wc_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wc_news-returns}

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

### Raises {#espn_wc_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_news-example}

```python
espn_wc_news()
```

## espn_wc_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/injuries](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wc_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_injuries-example}

```python
espn_wc_injuries()
```

## espn_wc_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/transactions?limit=500](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/transactions?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wc_transactions-returns}

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

### Raises {#espn_wc_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_transactions-example}

```python
espn_wc_transactions()
```

## espn_wc_conferences

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/groups](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wc_conferences-returns}

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

### Raises {#espn_wc_conferences-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_conferences-example}

```python
espn_wc_conferences()
```

## espn_wc_statistics_league

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/statistics](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wc_statistics_league-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: league, season, status, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_statistics_league-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_statistics_league-example}

```python
espn_wc_statistics_league()
```

## espn_wc_draft

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/draft](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wc_draft-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, nhl, wnba, 2026-10-07); its rows sit under keys it does not read (top level: breakingNews, broadcasts, displayName, picks, positions, rounds, shortDisplayName, status).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_draft-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_draft-example}

```python
espn_wc_draft()
```

## espn_wc_teams_site

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams?limit=1000](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wc_teams_site-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `team_id` | String | ESPN numeric identifier for the team. |
| `display_name` | String | Full display name of the team (e.g. 'Los Angeles Lakers'). |
| `abbreviation` | String | Team abbreviation. |
| `location` | String | Team location/city. |
| `name` | String | Short team name, typically the mascot (e.g. 'Lakers'). |
| `short_display_name` | String | Short team display name. |
| `nickname` | String | Alternative nickname used by ESPN for the team. |
| `slug` | String | URL slug for the team. |
| `uid` | String | ESPN universal id for the team. |
| `color` | String | Primary team color (hex). |
| `alternate_color` | String | Secondary team color (hex). |
| `is_active` | Boolean | Whether the team is currently active. |
| `is_all_star` | Boolean | Whether the team is an all-star side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_teams_site-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_teams_site-example}

```python
espn_wc_teams_site()
```

## espn_wc_team

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team-returns}

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

### Raises {#espn_wc_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team-example}

```python
espn_wc_team(team_id='4')
```

## espn_wc_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/roster?limit=500](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/roster?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wc_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `athlete_id` | String | ESPN numeric identifier for the athlete. |
| `uid` | String | ESPN universal id for the athlete. |
| `first_name` | String | Athlete's first (given) name. |
| `last_name` | String | Athlete's last (family) name. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `short_name` | String | Athlete's abbreviated display name (e.g. 'L. James'). |
| `jersey` | String | Athlete's jersey number as a string. |
| `age` | Int64 | Athlete age in years. |
| `date_of_birth` | String | Athlete date of birth (ISO 8601). |
| `height` | Float64 | Athlete height in inches. |
| `display_height` | String | Athlete height, formatted for display. |
| `weight` | Float64 | Athlete weight in pounds. |
| `display_weight` | String | Athlete weight, formatted for display. |
| `position` | String | Position abbreviation. |
| `position_name` | String | Full position name (e.g. 'Point Guard', 'Goalkeeper'). |
| `birth_city` | String | Athlete birth city. |
| `birth_country` | String | Athlete birth country. |
| `citizenship` | String | Athlete citizenship. |
| `gender` | String | Athlete gender. |
| `slug` | String | URL slug for the athlete. |
| `status` | String | Roster status (e.g. Active). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_roster-example}

```python
espn_wc_team_roster(team_id='4')
```

## espn_wc_team_schedule

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/schedule)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |

### Returns {#espn_wc_team_schedule-returns}

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

### Raises {#espn_wc_team_schedule-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_schedule-example}

```python
espn_wc_team_schedule(team_id='4')
```

## espn_wc_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team_record-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_record-example}

```python
espn_wc_team_record(team_id='4')
```

## espn_wc_team_depthcharts

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team_depthcharts-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb, 2026-10-07); its rows sit under keys it does not read (top level: depthchart, season, status, team, timestamp).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_depthcharts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_depthcharts-example}

```python
espn_wc_team_depthcharts(team_id='4')
```

## espn_wc_team_injuries

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/injuries)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team_injuries-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `id` | Int64 | ESPN numeric identifier for the athlete. |
| `display_name` | String | Athlete's full display name as shown on ESPN. |
| `injuries` | String | Injury entries for the athlete (list of dicts, stringified): status, type, details, dates. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_injuries-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_injuries-example}

```python
espn_wc_team_injuries(team_id='4')
```

## espn_wc_team_transactions

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team_transactions-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_transactions-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_transactions-example}

```python
espn_wc_team_transactions(team_id='4')
```

## espn_wc_team_history

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team_history-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, mbb, wbb; 400 in cfb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_history-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_history-example}

```python
espn_wc_team_history(team_id='4')
```

## espn_wc_team_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/news?limit=50](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/news?limit=50)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wc_team_news-returns}

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

### Raises {#espn_wc_team_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_news-example}

```python
espn_wc_team_news(team_id='4')
```

## espn_wc_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wc_team_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: ESPN answers an empty object in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_team_leaders-example}

```python
espn_wc_team_leaders(team_id='4')
```

## espn_wc_player_info

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wc_player_info-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_player_info-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_player_info-example}

```python
espn_wc_player_info(athlete_id='4239')
```

## espn_wc_player_bio

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wc_player_bio-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_player_bio-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_player_bio-example}

```python
espn_wc_player_bio(athlete_id='4239')
```

## espn_wc_player_news

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/athletes/4239/news)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns {#espn_wc_player_news-returns}

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

### Raises {#espn_wc_player_news-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_player_news-example}

```python
espn_wc_player_news(athlete_id='4239')
```

## espn_wc_standings

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/soccer/fifa.world/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/soccer/fifa.world/standings](https://site.api.espn.com/apis/v2/sports/soccer/fifa.world/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns {#espn_wc_standings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `group` | String | Conference/group/table the row belongs to, flattened from the standings children hierarchy. |
| `team` | String | Display name of the team in this standings row. |
| `team_id` | String | ESPN numeric identifier for the team. |
| `team_abbreviation` | String | Team abbreviation. |
| `note` | String | Standings note (e.g. qualification/relegation marker). |
| `games_played` | Float64 | Matches played. |
| `losses` | Float64 | Number of matches the team has lost. |
| `point_differential` | Float64 | Goal difference (for minus against). |
| `points` | Float64 | Competition points. |
| `points_against` | Float64 | Goals conceded. |
| `points_for` | Float64 | Goals (or runs) scored by the team. |
| `ties` | Float64 | Number of matches the team has drawn. |
| `wins` | Float64 | Number of matches the team has won. |
| `advanced` | Float64 | Whether the team has advanced/qualified. |
| `deductions` | Float64 | Points deducted. |
| `ppg` | Float64 | Points per game. |
| `rank` | Float64 | Position within the group/table. |
| `rank_change` | Float64 | Change in rank versus the previous update. |
| `overall` | String | Overall record summary as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_wc_standings-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_wc_standings-example}

```python
espn_wc_standings()
```
