---
title: SOCCER — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
description: "SOCCER — ESPN site API (v2) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 20
---
# SOCCER — ESPN site API (v2)

`sportsdataverse.soccer` — 24 endpoints.

## `espn_soccer_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard?dates=20240115)

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
| `event_id` | character | ESPN event id for the match. |
| `date` | character | Match start timestamp (ISO 8601, UTC). |
| `name` | character | Full event name (e.g. 'Team A at Team B'). |
| `short_name` | character | Abbreviated event name (e.g. 'TA @ TB'). |
| `home_team` | character | Home team display name. |
| `home_team_id` | character | Home team ESPN id. |
| `home_score` | character | Home team's score. For cricket, the innings string (e.g. '161/5 (18/20 ov, target 156)'). |
| `away_team` | character | Away team display name. |
| `away_team_id` | character | Away team ESPN id. |
| `away_score` | character | Away team's score. For cricket, the innings string. |
| `status` | character | Status type name (e.g. STATUS_FINAL, STATUS_SCHEDULED, STATUS_IN_PROGRESS). |
| `venue` | character | Full name of the venue where the match was played. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_soccer_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event` | `event_id` |  |  | `Y` | event query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**header**

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `uid` | character | ESPN UID string. |
| `time_valid` | logical | Whether the start time is confirmed. |
| `season_year` | integer | Season year string ('YYYY-YY' format). |
| `season_type` | integer | ESPN season-phase numeric code (1=pre-season, 2=regular season, 3=postseason, 4=off-season). |
| `season_slug` | character | Season slug. |
| `league_id` | character | ESPN numeric identifier for the league or competition. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | Abbreviation for the league or competition the game belongs to. |
| `competition_id` | character | ESPN numeric identifier for the primary competition (game) in the header. |
| `competition_date` | character | Date and time of the competition as recorded in the game header. |
| `neutral_site` | logical | Neutral site. |
| `status_name` | character | Status label. |
| `status_description` | character | Roster status description (e.g. 'Active'). |
| `is_final` | logical | Boolean flag indicating whether the game has reached a final or completed status. |

**lineups**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `athlete` | character | Reference or identifier string for the athlete associated with this row in the box score. |
| `athlete_id` | character | Unique athlete identifier (ESPN). |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `starter` | logical | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `jersey` | character | Jersey number worn by the player. |
| `formation_place` | character | Starting formation position or role assigned to the player within the team lineup for the match. |
| `subbed_in` | logical | Indicates whether the player entered the game as a substitute, or the minute of substitution. |
| `subbed_out` | logical | Indicates whether the player was substituted out of the game, or the minute of substitution. |

**key_events**

| col_name | type | description |
|---|---|---|
| `id` | character | ID of the player in the 'name' column. |
| `type` | character | Record type / category. |
| `type_id` | character | Type identifier (numeric). |
| `type_slug` | character | Broadcast-type slug (e.g. `streaming`, `tv`). |
| `text` | character | Text description of the play / record. |
| `short_text` | character | Short play description text. |
| `clock` | character | Game clock value. |
| `clock_value` | double | Clock value in seconds. |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full display name of the team. |
| `scoring_play` | logical | TRUE if the play resulted in points scored. |
| `athlete_id` | character | Unique athlete identifier (ESPN). |
| `athlete_name` | character | Athlete display name (ESPN). |
| `wallclock` | character | Wallclock. |

**team_stats**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full display name of the team. |
| `team_abbreviation` | character | Short team abbreviation (e.g. 'LAS'). |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `fouls_committed` | character | Total number of fouls committed by the team during the match. |
| `yellow_cards` | character | Total number of yellow cards received by the team during the match. |
| `red_cards` | character | Total number of red cards received by the team during the match. |
| `offsides` | character | Total number of offside calls against the team during the match. |
| `won_corners` | character | Total number of corner kicks earned by the team during the match. |
| `saves` | character | Saves made. |
| `possession_pct` | character | Percentage of total ball possession time held by the team during the match. |
| `total_shots` | character | Total number of shot attempts taken by the team during the match. |
| `shots_on_target` | character | Number of the team's shot attempts that were on target, requiring a save or resulting in a goal. |
| `shot_pct` | character | Percentage of the team's shot attempts that were on target during the match. |
| `penalty_kick_goals` | character | Number of goals scored by the team from penalty kicks during the match. |
| `penalty_kick_shots` | character | Number of penalty kick attempts taken by the team during the match. |
| `accurate_passes` | character | Number of passes completed successfully by the team during the match. |
| `total_passes` | character | Total number of passes attempted by the team during the match. |
| `pass_pct` | character | Percentage of passes attempted by the team that were completed successfully. |
| `accurate_crosses` | character | Number of crosses into the opponent's area completed successfully by the team. |
| `total_crosses` | character | Total number of crossing attempts made by the team during the match. |
| `cross_pct` | character | Percentage of the team's crossing attempts that were accurate during the match. |
| `total_long_balls` | character | Total number of long-ball passes attempted by the team during the match. |
| `accurate_long_balls` | character | Number of long-ball passes completed successfully by the team during the match. |
| `longball_pct` | character | Percentage of long-ball attempts by the team that were accurate during the match. |
| `blocked_shots` | character | Blocked shots. |
| `effective_tackles` | character | Number of tackles by the team that successfully dispossessed the opposing player. |
| `total_tackles` | character | Total tackles recorded by the player, including both solo and assisted tackles. |
| `tackle_pct` | character | Percentage of the team's tackle attempts that were effective in winning possession. |
| `interceptions` | character | The number of interceptions thrown. |
| `effective_clearance` | character | Number of defensive clearances that successfully removed the ball from the danger area for the team. |
| `total_clearance` | character | Total number of defensive clearances made by the team during the match. |

**commentary**

| col_name | type | description |
|---|---|---|
| `sequence` | integer | Sequence order of the season row. |
| `time_display` | character | Human-readable game clock time at which a match event occurred. |
| `time_value` | double | Numeric game clock value at which a match event occurred. |
| `text` | character | Text description of the play / record. |

**leaders**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full display name of the team. |
| `category` | character | Category label. |
| `category_slug` | character | Machine-readable slug identifying the statistical category the player entry belongs to in the box score. |
| `athlete_id` | character | Unique athlete identifier (ESPN). |
| `athlete` | character | Reference or identifier string for the athlete associated with this row in the box score. |
| `athlete_position` | character | Player position name; `athlete_detail = TRUE` only. |
| `value` | character | Numeric or string value field. |
| `main_stat_label` | character | Human-readable label for the primary statistic displayed for the player in the box score. |
| `main_stat_value` | character | Value of the primary statistic displayed for the player in the box score. |
| `summary` | character | Record summary string (e.g. "25-15-10"). |

**standings**

| col_name | type | description |
|---|---|---|
| `group` | character | Stat group (e.g. "hitting", "pitching", "fielding"). |
| `team` | character | Team-side label or team identifier. |
| `team_id` | character | Unique team identifier. |
| `team_uid` | character | ESPN universal team identifier (UID format 's:40~l:...~t:...'). |
| `games_played` | double | Games played. |
| `losses` | double | Total losses. |
| `point_differential` | double | Point differential. |
| `points` | double | Points scored. |
| `ties` | double | Number of ties in the series. |
| `wins` | double | Total wins. |
| `rank` | double | Position of the school within the poll for the given week (1 = top-ranked). |
| `overall` | character | Overall pick number. |

**head_to_head**

| col_name | type | description |
|---|---|---|
| `event_id` | character | Unique event / game identifier (ESPN). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `at_vs` | character | "at" or "vs" home/away indicator. |
| `score` | character | Final score string. |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `home_team_score` | character | Home team final score. |
| `away_team_score` | character | Away team final score. |
| `home_aggregate_score` | character | Aggregate score for the home team across both legs of the two-legged tie. |
| `away_aggregate_score` | character | Aggregate score for the away team across both legs of the two-legged tie. |
| `home_shootout_score` | character | Number of penalties scored by the home team in a shootout to decide the tie. |
| `away_shootout_score` | character | Number of penalties scored by the away team in a shootout to decide the tie. |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `match_note` | character | Optional editorial note or special circumstance annotation attached to this match in the series. |
| `competition_name` | character | Full name of the competition or tournament to which this series match belongs. |
| `round_name` | character | Name or label for the round or stage of the competition in which this series match is played. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | Abbreviation for the league or competition the game belongs to. |
| `opponent` | integer | Opposing team of player |
| `perspective_team_id` | character | ESPN identifier for the team whose perspective is used to orient the series or match result display. |

**last_five**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `team_name` | character | Full display name of the team. |
| `display_order` | integer | Numeric order in which the team or entry is displayed within the box score table. |
| `event_id` | character | Unique event / game identifier (ESPN). |
| `game_date` | character | Game date (YYYY-MM-DD). |
| `at_vs` | character | "at" or "vs" home/away indicator. |
| `score` | character | Final score string. |
| `home_team_id` | character | Unique identifier for the home team. |
| `away_team_id` | character | Unique identifier for the away team. |
| `home_team_score` | character | Home team final score. |
| `away_team_score` | character | Away team final score. |
| `game_result` | character | Game result for the player's team (`W`/`L`). |
| `competition_name` | character | Full name of the competition or tournament to which this series match belongs. |
| `league_name` | character | League name. |
| `league_abbreviation` | character | Abbreviation for the league or competition the game belongs to. |
| `opponent` | integer | Opposing team of player |

**game_info**

| col_name | type | description |
|---|---|---|
| `venue_id` | character | Unique venue identifier. |
| `venue_full_name` | character | Venue full name. |
| `venue_short_name` | character | Abbreviated or shortened display name for the venue where the game was played. |
| `venue_city` | character | Venue city. |
| `venue_country` | character | Country name for the venue where the game was played. |
| `attendance` | integer | Reported attendance. |
| `officials` | character | Whether officials data is available. |

**shootout**

| col_name | type | description |
|---|---|---|
| `team_entry_id` | character | ESPN identifier for the team entry associated with this shootout or penalty record. |
| `team_name` | character | Full display name of the team. |
| `shot_id` | character | ESPN identifier for an individual shootout or penalty kick attempt. |
| `player_id` | character | Unique player identifier. |
| `player` | character | Player name. |
| `shot_number` | integer | Sequential number of the penalty kick or shootout attempt within the series. |
| `did_score` | logical | Boolean flag indicating whether the penalty kick or shootout attempt resulted in a goal. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_summary()
```

_Last validated n/a._

## `espn_soccer_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/calendar](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_calendar()
```

_Last validated n/a._

## `espn_soccer_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/news](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/news)

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
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_news()
```

_Last validated n/a._

## `espn_soccer_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/injuries](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/injuries)

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
espn_soccer_injuries()
```

_Last validated n/a._

## `espn_soccer_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/transactions](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_transactions()
```

_Last validated n/a._

## `espn_soccer_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/groups](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_groups`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_conferences()
```

_Last validated n/a._

## `espn_soccer_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/statistics](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_statistics_league()
```

_Last validated n/a._

## `espn_soccer_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/draft](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_draft()
```

_Last validated n/a._

## `espn_soccer_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `team_id` | character | ESPN numeric identifier for the team. |
| `display_name` | character | Full display name of the team (e.g. 'Los Angeles Lakers'). |
| `abbreviation` | character | Team abbreviation. |
| `location` | character | Team location/city. |
| `name` | character | Short team name, typically the mascot (e.g. 'Lakers'). |
| `short_display_name` | character | Short team display name. |
| `nickname` | character | Alternative nickname used by ESPN for the team. |
| `slug` | character | URL slug for the team. |
| `uid` | character | ESPN universal id for the team. |
| `color` | character | Primary team color (hex). |
| `alternate_color` | character | Secondary team color (hex). |
| `is_active` | logical | Whether the team is currently active. |
| `is_all_star` | logical | Whether the team is an all-star side. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_teams_site()
```

_Last validated n/a._

## `espn_soccer_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `athlete_id` | character | ESPN numeric identifier for the athlete. |
| `uid` | character | ESPN universal id for the athlete. |
| `first_name` | character | Athlete's first (given) name. |
| `last_name` | character | Athlete's last (family) name. |
| `display_name` | character | Athlete's full display name as shown on ESPN. |
| `short_name` | character | Athlete's abbreviated display name (e.g. 'L. James'). |
| `jersey` | character | Athlete's jersey number as a string. |
| `age` | integer | Athlete age in years. |
| `date_of_birth` | character | Athlete date of birth (ISO 8601). |
| `height` | double | Athlete height in inches. |
| `display_height` | character | Athlete height, formatted for display. |
| `weight` | double | Athlete weight in pounds. |
| `display_weight` | character | Athlete weight, formatted for display. |
| `position` | character | Position abbreviation. |
| `position_name` | character | Full position name (e.g. 'Point Guard', 'Goalkeeper'). |
| `birth_city` | character | Athlete birth city. |
| `birth_country` | character | Athlete birth country. |
| `citizenship` | character | Athlete citizenship. |
| `gender` | character | Athlete gender. |
| `slug` | character | URL slug for the athlete. |
| `status` | character | Roster status (e.g. Active). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/schedule)

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_record(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/injuries)

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
espn_soccer_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_history(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/news)

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
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_news(team_id='4')
```

_Last validated n/a._

## `espn_soccer_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_soccer_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_soccer_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_soccer_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/athletes/4239/news)

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
| `byline` | character | Author byline string as published by ESPN. |
| `links_web_href` | character | Web article URL. |
| `links_mobile_href` | character | Mobile article URL. |
| `links_api_self_href` | character | ESPN API canonical self-link for the article resource. |
| `links_app_sportscenter_href` | character | SportsCenter app deep link. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_soccer_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/soccer/eng.1/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/soccer/eng.1/standings](https://site.api.espn.com/apis/v2/sports/soccer/eng.1/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `season` | `season` |  |  | `Y` | Season year (e.g. 2024). |
| `group` | `group` |  |  | `Y` | Conference or group id filter (e.g. an ESPN conference id). |
| `type` | `standings_type` |  |  | `Y` | Standings variant (e.g. 'by-division' or 'by-conference'). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `group` | character | Conference/group/table the row belongs to, flattened from the standings children hierarchy. |
| `team` | character | Display name of the team in this standings row. |
| `team_id` | character | ESPN numeric identifier for the team. |
| `team_abbreviation` | character | Team abbreviation. |
| `note` | character | Standings note (e.g. qualification/relegation marker). |
| `games_played` | double | Matches played. |
| `losses` | double | Number of matches the team has lost. |
| `point_differential` | double | Goal difference (for minus against). |
| `points` | double | Competition points. |
| `points_against` | double | Goals conceded. |
| `points_for` | double | Goals (or runs) scored by the team. |
| `ties` | double | Number of matches the team has drawn. |
| `wins` | double | Number of matches the team has won. |
| `advanced` | double | Whether the team has advanced/qualified. |
| `deductions` | double | Points deducted. |
| `ppg` | double | Points per game. |
| `rank` | double | Position within the group/table. |
| `rank_change` | double | Change in rank versus the previous update. |
| `overall` | character | Overall record summary as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_soccer_standings()
```

_Last validated n/a._
