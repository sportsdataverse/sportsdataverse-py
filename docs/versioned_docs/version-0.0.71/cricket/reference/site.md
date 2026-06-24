---
title: CRICKET — ESPN site API (v2)
sidebar_label: ESPN site API (v2)
sidebar_position: 20
---
# CRICKET — ESPN site API (v2)

`sportsdataverse.cricket` — 24 endpoints.

## `espn_cricket_scoreboard`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/scoreboard`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/scoreboard?dates=20240115](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/scoreboard?dates=20240115)

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
| `status_detail` | character | Human-readable status detail (e.g. 'Final', the over/innings summary). |
| `venue` | character | Full name of the venue where the match was played. |
| `neutral_site` | logical | Whether the match is played at a neutral venue. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_scoreboard(dates='20240115')
```

_Last validated n/a._

## `espn_cricket_summary`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/summary`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/summary](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/summary)

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
| `is_final` | character | Boolean flag indicating whether the game has reached a final or completed status. |

**matchcards_batting**

| col_name | type | description |
|---|---|---|
| `innings_number` | character | Innings number within the match to which this batting or bowling entry belongs. |
| `team_name` | character | Full display name of the team. |
| `total` | character | Total. |
| `runs_total` | character | Total runs scored by the team or batting side in this innings. |
| `extras` | character | Additional runs awarded to the batting side from wides, no-balls, byes, or leg byes in this innings. |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `dismissal` | character | Method by which the batter was dismissed in this innings (e.g., caught, bowled, run out). |
| `runs` | character | Runs scored. |
| `balls_faced` | character | Total number of balls faced by the batter during their innings. |
| `fours` | character | Number of boundaries hit for four runs by the batter in this innings. |
| `sixes` | character | Number of boundaries hit for six runs by the batter in this innings. |

**matchcards_bowling**

| col_name | type | description |
|---|---|---|
| `innings_number` | character | Innings number within the match to which this batting or bowling entry belongs. |
| `team_name` | character | Full display name of the team. |
| `player_id` | character | Unique player identifier. |
| `player_name` | character | Player name. |
| `overs` | character | Number of overs bowled by the bowler in this innings. |
| `maidens` | character | Number of maiden overs bowled by the bowler in this innings, in which no runs were conceded. |
| `conceded` | character | Total runs conceded by the bowler during their spell in this innings. |
| `wickets` | character | Number of wickets taken by the bowler in this innings. |
| `economy_rate` | character | Average runs conceded per over by the bowler in this innings. |
| `nbw` | character | Combined no-balls and wides bowled by the bowler in this innings. |

**matchcards_partnerships**

| col_name | type | description |
|---|---|---|
| `innings_number` | character | Innings number within the match to which this batting or bowling entry belongs. |
| `team_name` | character | Full display name of the team. |
| `partnership_runs` | character | Total runs scored during the batting partnership for this fall-of-wicket record. |
| `partnership_overs` | character | Number of overs faced during the batting partnership for this fall-of-wicket record. |
| `wicket_name` | character | Name or label identifying the wicket at which this batting partnership ended. |
| `fow_type` | character | Type classification for this fall-of-wicket entry (e.g., caught, bowled, run out). |
| `player1_name` | character | Name of the first batter in the batting partnership for this fall-of-wicket entry. |
| `player1_runs` | character | Runs contributed by the first player in the batting partnership. |
| `player2_name` | character | Name of the second batter in the batting partnership for this fall-of-wicket entry. |
| `player2_runs` | character | Runs contributed by the second player in the batting partnership. |

**rosters**

| col_name | type | description |
|---|---|---|
| `team_id` | character | Unique team identifier. |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `winner` | logical | Winner. |
| `athlete_id` | character | Unique athlete identifier (ESPN). |
| `athlete` | character | Reference or identifier string for the athlete associated with this row in the box score. |
| `jersey` | character | Jersey number worn by the player. |
| `starter` | logical | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `position` | character | Listed roster position (G, F, C, etc.). |
| `captain` | logical | Indicates whether the player was designated as a team captain for this game. |

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

**standings**

| col_name | type | description |
|---|---|---|
| `group` | character | Group identifier (e.g. conference 'group_id'). |
| `team` | character | Team-side label or team identifier. |
| `team_id` | character | Unique team identifier. |
| `rank` | integer | Position of the school within the poll for the given week (1 = top-ranked). |
| `matches_played` | integer | Total number of matches played by the team in the current stage or competition group. |
| `matches_won` | integer | Total number of matches won by the team in the current stage or competition group. |
| `matches_lost` | integer | Total number of matches lost by the team in the current stage or competition group. |
| `noresult` | integer | Number of matches that ended without a result (e.g., rain-affected or abandoned) for the team. |
| `match_points` | integer | Total competition points accumulated by the team based on match outcomes in the group or stage. |
| `qualified` | integer | Boolean qualification flag indicating whether the team has secured advancement from the current group or stage. |
| `netrr` | double | Net Run Rate for the team, a tiebreaker metric used in cricket group standings. |
| `for` | double | Total runs or score accumulated by the team across all matches in the group or stage. |
| `against` | double | Total runs or score conceded by the team across all matches in the group or stage. |
| `total` | character | Total. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_summary()
```

_Last validated n/a._

## `espn_cricket_calendar`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/calendar`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/calendar](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/calendar)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_calendar()
```

_Last validated n/a._

## `espn_cricket_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/news](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/news)

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
espn_cricket_news()
```

_Last validated n/a._

## `espn_cricket_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/injuries](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/injuries)

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
espn_cricket_injuries()
```

_Last validated n/a._

## `espn_cricket_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/transactions](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_transactions()
```

_Last validated n/a._

## `espn_cricket_conferences`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/groups`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/groups](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/groups)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_groups`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_conferences()
```

_Last validated n/a._

## `espn_cricket_statistics_league`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/statistics`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/statistics](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_statistics_league()
```

_Last validated n/a._

## `espn_cricket_draft`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/draft`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/draft](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/draft)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_draft()
```

_Last validated n/a._

## `espn_cricket_teams_site`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams)

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
espn_cricket_teams_site()
```

_Last validated n/a._

## `espn_cricket_team`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_roster`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/roster`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/roster](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team_roster(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_schedule`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/schedule`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/schedule](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/schedule)

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
espn_cricket_team_schedule(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_record`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/record`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/record](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team_record(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_depthcharts`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/depthcharts`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/depthcharts](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/depthcharts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team_depthcharts(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_injuries`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/injuries`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/injuries](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/injuries)

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
espn_cricket_team_injuries(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_transactions`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/transactions`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/transactions](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/transactions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team_transactions(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_history`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/history`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/history](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/history)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team_history(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/news](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/news)

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
espn_cricket_team_news(team_id='4')
```

_Last validated n/a._

## `espn_cricket_team_leaders`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/{team_id}/leaders`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/leaders](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/teams/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_team_leaders(team_id='4')
```

_Last validated n/a._

## `espn_cricket_player_info`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/{athlete_id}`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/4239](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/4239)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_player_info(athlete_id='4239')
```

_Last validated n/a._

## `espn_cricket_player_bio`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/{athlete_id}/bio`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/4239/bio](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/4239/bio)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `athlete_id` | `athlete_id` |  | `Y` |  | athlete_id path parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_player_bio(athlete_id='4239')
```

_Last validated n/a._

## `espn_cricket_player_news`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/{athlete_id}/news`

**Valid URL:** [https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/4239/news](https://site.api.espn.com/apis/site/v2/sports/cricket/eng.1/athletes/4239/news)

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
espn_cricket_player_news(athlete_id='4239')
```

_Last validated n/a._

## `espn_cricket_standings`

ESPN endpoint.

**Endpoint URL:** `GET https://site.api.espn.com/apis/v2/sports/cricket/eng.1/standings`

**Valid URL:** [https://site.api.espn.com/apis/v2/sports/cricket/eng.1/standings](https://site.api.espn.com/apis/v2/sports/cricket/eng.1/standings)

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
| `rank` | integer | Position within the group/table. |
| `matches_played` | integer | Matches played (cricket). |
| `matches_won` | integer | Matches won (cricket). |
| `matches_lost` | integer | Matches lost (cricket). |
| `noresult` | integer | Matches with no result (cricket). |
| `match_points` | integer | Competition points (cricket). |
| `qualified` | integer | Qualification flag (cricket). |
| `netrr` | double | Net run rate (cricket). |
| `for` | double | Runs/goals for. |
| `against` | double | Runs/goals against. |
| `total` | character | Aggregate/summary value as published by ESPN. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
espn_cricket_standings()
```

_Last validated n/a._
