---
title: "NBA — ESPN core API (v2) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "NBA — ESPN core API (v2) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NBA — ESPN core API (v2) — Game

## espn_nba_game_competition

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `attendance` | integer | Reported attendance. |
| `boxscore_available` | logical |  |
| `bracket_available` | logical |  |
| `commentary_available` | logical |  |
| `competitors` | character |  |
| `conference_competition` | logical | Conference competition. |
| `conversation_available` | logical |  |
| `date` | character | Date in YYYY-MM-DD format. |
| `division_competition` | logical |  |
| `gamecast_available` | logical |  |
| `guid` | character | Stable cross-league team GUID. |
| `highlights_available` | logical |  |
| `id` | character | Id. |
| `lineup_available` | logical |  |
| `links` | character |  |
| `live_available` | logical |  |
| `necessary` | logical |  |
| `neutral_site` | logical | Neutral site. |
| `notes` | character | Free-form notes attached to the record. |
| `on_watch_espn` | logical |  |
| `pickcenter_available` | logical |  |
| `play_by_play_available` | logical |  |
| `possession_arrow_available` | logical |  |
| `preview_available` | logical |  |
| `recap_available` | logical |  |
| `recent` | logical | Recent. |
| `series` | character |  |
| `shot_chart_available` | logical |  |
| `summary_available` | logical |  |
| `tickets_available` | logical |  |
| `time_valid` | logical | Time valid. |
| `timeouts_available` | logical |  |
| `uid` | character | ESPN UID string. |
| `wallclock_available` | logical |  |
| `boxscore_source_description` | character |  |
| `boxscore_source_id` | character |  |
| `boxscore_source_state` | character |  |
| `broadcasts_$ref` | character |  |
| `details_$ref` | character |  |
| `format_overtime_clock` | double |  |
| `format_overtime_display_name` | character |  |
| `format_overtime_slug` | character |  |
| `format_regulation_clock` | double |  |
| `format_regulation_display_name` | character |  |
| `format_regulation_periods` | integer | Format regulation periods. |
| `format_regulation_slug` | character |  |
| `game_source_description` | character |  |
| `game_source_id` | character |  |
| `game_source_state` | character |  |
| `linescore_source_description` | character |  |
| `linescore_source_id` | character |  |
| `linescore_source_state` | character |  |
| `odds_$ref` | character |  |
| `officials_$ref` | character |  |
| `play_by_play_source_description` | character |  |
| `play_by_play_source_id` | character |  |
| `play_by_play_source_state` | character |  |
| `power_indexes_$ref` | character |  |
| `predictor_$ref` | character |  |
| `probabilities_$ref` | character |  |
| `relevancy_$ref` | character |  |
| `situation_$ref` | character |  |
| `stats_source_description` | character |  |
| `stats_source_id` | character |  |
| `stats_source_state` | character |  |
| `status_$ref` | character |  |
| `type_abbreviation` | character | Type abbreviation. |
| `type_id` | character | Type identifier (numeric). |
| `type_slug` | character |  |
| `type_text` | character | Display text for the type field. |
| `type_type` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character | Venue address city. |
| `venue_address_state` | character | Venue address state / region. |
| `venue_full_name` | character | Venue full name. |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character | Unique venue identifier. |
| `venue_images` | character |  |
| `venue_indoor` | logical | TRUE if the venue is indoors. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_competition-example}

```python
espn_nba_game_competition(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `id` | character | Id. |
| `order` | integer | Display order within the result set. |
| `type` | character | Record type / category. |
| `uid` | character | ESPN UID string. |
| `winner` | logical | Winner. |
| `leaders_$ref` | character |  |
| `linescores_$ref` | character |  |
| `record_$ref` | character |  |
| `roster_$ref` | character |  |
| `score_$ref` | character |  |
| `statistics_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_teams-example}

```python
espn_nba_game_teams(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `home_away` | character | Game venue label ('home' or 'away'). |
| `id` | character | Id. |
| `order` | integer | Display order within the result set. |
| `type` | character | Record type / category. |
| `uid` | character | ESPN UID string. |
| `winner` | logical | Winner. |
| `leaders_$ref` | character |  |
| `linescores_$ref` | character |  |
| `record_$ref` | character |  |
| `roster_$ref` | character |  |
| `score_$ref` | character |  |
| `statistics_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_team-example}

```python
espn_nba_game_team(event_id='401584793', team_id='15')
```

_Last validated n/a._

## espn_nba_game_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15/roster](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `active` | logical | TRUE if the row represents an active record (player / team / season). |
| `starter` | logical | TRUE if the player was in the starting lineup; FALSE otherwise. |
| `did_not_play` | logical | TRUE if the player did not appear in the game. |
| `ejected` | logical | TRUE if the player was ejected from the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_team_roster-example}

```python
espn_nba_game_team_roster(event_id='401584793', team_id='15')
```

_Last validated n/a._

## espn_nba_game_team_linescores

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/4/linescores](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/4/linescores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_team_linescores-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_linescores`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_team_linescores-example}

```python
espn_nba_game_team_linescores(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_nba_game_team_statistics

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15/statistics](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/15/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_event_competitor_statistics`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_team_statistics-example}

```python
espn_nba_game_team_statistics(event_id='401584793', team_id='15')
```

_Last validated n/a._

## espn_nba_game_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/4/record](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_team_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_team_record-example}

```python
espn_nba_game_team_record(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_nba_game_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/4/leaders](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/competitors/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_team_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_team_leaders-example}

```python
espn_nba_game_team_leaders(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_nba_game_odds

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/odds](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `details` | character | Details. |
| `moneyline_winner` | logical |  |
| `over_odds` | double |  |
| `over_under` | double | Over under. |
| `spread` | double | Spread. |
| `spread_winner` | logical |  |
| `under_odds` | double |  |
| `away_team_odds_close_money_line_alternate_display_value` | character |  |
| `away_team_odds_close_money_line_american` | character |  |
| `away_team_odds_close_money_line_decimal` | double |  |
| `away_team_odds_close_money_line_display_value` | character |  |
| `away_team_odds_close_money_line_fraction` | character |  |
| `away_team_odds_close_money_line_value` | double |  |
| `away_team_odds_close_point_spread_alternate_display_value` | character |  |
| `away_team_odds_close_point_spread_american` | character |  |
| `away_team_odds_close_spread_alternate_display_value` | character |  |
| `away_team_odds_close_spread_american` | character |  |
| `away_team_odds_close_spread_decimal` | double |  |
| `away_team_odds_close_spread_display_value` | character |  |
| `away_team_odds_close_spread_fraction` | character |  |
| `away_team_odds_close_spread_value` | double |  |
| `away_team_odds_current_money_line_alternate_display_value` | character |  |
| `away_team_odds_current_money_line_american` | character |  |
| `away_team_odds_current_money_line_decimal` | double |  |
| `away_team_odds_current_money_line_display_value` | character |  |
| `away_team_odds_current_money_line_fraction` | character |  |
| `away_team_odds_current_money_line_value` | double |  |
| `away_team_odds_current_point_spread_alternate_display_value` | character |  |
| `away_team_odds_current_point_spread_american` | character |  |
| `away_team_odds_current_spread_alternate_display_value` | character |  |
| `away_team_odds_current_spread_american` | character |  |
| `away_team_odds_current_spread_decimal` | double |  |
| `away_team_odds_current_spread_display_value` | character |  |
| `away_team_odds_current_spread_fraction` | character |  |
| `away_team_odds_current_spread_value` | double |  |
| `away_team_odds_favorite` | logical | Away team's team odds favorite. |
| `away_team_odds_money_line` | integer | Away team's team odds money line. |
| `away_team_odds_open_favorite` | logical |  |
| `away_team_odds_open_money_line_alternate_display_value` | character |  |
| `away_team_odds_open_money_line_american` | character |  |
| `away_team_odds_open_money_line_decimal` | double |  |
| `away_team_odds_open_money_line_display_value` | character |  |
| `away_team_odds_open_money_line_fraction` | character |  |
| `away_team_odds_open_money_line_value` | double |  |
| `away_team_odds_open_point_spread_alternate_display_value` | character |  |
| `away_team_odds_open_point_spread_american` | character |  |
| `away_team_odds_open_spread_alternate_display_value` | character |  |
| `away_team_odds_open_spread_american` | character |  |
| `away_team_odds_open_spread_decimal` | double |  |
| `away_team_odds_open_spread_display_value` | character |  |
| `away_team_odds_open_spread_fraction` | character |  |
| `away_team_odds_open_spread_value` | double |  |
| `away_team_odds_spread_odds` | double | Away team's team odds spread odds. |
| `away_team_odds_team_$ref` | character |  |
| `away_team_odds_underdog` | logical | Away team's team odds underdog. |
| `close_over_alternate_display_value` | character |  |
| `close_over_american` | character |  |
| `close_over_decimal` | double |  |
| `close_over_display_value` | character |  |
| `close_over_fraction` | character |  |
| `close_over_value` | double |  |
| `close_total_alternate_display_value` | character |  |
| `close_total_american` | character |  |
| `close_total_decimal` | double |  |
| `close_total_display_value` | character |  |
| `close_total_fraction` | character |  |
| `close_total_value` | double |  |
| `close_under_alternate_display_value` | character |  |
| `close_under_american` | character |  |
| `close_under_decimal` | double |  |
| `close_under_display_value` | character |  |
| `close_under_fraction` | character |  |
| `close_under_value` | double |  |
| `current_over_alternate_display_value` | character |  |
| `current_over_american` | character |  |
| `current_over_decimal` | double |  |
| `current_over_display_value` | character |  |
| `current_over_fraction` | character |  |
| `current_over_value` | double |  |
| `current_total_alternate_display_value` | character |  |
| `current_total_american` | character |  |
| `current_total_decimal` | double |  |
| `current_total_display_value` | character |  |
| `current_total_fraction` | character |  |
| `current_total_value` | double |  |
| `current_under_alternate_display_value` | character |  |
| `current_under_american` | character |  |
| `current_under_decimal` | double |  |
| `current_under_display_value` | character |  |
| `current_under_fraction` | character |  |
| `current_under_value` | double |  |
| `home_team_odds_close_money_line_alternate_display_value` | character |  |
| `home_team_odds_close_money_line_american` | character |  |
| `home_team_odds_close_money_line_decimal` | double |  |
| `home_team_odds_close_money_line_display_value` | character |  |
| `home_team_odds_close_money_line_fraction` | character |  |
| `home_team_odds_close_money_line_value` | double |  |
| `home_team_odds_close_point_spread_alternate_display_value` | character |  |
| `home_team_odds_close_point_spread_american` | character |  |
| `home_team_odds_close_point_spread_decimal` | double |  |
| `home_team_odds_close_point_spread_display_value` | character |  |
| `home_team_odds_close_point_spread_fraction` | character |  |
| `home_team_odds_close_point_spread_value` | double |  |
| `home_team_odds_close_spread_alternate_display_value` | character |  |
| `home_team_odds_close_spread_american` | character |  |
| `home_team_odds_close_spread_decimal` | double |  |
| `home_team_odds_close_spread_display_value` | character |  |
| `home_team_odds_close_spread_fraction` | character |  |
| `home_team_odds_close_spread_value` | double |  |
| `home_team_odds_current_money_line_alternate_display_value` | character |  |
| `home_team_odds_current_money_line_american` | character |  |
| `home_team_odds_current_money_line_decimal` | double |  |
| `home_team_odds_current_money_line_display_value` | character |  |
| `home_team_odds_current_money_line_fraction` | character |  |
| `home_team_odds_current_money_line_value` | double |  |
| `home_team_odds_current_point_spread_alternate_display_value` | character |  |
| `home_team_odds_current_point_spread_american` | character |  |
| `home_team_odds_current_point_spread_decimal` | double |  |
| `home_team_odds_current_point_spread_display_value` | character |  |
| `home_team_odds_current_point_spread_fraction` | character |  |
| `home_team_odds_current_point_spread_value` | double |  |
| `home_team_odds_current_spread_alternate_display_value` | character |  |
| `home_team_odds_current_spread_american` | character |  |
| `home_team_odds_current_spread_decimal` | double |  |
| `home_team_odds_current_spread_display_value` | character |  |
| `home_team_odds_current_spread_fraction` | character |  |
| `home_team_odds_current_spread_value` | double |  |
| `home_team_odds_favorite` | logical | Home team's team odds favorite. |
| `home_team_odds_money_line` | integer | Home team's team odds money line. |
| `home_team_odds_open_favorite` | logical |  |
| `home_team_odds_open_money_line_alternate_display_value` | character |  |
| `home_team_odds_open_money_line_american` | character |  |
| `home_team_odds_open_money_line_decimal` | double |  |
| `home_team_odds_open_money_line_display_value` | character |  |
| `home_team_odds_open_money_line_fraction` | character |  |
| `home_team_odds_open_money_line_value` | double |  |
| `home_team_odds_open_point_spread_alternate_display_value` | character |  |
| `home_team_odds_open_point_spread_american` | character |  |
| `home_team_odds_open_point_spread_decimal` | double |  |
| `home_team_odds_open_point_spread_display_value` | character |  |
| `home_team_odds_open_point_spread_fraction` | character |  |
| `home_team_odds_open_point_spread_value` | double |  |
| `home_team_odds_open_spread_alternate_display_value` | character |  |
| `home_team_odds_open_spread_american` | character |  |
| `home_team_odds_open_spread_decimal` | double |  |
| `home_team_odds_open_spread_display_value` | character |  |
| `home_team_odds_open_spread_fraction` | character |  |
| `home_team_odds_open_spread_value` | double |  |
| `home_team_odds_spread_odds` | double | Home team's team odds spread odds. |
| `home_team_odds_team_$ref` | character |  |
| `home_team_odds_underdog` | logical | Home team's team odds underdog. |
| `open_over_alternate_display_value` | character |  |
| `open_over_american` | character |  |
| `open_over_decimal` | double |  |
| `open_over_display_value` | character |  |
| `open_over_fraction` | character |  |
| `open_over_value` | double |  |
| `open_total_alternate_display_value` | character |  |
| `open_total_american` | character |  |
| `open_total_decimal` | double |  |
| `open_total_display_value` | character |  |
| `open_total_fraction` | character |  |
| `open_total_value` | double |  |
| `open_under_alternate_display_value` | character |  |
| `open_under_american` | character |  |
| `open_under_decimal` | double |  |
| `open_under_display_value` | character |  |
| `open_under_fraction` | character |  |
| `open_under_value` | double |  |
| `provider_$ref` | character |  |
| `provider_id` | character | Unique identifier for provider. |
| `provider_name` | character | Provider name. |
| `provider_priority` | integer | Provider priority. |
| `prop_bets_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_odds-example}

```python
espn_nba_game_odds(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_probabilities

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/probabilities?limit=300](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/probabilities?limit=300)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_game_probabilities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `away_win_percentage` | double | Away win percentage (0-1 decimal). |
| `home_win_percentage` | double | Home win percentage (0-1 decimal). |
| `last_modified` | character |  |
| `sequence_number` | character | Sequence number representing a shot-possession (V3 PBP). |
| `spread_cover_prob_home` | double |  |
| `spread_push_prob` | double |  |
| `tie_percentage` | double | Tie percentage (0-1 decimal). |
| `total_over_prob` | double |  |
| `away_team_$ref` | character |  |
| `competition_$ref` | character |  |
| `home_team_$ref` | character |  |
| `play_$ref` | character |  |
| `source_description` | character |  |
| `source_id` | character | Source (ESPN) conference id. |
| `source_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_probabilities-example}

```python
espn_nba_game_probabilities(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_plays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays?limit=1000](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nba_game_plays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `alternative_text` | character |  |
| `away_score` | integer | Away team score at the time of the play. |
| `clock_display_value` | character | Game clock display string (e.g. '8:32'). |
| `clock_value` | double |  |
| `coordinate_x` | integer | X coordinate on the court (half-court layout). |
| `coordinate_y` | integer | Y coordinate on the court (half-court layout). |
| `home_score` | integer | Home team score at the time of the play. |
| `id` | character | Id. |
| `modified` | character | Modified. |
| `period_display_value` | character | Period display label (e.g. '1st Quarter', 'OT'). |
| `period_number` | integer | Numeric period (1-4 for quarters; 5+ for OT). |
| `points_attempted` | integer |  |
| `priority` | logical |  |
| `probability_$ref` | character |  |
| `score_value` | integer | Point value of the play (2 / 3 / 1). |
| `scoring_play` | logical | TRUE if the play resulted in points scored. |
| `sequence_number` | character | Sequence number representing a shot-possession (V3 PBP). |
| `shooting_play` | logical | TRUE if the play was a shooting attempt. |
| `team_$ref` | character |  |
| `text` | character | Text description of the play / record. |
| `type_id` | character | Type identifier (numeric). |
| `type_text` | character | Display text for the type field. |
| `valid` | logical | Valid. |
| `wallclock` | character | Wallclock. |
| `short_alternative_text` | character |  |
| `short_text` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_plays-example}

```python
espn_nba_game_plays(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_play

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays/4015847934](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays/4015847934)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `alternative_text` | character |  |
| `away_score` | integer | Away team score at the time of the play. |
| `home_score` | integer | Home team score at the time of the play. |
| `id` | character | Id. |
| `modified` | character | Modified. |
| `participants` | character | Participants. |
| `points_attempted` | integer |  |
| `priority` | logical |  |
| `score_value` | integer | Point value of the play (2 / 3 / 1). |
| `scoring_play` | logical | TRUE if the play resulted in points scored. |
| `sequence_number` | character | Sequence number representing a shot-possession (V3 PBP). |
| `shooting_play` | logical | TRUE if the play was a shooting attempt. |
| `text` | character | Text description of the play / record. |
| `valid` | logical | Valid. |
| `wallclock` | character | Wallclock. |
| `clock_display_value` | character | Game clock display string (e.g. '8:32'). |
| `clock_value` | double |  |
| `coordinate_x` | integer | X coordinate on the court (half-court layout). |
| `coordinate_y` | integer | Y coordinate on the court (half-court layout). |
| `period_display_value` | character | Period display label (e.g. '1st Quarter', 'OT'). |
| `period_number` | integer | Numeric period (1-4 for quarters; 5+ for OT). |
| `probability_$ref` | character |  |
| `team_$ref` | character |  |
| `type_id` | character | Type identifier (numeric). |
| `type_text` | character | Display text for the type field. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_play-example}

```python
espn_nba_game_play(event_id='401584793', play_id='4015847934')
```

_Last validated n/a._

## espn_nba_game_play_personnel

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays/4015847934/personnel](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/plays/4015847934/personnel)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_play_personnel-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_play_personnel-example}

```python
espn_nba_game_play_personnel(event_id='401584793', play_id='4015847934')
```

_Last validated n/a._

## espn_nba_game_situation

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/situation](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/situation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_situation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `away_fouls_bonus_state` | character |  |
| `away_fouls_fouls_to_give` | integer |  |
| `away_fouls_team_fouls` | integer |  |
| `away_fouls_team_fouls_current` | integer |  |
| `away_timeouts_timeouts_current` | integer |  |
| `away_timeouts_timeouts_remaining_current` | integer |  |
| `home_fouls_bonus_state` | character |  |
| `home_fouls_fouls_to_give` | integer |  |
| `home_fouls_team_fouls` | integer |  |
| `home_fouls_team_fouls_current` | integer |  |
| `home_timeouts_timeouts_current` | integer |  |
| `home_timeouts_timeouts_remaining_current` | integer |  |
| `last_play_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_situation-example}

```python
espn_nba_game_situation(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_status

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/status](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `clock` | double | Game clock value. |
| `display_clock` | character |  |
| `period` | integer | Period of the game (1-4 quarters; 5+ for OT). |
| `type_completed` | logical |  |
| `type_description` | character |  |
| `type_detail` | character |  |
| `type_id` | character | Type identifier (numeric). |
| `type_name` | character | Type name. |
| `type_short_detail` | character |  |
| `type_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_status-example}

```python
espn_nba_game_status(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_officials

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/officials](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/officials)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_officials-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character | Display name. |
| `first_name` | character | Player's first name. |
| `full_name` | character | Player's full name. |
| `id` | character | Id. |
| `last_name` | character | Player's last name. |
| `order` | integer | Display order within the result set. |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Unique position identifier. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_officials-example}

```python
espn_nba_game_officials(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_broadcasts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/broadcasts](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/broadcasts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_broadcasts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `channel` | integer |  |
| `lang` | character | Lang. |
| `partnered` | logical |  |
| `priority` | integer |  |
| `region` | character | Region label. |
| `slug` | character | URL-safe identifier. |
| `station` | character |  |
| `competition_$ref` | character |  |
| `market_id` | character | ESPN futures-market identifier. |
| `market_type` | character | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `media_$ref` | character |  |
| `media_call_letters` | character |  |
| `media_id` | character | Media identifier (video / image). |
| `media_logos` | character |  |
| `media_name` | character |  |
| `media_short_name` | character |  |
| `media_slug` | character |  |
| `type_id` | character | Type identifier (numeric). |
| `type_long_name` | character | Type long name. |
| `type_short_name` | character | Type short name. |
| `type_slug` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_broadcasts-example}

```python
espn_nba_game_broadcasts(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_predictor

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/predictor](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/predictor)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_predictor-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `last_modified` | character |  |
| `name` | character | Display name. |
| `short_name` | character | Short display name. |
| `away_team_statistics` | character |  |
| `away_team_team_$ref` | character |  |
| `home_team_team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_predictor-example}

```python
espn_nba_game_predictor(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/powerindex](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer | Count of count. |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_powerindex-example}

```python
espn_nba_game_powerindex(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_propbets

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/propbets](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/propbets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_propbets-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_propbets-example}

```python
espn_nba_game_propbets(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/leaders](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_leaders-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_leaders-example}

```python
espn_nba_game_leaders(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_scoringplays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/scoringplays](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/scoringplays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_scoringplays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_scoringplays-example}

```python
espn_nba_game_scoringplays(event_id='401584793')
```

_Last validated n/a._

## espn_nba_game_official_detail

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/officials/1](https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba/events/401584793/competitions/401584793/officials/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `official_id` | `official_id` |  | `Y` |  | official_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_nba_game_official_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character | Display name. |
| `first_name` | character | Player's first name. |
| `full_name` | character | Player's full name. |
| `id` | character | Id. |
| `last_name` | character | Player's last name. |
| `order` | integer | Display order within the result set. |
| `position_display_name` | character | Position display name. |
| `position_id` | character | Unique position identifier. |
| `position_name` | character | Listed roster position ('Guard', 'Forward', 'Center'). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nba_game_official_detail-example}

```python
espn_nba_game_official_detail(event_id='401584793', official_id='1')
```

_Last validated n/a._
