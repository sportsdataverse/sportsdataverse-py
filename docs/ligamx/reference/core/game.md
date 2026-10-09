# LIGAMX — ESPN core API (v2) — Game

> LIGAMX — ESPN core API (v2) — Game — function reference in sdv-py, the SportsDataverse Python package.

## espn_ligamx_game_competition

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `attendance` | integer |  |
| `boxscore_available` | logical |  |
| `bracket_available` | logical |  |
| `commentary_available` | logical |  |
| `competitors` | character |  |
| `conference_competition` | logical |  |
| `conversation_available` | logical |  |
| `date` | character |  |
| `division_competition` | logical |  |
| `gamecast_available` | logical |  |
| `guid` | character |  |
| `highlights_available` | logical |  |
| `id` | character |  |
| `lineup_available` | logical |  |
| `links` | character |  |
| `live_available` | logical |  |
| `necessary` | logical |  |
| `neutral_site` | logical |  |
| `notes` | character |  |
| `on_watch_espn` | logical |  |
| `pickcenter_available` | logical |  |
| `play_by_play_available` | logical |  |
| `possession_arrow_available` | logical |  |
| `preview_available` | logical |  |
| `recap_available` | logical |  |
| `recent` | logical |  |
| `series` | character |  |
| `shot_chart_available` | logical |  |
| `summary_available` | logical |  |
| `tickets_available` | logical |  |
| `time_valid` | logical |  |
| `timeouts_available` | logical |  |
| `uid` | character |  |
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
| `format_regulation_periods` | integer |  |
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
| `type_abbreviation` | character |  |
| `type_id` | character |  |
| `type_slug` | character |  |
| `type_text` | character |  |
| `type_type` | character |  |
| `venue_$ref` | character |  |
| `venue_address_city` | character |  |
| `venue_address_state` | character |  |
| `venue_full_name` | character |  |
| `venue_grass` | logical |  |
| `venue_guid` | character |  |
| `venue_id` | character |  |
| `venue_images` | character |  |
| `venue_indoor` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_competition-example}

```python
espn_ligamx_game_competition(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `home_away` | character |  |
| `id` | character |  |
| `order` | integer |  |
| `type` | character |  |
| `uid` | character |  |
| `winner` | logical |  |
| `leaders_$ref` | character |  |
| `linescores_$ref` | character |  |
| `record_$ref` | character |  |
| `roster_$ref` | character |  |
| `score_$ref` | character |  |
| `statistics_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_teams-example}

```python
espn_ligamx_game_teams(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `home_away` | character |  |
| `id` | character |  |
| `order` | integer |  |
| `type` | character |  |
| `uid` | character |  |
| `winner` | logical |  |
| `leaders_$ref` | character |  |
| `linescores_$ref` | character |  |
| `record_$ref` | character |  |
| `roster_$ref` | character |  |
| `score_$ref` | character |  |
| `statistics_$ref` | character |  |
| `team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_team-example}

```python
espn_ligamx_game_team(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_ligamx_game_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/roster](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `active` | logical |  |
| `starter` | logical |  |
| `did_not_play` | logical |  |
| `ejected` | logical |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_team_roster-example}

```python
espn_ligamx_game_team_roster(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_ligamx_game_team_linescores

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/linescores](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/linescores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_team_linescores-returns}

**`return_parsed=True`** (default) — the output of `parse_event_competitor_linescores`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_team_linescores-example}

```python
espn_ligamx_game_team_linescores(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_ligamx_game_team_statistics

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/statistics](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_team_statistics-returns}

**`return_parsed=True`** (default) — the output of `parse_event_competitor_statistics`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_event_competitor_statistics raises AttributeError: 'str' object has no attribute 'get' on the live payload (nba, 2026-10-07), so no columns can be derived.

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_team_statistics-example}

```python
espn_ligamx_game_team_statistics(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_ligamx_game_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/record](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_team_record-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_team_record-example}

```python
espn_ligamx_game_team_record(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_ligamx_game_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/competitors/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_team_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_team_leaders-example}

```python
espn_ligamx_game_team_leaders(event_id='401584793', team_id='4')
```

_Last validated n/a._

## espn_ligamx_game_odds

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/odds](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `details` | character |  |
| `moneyline_winner` | logical |  |
| `over_odds` | double |  |
| `over_under` | double |  |
| `spread` | double |  |
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
| `away_team_odds_favorite` | logical |  |
| `away_team_odds_money_line` | integer |  |
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
| `away_team_odds_spread_odds` | double |  |
| `away_team_odds_team_$ref` | character |  |
| `away_team_odds_underdog` | logical |  |
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
| `home_team_odds_favorite` | logical |  |
| `home_team_odds_money_line` | integer |  |
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
| `home_team_odds_spread_odds` | double |  |
| `home_team_odds_team_$ref` | character |  |
| `home_team_odds_underdog` | logical |  |
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
| `provider_id` | character |  |
| `provider_name` | character |  |
| `provider_priority` | integer |  |
| `prop_bets_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_odds-example}

```python
espn_ligamx_game_odds(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_probabilities

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/probabilities?limit=300](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/probabilities?limit=300)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_ligamx_game_probabilities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `away_win_percentage` | double |  |
| `home_win_percentage` | double |  |
| `last_modified` | character |  |
| `sequence_number` | character |  |
| `spread_cover_prob_home` | double |  |
| `spread_push_prob` | double |  |
| `tie_percentage` | double |  |
| `total_over_prob` | double |  |
| `away_team_$ref` | character |  |
| `competition_$ref` | character |  |
| `home_team_$ref` | character |  |
| `play_$ref` | character |  |
| `source_description` | character |  |
| `source_id` | character |  |
| `source_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_probabilities-example}

```python
espn_ligamx_game_probabilities(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_plays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/plays?limit=1000](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/plays?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_ligamx_game_plays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `alternative_text` | character |  |
| `away_score` | integer |  |
| `clock_display_value` | character |  |
| `clock_value` | double |  |
| `coordinate_x` | integer |  |
| `coordinate_y` | integer |  |
| `home_score` | integer |  |
| `id` | character |  |
| `modified` | character |  |
| `period_display_value` | character |  |
| `period_number` | integer |  |
| `points_attempted` | integer |  |
| `priority` | logical |  |
| `probability_$ref` | character |  |
| `score_value` | integer |  |
| `scoring_play` | logical |  |
| `sequence_number` | character |  |
| `shooting_play` | logical |  |
| `team_$ref` | character |  |
| `text` | character |  |
| `type_id` | character |  |
| `type_text` | character |  |
| `valid` | logical |  |
| `wallclock` | character |  |
| `short_alternative_text` | character |  |
| `short_text` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_plays-example}

```python
espn_ligamx_game_plays(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_play

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/plays/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/plays/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `alternative_text` | character |  |
| `away_score` | integer |  |
| `home_score` | integer |  |
| `id` | character |  |
| `modified` | character |  |
| `participants` | character |  |
| `points_attempted` | integer |  |
| `priority` | logical |  |
| `score_value` | integer |  |
| `scoring_play` | logical |  |
| `sequence_number` | character |  |
| `shooting_play` | logical |  |
| `text` | character |  |
| `valid` | logical |  |
| `wallclock` | character |  |
| `clock_display_value` | character |  |
| `clock_value` | double |  |
| `coordinate_x` | integer |  |
| `coordinate_y` | integer |  |
| `period_display_value` | character |  |
| `period_number` | integer |  |
| `probability_$ref` | character |  |
| `team_$ref` | character |  |
| `type_id` | character |  |
| `type_text` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_play-example}

```python
espn_ligamx_game_play(event_id='401584793', play_id='1')
```

_Last validated n/a._

## espn_ligamx_game_play_personnel

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/plays/1/personnel](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/plays/1/personnel)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_play_personnel-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, 2026-10-07); its rows sit under keys it does not read (top level: $ref, playPersonnel).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_play_personnel-example}

```python
espn_ligamx_game_play_personnel(event_id='401584793', play_id='1')
```

_Last validated n/a._

## espn_ligamx_game_situation

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/situation](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/situation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_situation-returns}

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

### Example {#espn_ligamx_game_situation-example}

```python
espn_ligamx_game_situation(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_status

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/status](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `clock` | double |  |
| `display_clock` | character |  |
| `period` | integer |  |
| `type_completed` | logical |  |
| `type_description` | character |  |
| `type_detail` | character |  |
| `type_id` | character |  |
| `type_name` | character |  |
| `type_short_detail` | character |  |
| `type_state` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_status-example}

```python
espn_ligamx_game_status(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_officials

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/officials](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/officials)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_officials-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character |  |
| `first_name` | character |  |
| `full_name` | character |  |
| `id` | character |  |
| `last_name` | character |  |
| `order` | integer |  |
| `position_display_name` | character |  |
| `position_id` | character |  |
| `position_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_officials-example}

```python
espn_ligamx_game_officials(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_broadcasts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/broadcasts](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/broadcasts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_broadcasts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `channel` | integer |  |
| `lang` | character |  |
| `partnered` | logical |  |
| `priority` | integer |  |
| `region` | character |  |
| `slug` | character |  |
| `station` | character |  |
| `competition_$ref` | character |  |
| `market_id` | character |  |
| `market_type` | character |  |
| `media_$ref` | character |  |
| `media_call_letters` | character |  |
| `media_id` | character |  |
| `media_logos` | character |  |
| `media_name` | character |  |
| `media_short_name` | character |  |
| `media_slug` | character |  |
| `type_id` | character |  |
| `type_long_name` | character |  |
| `type_short_name` | character |  |
| `type_slug` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_broadcasts-example}

```python
espn_ligamx_game_broadcasts(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_predictor

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/predictor](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/predictor)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_predictor-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `last_modified` | character |  |
| `name` | character |  |
| `short_name` | character |  |
| `away_team_statistics` | character |  |
| `away_team_team_$ref` | character |  |
| `home_team_team_$ref` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_predictor-example}

```python
espn_ligamx_game_predictor(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/powerindex](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | integer |  |
| `items` | character |  |
| `page_count` | integer |  |
| `page_index` | integer |  |
| `page_size` | integer |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_powerindex-example}

```python
espn_ligamx_game_powerindex(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_propbets

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/propbets](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/propbets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_propbets-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_propbets-example}

```python
espn_ligamx_game_propbets(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nfl, mlb, nhl, wnba, cfb, mbb, wbb; 400 in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_leaders-example}

```python
espn_ligamx_game_leaders(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_scoringplays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/scoringplays](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/scoringplays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_scoringplays-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_scoringplays-example}

```python
espn_ligamx_game_scoringplays(event_id='401584793')
```

_Last validated n/a._

## espn_ligamx_game_official_detail

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/officials/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/mex.1/events/401584793/competitions/401584793/officials/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `official_id` | `official_id` |  | `Y` |  | official_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_ligamx_game_official_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | character |  |
| `display_name` | character |  |
| `first_name` | character |  |
| `full_name` | character |  |
| `id` | character |  |
| `last_name` | character |  |
| `order` | integer |  |
| `position_display_name` | character |  |
| `position_id` | character |  |
| `position_name` | character |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_ligamx_game_official_detail-example}

```python
espn_ligamx_game_official_detail(event_id='401584793', official_id='1')
```

_Last validated n/a._
