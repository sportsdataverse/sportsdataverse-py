---
title: "MLB — ESPN core API (v2) — Game"
sidebar_label: "Game"
sidebar_position: 1
description: "MLB — ESPN core API (v2) — Game — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — ESPN core API (v2) — Game

## espn_mlb_game_competition

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_competition-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `attendance` | Int64 | Reported attendance (NA on the redesigned page). |
| `boxscore_available` | Boolean |  |
| `bracket_available` | Boolean |  |
| `commentary_available` | Boolean |  |
| `competitors` | String |  |
| `conference_competition` | Boolean | Conference competition. |
| `conversation_available` | Boolean |  |
| `date` | String | Date in YYYY-MM-DD format. |
| `division_competition` | Boolean |  |
| `gamecast_available` | Boolean |  |
| `guid` | String | Stable cross-league team GUID. |
| `highlights_available` | Boolean |  |
| `id` | Int64 | Id. |
| `lineup_available` | Boolean |  |
| `links` | String |  |
| `live_available` | Boolean |  |
| `necessary` | Boolean |  |
| `neutral_site` | Boolean | Neutral-site venue (when not hosted by either team). |
| `notes` | String | Notes. |
| `on_watch_espn` | Boolean |  |
| `pickcenter_available` | Boolean |  |
| `play_by_play_available` | Boolean | TRUE if play-by-play is available. |
| `possession_arrow_available` | Boolean |  |
| `preview_available` | Boolean |  |
| `recap_available` | Boolean |  |
| `recent` | Boolean |  |
| `series` | String |  |
| `shot_chart_available` | Boolean |  |
| `summary_available` | Boolean |  |
| `tickets_available` | Boolean |  |
| `time_valid` | Boolean |  |
| `timeouts_available` | Boolean |  |
| `uid` | String | ESPN UID string. |
| `wallclock_available` | Boolean |  |
| `boxscore_source_description` | String |  |
| `boxscore_source_id` | Int64 |  |
| `boxscore_source_state` | String |  |
| `broadcasts_$ref` | String |  |
| `details_$ref` | String |  |
| `format_overtime_clock` | Float64 |  |
| `format_overtime_display_name` | String |  |
| `format_overtime_slug` | String |  |
| `format_regulation_clock` | Float64 |  |
| `format_regulation_display_name` | String |  |
| `format_regulation_periods` | Int64 |  |
| `format_regulation_slug` | String |  |
| `game_source_description` | String |  |
| `game_source_id` | Int64 |  |
| `game_source_state` | String |  |
| `linescore_source_description` | String |  |
| `linescore_source_id` | Int64 |  |
| `linescore_source_state` | String |  |
| `odds_$ref` | String |  |
| `officials_$ref` | String |  |
| `play_by_play_source_description` | String |  |
| `play_by_play_source_id` | Int64 |  |
| `play_by_play_source_state` | String |  |
| `power_indexes_$ref` | String |  |
| `predictor_$ref` | String |  |
| `probabilities_$ref` | String |  |
| `relevancy_$ref` | String |  |
| `situation_$ref` | String |  |
| `stats_source_description` | String |  |
| `stats_source_id` | Int64 |  |
| `stats_source_state` | String |  |
| `status_$ref` | String |  |
| `type_abbreviation` | String | Play type abbreviation. |
| `type_id` | Int64 | Type id. |
| `type_slug` | String |  |
| `type_text` | String | Type text. |
| `type_type` | String | Play type category. |
| `venue_$ref` | String |  |
| `venue_address_city` | String |  |
| `venue_address_state` | String |  |
| `venue_full_name` | String |  |
| `venue_grass` | Boolean |  |
| `venue_guid` | String |  |
| `venue_id` | Int64 | MLBAM venue ID. |
| `venue_images` | String |  |
| `venue_indoor` | Boolean |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_competition-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_competition-example}

```python
espn_mlb_game_competition(event_id='401584793')
```

## espn_mlb_game_teams

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `home_away` | String | Venue label for the team ('home' or 'away'). |
| `id` | Int64 | Id. |
| `order` | Int64 | Order. |
| `type` | String | Record type / category. |
| `uid` | String | ESPN UID string. |
| `winner` | Boolean | Winner. |
| `leaders_$ref` | String |  |
| `linescores_$ref` | String |  |
| `record_$ref` | String |  |
| `roster_$ref` | String |  |
| `score_$ref` | String |  |
| `statistics_$ref` | String |  |
| `team_$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_teams-example}

```python
espn_mlb_game_teams(event_id='401584793')
```

## espn_mlb_game_team

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `home_away` | String | Venue label for the team ('home' or 'away'). |
| `id` | Int64 | Id. |
| `order` | Int64 | Order. |
| `type` | String | Record type / category. |
| `uid` | String | ESPN UID string. |
| `winner` | Boolean | Winner. |
| `leaders_$ref` | String |  |
| `linescores_$ref` | String |  |
| `record_$ref` | String |  |
| `roster_$ref` | String |  |
| `score_$ref` | String |  |
| `statistics_$ref` | String |  |
| `team_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_team-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_team-example}

```python
espn_mlb_game_team(event_id='401584793', team_id='4')
```

## espn_mlb_game_team_roster

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/roster`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/roster](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/roster)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_roster-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `active` | Boolean | Whether the player is currently active. |
| `starter` | Boolean | TRUE if the player started the game. |
| `did_not_play` | Boolean | Did not play. |
| `ejected` | Boolean | Ejected. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_team_roster-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_team_roster-example}

```python
espn_mlb_game_team_roster(event_id='401584793', team_id='4')
```

## espn_mlb_game_team_linescores

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/linescores`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/linescores](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/linescores)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_linescores-returns}

**`return_parsed=True`** (default) — the output of `parse_event_competitor_linescores`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: the page holds zero items (count 0) in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_team_linescores-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_team_linescores-example}

```python
espn_mlb_game_team_linescores(event_id='401584793', team_id='4')
```

## espn_mlb_game_team_statistics

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/statistics`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/statistics](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_statistics-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `split_name` | String | Name of the stat split (Core v2 answers one, "All Splits"). |
| `category_name` | String | ESPN stat category of the row (e.g., "defensive", "general", "offensive"). |
| `stat_name` | String | ESPN stat key in camelCase (e.g., "blocks", "defensiveRebounds"). |
| `stat_abbreviation` | String | Short label of the stat (e.g., "BLK"). |
| `stat_value` | Float64 | Numeric value of the team's stat in this game. |
| `stat_display_value` | String | The value as ESPN displays it (a string, e.g., "7"). |
| `stat_description` | String | ESPN's one-sentence definition of the stat. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_team_statistics-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_team_statistics-example}

```python
espn_mlb_game_team_statistics(event_id='401584793', team_id='4')
```

## espn_mlb_game_team_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/record`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/record](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_record-returns}

**`return_parsed=True`** (default) — the output of `parse_single_entity`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_team_record-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_team_record-example}

```python
espn_mlb_game_team_record(event_id='401584793', team_id='4')
```

## espn_mlb_game_team_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/competitors/{team_id}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/leaders](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/competitors/4/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_team_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, 2026-10-07); its rows sit under keys it does not read (top level: $ref, abbreviation, categories, id, name, type).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_team_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_team_leaders-example}

```python
espn_mlb_game_team_leaders(event_id='401584793', team_id='4')
```

## espn_mlb_game_odds

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/odds`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/odds](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/odds)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_odds-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `details` | String | Details. |
| `moneyline_winner` | Boolean |  |
| `over_odds` | Float64 |  |
| `over_under` | Float64 | Over under. |
| `spread` | Float64 | Spread. |
| `spread_winner` | Boolean |  |
| `under_odds` | Float64 |  |
| `away_team_odds_close_money_line_alternate_display_value` | String |  |
| `away_team_odds_close_money_line_american` | String |  |
| `away_team_odds_close_money_line_decimal` | Float64 |  |
| `away_team_odds_close_money_line_display_value` | String |  |
| `away_team_odds_close_money_line_fraction` | String |  |
| `away_team_odds_close_money_line_value` | Float64 |  |
| `away_team_odds_close_point_spread_alternate_display_value` | String |  |
| `away_team_odds_close_point_spread_american` | String |  |
| `away_team_odds_close_spread_alternate_display_value` | String |  |
| `away_team_odds_close_spread_american` | String |  |
| `away_team_odds_close_spread_decimal` | Float64 |  |
| `away_team_odds_close_spread_display_value` | String |  |
| `away_team_odds_close_spread_fraction` | String |  |
| `away_team_odds_close_spread_value` | Float64 |  |
| `away_team_odds_current_money_line_alternate_display_value` | String |  |
| `away_team_odds_current_money_line_american` | String |  |
| `away_team_odds_current_money_line_decimal` | Float64 |  |
| `away_team_odds_current_money_line_display_value` | String |  |
| `away_team_odds_current_money_line_fraction` | String |  |
| `away_team_odds_current_money_line_value` | Float64 |  |
| `away_team_odds_current_point_spread_alternate_display_value` | String |  |
| `away_team_odds_current_point_spread_american` | String |  |
| `away_team_odds_current_spread_alternate_display_value` | String |  |
| `away_team_odds_current_spread_american` | String |  |
| `away_team_odds_current_spread_decimal` | Float64 |  |
| `away_team_odds_current_spread_display_value` | String |  |
| `away_team_odds_current_spread_fraction` | String |  |
| `away_team_odds_current_spread_value` | Float64 |  |
| `away_team_odds_favorite` | Boolean | Away team's team odds favorite. |
| `away_team_odds_money_line` | Int64 | Away team's team odds money line. |
| `away_team_odds_open_favorite` | Boolean |  |
| `away_team_odds_open_money_line_alternate_display_value` | String |  |
| `away_team_odds_open_money_line_american` | String |  |
| `away_team_odds_open_money_line_decimal` | Float64 |  |
| `away_team_odds_open_money_line_display_value` | String |  |
| `away_team_odds_open_money_line_fraction` | String |  |
| `away_team_odds_open_money_line_value` | Float64 |  |
| `away_team_odds_open_point_spread_alternate_display_value` | String |  |
| `away_team_odds_open_point_spread_american` | String |  |
| `away_team_odds_open_spread_alternate_display_value` | String |  |
| `away_team_odds_open_spread_american` | String |  |
| `away_team_odds_open_spread_decimal` | Float64 |  |
| `away_team_odds_open_spread_display_value` | String |  |
| `away_team_odds_open_spread_fraction` | String |  |
| `away_team_odds_open_spread_value` | Float64 |  |
| `away_team_odds_spread_odds` | Float64 | Away team's team odds spread odds. |
| `away_team_odds_team_$ref` | String |  |
| `away_team_odds_underdog` | Boolean | Away team's team odds underdog. |
| `close_over_alternate_display_value` | String |  |
| `close_over_american` | String |  |
| `close_over_decimal` | Float64 |  |
| `close_over_display_value` | String |  |
| `close_over_fraction` | String |  |
| `close_over_value` | Float64 |  |
| `close_total_alternate_display_value` | String |  |
| `close_total_american` | String |  |
| `close_total_decimal` | Float64 |  |
| `close_total_display_value` | String |  |
| `close_total_fraction` | String |  |
| `close_total_value` | Float64 |  |
| `close_under_alternate_display_value` | String |  |
| `close_under_american` | String |  |
| `close_under_decimal` | Float64 |  |
| `close_under_display_value` | String |  |
| `close_under_fraction` | String |  |
| `close_under_value` | Float64 |  |
| `current_over_alternate_display_value` | String |  |
| `current_over_american` | String |  |
| `current_over_decimal` | Float64 |  |
| `current_over_display_value` | String |  |
| `current_over_fraction` | String |  |
| `current_over_value` | Float64 |  |
| `current_total_alternate_display_value` | String |  |
| `current_total_american` | String |  |
| `current_total_decimal` | Float64 |  |
| `current_total_display_value` | String |  |
| `current_total_fraction` | String |  |
| `current_total_value` | Float64 |  |
| `current_under_alternate_display_value` | String |  |
| `current_under_american` | String |  |
| `current_under_decimal` | Float64 |  |
| `current_under_display_value` | String |  |
| `current_under_fraction` | String |  |
| `current_under_value` | Float64 |  |
| `home_team_odds_close_money_line_alternate_display_value` | String |  |
| `home_team_odds_close_money_line_american` | String |  |
| `home_team_odds_close_money_line_decimal` | Float64 |  |
| `home_team_odds_close_money_line_display_value` | String |  |
| `home_team_odds_close_money_line_fraction` | String |  |
| `home_team_odds_close_money_line_value` | Float64 |  |
| `home_team_odds_close_point_spread_alternate_display_value` | String |  |
| `home_team_odds_close_point_spread_american` | String |  |
| `home_team_odds_close_point_spread_decimal` | Float64 |  |
| `home_team_odds_close_point_spread_display_value` | String |  |
| `home_team_odds_close_point_spread_fraction` | String |  |
| `home_team_odds_close_point_spread_value` | Float64 |  |
| `home_team_odds_close_spread_alternate_display_value` | String |  |
| `home_team_odds_close_spread_american` | String |  |
| `home_team_odds_close_spread_decimal` | Float64 |  |
| `home_team_odds_close_spread_display_value` | String |  |
| `home_team_odds_close_spread_fraction` | String |  |
| `home_team_odds_close_spread_value` | Float64 |  |
| `home_team_odds_current_money_line_alternate_display_value` | String |  |
| `home_team_odds_current_money_line_american` | String |  |
| `home_team_odds_current_money_line_decimal` | Float64 |  |
| `home_team_odds_current_money_line_display_value` | String |  |
| `home_team_odds_current_money_line_fraction` | String |  |
| `home_team_odds_current_money_line_value` | Float64 |  |
| `home_team_odds_current_point_spread_alternate_display_value` | String |  |
| `home_team_odds_current_point_spread_american` | String |  |
| `home_team_odds_current_point_spread_decimal` | Float64 |  |
| `home_team_odds_current_point_spread_display_value` | String |  |
| `home_team_odds_current_point_spread_fraction` | String |  |
| `home_team_odds_current_point_spread_value` | Float64 |  |
| `home_team_odds_current_spread_alternate_display_value` | String |  |
| `home_team_odds_current_spread_american` | String |  |
| `home_team_odds_current_spread_decimal` | Float64 |  |
| `home_team_odds_current_spread_display_value` | String |  |
| `home_team_odds_current_spread_fraction` | String |  |
| `home_team_odds_current_spread_value` | Float64 |  |
| `home_team_odds_favorite` | Boolean | Home team's team odds favorite. |
| `home_team_odds_money_line` | Int64 | Home team's team odds money line. |
| `home_team_odds_open_favorite` | Boolean |  |
| `home_team_odds_open_money_line_alternate_display_value` | String |  |
| `home_team_odds_open_money_line_american` | String |  |
| `home_team_odds_open_money_line_decimal` | Float64 |  |
| `home_team_odds_open_money_line_display_value` | String |  |
| `home_team_odds_open_money_line_fraction` | String |  |
| `home_team_odds_open_money_line_value` | Float64 |  |
| `home_team_odds_open_point_spread_alternate_display_value` | String |  |
| `home_team_odds_open_point_spread_american` | String |  |
| `home_team_odds_open_point_spread_decimal` | Float64 |  |
| `home_team_odds_open_point_spread_display_value` | String |  |
| `home_team_odds_open_point_spread_fraction` | String |  |
| `home_team_odds_open_point_spread_value` | Float64 |  |
| `home_team_odds_open_spread_alternate_display_value` | String |  |
| `home_team_odds_open_spread_american` | String |  |
| `home_team_odds_open_spread_decimal` | Float64 |  |
| `home_team_odds_open_spread_display_value` | String |  |
| `home_team_odds_open_spread_fraction` | String |  |
| `home_team_odds_open_spread_value` | Float64 |  |
| `home_team_odds_spread_odds` | Float64 | Home team's team odds spread odds. |
| `home_team_odds_team_$ref` | String |  |
| `home_team_odds_underdog` | Boolean | Home team's team odds underdog. |
| `open_over_alternate_display_value` | String |  |
| `open_over_american` | String |  |
| `open_over_decimal` | Float64 |  |
| `open_over_display_value` | String |  |
| `open_over_fraction` | String |  |
| `open_over_value` | Float64 |  |
| `open_total_alternate_display_value` | String |  |
| `open_total_american` | String |  |
| `open_total_decimal` | Float64 |  |
| `open_total_display_value` | String |  |
| `open_total_fraction` | String |  |
| `open_total_value` | Float64 |  |
| `open_under_alternate_display_value` | String |  |
| `open_under_american` | String |  |
| `open_under_decimal` | Float64 |  |
| `open_under_display_value` | String |  |
| `open_under_fraction` | String |  |
| `open_under_value` | Float64 |  |
| `provider_$ref` | String |  |
| `provider_id` | Int64 | Unique identifier for provider. |
| `provider_name` | String | Provider name. |
| `provider_priority` | Int64 | Provider priority. |
| `prop_bets_$ref` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_odds-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_odds-example}

```python
espn_mlb_game_odds(event_id='401584793')
```

## espn_mlb_game_probabilities

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/probabilities`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/probabilities?limit=300](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/probabilities?limit=300)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_game_probabilities-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `away_win_percentage` | Float64 | Away team win probability (0-1 decimal). |
| `home_win_percentage` | Float64 | Home team win probability (0-1 decimal). |
| `last_modified` | String | Last modified. |
| `sequence_number` | String | Sequence number of the play within the game. |
| `spread_cover_prob_home` | Float64 |  |
| `spread_push_prob` | Float64 |  |
| `tie_percentage` | Float64 | Tie probability (0-1 decimal; extra-innings tie state). |
| `total_over_prob` | Float64 |  |
| `away_team_$ref` | String |  |
| `competition_$ref` | String |  |
| `home_team_$ref` | String |  |
| `play_$ref` | String |  |
| `source_description` | String | Source description. |
| `source_id` | Int64 | Source id. |
| `source_state` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_probabilities-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_probabilities-example}

```python
espn_mlb_game_probabilities(event_id='401584793')
```

## espn_mlb_game_plays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/plays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/plays?limit=1000](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/plays?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_game_plays-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `alternative_text` | String |  |
| `away_score` | Int64 | Away team run total after the play. |
| `clock_display_value` | String |  |
| `clock_value` | Float64 |  |
| `coordinate_x` | Int64 | Coordinate x. |
| `coordinate_y` | Int64 | Coordinate y. |
| `home_score` | Int64 | Home team run total after the play. |
| `id` | Int64 | Id. |
| `modified` | String |  |
| `period_display_value` | String | Inning display value (e.g. 'Top 1st'). |
| `period_number` | Int64 | Inning number. |
| `points_attempted` | Int64 |  |
| `priority` | Boolean |  |
| `probability_$ref` | String |  |
| `score_value` | Int64 | Runs scored on the play. |
| `scoring_play` | Boolean | TRUE if the play scored a run. |
| `sequence_number` | String | Sequence number of the play within the game. |
| `shooting_play` | Boolean | Shooting play. |
| `team_$ref` | String |  |
| `text` | String | Text description of the play. |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |
| `valid` | Boolean | Valid. |
| `wallclock` | String | Wall-clock timestamp of the play (ISO 8601). |
| `short_alternative_text` | String |  |
| `short_text` | String | Short text. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_plays-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_plays-example}

```python
espn_mlb_game_plays(event_id='401584793')
```

## espn_mlb_game_play

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/plays/{play_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/plays/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/plays/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_play-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `alternative_text` | String |  |
| `away_score` | Int64 | Away team run total after the play. |
| `home_score` | Int64 | Home team run total after the play. |
| `id` | Int64 | Id. |
| `modified` | String |  |
| `participants` | String | List of athlete participants in the play. |
| `points_attempted` | Int64 |  |
| `priority` | Boolean |  |
| `score_value` | Int64 | Runs scored on the play. |
| `scoring_play` | Boolean | TRUE if the play scored a run. |
| `sequence_number` | String | Sequence number of the play within the game. |
| `shooting_play` | Boolean | Shooting play. |
| `text` | String | Text description of the play. |
| `valid` | Boolean | Valid. |
| `wallclock` | String | Wall-clock timestamp of the play (ISO 8601). |
| `clock_display_value` | String |  |
| `clock_value` | Float64 |  |
| `coordinate_x` | Int64 | Coordinate x. |
| `coordinate_y` | Int64 | Coordinate y. |
| `period_display_value` | String | Inning display value (e.g. 'Top 1st'). |
| `period_number` | Int64 | Inning number. |
| `probability_$ref` | String |  |
| `team_$ref` | String |  |
| `type_id` | Int64 | Type id. |
| `type_text` | String | Type text. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_play-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_play-example}

```python
espn_mlb_game_play(event_id='401584793', play_id='1')
```

## espn_mlb_game_play_personnel

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/plays/{play_id}/personnel`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/plays/1/personnel](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/plays/1/personnel)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `play_id` | `play_id` |  | `Y` |  | play_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_play_personnel-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: parser: parse_items returns no columns on the live payload (nba, 2026-10-07); its rows sit under keys it does not read (top level: $ref, playPersonnel).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_play_personnel-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_play_personnel-example}

```python
espn_mlb_game_play_personnel(event_id='401584793', play_id='1')
```

## espn_mlb_game_situation

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/situation`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/situation](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/situation)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_situation-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `away_fouls_bonus_state` | String |  |
| `away_fouls_fouls_to_give` | Int64 |  |
| `away_fouls_team_fouls` | Int64 |  |
| `away_fouls_team_fouls_current` | Int64 |  |
| `away_timeouts_timeouts_current` | Int64 |  |
| `away_timeouts_timeouts_remaining_current` | Int64 |  |
| `home_fouls_bonus_state` | String |  |
| `home_fouls_fouls_to_give` | Int64 |  |
| `home_fouls_team_fouls` | Int64 |  |
| `home_fouls_team_fouls_current` | Int64 |  |
| `home_timeouts_timeouts_current` | Int64 |  |
| `home_timeouts_timeouts_remaining_current` | Int64 |  |
| `last_play_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_situation-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_situation-example}

```python
espn_mlb_game_situation(event_id='401584793')
```

## espn_mlb_game_status

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/status`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/status](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/status)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_status-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `clock` | Float64 | Game clock value (typically empty for baseball). |
| `display_clock` | String |  |
| `period` | Int64 | Inning number. |
| `type_completed` | Boolean |  |
| `type_description` | String |  |
| `type_detail` | String |  |
| `type_id` | Int64 | Type id. |
| `type_name` | String | Type name. |
| `type_short_detail` | String |  |
| `type_state` | String | Type state. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_status-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_status-example}

```python
espn_mlb_game_status(event_id='401584793')
```

## espn_mlb_game_officials

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/officials`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/officials](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/officials)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_officials-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_name` | String | Display name. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player's full name. |
| `id` | Int64 | Id. |
| `last_name` | String | Player last name. |
| `order` | Int64 | Order. |
| `position_display_name` | String | Position display name. |
| `position_id` | Int64 | Unique position identifier. |
| `position_name` | String | Position name. |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_officials-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_officials-example}

```python
espn_mlb_game_officials(event_id='401584793')
```

## espn_mlb_game_broadcasts

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/broadcasts`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/broadcasts](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/broadcasts)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_broadcasts-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `channel` | Int64 |  |
| `lang` | String |  |
| `partnered` | Boolean |  |
| `priority` | Int64 |  |
| `region` | String | Region label. |
| `slug` | String | URL-safe identifier. |
| `station` | String |  |
| `competition_$ref` | String |  |
| `market_id` | Int64 | ESPN futures-market identifier. |
| `market_type` | String | Market type code (`winLeague`, `winConference`, `winDivision`, ...). |
| `media_$ref` | String |  |
| `media_call_letters` | String |  |
| `media_id` | Int64 | Media identifier. |
| `media_logos` | String |  |
| `media_name` | String |  |
| `media_short_name` | String |  |
| `media_slug` | String |  |
| `type_id` | Int64 | Type id. |
| `type_long_name` | String |  |
| `type_short_name` | String |  |
| `type_slug` | String |  |
| `count` | Int64 | Total items in the ESPN collection across every page (the page envelope's count), the same on every row; more than the row count means this is one page of several (pass the wrapper's limit= or page=). |
| `page_count` | Int64 | Pages the collection spans at the requested limit (the page envelope's pageCount), the same on every row; above 1 means rows are missing from this frame. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_broadcasts-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_broadcasts-example}

```python
espn_mlb_game_broadcasts(event_id='401584793')
```

## espn_mlb_game_predictor

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/predictor`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/predictor](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/predictor)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_predictor-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `last_modified` | String | Last modified. |
| `name` | String | Display name. |
| `short_name` | String | Short display name. |
| `away_team_statistics` | String |  |
| `away_team_team_$ref` | String |  |
| `home_team_team_$ref` | String |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_predictor-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_predictor-example}

```python
espn_mlb_game_predictor(event_id='401584793')
```

## espn_mlb_game_powerindex

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/powerindex`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/powerindex](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/powerindex)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_powerindex-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `count` | Int64 | Count of count. |
| `items` | String |  |
| `page_count` | Int64 |  |
| `page_index` | Int64 |  |
| `page_size` | Int64 |  |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_powerindex-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_powerindex-example}

```python
espn_mlb_game_powerindex(event_id='401584793')
```

## espn_mlb_game_propbets

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/propbets`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/propbets](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/propbets)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_propbets-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_propbets-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_propbets-example}

```python
espn_mlb_game_propbets(event_id='401584793')
```

## espn_mlb_game_leaders

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/leaders](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_leaders-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nfl, mlb, nhl, wnba, cfb, mbb, wbb; 400 in nba with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_leaders-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_leaders-example}

```python
espn_mlb_game_leaders(event_id='401584793')
```

## espn_mlb_game_scoringplays

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/scoringplays`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/scoringplays](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/scoringplays)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_scoringplays-returns}

**`return_parsed=True`** (default) — the output of `parse_items`; pass `return_as_pandas=True` for a `pandas.DataFrame`.

No returns table is published for this endpoint: no capture: 404 in nba, nfl, mlb, nhl, wnba, cfb, mbb, wbb with the documented example arguments (2026-10-07).

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_scoringplays-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_scoringplays-example}

```python
espn_mlb_game_scoringplays(event_id='401584793')
```

## espn_mlb_game_official_detail

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}/competitions/{cid}/officials/{official_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/officials/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793/competitions/401584793/officials/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |
| `official_id` | `official_id` |  | `Y` |  | official_id path parameter. |
| `cid` | `cid` |  |  | `Y` | cid path parameter. |

### Returns {#espn_mlb_game_official_detail-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `$ref` | String |  |
| `display_name` | String | Display name. |
| `first_name` | String | Player first name. |
| `full_name` | String | Player's full name. |
| `id` | Int64 | Id. |
| `last_name` | String | Player last name. |
| `order` | Int64 | Order. |
| `position_display_name` | String | Position display name. |
| `position_id` | Int64 | Unique position identifier. |
| `position_name` | String | Position name. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#espn_mlb_game_official_detail-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#espn_mlb_game_official_detail-example}

```python
espn_mlb_game_official_detail(event_id='401584793', official_id='1')
```
