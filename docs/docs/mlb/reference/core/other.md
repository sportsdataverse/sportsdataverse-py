---
title: "MLB — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "MLB — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MLB — ESPN core API (v2) — Other

## espn_mlb_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_league_root-example}

```python
espn_mlb_league_root()
```

_Last validated n/a._

## espn_mlb_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_seasons-example}

```python
espn_mlb_seasons()
```

_Last validated n/a._

## espn_mlb_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_games-example}

```python
espn_mlb_games()
```

_Last validated n/a._

## espn_mlb_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_mlb_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_game-example}

```python
espn_mlb_game(event_id='401584793')
```

_Last validated n/a._

## espn_mlb_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_mlb_teams_core-returns}

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

### Example {#espn_mlb_teams_core-example}

```python
espn_mlb_teams_core()
```

_Last validated n/a._

## espn_mlb_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams/4](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mlb_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_team_core-example}

```python
espn_mlb_team_core(team_id='4')
```

_Last validated n/a._

## espn_mlb_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_venues-example}

```python
espn_mlb_venues()
```

_Last validated n/a._

## espn_mlb_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues/3663](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_mlb_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_venue-example}

```python
espn_mlb_venue(venue_id='3663')
```

_Last validated n/a._

## espn_mlb_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_franchises-example}

```python
espn_mlb_franchises()
```

_Last validated n/a._

## espn_mlb_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises/2](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_mlb_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_franchise-example}

```python
espn_mlb_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_mlb_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_mlb_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_coach-example}

```python
espn_mlb_coach(coach_id='1')
```

_Last validated n/a._

## espn_mlb_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/record](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_mlb_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_coach_record-example}

```python
espn_mlb_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_mlb_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_mlb_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_coach_season-example}

```python
espn_mlb_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_mlb_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_positions-example}

```python
espn_mlb_positions()
```

_Last validated n/a._

## espn_mlb_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_mlb_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_position-example}

```python
espn_mlb_position(position_id='1')
```

_Last validated n/a._

## espn_mlb_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/tournaments](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/tournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_tournaments-example}

```python
espn_mlb_tournaments()
```

_Last validated n/a._

## espn_mlb_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mlb_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_awards-example}

```python
espn_mlb_awards()
```

_Last validated n/a._

## espn_mlb_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_mlb_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_award-example}

```python
espn_mlb_award(award_id='1')
```

_Last validated n/a._

## espn_mlb_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/standings](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_standings_core-returns}

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

### Example {#espn_mlb_standings_core-example}

```python
espn_mlb_standings_core()
```

_Last validated n/a._

## espn_mlb_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_leaders_core-example}

```python
espn_mlb_leaders_core()
```

_Last validated n/a._

## espn_mlb_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_league_notes-example}

```python
espn_mlb_league_notes()
```

_Last validated n/a._

## espn_mlb_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks](https://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mlb_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mlb_talentpicks-example}

```python
espn_mlb_talentpicks()
```

_Last validated n/a._
