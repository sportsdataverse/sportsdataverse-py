---
title: "NWSL — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "NWSL — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NWSL — ESPN core API (v2) — Other

## espn_nwsl_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nwsl_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_league_root-example}

```python
espn_nwsl_league_root()
```

_Last validated n/a._

## espn_nwsl_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/seasons](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_seasons-example}

```python
espn_nwsl_seasons()
```

_Last validated n/a._

## espn_nwsl_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/events](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/events)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_games-example}

```python
espn_nwsl_games()
```

_Last validated n/a._

## espn_nwsl_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/events/401584793](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_nwsl_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_game-example}

```python
espn_nwsl_game(event_id='401584793')
```

_Last validated n/a._

## espn_nwsl_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/teams](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_nwsl_teams_core-returns}

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

### Example {#espn_nwsl_teams_core-example}

```python
espn_nwsl_teams_core()
```

_Last validated n/a._

## espn_nwsl_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/teams/4](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_nwsl_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_team_core-example}

```python
espn_nwsl_team_core(team_id='4')
```

_Last validated n/a._

## espn_nwsl_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/venues](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_venues-example}

```python
espn_nwsl_venues()
```

_Last validated n/a._

## espn_nwsl_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/venues/3663](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_nwsl_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_venue-example}

```python
espn_nwsl_venue(venue_id='3663')
```

_Last validated n/a._

## espn_nwsl_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/franchises](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/franchises)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_franchises-example}

```python
espn_nwsl_franchises()
```

_Last validated n/a._

## espn_nwsl_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/franchises/2](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_nwsl_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_franchise-example}

```python
espn_nwsl_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_nwsl_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_nwsl_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_coach-example}

```python
espn_nwsl_coach(coach_id='1')
```

_Last validated n/a._

## espn_nwsl_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/1/record](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/1/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_nwsl_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_coach_record-example}

```python
espn_nwsl_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_nwsl_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_nwsl_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_coach_season-example}

```python
espn_nwsl_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_nwsl_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/positions](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_positions-example}

```python
espn_nwsl_positions()
```

_Last validated n/a._

## espn_nwsl_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/positions/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_nwsl_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_position-example}

```python
espn_nwsl_position(position_id='1')
```

_Last validated n/a._

## espn_nwsl_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/tournaments](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/tournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_tournaments-example}

```python
espn_nwsl_tournaments()
```

_Last validated n/a._

## espn_nwsl_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/awards](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_nwsl_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_awards-example}

```python
espn_nwsl_awards()
```

_Last validated n/a._

## espn_nwsl_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/awards/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_nwsl_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_award-example}

```python
espn_nwsl_award(award_id='1')
```

_Last validated n/a._

## espn_nwsl_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/standings](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nwsl_standings_core-returns}

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
| `avg_points_against` | double | Avg points against. |
| `avg_points_for` | double | Avg points for. |
| `clincher` | double | Clincher. |
| `differential` | double | Differential. |
| `division_win_percent` | double | Division win percent. |
| `games_behind` | double | Games behind. |
| `league_win_percent` | double | League win percent. |
| `losses` | double | Losses. |
| `playoff_seed` | double | Playoff seed. |
| `point_differential` | double | Point differential. |
| `points` | double | Points. |
| `points_against` | double | Points against. |
| `points_for` | double | Points for. |
| `streak` | double | Streak. |
| `win_percent` | double | Win percent. |
| `wins` | double | Wins. |
| `games_ahead` | double | Games ahead. |
| `overall` | character | Overall. |
| `home` | character | Home. |
| `road` | character | Road. |
| `vs. div.` | character | Vs. div.. |
| `vs. conf.` | character | Vs. conf.. |
| `last ten games` | character | Last ten games. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_standings_core-example}

```python
espn_nwsl_standings_core()
```

_Last validated n/a._

## espn_nwsl_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nwsl_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_leaders_core-example}

```python
espn_nwsl_leaders_core()
```

_Last validated n/a._

## espn_nwsl_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/notes](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nwsl_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_league_notes-example}

```python
espn_nwsl_league_notes()
```

_Last validated n/a._

## espn_nwsl_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/talentpicks](https://sports.core.api.espn.com/v2/sports/soccer/leagues/usa.nwsl/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_nwsl_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_nwsl_talentpicks-example}

```python
espn_nwsl_talentpicks()
```

_Last validated n/a._
