---
title: "WWC — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "WWC — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# WWC — ESPN core API (v2) — Other

## espn_wwc_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wwc_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_league_root-example}

```python
espn_wwc_league_root()
```

_Last validated n/a._

## espn_wwc_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_seasons-example}

```python
espn_wwc_seasons()
```

_Last validated n/a._

## espn_wwc_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/events?limit=500](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_games-example}

```python
espn_wwc_games()
```

_Last validated n/a._

## espn_wwc_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/events/401584793](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_wwc_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_game-example}

```python
espn_wwc_game(event_id='401584793')
```

_Last validated n/a._

## espn_wwc_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_wwc_teams_core-returns}

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

### Example {#espn_wwc_teams_core-example}

```python
espn_wwc_teams_core()
```

_Last validated n/a._

## espn_wwc_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/teams/4](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_wwc_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_team_core-example}

```python
espn_wwc_team_core(team_id='4')
```

_Last validated n/a._

## espn_wwc_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_venues-example}

```python
espn_wwc_venues()
```

_Last validated n/a._

## espn_wwc_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/venues/3663](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_wwc_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_venue-example}

```python
espn_wwc_venue(venue_id='3663')
```

_Last validated n/a._

## espn_wwc_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_franchises-example}

```python
espn_wwc_franchises()
```

_Last validated n/a._

## espn_wwc_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/franchises/2](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_wwc_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_franchise-example}

```python
espn_wwc_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_wwc_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_wwc_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_coach-example}

```python
espn_wwc_coach(coach_id='1')
```

_Last validated n/a._

## espn_wwc_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_wwc_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_coach_record-example}

```python
espn_wwc_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_wwc_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_wwc_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_coach_season-example}

```python
espn_wwc_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_wwc_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/positions?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_positions-example}

```python
espn_wwc_positions()
```

_Last validated n/a._

## espn_wwc_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/positions/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_wwc_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_position-example}

```python
espn_wwc_position(position_id='1')
```

_Last validated n/a._

## espn_wwc_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_tournaments-example}

```python
espn_wwc_tournaments()
```

_Last validated n/a._

## espn_wwc_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/awards?limit=200](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_wwc_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_awards-example}

```python
espn_wwc_awards()
```

_Last validated n/a._

## espn_wwc_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/awards/1](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_wwc_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_award-example}

```python
espn_wwc_award(award_id='1')
```

_Last validated n/a._

## espn_wwc_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/standings](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wwc_standings_core-returns}

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

### Example {#espn_wwc_standings_core-example}

```python
espn_wwc_standings_core()
```

_Last validated n/a._

## espn_wwc_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/leaders](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wwc_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_leaders_core-example}

```python
espn_wwc_leaders_core()
```

_Last validated n/a._

## espn_wwc_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/notes](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wwc_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_league_notes-example}

```python
espn_wwc_league_notes()
```

_Last validated n/a._

## espn_wwc_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/talentpicks](https://sports.core.api.espn.com/v2/sports/soccer/leagues/fifa.wwc/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_wwc_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_wwc_talentpicks-example}

```python
espn_wwc_talentpicks()
```

_Last validated n/a._
