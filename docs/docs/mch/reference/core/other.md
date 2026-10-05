---
title: "MCH — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "MCH — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# MCH — ESPN core API (v2) — Other

## espn_mch_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mch_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_league_root-example}

```python
espn_mch_league_root()
```

_Last validated n/a._

## espn_mch_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/seasons?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/seasons?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_seasons-example}

```python
espn_mch_seasons()
```

_Last validated n/a._

## espn_mch_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/events?limit=500](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/events?limit=500)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_games-example}

```python
espn_mch_games()
```

_Last validated n/a._

## espn_mch_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/events/401584793](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_mch_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_game-example}

```python
espn_mch_game(event_id='401584793')
```

_Last validated n/a._

## espn_mch_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/teams?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/teams?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_mch_teams_core-returns}

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

### Example {#espn_mch_teams_core-example}

```python
espn_mch_teams_core()
```

_Last validated n/a._

## espn_mch_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/teams/4](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_mch_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_team_core-example}

```python
espn_mch_team_core(team_id='4')
```

_Last validated n/a._

## espn_mch_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/venues?limit=1000](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/venues?limit=1000)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_venues-example}

```python
espn_mch_venues()
```

_Last validated n/a._

## espn_mch_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/venues/3663](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_mch_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_venue-example}

```python
espn_mch_venue(venue_id='3663')
```

_Last validated n/a._

## espn_mch_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/franchises?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/franchises?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_franchises-example}

```python
espn_mch_franchises()
```

_Last validated n/a._

## espn_mch_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/franchises/2](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_mch_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_franchise-example}

```python
espn_mch_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_mch_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_mch_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_coach-example}

```python
espn_mch_coach(coach_id='1')
```

_Last validated n/a._

## espn_mch_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/1/record/0](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/1/record/0)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_mch_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_coach_record-example}

```python
espn_mch_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_mch_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_mch_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_coach_season-example}

```python
espn_mch_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_mch_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/positions?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/positions?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_positions-example}

```python
espn_mch_positions()
```

_Last validated n/a._

## espn_mch_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/positions/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_mch_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_position-example}

```python
espn_mch_position(position_id='1')
```

_Last validated n/a._

## espn_mch_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/tournaments?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/tournaments?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_tournaments-example}

```python
espn_mch_tournaments()
```

_Last validated n/a._

## espn_mch_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/awards?limit=200](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/awards?limit=200)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_mch_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_awards-example}

```python
espn_mch_awards()
```

_Last validated n/a._

## espn_mch_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/awards/1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_mch_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_award-example}

```python
espn_mch_award(award_id='1')
```

_Last validated n/a._

## espn_mch_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/standings](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mch_standings_core-returns}

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

### Example {#espn_mch_standings_core-example}

```python
espn_mch_standings_core()
```

_Last validated n/a._

## espn_mch_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/leaders](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mch_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_leaders_core-example}

```python
espn_mch_leaders_core()
```

_Last validated n/a._

## espn_mch_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/notes](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mch_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_league_notes-example}

```python
espn_mch_league_notes()
```

_Last validated n/a._

## espn_mch_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/talentpicks](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mch_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_talentpicks-example}

```python
espn_mch_talentpicks()
```

_Last validated n/a._

## espn_mch_recruiting_years

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_mch_recruiting_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_recruiting_years-example}

```python
espn_mch_recruiting_years()
```

_Last validated n/a._

## espn_mch_recruiting_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting/{year}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting/2026/athletes?limit=1000&page=1](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting/2026/athletes?limit=1000&page=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_mch_recruiting_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_recruiting_players-example}

```python
espn_mch_recruiting_players(year=2026)
```

_Last validated n/a._

## espn_mch_recruiting_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting/{year}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting/2026/rankings](https://sports.core.api.espn.com/v2/sports/hockey/leagues/mens-college-hockey/recruiting/2026/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#espn_mch_recruiting_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_mch_recruiting_rankings-example}

```python
espn_mch_recruiting_rankings(year=2026)
```

_Last validated n/a._
