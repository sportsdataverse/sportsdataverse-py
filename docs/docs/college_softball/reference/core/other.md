---
title: "COLLEGE_SOFTBALL — ESPN core API (v2) — Other"
sidebar_label: "Other"
sidebar_position: 4
description: "COLLEGE_SOFTBALL — ESPN core API (v2) — Other — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# COLLEGE_SOFTBALL — ESPN core API (v2) — Other

## espn_college_softball_league_root

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_softball_league_root-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_league_root-example}

```python
espn_college_softball_league_root()
```

_Last validated n/a._

## espn_college_softball_seasons

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/seasons`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/seasons](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_seasons-example}

```python
espn_college_softball_seasons()
```

_Last validated n/a._

## espn_college_softball_games

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/events`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/events](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/events)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `dates` | `dates` |  |  | `Y` | Date or date range filter (YYYYMMDD or YYYYMMDD-YYYYMMDD). |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_games-example}

```python
espn_college_softball_games()
```

_Last validated n/a._

## espn_college_softball_game

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/events/{event_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/events/401584793](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/events/401584793)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `event_id` | `event_id` |  | `Y` |  | event_id path parameter. |

### Returns {#espn_college_softball_game-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_game-example}

```python
espn_college_softball_game(event_id='401584793')
```

_Last validated n/a._

## espn_college_softball_teams_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/teams`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/teams](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_college_softball_teams_core-returns}

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

### Example {#espn_college_softball_teams_core-example}

```python
espn_college_softball_teams_core()
```

_Last validated n/a._

## espn_college_softball_team_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/teams/{team_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/teams/4](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/teams/4)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `team_id` | `team_id` |  | `Y` |  | team_id path parameter. |

### Returns {#espn_college_softball_team_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_team_core-example}

```python
espn_college_softball_team_core(team_id='4')
```

_Last validated n/a._

## espn_college_softball_venues

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/venues`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/venues](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/venues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_venues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_venues-example}

```python
espn_college_softball_venues()
```

_Last validated n/a._

## espn_college_softball_venue

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/venues/{venue_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/venues/3663](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/venues/3663)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `venue_id` | `venue_id` |  | `Y` |  | venue_id path parameter. |

### Returns {#espn_college_softball_venue-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_venue-example}

```python
espn_college_softball_venue(venue_id='3663')
```

_Last validated n/a._

## espn_college_softball_franchises

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/franchises`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/franchises](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/franchises)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_franchises-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_franchises-example}

```python
espn_college_softball_franchises()
```

_Last validated n/a._

## espn_college_softball_franchise

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/franchises/{franchise_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/franchises/2](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/franchises/2)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `franchise_id` | `franchise_id` |  | `Y` |  | franchise_id path parameter. |

### Returns {#espn_college_softball_franchise-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_franchise-example}

```python
espn_college_softball_franchise(franchise_id='2')
```

_Last validated n/a._

## espn_college_softball_coach

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/{coach_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |

### Returns {#espn_college_softball_coach-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_coach-example}

```python
espn_college_softball_coach(coach_id='1')
```

_Last validated n/a._

## espn_college_softball_coach_record

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/{coach_id}/record/{record_type}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/1/record](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/1/record)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `record_type` | `record_type` |  |  | `Y` | record_type path parameter. |

### Returns {#espn_college_softball_coach_record-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_coach_record-example}

```python
espn_college_softball_coach_record(coach_id='1')
```

_Last validated n/a._

## espn_college_softball_coach_season

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/{coach_id}/seasons/{season}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/1/seasons/2024](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/coaches/1/seasons/2024)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `coach_id` | `coach_id` |  | `Y` |  | coach_id path parameter. |
| `season` | `season` |  | `Y` |  | season path parameter. |

### Returns {#espn_college_softball_coach_season-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_coach_season-example}

```python
espn_college_softball_coach_season(coach_id='1', season=2024)
```

_Last validated n/a._

## espn_college_softball_positions

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/positions`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/positions](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/positions)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_positions-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_positions-example}

```python
espn_college_softball_positions()
```

_Last validated n/a._

## espn_college_softball_position

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/positions/{position_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/positions/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/positions/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `position_id` | `position_id` |  | `Y` |  | position_id path parameter. |

### Returns {#espn_college_softball_position-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_position-example}

```python
espn_college_softball_position(position_id='1')
```

_Last validated n/a._

## espn_college_softball_tournaments

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/tournaments`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/tournaments](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/tournaments)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_tournaments-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_tournaments-example}

```python
espn_college_softball_tournaments()
```

_Last validated n/a._

## espn_college_softball_awards

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/awards`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/awards](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/awards)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |

### Returns {#espn_college_softball_awards-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_awards-example}

```python
espn_college_softball_awards()
```

_Last validated n/a._

## espn_college_softball_award

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/awards/{award_id}`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/awards/1](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/awards/1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `award_id` | `award_id` |  | `Y` |  | award_id path parameter. |

### Returns {#espn_college_softball_award-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_single_entity`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_award-example}

```python
espn_college_softball_award(award_id='1')
```

_Last validated n/a._

## espn_college_softball_standings_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/standings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/standings](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/standings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_softball_standings_core-returns}

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

### Example {#espn_college_softball_standings_core-example}

```python
espn_college_softball_standings_core()
```

_Last validated n/a._

## espn_college_softball_leaders_core

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/leaders`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/leaders](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/leaders)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_softball_leaders_core-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_leaders_core-example}

```python
espn_college_softball_leaders_core()
```

_Last validated n/a._

## espn_college_softball_league_notes

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/notes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/notes](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/notes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_softball_league_notes-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_league_notes-example}

```python
espn_college_softball_league_notes()
```

_Last validated n/a._

## espn_college_softball_talentpicks

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/talentpicks`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/talentpicks](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/talentpicks)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_softball_talentpicks-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_talentpicks-example}

```python
espn_college_softball_talentpicks()
```

_Last validated n/a._

## espn_college_softball_recruiting_years

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#espn_college_softball_recruiting_years-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_recruiting_years-example}

```python
espn_college_softball_recruiting_years()
```

_Last validated n/a._

## espn_college_softball_recruiting_players

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting/{year}/athletes`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting/2026/athletes](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting/2026/athletes)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |
| `limit` | `limit` |  |  | `Y` | Maximum number of items to return. |
| `page` | `page` |  |  | `Y` | page query parameter. |

### Returns {#espn_college_softball_recruiting_players-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_recruiting_players-example}

```python
espn_college_softball_recruiting_players(year=2026)
```

_Last validated n/a._

## espn_college_softball_recruiting_rankings

ESPN endpoint.

**Endpoint URL:** `GET https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting/{year}/rankings`

**Valid URL:** [https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting/2026/rankings](https://sports.core.api.espn.com/v2/sports/baseball/leagues/college-softball/recruiting/2026/rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `year` | `year` |  | `Y` |  | year path parameter. |

### Returns {#espn_college_softball_recruiting_rankings-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_items`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#espn_college_softball_recruiting_rankings-example}

```python
espn_college_softball_recruiting_rankings(year=2026)
```

_Last validated n/a._
