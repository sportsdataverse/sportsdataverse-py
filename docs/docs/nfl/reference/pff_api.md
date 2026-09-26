---
title: NFL — PFF Developer API (api.pff.com, API key)
sidebar_label: PFF Developer API (api.pff.com, API key)
description: "NFL — PFF Developer API (api.pff.com, API key) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 13
---
# NFL — PFF Developer API (api.pff.com, API key)

`sportsdataverse.nfl` — 68 endpoints.

## `pff_api_ref_leagues`

List the leagues you can read, with their seasons and weeks

**Endpoint URL:** `GET https://api.pff.com/v1/leagues`

**Valid URL:** [https://api.pff.com/v1/leagues](https://api.pff.com/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `abbreviation` | character | Metric abbreviation. |
| `default_season` | numeric | Season the source API currently treats as the default for this league. |
| `default_week` | numeric | Week number the source API currently treats as the default for this league. |
| `default_week_group` | character | Identifier of the week grouping (e.g., regular season or postseason phase) currently set as the league default. |
| `id` | numeric | ID of the player in the 'name' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `seasons` | list | NBA seasons played. |
| `slug` | character | URL slug for the team. |
| `week_groups` | list | Nested list of week-group objects (phase label and week span) defined for the league. |
| `weeks` | list | Nested list of week objects available for the league. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_ref_leagues()
```

_Last validated n/a._

## `pff_api_ref_games`

List game results for a league, season and week

**Endpoint URL:** `GET https://api.pff.com/v1/games`

**Valid URL:** [https://api.pff.com/v1/games?league=nfl&season=2022&week=1](https://api.pff.com/v1/games?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of games — the one command that takes a single week number rather than a comma-separated list. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `away_franchise_id` | numeric | PFF franchise id of the away team. |
| `away_team` | list | Away team object (JSON-stringified in the tidy frame). |
| `has_stats` | logical | Whether PFF has published stats for the game. |
| `home_franchise_id` | numeric | PFF franchise id of the home team. |
| `home_team` | list | Home team object (JSON-stringified in the tidy frame). |
| `id` | numeric | PFF game id (integer join key). |
| `league` | list | League slug. |
| `league_id` | numeric | PFF league id (integer). |
| `lock_status` | character | Data lock/publish status for the game. |
| `score` | list | Final score string. |
| `season` | numeric | Season (starting year) of the game. |
| `stadium_id` | numeric | PFF stadium identifier for the game venue. |
| `start` | character | Kickoff timestamp (ISO 8601 string). |
| `week` | numeric | Week number of the game. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_ref_games(league='nfl', season=2022, week=1)
```

_Last validated n/a._

## `pff_api_ref_players`

Search the player directory by name or id

**Endpoint URL:** `GET https://api.pff.com/v1/players`

**Valid URL:** [https://api.pff.com/v1/players?league=nfl&name=burrow](https://api.pff.com/v1/players?league=nfl&name=burrow)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `id` | `id` |  |  | `Y` | Exact player-id lookup, ref-players only. |
| `name` | `name` |  |  | `Y` | Free-text player-name search, ref-players only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `college` | character | Official college (usually the last one attended) |
| `current_class` | character | Player's current college class designation (e.g., Freshman, Senior), per PFF. |
| `current_eligible_year` | numeric | Year the player is or was first draft-eligible, per PFF. |
| `dob` | character | Player date of birth. |
| `draft` | list | Nested draft-selection details for the player (year, round, pick, and franchise) as returned by the source API. |
| `first_name` | character | First name of player |
| `height` | numeric | Official height, in inches |
| `id` | numeric | ID of the player in the 'name' column. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `last_name` | character | Last name of player |
| `position` | character | Primary position as reported by NFL.com |
| `speed` | numeric | Speed. |
| `team` | list | NFL team. Uses official abbreviations as per NFL.com |
| `weight` | numeric | Official weight, in pounds |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_ref_players(league='nfl', name='burrow')
```

_Last validated n/a._

## `pff_api_team_list`

List a season's teams, franchise groups and schedule

**Endpoint URL:** `GET https://api.pff.com/v1/teams`

**Valid URL:** [https://api.pff.com/v1/teams?league=nfl&season=2022](https://api.pff.com/v1/teams?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `heirarchy` | list | Nested hierarchy of franchise groupings as returned by the PFF API; the field name's spelling follows the source. |
| `id` | numeric | ID of the player in the 'name' column. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `slug` | character | URL slug for the team. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_list(league='nfl', season=2022)
```

_Last validated n/a._

## `pff_api_team_overview`

Season-to-date team report, one row per team

**Endpoint URL:** `GET https://api.pff.com/v1/teams/overview`

**Valid URL:** [https://api.pff.com/v1/teams/overview?league=nfl&season=2022](https://api.pff.com/v1/teams/overview?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `abbreviation` | character | Team abbreviation. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | numeric | PFF team coverage grade (0-100). |
| `grades_defense` | numeric | PFF team defense grade (0-100). |
| `grades_misc_st` | numeric | Team-level PFF miscellaneous special-teams grade, 0-100. |
| `grades_offense` | numeric | PFF team offense grade (0-100). |
| `grades_overall` | numeric | Team-level PFF overall grade, 0-100. |
| `grades_pass` | numeric | Team-level PFF passing grade, 0-100. |
| `grades_pass_block` | numeric | Team-level PFF pass-blocking grade, 0-100. |
| `grades_pass_route` | numeric | Team-level PFF receiving (route-running) grade, 0-100. |
| `grades_pass_rush_defense` | numeric | Team-level PFF pass-rush grade, 0-100. |
| `grades_run` | numeric | Team-level PFF rushing grade, 0-100. |
| `grades_run_block` | numeric | Team-level PFF run-blocking grade, 0-100. |
| `grades_run_defense` | numeric | Team-level PFF run-defense grade, 0-100. |
| `grades_tackle` | numeric | Team-level PFF tackling grade, 0-100. |
| `losses` | numeric | Losses against the spread in the split. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `points_allowed` | numeric | Points for the opponent. |
| `points_scored` | numeric | Total points scored by the team over the covered span. |
| `ties` | numeric | Number of ties in the series. |
| `wins` | numeric | Wins against the spread in the split. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_overview(league='nfl', season=2022)
```

_Last validated n/a._

## `pff_api_team_summary`

Per-game team report for one franchise, one row per game

**Endpoint URL:** `GET https://api.pff.com/v1/teams/summary`

**Valid URL:** [https://api.pff.com/v1/teams/summary?league=nfl&season=2022&franchise_id=7](https://api.pff.com/v1/teams/summary?league=nfl&season=2022&franchise_id=7)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  | `Y` |  | Franchise (team) id, and the THIRD positional argument of team-summary — the report is franchise-scoped, so there is no all-teams form of it. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `away` | logical | Away team name. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `grades_coverage_defense` | numeric |  |
| `grades_defense` | numeric |  |
| `grades_misc_st` | numeric |  |
| `grades_offense` | numeric |  |
| `grades_overall` | numeric |  |
| `grades_pass` | numeric |  |
| `grades_pass_block` | numeric |  |
| `grades_pass_route` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `grades_run` | numeric |  |
| `grades_run_block` | numeric |  |
| `grades_run_defense` | numeric |  |
| `grades_tackle` | numeric |  |
| `home` | logical | Home team name. |
| `lock_status` | character |  |
| `opponent` | character | Opposing team of player |
| `opponent_franchise_id` | integer |  |
| `points_allowed` | integer | Points for the opponent. |
| `points_scored` | integer |  |
| `start` | character | Start. |
| `week` | integer | Season week. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_summary(franchise_id=7, league='nfl', season=2022)
```

_Last validated n/a._

## `pff_api_player_seasons`

List the seasons a player has data for

**Endpoint URL:** `GET https://api.pff.com/v1/player/seasons`

**Valid URL:** [https://api.pff.com/v1/player/seasons?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/seasons?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_seasons(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_snaps_summary`

Snap counts for a player, broken out by position

**Endpoint URL:** `GET https://api.pff.com/v1/player/snaps/summary`

**Valid URL:** [https://api.pff.com/v1/player/snaps/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/snaps/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_snaps_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_position_pivot`

Player snap counts pivoted by position

**Endpoint URL:** `GET https://api.pff.com/v1/player/position/pivot`

**Valid URL:** [https://api.pff.com/v1/player/position/pivot?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/position/pivot?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_position_pivot(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_offense_summary`

Offense summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/summary`

**Valid URL:** [https://api.pff.com/v1/player/offense/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_offense_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_offense_blocking`

Blocking report for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/blocking`

**Valid URL:** [https://api.pff.com/v1/player/offense/blocking?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/blocking?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_offense_blocking(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_offense_pass_blocking`

Pass-blocking report for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/pass_blocking`

**Valid URL:** [https://api.pff.com/v1/player/offense/pass_blocking?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/pass_blocking?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_offense_pass_blocking(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_offense_run_blocking`

Run-blocking report for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/offense/run_blocking`

**Valid URL:** [https://api.pff.com/v1/player/offense/run_blocking?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/offense/run_blocking?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_offense_run_blocking(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_passing_summary`

Passing summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/summary`

**Valid URL:** [https://api.pff.com/v1/player/passing/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_passing_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_passing_concept`

Passing by play concept for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/concept`

**Valid URL:** [https://api.pff.com/v1/player/passing/concept?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/concept?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comp_pct_diff` | numeric | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | numeric | PFF passing grade (0-100) on play-action dropbacks. |
| `no_screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | numeric | Traditional NFL passer rating on play-action dropbacks. |
| `no_screen_qb_rating` | numeric | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | numeric | Number of completed passes on play-action dropbacks. |
| `pa_thrown_aways` | numeric | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | numeric | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `no_screen_drops` | numeric | Number of catchable passes dropped by receivers excluding screen passes. |
| `screen_completion_percent` | numeric | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `no_screen_thrown_aways` | numeric | Number of intentional throwaways excluding screen passes. |
| `pa_grades_run` | numeric | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on screen passes. |
| `dropbacks` | numeric | Number of dropbacks. |
| `npa_thrown_aways` | numeric | Number of intentional throwaways on non-play-action dropbacks. |
| `no_screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | numeric | Number of passing touchdowns thrown on play-action dropbacks. |
| `npa_ypa` | numeric | Yards gained per pass attempt on non-play-action dropbacks. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds on screen passes. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on non-play-action dropbacks, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `npa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on non-play-action dropbacks. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `no_screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `npa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `pa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on screen passes. |
| `no_screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_interceptions` | numeric | Number of passes intercepted on screen passes. |
| `no_screen_passing_snaps` | numeric | Number of passing snaps played excluding screen passes. |
| `no_screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `screen_scrambles` | numeric | Number of scrambles on screen passes. |
| `screen_grades_pass` | numeric | PFF passing grade (0-100) on screen passes. |
| `npa_qb_rating` | numeric | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_screen_grades_pass` | numeric | PFF passing grade (0-100) excluding screen passes. |
| `pa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on play-action dropbacks. |
| `screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `npa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `npa_drops` | numeric | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `screen_yards` | numeric | Passing yards gained on screen passes. |
| `no_screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `no_screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_passing_snaps` | numeric | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `npa_spikes` | numeric | Number of clock-stopping spikes on non-play-action dropbacks. |
| `screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) excluding screen passes, as charted by PFF. |
| `screen_drops` | numeric | Number of catchable passes dropped by receivers on screen passes. |
| `screen_ypa` | numeric | Yards gained per pass attempt on screen passes. |
| `npa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `npa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `pa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `pa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in air yards on screen passes. |
| `pa_sacks` | numeric | Number of sacks taken on play-action dropbacks. |
| `screen_passing_snaps` | numeric | Number of passing snaps played on screen passes. |
| `no_screen_grades_run` | numeric | PFF rushing grade for the player (0-100) excluding screen passes. |
| `no_screen_first_downs` | numeric | Number of passing first downs gained excluding screen passes. |
| `pa_ypa` | numeric | Yards gained per pass attempt on play-action dropbacks. |
| `npa_scrambles` | numeric | Number of scrambles on non-play-action dropbacks. |
| `npa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `screen_completions` | numeric | Number of completed passes on screen passes. |
| `screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | numeric | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_interceptions` | numeric | Number of passes intercepted excluding screen passes. |
| `npa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_sacks` | numeric | Number of sacks taken excluding screen passes. |
| `penalties` | numeric | Total number of penalties. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `no_screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds excluding screen passes. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on play-action dropbacks, as charted by PFF. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `pa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on screen passes. |
| `no_screen_dropbacks` | numeric | Number of dropbacks excluding screen passes. |
| `no_screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `npa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `position` | character | Primary position as reported by NFL.com |
| `pa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `npa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | numeric | Average depth of target in air yards excluding screen passes. |
| `pa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | numeric | Percentage of pass attempts completed excluding screen passes. |
| `pa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `pa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `npa_grades_pass` | numeric | PFF passing grade (0-100) on non-play-action dropbacks. |
| `screen_grades_run` | numeric | PFF rushing grade for the player (0-100) on screen passes. |
| `screen_first_downs` | numeric | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | numeric | Percentage of pass attempts completed on non-play-action dropbacks. |
| `no_screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `no_screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `npa_avg_depth_of_target` | numeric | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | numeric | Number of dropbacks on non-play-action dropbacks. |
| `player` | character | Player name |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on screen passes, as charted by PFF. |
| `npa_touchdowns` | numeric | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `screen_attempts` | numeric | Number of pass attempts on screen passes. |
| `screen_dropbacks` | numeric | Number of dropbacks on screen passes. |
| `no_screen_yards` | numeric | Passing yards gained excluding screen passes. |
| `pa_scrambles` | numeric | Number of scrambles on play-action dropbacks. |
| `pa_completion_percent` | numeric | Percentage of pass attempts completed on play-action dropbacks. |
| `pa_avg_depth_of_target` | numeric | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | numeric | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `no_screen_spikes` | numeric | Number of clock-stopping spikes excluding screen passes. |
| `pa_dropbacks` | numeric | Number of dropbacks on play-action dropbacks. |
| `npa_big_time_throws` | numeric | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `no_screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `pa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `no_screen_attempts` | numeric | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | numeric | Number of passing snaps played on play-action dropbacks. |
| `npa_completions` | numeric | Number of completed passes on non-play-action dropbacks. |
| `screen_touchdowns` | numeric | Number of passing touchdowns thrown on screen passes. |
| `npa_first_downs` | numeric | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | numeric | Number of passing touchdowns thrown excluding screen passes. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric | Passing yards gained on non-play-action dropbacks. |
| `no_screen_ypa` | numeric | Yards gained per pass attempt excluding screen passes. |
| `npa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `npa_interceptions` | numeric | Number of passes intercepted on non-play-action dropbacks. |
| `no_screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `screen_sacks` | numeric | Number of sacks taken on screen passes. |
| `screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `pa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `npa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on screen passes. |
| `npa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `pa_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on play-action dropbacks. |
| `no_screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) excluding screen passes. |
| `pa_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_tackle` | numeric | PFF tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on screen passes. |
| `pa_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) excluding screen passes. |
| `npa_grades_tackle` | numeric | PFF tackling grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_tackle` | character | PFF tackling grade for the player (0-100) on screen passes. |
| `no_screen_grades_tackle` | numeric | PFF tackling grade for the player (0-100) excluding screen passes. |
| `npa_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `screen_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on screen passes. |
| `npa_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) on screen passes. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_passing_concept(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_passing_depth`

Passing by target depth for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/depth`

**Valid URL:** [https://api.pff.com/v1/player/passing/depth?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/depth?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `penalties` | numeric | Total number of penalties. |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `base_attempts` | numeric | Number of pass attempts across all splits, the baseline total for this facet. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_passing_depth(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_passing_pressure`

Passing under pressure for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/passing/pressure`

**Valid URL:** [https://api.pff.com/v1/player/passing/pressure?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/passing/pressure?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `penalties` | numeric | Total number of penalties. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `position` | character | Primary position as reported by NFL.com |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `player` | character | Player name |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when under pressure. |
| `pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when under pressure. |
| `no_blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when not blitzed. |
| `no_blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when not blitzed. |
| `blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when blitzed. |
| `no_pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when not blitzed. |
| `blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when blitzed. |
| `blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when blitzed. |
| `no_pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when under pressure. |
| `pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when under pressure. |
| `blitz_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when blitzed. |
| `no_pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when blitzed. |
| `blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when not blitzed. |
| `pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when not blitzed. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_passing_pressure(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_rushing_direction`

Rushing by direction for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/rushing/direction`

**Valid URL:** [https://api.pff.com/v1/player/rushing/direction?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/rushing/direction?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_rushing_direction(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_rushing_summary`

Rushing summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/rushing/summary`

**Valid URL:** [https://api.pff.com/v1/player/rushing/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/rushing/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_rushing_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_receiving_depth`

Receiving by target depth for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/receiving/depth`

**Valid URL:** [https://api.pff.com/v1/player/receiving/depth?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/receiving/depth?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield). |
| `right_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `left_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield). |
| `deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield). |
| `center_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage. |
| `right_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `left_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield). |
| `deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `center_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield). |
| `left_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield). |
| `behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `center_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield). |
| `center_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `center_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield). |
| `medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield). |
| `deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `right_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield). |
| `left_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the left third of the field. |
| `right_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `center_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield). |
| `center_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield). |
| `center_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield). |
| `center_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `center_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield). |
| `deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield), 0-100. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield), 0-100. |
| `center_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the middle of the field. |
| `medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield). |
| `center_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield). |
| `behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage. |
| `left_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the left third of the field. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield). |
| `left_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage. |
| `right_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the right third of the field. |
| `medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `right_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield). |
| `left_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield). |
| `center_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield). |
| `short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield). |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield). |
| `behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage. |
| `center_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield). |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the right third of the field. |
| `left_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `center_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield). |
| `left_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the right third of the field. |
| `behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `left_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage. |
| `right_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the left third of the field. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `center_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `left_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage. |
| `right_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage. |
| `left_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `right_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield). |
| `center_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `left_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield). |
| `left_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield). |
| `medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield). |
| `left_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `left_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield). |
| `center_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield). |
| `left_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield). |
| `left_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the left third of the field. |
| `short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield). |
| `right_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `right_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the right third of the field. |
| `medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield). |
| `right_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage. |
| `center_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage. |
| `right_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield). |
| `left_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage. |
| `short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield). |
| `medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield). |
| `behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage. |
| `left_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield). |
| `deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield). |
| `left_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the left third of the field. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all depths and directions. |
| `deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield). |
| `center_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield). |
| `short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield). |
| `left_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield). |
| `medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield). |
| `center_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the left third of the field. |
| `behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage. |
| `behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage. |
| `medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield). |
| `left_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield). |
| `center_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the middle of the field. |
| `penalties` | numeric | Total number of penalties. |
| `short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage. |
| `left_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `left_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield). |
| `short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield). |
| `deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield). |
| `center_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield). |
| `center_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield), 0-100. |
| `right_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield). |
| `left_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `center_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield). |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `center_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage. |
| `short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield). |
| `deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield). |
| `left_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the right third of the field. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield). |
| `left_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield). |
| `left_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `right_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield). |
| `behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage. |
| `center_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield). |
| `right_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage, 0-100. |
| `left_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the right third of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `right_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `center_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield). |
| `center_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield). |
| `center_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield). |
| `left_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield), 0-100. |
| `left_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage, 0-100. |
| `right_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `left_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the right third of the field. |
| `medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield). |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `center_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield). |
| `center_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield). |
| `center_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the left third of the field. |
| `left_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield). |
| `center_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the right third of the field. |
| `right_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield). |
| `left_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage. |
| `left_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `right_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `left_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield). |
| `center_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage. |
| `behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage. |
| `left_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the middle of the field. |
| `short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield). |
| `right_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage. |
| `short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield). |
| `left_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield). |
| `medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield). |
| `behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage. |
| `left_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield). |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the right third of the field. |
| `short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield). |
| `right_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield). |
| `deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield). |
| `center_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage. |
| `medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `left_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the left third of the field. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_receiving_depth(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_receiving_summary`

Receiving summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/receiving/summary`

**Valid URL:** [https://api.pff.com/v1/player/receiving/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/receiving/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_receiving_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_defense_summary`

Defense summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/defense/summary`

**Valid URL:** [https://api.pff.com/v1/player/defense/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/defense/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_defense_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_field_goal_summary`

Field-goal kicking for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/field_goal/summary`

**Valid URL:** [https://api.pff.com/v1/player/field_goal/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/field_goal/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_field_goal_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_kickoff_summary`

Kickoffs for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/kickoff/summary`

**Valid URL:** [https://api.pff.com/v1/player/kickoff/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/kickoff/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_kickoff_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_punting_summary`

Punting for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/punting/summary`

**Valid URL:** [https://api.pff.com/v1/player/punting/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/punting/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_punting_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_return_summary`

Kick and punt returns for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/return/summary`

**Valid URL:** [https://api.pff.com/v1/player/return/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/return/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_return_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_player_special_summary`

Special-teams summary for one player

**Endpoint URL:** `GET https://api.pff.com/v1/player/special/summary`

**Valid URL:** [https://api.pff.com/v1/player/special/summary?league=nfl&season=2022&player_id=28022](https://api.pff.com/v1/player/special/summary?league=nfl&season=2022&player_id=28022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `player_id` | `player_id` |  | `Y` |  | Player id, taken as a positional argument. |
| `career` | `career` |  |  | `Y` | Career-aggregate toggle, on the player report operations only. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_player_special_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## `pff_api_facet_offense_summary`

League-wide offense summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/summary`

**Valid URL:** [https://api.pff.com/v1/facet/offense/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `grades_run_block` | numeric | PFF run-blocking grade (0-100). |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `position` | character | Primary position as reported by NFL.com |
| `snap_counts_pass` | numeric | Pass-play snaps spent as the passer, rather than blocking or running a route. |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `snap_counts_pass_route` | numeric | Snaps spent running a pass route. |
| `snap_counts_run` | numeric | Run-play snaps spent as a runner, rather than run blocking. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `snap_counts_total` | numeric | Total offensive snaps played. |
| `snap_counts_total_pass` | numeric | Total pass-play snaps across passing, pass blocking, and route running. |
| `snap_counts_total_run` | numeric | Total run-play snaps across rushing and run blocking. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `grades_pass_block` | numeric | PFF pass-blocking grade (0-100). |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_offense_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_offense_blocking`

League-wide blocking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/blocking`

**Valid URL:** [https://api.pff.com/v1/facet/offense/blocking?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/blocking?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade, 0-100. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `snap_counts_rg` | numeric | Snaps aligned at right guard. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `snap_counts_ce` | numeric | Snaps aligned at center. |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `block_percent` | numeric | Share of offensive snaps spent blocking. |
| `snap_counts_offense` | numeric | Offensive snaps played. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_block` | numeric | Total blocking snaps played. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `snap_counts_te` | numeric | Snaps aligned at tight end. |
| `snap_counts_rt` | numeric | Snaps aligned at right tackle. |
| `position` | character | Primary position as reported by NFL.com |
| `non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays. |
| `snap_counts_lt` | numeric | Snaps aligned at left tackle. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `snap_counts_lg` | numeric | Snaps aligned at left guard. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_offense_blocking(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_offense_pass_blocking`

League-wide pass-blocking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/pass_blocking`

**Valid URL:** [https://api.pff.com/v1/facet/offense/pass_blocking?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/pass_blocking?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `true_pass_set_non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `true_pass_set_pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `true_pass_set_pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking on PFF-designated true pass sets. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `non_spike_pass_block_percentage` | numeric | Share of non-spike pass-play snaps spent pass blocking. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `true_pass_set_non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays on PFF-designated true pass sets. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `true_pass_set_hurries_allowed` | numeric | Quarterback hurries allowed on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_play` | numeric | Pass-play snaps on PFF-designated true pass sets. |
| `true_pass_set_hits_allowed` | numeric | Quarterback hits allowed on PFF-designated true pass sets. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `true_pass_set_pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks on PFF-designated true pass sets. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_grades_pass_block` | numeric | PFF pass-blocking grade on PFF-designated true pass sets, 0-100. |
| `non_spike_pass_block` | numeric | Pass-blocking snaps excluding spike plays. |
| `true_pass_set_snap_counts_pass_block` | numeric | Pass-blocking snaps played on PFF-designated true pass sets. |
| `player` | character | Player name |
| `true_pass_set_sacks_allowed` | numeric | Sacks allowed on PFF-designated true pass sets. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_pass_block` | numeric | Pass-blocking snaps played. |
| `pass_block_percent` | numeric | Share of pass-play snaps spent pass blocking. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_offense_pass_blocking(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_offense_run_blocking`

League-wide run-blocking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/offense/run_blocking`

**Valid URL:** [https://api.pff.com/v1/facet/offense/run_blocking?league=nfl&season=2022](https://api.pff.com/v1/facet/offense/run_blocking?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `gap_grades_run_block` | numeric | PFF run-blocking grade on gap-scheme runs, 0-100. |
| `gap_run_block_percent` | numeric | Share of run-play snaps spent run blocking on gap-scheme runs. |
| `gap_snap_counts_run_block` | numeric | Run-blocking snaps played on gap-scheme runs. |
| `gap_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on gap-scheme runs. |
| `gap_snap_counts_run_play` | numeric | Run-play snaps on gap-scheme runs. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `run_block_percent` | numeric | Share of run-play snaps spent run blocking. |
| `snap_counts_run_block` | numeric | Run-blocking snaps played. |
| `snap_counts_run_play` | numeric | Run-play snaps. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `zone_grades_run_block` | numeric | PFF run-blocking grade on zone-scheme runs, 0-100. |
| `zone_run_block_percent` | numeric | Share of run-play snaps spent run blocking on zone-scheme runs. |
| `zone_snap_counts_run_block` | numeric | Run-blocking snaps played on zone-scheme runs. |
| `zone_snap_counts_run_block_percent` | numeric | Share of the player's run-blocking snaps on zone-scheme runs. |
| `zone_snap_counts_run_play` | numeric | Run-play snaps on zone-scheme runs. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_offense_run_blocking(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_passing_allowed_pressure`

League-wide pressure-allowed leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/allowed_pressure`

**Valid URL:** [https://api.pff.com/v1/facet/passing/allowed_pressure?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/allowed_pressure?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `te_percent` | numeric | Share of allowed pressures attributed to tight ends, expressed as a percentage. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `pressures_lt` | numeric | Number of allowed pressures PFF attributes to left tackle. |
| `pressures_rg` | numeric | Number of allowed pressures PFF attributes to right guard. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `lt_percent` | numeric | Share of allowed pressures attributed to left tackle, expressed as a percentage. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `hits_allowed` | numeric | Number of quarterback hits allowed on the player's dropbacks, as charted by PFF. |
| `pressures_lg` | numeric | Number of allowed pressures PFF attributes to left guard. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `hurries_allowed` | numeric | Number of hurries allowed on the player's dropbacks, as charted by PFF. |
| `self_percent` | numeric | Share of allowed pressures attributed to the quarterback himself, expressed as a percentage. |
| `pressures_rt` | numeric | Number of allowed pressures PFF attributes to right tackle. |
| `pressures_allowed` | numeric | Total pressures allowed on the quarterback's dropbacks, as attributed by PFF. |
| `pressures_ol_te` | numeric | Number of allowed pressures PFF attributes to the offensive line and tight ends combined. |
| `pressures_ce` | numeric | Number of allowed pressures PFF attributes to center. |
| `penalties` | numeric | Total number of penalties. |
| `sacks_allowed` | numeric | Opponent sacks. |
| `ol_te_percent` | numeric | Share of allowed pressures attributed to the offensive line and tight ends combined, expressed as a percentage. |
| `allowed_pressure_dropbacks` | numeric | Number of dropbacks over which allowed pressures are attributed, from the PFF allowed-pressure facet. |
| `pressures_other` | numeric | Number of allowed pressures PFF attributes to other players outside the listed blocking positions. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `ce_percent` | numeric | Share of allowed pressures attributed to center, expressed as a percentage. |
| `position` | character | Primary position as reported by NFL.com |
| `pressures_te` | numeric | Number of allowed pressures PFF attributes to tight ends. |
| `pressures_self` | numeric | Number of allowed pressures PFF attributes to the quarterback himself. |
| `lg_percent` | numeric | Share of allowed pressures attributed to left guard, expressed as a percentage. |
| `player` | character | Player name |
| `rt_percent` | numeric | Share of allowed pressures attributed to right tackle, expressed as a percentage. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `other_percent` | numeric | Share of allowed pressures attributed to other players outside the listed blocking positions, expressed as a percentage. |
| `pressures_off` | numeric | Number of allowed pressures PFF attributes to the offense without a specific blocker charged. |
| `rg_percent` | numeric | Share of allowed pressures attributed to right guard, expressed as a percentage. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_passing_allowed_pressure(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_passing_concept`

League-wide passing-by-concept leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/concept`

**Valid URL:** [https://api.pff.com/v1/facet/passing/concept?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/concept?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `comp_pct_diff` | numeric | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | numeric | PFF passing grade (0-100) on play-action dropbacks. |
| `no_screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | numeric | Traditional NFL passer rating on play-action dropbacks. |
| `no_screen_qb_rating` | numeric | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | numeric | Number of completed passes on play-action dropbacks. |
| `pa_thrown_aways` | numeric | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | numeric | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `no_screen_drops` | numeric | Number of catchable passes dropped by receivers excluding screen passes. |
| `screen_completion_percent` | numeric | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `no_screen_thrown_aways` | numeric | Number of intentional throwaways excluding screen passes. |
| `pa_grades_run` | numeric | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on screen passes. |
| `dropbacks` | numeric | Number of dropbacks. |
| `npa_thrown_aways` | numeric | Number of intentional throwaways on non-play-action dropbacks. |
| `no_screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | numeric | Number of passing touchdowns thrown on play-action dropbacks. |
| `npa_ypa` | numeric | Yards gained per pass attempt on non-play-action dropbacks. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds on screen passes. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on non-play-action dropbacks, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `npa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on non-play-action dropbacks. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `no_screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `npa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `pa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on screen passes. |
| `no_screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_interceptions` | numeric | Number of passes intercepted on screen passes. |
| `no_screen_passing_snaps` | numeric | Number of passing snaps played excluding screen passes. |
| `no_screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `screen_scrambles` | numeric | Number of scrambles on screen passes. |
| `screen_grades_pass` | numeric | PFF passing grade (0-100) on screen passes. |
| `npa_qb_rating` | numeric | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_screen_grades_pass` | numeric | PFF passing grade (0-100) excluding screen passes. |
| `pa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on play-action dropbacks. |
| `screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `npa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `npa_drops` | numeric | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `screen_yards` | numeric | Passing yards gained on screen passes. |
| `no_screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `no_screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_passing_snaps` | numeric | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `npa_spikes` | numeric | Number of clock-stopping spikes on non-play-action dropbacks. |
| `screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) excluding screen passes, as charted by PFF. |
| `screen_drops` | numeric | Number of catchable passes dropped by receivers on screen passes. |
| `screen_ypa` | numeric | Yards gained per pass attempt on screen passes. |
| `npa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `npa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `pa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `pa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in air yards on screen passes. |
| `pa_sacks` | numeric | Number of sacks taken on play-action dropbacks. |
| `screen_passing_snaps` | numeric | Number of passing snaps played on screen passes. |
| `no_screen_grades_run` | numeric | PFF rushing grade for the player (0-100) excluding screen passes. |
| `no_screen_first_downs` | numeric | Number of passing first downs gained excluding screen passes. |
| `pa_ypa` | numeric | Yards gained per pass attempt on play-action dropbacks. |
| `npa_scrambles` | numeric | Number of scrambles on non-play-action dropbacks. |
| `npa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `npa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `screen_completions` | numeric | Number of completed passes on screen passes. |
| `screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | numeric | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_interceptions` | numeric | Number of passes intercepted excluding screen passes. |
| `npa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_sacks` | numeric | Number of sacks taken excluding screen passes. |
| `penalties` | numeric | Total number of penalties. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `no_screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds excluding screen passes. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on play-action dropbacks, as charted by PFF. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `pa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on screen passes. |
| `no_screen_dropbacks` | numeric | Number of dropbacks excluding screen passes. |
| `no_screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `npa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `pa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `position` | character | Primary position as reported by NFL.com |
| `pa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `npa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | numeric | Average depth of target in air yards excluding screen passes. |
| `pa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | numeric | Percentage of pass attempts completed excluding screen passes. |
| `pa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `pa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `npa_grades_pass` | numeric | PFF passing grade (0-100) on non-play-action dropbacks. |
| `screen_grades_run` | numeric | PFF rushing grade for the player (0-100) on screen passes. |
| `screen_first_downs` | numeric | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | numeric | Percentage of pass attempts completed on non-play-action dropbacks. |
| `no_screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `no_screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `npa_avg_depth_of_target` | numeric | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | numeric | Number of dropbacks on non-play-action dropbacks. |
| `player` | character | Player name |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on screen passes, as charted by PFF. |
| `npa_touchdowns` | numeric | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `screen_attempts` | numeric | Number of pass attempts on screen passes. |
| `screen_dropbacks` | numeric | Number of dropbacks on screen passes. |
| `no_screen_yards` | numeric | Passing yards gained excluding screen passes. |
| `pa_scrambles` | numeric | Number of scrambles on play-action dropbacks. |
| `pa_completion_percent` | numeric | Percentage of pass attempts completed on play-action dropbacks. |
| `pa_avg_depth_of_target` | numeric | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | numeric | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `no_screen_spikes` | numeric | Number of clock-stopping spikes excluding screen passes. |
| `pa_dropbacks` | numeric | Number of dropbacks on play-action dropbacks. |
| `npa_big_time_throws` | numeric | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `no_screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `pa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `no_screen_attempts` | numeric | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | numeric | Number of passing snaps played on play-action dropbacks. |
| `npa_completions` | numeric | Number of completed passes on non-play-action dropbacks. |
| `screen_touchdowns` | numeric | Number of passing touchdowns thrown on screen passes. |
| `npa_first_downs` | numeric | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | numeric | Number of passing touchdowns thrown excluding screen passes. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric | Passing yards gained on non-play-action dropbacks. |
| `no_screen_ypa` | numeric | Yards gained per pass attempt excluding screen passes. |
| `npa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `npa_interceptions` | numeric | Number of passes intercepted on non-play-action dropbacks. |
| `no_screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `screen_sacks` | numeric | Number of sacks taken on screen passes. |
| `screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `pa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `npa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on screen passes. |
| `npa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `pa_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) on play-action dropbacks. |
| `no_screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) excluding screen passes. |
| `pa_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_tackle` | numeric | PFF tackling grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on screen passes. |
| `pa_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) excluding screen passes. |
| `npa_grades_tackle` | numeric | PFF tackling grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_tackle` | character | PFF tackling grade for the player (0-100) on screen passes. |
| `no_screen_grades_tackle` | numeric | PFF tackling grade for the player (0-100) excluding screen passes. |
| `npa_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) on non-play-action dropbacks. |
| `no_screen_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `screen_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on screen passes. |
| `npa_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) on screen passes. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_passing_concept(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_passing_depth`

League-wide passing-by-depth leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/depth`

**Valid URL:** [https://api.pff.com/v1/facet/passing/depth?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/depth?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `penalties` | numeric | Total number of penalties. |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `base_attempts` | numeric | Number of pass attempts across all splits, the baseline total for this facet. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_passing_depth(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_passing_detail`

League-wide passing detail leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/detail`

**Valid URL:** [https://api.pff.com/v1/facet/passing/detail?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/detail?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the left side of the field. |
| `left_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the left side of the field. |
| `center_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the center of the field. |
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `right_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the left side of the field. |
| `comp_pct_diff` | numeric | Difference in completion percentage between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `pa_grades_pass` | numeric | PFF passing grade (0-100) on play-action dropbacks. |
| `left_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the right side of the field. |
| `right_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage, expressed as a percentage. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) excluding screen passes. |
| `pa_qb_rating` | numeric | Traditional NFL passer rating on play-action dropbacks. |
| `right_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `no_screen_qb_rating` | numeric | Traditional NFL passer rating excluding screen passes. |
| `pa_completions` | numeric | Number of completed passes on play-action dropbacks. |
| `right_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the right side of the field. |
| `deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `center_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the center of the field. |
| `medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws. |
| `left_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the left side of the field. |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts, per PFF charting. |
| `behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage. |
| `medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws, as charted by PFF. |
| `pa_thrown_aways` | numeric | Number of intentional throwaways on play-action dropbacks. |
| `pa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on play-action dropbacks, expressed as a percentage. |
| `ypa_diff` | numeric | Difference in yards per attempt between play-action and non-play-action attempts (PA minus non-PA), from the PFF passing-concept split. |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `center_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `center_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the center of the field. |
| `no_screen_drops` | numeric | Number of catchable passes dropped by receivers excluding screen passes. |
| `behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `screen_completion_percent` | numeric | Percentage of pass attempts completed on screen passes. |
| `npa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on non-play-action dropbacks. |
| `left_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the left side of the field. |
| `deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws. |
| `btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts, per PFF charting. |
| `center_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `center_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the center of the field. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `center_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_screen_thrown_aways` | numeric | Number of intentional throwaways excluding screen passes. |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `center_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the center of the field. |
| `pa_grades_run` | numeric | PFF rushing grade for the player (0-100) on play-action dropbacks. |
| `center_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the center of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the right side of the field. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `center_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `right_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on screen passes. |
| `pa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on play-action dropbacks. |
| `center_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws. |
| `right_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the right side of the field. |
| `spikes` | numeric | Spikes |
| `left_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `screen_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on screen passes. |
| `right_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the right side of the field, expressed as a percentage. |
| `right_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `center_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the center of the field, expressed as a percentage. |
| `deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `center_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the center of the field. |
| `dropbacks` | numeric | Number of dropbacks. |
| `right_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `center_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the center of the field. |
| `npa_thrown_aways` | numeric | Number of intentional throwaways on non-play-action dropbacks. |
| `center_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the center of the field. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `left_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `center_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `center_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the center of the field. |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `right_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the right side of the field. |
| `no_screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks excluding screen passes, as charted by PFF. |
| `right_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the right side of the field. |
| `screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on screen passes. |
| `pa_touchdowns` | numeric | Number of passing touchdowns thrown on play-action dropbacks. |
| `short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws. |
| `center_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the left side of the field. |
| `npa_ypa` | numeric | Yards gained per pass attempt on non-play-action dropbacks. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `left_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the left side of the field. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `left_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the left side of the field. |
| `thrown_aways` | numeric | Number of intentional throwaways. |
| `right_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the right side of the field. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds on screen passes. |
| `right_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `screen_thrown_aways` | numeric | Number of intentional throwaways on screen passes. |
| `behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage, as charted by PFF. |
| `npa_sacks` | numeric | Number of sacks taken on non-play-action dropbacks. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `npa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on non-play-action dropbacks, as charted by PFF. |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `right_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the right side of the field. |
| `deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws, as charted by PFF. |
| `right_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the center of the field. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `left_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the left side of the field. |
| `deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `center_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `center_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the center of the field. |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `no_screen_completions` | numeric | Number of completed passes excluding screen passes. |
| `right_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `no_screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added excluding screen passes. |
| `center_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the center of the field. |
| `medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws. |
| `right_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the right side of the field. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `center_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the center of the field. |
| `screen_spikes` | numeric | Number of clock-stopping spikes on screen passes. |
| `pa_first_downs` | numeric | Number of passing first downs gained on play-action dropbacks. |
| `medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `right_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage. |
| `center_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `right_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the right side of the field, per PFF charting. |
| `pa_big_time_throws` | numeric | Number of big-time throws on play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws. |
| `pa_spikes` | numeric | Number of clock-stopping spikes on play-action dropbacks. |
| `center_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `left_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `pa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on play-action dropbacks. |
| `deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws. |
| `right_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `center_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the center of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws. |
| `left_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the center of the field. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws, expressed as a percentage. |
| `right_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the right side of the field. |
| `screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on screen passes, expressed as a percentage. |
| `center_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the center of the field. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `left_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `no_screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks excluding screen passes. |
| `center_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `center_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the center of the field. |
| `left_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `npa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on non-play-action dropbacks. |
| `deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws. |
| `right_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the center of the field. |
| `right_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the right side of the field. |
| `hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw, as charted by PFF. |
| `right_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the left side of the field. |
| `center_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `right_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the right side of the field. |
| `left_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the left side of the field. |
| `screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on screen passes. |
| `right_deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `first_downs` | numeric | First downs earned by the team. |
| `screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on screen passes. |
| `left_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `left_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the right side of the field. |
| `left_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the center of the field. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `right_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws, as charted by PFF. |
| `right_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws, as charted by PFF. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws to the left side of the field. |
| `left_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the left side of the field. |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `center_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `right_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the left side of the field. |
| `no_screen_bats` | numeric | Number of pass attempts batted down at the line of scrimmage excluding screen passes. |
| `left_behind_los_dropbacks` | numeric | Number of dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws. |
| `right_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the right side of the field. |
| `sack_percent` | numeric | Percentage of dropbacks that ended in a sack. |
| `behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage. |
| `right_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the right side of the field. |
| `right_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the right side of the field. |
| `right_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `npa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on non-play-action dropbacks. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the right side of the field. |
| `short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws. |
| `right_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the right side of the field, per PFF charting. |
| `right_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the right side of the field. |
| `screen_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on screen passes, as charted by PFF. |
| `deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws. |
| `pa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on play-action dropbacks. |
| `center_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the center of the field. |
| `screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on screen passes. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage, as charted by PFF. |
| `right_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the right side of the field. |
| `bats` | numeric | Number of pass attempts batted down at the line of scrimmage. |
| `right_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws. |
| `short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws. |
| `center_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `left_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the left side of the field. |
| `short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws, as charted by PFF. |
| `short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws. |
| `right_short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `npa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws. |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage to the left side of the field. |
| `screen_interceptions` | numeric | Number of passes intercepted on screen passes. |
| `center_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the center of the field, expressed as a percentage. |
| `left_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the left side of the field. |
| `center_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the center of the field. |
| `left_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the left side of the field. |
| `no_screen_passing_snaps` | numeric | Number of passing snaps played excluding screen passes. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `center_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the center of the field. |
| `center_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the center of the field. |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `right_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) excluding screen passes. |
| `medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `center_behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage to the center of the field, per PFF charting. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the center of the field. |
| `left_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the left side of the field. |
| `right_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the right side of the field. |
| `left_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the left side of the field. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `right_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the right side of the field. |
| `screen_scrambles` | numeric | Number of scrambles on screen passes. |
| `left_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws, plays PFF charts as deserving of a turnover. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `left_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `left_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `screen_grades_pass` | numeric | PFF passing grade (0-100) on screen passes. |
| `center_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the center of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the center of the field. |
| `sacks` | numeric | The Number of times sacked. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `center_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the center of the field. |
| `right_short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the center of the field. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `right_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the right side of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the left side of the field. |
| `center_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `npa_qb_rating` | numeric | Traditional NFL passer rating on non-play-action dropbacks. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws. |
| `no_screen_grades_pass` | numeric | PFF passing grade (0-100) excluding screen passes. |
| `medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the left side of the field. |
| `deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws, per PFF charting. |
| `pa_avg_time_to_throw` | numeric | Average time from snap to release in seconds on play-action dropbacks. |
| `left_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the left side of the field. |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws. |
| `left_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the left side of the field. |
| `short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws. |
| `center_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws, per PFF charting. |
| `right_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the right side of the field. |
| `center_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the center of the field. |
| `right_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the right side of the field. |
| `behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage. |
| `right_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field, as charted by PFF. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws. |
| `npa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on non-play-action dropbacks. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws. |
| `short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws. |
| `right_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the right side of the field, expressed as a percentage. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `center_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the center of the field. |
| `npa_drops` | numeric | Number of catchable passes dropped by receivers on non-play-action dropbacks. |
| `center_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage to the center of the field. |
| `right_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `center_short_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the left side of the field. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `center_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the center of the field. |
| `right_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws. |
| `right_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the right side of the field. |
| `completions` | numeric | The number of completed passes. |
| `medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws. |
| `left_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the left side of the field. |
| `screen_yards` | numeric | Passing yards gained on screen passes. |
| `right_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `center_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `left_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the left side of the field. |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `no_screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers excluding screen passes. |
| `center_short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws to the center of the field. |
| `right_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage to the right side of the field. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `left_short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws to the left side of the field. |
| `deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage to the right side of the field. |
| `center_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the center of the field. |
| `center_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the center of the field, expressed as a percentage. |
| `no_screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) excluding screen passes. |
| `right_deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws to the right side of the field. |
| `left_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the left side of the field. |
| `deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws. |
| `left_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the left side of the field. |
| `npa_passing_snaps` | numeric | Number of passing snaps played on non-play-action dropbacks. |
| `screen_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on screen passes. |
| `screen_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on screen passes. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `yards` | numeric | The number of receiving yards |
| `right_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `screen_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on screen passes. |
| `right_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the right side of the field. |
| `npa_spikes` | numeric | Number of clock-stopping spikes on non-play-action dropbacks. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on screen passes, as charted by PFF. |
| `left_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the left side of the field, as charted by PFF. |
| `left_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the left side of the field. |
| `deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws. |
| `left_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the left side of the field. |
| `medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws. |
| `no_screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) excluding screen passes, as charted by PFF. |
| `screen_drops` | numeric | Number of catchable passes dropped by receivers on screen passes. |
| `left_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the left side of the field. |
| `accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF. |
| `right_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the right side of the field. |
| `screen_ypa` | numeric | Yards gained per pass attempt on screen passes. |
| `medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `npa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on non-play-action dropbacks, per PFF charting. |
| `behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage. |
| `left_short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws to the left side of the field. |
| `center_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the center of the field. |
| `center_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the center of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the right side of the field. |
| `right_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the right side of the field. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `right_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the right side of the field. |
| `screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on screen passes. |
| `right_behind_los_interceptions` | numeric | Number of passes intercepted on throws behind the line of scrimmage to the right side of the field. |
| `behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage. |
| `center_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the center of the field. |
| `left_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `npa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on non-play-action dropbacks, as charted by PFF. |
| `center_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the center of the field. |
| `scrambles` | numeric | Number of scrambles. |
| `right_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `left_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the left side of the field. |
| `right_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the right side of the field. |
| `left_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the left side of the field. |
| `pa_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on play-action dropbacks. |
| `center_deep_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on deep (20+ air yards) throws to the center of the field, per PFF charting. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the left side of the field. |
| `short_attempts` | numeric | Number of pass attempts on short (0-9 air yards) throws. |
| `right_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the right side of the field. |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `left_behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage to the left side of the field. |
| `pa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on play-action dropbacks, as charted by PFF. |
| `right_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `interceptions` | numeric | The number of interceptions thrown. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in air yards on screen passes. |
| `pa_sacks` | numeric | Number of sacks taken on play-action dropbacks. |
| `short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws, plays PFF charts as deserving of a turnover. |
| `screen_passing_snaps` | numeric | Number of passing snaps played on screen passes. |
| `right_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the right side of the field. |
| `drop_rate` | numeric | Percentage of catchable passes dropped by receivers. |
| `right_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `no_screen_grades_run` | numeric | PFF rushing grade for the player (0-100) excluding screen passes. |
| `right_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the right side of the field. |
| `medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws. |
| `center_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the center of the field, expressed as a percentage. |
| `left_short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage. |
| `short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `no_screen_first_downs` | numeric | Number of passing first downs gained excluding screen passes. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `left_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the left side of the field. |
| `left_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the left side of the field. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pa_ypa` | numeric | Yards gained per pass attempt on play-action dropbacks. |
| `behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage. |
| `npa_scrambles` | numeric | Number of scrambles on non-play-action dropbacks. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `behind_los_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on throws behind the line of scrimmage. |
| `left_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the left side of the field. |
| `left_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `npa_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on non-play-action dropbacks. |
| `center_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the center of the field. |
| `pa_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `left_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the left side of the field. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `left_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the left side of the field. |
| `center_short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws to the center of the field. |
| `center_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the center of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in air yards on short (0-9 air yards) throws. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `left_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the left side of the field. |
| `deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws. |
| `center_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the center of the field, plays PFF charts as deserving of a turnover. |
| `medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws. |
| `left_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the left side of the field. |
| `deep_first_downs` | numeric | Number of passing first downs gained on deep (20+ air yards) throws. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `left_medium_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on medium (10-19 air yards) throws to the left side of the field. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `center_deep_spikes` | numeric | Number of clock-stopping spikes on deep (20+ air yards) throws to the center of the field. |
| `center_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `left_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_touchdowns` | numeric | Number of passing touchdowns thrown on deep (20+ air yards) throws to the center of the field. |
| `right_deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws to the right side of the field. |
| `right_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_big_time_throws` | numeric | Number of big-time throws on short (0-9 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `short_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on short (0-9 air yards) throws. |
| `no_screen_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts excluding screen passes, per PFF charting. |
| `left_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the left side of the field. |
| `right_deep_passing_snaps` | numeric | Number of passing snaps played on deep (20+ air yards) throws to the right side of the field. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `right_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the right side of the field, per PFF charting. |
| `center_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the center of the field. |
| `right_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the right side of the field. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `center_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `npa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on non-play-action dropbacks. |
| `behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage. |
| `right_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the right side of the field, per PFF charting. |
| `right_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `right_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the right side of the field. |
| `deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on short (0-9 air yards) throws to the left side of the field, plays PFF charts as deserving of a turnover. |
| `qb_rating` | numeric | Traditional NFL passer rating. |
| `center_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the center of the field. |
| `right_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `left_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the left side of the field. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `short_ypa` | numeric | Yards gained per pass attempt on short (0-9 air yards) throws. |
| `medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws. |
| `left_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the left side of the field. |
| `right_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the right side of the field. |
| `completion_percent` | numeric | Percentage of pass attempts completed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `behind_los_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `screen_completions` | numeric | Number of completed passes on screen passes. |
| `screen_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on screen passes, per PFF charting. |
| `npa_grades_run` | numeric | PFF rushing grade for the player (0-100) on non-play-action dropbacks. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_screen_interceptions` | numeric | Number of passes intercepted excluding screen passes. |
| `medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws. |
| `center_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the center of the field. |
| `behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage. |
| `right_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the right side of the field. |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `center_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the center of the field. |
| `npa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `left_deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `left_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the left side of the field. |
| `center_medium_touchdowns` | numeric | Number of passing touchdowns thrown on medium (10-19 air yards) throws to the center of the field. |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `no_screen_sacks` | numeric | Number of sacks taken excluding screen passes. |
| `center_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the center of the field. |
| `left_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `right_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the right side of the field, expressed as a percentage. |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in air yards on deep (20+ air yards) throws to the right side of the field. |
| `behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage. |
| `center_deep_yards` | numeric | Passing yards gained on deep (20+ air yards) throws to the center of the field. |
| `short_dropbacks` | numeric | Number of dropbacks on short (0-9 air yards) throws. |
| `no_screen_big_time_throws` | numeric | Number of big-time throws excluding screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `left_deep_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `center_behind_los_avg_time_to_throw` | numeric | Average time from snap to release in seconds on throws behind the line of scrimmage to the center of the field. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in air yards on throws behind the line of scrimmage. |
| `left_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the left side of the field. |
| `medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws, per PFF charting. |
| `left_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `npa_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on non-play-action dropbacks. |
| `medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws, plays PFF charts as deserving of a turnover. |
| `behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage, per PFF charting. |
| `left_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the left side of the field. |
| `left_short_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the center of the field. |
| `npa_attempts` | numeric | Number of pass attempts on non-play-action dropbacks. |
| `right_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the right side of the field. |
| `screen_qb_rating` | numeric | Traditional NFL passer rating on screen passes. |
| `medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws. |
| `center_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the center of the field. |
| `center_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the center of the field. |
| `center_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the center of the field. |
| `npa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `center_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the center of the field. |
| `pa_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on play-action dropbacks. |
| `pa_attempts` | numeric | Number of pass attempts on play-action dropbacks. |
| `behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage. |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `npa_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on non-play-action dropbacks, as charted by PFF. |
| `left_short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws to the left side of the field, as charted by PFF. |
| `deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `no_screen_avg_time_to_throw` | numeric | Average time from snap to release in seconds excluding screen passes. |
| `center_deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws to the center of the field. |
| `behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage. |
| `pa_yards` | numeric | Passing yards gained on play-action dropbacks. |
| `right_short_interceptions` | numeric | Number of passes intercepted on short (0-9 air yards) throws to the right side of the field. |
| `left_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the left side of the field. |
| `right_medium_sacks` | numeric | Number of sacks taken on medium (10-19 air yards) throws to the right side of the field. |
| `npa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on non-play-action dropbacks. |
| `right_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the right side of the field, plays PFF charts as deserving of a turnover. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_screen_scrambles` | numeric | Number of scrambles excluding screen passes. |
| `pa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `right_medium_spikes` | numeric | Number of clock-stopping spikes on medium (10-19 air yards) throws to the right side of the field. |
| `behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage, as charted by PFF. |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `left_medium_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws. |
| `deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws, as charted by PFF. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `right_medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws to the right side of the field. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws. |
| `deep_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on deep (20+ air yards) throws. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `pa_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on play-action dropbacks, as charted by PFF. |
| `deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws. |
| `pa_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on play-action dropbacks. |
| `screen_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on screen passes. |
| `left_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the left side of the field. |
| `no_screen_dropbacks` | numeric | Number of dropbacks excluding screen passes. |
| `right_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the right side of the field. |
| `no_screen_dropbacks_percent` | numeric | Share of the player's total dropbacks that came excluding screen passes, expressed as a percentage. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws, expressed as a percentage. |
| `right_behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the left side of the field. |
| `right_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the right side of the field, as charted by PFF. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `right_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the right side of the field, expressed as a percentage. |
| `center_short_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `npa_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on non-play-action dropbacks. |
| `short_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on short (0-9 air yards) throws, as charted by PFF. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `right_medium_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on medium (10-19 air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the left side of the field. |
| `center_behind_los_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on throws behind the line of scrimmage to the center of the field. |
| `pa_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on play-action dropbacks. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `right_behind_los_ypa` | numeric | Yards gained per pass attempt on throws behind the line of scrimmage to the right side of the field. |
| `deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws. |
| `left_medium_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `passing_snaps` | numeric | Number of passing snaps played. |
| `pa_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) on play-action dropbacks. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards gained per pass attempt. |
| `right_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the right side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `center_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the center of the field. |
| `drops` | numeric | Throws dropped |
| `center_deep_thrown_aways` | numeric | Number of intentional throwaways on deep (20+ air yards) throws to the center of the field. |
| `short_thrown_aways` | numeric | Number of intentional throwaways on short (0-9 air yards) throws. |
| `left_short_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on short (0-9 air yards) throws to the left side of the field, per PFF charting. |
| `no_screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays excluding screen passes, plays PFF charts as deserving of a turnover. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `right_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the right side of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws, as charted by PFF. |
| `center_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the center of the field. |
| `center_medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws to the center of the field. |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `right_behind_los_yards` | numeric | Passing yards gained on throws behind the line of scrimmage to the right side of the field. |
| `right_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the right side of the field. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `left_medium_attempts` | numeric | Number of pass attempts on medium (10-19 air yards) throws to the left side of the field. |
| `npa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on non-play-action dropbacks. |
| `no_screen_avg_depth_of_target` | numeric | Average depth of target in air yards excluding screen passes. |
| `medium_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on medium (10-19 air yards) throws. |
| `left_medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws to the left side of the field. |
| `pa_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on play-action dropbacks. |
| `no_screen_completion_percent` | numeric | Percentage of pass attempts completed excluding screen passes. |
| `left_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the left side of the field. |
| `pa_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on play-action dropbacks, as charted by PFF. |
| `medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws. |
| `center_short_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on short (0-9 air yards) throws to the center of the field. |
| `left_behind_los_passing_snaps` | numeric | Number of passing snaps played on throws behind the line of scrimmage to the left side of the field. |
| `left_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the left side of the field. |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `pa_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on play-action dropbacks. |
| `deep_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on deep (20+ air yards) throws. |
| `npa_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on non-play-action dropbacks, expressed as a percentage. |
| `left_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the left side of the field. |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `left_medium_scrambles` | numeric | Number of scrambles on medium (10-19 air yards) throws to the left side of the field. |
| `npa_grades_pass` | numeric | PFF passing grade (0-100) on non-play-action dropbacks. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `right_deep_completion_percent` | numeric | Percentage of pass attempts completed on deep (20+ air yards) throws to the right side of the field. |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `screen_grades_run` | numeric | PFF rushing grade for the player (0-100) on screen passes. |
| `left_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `screen_first_downs` | numeric | Number of passing first downs gained on screen passes. |
| `npa_completion_percent` | numeric | Percentage of pass attempts completed on non-play-action dropbacks. |
| `left_deep_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on deep (20+ air yards) throws to the left side of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage, plays PFF charts as deserving of a turnover. |
| `right_medium_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on medium (10-19 air yards) throws to the right side of the field. |
| `right_deep_dropbacks` | numeric | Number of dropbacks on deep (20+ air yards) throws to the right side of the field. |
| `deep_ypa` | numeric | Yards gained per pass attempt on deep (20+ air yards) throws. |
| `center_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the center of the field. |
| `left_medium_avg_time_to_throw` | numeric | Average time from snap to release in seconds on medium (10-19 air yards) throws to the left side of the field. |
| `left_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_spikes` | numeric | Number of clock-stopping spikes on throws behind the line of scrimmage to the left side of the field. |
| `no_screen_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) excluding screen passes. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `left_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `no_screen_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw excluding screen passes, as charted by PFF. |
| `center_short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws to the center of the field. |
| `center_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the center of the field. |
| `right_deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws to the right side of the field. |
| `center_behind_los_sacks` | numeric | Number of sacks taken on throws behind the line of scrimmage to the center of the field. |
| `screen_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on screen passes, plays PFF charts as deserving of a turnover. |
| `deep_completions` | numeric | Number of completed passes on deep (20+ air yards) throws. |
| `left_medium_attempts_percent` | numeric | Share of the player's total pass attempts that came on medium (10-19 air yards) throws to the left side of the field, expressed as a percentage. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `short_yards` | numeric | Passing yards gained on short (0-9 air yards) throws. |
| `behind_los_qb_rating` | numeric | Traditional NFL passer rating on throws behind the line of scrimmage. |
| `right_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the right side of the field. |
| `right_behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage to the right side of the field. |
| `center_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the center of the field. |
| `center_behind_los_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `center_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the center of the field, plays PFF charts as deserving of a turnover. |
| `left_medium_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on medium (10-19 air yards) throws to the left side of the field. |
| `behind_los_completion_percent` | numeric | Percentage of pass attempts completed on throws behind the line of scrimmage. |
| `left_deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws to the left side of the field, expressed as a percentage. |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `center_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `npa_avg_depth_of_target` | numeric | Average depth of target in air yards on non-play-action dropbacks. |
| `npa_dropbacks` | numeric | Number of dropbacks on non-play-action dropbacks. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the left side of the field. |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `behind_los_thrown_aways` | numeric | Number of intentional throwaways on throws behind the line of scrimmage. |
| `center_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws. |
| `left_deep_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on deep (20+ air yards) throws to the left side of the field, per PFF charting. |
| `center_behind_los_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on throws behind the line of scrimmage to the center of the field. |
| `left_behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `pa_drops` | numeric | Number of catchable passes dropped by receivers on play-action dropbacks. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `right_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the right side of the field. |
| `pa_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on play-action dropbacks, per PFF charting. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `right_medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws to the right side of the field. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `center_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the center of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the left side of the field. |
| `screen_big_time_throws` | numeric | Number of big-time throws on screen passes, per PFF's highest-value, highest-difficulty throw designation. |
| `left_short_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on short (0-9 air yards) throws to the left side of the field. |
| `left_behind_los_scrambles` | numeric | Number of scrambles on throws behind the line of scrimmage to the left side of the field. |
| `left_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the left side of the field, per PFF charting. |
| `screen_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on screen passes, as charted by PFF. |
| `right_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage. |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `npa_touchdowns` | numeric | Number of passing touchdowns thrown on non-play-action dropbacks. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `short_first_downs` | numeric | Number of passing first downs gained on short (0-9 air yards) throws. |
| `left_deep_attempts` | numeric | Number of pass attempts on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_attempts` | numeric | Number of pass attempts on throws behind the line of scrimmage to the right side of the field. |
| `screen_attempts` | numeric | Number of pass attempts on screen passes. |
| `screen_dropbacks` | numeric | Number of dropbacks on screen passes. |
| `right_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the right side of the field. |
| `left_behind_los_attempts_percent` | numeric | Share of the player's total pass attempts that came on throws behind the line of scrimmage to the left side of the field, expressed as a percentage. |
| `behind_los_big_time_throws` | numeric | Number of big-time throws on throws behind the line of scrimmage, per PFF's highest-value, highest-difficulty throw designation. |
| `center_deep_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on deep (20+ air yards) throws to the center of the field. |
| `left_medium_big_time_throws` | numeric | Number of big-time throws on medium (10-19 air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `right_behind_los_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on throws behind the line of scrimmage to the right side of the field. |
| `left_medium_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `no_screen_yards` | numeric | Passing yards gained excluding screen passes. |
| `right_deep_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on deep (20+ air yards) throws to the right side of the field, plays PFF charts as deserving of a turnover. |
| `left_short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws to the left side of the field. |
| `left_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the left side of the field, as charted by PFF. |
| `left_behind_los_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on throws behind the line of scrimmage to the left side of the field, per PFF charting. |
| `short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws. |
| `right_short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws to the right side of the field. |
| `right_short_avg_time_to_throw` | numeric | Average time from snap to release in seconds on short (0-9 air yards) throws to the right side of the field. |
| `pa_scrambles` | numeric | Number of scrambles on play-action dropbacks. |
| `right_medium_thrown_aways` | numeric | Number of intentional throwaways on medium (10-19 air yards) throws to the right side of the field. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `center_short_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on short (0-9 air yards) throws to the center of the field. |
| `right_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the right side of the field. |
| `center_short_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on short (0-9 air yards) throws to the center of the field, as charted by PFF. |
| `center_medium_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on medium (10-19 air yards) throws to the center of the field. |
| `medium_drops` | numeric | Number of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `center_medium_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on medium (10-19 air yards) throws to the center of the field, as charted by PFF. |
| `center_short_sacks` | numeric | Number of sacks taken on short (0-9 air yards) throws to the center of the field. |
| `pa_completion_percent` | numeric | Percentage of pass attempts completed on play-action dropbacks. |
| `deep_attempts_percent` | numeric | Share of the player's total pass attempts that came on deep (20+ air yards) throws, expressed as a percentage. |
| `center_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `left_short_passing_snaps` | numeric | Number of passing snaps played on short (0-9 air yards) throws to the left side of the field. |
| `center_medium_grades_pass` | numeric | PFF passing grade (0-100) on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws to the center of the field. |
| `pa_avg_depth_of_target` | numeric | Average depth of target in air yards on play-action dropbacks. |
| `pa_interceptions` | numeric | Number of passes intercepted on play-action dropbacks. |
| `no_screen_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF excluding screen passes. |
| `behind_los_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on throws behind the line of scrimmage. |
| `no_screen_spikes` | numeric | Number of clock-stopping spikes excluding screen passes. |
| `center_short_completion_percent` | numeric | Percentage of pass attempts completed on short (0-9 air yards) throws to the center of the field. |
| `pa_dropbacks` | numeric | Number of dropbacks on play-action dropbacks. |
| `left_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the left side of the field, as charted by PFF. |
| `center_short_grades_pass` | numeric | PFF passing grade (0-100) on short (0-9 air yards) throws to the center of the field. |
| `left_short_attempts_percent` | numeric | Share of the player's total pass attempts that came on short (0-9 air yards) throws to the left side of the field, expressed as a percentage. |
| `left_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `right_short_completions` | numeric | Number of completed passes on short (0-9 air yards) throws to the right side of the field. |
| `center_behind_los_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on throws behind the line of scrimmage to the center of the field, as charted by PFF. |
| `right_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the right side of the field. |
| `center_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the center of the field. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `npa_big_time_throws` | numeric | Number of big-time throws on non-play-action dropbacks, per PFF's highest-value, highest-difficulty throw designation. |
| `deep_avg_time_to_throw` | numeric | Average time from snap to release in seconds on deep (20+ air yards) throws. |
| `no_screen_sack_percent` | numeric | Percentage of dropbacks that ended in a sack excluding screen passes. |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `left_deep_drops` | numeric | Number of catchable passes dropped by receivers on deep (20+ air yards) throws to the left side of the field. |
| `right_behind_los_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on throws behind the line of scrimmage to the right side of the field, as charted by PFF. |
| `pa_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on play-action dropbacks. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `center_deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws to the center of the field. |
| `center_deep_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on deep (20+ air yards) throws to the center of the field, as charted by PFF. |
| `no_screen_attempts` | numeric | Number of pass attempts excluding screen passes. |
| `pa_passing_snaps` | numeric | Number of passing snaps played on play-action dropbacks. |
| `aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways), as charted by PFF. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `npa_completions` | numeric | Number of completed passes on non-play-action dropbacks. |
| `short_spikes` | numeric | Number of clock-stopping spikes on short (0-9 air yards) throws. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `right_behind_los_completions` | numeric | Number of completed passes on throws behind the line of scrimmage to the right side of the field. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `center_behind_los_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on throws behind the line of scrimmage to the center of the field. |
| `short_drops` | numeric | Number of catchable passes dropped by receivers on short (0-9 air yards) throws. |
| `deep_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on deep (20+ air yards) throws, as charted by PFF. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in air yards on medium (10-19 air yards) throws to the right side of the field. |
| `short_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on short (0-9 air yards) throws. |
| `screen_touchdowns` | numeric | Number of passing touchdowns thrown on screen passes. |
| `center_deep_qb_rating` | numeric | Traditional NFL passer rating on deep (20+ air yards) throws to the center of the field. |
| `left_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the left side of the field. |
| `center_deep_sacks` | numeric | Number of sacks taken on deep (20+ air yards) throws to the center of the field. |
| `left_deep_big_time_throws` | numeric | Number of big-time throws on deep (20+ air yards) throws to the left side of the field, per PFF's highest-value, highest-difficulty throw designation. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `center_medium_first_downs` | numeric | Number of passing first downs gained on medium (10-19 air yards) throws to the center of the field. |
| `center_medium_dropbacks` | numeric | Number of dropbacks on medium (10-19 air yards) throws to the center of the field. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `npa_first_downs` | numeric | Number of passing first downs gained on non-play-action dropbacks. |
| `no_screen_touchdowns` | numeric | Number of passing touchdowns thrown excluding screen passes. |
| `right_deep_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on deep (20+ air yards) throws to the right side of the field, as charted by PFF. |
| `left_short_touchdowns` | numeric | Number of passing touchdowns thrown on short (0-9 air yards) throws to the left side of the field. |
| `medium_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on medium (10-19 air yards) throws. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `npa_yards` | numeric | Passing yards gained on non-play-action dropbacks. |
| `left_medium_ypa` | numeric | Yards gained per pass attempt on medium (10-19 air yards) throws to the left side of the field. |
| `right_medium_passing_snaps` | numeric | Number of passing snaps played on medium (10-19 air yards) throws to the right side of the field. |
| `no_screen_ypa` | numeric | Yards gained per pass attempt excluding screen passes. |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `center_short_scrambles` | numeric | Number of scrambles on short (0-9 air yards) throws to the center of the field. |
| `npa_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on non-play-action dropbacks, plays PFF charts as deserving of a turnover. |
| `left_behind_los_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on throws behind the line of scrimmage to the left side of the field, plays PFF charts as deserving of a turnover. |
| `center_medium_yards` | numeric | Passing yards gained on medium (10-19 air yards) throws to the center of the field. |
| `center_deep_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on deep (20+ air yards) throws to the center of the field. |
| `npa_interceptions` | numeric | Number of passes intercepted on non-play-action dropbacks. |
| `left_medium_interceptions` | numeric | Number of passes intercepted on medium (10-19 air yards) throws to the left side of the field. |
| `right_deep_interceptions` | numeric | Number of passes intercepted on deep (20+ air yards) throws to the right side of the field. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `medium_completion_percent` | numeric | Percentage of pass attempts completed on medium (10-19 air yards) throws. |
| `right_behind_los_grades_pass` | numeric | PFF passing grade (0-100) on throws behind the line of scrimmage to the right side of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on deep (20+ air yards) throws. |
| `no_screen_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack excluding screen passes. |
| `behind_los_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on throws behind the line of scrimmage. |
| `center_deep_scrambles` | numeric | Number of scrambles on deep (20+ air yards) throws to the center of the field. |
| `center_short_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on short (0-9 air yards) throws to the center of the field, per PFF charting. |
| `behind_los_touchdowns` | numeric | Number of passing touchdowns thrown on throws behind the line of scrimmage. |
| `left_medium_qb_rating` | numeric | Traditional NFL passer rating on medium (10-19 air yards) throws to the left side of the field. |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `screen_sacks` | numeric | Number of sacks taken on screen passes. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |
| `right_medium_completions` | numeric | Number of completed passes on medium (10-19 air yards) throws to the right side of the field. |
| `right_behind_los_drops` | numeric | Number of catchable passes dropped by receivers on throws behind the line of scrimmage to the right side of the field. |
| `left_behind_los_first_downs` | numeric | Number of passing first downs gained on throws behind the line of scrimmage to the left side of the field. |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `short_qb_rating` | numeric | Traditional NFL passer rating on short (0-9 air yards) throws. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `center_medium_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on medium (10-19 air yards) throws to the center of the field, per PFF charting. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `left_deep_grades_pass` | numeric | PFF passing grade (0-100) on deep (20+ air yards) throws to the left side of the field. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on screen passes. |
| `screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on screen passes. |
| `no_screen_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) excluding screen passes. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `no_screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) excluding screen passes. |
| `pa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `screen_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on screen passes. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `npa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `pa_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) on play-action dropbacks. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `npa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on non-play-action dropbacks. |
| `npa_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `no_screen_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) excluding screen passes. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `pa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on play-action dropbacks. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on screen passes. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_screen_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) excluding screen passes. |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `npa_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) on non-play-action dropbacks. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pa_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on play-action dropbacks. |
| `screen_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on screen passes. |
| `npa_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) on non-play-action dropbacks. |
| `pa_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on play-action dropbacks. |
| `pa_grades_defense` | character | PFF overall defense grade for the player (0-100) on play-action dropbacks. |
| `npa_grades_defense` | numeric | PFF overall defense grade for the player (0-100) on non-play-action dropbacks. |
| `screen_grades_defense` | character | PFF overall defense grade for the player (0-100) on screen passes. |
| `screen_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on screen passes. |
| `no_screen_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_defense` | numeric | PFF overall defense grade for the player (0-100) excluding screen passes. |
| `no_screen_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) excluding screen passes. |
| `npa_grades_run_defense` | numeric | PFF run-defense grade for the player (0-100) on non-play-action dropbacks. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_passing_detail(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_passing_pressure`

League-wide passing-under-pressure leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/pressure`

**Valid URL:** [https://api.pff.com/v1/facet/passing/pressure?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/pressure?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `no_blitz_completion_percent` | numeric | Percentage of pass attempts completed when not blitzed. |
| `grades_offense` | numeric | PFF overall offense grade for the player (0-100). |
| `no_pressure_scrambles` | numeric | Number of scrambles from a clean pocket (no pressure). |
| `blitz_touchdowns` | numeric | Number of passing touchdowns thrown when blitzed. |
| `pressure_yards` | numeric | Passing yards gained when under pressure. |
| `no_pressure_spikes` | numeric | Number of clock-stopping spikes from a clean pocket (no pressure). |
| `blitz_ypa` | numeric | Yards gained per pass attempt when blitzed. |
| `no_blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when not blitzed. |
| `blitz_qb_rating` | numeric | Traditional NFL passer rating when blitzed. |
| `no_pressure_thrown_aways` | numeric | Number of intentional throwaways from a clean pocket (no pressure). |
| `no_blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when not blitzed. |
| `no_blitz_drops` | numeric | Number of catchable passes dropped by receivers when not blitzed. |
| `no_pressure_completion_percent` | numeric | Percentage of pass attempts completed from a clean pocket (no pressure). |
| `blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when blitzed, as charted by PFF. |
| `pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) when under pressure. |
| `no_blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when not blitzed. |
| `no_pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage from a clean pocket (no pressure). |
| `pressure_completions` | numeric | Number of completed passes when under pressure. |
| `blitz_big_time_throws` | numeric | Number of big-time throws when blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when not blitzed. |
| `blitz_spikes` | numeric | Number of clock-stopping spikes when blitzed. |
| `no_pressure_completions` | numeric | Number of completed passes from a clean pocket (no pressure). |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `no_pressure_passing_snaps` | numeric | Number of passing snaps played from a clean pocket (no pressure). |
| `no_blitz_first_downs` | numeric | Number of passing first downs gained when not blitzed. |
| `blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when blitzed. |
| `no_pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) from a clean pocket (no pressure). |
| `pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when under pressure, as charted by PFF. |
| `blitz_sacks` | numeric | Number of sacks taken when blitzed. |
| `no_pressure_interceptions` | numeric | Number of passes intercepted from a clean pocket (no pressure). |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when under pressure. |
| `blitz_completions` | numeric | Number of completed passes when blitzed. |
| `blitz_attempts` | numeric | Number of pass attempts when blitzed. |
| `pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `pressure_sacks` | numeric | Number of sacks taken when under pressure. |
| `no_blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when not blitzed. |
| `no_pressure_ypa` | numeric | Yards gained per pass attempt from a clean pocket (no pressure). |
| `pressure_passing_snaps` | numeric | Number of passing snaps played when under pressure. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `pressure_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when under pressure. |
| `blitz_thrown_aways` | numeric | Number of intentional throwaways when blitzed. |
| `no_pressure_drops` | numeric | Number of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when under pressure. |
| `pressure_scrambles` | numeric | Number of scrambles when under pressure. |
| `blitz_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when blitzed. |
| `pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when under pressure. |
| `blitz_completion_percent` | numeric | Percentage of pass attempts completed when blitzed. |
| `no_pressure_first_downs` | numeric | Number of passing first downs gained from a clean pocket (no pressure). |
| `blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when blitzed. |
| `no_blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when not blitzed. |
| `blitz_interceptions` | numeric | Number of passes intercepted when blitzed. |
| `no_blitz_dropbacks` | numeric | Number of dropbacks when not blitzed. |
| `no_blitz_grades_pass` | numeric | PFF passing grade (0-100) when not blitzed. |
| `no_blitz_scrambles` | numeric | Number of scrambles when not blitzed. |
| `pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers when under pressure. |
| `no_blitz_yards` | numeric | Passing yards gained when not blitzed. |
| `base_dropbacks` | numeric | Number of dropbacks across all splits, the baseline total for this facet. |
| `pressure_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack, reported within the pressure split. |
| `no_blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when not blitzed. |
| `no_pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds from a clean pocket (no pressure). |
| `pressure_dropbacks` | numeric | Number of dropbacks when under pressure. |
| `no_blitz_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_run` | numeric | PFF rushing grade for the player (0-100) from a clean pocket (no pressure). |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when blitzed, plays PFF charts as deserving of a turnover. |
| `no_blitz_touchdowns` | numeric | Number of passing touchdowns thrown when not blitzed. |
| `no_blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when not blitzed. |
| `no_pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came from a clean pocket (no pressure), expressed as a percentage. |
| `blitz_grades_pass` | numeric | PFF passing grade (0-100) when blitzed. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `blitz_avg_depth_of_target` | numeric | Average depth of target in air yards when blitzed. |
| `no_blitz_spikes` | numeric | Number of clock-stopping spikes when not blitzed. |
| `no_pressure_dropbacks` | numeric | Number of dropbacks from a clean pocket (no pressure). |
| `blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when blitzed. |
| `no_pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_pressure_drop_rate` | numeric | Percentage of catchable passes dropped by receivers from a clean pocket (no pressure). |
| `no_blitz_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when not blitzed, plays PFF charts as deserving of a turnover. |
| `pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when under pressure. |
| `blitz_first_downs` | numeric | Number of passing first downs gained when blitzed. |
| `no_blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when not blitzed, expressed as a percentage. |
| `pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays when under pressure, plays PFF charts as deserving of a turnover. |
| `no_pressure_epa` | numeric | Total expected points added (EPA) on the player's dropbacks from a clean pocket (no pressure). |
| `no_blitz_avg_time_to_throw` | numeric | Average time from snap to release in seconds when not blitzed. |
| `no_blitz_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added when not blitzed. |
| `pressure_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when under pressure, per PFF charting. |
| `no_pressure_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) from a clean pocket (no pressure), as charted by PFF. |
| `pressure_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when under pressure, expressed as a percentage. |
| `grades_run` | numeric | PFF rushing grade for the player (0-100). |
| `no_pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks from a clean pocket (no pressure), as charted by PFF. |
| `pressure_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when under pressure. |
| `pressure_completion_percent` | numeric | Percentage of pass attempts completed when under pressure. |
| `pressure_avg_depth_of_target` | numeric | Average depth of target in air yards when under pressure. |
| `blitz_epa` | numeric | Total expected points added (EPA) on the player's dropbacks when blitzed. |
| `pressure_drops` | numeric | Number of catchable passes dropped by receivers when under pressure. |
| `no_blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when not blitzed, as charted by PFF. |
| `pressure_attempts` | numeric | Number of pass attempts when under pressure. |
| `pressure_big_time_throws` | numeric | Number of big-time throws when under pressure, per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_thrown_aways` | numeric | Number of intentional throwaways when not blitzed. |
| `blitz_drops` | numeric | Number of catchable passes dropped by receivers when blitzed. |
| `no_pressure_touchdowns` | numeric | Number of passing touchdowns thrown from a clean pocket (no pressure). |
| `no_pressure_sacks` | numeric | Number of sacks taken from a clean pocket (no pressure). |
| `blitz_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when blitzed. |
| `pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack when under pressure. |
| `penalties` | numeric | Total number of penalties. |
| `no_pressure_attempts` | numeric | Number of pass attempts from a clean pocket (no pressure). |
| `no_blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when not blitzed. |
| `blitz_scrambles` | numeric | Number of scrambles when blitzed. |
| `blitz_dropbacks_percent` | numeric | Share of the player's total dropbacks that came when blitzed, expressed as a percentage. |
| `no_pressure_qb_rating` | numeric | Traditional NFL passer rating from a clean pocket (no pressure). |
| `no_blitz_passing_snaps` | numeric | Number of passing snaps played when not blitzed. |
| `no_blitz_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) when not blitzed, as charted by PFF. |
| `blitz_yards` | numeric | Passing yards gained when blitzed. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_blitz_attempts` | numeric | Number of pass attempts when not blitzed. |
| `pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when under pressure. |
| `no_blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when not blitzed, as charted by PFF. |
| `pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when under pressure, as charted by PFF. |
| `blitz_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack when blitzed. |
| `declined_penalties` | numeric | Number of declined penalties committed by the player. |
| `no_blitz_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_blitz_ypa` | numeric | Yards gained per pass attempt when not blitzed. |
| `blitz_grades_run` | numeric | PFF rushing grade for the player (0-100) when blitzed. |
| `pressure_interceptions` | numeric | Number of passes intercepted when under pressure. |
| `blitz_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when blitzed. |
| `no_pressure_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `position` | character | Primary position as reported by NFL.com |
| `blitz_grades_offense` | numeric | PFF overall offense grade for the player (0-100) when blitzed. |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `pressure_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when under pressure, as charted by PFF. |
| `pressure_grades_pass` | numeric | PFF passing grade (0-100) when under pressure. |
| `no_blitz_qb_rating` | numeric | Traditional NFL passer rating when not blitzed. |
| `blitz_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw when blitzed, as charted by PFF. |
| `pressure_ypa` | numeric | Yards gained per pass attempt when under pressure. |
| `blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when blitzed. |
| `pressure_avg_time_to_throw` | numeric | Average time from snap to release in seconds when under pressure. |
| `no_blitz_completions` | numeric | Number of completed passes when not blitzed. |
| `no_pressure_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_sack_percent` | numeric | Percentage of dropbacks that ended in a sack from a clean pocket (no pressure). |
| `player` | character | Player name |
| `no_blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when not blitzed. |
| `no_pressure_pressure_to_sack_rate` | character | Pressure-to-sack rate as reported within the no-pressure split of the PFF passing-pressure facet. |
| `no_pressure_turnover_worthy_plays` | numeric | Number of turnover-worthy plays from a clean pocket (no pressure), plays PFF charts as deserving of a turnover. |
| `pressure_first_downs` | numeric | Number of passing first downs gained when under pressure. |
| `blitz_passing_snaps` | numeric | Number of passing snaps played when blitzed. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `no_pressure_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts from a clean pocket (no pressure), per PFF charting. |
| `no_blitz_sacks` | numeric | Number of sacks taken when not blitzed. |
| `no_blitz_grades_pass_route` | numeric | PFF receiving (route) grade for the player (0-100) when not blitzed. |
| `no_pressure_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF from a clean pocket (no pressure). |
| `no_blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when not blitzed, per PFF charting. |
| `no_pressure_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw from a clean pocket (no pressure), as charted by PFF. |
| `no_pressure_grades_pass` | numeric | PFF passing grade (0-100) from a clean pocket (no pressure). |
| `no_pressure_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added from a clean pocket (no pressure). |
| `pressure_spikes` | numeric | Number of clock-stopping spikes when under pressure. |
| `pressure_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) when under pressure. |
| `pressure_qb_rating` | numeric | Traditional NFL passer rating when under pressure. |
| `blitz_bats` | numeric | Number of pass attempts batted down at the line of scrimmage when blitzed. |
| `pressure_touchdowns` | numeric | Number of passing touchdowns thrown when under pressure. |
| `pressure_thrown_aways` | numeric | Number of intentional throwaways when under pressure. |
| `no_blitz_interceptions` | numeric | Number of passes intercepted when not blitzed. |
| `blitz_dropbacks` | numeric | Number of dropbacks when blitzed. |
| `blitz_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks when blitzed, as charted by PFF. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `no_blitz_big_time_throws` | numeric | Number of big-time throws when not blitzed, per PFF's highest-value, highest-difficulty throw designation. |
| `no_pressure_avg_depth_of_target` | numeric | Average depth of target in air yards from a clean pocket (no pressure). |
| `no_pressure_yards` | numeric | Passing yards gained from a clean pocket (no pressure). |
| `blitz_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts when blitzed, per PFF charting. |
| `blitz_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF when blitzed. |
| `no_pressure_big_time_throws` | numeric | Number of big-time throws from a clean pocket (no pressure), per PFF's highest-value, highest-difficulty throw designation. |
| `no_blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_pass_block` | numeric | PFF pass-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when blitzed. |
| `pressure_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when under pressure. |
| `no_blitz_grades_hands_drop` | numeric | PFF hands (drop) grade for the player (0-100) when not blitzed. |
| `blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when blitzed. |
| `no_pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when not blitzed. |
| `blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when blitzed. |
| `pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when under pressure. |
| `no_blitz_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) when not blitzed. |
| `pressure_grades_run_block` | numeric | PFF run-blocking grade for the player (0-100) when under pressure. |
| `no_pressure_grades_screen_block` | numeric | PFF screen-blocking grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when under pressure. |
| `pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when under pressure. |
| `no_blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when not blitzed. |
| `no_blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when not blitzed. |
| `blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when blitzed. |
| `no_pressure_grades_defense` | numeric | PFF overall defense grade for the player (0-100) from a clean pocket (no pressure). |
| `no_pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) when not blitzed. |
| `blitz_grades_defense` | numeric | PFF overall defense grade for the player (0-100) when blitzed. |
| `blitz_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when blitzed. |
| `no_pressure_grades_coverage_defense` | numeric | PFF coverage grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_defense_penalty` | numeric | PFF defensive penalty grade for the player (0-100) when under pressure. |
| `pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when under pressure. |
| `blitz_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) when blitzed. |
| `no_pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when blitzed. |
| `blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when blitzed. |
| `no_pressure_grades_pass_rush_defense` | numeric | PFF pass-rush grade for the player (0-100) from a clean pocket (no pressure). |
| `pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) when not blitzed. |
| `pressure_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when under pressure. |
| `no_blitz_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) when not blitzed. |
| `no_pressure_grades_overall_tackle` | numeric | PFF overall tackling grade for the player (0-100) from a clean pocket (no pressure). |
| `no_blitz_grades_tackle` | numeric | PFF tackling grade for the player (0-100) when not blitzed. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_passing_pressure(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_passing_summary`

League-wide passing summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/passing/summary`

**Valid URL:** [https://api.pff.com/v1/facet/passing/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/passing/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `twp_rate` | numeric | Turnover-worthy-play rate. |
| `btt_rate` | numeric | Big-time-throw rate. |
| `spikes` | numeric | Clock-stopping spike plays. |
| `dropbacks` | numeric | Total quarterback dropbacks. |
| `thrown_aways` | numeric | Passes intentionally thrown away. |
| `draft_season` | numeric | Draft class (year) of the player. |
| `team_name` | character | Team name/abbreviation the player is credited to for the range. |
| `grades_pass` | numeric | PFF passing grade (0-100). |
| `hit_as_threw` | numeric | Plays where the quarterback was hit as he threw. |
| `first_downs` | numeric | Passing first downs. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `sack_percent` | numeric | Sack rate (sacks per dropback). |
| `bats` | numeric | Passes batted at the line. |
| `sacks` | numeric | Times the passer was sacked. |
| `player_game_count` | numeric | Games with at least one qualifying dropback in the requested range. |
| `eligible_season` | numeric | First eligible season for the player. |
| `completions` | numeric | Completed passes by the passer. |
| `yards` | numeric | Total passing yards gained. |
| `accuracy_percent` | numeric | Charted accuracy percentage. |
| `scrambles` | numeric | Scramble plays. |
| `interceptions` | numeric | Interceptions thrown. |
| `drop_rate` | numeric | Receiver drop rate on the quarterback's throws. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `qb_rating` | numeric | NFL passer rating. |
| `completion_percent` | numeric | Completion percentage. |
| `penalties` | numeric | Penalties charged. |
| `attempts` | numeric | Pass attempts thrown by the passer. |
| `team` | character | Team abbreviation the player is credited to for the range. |
| `declined_penalties` | numeric | Declined penalties. |
| `passing_snaps` | numeric | Number of passing snaps played. |
| `pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack. |
| `ypa` | numeric | Yards gained per pass attempt. |
| `drops` | numeric | Throws dropped |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100). |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `big_time_throws` | numeric | Number of big-time throws, per PFF's highest-value, highest-difficulty throw designation. |
| `player` | character | Player name |
| `positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `avg_depth_of_target` | numeric | Average depth of target in air yards. |
| `turnover_worthy_plays` | numeric | Number of turnover-worthy plays, plays PFF charts as deserving of a turnover. |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways), as charted by PFF. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Number of passing touchdowns thrown. |
| `def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks, as charted by PFF. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_passing_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_receiving_concept`

League-wide receiving-by-concept leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/concept`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/concept?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/concept?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `screen_caught_percent` | numeric | Percentage of targets caught on screen concepts. |
| `screen_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on screen concepts. |
| `slot_grades_pass_route` | numeric | PFF route-running (receiving) grade when aligned in the slot, 0-100. |
| `slot_avg_depth_of_target` | numeric | Average depth of target in yards downfield when aligned in the slot. |
| `slot_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added when aligned in the slot. |
| `screen_yprr` | numeric | Yards per route run on screen concepts. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `slot_routes` | numeric | Pass routes run by the player when aligned in the slot. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `screen_grades_hands_drop` | numeric | PFF hands/drop grade on screen concepts, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `screen_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on screen concepts. |
| `slot_yards_per_reception` | numeric | Average yards per reception when aligned in the slot. |
| `screen_interceptions` | numeric | Interceptions thrown on passes targeting the player on screen concepts. |
| `slot_targets_percent` | numeric | Share of the team's targets thrown to the player when aligned in the slot. |
| `screen_longest` | numeric | Longest reception in yards on screen concepts. |
| `slot_avoided_tackles` | numeric | Tackles avoided after the catch when aligned in the slot. |
| `slot_yards_after_catch` | numeric | Yards gained after the catch when aligned in the slot. |
| `slot_grades_hands_drop` | numeric | PFF hands/drop grade when aligned in the slot, 0-100. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `screen_contested_targets` | numeric | PFF-charted contested targets on screen concepts. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `screen_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on screen concepts. |
| `screen_yards` | numeric | Receiving yards gained on screen concepts. |
| `slot_route_rate` | numeric | Share of pass-play snaps on which the player ran a route when aligned in the slot. |
| `screen_drop_rate` | numeric | Share of catchable targets the player dropped on screen concepts. |
| `screen_epa` | numeric | Total expected points added on targets to the player on screen concepts. |
| `screen_grades_pass_route` | numeric | PFF route-running (receiving) grade on screen concepts, 0-100. |
| `screen_drops` | numeric | PFF-charted drops on screen concepts. |
| `screen_fumbles` | numeric | Fumbles by the player after the catch on screen concepts. |
| `slot_interceptions` | numeric | Interceptions thrown on passes targeting the player when aligned in the slot. |
| `screen_yards_after_catch` | numeric | Yards gained after the catch on screen concepts. |
| `screen_avg_depth_of_target` | numeric | Average depth of target in yards downfield on screen concepts. |
| `screen_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on screen concepts. |
| `slot_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_yprr` | numeric | Yards per route run when aligned in the slot. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all concepts. |
| `slot_longest` | numeric | Longest reception in yards when aligned in the slot. |
| `slot_drops` | numeric | PFF-charted drops when aligned in the slot. |
| `screen_routes` | numeric | Pass routes run by the player on screen concepts. |
| `slot_fumbles` | numeric | Fumbles by the player after the catch when aligned in the slot. |
| `penalties` | numeric | Total number of penalties. |
| `slot_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught when aligned in the slot. |
| `slot_pass_plays` | numeric | Pass-play snaps when aligned in the slot. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `screen_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on screen concepts. |
| `slot_first_downs` | numeric | Receptions that converted a first down when aligned in the slot. |
| `screen_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on screen concepts. |
| `position` | character | Primary position as reported by NFL.com |
| `screen_pass_blocks` | numeric | Pass-play snaps spent pass blocking on screen concepts. |
| `slot_targets` | numeric | Pass targets to the player when aligned in the slot. |
| `slot_pass_blocks` | numeric | Pass-play snaps spent pass blocking when aligned in the slot. |
| `slot_receptions` | numeric | Receptions made when aligned in the slot. |
| `screen_first_downs` | numeric | Receptions that converted a first down on screen concepts. |
| `slot_caught_percent` | numeric | Percentage of targets caught when aligned in the slot. |
| `screen_avoided_tackles` | numeric | Tackles avoided after the catch on screen concepts. |
| `player` | character | Player name |
| `slot_epa` | numeric | Total expected points added on targets to the player when aligned in the slot. |
| `slot_drop_rate` | numeric | Share of catchable targets the player dropped when aligned in the slot. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `slot_touchdowns` | numeric | Receiving touchdowns scored when aligned in the slot. |
| `slot_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception when aligned in the slot. |
| `screen_receptions` | numeric | Receptions made on screen concepts. |
| `slot_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player when aligned in the slot. |
| `slot_contested_targets` | numeric | PFF-charted contested targets when aligned in the slot. |
| `screen_yards_per_reception` | numeric | Average yards per reception on screen concepts. |
| `slot_contested_receptions` | numeric | Catches made on PFF-charted contested targets when aligned in the slot. |
| `screen_pass_plays` | numeric | Pass-play snaps on screen concepts. |
| `screen_contested_receptions` | numeric | Catches made on PFF-charted contested targets on screen concepts. |
| `screen_touchdowns` | numeric | Receiving touchdowns scored on screen concepts. |
| `screen_targets` | numeric | Pass targets to the player on screen concepts. |
| `screen_targets_percent` | numeric | Share of the team's targets thrown to the player on screen concepts. |
| `slot_yards` | numeric | Receiving yards gained when aligned in the slot. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_receiving_concept(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_receiving_coverage`

League-wide receiving-versus-coverage leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/coverage`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/coverage?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/coverage?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_receiving_coverage(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_receiving_depth`

League-wide receiving-by-depth leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/depth`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/depth?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/depth?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `left_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield). |
| `right_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `left_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield). |
| `deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield). |
| `center_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the middle of the field. |
| `right_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage. |
| `right_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `left_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield). |
| `deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `center_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield). |
| `left_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield). |
| `behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `center_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield). |
| `center_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the left third of the field. |
| `center_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `center_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield). |
| `medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield). |
| `deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `right_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield). |
| `left_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the left third of the field. |
| `right_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the right third of the field, 0-100. |
| `center_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield). |
| `center_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield). |
| `center_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield). |
| `short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield). |
| `center_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `center_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield). |
| `deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield), 0-100. |
| `center_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield), 0-100. |
| `center_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the middle of the field. |
| `medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield). |
| `center_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield). |
| `behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage. |
| `left_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the left third of the field. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield). |
| `left_short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage. |
| `right_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the right third of the field. |
| `medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `right_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield). |
| `left_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield). |
| `center_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield). |
| `short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield). |
| `left_behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield). |
| `behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage. |
| `center_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the middle of the field. |
| `center_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield). |
| `center_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the right third of the field. |
| `left_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `center_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield). |
| `left_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield) to the right third of the field. |
| `behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage. |
| `right_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `left_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the left third of the field. |
| `right_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage. |
| `right_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the left third of the field. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `center_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `left_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage. |
| `right_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield). |
| `behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage. |
| `left_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `right_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the right third of the field. |
| `right_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `right_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield) to the left third of the field. |
| `right_behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield). |
| `center_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_pass_blocks` | numeric | Pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `left_short_contested_receptions` | numeric | Catches made on PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield). |
| `left_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the left third of the field. |
| `center_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield). |
| `medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield). |
| `left_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the left third of the field, 0-100. |
| `left_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield). |
| `center_medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield). |
| `left_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield). |
| `left_short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield) to the left third of the field. |
| `short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield). |
| `right_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the right third of the field, 0-100. |
| `behind_los_contested_targets` | numeric | PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `right_behind_los_avoided_tackles` | numeric | Tackles avoided after the catch on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the right third of the field. |
| `right_behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage to the right third of the field. |
| `medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield). |
| `short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield). |
| `right_behind_los_interceptions` | numeric | Interceptions thrown on passes targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage. |
| `center_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage. |
| `right_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield). |
| `left_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield). |
| `left_short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the left third of the field. |
| `behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage. |
| `short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield). |
| `medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield). |
| `behind_los_pass_plays` | numeric | Pass-play snaps on passes thrown behind the line of scrimmage. |
| `left_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `short_avg_depth_of_target` | numeric | Average depth of target in yards downfield on short passes (0-9 yards downfield). |
| `deep_yprr` | numeric | Yards per route run on deep passes (20 or more yards downfield). |
| `left_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the left third of the field. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all depths and directions. |
| `deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield). |
| `center_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_first_downs` | numeric | Receptions that converted a first down on deep passes (20 or more yards downfield). |
| `short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield). |
| `left_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_deep_touchdowns` | numeric | Receiving touchdowns scored on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on short passes (0-9 yards downfield). |
| `medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield). |
| `center_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield) to the left third of the field. |
| `behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage. |
| `behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage. |
| `right_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_short_grades_pass_route` | numeric | PFF route-running (receiving) grade on short passes (0-9 yards downfield) to the middle of the field, 0-100. |
| `left_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_short_fumbles` | numeric | Fumbles by the player after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_routes` | numeric | Pass routes run by the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage. |
| `medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield). |
| `left_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_touchdowns` | numeric | Receiving touchdowns scored on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on medium passes (10-19 yards downfield). |
| `center_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the middle of the field. |
| `penalties` | numeric | Total number of penalties. |
| `short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield). |
| `right_deep_avg_depth_of_target` | numeric | Average depth of target in yards downfield on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_yards` | numeric | Receiving yards gained on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_targets` | numeric | Pass targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `behind_los_avg_depth_of_target` | numeric | Average depth of target in yards downfield on passes thrown behind the line of scrimmage. |
| `left_behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `center_short_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on short passes (0-9 yards downfield) to the middle of the field. |
| `left_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield). |
| `short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield). |
| `deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield). |
| `center_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield). |
| `center_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the left third of the field. |
| `center_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_deep_contested_targets` | numeric | PFF-charted contested targets on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_short_targets_percent` | numeric | Share of the team's targets thrown to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield), 0-100. |
| `right_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield). |
| `left_medium_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_short_interceptions` | numeric | Interceptions thrown on passes targeting the player on short passes (0-9 yards downfield) to the right third of the field. |
| `center_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `left_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the left third of the field. |
| `short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield). |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `center_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage. |
| `short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield). |
| `deep_drop_rate` | numeric | Share of catchable targets the player dropped on deep passes (20 or more yards downfield). |
| `left_deep_yards_per_reception` | numeric | Average yards per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_yprr` | numeric | Yards per route run on short passes (0-9 yards downfield) to the right third of the field. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `medium_longest` | numeric | Longest reception in yards on medium passes (10-19 yards downfield). |
| `left_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield). |
| `left_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the left third of the field, 0-100. |
| `right_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the right third of the field. |
| `left_behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield). |
| `behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage. |
| `center_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the middle of the field. |
| `center_short_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on short passes (0-9 yards downfield) to the middle of the field. |
| `right_medium_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the left third of the field. |
| `left_deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield). |
| `right_short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_caught_percent` | numeric | Percentage of targets caught on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_medium_fumbles` | numeric | Fumbles by the player after the catch on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage, 0-100. |
| `left_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_yprr` | numeric | Yards per route run on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_receptions` | numeric | Receptions made on short passes (0-9 yards downfield) to the right third of the field. |
| `position` | character | Primary position as reported by NFL.com |
| `right_short_contested_targets` | numeric | PFF-charted contested targets on short passes (0-9 yards downfield) to the right third of the field. |
| `center_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_behind_los_yards` | numeric | Receiving yards gained on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_behind_los_grades_hands_drop` | numeric | PFF hands/drop grade on passes thrown behind the line of scrimmage to the middle of the field, 0-100. |
| `right_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield) to the left third of the field. |
| `short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield). |
| `center_short_epa` | numeric | Total expected points added on targets to the player on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets` | numeric | Pass targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `left_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield). |
| `center_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the middle of the field. |
| `right_medium_epa` | numeric | Total expected points added on targets to the player on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield) to the middle of the field. |
| `center_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_behind_los_pass_blocks` | numeric | Pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_longest` | numeric | Longest reception in yards on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_yards` | numeric | Receiving yards gained on short passes (0-9 yards downfield). |
| `left_deep_receptions` | numeric | Receptions made on deep passes (20 or more yards downfield) to the left third of the field. |
| `deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield), 0-100. |
| `left_medium_targets` | numeric | Pass targets to the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `behind_los_grades_pass_route` | numeric | PFF route-running (receiving) grade on passes thrown behind the line of scrimmage, 0-100. |
| `right_deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage. |
| `left_medium_pass_blocks` | numeric | Pass-play snaps spent pass blocking on medium passes (10-19 yards downfield) to the left third of the field. |
| `player` | character | Player name |
| `left_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the left third of the field. |
| `deep_avoided_tackles` | numeric | Tackles avoided after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_behind_los_epa` | numeric | Total expected points added on targets to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the right third of the field. |
| `center_deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield) to the left third of the field, 0-100. |
| `right_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the right third of the field. |
| `left_deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield) to the left third of the field. |
| `left_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_short_yards_after_catch` | numeric | Yards gained after the catch on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield) to the right third of the field. |
| `medium_grades_hands_drop` | numeric | PFF hands/drop grade on medium passes (10-19 yards downfield), 0-100. |
| `right_deep_pass_blocks` | numeric | Pass-play snaps spent pass blocking on deep passes (20 or more yards downfield) to the right third of the field. |
| `medium_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on medium passes (10-19 yards downfield). |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `center_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the right third of the field. |
| `short_pass_plays` | numeric | Pass-play snaps on short passes (0-9 yards downfield). |
| `center_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `deep_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on deep passes (20 or more yards downfield). |
| `center_short_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on short passes (0-9 yards downfield) to the middle of the field. |
| `right_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the right third of the field. |
| `left_short_drop_rate` | numeric | Share of catchable targets the player dropped on short passes (0-9 yards downfield) to the left third of the field. |
| `left_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the left third of the field. |
| `left_medium_receptions` | numeric | Receptions made on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `short_first_downs` | numeric | Receptions that converted a first down on short passes (0-9 yards downfield). |
| `center_medium_targets_percent` | numeric | Share of the team's targets thrown to the player on medium passes (10-19 yards downfield) to the middle of the field. |
| `right_short_longest` | numeric | Longest reception in yards on short passes (0-9 yards downfield) to the right third of the field. |
| `left_deep_longest` | numeric | Longest reception in yards on deep passes (20 or more yards downfield) to the left third of the field. |
| `right_short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield) to the right third of the field. |
| `right_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_behind_los_receptions` | numeric | Receptions made on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_deep_targets` | numeric | Pass targets to the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_medium_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on medium passes (10-19 yards downfield) to the middle of the field. |
| `medium_drops` | numeric | PFF-charted drops on medium passes (10-19 yards downfield). |
| `left_deep_grades_hands_drop` | numeric | PFF hands/drop grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `behind_los_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on passes thrown behind the line of scrimmage. |
| `left_short_avoided_tackles` | numeric | Tackles avoided after the catch on short passes (0-9 yards downfield) to the left third of the field. |
| `right_medium_grades_pass_route` | numeric | PFF route-running (receiving) grade on medium passes (10-19 yards downfield) to the right third of the field, 0-100. |
| `right_short_grades_hands_drop` | numeric | PFF hands/drop grade on short passes (0-9 yards downfield) to the right third of the field, 0-100. |
| `left_behind_los_yards_per_reception` | numeric | Average yards per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `medium_yards_after_catch` | numeric | Yards gained after the catch on medium passes (10-19 yards downfield). |
| `center_medium_pass_plays` | numeric | Pass-play snaps on medium passes (10-19 yards downfield) to the middle of the field. |
| `left_behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage to the left third of the field. |
| `left_medium_contested_targets` | numeric | PFF-charted contested targets on medium passes (10-19 yards downfield) to the left third of the field. |
| `center_medium_contested_receptions` | numeric | Catches made on PFF-charted contested targets on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on passes thrown behind the line of scrimmage. |
| `behind_los_yards_after_catch` | numeric | Yards gained after the catch on passes thrown behind the line of scrimmage. |
| `left_deep_drops` | numeric | PFF-charted drops on deep passes (20 or more yards downfield) to the left third of the field. |
| `center_deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_routes` | numeric | Pass routes run by the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `center_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the middle of the field, 0-100. |
| `deep_yards_after_catch` | numeric | Yards gained after the catch on deep passes (20 or more yards downfield). |
| `center_behind_los_drop_rate` | numeric | Share of catchable targets the player dropped on passes thrown behind the line of scrimmage to the middle of the field. |
| `short_drops` | numeric | PFF-charted drops on short passes (0-9 yards downfield). |
| `right_deep_contested_receptions` | numeric | Catches made on PFF-charted contested targets on deep passes (20 or more yards downfield) to the right third of the field. |
| `left_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_medium_avg_depth_of_target` | numeric | Average depth of target in yards downfield on medium passes (10-19 yards downfield) to the right third of the field. |
| `right_medium_avoided_tackles` | numeric | Tackles avoided after the catch on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_behind_los_fumbles` | numeric | Fumbles by the player after the catch on passes thrown behind the line of scrimmage to the middle of the field. |
| `center_medium_first_downs` | numeric | Receptions that converted a first down on medium passes (10-19 yards downfield) to the middle of the field. |
| `behind_los_caught_percent` | numeric | Percentage of targets caught on passes thrown behind the line of scrimmage. |
| `short_routes` | numeric | Pass routes run by the player on short passes (0-9 yards downfield). |
| `left_deep_grades_pass_route` | numeric | PFF route-running (receiving) grade on deep passes (20 or more yards downfield) to the left third of the field, 0-100. |
| `medium_routes` | numeric | Pass routes run by the player on medium passes (10-19 yards downfield). |
| `medium_caught_percent` | numeric | Percentage of targets caught on medium passes (10-19 yards downfield). |
| `behind_los_contested_receptions` | numeric | Catches made on PFF-charted contested targets on passes thrown behind the line of scrimmage. |
| `behind_los_yprr` | numeric | Yards per route run on passes thrown behind the line of scrimmage. |
| `left_short_touchdowns` | numeric | Receiving touchdowns scored on short passes (0-9 yards downfield) to the left third of the field. |
| `medium_drop_rate` | numeric | Share of catchable targets the player dropped on medium passes (10-19 yards downfield). |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `left_behind_los_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_deep_pass_plays` | numeric | Pass-play snaps on deep passes (20 or more yards downfield) to the right third of the field. |
| `short_caught_percent` | numeric | Percentage of targets caught on short passes (0-9 yards downfield). |
| `right_medium_yards_per_reception` | numeric | Average yards per reception on medium passes (10-19 yards downfield) to the right third of the field. |
| `center_medium_yards` | numeric | Receiving yards gained on medium passes (10-19 yards downfield) to the middle of the field. |
| `center_deep_epa` | numeric | Total expected points added on targets to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `left_medium_interceptions` | numeric | Interceptions thrown on passes targeting the player on medium passes (10-19 yards downfield) to the left third of the field. |
| `right_deep_interceptions` | numeric | Interceptions thrown on passes targeting the player on deep passes (20 or more yards downfield) to the right third of the field. |
| `deep_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added on deep passes (20 or more yards downfield). |
| `deep_fumbles` | numeric | Fumbles by the player after the catch on deep passes (20 or more yards downfield). |
| `center_short_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught on short passes (0-9 yards downfield) to the middle of the field. |
| `center_behind_los_targets_percent` | numeric | Share of the team's targets thrown to the player on passes thrown behind the line of scrimmage to the middle of the field. |
| `right_short_yards_per_reception` | numeric | Average yards per reception on short passes (0-9 yards downfield) to the right third of the field. |
| `center_deep_targets_percent` | numeric | Share of the team's targets thrown to the player on deep passes (20 or more yards downfield) to the middle of the field. |
| `center_behind_los_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception on passes thrown behind the line of scrimmage to the middle of the field. |
| `behind_los_touchdowns` | numeric | Receiving touchdowns scored on passes thrown behind the line of scrimmage. |
| `medium_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking on medium passes (10-19 yards downfield). |
| `left_behind_los_route_rate` | numeric | Share of pass-play snaps on which the player ran a route on passes thrown behind the line of scrimmage to the left third of the field. |
| `right_behind_los_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player on passes thrown behind the line of scrimmage to the right third of the field. |
| `right_behind_los_drops` | numeric | PFF-charted drops on passes thrown behind the line of scrimmage to the right third of the field. |
| `left_behind_los_first_downs` | numeric | Receptions that converted a first down on passes thrown behind the line of scrimmage to the left third of the field. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_receiving_depth(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_receiving_scheme`

League-wide receiving-by-scheme leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/scheme`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/scheme?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/scheme?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `man_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught against man coverage. |
| `man_touchdowns` | numeric | Receiving touchdowns scored against man coverage. |
| `zone_targets_percent` | numeric | Share of the team's targets thrown to the player against zone coverage. |
| `man_interceptions` | numeric | Interceptions thrown on passes targeting the player against man coverage. |
| `zone_epa` | numeric | Total expected points added on targets to the player against zone coverage. |
| `man_avg_depth_of_target` | numeric | Average depth of target in yards downfield against man coverage. |
| `zone_pass_blocks` | numeric | Pass-play snaps spent pass blocking against zone coverage. |
| `zone_avoided_tackles` | numeric | Tackles avoided after the catch against zone coverage. |
| `man_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player against man coverage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `zone_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added against zone coverage. |
| `man_targets_percent` | numeric | Share of the team's targets thrown to the player against man coverage. |
| `man_yards_per_reception` | numeric | Average yards per reception against man coverage. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `man_drop_rate` | numeric | Share of catchable targets the player dropped against man coverage. |
| `zone_grades_pass_route` | numeric | PFF route-running (receiving) grade against zone coverage, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `zone_fumbles` | numeric | Fumbles by the player after the catch against zone coverage. |
| `man_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking against man coverage. |
| `zone_yards` | numeric | Receiving yards gained against zone coverage. |
| `man_yprr` | numeric | Yards per route run against man coverage. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `zone_drops` | numeric | PFF-charted drops against zone coverage. |
| `zone_receptions` | numeric | Receptions made against zone coverage. |
| `man_pass_plays` | numeric | Pass-play snaps against man coverage. |
| `man_epa` | numeric | Total expected points added on targets to the player against man coverage. |
| `zone_yards_per_reception` | numeric | Average yards per reception against zone coverage. |
| `man_contested_targets` | numeric | PFF-charted contested targets against man coverage. |
| `man_longest` | numeric | Longest reception in yards against man coverage. |
| `zone_yards_after_catch` | numeric | Yards gained after the catch against zone coverage. |
| `man_receptions` | numeric | Receptions made against man coverage. |
| `man_avoided_tackles` | numeric | Tackles avoided after the catch against man coverage. |
| `man_first_downs` | numeric | Receptions that converted a first down against man coverage. |
| `base_targets` | numeric | Total targets from the facet's unsplit base row, across all coverage schemes. |
| `zone_contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught against zone coverage. |
| `zone_targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player against zone coverage. |
| `man_routes` | numeric | Pass routes run by the player against man coverage. |
| `zone_grades_hands_drop` | numeric | PFF hands/drop grade against zone coverage, 0-100. |
| `man_route_rate` | numeric | Share of pass-play snaps on which the player ran a route against man coverage. |
| `zone_pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking against zone coverage. |
| `man_grades_pass_route` | numeric | PFF route-running (receiving) grade against man coverage, 0-100. |
| `penalties` | numeric | Total number of penalties. |
| `zone_first_downs` | numeric | Receptions that converted a first down against zone coverage. |
| `zone_yprr` | numeric | Yards per route run against zone coverage. |
| `man_drops` | numeric | PFF-charted drops against man coverage. |
| `zone_caught_percent` | numeric | Percentage of targets caught against zone coverage. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `man_fumbles` | numeric | Fumbles by the player after the catch against man coverage. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards gained after the catch against man coverage. |
| `man_yards` | numeric | Receiving yards gained against man coverage. |
| `zone_pass_plays` | numeric | Pass-play snaps against zone coverage. |
| `position` | character | Primary position as reported by NFL.com |
| `man_targets` | numeric | Pass targets to the player against man coverage. |
| `man_grades_hands_drop` | numeric | PFF hands/drop grade against man coverage, 0-100. |
| `man_pass_blocks` | numeric | Pass-play snaps spent pass blocking against man coverage. |
| `zone_touchdowns` | numeric | Receiving touchdowns scored against zone coverage. |
| `zone_route_rate` | numeric | Share of pass-play snaps on which the player ran a route against zone coverage. |
| `zone_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception against zone coverage. |
| `zone_avg_depth_of_target` | numeric | Average depth of target in yards downfield against zone coverage. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `zone_contested_targets` | numeric | PFF-charted contested targets against zone coverage. |
| `zone_contested_receptions` | numeric | Catches made on PFF-charted contested targets against zone coverage. |
| `man_contested_receptions` | numeric | Catches made on PFF-charted contested targets against man coverage. |
| `man_yards_after_catch_per_reception` | numeric | Average yards after the catch per reception against man coverage. |
| `zone_targets` | numeric | Pass targets to the player against zone coverage. |
| `zone_longest` | numeric | Longest reception in yards against zone coverage. |
| `man_caught_percent` | numeric | Percentage of targets caught against man coverage. |
| `zone_drop_rate` | numeric | Share of catchable targets the player dropped against zone coverage. |
| `zone_interceptions` | numeric | Interceptions thrown on passes targeting the player against zone coverage. |
| `man_positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added against man coverage. |
| `zone_routes` | numeric | Pass routes run by the player against zone coverage. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_receiving_scheme(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_receiving_summary`

League-wide receiving summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/receiving/summary`

**Valid URL:** [https://api.pff.com/v1/facet/receiving/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/receiving/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | Times targeted. |
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_catch_per_reception` | numeric | Average yards after the catch per reception. |
| `grades_pass_route` | numeric | PFF receiving/route grade (0-100). |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `yprr` | numeric | Yards per route run. |
| `wide_snaps` | numeric | Receiving snaps aligned out wide. |
| `fumbles` | numeric | Fumbles by the player after the catch. |
| `first_downs` | numeric | First downs earned by the team. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `inline_snaps` | numeric | Receiving snaps aligned inline, tight to the formation. |
| `contested_targets` | numeric | Contested targets. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `inline_rate` | numeric | Share of receiving snaps aligned inline, tight to the formation. |
| `contested_catch_rate` | numeric | Percentage of PFF-charted contested targets caught. |
| `yards` | numeric | Receiving yards. |
| `receptions` | numeric | Passes caught by the receiver. |
| `targeted_qb_rating` | numeric | NFL passer rating on throws targeting the player. |
| `interceptions` | numeric | The number of interceptions thrown. |
| `caught_percent` | numeric | Percentage of targets caught. |
| `drop_rate` | numeric | Share of catchable targets the player dropped. |
| `grades_hands_drop` | numeric | PFF hands/drop grade (0-100). |
| `slot_rate` | numeric | Share of receiving snaps aligned in the slot. |
| `slot_snaps` | numeric | Receiving snaps aligned in the slot. |
| `penalties` | numeric | Total number of penalties. |
| `wide_rate` | numeric | Share of receiving snaps aligned out wide. |
| `pass_block_rate` | numeric | Share of pass-play snaps spent pass blocking. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `route_rate` | numeric | Share of pass-play snaps on which the player ran a route. |
| `drops` | numeric | Dropped passes. |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | numeric | Longest reception in yards. |
| `pass_blocks` | numeric | Pass-play snaps spent pass blocking. |
| `routes` | numeric | Pass routes run by the receiver. |
| `pass_plays` | numeric | Pass-play snaps. |
| `yards_per_reception` | numeric | Average yards per reception. |
| `player` | character | Player name |
| `positive_epa_percent` | numeric | Percentage of the player's targets producing positive expected points added. |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `contested_receptions` | numeric | Contested catches made. |
| `yards_after_catch` | numeric | Yards after the catch. |
| `avg_depth_of_target` | numeric | Average depth of target in yards downfield. |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `avoided_tackles` | numeric | Tackles avoided after the catch. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Receiving touchdowns. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_receiving_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_rushing_direction`

League-wide rushing-by-direction leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/rushing/direction`

**Valid URL:** [https://api.pff.com/v1/facet/rushing/direction?league=nfl&season=2022](https://api.pff.com/v1/facet/rushing/direction?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `directions` | list | Nested per-direction rushing splits (attempts and results by run direction) as returned by the PFF API. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player` | character | Player name |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | numeric | Total rushing attempts across all run directions. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_rushing_direction(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_rushing_summary`

League-wide rushing summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/rushing/summary`

**Valid URL:** [https://api.pff.com/v1/facet/rushing/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/rushing/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `grades_pass_block` | numeric | PFF pass-blocking grade, 0-100. |
| `grades_offense` | numeric | PFF overall offense grade (0-100). |
| `yards_after_contact` | numeric | Yards after contact. |
| `explosive` | numeric | Runs PFF designates as explosive. |
| `grades_pass_route` | numeric | PFF route-running (receiving) grade, 0-100. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `elu_rush_mtf` | numeric | Missed tackles forced as a rusher, an input to PFF's elusive rating. |
| `breakaway_attempts` | numeric | Runs of 15 or more yards, PFF's breakaway designation. |
| `designed_yards` | numeric | Rushing yards gained on designed runs, excluding scrambles. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `yprr` | numeric | Yards per route run. |
| `breakaway_percent` | numeric | Share of rushing yards gained on breakaway runs of 15 or more yards. |
| `fumbles` | numeric | Fumbles by the ball carrier. |
| `first_downs` | numeric | Rushing first downs. |
| `elusive_rating` | numeric | PFF elusive rating. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `breakaway_yards` | numeric | Breakaway (long-run) yards. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `total_touches` | numeric | Combined carries and receptions. |
| `scramble_yards` | numeric | Rushing yards gained on scrambles. |
| `yco_attempt` | numeric | Average yards after contact per rushing attempt. |
| `yards` | numeric | Total rushing yards gained. |
| `grades_run_block` | numeric | PFF run-blocking grade, 0-100. |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `zone_attempts` | numeric | Rushing attempts on zone-scheme runs. |
| `scrambles` | numeric | Quarterback scrambles. |
| `grades_run` | numeric | PFF rushing grade (0-100). |
| `penalties` | numeric | Total number of penalties. |
| `attempts` | numeric | Rushing attempts (carries) by the runner. |
| `elu_yco` | numeric | Yards-after-contact component used in PFF's elusive rating. |
| `elu_recv_mtf` | numeric | Missed tackles forced as a receiver, an input to PFF's elusive rating. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `ypa` | numeric | Average yards per rushing attempt. |
| `drops` | numeric | Throws dropped |
| `position` | character | Primary position as reported by NFL.com |
| `grades_hands_fumble` | numeric | PFF ball-security (hands/fumble) grade, 0-100. |
| `longest` | numeric | Longest run in yards. |
| `routes` | numeric | Pass routes run by the player. |
| `player` | character | Player name |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `rec_yards` | numeric | Career receiving yards |
| `gap_attempts` | numeric | Rushing attempts on gap-scheme runs. |
| `run_plays` | numeric | Run-play snaps. |
| `avoided_tackles` | numeric | Missed tackles forced. |
| `grades_offense_penalty` | numeric | PFF offensive penalty grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `touchdowns` | numeric | Rushing touchdowns. |
| `grades_pass` | numeric | PFF passing grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_rushing_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_defense_coverage`

League-wide coverage leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/coverage`

**Valid URL:** [https://api.pff.com/v1/facet/defense/coverage?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/coverage?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `missed_tackles` | numeric | Missed tackles. |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | numeric | Team tackles. |
| `coverage_percent` | numeric | Share of pass-play snaps spent in coverage. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `dropped_ints` | numeric | Interception chances PFF charted as dropped. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | numeric | The number of receiving yards |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion. |
| `grades_coverage_defense` | numeric | PFF coverage grade, 0-100. |
| `interceptions` | numeric | The number of interceptions thrown. |
| `snap_counts_coverage` | numeric | Coverage snaps played. |
| `grades_run_defense` | numeric | PFF run-defense grade, 0-100. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `penalties` | numeric | Total number of penalties. |
| `forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `position` | character | Primary position as reported by NFL.com |
| `longest` | numeric | Longest completion allowed, in yards. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade, 0-100. |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield. |
| `pass_break_ups` | numeric | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed. |
| `assists` | numeric | Total assists. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_defense_coverage(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_defense_coverage_scheme`

League-wide coverage-by-scheme leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/coverage_scheme`

**Valid URL:** [https://api.pff.com/v1/facet/defense/coverage_scheme?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/coverage_scheme?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `man_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in man coverage. |
| `man_interceptions` | numeric | Interceptions made in coverage when in man coverage. |
| `zone_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in zone coverage. |
| `man_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in man coverage. |
| `man_dropped_ints` | numeric | Interception chances PFF charted as dropped when in man coverage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `zone_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in zone coverage. |
| `man_yards_per_reception` | numeric | Average yards allowed per reception when in man coverage. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `man_snap_counts_coverage` | numeric | Coverage snaps played when in man coverage. |
| `man_tackles` | numeric | Tackles made when in man coverage. |
| `zone_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when in zone coverage. |
| `zone_snap_counts_coverage` | numeric | Coverage snaps played when in zone coverage. |
| `zone_yards` | numeric | Receiving yards allowed when in zone coverage. |
| `man_coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage when in man coverage. |
| `zone_coverage_percent` | numeric | Share of pass-play snaps spent in coverage when in zone coverage. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `zone_receptions` | numeric | Receptions allowed into the player's coverage when in zone coverage. |
| `zone_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in zone coverage. |
| `man_snap_counts_pass_play` | numeric | Pass-play snaps when in man coverage. |
| `zone_yards_per_reception` | numeric | Average yards allowed per reception when in zone coverage. |
| `man_longest` | numeric | Longest completion allowed, in yards when in man coverage. |
| `man_assists` | numeric | Assisted tackles when in man coverage. |
| `zone_yards_after_catch` | numeric | Yards after the catch allowed when in zone coverage. |
| `man_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when in man coverage. |
| `man_receptions` | numeric | Receptions allowed into the player's coverage when in man coverage. |
| `zone_tackles` | numeric | Tackles made when in zone coverage. |
| `man_coverage_percent` | numeric | Share of pass-play snaps spent in coverage when in man coverage. |
| `penalties` | numeric | Total number of penalties. |
| `zone_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in zone coverage. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `man_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in man coverage. |
| `man_grades_coverage_defense` | numeric | PFF coverage grade when in man coverage, 0-100. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `man_yards_after_catch` | numeric | Yards after the catch allowed when in man coverage. |
| `man_pass_break_ups` | numeric | Passes broken up when in man coverage. |
| `man_yards` | numeric | Receiving yards allowed when in man coverage. |
| `position` | character | Primary position as reported by NFL.com |
| `man_targets` | numeric | Targets into the player's coverage when in man coverage. |
| `man_missed_tackle_rate` | numeric | Share of tackle attempts the player missed when in man coverage. |
| `zone_assists` | numeric | Assisted tackles when in zone coverage. |
| `zone_snap_counts_pass_play` | numeric | Pass-play snaps when in zone coverage. |
| `zone_missed_tackle_rate` | numeric | Share of tackle attempts the player missed when in zone coverage. |
| `zone_touchdowns` | numeric | Touchdowns allowed into the player's coverage when in zone coverage. |
| `zone_coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed when in zone coverage. |
| `man_coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed when in man coverage. |
| `zone_avg_depth_of_target` | numeric | Average depth of targets into the player's coverage, in yards downfield when in zone coverage. |
| `man_snap_counts_coverage_percent` | numeric | Share of the player's coverage snaps played when in man coverage. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `zone_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in zone coverage. |
| `man_forced_incompletion_rate` | numeric | Share of targets into the player's coverage with a PFF-charted forced incompletion when in man coverage. |
| `zone_targets` | numeric | Targets into the player's coverage when in zone coverage. |
| `zone_longest` | numeric | Longest completion allowed, in yards when in zone coverage. |
| `zone_dropped_ints` | numeric | Interception chances PFF charted as dropped when in zone coverage. |
| `zone_interceptions` | numeric | Interceptions made in coverage when in zone coverage. |
| `man_forced_incompletes` | numeric | Incompletions forced by the player's coverage, per PFF charting when in man coverage. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `zone_missed_tackles` | numeric | Missed tackles when in zone coverage. |
| `base_snap_counts_coverage` | numeric | Coverage snaps from the facet's unsplit base row, covering all coverage schemes. |
| `man_missed_tackles` | numeric | Missed tackles when in man coverage. |
| `zone_grades_coverage_defense` | numeric | PFF coverage grade when in zone coverage, 0-100. |
| `man_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in man coverage. |
| `zone_pass_break_ups` | numeric | Passes broken up when in zone coverage. |
| `zone_qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage when in zone coverage. |
| `man_yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap when in man coverage. |
| `zone_catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage when in zone coverage. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_defense_coverage_scheme(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_defense_coverage_matchup`

League-wide coverage matchup leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/coverage_matchup`

**Valid URL:** [https://api.pff.com/v1/facet/defense/coverage_matchup?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/coverage_matchup?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_defense_coverage_matchup(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_defense_pass_rush`

League-wide pass-rush leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/pass_rush`

**Valid URL:** [https://api.pff.com/v1/facet/defense/pass_rush?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/pass_rush?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `true_pass_set_total_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) on PFF-designated true pass sets. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `true_pass_set_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks on PFF-designated true pass sets. |
| `true_pass_set_hurries` | numeric | Quarterback hurries recorded on PFF-designated true pass sets. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `true_pass_set_sacks` | numeric | Sacks recorded on PFF-designated true pass sets. |
| `pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sacks` | numeric | The Number of times sacked. |
| `snap_counts_pass_rush` | numeric | Pass-rush snaps played. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `true_pass_set_snap_counts_pass_play` | numeric | Pass-play snaps on PFF-designated true pass sets. |
| `pass_rush_wins` | numeric | PFF-charted pass-rush wins. |
| `hurries` | numeric | Quarterback hurries recorded. |
| `pass_rush_opp` | numeric | Pass-rush snaps PFF counts as pressure opportunities. |
| `snap_counts_pass_play` | numeric | Pass-play snaps. |
| `hits` | numeric | Hits. |
| `true_pass_set_pass_rush_win_rate` | numeric | Percentage of pass-rush snaps with a PFF-charted pass-rush win on PFF-designated true pass sets. |
| `penalties` | numeric | Total number of penalties. |
| `batted_passes` | numeric | Passes batted down at the line of scrimmage. |
| `true_pass_set_hits` | numeric | Quarterback hits recorded on PFF-designated true pass sets. |
| `true_pass_set_snap_counts_pass_rush` | numeric | Pass-rush snaps played on PFF-designated true pass sets. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `true_pass_set_pass_rush_wins` | numeric | PFF-charted pass-rush wins on PFF-designated true pass sets. |
| `total_pressures` | numeric | Total pressures generated (sacks, hits, and hurries). |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_grades_pass_rush_defense` | numeric | PFF pass-rush grade on PFF-designated true pass sets, 0-100. |
| `true_pass_set_batted_passes` | numeric | Passes batted down at the line of scrimmage on PFF-designated true pass sets. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `true_pass_set_pass_rush_opp` | numeric | Pass-rush snaps PFF counts as pressure opportunities on PFF-designated true pass sets. |
| `true_pass_set_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer on PFF-designated true pass sets. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_defense_pass_rush(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_defense_run`

League-wide run-defense leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/run`

**Valid URL:** [https://api.pff.com/v1/facet/defense/run?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/run?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |
| `avg_depth_of_tackle` | numeric | Average depth downfield, in yards, at which the player made his tackles on run plays. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `forced_fumbles` | numeric | Fumbles forced by the player. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_coverage_defense` | numeric | PFF coverage grade, 0-100. |
| `grades_defense` | numeric | PFF overall defense grade, 0-100. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `grades_run_defense` | numeric | PFF run-defense grade, 0-100. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `missed_tackles` | numeric | Missed tackles. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `run_stop_opp` | numeric | Run-defense snaps PFF counts as run-stop opportunities. |
| `snap_counts_run` | numeric | Run-defense snaps played. |
| `stop_percent` | numeric | Percentage of run-stop opportunities converted into stops. |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `tackles` | numeric | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_defense_run(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_defense_summary`

League-wide defense summary leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/defense/summary`

**Valid URL:** [https://api.pff.com/v1/facet/defense/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/defense/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `interception_touchdowns` | numeric | Touchdowns scored on interception returns. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `forced_fumbles` | numeric | Forced fumbles. |
| `missed_tackles` | numeric | Missed tackles. |
| `catch_rate` | numeric | Completion percentage allowed on targets into the player's coverage. |
| `team_name` | character | Team abbreviation the player is credited to for the range. |
| `tackles` | numeric | Total tackles made by the defender. |
| `jersey_number` | character | Jersey number (string; zero-padded, e.g. "09"). |
| `snap_counts_offball` | numeric | Snaps aligned as an off-ball linebacker. |
| `snap_counts_box` | numeric | Snaps aligned in the box. |
| `sacks` | numeric | Sacks credited. |
| `snap_counts_pass_rush` | numeric | Pass-rush snaps played. |
| `player_game_count` | numeric | Games with at least one qualifying snap in the requested range. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `snap_counts_dl` | numeric | Snaps aligned on the defensive line. |
| `grades_tackle` | numeric | PFF tackling grade, 0-100. |
| `yards` | numeric | The number of receiving yards |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `grades_coverage_defense` | numeric | PFF coverage grade (0-100). |
| `hurries` | numeric | Quarterback hurries recorded. |
| `interceptions` | numeric | Interceptions made in coverage. |
| `snap_counts_coverage` | numeric | Coverage snaps played. |
| `snap_counts_dl_over_t` | numeric | Defensive-line snaps aligned head-up over the offensive tackle. |
| `snap_counts_dl_a_gap` | numeric | Defensive-line snaps aligned in the A gap. |
| `fumble_recoveries` | numeric | Opponent fumbles recovered by the player. |
| `grades_run_defense` | numeric | PFF run-defense grade (0-100). |
| `snap_counts_corner` | numeric | Snaps aligned at outside cornerback. |
| `hits` | numeric | Hits. |
| `penalties` | numeric | Total number of penalties. |
| `batted_passes` | numeric | Passes batted down at the line of scrimmage. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | numeric | Tackles that constitute an offensive failure ("stops"). |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `total_pressures` | numeric | Total quarterback pressures (sacks + hits + hurries). |
| `position` | character | Primary position as reported by NFL.com |
| `fumble_recovery_touchdowns` | numeric | Touchdowns scored on fumble recoveries. |
| `longest` | numeric | Longest completion allowed, in yards. |
| `snap_counts_slot` | numeric | Snaps aligned in the slot. |
| `missed_tackle_rate` | numeric | Share of tackle attempts the player missed. |
| `grades_defense` | numeric | PFF overall defense grade (0-100). |
| `yards_per_reception` | numeric | Average yards allowed per reception. |
| `grades_defense_penalty` | numeric | PFF defensive penalty grade, 0-100. |
| `safeties` | numeric | Safeties recorded by the player. |
| `player` | character | Player name |
| `franchise_id` | numeric | PFF franchise (team) id (integer join key). |
| `snap_counts_defense` | numeric | Total defensive snaps played. |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `snap_counts_dl_b_gap` | numeric | Defensive-line snaps aligned in the B gap. |
| `pass_break_ups` | numeric | Passes broken up. |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage. |
| `snap_counts_run_defense` | numeric | Run-defense snaps played. |
| `tackles_for_loss` | numeric | Team tackles for a loss. |
| `assists` | numeric | Assisted tackles. |
| `grades_pass_rush_defense` | numeric | PFF pass-rush grade, 0-100. |
| `player_id` | numeric | PFF player id (integer; matches the /players id and every player_id join key). |
| `snap_counts_fs` | numeric | Snaps aligned at free safety. |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage. |
| `snap_counts_dl_outside_t` | numeric | Defensive-line snaps aligned outside the offensive tackle. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_defense_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_field_goal_summary`

League-wide field-goal kicking leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/field_goal/summary`

**Valid URL:** [https://api.pff.com/v1/facet/field_goal/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/field_goal/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `twenty_attempts` | numeric | Field goals attempted from 20-29 yards. |
| `pat_percent` | numeric | Extra-point percentage. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `forty_made` | numeric | Field goals made from 40-49 yards. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `fifty_percent` | numeric | Field-goal percentage from 50 or more yards. |
| `total_made` | numeric | Total field goals made across all distances. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `one_made` | numeric | Field goals made from 1-19 yards. |
| `fifty_attempts` | numeric | Field goals attempted from 50 or more yards. |
| `forty_attempts` | numeric | Field goals attempted from 40-49 yards. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `thirty_percent` | numeric | Field-goal percentage from 30-39 yards. |
| `total_attempts` | numeric | Total field goals attempted across all distances. |
| `pat_attempts` | numeric | Extra points attempted. |
| `twenty_made` | numeric | Field goals made from 20-29 yards. |
| `one_attempts` | numeric | Field goals attempted from 1-19 yards. |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `thirty_attempts` | numeric | Field goals attempted from 30-39 yards. |
| `penalties` | numeric | Total number of penalties. |
| `pat_made` | numeric | Extra points made. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `one_percent` | numeric | Field-goal percentage from 1-19 yards. |
| `total_percent` | numeric | Overall field-goal percentage. |
| `position` | character | Primary position as reported by NFL.com |
| `twenty_percent` | numeric | Field-goal percentage from 20-29 yards. |
| `forty_percent` | numeric | Field-goal percentage from 40-49 yards. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `fifty_made` | numeric | Field goals made from 50 or more yards. |
| `thirty_made` | numeric | Field goals made from 30-39 yards. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_field_goal_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_kickoff_summary`

League-wide kickoff leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/kickoff/summary`

**Valid URL:** [https://api.pff.com/v1/facet/kickoff/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/kickoff/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `attempts_with_hangtime` | numeric | Kickoffs with a PFF-recorded hangtime. |
| `average_distance` | numeric | Average kickoff distance in yards. |
| `average_hangtime` | numeric | Average kickoff hangtime in seconds. |
| `average_starting_field_position` | numeric | Average opponent starting field position following the player's kickoffs. |
| `average_yards_per_return` | numeric | Average return yards allowed per kickoff returned. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `fair_catches` | numeric | Kickoffs fair-caught by the return team. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kicked_yards` | numeric | Total kickoff yards. |
| `kicks_returned` | numeric | Kickoffs returned by the opponent. |
| `onside_kicks` | numeric | Onside kicks attempted. |
| `penalties` | numeric | Total number of penalties. |
| `percent_returned` | numeric | Percentage of the player's kickoffs that were returned. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `return_yards` | numeric | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_hangtime` | numeric | Total kickoff hangtime in seconds. |
| `touchbacks` | numeric | Kickoffs resulting in touchbacks. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_kickoff_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_punting_summary`

League-wide punting leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/punting/summary`

**Valid URL:** [https://api.pff.com/v1/facet/punting/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/punting/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `touchbacks` | numeric | Punts resulting in touchbacks. |
| `attempts_with_hangtime` | numeric | Punts with a PFF-recorded hangtime. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `percent_returned` | numeric | Percentage of the player's punts that were returned. |
| `fair_catches` | numeric | Punts fair-caught by the return team. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `average_net_yards` | numeric | Average net punting yards per attempt. |
| `yards` | numeric | The number of receiving yards |
| `average_hangtime` | numeric | Average punt hangtime in seconds. |
| `total_net_yards` | numeric | Total net punting yards. |
| `penalties` | numeric | Total number of penalties. |
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `inside_twenties` | numeric | Punts downed inside the opponent 20-yard line. |
| `out_of_bounds` | numeric | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `average_yards_per_return` | numeric | Average return yards allowed per punt returned. |
| `total_hangtime` | numeric | Total punt hangtime in seconds. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `returns` | numeric | Punts returned by the opponent. |
| `position` | character | Primary position as reported by NFL.com |
| `long` | numeric | Longest punt in yards. |
| `blocks` | numeric | Total blocks. |
| `average_yards_per_attempt` | numeric | Average gross punting yards per attempt. |
| `player` | character | Player name |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `return_yards` | numeric | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `downeds` | numeric | Punts downed by the coverage unit. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `snaps` | numeric | Punting snaps played. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_punting_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_return_summary`

League-wide return leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/return/summary`

**Valid URL:** [https://api.pff.com/v1/facet/return/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/return/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_return` | numeric | PFF overall return grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kickoff_attempts` | numeric | Kickoff returns attempted. |
| `kickoff_fair_catches` | numeric | Kickoffs fair-caught by the player. |
| `kickoff_long` | numeric | Longest kickoff return in yards. |
| `kickoff_muffed_returns` | numeric | Kickoff returns the player muffed. |
| `kickoff_touchdowns` | numeric | Kickoff returns scoring a touchdown. |
| `kickoff_yards` | numeric | Total kickoff-return yards. |
| `kickoff_ypa` | numeric | Average yards per kickoff return. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `punt_attempts` | numeric | Punt returns attempted. |
| `punt_fair_catches` | numeric | Punts fair-caught by the player. |
| `punt_long` | numeric | Longest punt return in yards. |
| `punt_muffed_returns` | numeric | Punt returns the player muffed. |
| `punt_touchdowns` | numeric | Punt returns scoring a touchdown. |
| `punt_yards` | numeric | Total punt-return yards. |
| `punt_ypa` | numeric | Average yards per punt return. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | numeric | Total return attempts, kickoffs and punts combined. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_return_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_facet_special_summary`

League-wide special-teams leaderboard

**Endpoint URL:** `GET https://api.pff.com/v1/facet/special/summary`

**Valid URL:** [https://api.pff.com/v1/facet/special/summary?league=nfl&season=2022](https://api.pff.com/v1/facet/special/summary?league=nfl&season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug for the leaderboard commands — exactly nfl, ncaa, hs, aaf and ufl are recognised; anything else is rejected. |
| `season` | `season` |  |  | `Y` | Season for the leaderboard commands. |
| `week` | `week` |  |  | `Y` | Week filter. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |
| `game_id` | `game_id` |  |  | `Y` | Single-game filter, facet family only, forwarded uncoerced. |
| `division` | `division` |  |  | `Y` | NCAA division fan-out, facet family only, and **only honoured when league=ncaa** — for any other league it is ignored entirely. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `assists` | numeric | Total assists. |
| `declined_penalties` | numeric | Penalties committed by the player that were declined. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_fgep_kicker` | numeric | PFF field-goal and extra-point kicking grade, 0-100. |
| `grades_kickoff_kicker` | numeric | PFF kickoff kicking grade, 0-100. |
| `grades_misc_st` | numeric | PFF miscellaneous special-teams grade, 0-100. |
| `grades_special_teams_penalty` | numeric | PFF special-teams penalty grade, 0-100. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackles` | numeric | Missed tackles on special-teams plays. |
| `penalties` | numeric | Total number of penalties. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `snap_counts_field_goal` | numeric | Snaps on the field-goal and extra-point unit. |
| `snap_counts_field_goal_blocking` | numeric | Snaps on the field-goal and extra-point block unit. |
| `snap_counts_kickoff` | numeric | Snaps on the kickoff coverage unit. |
| `snap_counts_kickoff_return` | numeric | Snaps on the kickoff return unit. |
| `snap_counts_punt_coverage` | numeric | Snaps on the punt coverage unit. |
| `snap_counts_punt_return` | numeric | Snaps on the punt return unit. |
| `tackles` | numeric | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_fgep_defense` | numeric | PFF grade on field-goal and extra-point defense, 0-100. |
| `grades_fgep_offense` | numeric | PFF grade on the field-goal and extra-point protection unit, 0-100. |
| `grades_long_snap` | numeric | PFF long-snapping grade, 0-100. |
| `grades_punter` | numeric | PFF punting grade, 0-100. |
| `grades_kick_return` | numeric | PFF kickoff-return grade, 0-100. |
| `grades_punt_return` | numeric | PFF punt-return grade, 0-100. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_facet_special_summary(league='nfl', season='2022')
```

_Last validated n/a._

## `pff_api_signature_passing_time_in_pocket`

Signature stat: time in pocket

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/passing/time_in_pocket`

**Valid URL:** [https://api.pff.com/v1/facet/signature/passing/time_in_pocket?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/passing/time_in_pocket?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `more_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `more_grades_run` | numeric | PFF rushing grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_ypa` | numeric | Yards gained per pass attempt on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_passing_snaps` | numeric | Number of passing snaps played on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `dropbacks` | numeric | Number of dropbacks. |
| `less_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_first_downs` | numeric | Number of passing first downs gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_ypa` | numeric | Yards gained per pass attempt on dropbacks with time in pocket under 2.5 seconds. |
| `draft_season` | numeric | NFL season (year) in which the player was drafted, per PFF player metadata. |
| `more_grades_pass_route` | character | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_pressure_to_sack_rate` | numeric | Percentage of pressured dropbacks that ended in a sack on dropbacks with time in pocket under 2.5 seconds. |
| `less_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on dropbacks with time in pocket of 2.5 seconds or more, expressed as a percentage. |
| `less_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket under 2.5 seconds. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `avg_ttt_scrambles` | numeric | Average time in the pocket in seconds on dropbacks ending in a scramble. |
| `more_yards` | numeric | Passing yards gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_attempts` | numeric | Number of pass attempts on dropbacks with time in pocket under 2.5 seconds. |
| `less_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `more_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on dropbacks with time in pocket of 2.5 seconds or more, plays PFF charts as deserving of a turnover. |
| `more_interceptions` | numeric | Number of passes intercepted on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_sack_percent` | numeric | Percentage of dropbacks that ended in a sack on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket of 2.5 seconds or more, per PFF charting. |
| `less_completions` | numeric | Number of completed passes on dropbacks with time in pocket under 2.5 seconds. |
| `more_thrown_aways` | numeric | Number of intentional throwaways on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_btt_rate` | numeric | Big-time throws as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_big_time_throws` | numeric | Number of big-time throws on dropbacks with time in pocket of 2.5 seconds or more, per PFF's highest-value, highest-difficulty throw designation. |
| `less_qb_rating` | numeric | Traditional NFL passer rating on dropbacks with time in pocket under 2.5 seconds. |
| `less_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket under 2.5 seconds, as charted by PFF. |
| `more_attempts` | numeric | Number of pass attempts on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_game_count` | numeric | Number of games the player appeared in during the period covered. |
| `less_spikes` | numeric | Number of clock-stopping spikes on dropbacks with time in pocket under 2.5 seconds. |
| `eligible_season` | numeric | Season (year) of the player's NFL draft eligibility, per PFF player metadata. |
| `less_dropbacks_percent` | numeric | Share of the player's total dropbacks that came on dropbacks with time in pocket under 2.5 seconds, expressed as a percentage. |
| `more_qb_rating` | numeric | Traditional NFL passer rating on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_dropbacks` | numeric | Number of dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_avg_depth_of_target` | numeric | Average depth of target in air yards on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_scrambles` | numeric | Number of scrambles on dropbacks with time in pocket under 2.5 seconds. |
| `more_positive_epa_percent` | numeric | Percentage of dropbacks with positive expected points added on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_sacks` | numeric | Number of sacks taken on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass` | numeric | PFF passing grade (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_drops` | numeric | Number of catchable passes dropped by receivers on dropbacks with time in pocket under 2.5 seconds. |
| `more_sacks` | numeric | Number of sacks taken on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_first_downs` | numeric | Number of passing first downs gained on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_big_time_throws` | numeric | Number of big-time throws on dropbacks with time in pocket under 2.5 seconds, per PFF's highest-value, highest-difficulty throw designation. |
| `avg_ttt_attempts` | numeric | Average time from snap to release in seconds on dropbacks ending in a pass attempt. |
| `more_aimed_passes` | numeric | Number of aimed passes (attempts excluding spikes and throwaways) on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `less_accuracy_percent` | numeric | Percentage of aimed passes charted as accurate by PFF on dropbacks with time in pocket under 2.5 seconds. |
| `more_scrambles` | numeric | Number of scrambles on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket under 2.5 seconds. |
| `more_spikes` | numeric | Number of clock-stopping spikes on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_route` | character | PFF receiving (route) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `avg_ttt_sacks` | numeric | Average time from snap to sack in seconds on dropbacks ending in a sack. |
| `more_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `less_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run` | numeric | PFF rushing grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `position` | character | Primary position as reported by NFL.com |
| `less_touchdowns` | numeric | Number of passing touchdowns thrown on dropbacks with time in pocket under 2.5 seconds. |
| `less_yards` | numeric | Passing yards gained on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_run_block` | character | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_hands_fumble` | numeric | PFF hands (fumble) grade for the player, reflecting ball security (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_hit_as_threw` | numeric | Number of attempts on which the passer was hit as he threw on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_epa` | numeric | Total expected points added (EPA) on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `less_turnover_worthy_plays` | numeric | Number of turnover-worthy plays on dropbacks with time in pocket under 2.5 seconds, plays PFF charts as deserving of a turnover. |
| `less_completion_percent` | numeric | Percentage of pass attempts completed on dropbacks with time in pocket under 2.5 seconds. |
| `more_drops` | numeric | Number of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `player` | character | Player name |
| `more_touchdowns` | numeric | Number of passing touchdowns thrown on dropbacks with time in pocket of 2.5 seconds or more. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `more_drop_rate` | numeric | Percentage of catchable passes dropped by receivers on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_twp_rate` | numeric | Turnover-worthy plays as a percentage of qualifying attempts on dropbacks with time in pocket under 2.5 seconds, per PFF charting. |
| `more_grades_offense` | numeric | PFF overall offense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_thrown_aways` | numeric | Number of intentional throwaways on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_depth_of_target` | numeric | Average depth of target in air yards on dropbacks with time in pocket under 2.5 seconds. |
| `less_avg_time_to_throw` | numeric | Average time from snap to release in seconds on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_offense_penalty` | numeric | PFF offensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_interceptions` | numeric | Number of passes intercepted on dropbacks with time in pocket under 2.5 seconds. |
| `more_def_gen_pressures` | numeric | Number of defense-generated pressures on the player's dropbacks on dropbacks with time in pocket of 2.5 seconds or more, as charted by PFF. |
| `more_avg_time_to_throw` | numeric | Average time from snap to release in seconds on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_dropbacks` | numeric | Number of dropbacks on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_pass` | numeric | PFF passing grade (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `less_passing_snaps` | numeric | Number of passing snaps played on dropbacks with time in pocket under 2.5 seconds. |
| `more_completions` | numeric | Number of completed passes on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_block` | character | PFF run-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_bats` | numeric | Number of pass attempts batted down at the line of scrimmage on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_completion_percent` | numeric | Percentage of pass attempts completed on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_hands_drop` | character | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_hands_drop` | character | PFF hands (drop) grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_block` | character | PFF pass-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_screen_block` | character | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_screen_block` | character | PFF screen-blocking grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_coverage_defense` | character | PFF coverage grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_defense_penalty` | character | PFF defensive penalty grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_defense` | character | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_coverage_defense` | character | PFF coverage grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_defense` | character | PFF overall defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `more_grades_tackle` | character | PFF tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_pass_rush_defense` | character | PFF pass-rush grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `less_grades_tackle` | character | PFF tackling grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |
| `more_grades_overall_tackle` | character | PFF overall tackling grade for the player (0-100) on dropbacks with time in pocket of 2.5 seconds or more. |
| `less_grades_run_defense` | character | PFF run-defense grade for the player (0-100) on dropbacks with time in pocket under 2.5 seconds. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_signature_passing_time_in_pocket(league='nfl', season='2022', week='1')
```

_Last validated n/a._

## `pff_api_signature_pass_blocking_efficiency_line`

Signature stat: pass-blocking efficiency, by line

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/pass-blocking/efficiency/line`

**Valid URL:** [https://api.pff.com/v1/facet/signature/pass-blocking/efficiency/line?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/pass-blocking/efficiency/line?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `attempts` | numeric | The number of pass attempts as defined by the NFL. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `hits_allowed` | numeric | Quarterback hits allowed. |
| `hurries_allowed` | numeric | Quarterback hurries allowed. |
| `pass_snaps` | numeric | Pass-play snaps. |
| `pbe` | numeric | PFF Pass Blocking Efficiency rating, pressures allowed per pass-blocking snap weighted toward sacks. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `pressures_allowed` | numeric | Total pressures allowed (sacks, hits, and hurries). |
| `sacks_allowed` | numeric | Opponent sacks. |
| `season_id` | numeric | Unique season identifier. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_signature_pass_blocking_efficiency_line(league='nfl', season='2022', week='1')
```

_Last validated n/a._

## `pff_api_signature_defense_outside_pass_rush`

Signature stat: outside pass rush

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/defense/outside_pass_rush`

**Valid URL:** [https://api.pff.com/v1/facet/signature/defense/outside_pass_rush?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/defense/outside_pass_rush?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `lhs_sacks` | numeric | Sacks recorded when rushing from the left side. |
| `rhs_hits` | numeric | Quarterback hits recorded when rushing from the right side. |
| `rhs_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the right side. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `pass_snaps` | numeric | Pass-play snaps. |
| `lhs_hurries` | numeric | Quarterback hurries recorded when rushing from the left side. |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks. |
| `tackles` | numeric | Team tackles. |
| `rhs_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) when rushing from the right side. |
| `rhs_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer when rushing from the right side. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `lhs_pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer when rushing from the left side. |
| `lhs_pass_rush_snaps` | numeric | Pass-rush snaps played when rushing from the left side. |
| `sacks` | numeric | The Number of times sacked. |
| `lhs_assists` | numeric | Assisted tackles when rushing from the left side. |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `lhs_pressures` | numeric | Total pressures generated (sacks, hits, and hurries) when rushing from the left side. |
| `pass_rush_snaps` | numeric | Pass-rush snaps played. |
| `hurries` | numeric | Quarterback hurries recorded. |
| `rhs_pass_rush_snaps` | numeric | Pass-rush snaps played when rushing from the right side. |
| `lhs_prp` | numeric | PFF Pass Rush Productivity rating, pressure generated per pass-rush snap weighted toward sacks when rushing from the left side. |
| `hits` | numeric | Hits. |
| `lhs_hits` | numeric | Quarterback hits recorded when rushing from the left side. |
| `rhs_tackles` | numeric | Tackles made when rushing from the right side. |
| `lhs_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the left side. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense. |
| `rhs_misses` | numeric | Missed tackles when rushing from the right side. |
| `position` | character | Primary position as reported by NFL.com |
| `pressures` | numeric | Total pressures generated (sacks, hits, and hurries). |
| `misses` | numeric | Missed tackles. |
| `player` | character | Player name |
| `rhs_sacks` | numeric | Sacks recorded when rushing from the right side. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric | Share of pass-play snaps spent rushing the passer. |
| `rhs_hurries` | numeric | Quarterback hurries recorded when rushing from the right side. |
| `rhs_assists` | numeric | Assisted tackles when rushing from the right side. |
| `rhs_stops` | numeric | Stops, PFF's tackles that constitute a failed play for the offense when rushing from the right side. |
| `assists` | numeric | Total assists. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `lhs_tackles` | numeric | Tackles made when rushing from the left side. |
| `lhs_misses` | numeric | Missed tackles when rushing from the left side. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_signature_defense_outside_pass_rush(league='nfl', season='2022', week='1')
```

_Last validated n/a._

## `pff_api_signature_defense_slot_coverage`

Signature stat: slot coverage

**Endpoint URL:** `GET https://api.pff.com/v1/facet/signature/defense/slot_coverage`

**Valid URL:** [https://api.pff.com/v1/facet/signature/defense/slot_coverage?league=nfl&season=2022&week=1](https://api.pff.com/v1/facet/signature/defense/slot_coverage?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, and the SECOND positional argument of the four signature commands. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of the four signature commands. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `coverage_snaps` | numeric | Coverage snaps played while covering the slot. |
| `coverage_snaps_per_reception` | numeric | Coverage snaps played per reception allowed while covering the slot. |
| `coverage_snaps_per_target` | numeric | Coverage snaps played per target into the player's coverage while covering the slot. |
| `draft_season` | numeric | Season of the player's NFL draft class, per PFF. |
| `eligible_season` | numeric | Season of the player's NFL draft eligibility, per PFF. |
| `franchise_id` | numeric | ESPN franchise id (parsed from `franchise_ref`). |
| `interceptions` | numeric | The number of interceptions thrown. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player` | character | Player name |
| `player_game_count` | numeric | Number of games the player appeared in over the covered span. |
| `player_id` | numeric | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `position` | character | Primary position as reported by NFL.com |
| `qb_rating_against` | numeric | NFL passer rating allowed on targets into the player's coverage while covering the slot. |
| `receptions` | numeric | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `targets` | numeric | The number of pass plays where the player was the targeted receiver. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `touchdowns` | numeric | Touchdowns allowed into the player's coverage while covering the slot. |
| `yards` | numeric | The number of receiving yards |
| `yards_after_catch` | numeric | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `yards_per_coverage_snap` | numeric | Receiving yards allowed per coverage snap while covering the slot. |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_signature_defense_slot_coverage(league='nfl', season='2022', week='1')
```

_Last validated n/a._

## `pff_api_whoami`

Show what this API believes about the current credential

**Endpoint URL:** `GET https://api.pff.com/v1/auth/whoami`

**Valid URL:** [https://api.pff.com/v1/auth/whoami](https://api.pff.com/v1/auth/whoami)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns

Show what this API believes about the current credential

### Example

```python
pff_api_whoami()
```

_Last validated n/a._

## `pff_api_position_report`

One of nineteen player reports for the whole league, one row per player

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/positions/reports/{report}`

**Valid URL:** [https://api.pff.com/v2/nfl/positions/reports/offense?season=2022](https://api.pff.com/v2/nfl/positions/reports/offense?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `report` | `report` |  | `Y` |  | Which report: offense, passing, passing-depth, passing-pressure, receiving, receiving-depth, rushing, blocking, pass-blocking, run-blocking, defense, run-defense, pass-rush, coverage, special-teams, kick-returns, field-goals, punting, kickoffs — the same vocabulary as team-report. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `week` | `week` |  |  | `Y` | Narrow the report to one week of the weekGroup — or, with weekTo, to a span of weeks. |
| `weekTo` | `week_to` |  |  | `Y` | The last week of a span that starts at week; requires week. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**offense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_hands_fumble` | numeric |  |
| `grades_offense` | numeric |  |
| `grades_offense_penalty` | numeric |  |
| `grades_pass` | numeric |  |
| `grades_pass_route` | numeric |  |
| `grades_run` | numeric |  |
| `grades_run_block` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `snap_counts_pass` | integer |  |
| `snap_counts_pass_block` | integer |  |
| `snap_counts_pass_route` | integer |  |
| `snap_counts_run` | integer |  |
| `snap_counts_run_block` | integer |  |
| `snap_counts_total` | integer |  |
| `snap_counts_total_pass` | integer |  |
| `snap_counts_total_run` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_pass_block` | numeric |  |

**passing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `grades_offense` | numeric |  |
| `twp_rate` | numeric |  |
| `btt_rate` | numeric |  |
| `spikes` | integer | Spikes |
| `dropbacks` | integer |  |
| `thrown_aways` | integer |  |
| `draft_season` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_pass` | numeric |  |
| `hit_as_threw` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sack_percent` | numeric |  |
| `bats` | integer |  |
| `sacks` | integer | The Number of times sacked. |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `completions` | integer | The number of completed passes. |
| `yards` | integer | The number of receiving yards |
| `accuracy_percent` | numeric |  |
| `scrambles` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `drop_rate` | numeric |  |
| `grades_run` | numeric |  |
| `qb_rating` | numeric |  |
| `completion_percent` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `passing_snaps` | integer |  |
| `pressure_to_sack_rate` | numeric |  |
| `ypa` | numeric |  |
| `drops` | integer | Throws dropped |
| `grades_hands_fumble` | numeric |  |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `big_time_throws` | integer |  |
| `positive_epa_percent` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `avg_depth_of_target` | numeric |  |
| `turnover_worthy_plays` | integer |  |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `aimed_passes` | integer |  |
| `touchdowns` | integer |  |
| `def_gen_pressures` | integer |  |
| `npa_epa` | numeric |  |
| `npa_positive_epa_percent` | numeric |  |
| `no_screen_epa` | numeric |  |
| `no_screen_positive_epa_percent` | numeric |  |
| `pass_rate_oe` | numeric |  |
| `completion_oe` | numeric |  |
| `accuracy_oe` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**passing-depth**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `left_behind_los_accuracy_percent` | numeric |  |
| `left_short_scrambles` | integer |  |
| `center_short_first_downs` | integer |  |
| `right_short_bats` | integer |  |
| `right_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_completions` | integer |  |
| `left_medium_dropbacks` | integer |  |
| `right_medium_grades_pass` | numeric |  |
| `right_medium_hit_as_threw` | integer |  |
| `behind_los_attempts_percent` | numeric |  |
| `right_short_turnover_worthy_plays` | integer |  |
| `right_medium_qb_rating` | numeric |  |
| `deep_twp_rate` | numeric |  |
| `center_medium_sacks` | integer |  |
| `medium_interceptions` | integer |  |
| `left_deep_completions` | integer |  |
| `behind_los_spikes` | integer |  |
| `medium_aimed_passes` | integer |  |
| `center_behind_los_drops` | integer |  |
| `center_behind_los_interceptions` | integer |  |
| `behind_los_dropbacks` | integer |  |
| `left_behind_los_interceptions` | integer |  |
| `left_deep_first_downs` | integer |  |
| `deep_passing_snaps` | integer |  |
| `center_short_btt_rate` | numeric |  |
| `center_medium_thrown_aways` | integer |  |
| `center_deep_avg_depth_of_target` | numeric |  |
| `center_short_drops` | integer |  |
| `center_deep_first_downs` | integer |  |
| `left_medium_completion_percent` | numeric |  |
| `center_deep_attempts` | integer |  |
| `center_behind_los_attempts` | integer |  |
| `right_short_positive_epa_percent` | numeric |  |
| `center_deep_aimed_passes` | integer |  |
| `right_deep_grades_pass` | numeric |  |
| `right_medium_big_time_throws` | integer |  |
| `deep_sack_percent` | integer |  |
| `center_medium_pressure_to_sack_rate` | integer |  |
| `deep_sacks` | integer |  |
| `right_medium_ypa` | numeric |  |
| `left_deep_drop_rate` | numeric |  |
| `right_medium_attempts_percent` | numeric |  |
| `right_deep_btt_rate` | numeric |  |
| `center_short_attempts_percent` | numeric |  |
| `deep_drops` | integer |  |
| `center_behind_los_big_time_throws` | integer |  |
| `center_behind_los_touchdowns` | integer |  |
| `right_behind_los_twp_rate` | numeric |  |
| `center_deep_completions` | integer |  |
| `center_short_attempts` | integer |  |
| `left_behind_los_sack_percent` | integer |  |
| `center_short_pressure_to_sack_rate` | integer |  |
| `deep_touchdowns` | integer |  |
| `center_behind_los_accuracy_percent` | numeric |  |
| `center_deep_drop_rate` | numeric |  |
| `right_short_first_downs` | integer |  |
| `right_behind_los_first_downs` | integer |  |
| `short_interceptions` | integer |  |
| `center_medium_scrambles` | integer |  |
| `left_behind_los_sacks` | integer |  |
| `left_short_ypa` | numeric |  |
| `left_short_qb_rating` | numeric |  |
| `right_behind_los_qb_rating` | numeric |  |
| `draft_season` | integer |  |
| `right_behind_los_dropbacks` | integer |  |
| `behind_los_def_gen_pressures` | integer |  |
| `right_short_thrown_aways` | integer |  |
| `deep_def_gen_pressures` | integer |  |
| `right_short_btt_rate` | numeric |  |
| `center_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_grades_pass` | numeric |  |
| `deep_grades_pass` | numeric |  |
| `center_short_turnover_worthy_plays` | integer |  |
| `center_behind_los_scrambles` | integer |  |
| `right_short_aimed_passes` | integer |  |
| `center_short_dropbacks` | integer |  |
| `medium_epa` | numeric |  |
| `right_short_ypa` | numeric |  |
| `center_medium_ypa` | numeric |  |
| `medium_attempts` | integer |  |
| `right_deep_accuracy_percent` | numeric |  |
| `behind_los_scrambles` | integer |  |
| `center_behind_los_completion_percent` | numeric |  |
| `left_behind_los_btt_rate` | integer |  |
| `right_behind_los_btt_rate` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | integer |  |
| `center_medium_drops` | integer |  |
| `left_behind_los_aimed_passes` | integer |  |
| `deep_attempts` | integer |  |
| `right_behind_los_sack_percent` | integer |  |
| `center_short_positive_epa_percent` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `left_deep_btt_rate` | numeric |  |
| `medium_scrambles` | integer |  |
| `center_behind_los_avg_depth_of_target` | numeric |  |
| `short_attempts_percent` | numeric |  |
| `right_behind_los_sacks` | integer |  |
| `center_medium_avg_time_to_throw` | numeric |  |
| `left_short_pressure_to_sack_rate` | integer |  |
| `center_deep_sack_percent` | integer |  |
| `center_deep_ypa` | numeric |  |
| `left_behind_los_hit_as_threw` | integer |  |
| `medium_big_time_throws` | integer |  |
| `deep_thrown_aways` | integer |  |
| `right_short_accuracy_percent` | numeric |  |
| `left_deep_turnover_worthy_plays` | integer |  |
| `center_medium_bats` | integer |  |
| `right_short_grades_pass` | numeric |  |
| `right_deep_spikes` | integer |  |
| `left_deep_passing_snaps` | integer |  |
| `center_medium_twp_rate` | numeric |  |
| `right_behind_los_passing_snaps` | integer |  |
| `left_deep_qb_rating` | numeric |  |
| `right_deep_drop_rate` | numeric |  |
| `left_behind_los_drop_rate` | numeric |  |
| `left_medium_drop_rate` | numeric |  |
| `right_deep_attempts` | integer |  |
| `left_deep_spikes` | integer |  |
| `center_behind_los_dropbacks` | integer |  |
| `right_deep_big_time_throws` | integer |  |
| `medium_hit_as_threw` | integer |  |
| `right_short_dropbacks` | integer |  |
| `medium_def_gen_pressures` | integer |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric |  |
| `left_short_attempts` | integer |  |
| `center_deep_twp_rate` | numeric |  |
| `right_medium_avg_time_to_throw` | numeric |  |
| `left_behind_los_qb_rating` | numeric |  |
| `left_behind_los_dropbacks` | integer |  |
| `deep_dropbacks` | integer |  |
| `right_behind_los_scrambles` | integer |  |
| `behind_los_interceptions` | integer |  |
| `right_deep_drops` | integer |  |
| `right_deep_yards` | integer |  |
| `right_short_hit_as_threw` | integer |  |
| `right_short_avg_depth_of_target` | numeric |  |
| `short_bats` | integer |  |
| `right_deep_twp_rate` | numeric |  |
| `right_behind_los_pressure_to_sack_rate` | integer |  |
| `deep_spikes` | integer |  |
| `center_medium_spikes` | integer |  |
| `behind_los_aimed_passes` | integer |  |
| `right_behind_los_touchdowns` | integer |  |
| `right_medium_pressure_to_sack_rate` | integer |  |
| `medium_thrown_aways` | integer |  |
| `short_sacks` | integer |  |
| `center_behind_los_twp_rate` | numeric |  |
| `left_behind_los_attempts` | integer |  |
| `short_aimed_passes` | integer |  |
| `short_completions` | integer |  |
| `right_short_pressure_to_sack_rate` | integer |  |
| `medium_avg_depth_of_target` | numeric |  |
| `left_behind_los_positive_epa_percent` | numeric |  |
| `center_medium_attempts_percent` | numeric |  |
| `left_medium_touchdowns` | integer |  |
| `center_short_bats` | integer |  |
| `left_deep_avg_time_to_throw` | numeric |  |
| `center_behind_los_passing_snaps` | integer |  |
| `center_short_yards` | integer |  |
| `right_medium_btt_rate` | numeric |  |
| `right_medium_scrambles` | integer |  |
| `medium_btt_rate` | numeric |  |
| `center_behind_los_btt_rate` | integer |  |
| `center_short_avg_depth_of_target` | numeric |  |
| `left_medium_sacks` | integer |  |
| `right_short_yards` | integer |  |
| `left_medium_first_downs` | integer |  |
| `right_medium_positive_epa_percent` | numeric |  |
| `left_short_big_time_throws` | integer |  |
| `deep_turnover_worthy_plays` | integer |  |
| `left_deep_bats` | integer |  |
| `left_medium_turnover_worthy_plays` | integer |  |
| `base_dropbacks` | integer |  |
| `center_deep_drops` | integer |  |
| `center_medium_avg_depth_of_target` | numeric |  |
| `center_behind_los_ypa` | numeric |  |
| `right_short_attempts` | integer |  |
| `center_medium_completion_percent` | numeric |  |
| `right_short_sack_percent` | integer |  |
| `left_behind_los_avg_depth_of_target` | numeric |  |
| `center_medium_epa` | numeric |  |
| `center_behind_los_sack_percent` | integer |  |
| `medium_completions` | integer |  |
| `medium_sack_percent` | integer |  |
| `left_short_grades_pass` | numeric |  |
| `deep_btt_rate` | numeric |  |
| `left_short_accuracy_percent` | numeric |  |
| `short_twp_rate` | numeric |  |
| `short_passing_snaps` | integer |  |
| `left_deep_sacks` | integer |  |
| `short_scrambles` | integer |  |
| `center_medium_turnover_worthy_plays` | integer |  |
| `behind_los_drop_rate` | numeric |  |
| `right_behind_los_epa` | numeric |  |
| `right_medium_aimed_passes` | integer |  |
| `short_accuracy_percent` | numeric |  |
| `player_game_count` | integer |  |
| `short_btt_rate` | numeric |  |
| `right_short_touchdowns` | integer |  |
| `center_short_completions` | integer |  |
| `right_short_spikes` | integer |  |
| `behind_los_first_downs` | integer |  |
| `right_medium_def_gen_pressures` | integer |  |
| `deep_pressure_to_sack_rate` | integer |  |
| `medium_qb_rating` | numeric |  |
| `short_completion_percent` | numeric |  |
| `right_deep_attempts_percent` | numeric |  |
| `eligible_season` | integer |  |
| `short_big_time_throws` | integer |  |
| `behind_los_attempts` | integer |  |
| `center_behind_los_yards` | integer |  |
| `center_behind_los_bats` | integer |  |
| `center_behind_los_qb_rating` | numeric |  |
| `right_deep_pressure_to_sack_rate` | integer |  |
| `center_short_accuracy_percent` | numeric |  |
| `left_behind_los_bats` | integer |  |
| `center_deep_passing_snaps` | integer |  |
| `right_behind_los_hit_as_threw` | integer |  |
| `medium_bats` | integer |  |
| `right_medium_first_downs` | integer |  |
| `medium_spikes` | integer |  |
| `left_deep_thrown_aways` | integer |  |
| `right_deep_epa` | numeric |  |
| `center_medium_hit_as_threw` | integer |  |
| `left_behind_los_completion_percent` | numeric |  |
| `center_short_ypa` | numeric |  |
| `right_short_drop_rate` | numeric |  |
| `right_behind_los_accuracy_percent` | numeric |  |
| `left_short_yards` | integer |  |
| `deep_accuracy_percent` | numeric |  |
| `right_behind_los_avg_depth_of_target` | numeric |  |
| `center_behind_los_thrown_aways` | integer |  |
| `center_behind_los_attempts_percent` | numeric |  |
| `right_deep_first_downs` | integer |  |
| `left_short_completions` | integer |  |
| `deep_completion_percent` | numeric |  |
| `left_deep_positive_epa_percent` | numeric |  |
| `right_deep_sack_percent` | integer |  |
| `behind_los_pressure_to_sack_rate` | integer |  |
| `right_deep_touchdowns` | integer |  |
| `left_behind_los_def_gen_pressures` | integer |  |
| `left_behind_los_drops` | integer |  |
| `deep_epa` | numeric |  |
| `left_deep_ypa` | numeric |  |
| `medium_touchdowns` | integer |  |
| `left_medium_grades_pass` | numeric |  |
| `right_behind_los_spikes` | integer |  |
| `medium_grades_pass` | numeric |  |
| `behind_los_passing_snaps` | integer |  |
| `left_short_first_downs` | integer |  |
| `center_short_passing_snaps` | integer |  |
| `center_deep_accuracy_percent` | numeric |  |
| `right_deep_positive_epa_percent` | numeric |  |
| `right_medium_sack_percent` | integer |  |
| `right_deep_qb_rating` | numeric |  |
| `right_short_completion_percent` | numeric |  |
| `right_behind_los_interceptions` | integer |  |
| `behind_los_yards` | integer |  |
| `center_behind_los_grades_pass` | numeric |  |
| `left_deep_dropbacks` | integer |  |
| `center_behind_los_spikes` | integer |  |
| `right_behind_los_aimed_passes` | integer |  |
| `left_short_interceptions` | integer |  |
| `right_medium_interceptions` | integer |  |
| `left_behind_los_yards` | integer |  |
| `center_deep_btt_rate` | numeric |  |
| `medium_first_downs` | integer |  |
| `left_short_avg_depth_of_target` | numeric |  |
| `left_behind_los_ypa` | numeric |  |
| `short_attempts` | integer |  |
| `right_medium_bats` | integer |  |
| `left_behind_los_touchdowns` | integer |  |
| `right_medium_dropbacks` | integer |  |
| `short_turnover_worthy_plays` | integer |  |
| `right_deep_thrown_aways` | integer |  |
| `right_behind_los_drop_rate` | numeric |  |
| `right_short_qb_rating` | numeric |  |
| `medium_sacks` | integer |  |
| `center_deep_attempts_percent` | numeric |  |
| `left_short_dropbacks` | integer |  |
| `behind_los_drops` | integer |  |
| `short_sack_percent` | integer |  |
| `left_medium_spikes` | integer |  |
| `left_medium_accuracy_percent` | numeric |  |
| `behind_los_completions` | integer |  |
| `behind_los_sack_percent` | integer |  |
| `left_deep_yards` | integer |  |
| `left_short_aimed_passes` | integer |  |
| `center_behind_los_completions` | integer |  |
| `left_short_thrown_aways` | integer |  |
| `left_short_sack_percent` | integer |  |
| `center_short_thrown_aways` | integer |  |
| `center_short_interceptions` | integer |  |
| `short_avg_depth_of_target` | numeric |  |
| `left_deep_touchdowns` | integer |  |
| `deep_yards` | integer |  |
| `center_behind_los_turnover_worthy_plays` | integer |  |
| `medium_ypa` | numeric |  |
| `left_medium_epa` | numeric |  |
| `left_deep_avg_depth_of_target` | numeric |  |
| `deep_first_downs` | integer |  |
| `short_drop_rate` | numeric |  |
| `left_medium_bats` | integer |  |
| `center_deep_spikes` | integer |  |
| `center_short_hit_as_threw` | integer |  |
| `left_medium_positive_epa_percent` | numeric |  |
| `center_deep_touchdowns` | integer |  |
| `right_deep_ypa` | numeric |  |
| `right_short_big_time_throws` | integer |  |
| `center_short_big_time_throws` | integer |  |
| `short_positive_epa_percent` | numeric |  |
| `left_behind_los_avg_time_to_throw` | numeric |  |
| `right_deep_passing_snaps` | integer |  |
| `right_short_twp_rate` | numeric |  |
| `center_medium_attempts` | integer |  |
| `right_deep_avg_time_to_throw` | numeric |  |
| `center_deep_def_gen_pressures` | integer |  |
| `behind_los_epa` | numeric |  |
| `right_medium_twp_rate` | numeric |  |
| `right_deep_aimed_passes` | integer |  |
| `right_deep_scrambles` | integer |  |
| `deep_big_time_throws` | integer |  |
| `left_short_turnover_worthy_plays` | integer |  |
| `center_short_touchdowns` | integer |  |
| `right_medium_drops` | integer |  |
| `left_deep_epa` | numeric |  |
| `short_ypa` | numeric |  |
| `medium_pressure_to_sack_rate` | integer |  |
| `left_medium_thrown_aways` | integer |  |
| `right_behind_los_avg_time_to_throw` | numeric |  |
| `behind_los_btt_rate` | integer |  |
| `medium_avg_time_to_throw` | numeric |  |
| `center_deep_completion_percent` | numeric |  |
| `behind_los_avg_time_to_throw` | numeric |  |
| `right_medium_touchdowns` | integer |  |
| `center_short_avg_time_to_throw` | numeric |  |
| `left_deep_aimed_passes` | integer |  |
| `left_medium_yards` | integer |  |
| `center_medium_touchdowns` | integer |  |
| `center_short_drop_rate` | numeric |  |
| `left_short_twp_rate` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `right_behind_los_attempts_percent` | numeric |  |
| `right_deep_avg_depth_of_target` | numeric |  |
| `behind_los_sacks` | integer |  |
| `center_deep_yards` | integer |  |
| `short_dropbacks` | integer |  |
| `left_deep_sack_percent` | integer |  |
| `center_behind_los_avg_time_to_throw` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `left_behind_los_epa` | numeric |  |
| `medium_twp_rate` | numeric |  |
| `left_short_def_gen_pressures` | integer |  |
| `medium_turnover_worthy_plays` | integer |  |
| `behind_los_twp_rate` | numeric |  |
| `left_behind_los_thrown_aways` | integer |  |
| `left_short_bats` | integer |  |
| `center_medium_drop_rate` | numeric |  |
| `right_deep_bats` | integer |  |
| `medium_yards` | integer |  |
| `center_deep_grades_pass` | numeric |  |
| `center_medium_passing_snaps` | integer |  |
| `center_behind_los_first_downs` | integer |  |
| `center_medium_interceptions` | integer |  |
| `behind_los_grades_pass` | numeric |  |
| `left_short_hit_as_threw` | integer |  |
| `deep_qb_rating` | numeric |  |
| `center_deep_bats` | integer |  |
| `behind_los_ypa` | numeric |  |
| `right_short_interceptions` | integer |  |
| `left_deep_interceptions` | integer |  |
| `right_medium_sacks` | integer |  |
| `right_behind_los_turnover_worthy_plays` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `right_medium_spikes` | integer |  |
| `behind_los_hit_as_threw` | integer |  |
| `left_medium_pressure_to_sack_rate` | integer |  |
| `medium_dropbacks` | integer |  |
| `deep_hit_as_threw` | integer |  |
| `right_medium_completion_percent` | numeric |  |
| `short_epa` | numeric |  |
| `deep_drop_rate` | numeric |  |
| `declined_penalties` | integer |  |
| `deep_scrambles` | integer |  |
| `left_deep_completion_percent` | numeric |  |
| `right_short_epa` | numeric |  |
| `medium_attempts_percent` | numeric |  |
| `right_behind_los_completion_percent` | numeric |  |
| `left_medium_hit_as_threw` | integer |  |
| `left_behind_los_pressure_to_sack_rate` | integer |  |
| `right_short_def_gen_pressures` | integer |  |
| `right_short_attempts_percent` | numeric |  |
| `center_short_aimed_passes` | integer |  |
| `short_hit_as_threw` | integer |  |
| `right_medium_turnover_worthy_plays` | integer |  |
| `left_short_epa` | numeric |  |
| `center_behind_los_pressure_to_sack_rate` | integer |  |
| `right_behind_los_ypa` | numeric |  |
| `deep_interceptions` | integer |  |
| `left_medium_twp_rate` | numeric |  |
| `right_behind_los_big_time_throws` | integer |  |
| `center_medium_sack_percent` | integer |  |
| `center_deep_thrown_aways` | integer |  |
| `short_thrown_aways` | integer |  |
| `left_short_btt_rate` | numeric |  |
| `right_medium_accuracy_percent` | numeric |  |
| `short_def_gen_pressures` | integer |  |
| `center_deep_dropbacks` | integer |  |
| `center_medium_accuracy_percent` | numeric |  |
| `right_behind_los_yards` | integer |  |
| `right_medium_attempts` | integer |  |
| `left_medium_attempts` | integer |  |
| `medium_accuracy_percent` | numeric |  |
| `left_medium_drops` | integer |  |
| `left_short_avg_time_to_throw` | numeric |  |
| `medium_passing_snaps` | integer |  |
| `center_short_epa` | numeric |  |
| `left_behind_los_passing_snaps` | integer |  |
| `left_deep_pressure_to_sack_rate` | integer |  |
| `deep_bats` | integer |  |
| `left_medium_passing_snaps` | integer |  |
| `left_medium_scrambles` | integer |  |
| `right_deep_completion_percent` | numeric |  |
| `left_short_drops` | integer |  |
| `left_deep_accuracy_percent` | numeric |  |
| `medium_positive_epa_percent` | numeric |  |
| `behind_los_turnover_worthy_plays` | integer |  |
| `right_medium_epa` | numeric |  |
| `right_deep_dropbacks` | integer |  |
| `deep_ypa` | numeric |  |
| `center_medium_completions` | integer |  |
| `left_medium_avg_time_to_throw` | numeric |  |
| `left_short_sacks` | integer |  |
| `left_behind_los_spikes` | integer |  |
| `left_deep_def_gen_pressures` | integer |  |
| `center_short_qb_rating` | numeric |  |
| `center_deep_interceptions` | integer |  |
| `right_deep_completions` | integer |  |
| `center_behind_los_sacks` | integer |  |
| `deep_completions` | integer |  |
| `left_medium_attempts_percent` | numeric |  |
| `short_yards` | integer |  |
| `behind_los_qb_rating` | numeric |  |
| `right_short_sacks` | integer |  |
| `right_behind_los_thrown_aways` | integer |  |
| `base_attempts` | integer |  |
| `center_short_spikes` | integer |  |
| `center_behind_los_hit_as_threw` | integer |  |
| `center_deep_turnover_worthy_plays` | integer |  |
| `left_medium_sack_percent` | integer |  |
| `behind_los_completion_percent` | numeric |  |
| `left_deep_attempts_percent` | numeric |  |
| `center_deep_big_time_throws` | integer |  |
| `left_medium_avg_depth_of_target` | numeric |  |
| `behind_los_thrown_aways` | integer |  |
| `center_medium_def_gen_pressures` | integer |  |
| `short_avg_time_to_throw` | numeric |  |
| `left_deep_twp_rate` | numeric |  |
| `center_behind_los_epa` | numeric |  |
| `left_behind_los_big_time_throws` | integer |  |
| `right_medium_yards` | integer |  |
| `right_medium_drop_rate` | numeric |  |
| `center_medium_big_time_throws` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric |  |
| `left_short_drop_rate` | numeric |  |
| `left_behind_los_scrambles` | integer |  |
| `left_medium_btt_rate` | numeric |  |
| `right_deep_hit_as_threw` | integer |  |
| `behind_los_bats` | integer |  |
| `short_first_downs` | integer |  |
| `left_deep_attempts` | integer |  |
| `right_behind_los_attempts` | integer |  |
| `right_deep_sacks` | integer |  |
| `left_behind_los_attempts_percent` | numeric |  |
| `behind_los_big_time_throws` | integer |  |
| `center_deep_pressure_to_sack_rate` | integer |  |
| `left_medium_big_time_throws` | integer |  |
| `right_behind_los_bats` | integer |  |
| `left_medium_def_gen_pressures` | integer |  |
| `right_deep_turnover_worthy_plays` | integer |  |
| `left_short_spikes` | integer |  |
| `left_medium_aimed_passes` | integer |  |
| `left_behind_los_twp_rate` | numeric |  |
| `short_grades_pass` | numeric |  |
| `right_short_drops` | integer |  |
| `right_short_avg_time_to_throw` | numeric |  |
| `right_medium_thrown_aways` | integer |  |
| `center_short_sack_percent` | integer |  |
| `right_short_passing_snaps` | integer |  |
| `center_short_def_gen_pressures` | integer |  |
| `center_medium_positive_epa_percent` | numeric |  |
| `medium_drops` | integer |  |
| `center_medium_aimed_passes` | integer |  |
| `center_short_sacks` | integer |  |
| `deep_attempts_percent` | numeric |  |
| `center_behind_los_def_gen_pressures` | integer |  |
| `left_short_passing_snaps` | integer |  |
| `center_medium_grades_pass` | numeric |  |
| `center_deep_avg_time_to_throw` | numeric |  |
| `behind_los_positive_epa_percent` | numeric |  |
| `center_short_completion_percent` | numeric |  |
| `left_deep_hit_as_threw` | integer |  |
| `center_short_grades_pass` | numeric |  |
| `left_short_attempts_percent` | numeric |  |
| `left_deep_scrambles` | integer |  |
| `right_short_completions` | integer |  |
| `center_behind_los_aimed_passes` | integer |  |
| `right_short_scrambles` | integer |  |
| `center_medium_qb_rating` | numeric |  |
| `deep_avg_time_to_throw` | numeric |  |
| `left_deep_drops` | integer |  |
| `right_behind_los_def_gen_pressures` | integer |  |
| `center_deep_positive_epa_percent` | numeric |  |
| `center_deep_hit_as_threw` | integer |  |
| `short_spikes` | integer |  |
| `right_behind_los_completions` | integer |  |
| `center_behind_los_drop_rate` | numeric |  |
| `short_drops` | integer |  |
| `deep_aimed_passes` | integer |  |
| `right_medium_avg_depth_of_target` | numeric |  |
| `short_pressure_to_sack_rate` | integer |  |
| `center_deep_qb_rating` | numeric |  |
| `left_medium_completions` | integer |  |
| `center_deep_sacks` | integer |  |
| `left_deep_big_time_throws` | integer |  |
| `center_medium_first_downs` | integer |  |
| `center_medium_dropbacks` | integer |  |
| `right_deep_def_gen_pressures` | integer |  |
| `left_short_touchdowns` | integer |  |
| `medium_drop_rate` | numeric |  |
| `left_medium_ypa` | numeric |  |
| `right_medium_passing_snaps` | integer |  |
| `center_short_scrambles` | integer |  |
| `left_behind_los_turnover_worthy_plays` | integer |  |
| `center_medium_yards` | integer |  |
| `center_deep_epa` | numeric |  |
| `left_medium_interceptions` | integer |  |
| `right_deep_interceptions` | integer |  |
| `medium_completion_percent` | numeric |  |
| `right_behind_los_grades_pass` | numeric |  |
| `deep_positive_epa_percent` | numeric |  |
| `behind_los_accuracy_percent` | numeric |  |
| `center_deep_scrambles` | integer |  |
| `center_short_twp_rate` | numeric |  |
| `behind_los_touchdowns` | integer |  |
| `left_medium_qb_rating` | numeric |  |
| `right_medium_completions` | integer |  |
| `right_behind_los_drops` | integer |  |
| `left_behind_los_first_downs` | integer |  |
| `short_qb_rating` | numeric |  |
| `center_medium_btt_rate` | numeric |  |
| `left_deep_grades_pass` | numeric |  |

**passing-pressure**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `no_blitz_completion_percent` | numeric |  |
| `grades_offense` | numeric |  |
| `no_pressure_scrambles` | integer |  |
| `blitz_touchdowns` | integer |  |
| `pressure_yards` | integer |  |
| `no_pressure_spikes` | integer |  |
| `blitz_ypa` | numeric |  |
| `no_blitz_grades_run` | numeric |  |
| `blitz_qb_rating` | numeric |  |
| `no_pressure_thrown_aways` | integer |  |
| `no_blitz_bats` | integer |  |
| `no_blitz_drops` | integer |  |
| `no_pressure_completion_percent` | numeric |  |
| `blitz_aimed_passes` | integer |  |
| `pressure_grades_run` | numeric |  |
| `no_blitz_sack_percent` | numeric |  |
| `no_pressure_bats` | integer |  |
| `pressure_completions` | integer |  |
| `blitz_big_time_throws` | integer |  |
| `no_blitz_drop_rate` | numeric |  |
| `blitz_spikes` | integer |  |
| `no_pressure_completions` | integer |  |
| `draft_season` | integer |  |
| `no_pressure_passing_snaps` | integer |  |
| `no_blitz_first_downs` | integer |  |
| `blitz_avg_time_to_throw` | numeric |  |
| `no_pressure_grades_hands_fumble` | numeric |  |
| `pressure_aimed_passes` | integer |  |
| `blitz_sacks` | integer |  |
| `no_pressure_interceptions` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pressure_epa` | numeric |  |
| `blitz_completions` | integer |  |
| `blitz_attempts` | integer |  |
| `pressure_twp_rate` | numeric |  |
| `pressure_sacks` | integer |  |
| `no_blitz_pressure_to_sack_rate` | numeric |  |
| `no_pressure_ypa` | numeric |  |
| `pressure_passing_snaps` | integer |  |
| `grades_pass` | numeric |  |
| `pressure_bats` | integer |  |
| `blitz_thrown_aways` | integer |  |
| `no_pressure_drops` | integer |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `pressure_grades_offense_penalty` | numeric |  |
| `pressure_scrambles` | integer |  |
| `blitz_drop_rate` | numeric |  |
| `blitz_completion_percent` | numeric |  |
| `no_pressure_first_downs` | integer |  |
| `blitz_grades_offense_penalty` | numeric |  |
| `no_blitz_epa` | numeric |  |
| `blitz_interceptions` | integer |  |
| `no_blitz_dropbacks` | integer |  |
| `no_blitz_grades_pass` | numeric |  |
| `no_blitz_scrambles` | integer |  |
| `pressure_drop_rate` | numeric |  |
| `no_blitz_yards` | integer |  |
| `base_dropbacks` | integer |  |
| `pressure_pressure_to_sack_rate` | numeric |  |
| `no_blitz_grades_hands_fumble` | numeric |  |
| `no_pressure_grades_offense` | numeric |  |
| `no_pressure_avg_time_to_throw` | numeric |  |
| `pressure_dropbacks` | integer |  |
| `no_blitz_grades_offense_penalty` | numeric |  |
| `no_pressure_grades_run` | numeric |  |
| `player_game_count` | integer |  |
| `blitz_turnover_worthy_plays` | integer |  |
| `no_blitz_touchdowns` | integer |  |
| `no_blitz_avg_depth_of_target` | numeric |  |
| `no_pressure_dropbacks_percent` | numeric |  |
| `blitz_grades_pass` | numeric |  |
| `eligible_season` | integer |  |
| `blitz_avg_depth_of_target` | numeric |  |
| `no_blitz_spikes` | integer |  |
| `no_pressure_dropbacks` | integer |  |
| `blitz_btt_rate` | numeric |  |
| `blitz_positive_epa_percent` | numeric |  |
| `no_pressure_btt_rate` | numeric |  |
| `no_pressure_drop_rate` | numeric |  |
| `no_blitz_turnover_worthy_plays` | integer |  |
| `pressure_positive_epa_percent` | numeric |  |
| `blitz_first_downs` | integer |  |
| `no_blitz_dropbacks_percent` | numeric |  |
| `pressure_turnover_worthy_plays` | integer |  |
| `no_pressure_epa` | numeric |  |
| `no_blitz_avg_time_to_throw` | numeric |  |
| `no_blitz_positive_epa_percent` | numeric |  |
| `pressure_btt_rate` | numeric |  |
| `no_pressure_aimed_passes` | integer |  |
| `pressure_dropbacks_percent` | numeric |  |
| `grades_run` | numeric |  |
| `no_pressure_def_gen_pressures` | integer |  |
| `pressure_grades_offense` | numeric |  |
| `pressure_completion_percent` | numeric |  |
| `pressure_avg_depth_of_target` | numeric |  |
| `blitz_epa` | numeric |  |
| `pressure_drops` | integer |  |
| `no_blitz_def_gen_pressures` | integer |  |
| `pressure_attempts` | integer |  |
| `pressure_big_time_throws` | integer |  |
| `no_blitz_thrown_aways` | integer |  |
| `blitz_drops` | integer |  |
| `no_pressure_touchdowns` | integer |  |
| `no_pressure_sacks` | integer |  |
| `blitz_sack_percent` | numeric |  |
| `pressure_sack_percent` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `no_pressure_attempts` | integer |  |
| `no_blitz_grades_offense` | numeric |  |
| `blitz_scrambles` | integer |  |
| `blitz_dropbacks_percent` | numeric |  |
| `no_pressure_qb_rating` | numeric |  |
| `no_blitz_passing_snaps` | integer |  |
| `no_blitz_aimed_passes` | integer |  |
| `blitz_yards` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_blitz_attempts` | integer |  |
| `pressure_accuracy_percent` | numeric |  |
| `no_blitz_hit_as_threw` | integer |  |
| `pressure_hit_as_threw` | integer |  |
| `blitz_pressure_to_sack_rate` | numeric |  |
| `declined_penalties` | integer |  |
| `no_blitz_btt_rate` | numeric |  |
| `no_blitz_ypa` | numeric |  |
| `blitz_grades_run` | numeric |  |
| `pressure_interceptions` | integer |  |
| `blitz_grades_hands_fumble` | numeric |  |
| `no_pressure_grades_offense_penalty` | numeric |  |
| `blitz_grades_offense` | numeric |  |
| `grades_hands_fumble` | numeric |  |
| `pressure_def_gen_pressures` | integer |  |
| `pressure_grades_pass` | numeric |  |
| `no_blitz_qb_rating` | numeric |  |
| `blitz_hit_as_threw` | integer |  |
| `pressure_ypa` | numeric |  |
| `pressure_avg_time_to_throw` | numeric |  |
| `no_blitz_completions` | integer |  |
| `no_pressure_sack_percent` | numeric |  |
| `no_blitz_accuracy_percent` | numeric |  |
| `no_pressure_pressure_to_sack_rate` | character |  |
| `no_pressure_turnover_worthy_plays` | integer |  |
| `pressure_first_downs` | integer |  |
| `blitz_passing_snaps` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `no_pressure_twp_rate` | numeric |  |
| `no_blitz_sacks` | integer |  |
| `no_pressure_accuracy_percent` | numeric |  |
| `no_blitz_twp_rate` | numeric |  |
| `no_pressure_hit_as_threw` | integer |  |
| `no_pressure_grades_pass` | numeric |  |
| `no_pressure_positive_epa_percent` | numeric |  |
| `pressure_spikes` | integer |  |
| `pressure_grades_hands_fumble` | numeric |  |
| `pressure_qb_rating` | numeric |  |
| `blitz_bats` | integer |  |
| `pressure_touchdowns` | integer |  |
| `pressure_thrown_aways` | integer |  |
| `no_blitz_interceptions` | integer |  |
| `blitz_dropbacks` | integer |  |
| `blitz_def_gen_pressures` | integer |  |
| `no_blitz_big_time_throws` | integer |  |
| `no_pressure_avg_depth_of_target` | numeric |  |
| `no_pressure_yards` | integer |  |
| `blitz_twp_rate` | numeric |  |
| `blitz_accuracy_percent` | numeric |  |
| `no_pressure_big_time_throws` | integer |  |
| `pressure_grades_pass_route` | numeric |  |
| `blitz_grades_pass_route` | numeric |  |
| `no_pressure_grades_pass_route` | numeric |  |
| `no_blitz_grades_pass_route` | numeric |  |
| `no_pressure_grades_hands_drop` | numeric |  |
| `blitz_grades_hands_drop` | numeric |  |
| `pressure_grades_hands_drop` | numeric |  |
| `no_blitz_grades_hands_drop` | numeric |  |
| `no_blitz_grades_pass_block` | numeric |  |
| `blitz_grades_pass_block` | numeric |  |
| `no_pressure_grades_pass_block` | numeric |  |
| `pressure_grades_pass_block` | numeric |  |
| `pressure_grades_defense` | character |  |
| `pressure_grades_pass_rush_defense` | character |  |
| `no_blitz_grades_defense` | integer |  |
| `blitz_grades_run_block` | numeric |  |
| `blitz_grades_pass_rush_defense` | character |  |
| `no_pressure_grades_run_block` | numeric |  |
| `no_blitz_grades_defense_penalty` | numeric |  |
| `no_blitz_grades_run_block` | numeric |  |
| `blitz_grades_screen_block` | numeric |  |
| `no_pressure_grades_defense` | integer |  |
| `no_pressure_grades_pass_rush_defense` | integer |  |
| `no_pressure_grades_defense_penalty` | numeric |  |
| `pressure_grades_screen_block` | numeric |  |
| `no_blitz_grades_screen_block` | numeric |  |
| `blitz_grades_defense` | character |  |
| `blitz_grades_defense_penalty` | character |  |
| `pressure_grades_run_block` | numeric |  |
| `pressure_grades_defense_penalty` | character |  |
| `no_pressure_grades_screen_block` | numeric |  |
| `no_blitz_grades_pass_rush_defense` | integer |  |
| `pressure_grades_coverage_defense` | character |  |
| `blitz_grades_coverage_defense` | character |  |
| `no_blitz_grades_coverage_defense` | integer |  |
| `no_pressure_grades_coverage_defense` | integer |  |
| `no_pressure_grades_tackle` | numeric |  |
| `blitz_grades_tackle` | numeric |  |
| `blitz_grades_overall_tackle` | numeric |  |
| `pressure_grades_overall_tackle` | numeric |  |
| `no_blitz_grades_overall_tackle` | numeric |  |
| `pressure_grades_tackle` | numeric |  |
| `no_pressure_grades_overall_tackle` | numeric |  |
| `no_blitz_grades_tackle` | numeric |  |

**receiving**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `team_targets_percent` | numeric |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `caught_percent` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `yards_per_reception` | numeric |  |
| `touchdowns` | integer |  |
| `grades_offense` | numeric |  |
| `grades_pass_route` | numeric |  |
| `grades_hands_drop` | numeric |  |
| `grades_hands_fumble` | numeric |  |
| `pass_plays` | integer |  |
| `routes` | integer |  |
| `route_rate` | numeric |  |
| `pass_blocks` | integer |  |
| `pass_block_rate` | numeric |  |
| `slot_snaps` | integer |  |
| `slot_rate` | numeric |  |
| `wide_snaps` | integer |  |
| `wide_rate` | numeric |  |
| `inline_snaps` | integer |  |
| `inline_rate` | numeric |  |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `yards_after_catch_per_reception` | numeric |  |
| `yprr` | numeric |  |
| `avg_depth_of_target` | numeric |  |
| `longest` | integer |  |
| `drops` | integer | Throws dropped |
| `drop_rate` | numeric |  |
| `contested_targets` | integer |  |
| `contested_receptions` | integer |  |
| `contested_catch_rate` | numeric |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `fumbles` | integer |  |
| `avoided_tackles` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `targeted_qb_rating` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `receptions_oe` | numeric |  |
| `receptions_oe_total` | numeric |  |
| `lined_up_vs_cb` | numeric |  |
| `lined_up_vs_s` | numeric |  |
| `lined_up_vs_lb` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**receiving-depth**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `base_targets` | integer |  |
| `deep_targets_percent` | numeric |  |
| `deep_targets` | integer |  |
| `deep_receptions` | integer |  |
| `deep_caught_percent` | numeric |  |
| `deep_yards` | integer |  |
| `deep_yards_per_reception` | numeric |  |
| `deep_touchdowns` | integer |  |
| `deep_grades_pass_route` | numeric |  |
| `deep_grades_hands_drop` | numeric |  |
| `deep_yards_after_catch` | integer |  |
| `deep_yards_after_catch_per_reception` | numeric |  |
| `deep_yprr` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `deep_drops` | integer |  |
| `deep_drop_rate` | numeric |  |
| `deep_contested_targets` | integer |  |
| `deep_contested_receptions` | integer |  |
| `deep_contested_catch_rate` | numeric |  |
| `deep_interceptions` | integer |  |
| `deep_fumbles` | integer |  |
| `deep_avoided_tackles` | integer |  |
| `deep_first_downs` | integer |  |
| `deep_targeted_qb_rating` | numeric |  |
| `medium_targets_percent` | numeric |  |
| `medium_targets` | integer |  |
| `medium_receptions` | integer |  |
| `medium_caught_percent` | numeric |  |
| `medium_yards` | integer |  |
| `medium_yards_per_reception` | numeric |  |
| `medium_touchdowns` | integer |  |
| `medium_grades_pass_route` | numeric |  |
| `medium_grades_hands_drop` | numeric |  |
| `medium_yards_after_catch` | integer |  |
| `medium_yards_after_catch_per_reception` | numeric |  |
| `medium_yprr` | numeric |  |
| `medium_avg_depth_of_target` | numeric |  |
| `medium_drops` | integer |  |
| `medium_drop_rate` | numeric |  |
| `medium_contested_targets` | integer |  |
| `medium_contested_receptions` | integer |  |
| `medium_contested_catch_rate` | numeric |  |
| `medium_interceptions` | integer |  |
| `medium_fumbles` | integer |  |
| `medium_avoided_tackles` | integer |  |
| `medium_first_downs` | integer |  |
| `medium_targeted_qb_rating` | numeric |  |
| `short_targets_percent` | numeric |  |
| `short_targets` | integer |  |
| `short_receptions` | integer |  |
| `short_caught_percent` | numeric |  |
| `short_yards` | integer |  |
| `short_yards_per_reception` | numeric |  |
| `short_touchdowns` | integer |  |
| `short_grades_pass_route` | numeric |  |
| `short_grades_hands_drop` | numeric |  |
| `short_yards_after_catch` | integer |  |
| `short_yards_after_catch_per_reception` | numeric |  |
| `short_yprr` | numeric |  |
| `short_avg_depth_of_target` | numeric |  |
| `short_drops` | integer |  |
| `short_drop_rate` | numeric |  |
| `short_contested_targets` | integer |  |
| `short_contested_receptions` | integer |  |
| `short_contested_catch_rate` | numeric |  |
| `short_interceptions` | integer |  |
| `short_fumbles` | integer |  |
| `short_avoided_tackles` | integer |  |
| `short_first_downs` | integer |  |
| `short_targeted_qb_rating` | numeric |  |
| `behind_los_targets_percent` | numeric |  |
| `behind_los_targets` | integer |  |
| `behind_los_receptions` | integer |  |
| `behind_los_caught_percent` | numeric |  |
| `behind_los_yards` | integer |  |
| `behind_los_yards_per_reception` | numeric |  |
| `behind_los_touchdowns` | integer |  |
| `behind_los_grades_pass_route` | numeric |  |
| `behind_los_grades_hands_drop` | numeric |  |
| `behind_los_yards_after_catch` | integer |  |
| `behind_los_yards_after_catch_per_reception` | numeric |  |
| `behind_los_yprr` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `behind_los_drops` | integer |  |
| `behind_los_drop_rate` | numeric |  |
| `behind_los_contested_targets` | integer |  |
| `behind_los_contested_receptions` | integer |  |
| `behind_los_contested_catch_rate` | integer |  |
| `behind_los_interceptions` | integer |  |
| `behind_los_fumbles` | integer |  |
| `behind_los_avoided_tackles` | integer |  |
| `behind_los_first_downs` | integer |  |
| `behind_los_targeted_qb_rating` | numeric |  |

**rushing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `grades_pass_block` | numeric |  |
| `grades_offense` | numeric |  |
| `yards_after_contact` | integer |  |
| `explosive` | integer |  |
| `grades_pass_route` | numeric |  |
| `draft_season` | integer |  |
| `elu_rush_mtf` | integer |  |
| `breakaway_attempts` | integer |  |
| `designed_yards` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `yprr` | numeric |  |
| `breakaway_percent` | numeric |  |
| `grades_pass` | numeric |  |
| `fumbles` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `elusive_rating` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `breakaway_yards` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `total_touches` | integer |  |
| `scramble_yards` | integer |  |
| `yco_attempt` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `grades_run_block` | numeric |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `zone_attempts` | integer |  |
| `scrambles` | integer |  |
| `grades_run` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `elu_yco` | integer |  |
| `elu_recv_mtf` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `ypa` | numeric |  |
| `drops` | integer | Throws dropped |
| `grades_hands_fumble` | numeric |  |
| `longest` | integer |  |
| `routes` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `rec_yards` | integer | Career receiving yards |
| `gap_attempts` | integer |  |
| `run_plays` | integer |  |
| `avoided_tackles` | integer |  |
| `grades_offense_penalty` | numeric |  |
| `touchdowns` | integer |  |
| `carry_share` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_offense` | integer |  |
| `snap_counts_block` | integer |  |
| `block_percent` | numeric |  |
| `snap_counts_run_block` | integer |  |
| `snap_counts_pass_block` | integer |  |
| `pass_block_percent` | numeric |  |
| `grades_offense` | numeric |  |
| `grades_run_block` | numeric |  |
| `grades_pass_block` | numeric |  |
| `non_spike_pass_block` | integer |  |
| `non_spike_pass_block_percentage` | numeric |  |
| `sacks_allowed` | integer | Opponent sacks. |
| `hits_allowed` | integer |  |
| `hurries_allowed` | integer |  |
| `pressures_allowed` | integer |  |
| `pbe` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `declined_penalties` | integer |  |
| `snap_counts_lt` | integer |  |
| `snap_counts_lg` | integer |  |
| `snap_counts_ce` | integer |  |
| `snap_counts_rg` | integer |  |
| `snap_counts_rt` | integer |  |
| `snap_counts_te` | integer |  |

**pass-blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_lt` | integer |  |
| `snap_counts_lg` | integer |  |
| `snap_counts_ce` | integer |  |
| `snap_counts_rg` | integer |  |
| `snap_counts_rt` | integer |  |
| `snap_counts_te` | integer |  |
| `snap_counts_pass_play` | integer |  |
| `snap_counts_pass_block` | integer |  |
| `pass_block_percent` | numeric |  |
| `grades_pass_block` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `declined_penalties` | integer |  |
| `non_spike_pass_block` | integer |  |
| `non_spike_pass_block_percentage` | numeric |  |
| `sacks_allowed` | integer | Opponent sacks. |
| `hits_allowed` | integer |  |
| `hurries_allowed` | integer |  |
| `pressures_allowed` | integer |  |
| `pressure_rate_allowed` | numeric |  |
| `sack_rate_allowed` | numeric |  |
| `pbe` | numeric |  |
| `true_pass_set_snap_counts_pass_play` | integer |  |
| `true_pass_set_snap_counts_pass_block` | integer |  |
| `true_pass_set_pass_block_percent` | numeric |  |
| `true_pass_set_grades_pass_block` | numeric |  |
| `true_pass_set_non_spike_pass_block` | integer |  |
| `true_pass_set_non_spike_pass_block_percentage` | numeric |  |
| `true_pass_set_sacks_allowed` | integer |  |
| `true_pass_set_hits_allowed` | integer |  |
| `true_pass_set_hurries_allowed` | integer |  |
| `true_pass_set_pressures_allowed` | integer |  |
| `true_pass_set_pressure_rate_allowed` | numeric |  |
| `true_pass_set_sack_rate_allowed` | numeric |  |
| `true_pass_set_pbe` | numeric |  |
| `pbwr` | numeric |  |
| `true_pass_set_pbwr` | numeric |  |
| `pass_block_grade_oe_percentile` | numeric |  |
| `island_rate` | numeric |  |
| `island_pass_block_win_rate` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**run-blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_lt` | integer |  |
| `snap_counts_lg` | integer |  |
| `snap_counts_ce` | integer |  |
| `snap_counts_rg` | integer |  |
| `snap_counts_rt` | integer |  |
| `snap_counts_te` | integer |  |
| `snap_counts_run_play` | integer |  |
| `snap_counts_run_block` | integer |  |
| `run_block_percent` | numeric |  |
| `grades_run_block` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `declined_penalties` | integer |  |
| `zone_snap_counts_run_play` | integer |  |
| `zone_snap_counts_run_block` | integer |  |
| `zone_snap_counts_run_block_percent` | numeric |  |
| `zone_run_block_percent` | numeric |  |
| `zone_grades_run_block` | numeric |  |
| `gap_snap_counts_run_play` | integer |  |
| `gap_snap_counts_run_block` | integer |  |
| `gap_snap_counts_run_block_percent` | numeric |  |
| `gap_run_block_percent` | numeric |  |
| `gap_grades_run_block` | numeric |  |
| `pos_graded_rate` | numeric |  |
| `neg_graded_rate` | numeric |  |
| `zone_pos_graded_rate` | numeric |  |
| `zone_neg_graded_rate` | numeric |  |
| `gap_pos_graded_rate` | numeric |  |
| `gap_neg_graded_rate` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `interception_touchdowns` | integer |  |
| `draft_season` | integer |  |
| `forced_fumbles` | integer |  |
| `missed_tackles` | integer |  |
| `catch_rate` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | integer | Team tackles. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `snap_counts_offball` | integer |  |
| `snap_counts_box` | integer |  |
| `sacks` | integer | The Number of times sacked. |
| `snap_counts_pass_rush` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_dl` | integer |  |
| `grades_tackle` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `grades_coverage_defense` | numeric |  |
| `hurries` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `snap_counts_coverage` | integer |  |
| `snap_counts_dl_over_t` | integer |  |
| `snap_counts_dl_a_gap` | integer |  |
| `fumble_recoveries` | integer |  |
| `grades_run_defense` | numeric |  |
| `snap_counts_corner` | integer |  |
| `hits` | integer | Hits. |
| `penalties` | integer | Total number of penalties. |
| `batted_passes` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | integer |  |
| `declined_penalties` | integer |  |
| `total_pressures` | integer |  |
| `fumble_recovery_touchdowns` | integer |  |
| `longest` | integer |  |
| `snap_counts_slot` | integer |  |
| `missed_tackle_rate` | numeric |  |
| `grades_defense` | numeric |  |
| `yards_per_reception` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `safeties` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_defense` | integer |  |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `snap_counts_dl_b_gap` | integer |  |
| `pass_break_ups` | integer |  |
| `qb_rating_against` | numeric |  |
| `snap_counts_run_defense` | integer |  |
| `tackles_for_loss` | integer | Team tackles for a loss. |
| `assists` | integer | Total assists. |
| `grades_pass_rush_defense` | numeric |  |
| `snap_counts_fs` | integer |  |
| `touchdowns` | integer |  |
| `snap_counts_dl_outside_t` | integer |  |

**run-defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `assists` | integer | Total assists. |
| `avg_depth_of_tackle` | numeric |  |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `forced_fumbles` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_coverage_defense` | numeric |  |
| `grades_defense` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `grades_run_defense` | numeric |  |
| `grades_tackle` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackle_rate` | numeric |  |
| `missed_tackles` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `run_stop_opp` | integer |  |
| `snap_counts_run` | integer |  |
| `stop_percent` | numeric |  |
| `stops` | integer |  |
| `tackles` | integer | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pos_graded_rate` | numeric |  |
| `neg_graded_rate` | numeric |  |

**pass-rush**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_total_pressures` | integer |  |
| `draft_season` | integer |  |
| `true_pass_set_prp` | numeric |  |
| `true_pass_set_hurries` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric |  |
| `true_pass_set_sacks` | integer |  |
| `pass_rush_win_rate` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sacks` | integer | The Number of times sacked. |
| `snap_counts_pass_rush` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `true_pass_set_snap_counts_pass_play` | integer |  |
| `pass_rush_wins` | integer |  |
| `hurries` | integer |  |
| `pass_rush_opp` | integer |  |
| `snap_counts_pass_play` | integer |  |
| `hits` | integer | Hits. |
| `true_pass_set_pass_rush_win_rate` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `batted_passes` | integer |  |
| `true_pass_set_hits` | integer |  |
| `true_pass_set_snap_counts_pass_rush` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `true_pass_set_pass_rush_wins` | integer |  |
| `total_pressures` | integer |  |
| `true_pass_set_grades_pass_rush_defense` | numeric |  |
| `true_pass_set_batted_passes` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric |  |
| `true_pass_set_pass_rush_opp` | integer |  |
| `true_pass_set_pass_rush_percent` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `knockdowns` | integer |  |
| `knockdown_rate` | numeric |  |
| `pressure_rate` | numeric |  |
| `sack_rate` | numeric |  |
| `true_pass_set_knockdowns` | integer |  |
| `true_pass_set_knockdown_rate` | numeric |  |
| `true_pass_set_pressure_rate` | numeric |  |
| `true_pass_set_sack_rate` | numeric |  |
| `lhs_pass_rush_snaps` | integer |  |
| `lhs_pass_rush_percent` | numeric |  |
| `lhs_sacks` | integer |  |
| `lhs_hits` | integer |  |
| `lhs_hurries` | integer |  |
| `lhs_pressures` | integer |  |
| `lhs_prp` | numeric |  |
| `lhs_stops` | integer |  |
| `lhs_tackles` | integer |  |
| `lhs_assists` | integer |  |
| `lhs_misses` | integer |  |
| `rhs_pass_rush_snaps` | integer |  |
| `rhs_pass_rush_percent` | numeric |  |
| `rhs_sacks` | integer |  |
| `rhs_hits` | integer |  |
| `rhs_hurries` | integer |  |
| `rhs_pressures` | integer |  |
| `rhs_prp` | numeric |  |
| `rhs_stops` | integer |  |
| `rhs_tackles` | integer |  |
| `rhs_assists` | integer |  |
| `rhs_misses` | integer |  |
| `pass_rush_grade_oe_percentile` | numeric |  |
| `double_team_rate` | numeric |  |

**coverage**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `yards_per_coverage_snap` | numeric |  |
| `draft_season` | integer |  |
| `missed_tackles` | integer |  |
| `catch_rate` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | integer | Team tackles. |
| `coverage_percent` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `dropped_ints` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `grades_tackle` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `forced_incompletion_rate` | numeric |  |
| `grades_coverage_defense` | numeric |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `snap_counts_coverage` | integer |  |
| `grades_run_defense` | numeric |  |
| `snap_counts_pass_play` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `forced_incompletes` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `coverage_snaps_per_target` | numeric |  |
| `stops` | integer |  |
| `declined_penalties` | integer |  |
| `longest` | integer |  |
| `missed_tackle_rate` | numeric |  |
| `grades_defense` | numeric |  |
| `yards_per_reception` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `avg_depth_of_target` | numeric |  |
| `pass_break_ups` | integer |  |
| `qb_rating_against` | numeric |  |
| `coverage_snaps_per_reception` | numeric |  |
| `assists` | integer | Total assists. |
| `grades_pass_rush_defense` | numeric |  |
| `touchdowns` | integer |  |

**special-teams**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `assists` | integer | Total assists. |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_misc_st` | numeric |  |
| `grades_special_teams_penalty` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackles` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `snap_counts_field_goal` | integer |  |
| `snap_counts_field_goal_blocking` | integer |  |
| `snap_counts_kickoff` | integer |  |
| `snap_counts_kickoff_return` | integer |  |
| `snap_counts_punt_coverage` | integer |  |
| `snap_counts_punt_return` | integer |  |
| `tackles` | integer | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_snaps` | integer |  |
| `grades_fgep_offense` | numeric |  |
| `grades_punter` | numeric |  |
| `grades_fgep_kicker` | numeric |  |
| `grades_kickoff_kicker` | numeric |  |
| `grades_long_snap` | numeric |  |
| `grades_fgep_defense` | numeric |  |
| `grades_kick_return` | numeric |  |
| `grades_punt_return` | numeric |  |

**kick-returns**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kick_return` | numeric |  |
| `grades_punt_return` | numeric |  |
| `grades_return` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kickoff_attempts` | integer |  |
| `kickoff_fair_catches` | integer |  |
| `kickoff_long` | integer |  |
| `kickoff_muffed_returns` | integer |  |
| `kickoff_touchdowns` | integer |  |
| `kickoff_yards` | integer |  |
| `kickoff_ypa` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `punt_attempts` | integer |  |
| `punt_fair_catches` | integer |  |
| `punt_long` | integer |  |
| `punt_muffed_returns` | integer |  |
| `punt_touchdowns` | integer |  |
| `punt_yards` | integer |  |
| `punt_ypa` | numeric |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | integer |  |

**field-goals**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `twenty_attempts` | integer |  |
| `pat_percent` | numeric |  |
| `draft_season` | integer |  |
| `forty_made` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `fifty_percent` | numeric |  |
| `total_made` | integer |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `one_made` | integer |  |
| `fifty_attempts` | integer |  |
| `forty_attempts` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `thirty_percent` | numeric |  |
| `total_attempts` | integer |  |
| `pat_attempts` | integer |  |
| `twenty_made` | integer |  |
| `one_attempts` | integer |  |
| `grades_fgep_kicker` | numeric |  |
| `thirty_attempts` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `pat_made` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `one_percent` | integer |  |
| `total_percent` | numeric |  |
| `twenty_percent` | numeric |  |
| `forty_percent` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `fifty_made` | integer |  |
| `thirty_made` | integer |  |
| `field_goal_oe` | numeric |  |
| `field_goal_oe_total` | numeric |  |

**punting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `touchbacks` | integer |  |
| `attempts_with_hangtime` | integer |  |
| `draft_season` | integer |  |
| `percent_returned` | numeric |  |
| `fair_catches` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `average_net_yards` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `average_hangtime` | numeric |  |
| `total_net_yards` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `inside_twenties` | integer |  |
| `out_of_bounds` | integer | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `average_yards_per_return` | numeric |  |
| `total_hangtime` | numeric |  |
| `declined_penalties` | integer |  |
| `returns` | integer |  |
| `long` | integer |  |
| `blocks` | integer | Total blocks. |
| `average_yards_per_attempt` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_punter` | numeric |  |
| `return_yards` | integer | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `downeds` | integer |  |
| `snaps` | integer |  |

**kickoffs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `attempts_with_hangtime` | integer |  |
| `average_distance` | numeric |  |
| `average_hangtime` | numeric |  |
| `average_starting_field_position` | numeric |  |
| `average_yards_per_return` | numeric |  |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `fair_catches` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kickoff_kicker` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kicked_yards` | integer |  |
| `kicks_returned` | integer |  |
| `onside_kicks` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `percent_returned` | numeric |  |
| `player_game_count` | integer |  |
| `return_yards` | integer | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_hangtime` | numeric |  |
| `touchbacks` | integer |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_position_report(league='nfl', report='offense', season=2022)
```

_Last validated n/a._

## `pff_api_team_directory`

The league's teams for a season, with ids, slugs, colours and groups

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams`

**Valid URL:** [https://api.pff.com/v2/nfl/teams?season=2022](https://api.pff.com/v2/nfl/teams?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `slug` | character | URL slug for the team. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `city` | character | Venue city. |
| `nickname` | character | Team nickname / location label. |
| `abbreviation` | character | Metric abbreviation. |
| `primary_color` | character | Primary team color (hex). |
| `secondary_color` | character | Secondary team color (hex). |
| `group_ids` | character |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_directory(league='nfl', season=2022)
```

_Last validated n/a._

## `pff_api_team_stats`

Team stats table for one category, every value ranked against the scope

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/stats`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/stats?season=2022](https://api.pff.com/v2/nfl/teams/stats?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `weekIds` | `week_ids` |  |  | `Y` | Comma-separated week ids to cover instead of a whole weekGroup — 1,2,3 is the first three regular-season weeks. |
| `category` | `category` |  |  | `Y` | Which stat category the table covers. |
| `scope` | `scope` |  |  | `Y` | Which teams the ranks are computed against — and which rows come back: league (every team, the default), a conference (afc, nfc) or a division (afc-east … nfc-west). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**offense-overall-success**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `epa_per_play` | numeric |  |
| `epa_per_play_rank` | integer |  |
| `success_rate` | numeric |  |
| `success_rate_rank` | integer |  |
| `explosive_play_rate` | numeric |  |
| `explosive_play_rate_rank` | integer |  |
| `points_per_drive` | numeric |  |
| `points_per_drive_rank` | integer |  |
| `third_down_conversion_rate` | numeric |  |
| `third_down_conversion_rate_rank` | integer |  |
| `red_zone_conversion_rate` | numeric |  |
| `red_zone_conversion_rate_rank` | integer |  |
| `offensive_turnovers` | integer | The number of times a player loses possession to the other team. |
| `offensive_turnovers_rank` | integer |  |
| `wepa` | numeric | Opponent-adjusted weighted EPA (passing predicted points added). |
| `wepa_rank` | integer |  |
| `conversion_after4` | numeric |  |
| `conversion_after4_rank` | integer |  |

**offense-passing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `epa_per_pass_play` | numeric |  |
| `epa_per_pass_play_rank` | integer |  |
| `pass_success_rate` | numeric |  |
| `pass_success_rate_rank` | integer |  |
| `yards_per_attempt` | numeric |  |
| `yards_per_attempt_rank` | integer |  |
| `completion_percentage` | numeric | Percentage of completed passes |
| `completion_percentage_rank` | integer |  |
| `explosive_pass_rate` | numeric |  |
| `explosive_pass_rate_rank` | integer |  |
| `passer_rating` | numeric | Overall NFL passer rating |
| `passer_rating_rank` | integer |  |
| `pressure_rate_against` | numeric |  |
| `pressure_rate_against_rank` | integer |  |
| `pressure_oe_allowed` | numeric |  |
| `pressure_oe_allowed_rank` | integer |  |
| `sack_rate_against` | numeric |  |
| `sack_rate_against_rank` | integer |  |
| `man_coverage_pct_against` | numeric |  |
| `man_coverage_pct_against_rank` | integer |  |
| `zone_coverage_pct_against` | numeric |  |
| `zone_coverage_pct_against_rank` | integer |  |
| `scramble_rushing_yards` | integer |  |
| `scramble_rushing_yards_rank` | integer |  |
| `scramble_rushing_touchdowns` | integer |  |
| `scramble_rushing_touchdowns_rank` | integer |  |
| `scramble_yards_per_carry` | numeric |  |
| `scramble_yards_per_carry_rank` | integer |  |
| `pass_play_pct` | numeric |  |
| `pass_play_pct_rank` | integer |  |
| `is_pass_oe` | numeric |  |
| `is_pass_oe_rank` | integer |  |
| `play_action_rate` | numeric |  |
| `play_action_rate_rank` | integer |  |
| `adot` | numeric |  |
| `adot_rank` | integer |  |
| `screen_rate` | numeric |  |
| `screen_rate_rank` | integer |  |
| `time_to_throw` | numeric | Duration (in seconds) between the time of the ball being snapped and the time of release of a pass attempt |
| `time_to_throw_rank` | integer |  |
| `passes_behind_los_pct` | numeric |  |
| `passes_behind_los_pct_rank` | integer |  |
| `passes_short_pct` | numeric |  |
| `passes_short_pct_rank` | integer |  |
| `passes_intermediate_pct` | numeric |  |
| `passes_intermediate_pct_rank` | integer |  |
| `passes_deep_pct` | numeric |  |
| `passes_deep_pct_rank` | integer |  |
| `target_share_wr` | numeric |  |
| `target_share_wr_rank` | integer |  |
| `target_share_te` | numeric |  |
| `target_share_te_rank` | integer |  |
| `target_share_rb` | numeric |  |
| `target_share_rb_rank` | integer |  |

**offense-rushing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `epa_per_run_play` | numeric |  |
| `epa_per_run_play_rank` | integer |  |
| `rush_success_rate` | numeric |  |
| `rush_success_rate_rank` | integer |  |
| `explosive_run_rate` | numeric |  |
| `explosive_run_rate_rank` | integer |  |
| `designed_rushing_yards` | integer |  |
| `designed_rushing_yards_rank` | integer |  |
| `designed_rushing_touchdowns` | integer |  |
| `designed_rushing_touchdowns_rank` | integer |  |
| `designed_yards_per_carry` | numeric |  |
| `designed_yards_per_carry_rank` | integer |  |
| `yards_after_contact_per_carry` | numeric |  |
| `yards_after_contact_per_carry_rank` | integer |  |
| `yards_before_contact_per_carry` | numeric |  |
| `yards_before_contact_per_carry_rank` | integer |  |
| `run_stuff_rate` | numeric |  |
| `run_stuff_rate_rank` | integer |  |
| `run_play_pct` | numeric |  |
| `run_play_pct_rank` | integer |  |
| `run_rate_oe` | numeric |  |
| `run_rate_oe_rank` | integer |  |
| `zone_run_pct` | numeric |  |
| `zone_run_pct_rank` | integer |  |
| `gap_run_pct` | numeric |  |
| `gap_run_pct_rank` | integer |  |
| `inside_zone_rate` | numeric |  |
| `inside_zone_rate_rank` | integer |  |
| `outside_zone_rate` | numeric |  |
| `outside_zone_rate_rank` | integer |  |
| `man_duo_rate` | numeric |  |
| `man_duo_rate_rank` | integer |  |
| `power_rate` | numeric |  |
| `power_rate_rank` | integer |  |
| `counter_rate` | numeric |  |
| `counter_rate_rank` | integer |  |
| `pull_lead_rate` | numeric |  |
| `pull_lead_rate_rank` | integer |  |

**defense-overall-success**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `epa_per_play_allowed` | numeric |  |
| `epa_per_play_allowed_rank` | integer |  |
| `success_rate_allowed` | numeric |  |
| `success_rate_allowed_rank` | integer |  |
| `explosive_play_rate_allowed` | numeric |  |
| `explosive_play_rate_allowed_rank` | integer |  |
| `red_zone_conversion_rate_allowed` | numeric |  |
| `red_zone_conversion_rate_allowed_rank` | integer |  |
| `third_down_conversion_rate_allowed` | numeric |  |
| `third_down_conversion_rate_allowed_rank` | integer |  |
| `defensive_turnovers` | integer |  |
| `defensive_turnovers_rank` | integer |  |
| `points_per_drive_allowed` | numeric |  |
| `points_per_drive_allowed_rank` | integer |  |
| `missed_tackle_rate` | numeric |  |
| `missed_tackle_rate_rank` | integer |  |
| `wepa_allowed` | numeric |  |
| `wepa_allowed_rank` | integer |  |
| `conversion_after4_allowed` | numeric |  |
| `conversion_after4_allowed_rank` | integer |  |

**defense-passing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `epa_per_pass_play_allowed` | numeric |  |
| `epa_per_pass_play_allowed_rank` | integer |  |
| `pass_success_rate_allowed` | numeric |  |
| `pass_success_rate_allowed_rank` | integer |  |
| `explosive_pass_rate_allowed` | numeric |  |
| `explosive_pass_rate_allowed_rank` | integer |  |
| `yards_per_attempt_allowed` | numeric |  |
| `yards_per_attempt_allowed_rank` | integer |  |
| `completion_percentage_allowed` | numeric |  |
| `completion_percentage_allowed_rank` | integer |  |
| `passer_rating_allowed` | numeric |  |
| `passer_rating_allowed_rank` | integer |  |
| `passing_touchdowns_allowed` | integer |  |
| `passing_touchdowns_allowed_rank` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `interceptions_rank` | integer |  |
| `air_yard_pct_allowed` | numeric |  |
| `air_yard_pct_allowed_rank` | integer |  |
| `yac_pct_allowed` | numeric |  |
| `yac_pct_allowed_rank` | integer |  |
| `yac_per_reception_allowed` | numeric |  |
| `yac_per_reception_allowed_rank` | integer |  |
| `pressure_rate` | numeric |  |
| `pressure_rate_rank` | integer |  |
| `pressure_oe` | numeric |  |
| `pressure_oe_rank` | integer |  |
| `total_pressures` | integer |  |
| `total_pressures_rank` | integer |  |
| `sack_rate` | numeric |  |
| `sack_rate_rank` | integer |  |
| `sacks` | integer | The Number of times sacked. |
| `sacks_rank` | integer |  |
| `scramble_rushing_yards_allowed` | integer |  |
| `scramble_rushing_yards_allowed_rank` | integer |  |
| `scramble_rushing_touchdowns_allowed` | integer |  |
| `scramble_rushing_touchdowns_allowed_rank` | integer |  |
| `scramble_yards_per_carry_allowed` | numeric |  |
| `scramble_yards_per_carry_allowed_rank` | integer |  |
| `man_coverage_pct` | numeric |  |
| `man_coverage_pct_rank` | integer |  |
| `zone_coverage_pct` | numeric |  |
| `zone_coverage_pct_rank` | integer |  |

**defense-rushing**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `epa_per_run_play_allowed` | numeric |  |
| `epa_per_run_play_allowed_rank` | integer |  |
| `rush_success_rate_allowed` | numeric |  |
| `rush_success_rate_allowed_rank` | integer |  |
| `explosive_run_rate_allowed` | numeric |  |
| `explosive_run_rate_allowed_rank` | integer |  |
| `designed_rushing_yards_allowed` | integer |  |
| `designed_rushing_yards_allowed_rank` | integer |  |
| `designed_rushing_touchdowns_allowed` | integer |  |
| `designed_rushing_touchdowns_allowed_rank` | integer |  |
| `designed_yards_per_carry_allowed` | numeric |  |
| `designed_yards_per_carry_allowed_rank` | integer |  |
| `yards_after_contact_per_carry_allowed` | numeric |  |
| `yards_after_contact_per_carry_allowed_rank` | integer |  |
| `yards_before_contact_per_carry_allowed` | numeric |  |
| `yards_before_contact_per_carry_allowed_rank` | integer |  |
| `stuff_rate` | numeric |  |
| `stuff_rate_rank` | integer |  |

**defense-opponent-tendencies**

| col_name | type | description |
|---|---|---|
| `team_id` | integer | ESPN team id. |
| `abbreviation` | character | Metric abbreviation. |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `play_action_rate_against` | numeric |  |
| `play_action_rate_against_rank` | integer |  |
| `screen_rate_against` | numeric |  |
| `screen_rate_against_rank` | integer |  |
| `adot_against` | numeric |  |
| `adot_against_rank` | integer |  |
| `time_to_throw_against` | numeric |  |
| `time_to_throw_against_rank` | integer |  |
| `zone_run_pct_against` | numeric |  |
| `zone_run_pct_against_rank` | integer |  |
| `gap_run_pct_against` | numeric |  |
| `gap_run_pct_against_rank` | integer |  |
| `pass_rate_against` | numeric |  |
| `pass_rate_against_rank` | integer |  |
| `run_rate_against` | numeric |  |
| `run_rate_against_rank` | integer |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_stats(league='nfl', season=2022)
```

_Last validated n/a._

## `pff_api_team_roster`

A team's depth-chart roster with grades, ranks and snap counts

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/roster`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/roster?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/roster?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `jersey` | character | Athlete's jersey number as a string. |
| `position` | character | Position abbreviation. |
| `alignment` | character |  |
| `unit` | character |  |
| `depth_order` | integer |  |
| `grade` | numeric | ESPN recruit grade (0-100; `0` = not rated). |
| `grade_rank` | integer |  |
| `grade_rank_of` | integer |  |
| `height` | integer | Athlete height in inches. |
| `weight` | integer | Athlete weight in pounds. |
| `birth_date` | character | Player birth date (sourced from NFL. Other sources may differ) |
| `eligibility_year` | integer |  |
| `status` | character | Roster status (e.g. Active). |
| `snap_counts` | integer |  |
| `snap_pct` | numeric |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_roster(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._

## `pff_api_team_schedule`

A team's season schedule, with results and strength of schedule

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/schedule`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/schedule?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/schedule?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `week` | integer | Season week. |
| `week_label` | character |  |
| `is_bye` | logical |  |
| `game_id` | integer | Ten digit identifier for NFL game. |
| `kickoff` | character |  |
| `home_away` | character | `home` or `away`. |
| `opponent_franchise_id` | integer |  |
| `opponent_abbr` | character | Opponent abbreviation. |
| `opponent_name` | character | Opponent display name. |
| `opponent_division` | character |  |
| `opponent_wins` | integer |  |
| `opponent_losses` | integer |  |
| `result` | character | The number of points the home team scored minus the number of points the visiting team scored. Equals h_score - v_score. Is NA for games which haven't yet been played. Convenient for evaluating against the spread bets. |
| `team_score` | integer | Offense team score at the time of the play. |
| `opponent_score` | integer | Defense / opponent team score at the time of the play. |
| `sos_score` | numeric |  |
| `opponent_elo` | integer |  |
| `opponent_elo_rank` | integer |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_schedule(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._

## `pff_api_team_leaders`

A team's leaders for one position group, with rank and percentile

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/leaders`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/leaders?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/leaders?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `group` | `group` |  |  | `Y` | Which position group the leaders come from — receiving (the default), passing, rushing or defense. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**receiving**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `position` | character | Primary position as reported by NFL.com |
| `jersey` | character | Jersey number. |
| `games` | integer | Games played in career |
| `games_rank` | integer |  |
| `games_rank_of` | integer |  |
| `games_percentile` | integer |  |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `targets_rank` | integer |  |
| `targets_rank_of` | integer |  |
| `targets_percentile` | integer |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `receptions_rank` | integer |  |
| `receptions_rank_of` | integer |  |
| `receptions_percentile` | integer |  |
| `reception_pct` | numeric |  |
| `reception_pct_rank` | integer |  |
| `reception_pct_rank_of` | integer |  |
| `reception_pct_percentile` | integer |  |
| `receiving_yards` | integer | Numeric yards by the receiver_player_name, excluding yards gained in pass plays with laterals. This should equal official receiving statistics but could miss yards gained in pass plays with laterals. Please see the description of `lateral_receiver_player_name` for further information. |
| `receiving_yards_rank` | integer |  |
| `receiving_yards_rank_of` | integer |  |
| `receiving_yards_percentile` | integer |  |
| `yards_per_reception` | numeric |  |
| `yards_per_reception_rank` | integer |  |
| `yards_per_reception_rank_of` | integer |  |
| `yards_per_reception_percentile` | integer |  |
| `receiving_td` | integer | Receiving - passing reception touchdowns. |
| `receiving_td_rank` | integer |  |
| `receiving_td_rank_of` | integer |  |
| `receiving_td_percentile` | integer |  |
| `grade_offense` | numeric |  |
| `grade_offense_rank` | integer |  |
| `grade_offense_rank_of` | integer |  |
| `grade_offense_percentile` | integer |  |
| `grade_receiving` | numeric |  |
| `grade_receiving_rank` | integer |  |
| `grade_receiving_rank_of` | integer |  |
| `grade_receiving_percentile` | integer |  |
| `grade_drop` | numeric |  |
| `grade_drop_rank` | integer |  |
| `grade_drop_rank_of` | integer |  |
| `grade_drop_percentile` | integer |  |
| `grade_fumble` | numeric |  |
| `grade_fumble_rank` | integer |  |
| `grade_fumble_rank_of` | integer |  |
| `grade_fumble_percentile` | integer |  |
| `grade_pass_block` | numeric |  |
| `grade_pass_block_rank` | integer |  |
| `grade_pass_block_rank_of` | integer |  |
| `grade_pass_block_percentile` | integer |  |

**passing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `position` | character | Primary position as reported by NFL.com |
| `jersey` | character | Jersey number. |
| `games` | integer | Games played in career |
| `games_rank` | integer |  |
| `games_rank_of` | integer |  |
| `games_percentile` | integer |  |
| `pass_attempts` | integer | Career pass attempts |
| `pass_attempts_rank` | integer |  |
| `pass_attempts_rank_of` | integer |  |
| `pass_attempts_percentile` | integer |  |
| `completions` | integer | The number of completed passes. |
| `completions_rank` | integer |  |
| `completions_rank_of` | integer |  |
| `completions_percentile` | integer |  |
| `completion_pct` | numeric | Passing completion percentage. |
| `completion_pct_rank` | integer |  |
| `completion_pct_rank_of` | integer |  |
| `completion_pct_percentile` | integer |  |
| `passing_yards` | integer | Numeric yards by the passer_player_name, including yards gained in pass plays with laterals. This should equal official passing statistics. |
| `passing_yards_rank` | integer |  |
| `passing_yards_rank_of` | integer |  |
| `passing_yards_percentile` | integer |  |
| `yards_per_attempt` | numeric |  |
| `yards_per_attempt_rank` | integer |  |
| `yards_per_attempt_rank_of` | integer |  |
| `yards_per_attempt_percentile` | integer |  |
| `passing_td` | integer | Passing touchdowns thrown. |
| `passing_td_rank` | integer |  |
| `passing_td_rank_of` | integer |  |
| `passing_td_percentile` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `interceptions_rank` | integer |  |
| `interceptions_rank_of` | integer |  |
| `interceptions_percentile` | integer |  |
| `grade_offense` | numeric |  |
| `grade_offense_rank` | integer |  |
| `grade_offense_rank_of` | integer |  |
| `grade_offense_percentile` | integer |  |
| `grade_pass` | numeric |  |
| `grade_pass_rank` | integer |  |
| `grade_pass_rank_of` | integer |  |
| `grade_pass_percentile` | integer |  |
| `grade_run` | numeric |  |
| `grade_run_rank` | integer |  |
| `grade_run_rank_of` | integer |  |
| `grade_run_percentile` | integer |  |
| `grade_fumble` | numeric |  |
| `grade_fumble_rank` | integer |  |
| `grade_fumble_rank_of` | integer |  |
| `grade_fumble_percentile` | integer |  |

**rushing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `position` | character | Primary position as reported by NFL.com |
| `jersey` | character | Jersey number. |
| `games` | integer | Games played in career |
| `games_rank` | integer |  |
| `games_rank_of` | integer |  |
| `games_percentile` | integer |  |
| `rush_attempts` | integer | The number of rushing attempts |
| `rush_attempts_rank` | integer |  |
| `rush_attempts_rank_of` | integer |  |
| `rush_attempts_percentile` | integer |  |
| `rushing_yards` | integer | Numeric yards by the rusher_player_name, excluding yards gained in rush plays with laterals. This should equal official rushing statistics but could miss yards gained in rush plays with laterals. Please see the description of `lateral_rusher_player_name` for further information. |
| `rushing_yards_rank` | integer |  |
| `rushing_yards_rank_of` | integer |  |
| `rushing_yards_percentile` | integer |  |
| `yards_per_carry` | numeric |  |
| `yards_per_carry_rank` | integer |  |
| `yards_per_carry_rank_of` | integer |  |
| `yards_per_carry_percentile` | integer |  |
| `yards_after_contact_per_attempt` | numeric |  |
| `yards_after_contact_per_attempt_rank` | integer |  |
| `yards_after_contact_per_attempt_rank_of` | integer |  |
| `yards_after_contact_per_attempt_percentile` | integer |  |
| `rushing_td` | integer | Rushing touchdowns. |
| `rushing_td_rank` | integer |  |
| `rushing_td_rank_of` | integer |  |
| `rushing_td_percentile` | integer |  |
| `fumbles` | integer |  |
| `fumbles_rank` | integer |  |
| `fumbles_rank_of` | integer |  |
| `fumbles_percentile` | integer |  |
| `grade_offense` | numeric |  |
| `grade_offense_rank` | integer |  |
| `grade_offense_rank_of` | integer |  |
| `grade_offense_percentile` | integer |  |
| `grade_run` | numeric |  |
| `grade_run_rank` | integer |  |
| `grade_run_rank_of` | integer |  |
| `grade_run_percentile` | integer |  |
| `grade_fumble` | numeric |  |
| `grade_fumble_rank` | integer |  |
| `grade_fumble_rank_of` | integer |  |
| `grade_fumble_percentile` | integer |  |
| `grade_receiving` | numeric |  |
| `grade_receiving_rank` | integer |  |
| `grade_receiving_rank_of` | integer |  |
| `grade_receiving_percentile` | integer |  |
| `grade_pass_block` | numeric |  |
| `grade_pass_block_rank` | integer |  |
| `grade_pass_block_rank_of` | integer |  |
| `grade_pass_block_percentile` | integer |  |

**defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `name` | character | Name, as reported by MFL but reordered into FirstName LastName instead of Last, First |
| `position` | character | Primary position as reported by NFL.com |
| `jersey` | character | Jersey number. |
| `games` | integer | Games played in career |
| `games_rank` | integer |  |
| `games_rank_of` | integer |  |
| `games_percentile` | integer |  |
| `tackles` | integer | Team tackles. |
| `tackles_rank` | integer |  |
| `tackles_rank_of` | integer |  |
| `tackles_percentile` | integer |  |
| `assists` | integer | Total assists. |
| `assists_rank` | integer |  |
| `assists_rank_of` | integer |  |
| `assists_percentile` | integer |  |
| `sacks` | numeric | The Number of times sacked. |
| `sacks_rank` | integer |  |
| `sacks_rank_of` | integer |  |
| `sacks_percentile` | integer |  |
| `qb_hurries` | integer | Team QB hurries. |
| `qb_hurries_rank` | integer |  |
| `qb_hurries_rank_of` | integer |  |
| `qb_hurries_percentile` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `interceptions_rank` | integer |  |
| `interceptions_rank_of` | integer |  |
| `interceptions_percentile` | integer |  |
| `passes_defended` | integer |  |
| `passes_defended_rank` | integer |  |
| `passes_defended_rank_of` | integer |  |
| `passes_defended_percentile` | integer |  |
| `grade_defense` | numeric |  |
| `grade_defense_rank` | integer |  |
| `grade_defense_rank_of` | integer |  |
| `grade_defense_percentile` | integer |  |
| `grade_run_defense` | numeric |  |
| `grade_run_defense_rank` | integer |  |
| `grade_run_defense_rank_of` | integer |  |
| `grade_run_defense_percentile` | integer |  |
| `grade_tackle` | numeric |  |
| `grade_tackle_rank` | integer |  |
| `grade_tackle_rank_of` | integer |  |
| `grade_tackle_percentile` | integer |  |
| `grade_pass_rush` | numeric |  |
| `grade_pass_rush_rank` | integer |  |
| `grade_pass_rush_rank_of` | integer |  |
| `grade_pass_rush_percentile` | integer |  |
| `grade_coverage` | numeric |  |
| `grade_coverage_rank` | integer |  |
| `grade_coverage_rank_of` | integer |  |
| `grade_coverage_percentile` | integer |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_leaders(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._

## `pff_api_team_rushing_direction`

A team's rushing by direction, one row per rusher and gap, plus totals

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/reports/rushing-direction`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/rushing-direction?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/rushing-direction?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `direction` | character |  |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `yards` | integer | The number of receiving yards |
| `ypa` | numeric |  |
| `touchdowns` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `explosive` | integer |  |
| `yards_after_contact` | integer |  |
| `yco_attempt` | numeric |  |
| `longest` | integer |  |
| `avoided_tackles` | integer |  |
| `fumbles` | integer |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_rushing_direction(league='nfl', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._

## `pff_api_team_report`

One of nineteen player reports for a team, one row per player

**Endpoint URL:** `GET https://api.pff.com/v2/{league}/teams/{team}/reports/{report}`

**Valid URL:** [https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/offense?season=2022](https://api.pff.com/v2/nfl/teams/cincinnati-bengals/reports/offense?season=2022)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug: nfl or ncaa. |
| `team` | `team` |  | `Y` |  | The team, as its slug (los-angeles-rams) or its numeric franchise id (26) — both resolve through the league's team directory for the season, and both produce the same answer. |
| `report` | `report` |  | `Y` |  | Which report: offense, passing, passing-depth, passing-pressure, receiving, receiving-depth, rushing, blocking, pass-blocking, run-blocking, defense, run-defense, pass-rush, coverage, special-teams, kick-returns, field-goals, punting, kickoffs. |
| `season` | `season` |  |  | `Y` | Season, as a four-digit year: 2006 or later, and at most one year past the current season. |
| `weekGroup` | `week_group` |  |  | `Y` | Which part of the season to cover: REG (regular season), PO (playoffs) or REGPO (both, the default). |
| `week` | `week` |  |  | `Y` | Narrow the report to one week of the weekGroup — or, with weekTo, to a span of weeks. |
| `weekTo` | `week_to` |  |  | `Y` | The last week of a span that starts at week; requires week. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.
**offense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_offense` | numeric |  |
| `grades_offense_penalty` | numeric |  |
| `grades_pass_block` | numeric |  |
| `grades_run_block` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `snap_counts_pass` | integer |  |
| `snap_counts_pass_block` | integer |  |
| `snap_counts_pass_route` | integer |  |
| `snap_counts_run` | integer |  |
| `snap_counts_run_block` | integer |  |
| `snap_counts_total` | integer |  |
| `snap_counts_total_pass` | integer |  |
| `snap_counts_total_run` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_hands_fumble` | numeric |  |
| `grades_pass` | numeric |  |
| `grades_pass_route` | numeric |  |
| `grades_run` | numeric |  |

**passing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `grades_offense` | numeric |  |
| `twp_rate` | numeric |  |
| `btt_rate` | numeric |  |
| `spikes` | integer | Spikes |
| `dropbacks` | integer |  |
| `thrown_aways` | integer |  |
| `draft_season` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `grades_pass` | numeric |  |
| `hit_as_threw` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sack_percent` | numeric |  |
| `bats` | integer |  |
| `sacks` | integer | The Number of times sacked. |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `completions` | integer | The number of completed passes. |
| `yards` | integer | The number of receiving yards |
| `accuracy_percent` | numeric |  |
| `scrambles` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `drop_rate` | numeric |  |
| `grades_run` | numeric |  |
| `qb_rating` | numeric |  |
| `completion_percent` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `passing_snaps` | integer |  |
| `pressure_to_sack_rate` | numeric |  |
| `ypa` | numeric |  |
| `drops` | integer | Throws dropped |
| `grades_hands_fumble` | numeric |  |
| `avg_time_to_throw` | numeric | Average time elapsed from the time of snap to throw on every pass attempt for a passer (sacks excluded). |
| `big_time_throws` | integer |  |
| `positive_epa_percent` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `avg_depth_of_target` | numeric |  |
| `turnover_worthy_plays` | integer |  |
| `epa` | numeric | Expected points added (EPA) by the posteam for the given play. |
| `aimed_passes` | integer |  |
| `touchdowns` | integer |  |
| `def_gen_pressures` | integer |  |
| `npa_epa` | numeric |  |
| `npa_positive_epa_percent` | numeric |  |
| `no_screen_epa` | numeric |  |
| `no_screen_positive_epa_percent` | numeric |  |
| `pass_rate_oe` | numeric |  |
| `completion_oe` | numeric |  |
| `accuracy_oe` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**passing-depth**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `left_behind_los_accuracy_percent` | integer |  |
| `left_short_scrambles` | integer |  |
| `center_short_first_downs` | integer |  |
| `right_short_bats` | integer |  |
| `right_behind_los_positive_epa_percent` | numeric |  |
| `left_behind_los_completions` | integer |  |
| `left_medium_dropbacks` | integer |  |
| `right_medium_grades_pass` | numeric |  |
| `right_medium_hit_as_threw` | integer |  |
| `behind_los_attempts_percent` | numeric |  |
| `right_short_turnover_worthy_plays` | integer |  |
| `right_medium_qb_rating` | numeric |  |
| `deep_twp_rate` | numeric |  |
| `center_medium_sacks` | integer |  |
| `medium_interceptions` | integer |  |
| `left_deep_completions` | integer |  |
| `behind_los_spikes` | integer |  |
| `medium_aimed_passes` | integer |  |
| `center_behind_los_drops` | integer |  |
| `center_behind_los_interceptions` | integer |  |
| `behind_los_dropbacks` | integer |  |
| `left_behind_los_interceptions` | integer |  |
| `left_deep_first_downs` | integer |  |
| `deep_passing_snaps` | integer |  |
| `center_short_btt_rate` | integer |  |
| `center_medium_thrown_aways` | integer |  |
| `center_deep_avg_depth_of_target` | integer |  |
| `center_short_drops` | integer |  |
| `center_deep_first_downs` | integer |  |
| `left_medium_completion_percent` | numeric |  |
| `center_deep_attempts` | integer |  |
| `center_behind_los_attempts` | integer |  |
| `right_short_positive_epa_percent` | numeric |  |
| `center_deep_aimed_passes` | integer |  |
| `right_deep_grades_pass` | numeric |  |
| `right_medium_big_time_throws` | integer |  |
| `deep_sack_percent` | integer |  |
| `center_medium_pressure_to_sack_rate` | integer |  |
| `deep_sacks` | integer |  |
| `right_medium_ypa` | numeric |  |
| `left_deep_drop_rate` | integer |  |
| `right_medium_attempts_percent` | numeric |  |
| `right_deep_btt_rate` | numeric |  |
| `center_short_attempts_percent` | numeric |  |
| `deep_drops` | integer |  |
| `center_behind_los_big_time_throws` | integer |  |
| `center_behind_los_touchdowns` | integer |  |
| `right_behind_los_twp_rate` | integer |  |
| `center_deep_completions` | integer |  |
| `center_short_attempts` | integer |  |
| `left_behind_los_sack_percent` | integer |  |
| `center_short_pressure_to_sack_rate` | integer |  |
| `deep_touchdowns` | integer |  |
| `center_behind_los_accuracy_percent` | integer |  |
| `center_deep_drop_rate` | integer |  |
| `right_short_first_downs` | integer |  |
| `right_behind_los_first_downs` | integer |  |
| `short_interceptions` | integer |  |
| `center_medium_scrambles` | integer |  |
| `left_behind_los_sacks` | integer |  |
| `left_short_ypa` | numeric |  |
| `left_short_qb_rating` | numeric |  |
| `right_behind_los_qb_rating` | numeric |  |
| `draft_season` | integer |  |
| `right_behind_los_dropbacks` | integer |  |
| `behind_los_def_gen_pressures` | integer |  |
| `right_short_thrown_aways` | integer |  |
| `deep_def_gen_pressures` | integer |  |
| `right_short_btt_rate` | integer |  |
| `center_behind_los_positive_epa_percent` | integer |  |
| `left_behind_los_grades_pass` | numeric |  |
| `deep_grades_pass` | numeric |  |
| `center_short_turnover_worthy_plays` | integer |  |
| `center_behind_los_scrambles` | integer |  |
| `right_short_aimed_passes` | integer |  |
| `center_short_dropbacks` | integer |  |
| `medium_epa` | numeric |  |
| `right_short_ypa` | numeric |  |
| `center_medium_ypa` | numeric |  |
| `medium_attempts` | integer |  |
| `right_deep_accuracy_percent` | numeric |  |
| `behind_los_scrambles` | integer |  |
| `center_behind_los_completion_percent` | integer |  |
| `left_behind_los_btt_rate` | integer |  |
| `right_behind_los_btt_rate` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `short_touchdowns` | integer |  |
| `center_medium_drops` | integer |  |
| `left_behind_los_aimed_passes` | integer |  |
| `deep_attempts` | integer |  |
| `right_behind_los_sack_percent` | integer |  |
| `center_short_positive_epa_percent` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `left_deep_btt_rate` | numeric |  |
| `medium_scrambles` | integer |  |
| `center_behind_los_avg_depth_of_target` | numeric |  |
| `short_attempts_percent` | numeric |  |
| `right_behind_los_sacks` | integer |  |
| `center_medium_avg_time_to_throw` | numeric |  |
| `left_short_pressure_to_sack_rate` | integer |  |
| `center_deep_sack_percent` | integer |  |
| `center_deep_ypa` | numeric |  |
| `left_behind_los_hit_as_threw` | integer |  |
| `medium_big_time_throws` | integer |  |
| `deep_thrown_aways` | integer |  |
| `right_short_accuracy_percent` | numeric |  |
| `left_deep_turnover_worthy_plays` | integer |  |
| `center_medium_bats` | integer |  |
| `right_short_grades_pass` | numeric |  |
| `right_deep_spikes` | integer |  |
| `left_deep_passing_snaps` | integer |  |
| `center_medium_twp_rate` | numeric |  |
| `right_behind_los_passing_snaps` | integer |  |
| `left_deep_qb_rating` | numeric |  |
| `right_deep_drop_rate` | numeric |  |
| `left_behind_los_drop_rate` | numeric |  |
| `left_medium_drop_rate` | numeric |  |
| `right_deep_attempts` | integer |  |
| `left_deep_spikes` | integer |  |
| `center_behind_los_dropbacks` | integer |  |
| `right_deep_big_time_throws` | integer |  |
| `medium_hit_as_threw` | integer |  |
| `right_short_dropbacks` | integer |  |
| `medium_def_gen_pressures` | integer |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `left_short_positive_epa_percent` | numeric |  |
| `left_short_attempts` | integer |  |
| `center_deep_twp_rate` | numeric |  |
| `right_medium_avg_time_to_throw` | numeric |  |
| `left_behind_los_qb_rating` | numeric |  |
| `left_behind_los_dropbacks` | integer |  |
| `deep_dropbacks` | integer |  |
| `right_behind_los_scrambles` | integer |  |
| `behind_los_interceptions` | integer |  |
| `right_deep_drops` | integer |  |
| `right_deep_yards` | integer |  |
| `right_short_hit_as_threw` | integer |  |
| `right_short_avg_depth_of_target` | numeric |  |
| `short_bats` | integer |  |
| `right_deep_twp_rate` | numeric |  |
| `right_behind_los_pressure_to_sack_rate` | integer |  |
| `deep_spikes` | integer |  |
| `center_medium_spikes` | integer |  |
| `behind_los_aimed_passes` | integer |  |
| `right_behind_los_touchdowns` | integer |  |
| `right_medium_pressure_to_sack_rate` | integer |  |
| `medium_thrown_aways` | integer |  |
| `short_sacks` | integer |  |
| `center_behind_los_twp_rate` | integer |  |
| `left_behind_los_attempts` | integer |  |
| `short_aimed_passes` | integer |  |
| `short_completions` | integer |  |
| `right_short_pressure_to_sack_rate` | integer |  |
| `medium_avg_depth_of_target` | numeric |  |
| `left_behind_los_positive_epa_percent` | numeric |  |
| `center_medium_attempts_percent` | numeric |  |
| `left_medium_touchdowns` | integer |  |
| `center_short_bats` | integer |  |
| `left_deep_avg_time_to_throw` | numeric |  |
| `center_behind_los_passing_snaps` | integer |  |
| `center_short_yards` | integer |  |
| `right_medium_btt_rate` | numeric |  |
| `right_medium_scrambles` | integer |  |
| `medium_btt_rate` | numeric |  |
| `center_behind_los_btt_rate` | integer |  |
| `center_short_avg_depth_of_target` | numeric |  |
| `left_medium_sacks` | integer |  |
| `right_short_yards` | integer |  |
| `left_medium_first_downs` | integer |  |
| `right_medium_positive_epa_percent` | numeric |  |
| `left_short_big_time_throws` | integer |  |
| `deep_turnover_worthy_plays` | integer |  |
| `left_deep_bats` | integer |  |
| `left_medium_turnover_worthy_plays` | integer |  |
| `base_dropbacks` | integer |  |
| `center_deep_drops` | integer |  |
| `center_medium_avg_depth_of_target` | numeric |  |
| `center_behind_los_ypa` | numeric |  |
| `right_short_attempts` | integer |  |
| `center_medium_completion_percent` | numeric |  |
| `right_short_sack_percent` | integer |  |
| `left_behind_los_avg_depth_of_target` | numeric |  |
| `center_medium_epa` | numeric |  |
| `center_behind_los_sack_percent` | integer |  |
| `medium_completions` | integer |  |
| `medium_sack_percent` | integer |  |
| `left_short_grades_pass` | numeric |  |
| `deep_btt_rate` | numeric |  |
| `left_short_accuracy_percent` | numeric |  |
| `short_twp_rate` | numeric |  |
| `short_passing_snaps` | integer |  |
| `left_deep_sacks` | integer |  |
| `short_scrambles` | integer |  |
| `center_medium_turnover_worthy_plays` | integer |  |
| `behind_los_drop_rate` | numeric |  |
| `right_behind_los_epa` | numeric |  |
| `right_medium_aimed_passes` | integer |  |
| `short_accuracy_percent` | numeric |  |
| `player_game_count` | integer |  |
| `short_btt_rate` | integer |  |
| `right_short_touchdowns` | integer |  |
| `center_short_completions` | integer |  |
| `right_short_spikes` | integer |  |
| `behind_los_first_downs` | integer |  |
| `right_medium_def_gen_pressures` | integer |  |
| `deep_pressure_to_sack_rate` | integer |  |
| `medium_qb_rating` | numeric |  |
| `short_completion_percent` | numeric |  |
| `right_deep_attempts_percent` | numeric |  |
| `eligible_season` | integer |  |
| `short_big_time_throws` | integer |  |
| `behind_los_attempts` | integer |  |
| `center_behind_los_yards` | integer |  |
| `center_behind_los_bats` | integer |  |
| `center_behind_los_qb_rating` | numeric |  |
| `right_deep_pressure_to_sack_rate` | integer |  |
| `center_short_accuracy_percent` | numeric |  |
| `left_behind_los_bats` | integer |  |
| `center_deep_passing_snaps` | integer |  |
| `right_behind_los_hit_as_threw` | integer |  |
| `medium_bats` | integer |  |
| `right_medium_first_downs` | integer |  |
| `medium_spikes` | integer |  |
| `left_deep_thrown_aways` | integer |  |
| `right_deep_epa` | numeric |  |
| `center_medium_hit_as_threw` | integer |  |
| `left_behind_los_completion_percent` | numeric |  |
| `center_short_ypa` | numeric |  |
| `right_short_drop_rate` | numeric |  |
| `right_behind_los_accuracy_percent` | numeric |  |
| `left_short_yards` | integer |  |
| `deep_accuracy_percent` | numeric |  |
| `right_behind_los_avg_depth_of_target` | numeric |  |
| `center_behind_los_thrown_aways` | integer |  |
| `center_behind_los_attempts_percent` | numeric |  |
| `right_deep_first_downs` | integer |  |
| `left_short_completions` | integer |  |
| `deep_completion_percent` | numeric |  |
| `left_deep_positive_epa_percent` | numeric |  |
| `right_deep_sack_percent` | integer |  |
| `behind_los_pressure_to_sack_rate` | integer |  |
| `right_deep_touchdowns` | integer |  |
| `left_behind_los_def_gen_pressures` | integer |  |
| `left_behind_los_drops` | integer |  |
| `deep_epa` | numeric |  |
| `left_deep_ypa` | numeric |  |
| `medium_touchdowns` | integer |  |
| `left_medium_grades_pass` | numeric |  |
| `right_behind_los_spikes` | integer |  |
| `medium_grades_pass` | numeric |  |
| `behind_los_passing_snaps` | integer |  |
| `left_short_first_downs` | integer |  |
| `center_short_passing_snaps` | integer |  |
| `center_deep_accuracy_percent` | numeric |  |
| `right_deep_positive_epa_percent` | numeric |  |
| `right_medium_sack_percent` | integer |  |
| `right_deep_qb_rating` | numeric |  |
| `right_short_completion_percent` | numeric |  |
| `right_behind_los_interceptions` | integer |  |
| `behind_los_yards` | integer |  |
| `center_behind_los_grades_pass` | numeric |  |
| `left_deep_dropbacks` | integer |  |
| `center_behind_los_spikes` | integer |  |
| `right_behind_los_aimed_passes` | integer |  |
| `left_short_interceptions` | integer |  |
| `right_medium_interceptions` | integer |  |
| `left_behind_los_yards` | integer |  |
| `center_deep_btt_rate` | numeric |  |
| `medium_first_downs` | integer |  |
| `left_short_avg_depth_of_target` | integer |  |
| `left_behind_los_ypa` | integer |  |
| `short_attempts` | integer |  |
| `right_medium_bats` | integer |  |
| `left_behind_los_touchdowns` | integer |  |
| `right_medium_dropbacks` | integer |  |
| `short_turnover_worthy_plays` | integer |  |
| `right_deep_thrown_aways` | integer |  |
| `right_behind_los_drop_rate` | numeric |  |
| `right_short_qb_rating` | integer |  |
| `medium_sacks` | integer |  |
| `center_deep_attempts_percent` | integer |  |
| `left_short_dropbacks` | integer |  |
| `behind_los_drops` | integer |  |
| `short_sack_percent` | integer |  |
| `left_medium_spikes` | integer |  |
| `left_medium_accuracy_percent` | numeric |  |
| `behind_los_completions` | integer |  |
| `behind_los_sack_percent` | integer |  |
| `left_deep_yards` | integer |  |
| `left_short_aimed_passes` | integer |  |
| `center_behind_los_completions` | integer |  |
| `left_short_thrown_aways` | integer |  |
| `left_short_sack_percent` | integer |  |
| `center_short_thrown_aways` | integer |  |
| `center_short_interceptions` | integer |  |
| `short_avg_depth_of_target` | numeric |  |
| `left_deep_touchdowns` | integer |  |
| `deep_yards` | integer |  |
| `center_behind_los_turnover_worthy_plays` | integer |  |
| `medium_ypa` | numeric |  |
| `left_medium_epa` | numeric |  |
| `left_deep_avg_depth_of_target` | integer |  |
| `deep_first_downs` | integer |  |
| `short_drop_rate` | integer |  |
| `left_medium_bats` | integer |  |
| `center_deep_spikes` | integer |  |
| `center_short_hit_as_threw` | integer |  |
| `left_medium_positive_epa_percent` | numeric |  |
| `center_deep_touchdowns` | integer |  |
| `right_deep_ypa` | numeric |  |
| `right_short_big_time_throws` | integer |  |
| `center_short_big_time_throws` | integer |  |
| `short_positive_epa_percent` | numeric |  |
| `left_behind_los_avg_time_to_throw` | numeric |  |
| `right_deep_passing_snaps` | integer |  |
| `right_short_twp_rate` | numeric |  |
| `center_medium_attempts` | integer |  |
| `right_deep_avg_time_to_throw` | numeric |  |
| `center_deep_def_gen_pressures` | integer |  |
| `behind_los_epa` | numeric |  |
| `right_medium_twp_rate` | numeric |  |
| `right_deep_aimed_passes` | integer |  |
| `right_deep_scrambles` | integer |  |
| `deep_big_time_throws` | integer |  |
| `left_short_turnover_worthy_plays` | integer |  |
| `center_short_touchdowns` | integer |  |
| `right_medium_drops` | integer |  |
| `left_deep_epa` | numeric |  |
| `short_ypa` | numeric |  |
| `medium_pressure_to_sack_rate` | integer |  |
| `left_medium_thrown_aways` | integer |  |
| `right_behind_los_avg_time_to_throw` | numeric |  |
| `behind_los_btt_rate` | integer |  |
| `medium_avg_time_to_throw` | numeric |  |
| `center_deep_completion_percent` | numeric |  |
| `behind_los_avg_time_to_throw` | numeric |  |
| `right_medium_touchdowns` | integer |  |
| `center_short_avg_time_to_throw` | numeric |  |
| `left_deep_aimed_passes` | integer |  |
| `left_medium_yards` | integer |  |
| `center_medium_touchdowns` | integer |  |
| `center_short_drop_rate` | numeric |  |
| `left_short_twp_rate` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `right_behind_los_attempts_percent` | numeric |  |
| `right_deep_avg_depth_of_target` | numeric |  |
| `behind_los_sacks` | integer |  |
| `center_deep_yards` | integer |  |
| `short_dropbacks` | integer |  |
| `left_deep_sack_percent` | integer |  |
| `center_behind_los_avg_time_to_throw` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `left_behind_los_epa` | numeric |  |
| `medium_twp_rate` | numeric |  |
| `left_short_def_gen_pressures` | integer |  |
| `medium_turnover_worthy_plays` | integer |  |
| `behind_los_twp_rate` | integer |  |
| `left_behind_los_thrown_aways` | integer |  |
| `left_short_bats` | integer |  |
| `center_medium_drop_rate` | numeric |  |
| `right_deep_bats` | integer |  |
| `medium_yards` | integer |  |
| `center_deep_grades_pass` | numeric |  |
| `center_medium_passing_snaps` | integer |  |
| `center_behind_los_first_downs` | integer |  |
| `center_medium_interceptions` | integer |  |
| `behind_los_grades_pass` | numeric |  |
| `left_short_hit_as_threw` | integer |  |
| `deep_qb_rating` | numeric |  |
| `center_deep_bats` | integer |  |
| `behind_los_ypa` | integer |  |
| `right_short_interceptions` | integer |  |
| `left_deep_interceptions` | integer |  |
| `right_medium_sacks` | integer |  |
| `right_behind_los_turnover_worthy_plays` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `right_medium_spikes` | integer |  |
| `behind_los_hit_as_threw` | integer |  |
| `left_medium_pressure_to_sack_rate` | integer |  |
| `medium_dropbacks` | integer |  |
| `deep_hit_as_threw` | integer |  |
| `right_medium_completion_percent` | numeric |  |
| `short_epa` | numeric |  |
| `deep_drop_rate` | numeric |  |
| `declined_penalties` | integer |  |
| `deep_scrambles` | integer |  |
| `left_deep_completion_percent` | numeric |  |
| `right_short_epa` | numeric |  |
| `medium_attempts_percent` | numeric |  |
| `right_behind_los_completion_percent` | numeric |  |
| `left_medium_hit_as_threw` | integer |  |
| `left_behind_los_pressure_to_sack_rate` | integer |  |
| `right_short_def_gen_pressures` | integer |  |
| `right_short_attempts_percent` | numeric |  |
| `center_short_aimed_passes` | integer |  |
| `short_hit_as_threw` | integer |  |
| `right_medium_turnover_worthy_plays` | integer |  |
| `left_short_epa` | numeric |  |
| `center_behind_los_pressure_to_sack_rate` | integer |  |
| `right_behind_los_ypa` | numeric |  |
| `deep_interceptions` | integer |  |
| `left_medium_twp_rate` | numeric |  |
| `right_behind_los_big_time_throws` | integer |  |
| `center_medium_sack_percent` | integer |  |
| `center_deep_thrown_aways` | integer |  |
| `short_thrown_aways` | integer |  |
| `left_short_btt_rate` | integer |  |
| `right_medium_accuracy_percent` | numeric |  |
| `short_def_gen_pressures` | integer |  |
| `center_deep_dropbacks` | integer |  |
| `center_medium_accuracy_percent` | numeric |  |
| `right_behind_los_yards` | integer |  |
| `right_medium_attempts` | integer |  |
| `left_medium_attempts` | integer |  |
| `medium_accuracy_percent` | numeric |  |
| `left_medium_drops` | integer |  |
| `left_short_avg_time_to_throw` | numeric |  |
| `medium_passing_snaps` | integer |  |
| `center_short_epa` | numeric |  |
| `left_behind_los_passing_snaps` | integer |  |
| `left_deep_pressure_to_sack_rate` | integer |  |
| `deep_bats` | integer |  |
| `left_medium_passing_snaps` | integer |  |
| `left_medium_scrambles` | integer |  |
| `right_deep_completion_percent` | numeric |  |
| `left_short_drops` | integer |  |
| `left_deep_accuracy_percent` | numeric |  |
| `medium_positive_epa_percent` | numeric |  |
| `behind_los_turnover_worthy_plays` | integer |  |
| `right_medium_epa` | numeric |  |
| `right_deep_dropbacks` | integer |  |
| `deep_ypa` | numeric |  |
| `center_medium_completions` | integer |  |
| `left_medium_avg_time_to_throw` | numeric |  |
| `left_short_sacks` | integer |  |
| `left_behind_los_spikes` | integer |  |
| `left_deep_def_gen_pressures` | integer |  |
| `center_short_qb_rating` | numeric |  |
| `center_deep_interceptions` | integer |  |
| `right_deep_completions` | integer |  |
| `center_behind_los_sacks` | integer |  |
| `deep_completions` | integer |  |
| `left_medium_attempts_percent` | numeric |  |
| `short_yards` | integer |  |
| `behind_los_qb_rating` | numeric |  |
| `right_short_sacks` | integer |  |
| `right_behind_los_thrown_aways` | integer |  |
| `base_attempts` | integer |  |
| `center_short_spikes` | integer |  |
| `center_behind_los_hit_as_threw` | integer |  |
| `center_deep_turnover_worthy_plays` | integer |  |
| `left_medium_sack_percent` | integer |  |
| `behind_los_completion_percent` | numeric |  |
| `left_deep_attempts_percent` | numeric |  |
| `center_deep_big_time_throws` | integer |  |
| `left_medium_avg_depth_of_target` | numeric |  |
| `behind_los_thrown_aways` | integer |  |
| `center_medium_def_gen_pressures` | integer |  |
| `short_avg_time_to_throw` | numeric |  |
| `left_deep_twp_rate` | integer |  |
| `center_behind_los_epa` | numeric |  |
| `left_behind_los_big_time_throws` | integer |  |
| `right_medium_yards` | integer |  |
| `right_medium_drop_rate` | numeric |  |
| `center_medium_big_time_throws` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `left_short_completion_percent` | numeric |  |
| `left_short_drop_rate` | integer |  |
| `left_behind_los_scrambles` | integer |  |
| `left_medium_btt_rate` | numeric |  |
| `right_deep_hit_as_threw` | integer |  |
| `behind_los_bats` | integer |  |
| `short_first_downs` | integer |  |
| `left_deep_attempts` | integer |  |
| `right_behind_los_attempts` | integer |  |
| `right_deep_sacks` | integer |  |
| `left_behind_los_attempts_percent` | numeric |  |
| `behind_los_big_time_throws` | integer |  |
| `center_deep_pressure_to_sack_rate` | integer |  |
| `left_medium_big_time_throws` | integer |  |
| `right_behind_los_bats` | integer |  |
| `left_medium_def_gen_pressures` | integer |  |
| `right_deep_turnover_worthy_plays` | integer |  |
| `left_short_spikes` | integer |  |
| `left_medium_aimed_passes` | integer |  |
| `left_behind_los_twp_rate` | integer |  |
| `short_grades_pass` | numeric |  |
| `right_short_drops` | integer |  |
| `right_short_avg_time_to_throw` | numeric |  |
| `right_medium_thrown_aways` | integer |  |
| `center_short_sack_percent` | integer |  |
| `right_short_passing_snaps` | integer |  |
| `center_short_def_gen_pressures` | integer |  |
| `center_medium_positive_epa_percent` | integer |  |
| `medium_drops` | integer |  |
| `center_medium_aimed_passes` | integer |  |
| `center_short_sacks` | integer |  |
| `deep_attempts_percent` | integer |  |
| `center_behind_los_def_gen_pressures` | integer |  |
| `left_short_passing_snaps` | integer |  |
| `center_medium_grades_pass` | numeric |  |
| `center_deep_avg_time_to_throw` | numeric |  |
| `behind_los_positive_epa_percent` | numeric |  |
| `center_short_completion_percent` | numeric |  |
| `left_deep_hit_as_threw` | integer |  |
| `center_short_grades_pass` | numeric |  |
| `left_short_attempts_percent` | numeric |  |
| `left_deep_scrambles` | integer |  |
| `right_short_completions` | integer |  |
| `center_behind_los_aimed_passes` | integer |  |
| `right_short_scrambles` | integer |  |
| `center_medium_qb_rating` | numeric |  |
| `deep_avg_time_to_throw` | numeric |  |
| `left_deep_drops` | integer |  |
| `right_behind_los_def_gen_pressures` | integer |  |
| `center_deep_positive_epa_percent` | numeric |  |
| `center_deep_hit_as_threw` | integer |  |
| `short_spikes` | integer |  |
| `right_behind_los_completions` | integer |  |
| `center_behind_los_drop_rate` | numeric |  |
| `short_drops` | integer |  |
| `deep_aimed_passes` | integer |  |
| `right_medium_avg_depth_of_target` | numeric |  |
| `short_pressure_to_sack_rate` | integer |  |
| `center_deep_qb_rating` | numeric |  |
| `left_medium_completions` | integer |  |
| `center_deep_sacks` | integer |  |
| `left_deep_big_time_throws` | integer |  |
| `center_medium_first_downs` | integer |  |
| `center_medium_dropbacks` | integer |  |
| `right_deep_def_gen_pressures` | integer |  |
| `left_short_touchdowns` | integer |  |
| `medium_drop_rate` | numeric |  |
| `left_medium_ypa` | integer |  |
| `right_medium_passing_snaps` | integer |  |
| `center_short_scrambles` | integer |  |
| `left_behind_los_turnover_worthy_plays` | integer |  |
| `center_medium_yards` | integer |  |
| `center_deep_epa` | numeric |  |
| `left_medium_interceptions` | integer |  |
| `right_deep_interceptions` | integer |  |
| `medium_completion_percent` | numeric |  |
| `right_behind_los_grades_pass` | numeric |  |
| `deep_positive_epa_percent` | numeric |  |
| `behind_los_accuracy_percent` | numeric |  |
| `center_deep_scrambles` | integer |  |
| `center_short_twp_rate` | integer |  |
| `behind_los_touchdowns` | integer |  |
| `left_medium_qb_rating` | numeric |  |
| `right_medium_completions` | integer |  |
| `right_behind_los_drops` | integer |  |
| `left_behind_los_first_downs` | integer |  |
| `short_qb_rating` | numeric |  |
| `center_medium_btt_rate` | numeric |  |
| `left_deep_grades_pass` | numeric |  |

**passing-pressure**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `no_blitz_completion_percent` | numeric |  |
| `grades_offense` | numeric |  |
| `no_pressure_scrambles` | integer |  |
| `blitz_touchdowns` | integer |  |
| `pressure_yards` | integer |  |
| `no_pressure_spikes` | integer |  |
| `blitz_ypa` | numeric |  |
| `no_blitz_grades_run` | numeric |  |
| `blitz_qb_rating` | numeric |  |
| `no_pressure_thrown_aways` | integer |  |
| `no_blitz_bats` | integer |  |
| `no_blitz_drops` | integer |  |
| `no_pressure_completion_percent` | numeric |  |
| `blitz_aimed_passes` | integer |  |
| `pressure_grades_run` | numeric |  |
| `no_blitz_sack_percent` | numeric |  |
| `no_pressure_bats` | integer |  |
| `pressure_completions` | integer |  |
| `blitz_big_time_throws` | integer |  |
| `no_blitz_drop_rate` | numeric |  |
| `blitz_spikes` | integer |  |
| `no_pressure_completions` | integer |  |
| `draft_season` | integer |  |
| `no_pressure_passing_snaps` | integer |  |
| `no_blitz_first_downs` | integer |  |
| `blitz_avg_time_to_throw` | numeric |  |
| `no_pressure_grades_hands_fumble` | numeric |  |
| `pressure_aimed_passes` | integer |  |
| `blitz_sacks` | integer |  |
| `no_pressure_interceptions` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pressure_epa` | numeric |  |
| `blitz_completions` | integer |  |
| `blitz_attempts` | integer |  |
| `pressure_twp_rate` | numeric |  |
| `pressure_sacks` | integer |  |
| `no_blitz_pressure_to_sack_rate` | numeric |  |
| `no_pressure_ypa` | numeric |  |
| `pressure_passing_snaps` | integer |  |
| `grades_pass` | numeric |  |
| `pressure_bats` | integer |  |
| `blitz_thrown_aways` | integer |  |
| `no_pressure_drops` | integer |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `pressure_grades_offense_penalty` | numeric |  |
| `pressure_scrambles` | integer |  |
| `blitz_drop_rate` | numeric |  |
| `pressure_grades_pass_route` | numeric |  |
| `blitz_completion_percent` | numeric |  |
| `no_pressure_first_downs` | integer |  |
| `blitz_grades_offense_penalty` | numeric |  |
| `no_blitz_epa` | numeric |  |
| `blitz_interceptions` | integer |  |
| `no_blitz_dropbacks` | integer |  |
| `no_blitz_grades_pass` | numeric |  |
| `no_blitz_scrambles` | integer |  |
| `pressure_drop_rate` | numeric |  |
| `no_blitz_yards` | integer |  |
| `base_dropbacks` | integer |  |
| `pressure_pressure_to_sack_rate` | numeric |  |
| `no_blitz_grades_hands_fumble` | numeric |  |
| `no_pressure_grades_offense` | numeric |  |
| `no_pressure_avg_time_to_throw` | numeric |  |
| `pressure_dropbacks` | integer |  |
| `no_blitz_grades_offense_penalty` | numeric |  |
| `no_pressure_grades_run` | numeric |  |
| `player_game_count` | integer |  |
| `blitz_turnover_worthy_plays` | integer |  |
| `no_blitz_touchdowns` | integer |  |
| `no_blitz_avg_depth_of_target` | numeric |  |
| `no_pressure_dropbacks_percent` | numeric |  |
| `blitz_grades_pass` | numeric |  |
| `eligible_season` | integer |  |
| `blitz_avg_depth_of_target` | numeric |  |
| `no_blitz_spikes` | integer |  |
| `no_pressure_dropbacks` | integer |  |
| `blitz_btt_rate` | numeric |  |
| `blitz_positive_epa_percent` | numeric |  |
| `no_pressure_btt_rate` | numeric |  |
| `no_pressure_drop_rate` | numeric |  |
| `no_blitz_turnover_worthy_plays` | integer |  |
| `pressure_positive_epa_percent` | numeric |  |
| `blitz_first_downs` | integer |  |
| `no_blitz_dropbacks_percent` | numeric |  |
| `pressure_turnover_worthy_plays` | integer |  |
| `no_pressure_epa` | numeric |  |
| `no_blitz_avg_time_to_throw` | numeric |  |
| `no_blitz_positive_epa_percent` | numeric |  |
| `pressure_btt_rate` | numeric |  |
| `no_pressure_aimed_passes` | integer |  |
| `pressure_dropbacks_percent` | numeric |  |
| `grades_run` | numeric |  |
| `no_pressure_def_gen_pressures` | integer |  |
| `pressure_grades_offense` | numeric |  |
| `pressure_completion_percent` | numeric |  |
| `pressure_avg_depth_of_target` | integer |  |
| `blitz_epa` | numeric |  |
| `pressure_drops` | integer |  |
| `no_blitz_def_gen_pressures` | integer |  |
| `pressure_attempts` | integer |  |
| `pressure_big_time_throws` | integer |  |
| `no_blitz_thrown_aways` | integer |  |
| `blitz_drops` | integer |  |
| `no_pressure_touchdowns` | integer |  |
| `no_pressure_sacks` | integer |  |
| `blitz_sack_percent` | numeric |  |
| `pressure_sack_percent` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `no_pressure_attempts` | integer |  |
| `no_blitz_grades_offense` | numeric |  |
| `blitz_scrambles` | integer |  |
| `blitz_dropbacks_percent` | numeric |  |
| `no_pressure_qb_rating` | numeric |  |
| `no_blitz_passing_snaps` | integer |  |
| `no_blitz_aimed_passes` | integer |  |
| `blitz_yards` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `no_blitz_attempts` | integer |  |
| `pressure_accuracy_percent` | numeric |  |
| `no_blitz_hit_as_threw` | integer |  |
| `pressure_hit_as_threw` | integer |  |
| `blitz_pressure_to_sack_rate` | numeric |  |
| `declined_penalties` | integer |  |
| `no_blitz_btt_rate` | numeric |  |
| `no_blitz_ypa` | numeric |  |
| `blitz_grades_run` | numeric |  |
| `pressure_interceptions` | integer |  |
| `blitz_grades_hands_fumble` | numeric |  |
| `no_pressure_grades_offense_penalty` | numeric |  |
| `blitz_grades_offense` | numeric |  |
| `grades_hands_fumble` | numeric |  |
| `pressure_def_gen_pressures` | integer |  |
| `pressure_grades_pass` | numeric |  |
| `no_blitz_qb_rating` | numeric |  |
| `blitz_hit_as_threw` | integer |  |
| `pressure_ypa` | numeric |  |
| `blitz_grades_pass_route` | numeric |  |
| `pressure_avg_time_to_throw` | numeric |  |
| `no_blitz_completions` | integer |  |
| `no_pressure_grades_pass_route` | numeric |  |
| `no_pressure_sack_percent` | integer |  |
| `no_blitz_accuracy_percent` | integer |  |
| `no_pressure_pressure_to_sack_rate` | character |  |
| `no_pressure_turnover_worthy_plays` | integer |  |
| `pressure_first_downs` | integer |  |
| `blitz_passing_snaps` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `no_pressure_twp_rate` | numeric |  |
| `no_blitz_sacks` | integer |  |
| `no_blitz_grades_pass_route` | numeric |  |
| `no_pressure_accuracy_percent` | numeric |  |
| `no_blitz_twp_rate` | numeric |  |
| `no_pressure_hit_as_threw` | integer |  |
| `no_pressure_grades_pass` | numeric |  |
| `no_pressure_positive_epa_percent` | numeric |  |
| `pressure_spikes` | integer |  |
| `pressure_grades_hands_fumble` | numeric |  |
| `pressure_qb_rating` | numeric |  |
| `blitz_bats` | integer |  |
| `pressure_touchdowns` | integer |  |
| `pressure_thrown_aways` | integer |  |
| `no_blitz_interceptions` | integer |  |
| `blitz_dropbacks` | integer |  |
| `blitz_def_gen_pressures` | integer |  |
| `no_blitz_big_time_throws` | integer |  |
| `no_pressure_avg_depth_of_target` | numeric |  |
| `no_pressure_yards` | integer |  |
| `blitz_twp_rate` | numeric |  |
| `blitz_accuracy_percent` | numeric |  |
| `no_pressure_big_time_throws` | integer |  |
| `no_blitz_grades_pass_block` | numeric |  |
| `blitz_grades_run_block` | numeric |  |
| `no_pressure_grades_run_block` | numeric |  |
| `blitz_grades_pass_block` | numeric |  |
| `no_pressure_grades_pass_block` | numeric |  |
| `pressure_grades_pass_block` | character |  |
| `no_blitz_grades_run_block` | numeric |  |
| `blitz_grades_screen_block` | numeric |  |
| `pressure_grades_screen_block` | numeric |  |
| `no_blitz_grades_screen_block` | numeric |  |
| `no_pressure_grades_hands_drop` | numeric |  |
| `blitz_grades_hands_drop` | numeric |  |
| `pressure_grades_run_block` | integer |  |
| `no_pressure_grades_screen_block` | numeric |  |
| `pressure_grades_hands_drop` | numeric |  |
| `no_blitz_grades_hands_drop` | numeric |  |

**receiving**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `team_targets_percent` | numeric |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `caught_percent` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `yards_per_reception` | numeric |  |
| `touchdowns` | integer |  |
| `grades_offense` | numeric |  |
| `grades_pass_route` | numeric |  |
| `grades_hands_drop` | numeric |  |
| `grades_hands_fumble` | numeric |  |
| `pass_plays` | integer |  |
| `routes` | integer |  |
| `route_rate` | numeric |  |
| `pass_blocks` | integer |  |
| `pass_block_rate` | numeric |  |
| `slot_snaps` | integer |  |
| `slot_rate` | numeric |  |
| `wide_snaps` | integer |  |
| `wide_rate` | numeric |  |
| `inline_snaps` | integer |  |
| `inline_rate` | numeric |  |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `yards_after_catch_per_reception` | numeric |  |
| `yprr` | numeric |  |
| `avg_depth_of_target` | numeric |  |
| `longest` | integer |  |
| `drops` | integer | Throws dropped |
| `drop_rate` | numeric |  |
| `contested_targets` | integer |  |
| `contested_receptions` | integer |  |
| `contested_catch_rate` | numeric |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `fumbles` | integer |  |
| `avoided_tackles` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `targeted_qb_rating` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `receptions_oe` | numeric |  |
| `receptions_oe_total` | numeric |  |
| `lined_up_vs_cb` | numeric |  |
| `lined_up_vs_s` | numeric |  |
| `lined_up_vs_lb` | numeric |  |
| `position_target_share` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**receiving-depth**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `base_targets` | integer |  |
| `deep_targets_percent` | numeric |  |
| `deep_targets` | integer |  |
| `deep_receptions` | integer |  |
| `deep_caught_percent` | numeric |  |
| `deep_yards` | integer |  |
| `deep_yards_per_reception` | numeric |  |
| `deep_touchdowns` | integer |  |
| `deep_grades_pass_route` | numeric |  |
| `deep_grades_hands_drop` | numeric |  |
| `deep_yards_after_catch` | integer |  |
| `deep_yards_after_catch_per_reception` | numeric |  |
| `deep_yprr` | numeric |  |
| `deep_avg_depth_of_target` | numeric |  |
| `deep_drops` | integer |  |
| `deep_drop_rate` | numeric |  |
| `deep_contested_targets` | integer |  |
| `deep_contested_receptions` | integer |  |
| `deep_contested_catch_rate` | integer |  |
| `deep_interceptions` | integer |  |
| `deep_fumbles` | integer |  |
| `deep_avoided_tackles` | integer |  |
| `deep_first_downs` | integer |  |
| `deep_targeted_qb_rating` | numeric |  |
| `medium_targets_percent` | numeric |  |
| `medium_targets` | integer |  |
| `medium_receptions` | integer |  |
| `medium_caught_percent` | numeric |  |
| `medium_yards` | integer |  |
| `medium_yards_per_reception` | numeric |  |
| `medium_touchdowns` | integer |  |
| `medium_grades_pass_route` | numeric |  |
| `medium_grades_hands_drop` | numeric |  |
| `medium_yards_after_catch` | integer |  |
| `medium_yards_after_catch_per_reception` | numeric |  |
| `medium_yprr` | numeric |  |
| `medium_avg_depth_of_target` | numeric |  |
| `medium_drops` | integer |  |
| `medium_drop_rate` | numeric |  |
| `medium_contested_targets` | integer |  |
| `medium_contested_receptions` | integer |  |
| `medium_contested_catch_rate` | numeric |  |
| `medium_interceptions` | integer |  |
| `medium_fumbles` | integer |  |
| `medium_avoided_tackles` | integer |  |
| `medium_first_downs` | integer |  |
| `medium_targeted_qb_rating` | numeric |  |
| `short_targets_percent` | numeric |  |
| `short_targets` | integer |  |
| `short_receptions` | integer |  |
| `short_caught_percent` | numeric |  |
| `short_yards` | integer |  |
| `short_yards_per_reception` | numeric |  |
| `short_touchdowns` | integer |  |
| `short_grades_pass_route` | numeric |  |
| `short_grades_hands_drop` | numeric |  |
| `short_yards_after_catch` | integer |  |
| `short_yards_after_catch_per_reception` | numeric |  |
| `short_yprr` | numeric |  |
| `short_avg_depth_of_target` | numeric |  |
| `short_drops` | integer |  |
| `short_drop_rate` | numeric |  |
| `short_contested_targets` | integer |  |
| `short_contested_receptions` | integer |  |
| `short_contested_catch_rate` | numeric |  |
| `short_interceptions` | integer |  |
| `short_fumbles` | integer |  |
| `short_avoided_tackles` | integer |  |
| `short_first_downs` | integer |  |
| `short_targeted_qb_rating` | numeric |  |
| `behind_los_targets_percent` | numeric |  |
| `behind_los_targets` | integer |  |
| `behind_los_receptions` | integer |  |
| `behind_los_caught_percent` | numeric |  |
| `behind_los_yards` | integer |  |
| `behind_los_yards_per_reception` | numeric |  |
| `behind_los_touchdowns` | integer |  |
| `behind_los_grades_pass_route` | numeric |  |
| `behind_los_grades_hands_drop` | numeric |  |
| `behind_los_yards_after_catch` | integer |  |
| `behind_los_yards_after_catch_per_reception` | numeric |  |
| `behind_los_yprr` | numeric |  |
| `behind_los_avg_depth_of_target` | numeric |  |
| `behind_los_drops` | integer |  |
| `behind_los_drop_rate` | numeric |  |
| `behind_los_contested_targets` | integer |  |
| `behind_los_contested_receptions` | integer |  |
| `behind_los_contested_catch_rate` | integer |  |
| `behind_los_interceptions` | integer |  |
| `behind_los_fumbles` | integer |  |
| `behind_los_avoided_tackles` | integer |  |
| `behind_los_first_downs` | integer |  |
| `behind_los_targeted_qb_rating` | numeric |  |

**rushing**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `grades_pass_block` | numeric |  |
| `grades_offense` | numeric |  |
| `yards_after_contact` | integer |  |
| `explosive` | integer |  |
| `grades_pass_route` | numeric |  |
| `draft_season` | integer |  |
| `elu_rush_mtf` | integer |  |
| `breakaway_attempts` | integer |  |
| `designed_yards` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `yprr` | numeric |  |
| `breakaway_percent` | numeric |  |
| `fumbles` | integer |  |
| `first_downs` | integer | First downs earned by the team. |
| `elusive_rating` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `breakaway_yards` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `total_touches` | integer |  |
| `scramble_yards` | integer |  |
| `yco_attempt` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `grades_run_block` | numeric |  |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `zone_attempts` | integer |  |
| `scrambles` | integer |  |
| `grades_run` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `elu_yco` | integer |  |
| `elu_recv_mtf` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `ypa` | numeric |  |
| `drops` | integer | Throws dropped |
| `grades_hands_fumble` | numeric |  |
| `longest` | integer |  |
| `routes` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `rec_yards` | integer | Career receiving yards |
| `gap_attempts` | integer |  |
| `run_plays` | integer |  |
| `avoided_tackles` | integer |  |
| `grades_offense_penalty` | numeric |  |
| `touchdowns` | integer |  |
| `carry_share` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |
| `grades_pass` | numeric |  |

**blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_offense` | integer |  |
| `snap_counts_block` | integer |  |
| `block_percent` | numeric |  |
| `snap_counts_run_block` | integer |  |
| `snap_counts_pass_block` | integer |  |
| `pass_block_percent` | numeric |  |
| `grades_offense` | numeric |  |
| `grades_run_block` | numeric |  |
| `grades_pass_block` | numeric |  |
| `non_spike_pass_block` | integer |  |
| `non_spike_pass_block_percentage` | numeric |  |
| `sacks_allowed` | integer | Opponent sacks. |
| `hits_allowed` | integer |  |
| `hurries_allowed` | integer |  |
| `pressures_allowed` | integer |  |
| `pbe` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `declined_penalties` | integer |  |
| `snap_counts_lt` | integer |  |
| `snap_counts_lg` | integer |  |
| `snap_counts_ce` | integer |  |
| `snap_counts_rg` | integer |  |
| `snap_counts_rt` | integer |  |
| `snap_counts_te` | integer |  |

**pass-blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_lt` | integer |  |
| `snap_counts_lg` | integer |  |
| `snap_counts_ce` | integer |  |
| `snap_counts_rg` | integer |  |
| `snap_counts_rt` | integer |  |
| `snap_counts_te` | integer |  |
| `snap_counts_pass_play` | integer |  |
| `snap_counts_pass_block` | integer |  |
| `pass_block_percent` | numeric |  |
| `grades_pass_block` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `declined_penalties` | integer |  |
| `non_spike_pass_block` | integer |  |
| `non_spike_pass_block_percentage` | numeric |  |
| `sacks_allowed` | integer | Opponent sacks. |
| `hits_allowed` | integer |  |
| `hurries_allowed` | integer |  |
| `pressures_allowed` | integer |  |
| `pressure_rate_allowed` | numeric |  |
| `sack_rate_allowed` | numeric |  |
| `pbe` | numeric |  |
| `true_pass_set_snap_counts_pass_play` | integer |  |
| `true_pass_set_snap_counts_pass_block` | integer |  |
| `true_pass_set_pass_block_percent` | numeric |  |
| `true_pass_set_grades_pass_block` | numeric |  |
| `true_pass_set_non_spike_pass_block` | integer |  |
| `true_pass_set_non_spike_pass_block_percentage` | numeric |  |
| `true_pass_set_sacks_allowed` | integer |  |
| `true_pass_set_hits_allowed` | integer |  |
| `true_pass_set_hurries_allowed` | integer |  |
| `true_pass_set_pressures_allowed` | integer |  |
| `true_pass_set_pressure_rate_allowed` | numeric |  |
| `true_pass_set_sack_rate_allowed` | numeric |  |
| `true_pass_set_pbe` | numeric |  |
| `pbwr` | numeric |  |
| `true_pass_set_pbwr` | numeric |  |
| `pass_block_grade_oe_percentile` | numeric |  |
| `island_rate` | numeric |  |
| `island_pass_block_win_rate` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**run-blocking**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `team_abbreviation` | character | Team abbreviation; `team_detail = TRUE` only. |
| `games_played` | integer | Games played. |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_lt` | integer |  |
| `snap_counts_lg` | integer |  |
| `snap_counts_ce` | integer |  |
| `snap_counts_rg` | integer |  |
| `snap_counts_rt` | integer |  |
| `snap_counts_te` | integer |  |
| `snap_counts_run_play` | integer |  |
| `snap_counts_run_block` | integer |  |
| `run_block_percent` | numeric |  |
| `grades_run_block` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `declined_penalties` | integer |  |
| `zone_snap_counts_run_play` | integer |  |
| `zone_snap_counts_run_block` | integer |  |
| `zone_snap_counts_run_block_percent` | numeric |  |
| `zone_run_block_percent` | numeric |  |
| `zone_grades_run_block` | numeric |  |
| `gap_snap_counts_run_play` | integer |  |
| `gap_snap_counts_run_block` | integer |  |
| `gap_snap_counts_run_block_percent` | numeric |  |
| `gap_run_block_percent` | numeric |  |
| `gap_grades_run_block` | numeric |  |
| `pos_graded_rate` | numeric |  |
| `neg_graded_rate` | numeric |  |
| `zone_pos_graded_rate` | numeric |  |
| `zone_neg_graded_rate` | numeric |  |
| `gap_pos_graded_rate` | numeric |  |
| `gap_neg_graded_rate` | numeric |  |
| `offense_pos_graded_rate` | numeric |  |
| `offense_neg_graded_rate` | numeric |  |

**defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `interception_touchdowns` | integer |  |
| `draft_season` | integer |  |
| `forced_fumbles` | integer |  |
| `missed_tackles` | integer |  |
| `catch_rate` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | integer | Team tackles. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `snap_counts_offball` | integer |  |
| `snap_counts_box` | integer |  |
| `sacks` | integer | The Number of times sacked. |
| `snap_counts_pass_rush` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `snap_counts_dl` | integer |  |
| `grades_tackle` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `grades_coverage_defense` | numeric |  |
| `hurries` | integer |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `snap_counts_coverage` | integer |  |
| `snap_counts_dl_over_t` | integer |  |
| `snap_counts_dl_a_gap` | integer |  |
| `fumble_recoveries` | integer |  |
| `grades_run_defense` | numeric |  |
| `snap_counts_corner` | integer |  |
| `hits` | integer | Hits. |
| `penalties` | integer | Total number of penalties. |
| `batted_passes` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `stops` | integer |  |
| `declined_penalties` | integer |  |
| `total_pressures` | integer |  |
| `fumble_recovery_touchdowns` | integer |  |
| `longest` | integer |  |
| `snap_counts_slot` | integer |  |
| `missed_tackle_rate` | numeric |  |
| `grades_defense` | numeric |  |
| `yards_per_reception` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `safeties` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `snap_counts_defense` | integer |  |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `snap_counts_dl_b_gap` | integer |  |
| `pass_break_ups` | integer |  |
| `qb_rating_against` | numeric |  |
| `snap_counts_run_defense` | integer |  |
| `tackles_for_loss` | integer | Team tackles for a loss. |
| `assists` | integer | Total assists. |
| `grades_pass_rush_defense` | numeric |  |
| `snap_counts_fs` | integer |  |
| `touchdowns` | integer |  |
| `snap_counts_dl_outside_t` | integer |  |

**run-defense**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `assists` | integer | Total assists. |
| `avg_depth_of_tackle` | numeric |  |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `forced_fumbles` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_coverage_defense` | numeric |  |
| `grades_defense` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `grades_run_defense` | numeric |  |
| `grades_tackle` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackle_rate` | numeric |  |
| `missed_tackles` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `run_stop_opp` | integer |  |
| `snap_counts_run` | integer |  |
| `stop_percent` | numeric |  |
| `stops` | integer |  |
| `tackles` | integer | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `pos_graded_rate` | numeric |  |
| `neg_graded_rate` | numeric |  |

**pass-rush**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `true_pass_set_total_pressures` | integer |  |
| `draft_season` | integer |  |
| `true_pass_set_prp` | numeric |  |
| `true_pass_set_hurries` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `prp` | numeric |  |
| `true_pass_set_sacks` | integer |  |
| `pass_rush_win_rate` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `sacks` | integer | The Number of times sacked. |
| `snap_counts_pass_rush` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `true_pass_set_snap_counts_pass_play` | integer |  |
| `pass_rush_wins` | integer |  |
| `hurries` | integer |  |
| `pass_rush_opp` | integer |  |
| `snap_counts_pass_play` | integer |  |
| `hits` | integer | Hits. |
| `true_pass_set_pass_rush_win_rate` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `batted_passes` | integer |  |
| `true_pass_set_hits` | integer |  |
| `true_pass_set_snap_counts_pass_rush` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `true_pass_set_pass_rush_wins` | integer |  |
| `total_pressures` | integer |  |
| `true_pass_set_grades_pass_rush_defense` | numeric |  |
| `true_pass_set_batted_passes` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `pass_rush_percent` | numeric |  |
| `true_pass_set_pass_rush_opp` | integer |  |
| `true_pass_set_pass_rush_percent` | numeric |  |
| `grades_pass_rush_defense` | numeric |  |
| `knockdowns` | integer |  |
| `knockdown_rate` | numeric |  |
| `pressure_rate` | numeric |  |
| `sack_rate` | numeric |  |
| `true_pass_set_knockdowns` | integer |  |
| `true_pass_set_knockdown_rate` | numeric |  |
| `true_pass_set_pressure_rate` | numeric |  |
| `true_pass_set_sack_rate` | numeric |  |
| `lhs_pass_rush_snaps` | integer |  |
| `lhs_pass_rush_percent` | numeric |  |
| `lhs_sacks` | integer |  |
| `lhs_hits` | integer |  |
| `lhs_hurries` | integer |  |
| `lhs_pressures` | integer |  |
| `lhs_prp` | numeric |  |
| `lhs_stops` | integer |  |
| `lhs_tackles` | integer |  |
| `lhs_assists` | integer |  |
| `lhs_misses` | integer |  |
| `rhs_pass_rush_snaps` | integer |  |
| `rhs_pass_rush_percent` | numeric |  |
| `rhs_sacks` | integer |  |
| `rhs_hits` | integer |  |
| `rhs_hurries` | integer |  |
| `rhs_pressures` | integer |  |
| `rhs_prp` | numeric |  |
| `rhs_stops` | integer |  |
| `rhs_tackles` | integer |  |
| `rhs_assists` | integer |  |
| `rhs_misses` | integer |  |
| `pass_rush_grade_oe_percentile` | numeric |  |
| `double_team_rate` | numeric |  |

**coverage**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `targets` | integer | The number of pass plays where the player was the targeted receiver. |
| `yards_per_coverage_snap` | numeric |  |
| `draft_season` | integer |  |
| `missed_tackles` | integer |  |
| `catch_rate` | numeric |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `tackles` | integer | Team tackles. |
| `coverage_percent` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `dropped_ints` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `grades_tackle` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `receptions` | integer | The number of pass receptions. Lateral receptions officially don't count as reception. |
| `forced_incompletion_rate` | numeric |  |
| `grades_coverage_defense` | numeric |  |
| `interceptions` | integer | The number of interceptions thrown. |
| `snap_counts_coverage` | integer |  |
| `grades_run_defense` | numeric |  |
| `snap_counts_pass_play` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `forced_incompletes` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `coverage_snaps_per_target` | numeric |  |
| `stops` | integer |  |
| `declined_penalties` | integer |  |
| `longest` | integer |  |
| `missed_tackle_rate` | numeric |  |
| `grades_defense` | numeric |  |
| `yards_per_reception` | numeric |  |
| `grades_defense_penalty` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `yards_after_catch` | integer | Numeric value for distance in yards perpendicular to the yard line where the receiver made the reception to where the play ended. |
| `avg_depth_of_target` | numeric |  |
| `pass_break_ups` | integer |  |
| `qb_rating_against` | numeric |  |
| `coverage_snaps_per_reception` | numeric |  |
| `assists` | integer | Total assists. |
| `grades_pass_rush_defense` | numeric |  |
| `touchdowns` | integer |  |

**special-teams**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `assists` | integer | Total assists. |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_fgep_offense` | numeric |  |
| `grades_long_snap` | numeric |  |
| `grades_misc_st` | numeric |  |
| `grades_special_teams_penalty` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `missed_tackles` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `snap_counts_field_goal` | integer |  |
| `snap_counts_field_goal_blocking` | integer |  |
| `snap_counts_kickoff` | integer |  |
| `snap_counts_kickoff_return` | integer |  |
| `snap_counts_punt_coverage` | integer |  |
| `snap_counts_punt_return` | integer |  |
| `tackles` | integer | Team tackles. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_snaps` | integer |  |
| `grades_punter` | numeric |  |
| `grades_fgep_defense` | numeric |  |
| `grades_kick_return` | numeric |  |
| `grades_punt_return` | numeric |  |
| `grades_fgep_kicker` | numeric |  |
| `grades_kickoff_kicker` | numeric |  |

**kick-returns**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kick_return` | numeric |  |
| `grades_return` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kickoff_attempts` | integer |  |
| `kickoff_fair_catches` | integer |  |
| `kickoff_long` | integer |  |
| `kickoff_muffed_returns` | integer |  |
| `kickoff_touchdowns` | integer |  |
| `kickoff_yards` | integer |  |
| `kickoff_ypa` | numeric |  |
| `penalties` | integer | Total number of penalties. |
| `player_game_count` | integer |  |
| `punt_attempts` | integer |  |
| `punt_fair_catches` | integer |  |
| `punt_long` | integer |  |
| `punt_muffed_returns` | integer |  |
| `punt_touchdowns` | integer |  |
| `punt_yards` | integer |  |
| `punt_ypa` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_attempts` | integer |  |
| `grades_punt_return` | numeric |  |

**field-goals**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `twenty_attempts` | integer |  |
| `pat_percent` | numeric |  |
| `draft_season` | integer |  |
| `forty_made` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `fifty_percent` | integer |  |
| `total_made` | integer |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `one_made` | integer |  |
| `fifty_attempts` | integer |  |
| `forty_attempts` | integer |  |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `thirty_percent` | integer |  |
| `total_attempts` | integer |  |
| `pat_attempts` | integer |  |
| `twenty_made` | integer |  |
| `one_attempts` | integer |  |
| `grades_fgep_kicker` | numeric |  |
| `thirty_attempts` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `pat_made` | integer |  |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `declined_penalties` | integer |  |
| `one_percent` | integer |  |
| `total_percent` | numeric |  |
| `twenty_percent` | numeric |  |
| `forty_percent` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `fifty_made` | integer |  |
| `thirty_made` | integer |  |
| `field_goal_oe` | numeric |  |
| `field_goal_oe_total` | numeric |  |

**punting**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `touchbacks` | integer |  |
| `attempts_with_hangtime` | integer |  |
| `draft_season` | integer |  |
| `percent_returned` | numeric |  |
| `fair_catches` | integer |  |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `player_game_count` | integer |  |
| `eligible_season` | integer |  |
| `average_net_yards` | numeric |  |
| `yards` | integer | The number of receiving yards |
| `average_hangtime` | numeric |  |
| `total_net_yards` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `inside_twenties` | integer |  |
| `out_of_bounds` | integer | 1 if play description contains ran ob, pushed ob, or sacked ob; 0 otherwise. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `average_yards_per_return` | numeric |  |
| `total_hangtime` | numeric |  |
| `declined_penalties` | integer |  |
| `returns` | integer |  |
| `long` | integer |  |
| `blocks` | integer | Total blocks. |
| `average_yards_per_attempt` | numeric |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_punter` | numeric |  |
| `return_yards` | integer | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `downeds` | integer |  |
| `snaps` | integer |  |

**kickoffs**

| col_name | type | description |
|---|---|---|
| `player_id` | integer | Player ID (aka GSIS ID) as defined by nflreadr::load_rosters |
| `player` | character | Player name |
| `position` | character | Primary position as reported by NFL.com |
| `attempts` | integer | The number of pass attempts as defined by the NFL. |
| `attempts_with_hangtime` | integer |  |
| `average_distance` | numeric |  |
| `average_hangtime` | numeric |  |
| `average_starting_field_position` | numeric |  |
| `average_yards_per_return` | numeric |  |
| `declined_penalties` | integer |  |
| `draft_season` | integer |  |
| `eligible_season` | integer |  |
| `fair_catches` | integer |  |
| `franchise_id` | integer | ESPN franchise id (parsed from `franchise_ref`). |
| `grades_kickoff_kicker` | numeric |  |
| `jersey_number` | character | Jersey number. Often useful for joins by name/team/jersey. |
| `kicked_yards` | integer |  |
| `kicks_returned` | integer |  |
| `onside_kicks` | integer |  |
| `penalties` | integer | Total number of penalties. |
| `percent_returned` | numeric |  |
| `player_game_count` | integer |  |
| `return_yards` | integer | Yards gained by the return team. Returns may occur on any of: interception, fumble, kickoff, punt, or blocked kicks. |
| `team` | character | NFL team. Uses official abbreviations as per NFL.com |
| `team_name` | character | Team nickname; `team_detail = TRUE` only. |
| `total_hangtime` | numeric |  |
| `touchbacks` | integer |  |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example

```python
pff_api_team_report(league='nfl', report='offense', season=2022, team='cincinnati-bengals')
```

_Last validated n/a._
