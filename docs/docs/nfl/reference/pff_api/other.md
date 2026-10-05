---
title: "NFL — PFF Developer API (api.pff.com, API key) — Other: ref_leagues–whoami"
sidebar_label: "Other: ref_leagues–whoami"
sidebar_position: 17
description: "NFL — PFF Developer API (api.pff.com, API key) — Other: ref_leagues–whoami — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Other: ref_leagues–whoami

## pff_api_ref_leagues

List the leagues you can read, with their seasons and weeks

**Endpoint URL:** `GET https://api.pff.com/v1/leagues`

**Valid URL:** [https://api.pff.com/v1/leagues](https://api.pff.com/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#pff_api_ref_leagues-returns}

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

### Example {#pff_api_ref_leagues-example}

```python
pff_api_ref_leagues()
```

_Last validated n/a._

## pff_api_ref_games

List game results for a league, season and week

**Endpoint URL:** `GET https://api.pff.com/v1/games`

**Valid URL:** [https://api.pff.com/v1/games?league=nfl&season=2022&week=1](https://api.pff.com/v1/games?league=nfl&season=2022&week=1)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `season` | `season` |  | `Y` |  | Season, as a four-digit year, taken as a positional argument. |
| `week` | `week` |  | `Y` |  | Week, and the THIRD positional argument of games — the one command that takes a single week number rather than a comma-separated list. |
| `franchise_id` | `franchise_id` |  |  | `Y` | Franchise (team) id. |

### Returns {#pff_api_ref_games-returns}

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

### Example {#pff_api_ref_games-example}

```python
pff_api_ref_games(league='nfl', season=2022, week=1)
```

_Last validated n/a._

## pff_api_ref_players

Search the player directory by name or id

**Endpoint URL:** `GET https://api.pff.com/v1/players`

**Valid URL:** [https://api.pff.com/v1/players?league=nfl&name=burrow](https://api.pff.com/v1/players?league=nfl&name=burrow)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  | `Y` |  | League slug, and the FIRST positional argument of every command that takes one — every command except the facet leaderboards, where --league stays a flag because --game alone is a complete request. |
| `id` | `id` |  |  | `Y` | Exact player-id lookup, ref-players only. |
| `name` | `name` |  |  | `Y` | Free-text player-name search, ref-players only. |

### Returns {#pff_api_ref_players-returns}

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

### Example {#pff_api_ref_players-example}

```python
pff_api_ref_players(league='nfl', name='burrow')
```

_Last validated n/a._

## pff_api_whoami

Show what this API believes about the current credential

**Endpoint URL:** `GET https://api.pff.com/v1/auth/whoami`

**Valid URL:** [https://api.pff.com/v1/auth/whoami](https://api.pff.com/v1/auth/whoami)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#pff_api_whoami-returns}

Show what this API believes about the current credential

### Example {#pff_api_whoami-example}

```python
pff_api_whoami()
```

_Last validated n/a._
