# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Other

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Other — function reference in sdv-py, the SportsDataverse Python package.

## pff_leagues

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Leagues + seasons + week groups (bootstrap)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/leagues`

**Valid URL:** [https://premium.pff.com/api/v1/leagues](https://premium.pff.com/api/v1/leagues)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#pff_leagues-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `abbreviation` | character | League abbreviation as the source lists it (e.g. NFL). |
| `default_season` | numeric | Season the source API currently treats as the default for this league. |
| `default_week` | numeric | Week number the source API currently treats as the default for this league. |
| `default_week_group` | character | Identifier of the week grouping (e.g., regular season or postseason phase) currently set as the league default. |
| `id` | numeric | Numeric league id from the source API (PFF: 1 = NFL). |
| `name` | character | League display name as the source lists it (PFF: 'Pro Football'). |
| `seasons` | list | Nested list (stringified) of the seasons the source publishes for the league. |
| `slug` | character | URL slug of the league on premium.pff.com (e.g. nfl). |
| `week_groups` | list | Nested list of week-group objects (phase label and week span) defined for the league. |
| `weeks` | list | Nested list of week objects available for the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_leagues-example}

```python
pff_leagues()
```

_Last validated n/a._

## pff_teams

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

**Endpoint URL:** `GET https://premium.pff.com/api/v1/teams`

**Valid URL:** [https://premium.pff.com/api/v1/teams](https://premium.pff.com/api/v1/teams)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |

### Returns {#pff_teams-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `heirarchy` | list | Nested hierarchy of franchise groupings as returned by the PFF API; the field name's spelling follows the source. |
| `id` | numeric | PFF id of the conference, division or tier group (e.g. 1 = AFC, 2 = AFC East). |
| `name` | character | Group name (e.g. "AFC", "AFC East"; NCAA groups such as "FBS" or "The American"). |
| `slug` | character | URL-style slug of the group (e.g. "afc-east"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_teams-example}

```python
pff_teams()
```

_Last validated n/a._

## pff_teams_overview

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/teams/overview`

**Valid URL:** [https://premium.pff.com/api/v1/teams/overview](https://premium.pff.com/api/v1/teams/overview)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |

### Returns {#pff_teams_overview-returns}

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
| `losses` | numeric | Games the team lost over the covered span. |
| `name` | character | Full team name (e.g. "Arizona Cardinals"). |
| `points_allowed` | numeric | Total points allowed by the team over the covered span. |
| `points_scored` | numeric | Total points scored by the team over the covered span. |
| `ties` | numeric | Games the team tied over the covered span. |
| `wins` | numeric | Games the team won over the covered span. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_teams_overview-example}

```python
pff_teams_overview()
```

_Last validated n/a._

## pff_games

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/games`

**Valid URL:** [https://premium.pff.com/api/v1/games](https://premium.pff.com/api/v1/games)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Single week number. |

### Returns {#pff_games-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` with the columns below; pass `return_as_pandas=True` for a `pandas.DataFrame`.

| col_name | type | description |
|---|---|---|
| `away_franchise_id` | numeric | PFF franchise id of the away team. |
| `away_team` | list | Away team object (JSON-stringified in the tidy frame). |
| `has_stats` | logical | Whether PFF has published stats for the game. |
| `home_franchise_id` | numeric | PFF franchise id of the home team. |
| `home_team` | list | Home team object (JSON-stringified in the tidy frame). |
| `id` | numeric | PFF game id (integer join key). |
| `league` | list | Nested league object of the game (stringified) with the PFF league id, name and abbreviation (e.g. NFL). |
| `league_id` | numeric | PFF league id (integer). |
| `lock_status` | character | Data lock/publish status for the game. |
| `score` | list | Nested final-score object of the game (stringified) with away_team and home_team points. |
| `season` | numeric | Season (starting year) of the game. |
| `stadium_id` | numeric | PFF stadium identifier for the game venue. |
| `start` | character | Kickoff timestamp (ISO 8601 string). |
| `week` | numeric | Week number of the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_games-example}

```python
pff_games()
```

_Last validated n/a._
