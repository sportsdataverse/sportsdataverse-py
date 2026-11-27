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
| `abbreviation` | String | League abbreviation as the source lists it (e.g. NFL). |
| `default_season` | Float64 | Season the source API currently treats as the default for this league. |
| `default_week` | Float64 | Week number the source API currently treats as the default for this league. |
| `default_week_group` | String | Identifier of the week grouping (e.g., regular season or postseason phase) currently set as the league default. |
| `id` | Float64 | Numeric league id from the source API (PFF: 1 = NFL). |
| `name` | String | League display name as the source lists it (PFF: 'Pro Football'). |
| `seasons` | List | Nested list (stringified) of the seasons the source publishes for the league. |
| `slug` | String | URL slug of the league on premium.pff.com (e.g. nfl). |
| `week_groups` | List | Nested list of week-group objects (phase label and week span) defined for the league. |
| `weeks` | List | Nested list of week objects available for the league. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_leagues-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_leagues-example}

```python
pff_leagues()
```

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
| `heirarchy` | List | Nested hierarchy of franchise groupings as returned by the PFF API; the field name's spelling follows the source. |
| `id` | Float64 | PFF id of the conference, division or tier group (e.g. 1 = AFC, 2 = AFC East). |
| `name` | String | Group name (e.g. "AFC", "AFC East"; NCAA groups such as "FBS" or "The American"). |
| `slug` | String | URL-style slug of the group (e.g. "afc-east"). |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_teams-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_teams-example}

```python
pff_teams()
```

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
| `abbreviation` | String | Team abbreviation. |
| `franchise_id` | Float64 | PFF franchise (team) id (integer join key). |
| `grades_coverage_defense` | Float64 | PFF team coverage grade (0-100). |
| `grades_defense` | Float64 | PFF team defense grade (0-100). |
| `grades_misc_st` | Float64 | Team-level PFF miscellaneous special-teams grade, 0-100. |
| `grades_offense` | Float64 | PFF team offense grade (0-100). |
| `grades_overall` | Float64 | Team-level PFF overall grade, 0-100. |
| `grades_pass` | Float64 | Team-level PFF passing grade, 0-100. |
| `grades_pass_block` | Float64 | Team-level PFF pass-blocking grade, 0-100. |
| `grades_pass_route` | Float64 | Team-level PFF receiving (route-running) grade, 0-100. |
| `grades_pass_rush_defense` | Float64 | Team-level PFF pass-rush grade, 0-100. |
| `grades_run` | Float64 | Team-level PFF rushing grade, 0-100. |
| `grades_run_block` | Float64 | Team-level PFF run-blocking grade, 0-100. |
| `grades_run_defense` | Float64 | Team-level PFF run-defense grade, 0-100. |
| `grades_tackle` | Float64 | Team-level PFF tackling grade, 0-100. |
| `losses` | Float64 | Games the team lost over the covered span. |
| `name` | String | Full team name (e.g. "Arizona Cardinals"). |
| `points_allowed` | Float64 | Total points allowed by the team over the covered span. |
| `points_scored` | Float64 | Total points scored by the team over the covered span. |
| `ties` | Float64 | Games the team tied over the covered span. |
| `wins` | Float64 | Games the team won over the covered span. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_teams_overview-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_teams_overview-example}

```python
pff_teams_overview()
```

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
| `away_franchise_id` | Float64 | PFF franchise id of the away team. |
| `away_team` | List | Away team object (JSON-stringified in the tidy frame). |
| `has_stats` | Boolean | Whether PFF has published stats for the game. |
| `home_franchise_id` | Float64 | PFF franchise id of the home team. |
| `home_team` | List | Home team object (JSON-stringified in the tidy frame). |
| `id` | Float64 | PFF game id (integer join key). |
| `league` | List | Nested league object of the game (stringified) with the PFF league id, name and abbreviation (e.g. NFL). |
| `league_id` | Float64 | PFF league id (integer). |
| `lock_status` | String | Data lock/publish status for the game. |
| `score` | List | Nested final-score object of the game (stringified) with away_team and home_team points. |
| `season` | Float64 | Season (starting year) of the game. |
| `stadium_id` | Float64 | PFF stadium identifier for the game venue. |
| `start` | String | Kickoff timestamp (ISO 8601 string). |
| `week` | Float64 | Week number of the game. |

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Raises {#pff_games-raises}

| Exception | Description |
|---|---|
| `NoDataError` | The host answered 404 -- the requested resource does not exist. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. |

### Example {#pff_games-example}

```python
pff_games()
```
