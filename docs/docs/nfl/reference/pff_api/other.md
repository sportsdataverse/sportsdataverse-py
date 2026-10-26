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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_ref_leagues-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_ref_leagues-example}

```python
pff_api_ref_leagues()
```

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

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_ref_games-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_ref_games-example}

```python
pff_api_ref_games(league='nfl', season=2022, week=1)
```

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
| `college` | String | College the player attended, as the source lists it. |
| `current_class` | String | Player's current college class designation (e.g., Freshman, Senior), per PFF. |
| `current_eligible_year` | Float64 | Year the player is or was first draft-eligible, per PFF. |
| `dob` | String | Player's date of birth (YYYY-MM-DD). |
| `draft` | List | Nested draft-selection details for the player (year, round, pick, and franchise) as returned by the source API. |
| `first_name` | String | Player's first name as the source lists it. |
| `height` | Float64 | Player's height as the source encodes it (PFF uses feet and inches without a separator, 602 = 6'02"; others use inches or centimetres). |
| `id` | Float64 | PFF numeric player id, the key of the player-scoped endpoints. |
| `jersey_number` | String | Jersey number as a string. |
| `last_name` | String | Player's last name as the source lists it. |
| `position` | String | Position abbreviation as the source lists it (e.g. QB, WR). |
| `speed` | Float64 | 40-yard-dash time in seconds as recorded by PFF. |
| `team` | List | Current team of the player as the source ships it (a nested team object, stringified, or a team code). |
| `weight` | Float64 | Player's weight as the source lists it (pounds for US sources). |

**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Raises {#pff_api_ref_players-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_ref_players-example}

```python
pff_api_ref_players(league='nfl', name='burrow')
```

## pff_api_whoami

Show what this API believes about the current credential

**Endpoint URL:** `GET https://api.pff.com/v1/auth/whoami`

**Valid URL:** [https://api.pff.com/v1/auth/whoami](https://api.pff.com/v1/auth/whoami)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|

### Returns {#pff_api_whoami-returns}

Show what this API believes about the current credential

### Raises {#pff_api_whoami-raises}

| Exception | Description |
|---|---|
| `RuntimeError` | No PFF API key (pass `api_key=` or set `PFF_API_KEY`). |
| `NoDataError` | The host answered 404 -- the requested resource does not exist. PFF answered 404 (no such route, report or team). |
| `AssetFetchError` | The fetch failed (a non-2xx answer or a connection failure after retries, e.g. 401/403/429/5xx, or an empty or unreadable 200 body) -- the answer is unknown, not empty. The request was refused or failed (401/403/429/5xx) -- the answer is unknown, never an empty frame. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. PFF rejected the parameters (400 `invalid_parameter` / 422). |

### Example {#pff_api_whoami-example}

```python
pff_api_whoami()
```
