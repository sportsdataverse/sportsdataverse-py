---
title: "NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Player"
sidebar_label: "Player"
sidebar_position: 10
description: "NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Player — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Player

## pff_player_passing_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /passing/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/passing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/passing/summary](https://premium.pff.com/api/v1/player/passing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_passing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_passing_summary-example}

```python
pff_player_passing_summary()
```

_Last validated n/a._

## pff_player_rushing_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/rushing/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/rushing/summary](https://premium.pff.com/api/v1/player/rushing/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_rushing_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_rushing_summary-example}

```python
pff_player_rushing_summary()
```

_Last validated n/a._

## pff_player_receiving_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/receiving/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/receiving/summary](https://premium.pff.com/api/v1/player/receiving/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_receiving_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_receiving_summary-example}

```python
pff_player_receiving_summary()
```

_Last validated n/a._

## pff_player_defense_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /defense/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/defense/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/defense/summary](https://premium.pff.com/api/v1/player/defense/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_defense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_defense_summary-example}

```python
pff_player_defense_summary()
```

_Last validated n/a._

## pff_player_offense_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/offense/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/offense/summary](https://premium.pff.com/api/v1/player/offense/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_offense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_offense_summary-example}

```python
pff_player_offense_summary()
```

_Last validated n/a._

## pff_player_snaps_summary

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/snaps/summary`

**Valid URL:** [https://premium.pff.com/api/v1/player/snaps/summary](https://premium.pff.com/api/v1/player/snaps/summary)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_snaps_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_snaps_summary-example}

```python
pff_player_snaps_summary()
```

_Last validated n/a._

## pff_player_offense_blocking

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/blocking (per-week + totals for one player)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/offense/blocking`

**Valid URL:** [https://premium.pff.com/api/v1/player/offense/blocking](https://premium.pff.com/api/v1/player/offense/blocking)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `career` |  |  | `Y` | Career-rollup flag ("true"/"false"); player-detail views only. |

### Returns {#pff_player_offense_blocking-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_offense_blocking-example}

```python
pff_player_offense_blocking()
```

_Last validated n/a._

## pff_players

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/players`

**Valid URL:** [https://premium.pff.com/api/v1/players](https://premium.pff.com/api/v1/players)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `name` |  |  | `Y` | Player-name search prefix. |
| `id` | `id` |  |  | `Y` | Entity id (player lookup). |

### Returns {#pff_players-returns}

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

**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_players-example}

```python
pff_players()
```

_Last validated n/a._

## pff_player_seasons

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/seasons`

**Valid URL:** [https://premium.pff.com/api/v1/player/seasons](https://premium.pff.com/api/v1/player/seasons)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |

### Returns {#pff_player_seasons-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_seasons-example}

```python
pff_player_seasons()
```

_Last validated n/a._

## pff_player_position_pivot

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

**Endpoint URL:** `GET https://premium.pff.com/api/v1/player/position/pivot`

**Valid URL:** [https://premium.pff.com/api/v1/player/position/pivot](https://premium.pff.com/api/v1/player/position/pivot)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `league` | `league` |  |  | `Y` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `season` |  |  | `Y` | Season (starting year). |
| `week` | `week` |  |  | `Y` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `player_id` |  |  | `Y` | PFF player id (snake_case on the wire; matches the /players id). |

### Returns {#pff_player_position_pivot-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_report`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example {#pff_player_position_pivot-example}

```python
pff_player_position_pivot()
```

_Last validated n/a._
