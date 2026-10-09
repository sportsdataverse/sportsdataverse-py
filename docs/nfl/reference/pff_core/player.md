# NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Player

> NFL — PFF Premium Stats -- LEGACY (premium.pff.com, cookie auth; use the PFF Developer API) — Player — function reference in sdv-py, the SportsDataverse Python package.

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
| `college` | character | College the player attended, as the source lists it. |
| `current_class` | character | Player's current college class designation (e.g., Freshman, Senior), per PFF. |
| `current_eligible_year` | numeric | Year the player is or was first draft-eligible, per PFF. |
| `dob` | character | Player's date of birth (YYYY-MM-DD). |
| `draft` | list | Nested draft-selection details for the player (year, round, pick, and franchise) as returned by the source API. |
| `first_name` | character | Player's first name as the source lists it. |
| `height` | numeric | Player's height as the source encodes it (PFF uses feet and inches without a separator, 602 = 6'02"; others use inches or centimetres). |
| `id` | numeric | PFF numeric player id, the key of the player-scoped endpoints. |
| `jersey_number` | character | Jersey number as a string. |
| `last_name` | character | Player's last name as the source lists it. |
| `position` | character | Position abbreviation as the source lists it (e.g. QB, WR). |
| `speed` | numeric | 40-yard-dash time in seconds as recorded by PFF. |
| `team` | list | Current team of the player as the source ships it (a nested team object, stringified, or a team code). |
| `weight` | numeric | Player's weight as the source lists it (pounds for US sources). |

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
