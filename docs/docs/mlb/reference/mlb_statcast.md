---
title: MLB — MLB Statcast (Baseball Savant)
sidebar_label: MLB Statcast (Baseball Savant)
sidebar_position: 11
---
# MLB — MLB Statcast (Baseball Savant)

`sportsdataverse.mlb` — 3 endpoints.

## `mlb_statcast_leaderboard_expected_stats`

GET /leaderboard/expected_statistics — xBA/xSLG/xwOBA leaderboard (csv=true).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/expected_statistics`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/expected_statistics](https://baseballsavant.mlb.com/leaderboard/expected_statistics)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_statcast_leaderboard`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_statcast_leaderboard_expected_stats()
```

_Last validated n/a._

## `mlb_statcast_gamefeed`

GET /gf — Savant per-game JSON feed.

**Endpoint URL:** `GET https://baseballsavant.mlb.com/gf`

**Valid URL:** [https://baseballsavant.mlb.com/gf](https://baseballsavant.mlb.com/gf)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `game_pk` | `game_pk` |  |  | `Y` | game_pk query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_statcast_gamefeed`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_statcast_gamefeed()
```

_Last validated n/a._

## `mlb_statcast_player_percentile_rankings`

GET /leaderboard/percentile-rankings — player percentile sliders (csv=true).

**Endpoint URL:** `GET https://baseballsavant.mlb.com/leaderboard/percentile-rankings`

**Valid URL:** [https://baseballsavant.mlb.com/leaderboard/percentile-rankings](https://baseballsavant.mlb.com/leaderboard/percentile-rankings)

| API Parameter | Python | Pattern | Required | Nullable | Description |
|---|---|:---:|:---:|:---:|---|
| `type` | `type` |  |  | `Y` | type query parameter. |
| `year` | `year` |  |  | `Y` | year query parameter. |
| `csv` | `csv` |  |  | `Y` | csv query parameter. |

### Returns

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_mlb_statcast_leaderboard`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — the raw JSON `Dict` payload, unparsed.

### Example

```python
mlb_statcast_player_percentile_rankings()
```

_Last validated n/a._
