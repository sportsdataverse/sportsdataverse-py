---
title: "NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special"
sidebar_label: "Player: receiving–special"
sidebar_position: 12
description: "NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special — function reference in sdv-py, the SportsDataverse Python package."
toc_max_heading_level: 2
---
# NFL — PFF Developer API (api.pff.com, API key) — Player: receiving–special

## pff_api_player_receiving_summary

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

### Returns {#pff_api_player_receiving_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_receiving_summary-example}

```python
pff_api_player_receiving_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_defense_summary

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

### Returns {#pff_api_player_defense_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_defense_summary-example}

```python
pff_api_player_defense_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_field_goal_summary

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

### Returns {#pff_api_player_field_goal_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_field_goal_summary-example}

```python
pff_api_player_field_goal_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_kickoff_summary

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

### Returns {#pff_api_player_kickoff_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_kickoff_summary-example}

```python
pff_api_player_kickoff_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_punting_summary

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

### Returns {#pff_api_player_punting_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_punting_summary-example}

```python
pff_api_player_punting_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_return_summary

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

### Returns {#pff_api_player_return_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_return_summary-example}

```python
pff_api_player_return_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._

## pff_api_player_special_summary

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

### Returns {#pff_api_player_special_summary-returns}

**`return_parsed=True`** (default) — a tidy `polars.DataFrame` (parser: `parse_pff_player_detail`); pass `return_as_pandas=True` for a `pandas.DataFrame`.
**`return_parsed=False`** — The decoded JSON body. /v1 routes return the Premium Stats envelope (`{report_slug: rows}`); /v2 routes return `{..meta.., columns, rows}`..

### Example {#pff_api_player_special_summary-example}

```python
pff_api_player_special_summary(league='nfl', player_id=28022, season=2022)
```

_Last validated n/a._
