---
title: UFL — additional Python functions
sidebar_label: Additional functions
description: "UFL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# UFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.ufl`
not covered by the generated API-endpoint reference above.

## ESPN

### ufl_pbp {#ufl_pbp}

`ufl_pbp(game_id: 'Union[str, int]', *, return_as_pandas: 'bool' = False) -> "Union[pl.DataFrame, 'pd.DataFrame']"`

Enriched UFL play-by-play (EP/EPA/WP/WPA/CP/CPOE).

Same shared spring-football core as `~sportsdataverse.football.xfl.xfl_pbp`
(see `sportsdataverse.football.spring_football_ep_wp`).

**Capture finding:** ESPN publishes no play-by-play for UFL games as of
this port -- verified empty (`summary.drives` AND the Core v2
`.../plays` endpoint) across every completed 2024 + 2025 UFL game. This
function returns a zero-row (contract-shaped) frame on today's real data
-- not a stub -- and will pick up real rows automatically once ESPN
backfills UFL play-by-play. See
`tests/fixtures/league_ports/FEASIBILITY.md`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `Union[str, int]` |  | ESPN UFL event id. |
| `return_as_pandas` | `bool` | `False` | When `True`, return a `pandas.DataFrame`. |

**Returns**

One row per play with `ep`/`epa`/`wp`/`wpa`/`cp`/`cpoe` and the other `enrich_nfl_pbp` output columns. Zero rows today for every UFL game (see capture finding above).

| col_name | type | description |
|---|---|---|
| `game_id` | character |  |
| `play_id` | character |  |
| `season` | integer |  |
| `game_half` | character |  |
| `posteam` | character |  |
| `defteam` | character |  |
| `home_team` | character |  |
| `half_seconds_remaining` | double |  |
| `yardline_100` | integer |  |
| `ydstogo` | integer |  |
| `down` | integer |  |
| `posteam_timeouts_remaining` | integer |  |
| `defteam_timeouts_remaining` | integer |  |
| `home` | integer |  |
| `retractable` | integer |  |
| `dome` | integer |  |
| `outdoors` | integer |  |
| `score_differential` | integer |  |
| `game_seconds_remaining` | double |  |
| `spread_line` | double |  |
| `receive_2h_ko` | integer |  |
| `posteam_score` | integer |  |
| `defteam_score` | integer |  |
| `roof` | character |  |

**Example**

```python
from sportsdataverse.football.ufl import ufl_pbp

df = ufl_pbp("401638299")
print(df.height)  # 0 today -- see the capture-finding note above
```
