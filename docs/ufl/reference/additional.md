# UFL — additional Python functions

> UFL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package.

Hand-written wrappers, loaders, and helpers in `sportsdataverse.football.ufl` that are not
generated API-endpoint wrappers (those have their own reference pages).

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
| `game_id` | String |  |
| `play_id` | String |  |
| `season` | Int64 |  |
| `game_half` | String |  |
| `posteam` | String |  |
| `defteam` | String |  |
| `home_team` | String |  |
| `half_seconds_remaining` | Float64 |  |
| `yardline_100` | Int64 |  |
| `ydstogo` | Int64 |  |
| `down` | Int64 |  |
| `posteam_timeouts_remaining` | Int64 |  |
| `defteam_timeouts_remaining` | Int64 |  |
| `home` | Int64 |  |
| `retractable` | Int64 |  |
| `dome` | Int64 |  |
| `outdoors` | Int64 |  |
| `score_differential` | Int64 |  |
| `game_seconds_remaining` | Float64 |  |
| `spread_line` | Float64 |  |
| `receive_2h_ko` | Int64 |  |
| `posteam_score` | Int64 |  |
| `defteam_score` | Int64 |  |
| `roof` | String |  |

**Example**

```python
from sportsdataverse.football.ufl import ufl_pbp

df = ufl_pbp("401638299")
print(df.height)  # 0 today -- see the capture-finding note above
```
