---
title: DBL — additional Python functions
sidebar_label: Additional functions
description: "DBL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# DBL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.dbl`
not covered by the generated API-endpoint reference above.

## Basketligaen (Denmark, Sportality)

### sportality_player_crosswalk {#sportality_player_crosswalk}

`sportality_player_crosswalk(season: 'int', game_type: 'str' = 'regular', *, return_as_pandas: 'bool' = False) -> 'Frame'`

Genius person id -> athlete uuid for every player of a Basketligaen season, from one stats-info call.

The site has two player id systems: the **Genius person id** that play-by-play, box scores and
stats tables key on (and that joins the `genius` family), and the platform **athlete uuid**
that rosters and athlete routes key on. The `players_summary` table's `players{}` map is
keyed by Genius id and carries each player's uuid.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | season END year (2026 = 2025/26). |
| `game_type` | `str` | `'regular'` | `regular` or `finals` (the playoffs). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per player: `player_id` (Genius, `Int64`), `athlete_uuid` (null where the site has none, common before about 2005), names, nationality, gender, position, jersey number, height and weight. Birth dates are dropped.

**Example**

```python
from sportsdataverse.basketball.dbl import sportality_player_crosswalk
xw = sportality_player_crosswalk(2026)
xw.filter(xw["player_id"] == 1887576)
```

### sportality_ssgt_uuid {#sportality_ssgt_uuid}

`sportality_ssgt_uuid(season: 'int', game_type: 'str' = 'regular') -> 'str'`

The Basketligaen ssgt uuid of a season (END year) and game type, from `/statistics-v2/layout-info`.

Use it for the routes that take an ssgt uuid directly (`sportality_standings`,
`sportality_sites`, `sportality_ssgt_provider`, `sportality_post_game_team_stats`,
`sportality_player_h2h`). The directory is fetched once per process.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | season END year (2026 = 2025/26). |
| `game_type` | `str` | `'regular'` | `regular` or `finals` (the playoffs). |

**Returns**

the ssgt uuid (e.g. `"qzw34da6jg"` for the 2025/26 regular season).

**Example**

```python
from sportsdataverse.basketball.dbl import sportality_ssgt_uuid
sportality_ssgt_uuid(2026)              # 2025/26 regular season
sportality_ssgt_uuid(2026, "finals")    # 2025/26 playoffs
```
