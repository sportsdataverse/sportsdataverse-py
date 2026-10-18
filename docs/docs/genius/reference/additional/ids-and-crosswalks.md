---
title: "GENIUS — additional Python functions — IDs and crosswalks"
sidebar_label: "IDs and crosswalks"
sidebar_position: 6
description: "GENIUS — additional Python functions — IDs and crosswalks — function reference in sdv-py, the SportsDataverse Python package."
---
# GENIUS — additional Python functions — IDs and crosswalks

### genius_game_crosswalk {#genius_game_crosswalk}

`genius_game_crosswalk(df: 'pl.DataFrame', source: 'str', *, return_as_pandas: 'bool' = False) -> 'Any'`

Provider game key <-> Genius LiveStats match id, from a frame that carries both.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `polars.DataFrame` |  | a frame from the wrapper named in `GENIUS_GAME_SOURCES` for `source`. |
| `source` | `str` |  | `sportpress` (BNXT `sportpress_game_slider`: `game_id` / `genius_id`), `cebl` (`game_id` / `genius_match_id`), `tbf` (`match_id` / `genius_id`), `kzs` (`match_id` / `genius_match_id`) or `sportality` (`game_uuid` / `game_id`, the Genius match id). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per game with `game_key` (the provider's key: `Int64`, or the platform game uuid `Utf8` for `sportality`) and `genius_match_id` (`Int64`). Rows without a Genius id (older seasons, upcoming games) are dropped.

| col_name | type | description |
|---|---|---|
| `game_key` | integer / character | Provider's own game key: the BNXT, CEBL, TBF or KZS integer game id, or the Sportality platform game uuid (a string) for source sportality. |
| `genius_match_id` | integer | Genius Sports LiveStats match id of the same game (the match_id every genius_* wrapper takes). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | unknown `source`, a missing column, or a key that maps to two ids. |
| `TypeError` | `df` is not polars, or an id column has the wrong dtype. |

**Example**

```python
from sportsdataverse.basketball.basketball_crosswalk import genius_game_crosswalk
from sportsdataverse.basketball.cebl import cebl_games
from sportsdataverse.basketball.genius import genius_pbp

xw = genius_game_crosswalk(cebl_games(season=2025), "cebl")
pbp = genius_pbp(match_id=int(xw["genius_match_id"][0]))
```

### genius_team_crosswalk {#genius_team_crosswalk}

`genius_team_crosswalk(df: 'pl.DataFrame', source: 'str', *, return_as_pandas: 'bool' = False) -> 'Any'`

Sportality platform team uuid <-> Genius team id, from a Sportality frame that carries both.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df` | `polars.DataFrame` |  | a Sportality frame. |
| `source` | `str` |  | `sportality_standings` (`sportality_standings`: `team_uuid` / `team_id`), `sportality_teams` (`sportality_stats_info` `teams_summary`: `info_uuid` / `info_team_id`) or `sportality_players` (`players_summary`: `info_team_uuid` / `info_team_id`). |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: `team_uuid` (`Utf8`) and `genius_team_id` (`Int64`), one row per team. The Genius ids are the ones the `genius` schedule frames call `home_team_id`.

| col_name | type | description |
|---|---|---|
| `team_uuid` | character | Sportality platform team uuid. |
| `genius_team_id` | integer | Genius team id of the same team (home_team_id / away_team_id of the genius schedule frames). |

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | unknown `source`, a missing column, or a key that maps to two ids. |
| `TypeError` | `df` is not polars, or an id column has the wrong dtype. |

**Example**

```python
from sportsdataverse.basketball.basketball_crosswalk import genius_team_crosswalk
from sportsdataverse.basketball.dbl import sportality_standings

standings = sportality_standings(ssgt_uuid="xs07bd3fmp")
xw = genius_team_crosswalk(standings, "sportality_standings")
```
