---
title: KHL — additional Python functions
sidebar_label: Additional functions
description: "KHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# KHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.khl`
not covered by the generated API-endpoint reference above.

## KHL (site REST + webcaster API)

### khl_club_crosswalk {#khl_club_crosswalk}

`khl_club_crosswalk(season: 'Optional[int]' = None, stage: 'Optional[str]' = 'regular', *, tournament_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Frame'`

Club id <-> team-season id for one tournament, read from a covering set of game headers.

Fetches the schedule, picks the fewest games (greedy) that touch every club, fetches those game
headers (about 15 requests, each waiting its polite second) and maps the ids they carry.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | season END year (2026 = 2025-26); None = the site's current season. |
| `stage` | `Optional[str]` | `'regular'` | `regular` or `playoff` (team-season ids differ per tournament). |
| `tournament_id` | `Optional[int]` | `None` | khl.ru tournament id (tnId), instead of season / stage. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: `tournament_id`, `club_id` (stable), `team_season_id` (this tournament's), one row per club.

**Example**

```python
from sportsdataverse.hockey.khl import khl_club_crosswalk
xw = khl_club_crosswalk(season=2026)
xw.filter(xw["club_id"] == 7)
```

### khl_club_crosswalk_from_frames {#khl_club_crosswalk_from_frames}

`khl_club_crosswalk_from_frames(*frames: 'Any', return_as_pandas: 'bool' = False) -> 'Frame'`

Club id <-> team-season id from frames that carry both (game header / info, player career).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: one row per (`tournament_id`, `club_id`, `team_season_id`), all `Int64`, sorted; zero rows when no frame carries a pair.

**Example**

```python
from sportsdataverse.hockey.khl import khl_club_crosswalk_from_frames, khl_game_header
khl_club_crosswalk_from_frames(khl_game_header(898236, season=2026))
```
