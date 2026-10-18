---
title: "ATRIUM — additional Python functions — Sportradar Atrium embed API: nbl1_teams"
sidebar_label: "Sportradar Atrium embed API: nbl1_teams"
sidebar_position: 4
description: "ATRIUM — additional Python functions — Sportradar Atrium embed API: nbl1_teams — function reference in sdv-py, the SportsDataverse Python package."
---
# ATRIUM — additional Python functions — Sportradar Atrium embed API: nbl1_teams

### nbl1_teams {#nbl1_teams}

`nbl1_teams(season: 'Optional[int]' = None, season_id: 'Optional[str]' = None, competition_id: 'Optional[str]' = None, locale: 'Optional[str]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

Teams of a NBL1 (Australia) season.

NBL1 (Australia) (men's and women's); season rule: calendar year, Mar-Aug (END year); conferences are competition_ids.

Convenience wrapper: a thin binding of `atrium_teams` to this competition, minted from the
competition registry (`tools/codegen/basketball_competitions.yaml`).

Read before use: Unofficial: this is the public endpoint the Sportradar Atrium / Synergy `Connect` widget calls from every visitor's browser (keyless; the host has no robots.txt). The data is Sportradar's and the leagues'. The wrappers read it on the caller's behalf, one request at least 1 s after the last, and never republish it as release assets. Keep crawls small and incremental. Women's competition data: flagged for the wehoop mirror (Phase C).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season END year (see the season rule above). Omit season and season_id for the site's current season. |
| `season_id` | `Optional[str]` | `None` | Season UUID (season_id in the seasons listing); skips the END-year lookup. |
| `competition_id` | `Optional[str]` | `None` | Competition UUID (a site carries many: conferences, cups); needed when several share the season. |
| `locale` | `Optional[str]` | `None` | Label locale, e.g. en-EN; omitted = the site's own language. |
| `return_parsed` | `bool` | `True` | parse into a polars DataFrame (default True); False returns the raw body. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

| col_name | type | description |
|---|---|---|
| `website_id` | integer | Site config id the body came from (12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 236 Austria, 326 Montenegro), read from the body's links.self. |
| `season_id` | character | Season UUID of the team list. |
| `entity_id` | character | Team UUID (entity): one id per club across seasons, shared by every site that shows the team. |
| `name` | character | Team name in the site's language. |
| `image_url` | character | URL of the Sportradar-hosted image (400 px). |

**Raises**

| Exception | Description |
|---|---|
| `sportsdataverse.errors.NoDataError` | the site has nothing for the request (no such season, an empty fixture list, a game with no play-by-play yet, an unknown id answered 404). |
| `AssetFetchError` | also raised for an error body, a stub, or a body that belongs to another site, type, season, game, player or team than the one asked for. |
| `ValueError` | The host answered 400 / 422 -- the request is wrong; retrying cannot help. |

**Example**

```python
from sportsdataverse.basketball.atrium import nbl1_teams
nbl1_teams(season=2026)
```
