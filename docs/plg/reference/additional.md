# P. LEAGUE+ — additional Python functions

> P. LEAGUE+ — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package.

Hand-written wrappers, loaders, and helpers in `sportsdataverse.basketball.plg` that are not
generated API-endpoint wrappers (those have their own reference pages).

## P. LEAGUE+ (Taiwan) and Preciser

### season_slug {#season_slug}

`season_slug(season: 'Any') -> 'str'`

The site's slug for a season END year (2025 -> `'2024-25'`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Any` |  | the season END year. |

**Returns**

`f"{season - 1}-{season % 100:02d}"`.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | `season` is before 2021 (the site's first season is 2020-21). |
| `ValueError` | `season` is not an integer. |

**Example**

```python
from sportsdataverse.basketball.plg.pleague_runtime import season_slug

season_slug(2025)  # '2024-25'
```

## IDs and crosswalks

### pleague_team_crosswalk {#pleague_team_crosswalk}

`pleague_team_crosswalk(*, return_as_pandas: 'bool' = False) -> 'Any'`

P. LEAGUE+ site `team_id` (1-8) <-> Preciser team uuid, for every club ever listed.

The site's `team_id` is stable across seasons and renames; the vendor keys teams by uuid
(`team_uuid` / `home_team_uuid` / `away_team_uuid` of the `pleague_preciser` frames). Persons
need no table: the Preciser `person_id` is the site box row's `person_id` (same string).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

polars.DataFrame | pandas.DataFrame: eight rows, `team_id` (`Int64`) and `team_uuid` (`Utf8`).

| col_name | type | description |
|---|---|---|
| `team_id` | Int64 | P. LEAGUE+ site team id (1-8), stable across seasons and renames. |
| `team_uuid` | String | Preciser team uuid of the same club (team_uuid of the pleague_preciser frames). |

**Raises**

| Exception | Description |
|---|---|
| `Nothing` | The table is a constant, no request is made. |

**Example**

```python
from sportsdataverse.basketball.basketball_crosswalk import pleague_team_crosswalk
from sportsdataverse.basketball.plg import pleague_preciser_shots

shots = pleague_preciser_shots(uuid="e6f98675-3e5b-11ed-949e-5d17350689cf")
shots.join(pleague_team_crosswalk(), on="team_uuid")
```
