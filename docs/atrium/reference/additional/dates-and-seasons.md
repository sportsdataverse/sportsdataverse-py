# ATRIUM — additional Python functions — Dates and seasons

> ATRIUM — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package.

### atrium_season_id {#atrium_season_id}

`atrium_season_id(website_id: 'int', season: 'int', competition_id: 'Optional[str]' = None, **kwargs: 'Any') -> 'str'`

The `seasonId` of a site's season, from its END year and optionally its competition.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `website_id` | `int` |  | the site (see `SITES`: 12 LNB, 14 LNBP, 3 NBL1, 13 El Salvador, 22 Panama, 326 Montenegro). |
| `season` | `int` |  | the sdv season END year (2026 = 2025-26 on LNB; the calendar year on the other sites). |
| `competition_id` | `Optional[str]` | `None` | the competition when the site lists several for that year (see `atrium_seasons`). Omit it when the year is unique. |

**Returns**

The season's UUID, for the `season_id` argument of every season-scoped wrapper.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | `season` is not an integer, the site is not in `SITES`, it lists no seasons (236), or the year is shared by several competitions and `competition_id` is missing. |
| `sportsdataverse.errors.NoDataError` | The site lists no such season. |
| `AssetFetchError` | The season list could not be fetched or is not the site's. |

**Example**

```python
from sportsdataverse.basketball.atrium import atrium_season_id
atrium_season_id(14, 2026, competition_id="16be8396-24fb-11ef-9ab7-df6be196e88f")
```
