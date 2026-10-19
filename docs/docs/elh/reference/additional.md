---
title: ELH — additional Python functions
sidebar_label: Additional functions
description: "ELH — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# ELH — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.elh`
not covered by the generated API-endpoint reference above.

## hokej.cz (Czech Extraliga)

### elh_game_ids {#elh_game_ids}

`elh_game_ids(season: 'int', *, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame]'`

Every hokej.cz game id with a play-by-play file in a season, paging the S3 listing.

Lists `match/{season - 1}/full/` (the bucket files a season under its START year) a
thousand keys at a time, following `marker` until the listing is complete.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | the season's END year (`2027` = 2026-27). Play-by-play starts in 2014-15. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per file: `hokejcz_game_id` (Int64), `season` (END year), `key`, `last_modified`, `size`; ordered by game id.

| col_name | type | description |
|---|---|---|
| `hokejcz_game_id` | integer | hokej.cz game id, the file name of the match/{season}/full/{id}.json key. |
| `season` | integer | Season as its END year (the season the call asked for). |
| `key` | character | Full S3 object key of the play-by-play file. |
| `last_modified` | character | Time the file was last written, an ISO 8601 UTC timestamp string from the S3 listing. |
| `size` | integer | File size in bytes; a file of a few hundred bytes is written ahead of a game and holds no events. |

**Example**

```python
from sportsdataverse.hockey.elh import elh_game_ids
ids = elh_game_ids(season=2027)
print(ids.height)

# Skip the files written ahead of a game (a few hundred bytes)

ids.filter(pl.col("size") > 2000)
```
