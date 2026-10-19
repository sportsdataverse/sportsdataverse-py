---
title: VHL — additional Python functions
sidebar_label: Additional functions
description: "VHL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# VHL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.hockey.vhl`
not covered by the generated API-endpoint reference above.

## VHL

### vhl_tn {#vhl_tn}

`vhl_tn(season: 'int', stage: 'str' = 'regular') -> 'int'`

The VHL tournament id (`tn`) of a season (END year) and stage.

Read from the calendar's tournament select, fetched once per process. The tn is what the
site keys every season page by; the wrappers map it for you, so you need it only to read
`tn` columns or build a URL by hand.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` |  | END year of the season (2026 = 2025/26). The calendar covers 2010/11 on. |
| `stage` | `str` | `'regular'` | `"regular"` (default) or `"playoff"`. |

**Returns**

the tn (`1374` for the 2025/26 regular season).

**Example**

```python
from sportsdataverse.hockey.vhl import vhl_tn
vhl_tn(2026)              # 1374
vhl_tn(2026, "playoff")   # 1375
```
