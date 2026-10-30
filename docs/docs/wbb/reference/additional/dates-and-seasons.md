---
title: "Women's college basketball — additional Python functions — Dates and seasons"
sidebar_label: "Dates and seasons"
sidebar_position: 12
description: "Women's college basketball — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package."
---
# Women's college basketball — additional Python functions — Dates and seasons

### most_recent_wbb_season {#most_recent_wbb_season}

`most_recent_wbb_season()`

Return the most recent women's college basketball season year.

The women's college basketball season spans late October through early
April; for any month October-December the "current season" is the
following calendar year (e.g. October 2025 returns `2026`).

**Returns**

The most recent / current season year.

**Example**

```python
from sportsdataverse.wbb import most_recent_wbb_season, load_wbb_schedule
season = most_recent_wbb_season()
sched = load_wbb_schedule([season])
```
