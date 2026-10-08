---
title: "WNBA — additional Python functions — Dates and seasons"
sidebar_label: "Dates and seasons"
sidebar_position: 8
description: "WNBA — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package."
---
# WNBA — additional Python functions — Dates and seasons

### most_recent_wnba_season {#most_recent_wnba_season}

`most_recent_wnba_season()`

most_recent_wnba_season - return the most recent (likely-completed) WNBA season year.

Returns the current calendar year if it's May or later (the WNBA regular
season has tipped off), otherwise the previous calendar year.

**Returns**

Year (e.g. `2024`) suitable for passing as a `season` argument to schedule / loader functions.

**Example**

```python
from sportsdataverse.wnba import most_recent_wnba_season, espn_wnba_calendar
season = most_recent_wnba_season()
cal = espn_wnba_calendar(season=season)
print(season, cal.height)
```
