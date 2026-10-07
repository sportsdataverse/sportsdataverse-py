---
title: "MLB — additional Python functions — Dates and seasons"
sidebar_label: "Dates and seasons"
sidebar_position: 7
description: "MLB — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Dates and seasons

### most_recent_mlb_season {#most_recent_mlb_season}

`most_recent_mlb_season() -> 'int'`

most_recent_mlb_season - return the most recent / current MLB season year.

MLB seasons run calendar-year. Before April we still consider the *previous* year
the "most recent" season (since spring training only starts in late February).

**Returns**

The most recent MLB season year (e.g. `2024`).
