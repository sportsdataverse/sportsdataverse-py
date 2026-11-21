# NBA — additional Python functions — Dates and seasons

> NBA — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package.

### year_to_season {#year_to_season}

`year_to_season(year)`

Convert a season START year (e.g. 2023) to the NBA's hyphenated label

(e.g. `"2023-24"`).

Callers working in the end-year convention pass `end_year - 1` (e.g.
`year_to_season(most_recent_nba_season() - 1)`).

Handles century rollover (1999 -> `"1999-00"`) and zero-pads the
second half of the label.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `int` |  | The starting calendar year of the season (e.g. 2023 for the 2023-24 season). |

**Returns**

NBA-style season label.

**Example**

```python
from sportsdataverse.nba import year_to_season
label = year_to_season(2023)
print(label)  # "2023-24"

# Century rollover

print(year_to_season(1999))  # "1999-00"
```
