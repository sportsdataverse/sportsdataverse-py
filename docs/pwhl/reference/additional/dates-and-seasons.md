# PWHL — additional Python functions — Dates and seasons

> PWHL — additional Python functions — Dates and seasons — function reference in sdv-py, the SportsDataverse Python package.

### most_recent_pwhl_season {#most_recent_pwhl_season}

`most_recent_pwhl_season() -> 'int'`

Newest PWHL regular season as an end-year integer.

The highest `season_yr` of a regular season that is not a one-off event, so a
preseason the feed lists before its regular season is not a default.

**Returns**

The newest regular season's END year (2026 = the 2025-26 season).

### pwhl_season_id {#pwhl_season_id}

`pwhl_season_id(return_as_pandas: 'bool' = False) -> 'Any'`

All PWHL seasons with end-year + game-type labels (HockeyTech `seasons`).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per season: `season_id` (Int64), `season_name`, `season_short`, `career`, `playoff`, `start_date`, `end_date`, `season_yr` (Int64, the END year) and `game_type_label`. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `season_id` | integer | Season identifier. |
| `season_name` | character | Full season name (e.g., "2024-25 Regular Season"). |
| `season_short` | character | Short season name. |
| `career` | character | Whether this is a career-stats season. |
| `playoff` | character | Whether the row is playoff statistics. |
| `start_date` | character | Season start date. |
| `end_date` | character | Season end date. |
| `season_yr` | integer | End year of the season the row belongs to, read from its name: "2025-26", "2025/26" and "2025-2026" are 2026, "26-27" is 2027, a compact "2425" is 2025. A preseason or exhibition named with the one year it starts in belongs to the next season ("2026 Pre-season" is 2027). Null when the name holds no year. |
| `game_type_label` | character | Game type read from the season name, first match wins: "preseason" (pre-season, preseason), "playoffs" (playoff, post), "exhibition", else "regular". One-off events such as all-star games are labelled "regular" too; season resolution skips them. |
