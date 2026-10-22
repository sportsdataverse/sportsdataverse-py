# MBB — additional Python functions — ESPN

> MBB — additional Python functions — ESPN — function reference in sdv-py, the SportsDataverse Python package.

### espn_mbb_calendar {#espn_mbb_calendar}

`espn_mbb_calendar(season=None, ondays=None, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_mbb_calendar - look up the men's college basketball calendar for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season; `None` means the current season. |
| `ondays` | `boolean` | `None` | Used to return dates for calendar ondays |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing calendar dates for the requested season.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season year. |
| `datetime` | character | The calendar day as ESPN sends it, an ISO 8601 UTC timestamp string (e.g. 2024-11-04T08:00Z). |
| `date` | character | Date in YYYY-MM-DD format. |
| `year` | character | 4-digit year. |
| `month` | character | Month of the calendar day as a two-digit string, e.g. 11. |
| `day` | character | Day number within the month. |
| `dateURL` | character | The calendar day as YYYYMMDD, the dates= value of that day's scoreboard request. |
| `url` | character | RotoWire player page URL. |

**Raises**

| Exception | Description |
|---|---|
| `SeasonNotFoundError` | If `season` is less than 2002. |

**Example**

```python
from sportsdataverse.mbb import espn_mbb_calendar
cal = espn_mbb_calendar(season=2024)
cal.head()

# On-days only (dates with games on the schedule)

ondays = espn_mbb_calendar(season=2024, ondays=True)
ondays.head()

# Pandas round-trip

cal_pd = espn_mbb_calendar(season=2024, return_as_pandas=True)
cal_pd.head()
```

### scoreboard_event_parsing {#scoreboard_event_parsing}

`scoreboard_event_parsing(event)`

Flatten one ESPN scoreboard event for the schedule frame, in place.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` |  |  | One element of the scoreboard payload's `events` list. |

**Returns**

The same event, modified: `competitions[0]` gains `home` / `away` team dicts (with `score`, `winner`, `currentRank`, `linescores`, `records`), `notes_type` / `notes_headline` and `broadcast_market` / `broadcast_name`, and loses `competitors`, `broadcasts`, `notes`, `odds`, `leaders` and the other nested blocks the schedule frame does not use.
