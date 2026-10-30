# NBA — additional Python functions — NBA Official Injury Report

> NBA — additional Python functions — NBA Official Injury Report — function reference in sdv-py, the SportsDataverse Python package.

### nba_injury_report {#nba_injury_report}

`nba_injury_report(date: 'dt.date | None' = None, report_time: 'dt.datetime | None' = None, *, return_as_pandas: 'bool' = False, proxy: 'Any' = None, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

Fetch and parse an official NBA injury report.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `date \| None` | `None` | Report date (US Eastern). Defaults to today. With no `report_time`, the LATEST published slot of that date is used (probing backwards through `nba_injury_report_slots`, only slots up to now: a date ahead of today in US Eastern probes none). |
| `report_time` | `datetime \| None` | `None` | An exact slot (naive US Eastern), e.g. `dt.datetime(2026, 1, 15, 17, 30)`. Overrides `date`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |
| `proxy` | `Any` | `None` | Proxy configuration in the `requests` `proxies=` shape. |

**Returns**

The parsed report (see `parse_nba_injury_report`).

| col_name | type | description |
|---|---|---|
| `report_datetime` | Datetime | US Eastern time the league published this report, read from the PDF title (a slot before 2025-12-22 09:00 named HH is the HH:30 snapshot). |
| `game_date` | Date | Date of the game the row's player status applies to. |
| `game_time` | String | Scheduled tip as printed on the report, e.g. "07:30 (ET)"; the PDF carries no AM/PM. |
| `matchup` | String | Away@home team abbreviations, e.g. "WAS@SAC". |
| `away_team_abbr` | String | Away team abbreviation split from matchup. |
| `home_team_abbr` | String | Home team abbreviation split from matchup. |
| `team` | String | Full name of the team that filed the row, e.g. "Memphis Grizzlies". |
| `player_name` | String | Player as printed, "Last, First" (no player id on the report); null on a team's NOT YET SUBMITTED row. |
| `current_status` | String | Out, Doubtful, Questionable, Probable or Available; null on a NOT YET SUBMITTED row. |
| `reason` | String | Reason for the status (e.g. "Injury/Illness - Right Calf; Strain", "G League - Two-Way"), or "NOT YET SUBMITTED" when the team has not filed. |
| `category` | String | 2018-19 layout only - the reason's category (e.g. "Injury/Illness", "G League Team"); null afterwards. |
| `previous_status` | String | 2018-19 layout only - the status on the previous report; null afterwards. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The slot (or every slot of the date) is not published (S3's 403 `AccessDenied`). |
| `AssetFetchError` | Any other non-200 answer -- including a 404 and a 403 that is not S3's `AccessDenied` (a CDN / WAF block) -- an HTTP-200 body that is not a readable PDF (a CDN error page, a truncated file), or a connection that fails after the retries. |
| `ImportError` | `pdfplumber` is not installed. |

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_official_injury import nba_injury_report

df = nba_injury_report(report_time=dt.datetime(2026, 1, 15, 17, 30))
print(df.shape)

# Latest report today

df = nba_injury_report()

# Pandas output

df_pd = nba_injury_report(dt.date(2024, 1, 15), return_as_pandas=True)
```

### nba_injury_report_slots {#nba_injury_report_slots}

`nba_injury_report_slots(date: 'dt.date') -> 'list[dt.datetime]'`

Every slot the league's schedule allows on `date`, earliest first.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `date` | `date` |  | The report date (US Eastern). |

**Returns**

Naive US Eastern datetimes: three a day (13:00 / 17:00 / 20:00) before 2021-10-15, hourly until 2025-12-22 09:00, every 15 minutes after. Empty before the archive's first report (2018-12-17). Not every slot is published (off days, the All-Star break), so a slot is a candidate.

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_official_injury import nba_injury_report_slots

len(nba_injury_report_slots(dt.date(2026, 1, 15)))  # 96
```

### nba_injury_report_url {#nba_injury_report_url}

`nba_injury_report_url(report_time: 'dt.datetime') -> 'str'`

The archive URL of the report published at `report_time` (US Eastern).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `report_time` | `datetime` |  | The slot, naive and in US Eastern time. Before 2025-12-22 09:00 only the hour is used (`05PM`); after it the slot is quarter-hourly (`05_30PM`). |

**Returns**

The PDF URL. Whether that slot exists is only known by fetching it.

**Example**

```python
import datetime as dt
from sportsdataverse.nba.nba_official_injury import nba_injury_report_url

nba_injury_report_url(dt.datetime(2026, 1, 15, 17, 30))
# '.../Injury-Report_2026-01-15_05_30PM.pdf'
```
