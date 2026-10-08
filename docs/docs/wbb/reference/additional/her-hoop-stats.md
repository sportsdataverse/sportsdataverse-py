---
title: "WBB — additional Python functions — Her Hoop Stats"
sidebar_label: "Her Hoop Stats"
sidebar_position: 6
description: "WBB — additional Python functions — Her Hoop Stats — function reference in sdv-py, the SportsDataverse Python package."
---
# WBB — additional Python functions — Her Hoop Stats

### has_herhoopstats_login {#has_herhoopstats_login}

`has_herhoopstats_login() -> 'bool'`

Whether Her Hoop Stats credentials are set in the environment.

**Returns**

`True` when both an e-mail and a password resolve from the environment.

**Example**

```python
import pytest
from sportsdataverse.wbb import has_herhoopstats_login

pytestmark = pytest.mark.skipif(
    not has_herhoopstats_login(), reason="no Her Hoop Stats login"
)
```

### herhoopstats_login {#herhoopstats_login}

`herhoopstats_login(email: 'Optional[str]' = None, password: 'Optional[str]' = None, *, proxy: 'Any' = None) -> 'requests.Session'`

Log into herhoopstats.com and return the authenticated session.

The Python counterpart of wehoop's `.hhs_login()`. Optional -- every
wrapper logs in on demand and reuses a cached session -- but it is the
fastest way to verify credentials or a proxy before a long pull, and the
returned session can be handed to a wrapper as `session=`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `email` | `Optional[str]` | `None` | Subscription e-mail. Falls back to `HERHOOPSTATS_EMAIL`. |
| `password` | `Optional[str]` | `None` | Subscription password. Falls back to `HERHOOPSTATS_PW`. |
| `proxy` | `Any` | `None` | Proxy URL `str` or `requests` `proxies=` `dict`. Falls back to `SDV_PY_HERHOOPSTATS_PROXY` then `SDV_PY_PROXY`. |

**Returns**

An authenticated `requests.Session`.

**Example**

```python
from sportsdataverse.wbb import herhoopstats_login

session = herhoopstats_login(proxy="http://user:pw@proxy.example:8080")
```

### herhoopstats_team_roster {#herhoopstats_team_roster}

`herhoopstats_team_roster(team_link: 'str', *, email: 'Optional[str]' = None, password: 'Optional[str]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

The player roster table from one Her Hoop Stats team page.

Port of wehoop's `hhs_team_roster()`: picks the table carrying a player /
name column, falling back to the tallest table on the page.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_link` | `str` |  | A team page path or full URL. |
| `email` | `Optional[str]` | `None` | Subscription e-mail; falls back to `HERHOOPSTATS_EMAIL`. |
| `password` | `Optional[str]` | `None` | Subscription password; falls back to `HERHOOPSTATS_PW`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per player. A zero-row frame when the page carried no roster.

| col_name | type | description |
|---|---|---|
| `player` | character | Player name. |
| `g` | integer | Games played. |
| `gs` | integer | Games started. |
| `min` | double | Minutes played. |
| `pts` | double | Points scored. |
| `trb` | double | Career total rebounds. |
| `ast` | double | Assists. |
| `tov` | double | Turnovers. |
| `stl` | double | Steals. |
| `blk` | double | Blocks. |
| `fg_pct` | character | Field goal percentage (0-1). |
| `ft_pct` | character | Free throw percentage (0-1). |
| `2_p_pct` | character |  |
| `3_p_pct` | character |  |

**Example**

```python
from sportsdataverse.wbb import herhoopstats_team_roster

roster = herhoopstats_team_roster("/stats/ncaa/team/12345/2024/")
```

### herhoopstats_team_stats {#herhoopstats_team_stats}

`herhoopstats_team_stats(team_link: 'str', *, email: 'Optional[str]' = None, password: 'Optional[str]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Dict[str, pl.DataFrame | pd.DataFrame]'`

Every table on one Her Hoop Stats team page.

Port of wehoop's `hhs_team_stats()`, which keeps only the tallest table.
This returns all of them keyed by the table's HTML `id` (else caption, else
position), so a single fetch covers the whole page.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `team_link` | `str` |  | A team page path or full URL -- e.g. a link taken from the team-page column of `herhoopstats_teams`. |
| `email` | `Optional[str]` | `None` | Subscription e-mail; falls back to `HERHOOPSTATS_EMAIL`. |
| `password` | `Optional[str]` | `None` | Subscription password; falls back to `HERHOOPSTATS_PW`. |
| `return_as_pandas` | `bool` | `False` | Return `pandas.DataFrame` values instead of polars. |

**Returns**

`{table_key: DataFrame}`; empty when the page carried no data table.

**Example**

```python
from sportsdataverse.wbb import herhoopstats_team_stats

tables = herhoopstats_team_stats("/stats/ncaa/team/12345/2024/")
list(tables)
```

### herhoopstats_teams {#herhoopstats_teams}

`herhoopstats_teams(min_season: 'int', max_season: 'Optional[int]' = None, division: 'int' = 1, *, email: 'Optional[str]' = None, password: 'Optional[str]' = None, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame'`

NCAA women's team single-season summary table.

Port of wehoop's `hhs_teams()`. One row per team-season (record, scoring,
per-100-possession columns), with the requested `min_season` /
`max_season` / `division` attached so concatenated pulls stay traceable.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `min_season` | `int` |  | First season, as a 4-digit ENDING year (2024 = 2023-24). |
| `max_season` | `Optional[int]` | `None` | Last season, same convention. Defaults to `min_season`. |
| `division` | `int` | `1` | NCAA division -- `1` (default), `2` or `3`. |
| `email` | `Optional[str]` | `None` | Subscription e-mail; falls back to `HERHOOPSTATS_EMAIL`. |
| `password` | `Optional[str]` | `None` | Subscription password; falls back to `HERHOOPSTATS_PW`. |
| `return_as_pandas` | `bool` | `False` | Return a `pandas.DataFrame` instead of polars. |

**Returns**

One row per team-season, plus a `team_link` column carrying each team's page path -- the only way to reach `herhoopstats_team_stats` / `herhoopstats_team_roster`. A zero-row frame when nothing matched.

| col_name | type | description |
|---|---|---|
| `season` | character | Season identifier (4-digit year or 'YYYY-YY' string). |
| `team` | character | Team-side label or team identifier. |
| `g` | integer | Games played. |
| `w` | integer | Wins. |
| `l` | integer | Losses. |
| `win_pct` | character | Win percentage (0-1 decimal). |
| `pts` | double | Points scored. |
| `opp_pts` | double | Opponent points. |
| `net_pts` | double |  |
| `off_rtg` | double |  |
| `def_rtg` | double |  |
| `net_rtg` | double |  |
| `pace` | double | Possessions per 48 minutes. |
| `team_link` | character |  |
| `min_season` | integer |  |
| `max_season` | integer |  |
| `division` | integer | Team division. |

**Example**

```python
from sportsdataverse.wbb import herhoopstats_teams

teams = herhoopstats_teams(min_season=2024, division=1)

# Multiple seasons, through a proxy

teams = herhoopstats_teams(2022, 2024, proxy="http://127.0.0.1:8888")
```
