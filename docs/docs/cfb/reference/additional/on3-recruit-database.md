---
title: "CFB — additional Python functions — On3 Recruit Database"
sidebar_label: "On3 Recruit Database"
sidebar_position: 4
description: "CFB — additional Python functions — On3 Recruit Database — function reference in sdv-py, the SportsDataverse Python package."
---
# CFB — additional Python functions — On3 Recruit Database

### on3_industry_player_rankings {#on3_industry_player_rankings}

`on3_industry_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison player rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per recruit (consensus On3/Rivals/247/ESPN). Zero-row frame on empty.

**Example**

```python
from sportsdataverse.cfb import on3_players_industry_comparision  # forward RDB native
df = on3_players_industry_comparision(sport_key=1, year=2026)
print(df.shape)
```

### on3_industry_team_rankings {#on3_industry_team_rankings}

`on3_industry_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 Industry Comparison team rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (consensus ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_consensus_team_rankings  # forward RDB native
df = on3_team_ranking_consensus_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```

### on3_player_rankings {#on3_player_rankings}

`on3_player_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 player rankings for a class year (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per ranked recruit (On3 ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_person_sport_rankings  # forward RDB native
df = on3_person_sport_rankings(sport_key=1, year=2026)
print(df.shape)
```

### on3_team_rankings {#on3_team_rankings}

`on3_team_rankings(year: 'Union[int, str]', sport_slug: 'str' = 'football', page: 'Any' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

On3 team recruiting-class rankings (**deprecated** next/data` scrape).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `year` | `Union[int, str]` |  | recruiting class year (e.g. `2026`). |
| `sport_slug` | `str` | `'football'` | On3 sport slug (default `"football"`). |
| `page` | `Any` | `None` | 1-based page number, or `None` for the first page. |
| `return_parsed` | `bool` | `True` | return a tidy frame (default); `False` returns the raw dict. |
| `return_as_pandas` | `bool` | `False` | return a pandas DataFrame instead of polars. |

**Returns**

One row per team class (On3 ratings). Zero-row frame on empty payload.

**Example**

```python
from sportsdataverse.cfb import on3_team_ranking_team_rankings  # forward RDB native
df = on3_team_ranking_team_rankings(sport_slug="football", year=2025)
print(df.shape)
```
