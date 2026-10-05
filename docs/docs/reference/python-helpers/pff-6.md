---
title: "Package — additional Python functions — Pff: ufl_player–ufl_teams"
sidebar_label: "Pff: ufl_player–ufl_teams"
sidebar_position: 6
description: "Package — additional Python functions — Pff: ufl_player–ufl_teams — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Pff: ufl_player–ufl_teams

### pff_ufl_player_position_pivot {#pff_ufl_player_position_pivot}

`pff_ufl_player_position_pivot(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_position_pivot()
```

### pff_ufl_player_receiving_summary {#pff_ufl_player_receiving_summary}

`pff_ufl_player_receiving_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_receiving_summary()
```

### pff_ufl_player_rushing_summary {#pff_ufl_player_rushing_summary}

`pff_ufl_player_rushing_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_rushing_summary()
```

### pff_ufl_player_seasons {#pff_ufl_player_seasons}

`pff_ufl_player_seasons(*, league: 'Optional[str]' = 'ufl', player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_seasons()
```

### pff_ufl_player_snaps_summary {#pff_ufl_player_snaps_summary}

`pff_ufl_player_snaps_summary(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `player_id` | `Optional[int]` | `None` | PFF player id (snake_case on the wire; matches the /players id). |
| `career` | `Optional[str]` | `None` | Career-rollup flag ("true"/"false"); player-detail views only. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_player_detail -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_player_snaps_summary()
```

### pff_ufl_players {#pff_ufl_players}

`pff_ufl_players(*, league: 'Optional[str]' = 'ufl', name: 'Optional[str]' = None, id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `name` | `Optional[str]` | `None` | Player-name search prefix. |
| `id` | `Optional[int]` | `None` | Entity id (player lookup). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_players()
```

### pff_ufl_teams {#pff_ufl_teams}

`pff_ufl_teams(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams()
```

### pff_ufl_teams_overview {#pff_ufl_teams_overview}

`pff_ufl_teams_overview(*, league: 'Optional[str]' = 'ufl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ufl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_teams_overview()
```
