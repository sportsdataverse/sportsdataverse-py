---
title: "Package — additional Python functions — Pff (3)"
sidebar_label: "Pff (3)"
sidebar_position: 3
description: "Package — additional Python functions — Pff (3) — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — Pff (3)

### pff_ncaa_facet_receiving_summary {#pff_ncaa_facet_receiving_summary}

`pff_ncaa_facet_receiving_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_receiving_summary()
```

### pff_ncaa_facet_return_summary {#pff_ncaa_facet_return_summary}

`pff_ncaa_facet_return_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_return_summary()
```

### pff_ncaa_facet_run_blocking {#pff_ncaa_facet_run_blocking}

`pff_ncaa_facet_run_blocking(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_blocking()
```

### pff_ncaa_facet_run_defense_summary {#pff_ncaa_facet_run_defense_summary}

`pff_ncaa_facet_run_defense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_run_defense_summary()
```

### pff_ncaa_facet_rushing_direction_stats {#pff_ncaa_facet_rushing_direction_stats}

`pff_ncaa_facet_rushing_direction_stats(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_direction_stats()
```

### pff_ncaa_facet_rushing_summary {#pff_ncaa_facet_rushing_summary}

`pff_ncaa_facet_rushing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_rushing_summary()
```

### pff_ncaa_facet_slot_coverages {#pff_ncaa_facet_slot_coverages}

`pff_ncaa_facet_slot_coverages(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_slot_coverages()
```

### pff_ncaa_facet_special_teams_summary {#pff_ncaa_facet_special_teams_summary}

`pff_ncaa_facet_special_teams_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_special_teams_summary()
```

### pff_ncaa_facet_time_in_pockets {#pff_ncaa_facet_time_in_pockets}

`pff_ncaa_facet_time_in_pockets(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_time_in_pockets()
```

### pff_ncaa_games {#pff_ncaa_games}

`pff_ncaa_games(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[int]` | `None` | Single week number. |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_games()
```

### pff_ncaa_leagues {#pff_ncaa_leagues}

`pff_ncaa_leagues(headers: 'Optional[Dict[str, str]]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Leagues + seasons + week groups (bootstrap)

Endpoint: `GET https://premium.pff.com/api/v1/leagues`
Example URL: https://premium.pff.com/api/v1/leagues

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_leagues()
```

### pff_ncaa_player_defense_summary {#pff_ncaa_player_defense_summary}

`pff_ncaa_player_defense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /defense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/defense/summary`
Example URL: https://premium.pff.com/api/v1/player/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_player_defense_summary()
```

### pff_ncaa_player_offense_blocking {#pff_ncaa_player_offense_blocking}

`pff_ncaa_player_offense_blocking(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/blocking (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/blocking`
Example URL: https://premium.pff.com/api/v1/player/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_player_offense_blocking()
```

### pff_ncaa_player_offense_summary {#pff_ncaa_player_offense_summary}

`pff_ncaa_player_offense_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /offense/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/offense/summary`
Example URL: https://premium.pff.com/api/v1/player/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_player_offense_summary()
```

### pff_ncaa_player_passing_summary {#pff_ncaa_player_passing_summary}

`pff_ncaa_player_passing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /passing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/passing/summary`
Example URL: https://premium.pff.com/api/v1/player/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_player_passing_summary()
```

### pff_ncaa_player_position_pivot {#pff_ncaa_player_position_pivot}

`pff_ncaa_player_position_pivot(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Positional-pivot export (JSON; UI also uses this for CSV download)

Endpoint: `GET https://premium.pff.com/api/v1/player/position/pivot`
Example URL: https://premium.pff.com/api/v1/player/position/pivot

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_player_receiving_summary {#pff_ncaa_player_receiving_summary}

`pff_ncaa_player_receiving_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /receiving/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/receiving/summary`
Example URL: https://premium.pff.com/api/v1/player/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_player_rushing_summary {#pff_ncaa_player_rushing_summary}

`pff_ncaa_player_rushing_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /rushing/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/rushing/summary`
Example URL: https://premium.pff.com/api/v1/player/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_player_seasons {#pff_ncaa_player_seasons}

`pff_ncaa_player_seasons(*, league: 'Optional[str]' = 'ncaa', player_id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Seasons a player has data for

Endpoint: `GET https://premium.pff.com/api/v1/player/seasons`
Example URL: https://premium.pff.com/api/v1/player/seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_player_snaps_summary {#pff_ncaa_player_snaps_summary}

`pff_ncaa_player_snaps_summary(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, player_id: 'Optional[int]' = None, career: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player-detail report /snaps/summary (per-week + totals for one player)

Endpoint: `GET https://premium.pff.com/api/v1/player/snaps/summary`
Example URL: https://premium.pff.com/api/v1/player/snaps/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_players {#pff_ncaa_players}

`pff_ncaa_players(*, league: 'Optional[str]' = 'ncaa', name: 'Optional[str]' = None, id: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Player search (name=) or lookup (id=)

Endpoint: `GET https://premium.pff.com/api/v1/players`
Example URL: https://premium.pff.com/api/v1/players

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_teams {#pff_ncaa_teams}

`pff_ncaa_teams(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Teams / franchise groups + games for a league-season

Endpoint: `GET https://premium.pff.com/api/v1/teams`
Example URL: https://premium.pff.com/api/v1/teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_ncaa_teams_overview {#pff_ncaa_teams_overview}

`pff_ncaa_teams_overview(*, league: 'Optional[str]' = 'ncaa', season: 'Optional[int]' = None, week: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Team overview table (By Team landing)

Endpoint: `GET https://premium.pff.com/api/v1/teams/overview`
Example URL: https://premium.pff.com/api/v1/teams/overview

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'ncaa'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_nfl_facet_blocking_summary {#pff_nfl_facet_blocking_summary}

`pff_nfl_facet_blocking_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_blocking_summary()
```

### pff_nfl_facet_coverage_scheme {#pff_nfl_facet_coverage_scheme}

`pff_nfl_facet_coverage_scheme(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_scheme()
```

### pff_nfl_facet_coverage_summary {#pff_nfl_facet_coverage_summary}

`pff_nfl_facet_coverage_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_coverage_summary()
```

### pff_nfl_facet_defense_summary {#pff_nfl_facet_defense_summary}

`pff_nfl_facet_defense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_defense_summary()
```

### pff_nfl_facet_field_goal_summary {#pff_nfl_facet_field_goal_summary}

`pff_nfl_facet_field_goal_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_field_goal_summary()
```

### pff_nfl_facet_kicking_summary {#pff_nfl_facet_kicking_summary}

`pff_nfl_facet_kicking_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_kicking_summary()
```

### pff_nfl_facet_offense_summary {#pff_nfl_facet_offense_summary}

`pff_nfl_facet_offense_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_offense_summary()
```

### pff_nfl_facet_pass_blocking {#pff_nfl_facet_pass_blocking}

`pff_nfl_facet_pass_blocking(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_blocking()
```

### pff_nfl_facet_pass_rush_summary {#pff_nfl_facet_pass_rush_summary}

`pff_nfl_facet_pass_rush_summary(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_pass_rush_summary()
```

### pff_nfl_facet_passing_allowed_pressure {#pff_nfl_facet_passing_allowed_pressure}

`pff_nfl_facet_passing_allowed_pressure(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_allowed_pressure()
```

### pff_nfl_facet_passing_concept {#pff_nfl_facet_passing_concept}

`pff_nfl_facet_passing_concept(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_concept()
```

### pff_nfl_facet_passing_depth {#pff_nfl_facet_passing_depth}

`pff_nfl_facet_passing_depth(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_depth()
```

### pff_nfl_facet_passing_detail_stats {#pff_nfl_facet_passing_detail_stats}

`pff_nfl_facet_passing_detail_stats(*, league: 'Optional[str]' = 'nfl', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'nfl'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
| `season` | `Optional[int]` | `None` | Season (starting year). |
| `week` | `Optional[str]` | `None` | Week or week-group key (e.g. 'REG', a week number, or a range). |
| `franchise_id` | `Optional[int]` | `None` | PFF franchise (team) id; filters a report 'By Team'. |
| `game_id` | `Optional[int]` | `None` | PFF game id; filters a report 'By Game'. |
| `division` | `Optional[str]` | `None` | Division filter (NCAA). |
| `headers` | `Optional[Dict[str, str]]` | `None` | optional pre-minted auth headers dict (e.g. from nfl_headers_gen()) to reuse across calls; a fresh anonymous token is minted when omitted. |
| `return_parsed` | `bool` | `True` | parse the payload through parse_pff_report -> polars DataFrame (default True). Pass return_parsed=False for the raw JSON Dict. |
| `return_as_pandas` | `bool` | `False` | with return_parsed, return a pandas DataFrame instead of polars. |

**Returns**

A polars/pandas DataFrame by default; the raw JSON `Dict` when `return_parsed=False`.

**Example**

```python
pff_facet_passing_detail_stats()
```
