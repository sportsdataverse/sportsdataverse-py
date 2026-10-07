---
title: "Package — additional Python functions — PFF Premium Stats (LEGACY): aaf_facet–aaf_leagues"
sidebar_label: "PFF Premium Stats (LEGACY): aaf_facet–aaf_leagues"
sidebar_position: 1
description: "Package — additional Python functions — PFF Premium Stats (LEGACY): aaf_facet–aaf_leagues — function reference in sdv-py, the SportsDataverse Python package."
---
# Package — additional Python functions — PFF Premium Stats (LEGACY): aaf_facet–aaf_leagues

### pff_aaf_facet_blocking_summary {#pff_aaf_facet_blocking_summary}

`pff_aaf_facet_blocking_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_coverage_scheme {#pff_aaf_facet_coverage_scheme}

`pff_aaf_facet_coverage_scheme(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_scheme`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_coverage_summary {#pff_aaf_facet_coverage_summary}

`pff_aaf_facet_coverage_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_defense_summary {#pff_aaf_facet_defense_summary}

`pff_aaf_facet_defense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/summary`
Example URL: https://premium.pff.com/api/v1/facet/defense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_field_goal_summary {#pff_aaf_facet_field_goal_summary}

`pff_aaf_facet_field_goal_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /field_goal/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/field_goal/summary`
Example URL: https://premium.pff.com/api/v1/facet/field_goal/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_kicking_summary {#pff_aaf_facet_kicking_summary}

`pff_aaf_facet_kicking_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /kickoff/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/kickoff/summary`
Example URL: https://premium.pff.com/api/v1/facet/kickoff/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_offense_summary {#pff_aaf_facet_offense_summary}

`pff_aaf_facet_offense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/summary`
Example URL: https://premium.pff.com/api/v1/facet/offense/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_pass_blocking {#pff_aaf_facet_pass_blocking}

`pff_aaf_facet_pass_blocking(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/pass_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/pass_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/pass_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_pass_rush_summary {#pff_aaf_facet_pass_rush_summary}

`pff_aaf_facet_pass_rush_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/defense/pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_passing_allowed_pressure {#pff_aaf_facet_passing_allowed_pressure}

`pff_aaf_facet_passing_allowed_pressure(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/allowed_pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/allowed_pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/allowed_pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_passing_concept {#pff_aaf_facet_passing_concept}

`pff_aaf_facet_passing_concept(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/concept`
Example URL: https://premium.pff.com/api/v1/facet/passing/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_passing_depth {#pff_aaf_facet_passing_depth}

`pff_aaf_facet_passing_depth(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/depth`
Example URL: https://premium.pff.com/api/v1/facet/passing/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_passing_detail_stats {#pff_aaf_facet_passing_detail_stats}

`pff_aaf_facet_passing_detail_stats(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/detail (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/detail`
Example URL: https://premium.pff.com/api/v1/facet/passing/detail

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_passing_pressure {#pff_aaf_facet_passing_pressure}

`pff_aaf_facet_passing_pressure(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/pressure (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/pressure`
Example URL: https://premium.pff.com/api/v1/facet/passing/pressure

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_passing_pressure()
```

### pff_aaf_facet_passing_summary {#pff_aaf_facet_passing_summary}

`pff_aaf_facet_passing_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /passing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/passing/summary`
Example URL: https://premium.pff.com/api/v1/facet/passing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_passing_summary()
```

### pff_aaf_facet_pbes {#pff_aaf_facet_pbes}

`pff_aaf_facet_pbes(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/pass-blocking/efficiency/line (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line`
Example URL: https://premium.pff.com/api/v1/facet/signature/pass-blocking/efficiency/line

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_pbes()
```

### pff_aaf_facet_prps {#pff_aaf_facet_prps}

`pff_aaf_facet_prps(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/outside_pass_rush (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/outside_pass_rush

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_prps()
```

### pff_aaf_facet_punting_summary {#pff_aaf_facet_punting_summary}

`pff_aaf_facet_punting_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /punting/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/punting/summary`
Example URL: https://premium.pff.com/api/v1/facet/punting/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_punting_summary()
```

### pff_aaf_facet_receiving_concept {#pff_aaf_facet_receiving_concept}

`pff_aaf_facet_receiving_concept(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/concept (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/concept`
Example URL: https://premium.pff.com/api/v1/facet/receiving/concept

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_receiving_concept()
```

### pff_aaf_facet_receiving_coverage {#pff_aaf_facet_receiving_coverage}

`pff_aaf_facet_receiving_coverage(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/coverage`
Example URL: https://premium.pff.com/api/v1/facet/receiving/coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_receiving_coverage()
```

### pff_aaf_facet_receiving_coverage_stats {#pff_aaf_facet_receiving_coverage_stats}

`pff_aaf_facet_receiving_coverage_stats(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/coverage_matchup (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/coverage_matchup`
Example URL: https://premium.pff.com/api/v1/facet/defense/coverage_matchup

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_receiving_coverage_stats()
```

### pff_aaf_facet_receiving_depth {#pff_aaf_facet_receiving_depth}

`pff_aaf_facet_receiving_depth(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/depth (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/depth`
Example URL: https://premium.pff.com/api/v1/facet/receiving/depth

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_receiving_depth()
```

### pff_aaf_facet_receiving_scheme {#pff_aaf_facet_receiving_scheme}

`pff_aaf_facet_receiving_scheme(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/scheme (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/scheme`
Example URL: https://premium.pff.com/api/v1/facet/receiving/scheme

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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
pff_facet_receiving_scheme()
```

### pff_aaf_facet_receiving_summary {#pff_aaf_facet_receiving_summary}

`pff_aaf_facet_receiving_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /receiving/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/receiving/summary`
Example URL: https://premium.pff.com/api/v1/facet/receiving/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_return_summary {#pff_aaf_facet_return_summary}

`pff_aaf_facet_return_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /return/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/return/summary`
Example URL: https://premium.pff.com/api/v1/facet/return/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_run_blocking {#pff_aaf_facet_run_blocking}

`pff_aaf_facet_run_blocking(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /offense/run_blocking (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/offense/run_blocking`
Example URL: https://premium.pff.com/api/v1/facet/offense/run_blocking

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_run_defense_summary {#pff_aaf_facet_run_defense_summary}

`pff_aaf_facet_run_defense_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /defense/run (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/defense/run`
Example URL: https://premium.pff.com/api/v1/facet/defense/run

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_rushing_direction_stats {#pff_aaf_facet_rushing_direction_stats}

`pff_aaf_facet_rushing_direction_stats(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/direction (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/direction`
Example URL: https://premium.pff.com/api/v1/facet/rushing/direction

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_rushing_summary {#pff_aaf_facet_rushing_summary}

`pff_aaf_facet_rushing_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /rushing/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/rushing/summary`
Example URL: https://premium.pff.com/api/v1/facet/rushing/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_slot_coverages {#pff_aaf_facet_slot_coverages}

`pff_aaf_facet_slot_coverages(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/defense/slot_coverage (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage`
Example URL: https://premium.pff.com/api/v1/facet/signature/defense/slot_coverage

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_special_teams_summary {#pff_aaf_facet_special_teams_summary}

`pff_aaf_facet_special_teams_summary(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /special/summary (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/special/summary`
Example URL: https://premium.pff.com/api/v1/facet/special/summary

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_facet_time_in_pockets {#pff_aaf_facet_time_in_pockets}

`pff_aaf_facet_time_in_pockets(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[str]' = None, franchise_id: 'Optional[int]' = None, game_id: 'Optional[int]' = None, division: 'Optional[str]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Facet report /signature/passing/time_in_pocket (By Position leaderboard; add franchiseId for By Team, gameId for By Game)

Endpoint: `GET https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket`
Example URL: https://premium.pff.com/api/v1/facet/signature/passing/time_in_pocket

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_games {#pff_aaf_games}

`pff_aaf_games(*, league: 'Optional[str]' = 'aaf', season: 'Optional[int]' = None, week: 'Optional[int]' = None, headers: 'Optional[Dict[str, str]]' = None, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

LEGACY (premium.pff.com cookie auth; prefer the pff_api_* Developer API wrappers). Games list for league-season(-week)

Endpoint: `GET https://premium.pff.com/api/v1/games`
Example URL: https://premium.pff.com/api/v1/games

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `league` | `Optional[str]` | `'aaf'` | League slug (nfl/ncaa/aaf/ufl); pre-bound by the per-league shim modules. |
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

### pff_aaf_leagues {#pff_aaf_leagues}

`pff_aaf_leagues(headers: 'Optional[Dict[str, str]]' = None, *, return_parsed: 'bool' = True, return_as_pandas: 'bool' = False, **kwargs) -> 'Union[pl.DataFrame, pd.DataFrame, Dict]'`

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
