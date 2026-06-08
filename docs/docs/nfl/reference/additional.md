---
title: NFL — additional Python functions
sidebar_label: Additional functions
sidebar_position: 50
---
# NFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nfl`
not covered by the generated API-endpoint reference above.

## Play-by-play, schedule & rosters

### `espn_nfl_game_rosters(game_id: 'int', raw=False, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_game_rosters() - Pull the game by id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | Unique game_id, can be obtained from espn_nfl_schedule(). |
| `raw` |  | `False` |  |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe of game roster data with columns: 'athlete_id', 'athlete_uid', 'athlete_guid', 'athlete_type', 'first_name', 'last_name', 'full_name', 'athlete_display_name', 'short_name', 'weight', 'display_weight', 'height', 'display_height', 'age', 'date_of_birth', 'slug', 'jersey', 'linked', 'active', 'alternate_ids_sdr', 'birth_place_city', 'birth_place_state', 'birth_place_country', 'headshot_href', 'headshot_alt', 'experience_years', 'experience_display_value', 'experience_abbreviation', 'status_id', 'status_name', 'status_type', 'status_abbreviation', 'hand_type', 'hand_abbreviation', 'hand_display_value', 'draft_display_text', 'draft_round', 'draft_year', 'draft_selection', 'player_id', 'starter', 'valid', 'did_not_play', 'display_name', 'ejected', 'athlete_href', 'position_href', 'statistics_href', 'team_id', 'team_guid', 'team_uid', 'team_slug', 'team_location', 'team_name', 'team_nickname', 'team_abbreviation', 'team_display_name', 'team_short_display_name', 'team_color', 'team_alternate_color', 'is_active', 'is_all_star', 'team_alternate_ids_sdr', 'logo_href', 'logo_dark_href', 'game_id'

**Example**

```python
from sportsdataverse.nfl import espn_nfl_game_rosters
rosters = espn_nfl_game_rosters(game_id=401220403)
rosters.shape

Pandas round-trip with home/away split::

rosters_pd = espn_nfl_game_rosters(game_id=401220403, return_as_pandas=True)
home = rosters_pd[rosters_pd["home_away"] == "home"]
away = rosters_pd[rosters_pd["home_away"] == "away"]
```

### `espn_nfl_player_stats(athlete_id: 'int', season: 'int', *, season_type: 'str' = 'regular', total: 'bool' = False, raw: 'bool' = False, return_as_pandas: 'bool' = False, **kwargs: 'Any') -> 'pl.DataFrame | pd.DataFrame | dict[str, Any]'`

Pull an NFL athlete's ESPN **season** stat line as one wide row.

See :func:`sportsdataverse.wbb.espn_wbb_player_stats` for full documentation of the wide return shape, the ``{category}_{stat}`` stat columns (for football: ``passing_*``, ``rushing_*``, ``receiving_*``, ``scoring_*``, ...), the athlete / team metadata blocks, and the ``season_type`` / ``total`` parameters. For the richer multi-category web-v3 payload use :func:`sportsdataverse.nfl.espn_nfl_player_stats_v3`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `athlete_id` | `int` |  | ESPN NFL athlete identifier (e.g. ``3139477`` for Patrick Mahomes). |
| `season` | `int` |  | Season year, used in the core-v2 path. |
| `season_type` | `str` | `'regular'` | ``"regular"`` (type 2) or ``"postseason"`` (type 3). |
| `total` | `bool` | `False` | Forward-compat totals passthrough. |
| `raw` | `bool` | `False` | If True, returns the raw core-v2 statistics JSON dict. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas DataFrame; else polars. |

**Returns**

A single-row wide DataFrame (polars by default). When ``raw=True`` returns the raw statistics JSON ``dict``.

**Example**

```python
from sportsdataverse.nfl import espn_nfl_player_stats
df = espn_nfl_player_stats(athlete_id=3139477, season=2023)
df.select(["full_name", "team_display_name", "passing_passing_yards"])
```

### `espn_nfl_schedule(dates=None, week=None, season_type=None, groups=None, limit=500, return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_schedule - look up the NFL schedule for a given season

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dates` | `int` | `None` | Used to define different seasons. 2002 is the earliest available season. |
| `week` | `int` | `None` | Week of the schedule. |
| `season_type` | `int` | `None` | 2 for regular season, 3 for post-season, 4 for off-season. |
| `groups` |  | `None` |  |
| `limit` | `int` | `500` | number of records to return, default: 500. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing schedule dates for the requested season. Returns None if no games

**Example**

```python
from sportsdataverse.nfl import espn_nfl_schedule
sched = espn_nfl_schedule(dates=20240908)

Specific week of regular season (``season_type=2``)::

wk1 = espn_nfl_schedule(dates=2024, week=1, season_type=2)

Pandas round-trip::

sched_pd = espn_nfl_schedule(dates=20240908, return_as_pandas=True)
```

## Dataset loaders

### `load_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

Filter by draft year and position::

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

Pandas round-trip with sort by APY::

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Depth Chart data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

Multi-season range::

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

Filter to a single year and round::

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

Pass play-by-play opportunity stats::

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

Rush play-by-play opportunity stats with pinned model version::

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

Filter to active QBs::

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of ``"draft"`` (current draft rankings), ``"week"`` (weekly rankings), or ``"all"`` (full historical rankings). Defaults to ``"draft"``. Kept for nflreadpy parity since its parameter is also called ``type``; the forward-going preferred name is ``kind``. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as ``type``. If both are supplied, ``kind`` wins. If neither is supplied, defaults to ``"draft"`` via ``type``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

Weekly rankings::

weekly = load_nfl_ff_rankings(kind="week")

Full historical rankings (parquet)::

history = load_nfl_ff_rankings(kind="all")

nflreadpy-parity ``type=`` parameter (still supported)::

draft = load_nfl_ff_rankings(type="draft")
```

### `load_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL FTN charting data going back to 2022

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

Multi-season range::

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

Filter to plays with motion::

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL injuries data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

Multi-season range with team filter::

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats accessors. Mirrors the API surface of nflreadpy's ``load_nextgen_stats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — ``seasons`` is applied as a post-filter on the ``season`` column. |
| `stat_type` | `str` | `'passing'` | One of ``"passing"``, ``"rushing"``, ``"receiving"``. Defaults to ``"passing"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested ``stat_type`` and ``seasons``.

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

Rushing NextGen stats::

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

Receiving NextGen stats with a follow-up filter::

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

Pandas round-trip::

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_combine(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Combine information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL combine data available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_combine
combine = load_nfl_combine()
combine.shape

Filter by draft year and position::

import polars as pl
qbs_2024 = (
    load_nfl_combine()
    .filter((pl.col("season") == 2024) & (pl.col("pos") == "QB"))
)
```

### `load_nfl_contracts(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Historical contracts information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing historical contracts available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_contracts
contracts = load_nfl_contracts()
contracts.shape

Pandas round-trip with sort by APY::

contracts_pd = load_nfl_contracts(return_as_pandas=True)
contracts_pd.sort_values("apy", ascending=False).head()
```

### `load_nfl_depth_charts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Depth Chart data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2001 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing depth chart data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_depth_charts
depth = load_nfl_depth_charts(seasons=[2024])

Multi-season range::

depth = load_nfl_depth_charts(seasons=range(2020, 2025))
```

### `load_nfl_draft_picks(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Draft picks information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL Draft picks data available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_draft_picks
picks = load_nfl_draft_picks()
picks.shape

Filter to a single year and round::

import polars as pl
r1_2024 = (
    load_nfl_draft_picks()
    .filter((pl.col("season") == 2024) & (pl.col("round") == 1))
)
```

### `load_nfl_ff_opportunity(seasons: 'List[int]', stat_type: 'str' = 'weekly', model_version: 'str' = 'latest', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL fantasy football opportunity data from ffverse/ffopportunity

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2006 is the earliest available season. |
| `stat_type` | `str` | `'weekly'` | One of "weekly", "pbp_pass", "pbp_rush". Defaults to "weekly". |
| `model_version` | `str` | `'latest'` | One of "latest", "v1.0.0". Defaults to "latest". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football opportunity data for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_opportunity
weekly = load_nfl_ff_opportunity(seasons=[2024])

Pass play-by-play opportunity stats::

pbp_pass = load_nfl_ff_opportunity(seasons=[2024], stat_type="pbp_pass")

Rush play-by-play opportunity stats with pinned model version::

pbp_rush = load_nfl_ff_opportunity(
    seasons=[2024], stat_type="pbp_rush", model_version="v1.0.0"
)
```

### `load_nfl_ff_playerids(return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football player IDs from DynastyProcess.com

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football player ID mappings across platforms.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_playerids
ids = load_nfl_ff_playerids()
ids.shape

Filter to active QBs::

import polars as pl
qbs = (
    load_nfl_ff_playerids()
    .filter((pl.col("position") == "QB") & (pl.col("status") == "ACT"))
)
```

### `load_nfl_ff_rankings(type: 'str' = 'draft', kind: 'str' = None, return_as_pandas=False) -> 'pl.DataFrame'`

Load fantasy football rankings and projections

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `type` | `str` | `'draft'` | Type of rankings to load. One of ``"draft"`` (current draft rankings), ``"week"`` (weekly rankings), or ``"all"`` (full historical rankings). Defaults to ``"draft"``. Kept for nflreadpy parity since its parameter is also called ``type``; the forward-going preferred name is ``kind``. |
| `kind` | `str` | `None` | Preferred parameter name. Same semantics and allowed values as ``type``. If both are supplied, ``kind`` wins. If neither is supplied, defaults to ``"draft"`` via ``type``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing fantasy football rankings data.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ff_rankings
draft = load_nfl_ff_rankings(kind="draft")

Weekly rankings::

weekly = load_nfl_ff_rankings(kind="week")

Full historical rankings (parquet)::

history = load_nfl_ff_rankings(kind="all")

nflreadpy-parity ``type=`` parameter (still supported)::

draft = load_nfl_ff_rankings(type="draft")
```

### `load_nfl_ftn_charting(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL FTN charting data going back to 2022

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2022 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing FTN charting data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_ftn_charting
charting = load_nfl_ftn_charting(seasons=[2024])

Multi-season range::

charting = load_nfl_ftn_charting(seasons=range(2022, 2025))

Filter to plays with motion::

import polars as pl
motion_plays = (
    load_nfl_ftn_charting(seasons=[2024])
    .filter(pl.col("is_motion") == 1)
)
```

### `load_nfl_injuries(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL injuries data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2009 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing injuries data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_injuries
injuries = load_nfl_injuries(seasons=[2024])

Multi-season range with team filter::

import polars as pl
sf_injuries = (
    load_nfl_injuries(seasons=range(2020, 2025))
    .filter(pl.col("team") == "SF")
)
```

### `load_nfl_nextgen_stats(seasons: 'List[int]', stat_type: 'str' = 'passing', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load NFL NextGen Stats data going back to 2016.

Unified loader that consolidates the per-stat-type NextGen Stats accessors. Mirrors the API surface of nflreadpy's ``load_nextgen_stats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to filter to. The upstream parquet covers a single combined file per stat type — ``seasons`` is applied as a post-filter on the ``season`` column. |
| `stat_type` | `str` | `'passing'` | One of ``"passing"``, ``"rushing"``, ``"receiving"``. Defaults to ``"passing"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NextGen Stats data for the requested ``stat_type`` and ``seasons``.

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs_pass = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")

Rushing NextGen stats::

ngs_rush = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")

Receiving NextGen stats with a follow-up filter::

import polars as pl
ngs_rec = (
    load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
    .filter(pl.col("week") > 0)
)

Pandas round-trip::

ngs_pd = load_nfl_nextgen_stats(
    seasons=[2024], stat_type="passing", return_as_pandas=True
)
```

### `load_nfl_ngs_passing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='passing')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_nextgen_stats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="passing")
```

### `load_nfl_ngs_receiving(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='receiving')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_nextgen_stats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="receiving")
```

### `load_nfl_ngs_rushing(seasons: 'List[int]' = None, return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_nextgen_stats(stat_type='rushing')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_nextgen_stats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` | `None` |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_nextgen_stats
ngs = load_nfl_nextgen_stats(seasons=[2024], stat_type="rushing")
```

### `load_nfl_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

Pandas round-trip::

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_nfl_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play by play data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the play-by-plays available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

Multi-season range::

pbp = load_nfl_pbp(seasons=range(2020, 2025))

With cache off (development workflow)::

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

Pandas round-trip::

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### `load_nfl_pbp_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

Multi-season range::

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_nfl_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level PFR advstats accessors. Mirrors the API surface of nflreadpy's ``load_pfr_advstats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For ``summary_level='week'`` this drives the per-season parquet fan-out; for ``summary_level='season'`` it post-filters the combined parquet by the ``season`` column. |
| `stat_type` | `str` | `'pass'` | One of ``"pass"``, ``"rush"``, ``"rec"``, ``"def"``. Defaults to ``"pass"``. |
| `summary_level` | `str` | `'week'` | One of ``"week"`` or ``"season"``. Defaults to ``"week"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested ``stat_type``, ``summary_level``, and ``seasons``.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

Season-level rushing summaries (one row per player per season)::

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

Defensive stats with a follow-up filter::

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

Pandas round-trip::

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_nfl_pfr_def(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="season"
)
```

### `load_nfl_pfr_pass(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="season"
)
```

### `load_nfl_pfr_rec(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="season"
)
```

### `load_nfl_pfr_rush(return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='season')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)
```

### `load_nfl_pfr_weekly_def(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='def', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="def", summary_level="week"
)
```

### `load_nfl_pfr_weekly_pass(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='pass', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)
```

### `load_nfl_pfr_weekly_rec(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rec', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rec", summary_level="week"
)
```

### `load_nfl_pfr_weekly_rush(seasons: 'List[int]', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Deprecated alias for ``load_nfl_pfr_advstats(stat_type='rush', summary_level='week')``.

Will be removed in a future release. Migrate callers to the unified ``load_nfl_pfr_advstats`` function.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `List[int]` |  |  |
| `return_as_pandas` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
df = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="week"
)
```

### `load_nfl_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL player stats data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing player stats.

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

Kicking-only stats::

kicking = load_nfl_player_stats(kicking=True)

Filter to a single season after load::

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_nfl_players(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Player ID information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing players available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape

Pandas round-trip::

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

### `load_nfl_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL roster data for all seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1920 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing rosters available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

Multi-season range::

rosters = load_nfl_rosters(seasons=range(2020, 2025))

Filter to a single team::

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

### `load_nfl_schedule(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

Multi-season range::

schedule = load_nfl_schedule(seasons=range(2020, 2025))

Filter to a single week::

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

Pandas round-trip::

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_nfl_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL snap counts data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2012 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing snap counts available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

Multi-season range with offense-only filter::

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_nfl_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

Regular-season-only team stats::

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

Combined regular + post-season at season grain::

combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

### `load_nfl_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

Pandas round-trip::

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_nfl_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

Filter to a single season::

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

### `load_nfl_weekly_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL weekly roster data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2002 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing weekly rosters available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

Multi-season range with a follow-up week filter::

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_officials(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Officials information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing officials available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_officials
officials = load_nfl_officials()
officials.shape

Pandas round-trip::

officials_pd = load_nfl_officials(return_as_pandas=True)
officials_pd.head()
```

### `load_participation(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play-by-play participation data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2016 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing play-by-play participation data available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp_participation
participation = load_nfl_pbp_participation(seasons=[2022])

Multi-season range::

participation = load_nfl_pbp_participation(seasons=range(2018, 2023))
```

### `load_pbp(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL play by play data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the play-by-plays available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
pbp = load_nfl_pbp(seasons=[2024])
print(pbp.shape)

Multi-season range::

pbp = load_nfl_pbp(seasons=range(2020, 2025))

With cache off (development workflow)::

from sportsdataverse.nfl import load_nfl_pbp, update_config
update_config(cache_mode="off")
pbp = load_nfl_pbp(seasons=[2024])

Pandas round-trip::

pbp_pd = load_nfl_pbp(seasons=[2024], return_as_pandas=True)
pbp_pd.head()
```

### `load_pfr_advstats(seasons: 'List[int]', stat_type: 'str' = 'pass', summary_level: 'str' = 'week', return_as_pandas: 'bool' = False) -> 'pl.DataFrame'`

Load Pro-Football Reference advanced statistics going back to 2018.

Unified loader that consolidates the per-stat-type / per-summary-level PFR advstats accessors. Mirrors the API surface of nflreadpy's ``load_pfr_advstats`` so downstream code can swap engines without changing call sites.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list[int]` |  | Seasons to load. For ``summary_level='week'`` this drives the per-season parquet fan-out; for ``summary_level='season'`` it post-filters the combined parquet by the ``season`` column. |
| `stat_type` | `str` | `'pass'` | One of ``"pass"``, ``"rush"``, ``"rec"``, ``"def"``. Defaults to ``"pass"``. |
| `summary_level` | `str` | `'week'` | One of ``"week"`` or ``"season"``. Defaults to ``"week"``. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing PFR advanced stats data for the requested ``stat_type``, ``summary_level``, and ``seasons``.

**Example**

```python
from sportsdataverse.nfl import load_nfl_pfr_advstats
pass_week = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="pass", summary_level="week"
)

Season-level rushing summaries (one row per player per season)::

rush_season = load_nfl_pfr_advstats(
    seasons=[2024], stat_type="rush", summary_level="season"
)

Defensive stats with a follow-up filter::

import polars as pl
def_week = (
    load_nfl_pfr_advstats(seasons=[2024], stat_type="def", summary_level="week")
    .filter(pl.col("week") <= 8)
)

Pandas round-trip::

rec_pd = load_nfl_pfr_advstats(
    seasons=[2024],
    stat_type="rec",
    summary_level="season",
    return_as_pandas=True,
)
```

### `load_player_stats(kicking=False, return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL player stats data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `kicking` | `bool` | `False` | If True, load kicking stats. If False, load all other stats. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing player stats.

**Example**

```python
from sportsdataverse.nfl import load_nfl_player_stats
stats = load_nfl_player_stats()
stats.shape

Kicking-only stats::

kicking = load_nfl_player_stats(kicking=True)

Filter to a single season after load::

import polars as pl
stats_2024 = load_nfl_player_stats().filter(pl.col("season") == 2024)
```

### `load_players(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL Player ID information

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing players available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_players
players = load_nfl_players()
players.shape

Pandas round-trip::

players_pd = load_nfl_players(return_as_pandas=True)
players_pd.head()
```

### `load_rosters(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL roster data for all seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1920 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing rosters available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_rosters
rosters = load_nfl_rosters(seasons=[2024])

Multi-season range::

rosters = load_nfl_rosters(seasons=range(2020, 2025))

Filter to a single team::

import polars as pl
kc = load_nfl_rosters(seasons=[2024]).filter(pl.col("team") == "KC")
```

### `load_rosters_weekly(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL weekly roster data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2002 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing weekly rosters available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_weekly_rosters
weekly = load_nfl_weekly_rosters(seasons=[2024])

Multi-season range with a follow-up week filter::

import polars as pl
wk1 = (
    load_nfl_weekly_rosters(seasons=range(2022, 2025))
    .filter(pl.col("week") == 1)
)
```

### `load_schedules(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL schedule data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing the schedule for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[2024])
schedule.shape

Multi-season range::

schedule = load_nfl_schedule(seasons=range(2020, 2025))

Filter to a single week::

import polars as pl
week_one = load_nfl_schedule(seasons=[2024]).filter(pl.col("week") == 1)

Pandas round-trip::

schedule_pd = load_nfl_schedule(seasons=[2024], return_as_pandas=True)
schedule_pd[["game_id", "home_team", "away_team", "week"]].head()
```

### `load_snap_counts(seasons: 'List[int]', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL snap counts data for selected seasons

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 2012 is the earliest available season. |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing snap counts available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_snap_counts
snaps = load_nfl_snap_counts(seasons=[2024])

Multi-season range with offense-only filter::

import polars as pl
offense = (
    load_nfl_snap_counts(seasons=range(2022, 2025))
    .filter(pl.col("offense_snaps") > 0)
)
```

### `load_team_stats(seasons: 'List[int]', summary_level: 'str' = 'week', return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team stats data going back to 1999

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `seasons` | `list` |  | Used to define different seasons. 1999 is the earliest available season. |
| `summary_level` | `str` | `'week'` | Aggregation level. One of "week", "reg", "post", "reg+post". Defaults to "week". |
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing team stats available for the requested seasons.

**Example**

```python
from sportsdataverse.nfl import load_nfl_team_stats
weekly = load_nfl_team_stats(seasons=[2024])

Regular-season-only team stats::

reg = load_nfl_team_stats(seasons=[2024], summary_level="reg")

Combined regular + post-season at season grain::

combined = load_nfl_team_stats(seasons=[2023, 2024], summary_level="reg+post")
```

### `load_teams(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL team ID information and logos

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams available.

**Example**

```python
from sportsdataverse.nfl import load_nfl_teams
teams = load_nfl_teams()
teams.shape

Pandas round-trip::

teams_pd = load_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbr", "team_name", "team_conf", "team_division"]].head()
```

### `load_trades(return_as_pandas=False) -> 'pl.DataFrame'`

Load NFL trades data

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing NFL trade information.

**Example**

```python
from sportsdataverse.nfl import load_nfl_trades
trades = load_nfl_trades()
trades.shape

Filter to a single season::

import polars as pl
trades_2024 = load_nfl_trades().filter(pl.col("season") == 2024)
```

## Utilities & helpers

### `NFLPlayProcess(gameId=0, raw=False, path_to_json='/', return_keys=None, **kwargs)`

Process ESPN NFL play-by-play feeds into a tidy game-level dictionary.

Wraps the ESPN ``summary`` endpoint (or a local JSON dump) and pipes the result through a chain of feature-engineering steps -- down/distance, play-type flags, EPA, WPA, QBR, drive aggregation, and an advanced box score. Use ``run_processing_pipeline()`` for the full feature set or ``run_cleaning_pipeline()`` for a lighter clean.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `gameId` | `int` | `0` | ESPN ``event`` id (e.g. ``401671801``). |
| `raw` | `bool` | `False` | If ``True``, ``espn_nfl_pbp()`` returns the ESPN payload untouched. If ``False`` (default), it normalizes keys. |
| `path_to_json` | `str` | `'/'` | Directory containing ``{gameId}.json`` for the ``nfl_pbp_disk()`` flow (offline replay). |
| `return_keys` | `list[str] \| None` | `None` | If supplied, ``run_processing_pipeline`` returns only the listed keys from the result dict. |

**Example**

```python
from sportsdataverse.nfl import NFLPlayProcess
proc = NFLPlayProcess(gameId=401671801)
proc.espn_nfl_pbp()
result = proc.run_processing_pipeline()
len(result["plays"])

Offline replay from a JSON dump::

proc = NFLPlayProcess(gameId=401671801, path_to_json="./pbp_dump")
proc.nfl_pbp_disk()
cleaned = proc.run_cleaning_pipeline()

Subset the return payload::

proc = NFLPlayProcess(gameId=401671801, return_keys=["plays", "boxscore"])
proc.espn_nfl_pbp()
slim = proc.run_processing_pipeline()
sorted(slim.keys())  # ['boxscore', 'plays']
```

### `get_current_nfl_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

Roster-year semantics (March 15 cutover)::

roster_year = get_current_nfl_season(roster=True)

Pair with a loader to fetch only the active season::

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_nfl_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

Schedule-driven week (hits the live schedule parquet)::

week_live = get_current_nfl_week(use_date=False)

Roster-year season inference::

week_roster = get_current_nfl_week(roster=True)

Pair with a PBP fetch to grab only the most recent season+week::

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `get_current_season(roster: 'bool' = False) -> 'int'`

Return the current NFL season year.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` | If True, use roster-year logic (current calendar year on/after March 15, otherwise previous year). If False, use season logic (current calendar year on/after the Thursday following Labor Day, otherwise previous year). |

**Returns**

The current season (or roster) year.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_season
season = get_current_nfl_season()
print(season)

Roster-year semantics (March 15 cutover)::

roster_year = get_current_nfl_season(roster=True)

Pair with a loader to fetch only the active season::

from sportsdataverse.nfl import load_nfl_schedule
schedule = load_nfl_schedule(seasons=[get_current_nfl_season()])
```

### `get_current_week(use_date: 'bool' = True, roster: 'bool' = False) -> 'int'`

Return the current NFL week (1-22).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `use_date` | `bool` | `True` | If True (default), compute the week purely from the calendar (number of weeks since the first Thursday of September of the current season). If False, hit the live schedule via `load_nfl_schedule()` and return the week of the next unplayed game (matches nflreadpy's `use_date=False` path). |
| `roster` | `bool` | `False` | Forwarded to `get_current_nfl_season()` for season inference. |

**Returns**

The current week, capped at 22.

**Example**

```python
from sportsdataverse.nfl import get_current_nfl_week
week = get_current_nfl_week()

Schedule-driven week (hits the live schedule parquet)::

week_live = get_current_nfl_week(use_date=False)

Roster-year season inference::

week_roster = get_current_nfl_week(roster=True)

Pair with a PBP fetch to grab only the most recent season+week::

import polars as pl
from sportsdataverse.nfl import (
    get_current_nfl_season, get_current_nfl_week, load_nfl_pbp,
)
current_pbp = (
    load_nfl_pbp(seasons=[get_current_nfl_season()])
    .filter(pl.col("week") == get_current_nfl_week())
)
```

### `most_recent_nfl_season(roster: 'bool' = False) -> 'int'`

Alias for `get_current_nfl_season()` mirroring nflreadr's

`most_recent_season()`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `roster` | `bool` | `False` |  |

**Example**

```python
from sportsdataverse.nfl.utils_date import most_recent_nfl_season
season = most_recent_nfl_season()

Roster-year flavor::

roster_year = most_recent_nfl_season(roster=True)
```

## Other

### `NflConfig(cache_mode: 'CacheMode' = 'memory', cache_dir: 'Optional[Path]' = None, cache_duration: 'int' = 86400, verbose: 'bool' = True, timeout: 'int' = 30, user_agent: 'str' = 'sportsdataverse-py-nfl') -> None`

Runtime configuration for sdv-py NFL loaders.

Fields mirror nflreadpy's ``NflreadpyConfig`` so users can swap engines without changing call sites. The defaults are conservative: in-memory caching with a 24-hour TTL, verbose progress bars on, 30-second HTTP timeout.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache_mode` | `CacheMode` | `'memory'` |  |
| `cache_dir` | `Optional[Path]` | `None` |  |
| `cache_duration` | `int` | `86400` |  |
| `verbose` | `bool` | `True` |  |
| `timeout` | `int` | `30` |  |
| `user_agent` | `str` | `'sportsdataverse-py-nfl'` |  |

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()  # NflConfig instance
cfg.cache_mode      # "memory"
cfg.cache_duration  # 86400 (24h)
cfg.timeout         # 30 (seconds)

Construct a fresh instance directly (rarely needed -- prefer
``update_config``)::

from sportsdataverse.nfl import NflConfig
cfg = NflConfig(cache_mode="off", timeout=10)
```

### `cached_loader(func: 'F') -> 'F'`

Decorator that adds caching to a ``load_nfl_*`` function.

Honors the active ``NflConfig.cache_mode``: - ``memory``: dict-based per-process cache. - ``filesystem``: parquet-based cross-process cache under ``cache_dir``. - ``off``: no caching, function runs every time. The cache key is the hash of ``(qualified_name, args, kwargs)`` with ``return_as_pandas`` excluded so memory / disk hits work regardless of which return shape the caller asked for. The cache always stores the polars frame internally and converts to pandas on read when requested.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `func` | `F` |  |  |

**Example**

```python
import polars as pl
from sportsdataverse.nfl.cache import cached_loader

@cached_loader
def load_my_thing(season: int, return_as_pandas: bool = False):
    # ... fetch parquet, build a polars frame ...
    return pl.DataFrame({"season": [season]})

df1 = load_my_thing(2024)            # network hit, populates cache
df2 = load_my_thing(2024)            # served from cache
df_pd = load_my_thing(2024, return_as_pandas=True)
# `return_as_pandas` is excluded from the cache key, so the
# polars hit is reused and converted to pandas on the way out.

Switch caching modes at runtime::

from sportsdataverse.nfl import clear_cache, update_config

update_config(cache_mode="filesystem")  # parquet-on-disk reuse
df3 = load_my_thing(2024)               # writes parquet under cache_dir
clear_cache()                           # wipe both memory + filesystem
update_config(cache_mode="off")         # bypass cache entirely
```

### `clear_cache() -> 'None'`

Clear both memory and filesystem caches.

Memory: empties the in-process dict. Filesystem: removes all entries under ``config.cache_dir``. The directory itself is preserved so subsequent writes succeed without needing ``mkdir``.

**Example**

```python
from sportsdataverse.nfl import clear_cache, load_nfl_pbp
clear_cache()
pbp = load_nfl_pbp(seasons=[2024])

Pair with a cache-mode switch::

from sportsdataverse.nfl import clear_cache, update_config
update_config(cache_mode="filesystem")
# ... lots of cached calls accumulate parquet files on disk ...
clear_cache()  # wipe disk + memory together
```

### `espn_nfl_teams(return_as_pandas=False, **kwargs) -> 'pl.DataFrame'`

espn_nfl_teams - look up NFL teams

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | If True, returns a pandas dataframe. If False, returns a polars dataframe. |

**Returns**

Polars dataframe containing teams for the requested league. This function caches by default, so if you want to refresh the data, use the command sportsdataverse.nfl.espn_nfl_teams.clear_cache().

**Example**

```python
from sportsdataverse.nfl import espn_nfl_teams
teams = espn_nfl_teams()
teams.shape

Pandas round-trip::

teams_pd = espn_nfl_teams(return_as_pandas=True)
teams_pd[["team_abbreviation", "team_display_name"]].head()

Force a refresh after upstream ESPN updates::

espn_nfl_teams.cache_clear()  # underlying lru_cache
teams = espn_nfl_teams()
```

### `get_config() -> 'NflConfig'`

Return the live ``NflConfig`` singleton.

The same object is returned on every call; mutate via ``update_config`` rather than reassigning fields directly so future hooks (e.g. logging on config change) have a single choke point.

**Example**

```python
from sportsdataverse.nfl import get_config
cfg = get_config()
print(cfg.cache_mode, cfg.cache_duration, cfg.cache_dir)

Pair with ``update_config`` to verify a change took effect::

from sportsdataverse.nfl import update_config, get_config
update_config(cache_mode="off")
assert get_config().cache_mode == "off"
```

### `nfl_game_details(game_id=None, headers=None, raw=False) -> 'Dict'`

nfl_game_details() -- pull full ``api.nfl.com`` game details by game id.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `str` | `None` | UUID-style game id from ``api.nfl.com`` (e.g. ``'7ae87c4c-d24c-11ec-b23d-d15a91047884'``). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict (skip the auth roundtrip). Defaults to a fresh ``nfl_headers_gen()`` call. |
| `raw` | `bool` | `False` | If True, return the ESPN payload untouched. If False (default), normalize keys to the expected schema (filling missing keys with empty dicts/lists). |

**Returns**

Dictionary of game details (drives, plays, scoring summaries, timeouts, weather, attendance, etc.).

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_details
details = nfl_game_details(game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884")
sorted(details.keys())[:5]

Reuse headers across many calls (avoids re-minting tokens)::

from sportsdataverse.nfl.nfl_games import nfl_game_details, nfl_headers_gen
hdrs = nfl_headers_gen()
details = nfl_game_details(
    game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", headers=hdrs
)

Raw passthrough::

raw = nfl_game_details(
    game_id="7ae87c4c-d24c-11ec-b23d-d15a91047884", raw=True
)
```

### `nfl_game_schedule(season=2021, season_type='REG', week=1, headers=None, raw=False) -> 'Dict'`

nfl_game_schedule() -- list ``api.nfl.com`` games for a season/week slice.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `int` | `2021` | season year (e.g. ``2024``). |
| `season_type` | `str` | `'REG'` | season type. One of ``"REG"`` or ``"POST"``. |
| `week` | `int` | `1` | week number (1-18 regular season, 1-4 post-season). |
| `headers` | `Dict[str, str] \| None` | `None` | Pre-built header dict. Defaults to a fresh ``nfl_headers_gen()`` call. |
| `raw` | `bool` | `False` | Currently ignored -- the function always returns the raw NFL.com summary payload. |

**Returns**

Dictionary with the games list under ``"games"`` plus pagination metadata.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_game_schedule
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1)

Wild Card weekend (post-season)::

wild_card = nfl_game_schedule(season=2023, season_type="POST", week=1)

Reuse headers across many calls::

from sportsdataverse.nfl.nfl_games import nfl_game_schedule, nfl_headers_gen
hdrs = nfl_headers_gen()
for week in range(1, 19):
    summary = nfl_game_schedule(
        season=2024, season_type="REG", week=week, headers=hdrs,
    )
```

### `nfl_headers_gen()`

Build the full request-header dict expected by ``api.nfl.com``.

Mints a fresh bearer token via :func:`nfl_token_gen` and combines it with the browser-style headers (``Origin``, ``Referer``, ``User-Agent``, ``Sec-Fetch-*``, etc.) the NFL.com web app sends on every request.

**Returns**

Header dict ready to drop into ``requests.get``.

**Example**

```python
from sportsdataverse.nfl.nfl_games import (
    nfl_headers_gen, nfl_game_schedule,
)
hdrs = nfl_headers_gen()
week_one = nfl_game_schedule(season=2024, season_type="REG", week=1, headers=hdrs)
week_two = nfl_game_schedule(season=2024, season_type="REG", week=2, headers=hdrs)
```

### `nfl_token_gen()`

Mint a fresh ``api.nfl.com`` access token via the public reroute endpoint.

Wraps the unauthenticated ``client_credentials`` grant the NFL.com web app uses. The returned bearer token is what ``nfl_headers_gen()`` puts on the ``Authorization`` header.

**Returns**

The access token string.

**Example**

```python
from sportsdataverse.nfl.nfl_games import nfl_token_gen
token = nfl_token_gen()
assert isinstance(token, str)

Pair with a downstream call (``nfl_headers_gen`` does this for you)::

import requests
token = nfl_token_gen()
headers = {"Authorization": f"Bearer {token}"}
```

### `reset_config() -> 'NflConfig'`

Reset the active config to its env-var-derived defaults.

Convenience for tests / interactive sessions that want to undo a chain of ``update_config()`` calls without restarting the interpreter.

**Example**

```python
from sportsdataverse.nfl import update_config, reset_config
update_config(cache_mode="off", timeout=5)
# ... do work ...
reset_config()  # back to env-derived defaults
```

### `scoreboard_event_parsing(event)`

Normalize one ESPN scoreboard ``event`` into a flatter shape.

Splits the competitors list into ``home`` / ``away`` siblings, hoists notes / broadcast metadata onto the competition root, and drops the fields the schedule helper does not need (``odds``, ``leaders``, ``geoBroadcasts``, etc.). Used internally by :func:`espn_nfl_schedule`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event` | `Dict` |  | A single ``events[i]`` dict from the ESPN scoreboard endpoint. |

**Returns**

The mutated event dict with normalized ``home`` / ``away`` / broadcast keys.

**Example**

```python
from sportsdataverse.dl_utils import download
from sportsdataverse.nfl.nfl_schedule import scoreboard_event_parsing
url = "http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
payload = download(url=url).json()
for ev in payload.get("events", []):
    scoreboard_event_parsing(ev)
    ev["competitions"][0]["home"]["abbreviation"]
```

### `update_config(**kwargs: 'object') -> 'NflConfig'`

Update the active config in place.

Pass keyword arguments matching ``NflConfig`` fields:: update_config(cache_mode="filesystem", cache_duration=3600) String values for ``cache_dir`` are coerced to ``pathlib.Path`` and ``~`` is expanded for convenience.

**Returns**

The (mutated) global config object, for chaining or inspection.

**Example**

```python
from sportsdataverse.nfl import update_config
update_config(cache_mode="filesystem", cache_duration=3600)

Disable caching for development::

update_config(cache_mode="off")

Point cache at a custom directory::

update_config(cache_dir="~/sdv-cache")
```
