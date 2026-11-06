# PWHL — additional Python functions — HockeyTech / LeagueStat: teams–unit_ratings

> PWHL — additional Python functions — HockeyTech / LeagueStat: teams–unit_ratings — function reference in sdv-py, the SportsDataverse Python package.

### pwhl_teams {#pwhl_teams}

`pwhl_teams(season: 'Optional[int]' = None, season_id: 'Optional[int]' = None, return_as_pandas: 'bool' = False) -> 'Any'`

PWHL teams for a given season.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `season` | `Optional[int]` | `None` | Season as an END year (2026 = the 2025-26 season). Defaults to the newest regular season when neither `season` nor `season_id` is given. |
| `season_id` | `Optional[int]` | `None` | The HockeyTech season id, when it is already known. |
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per team: `team_id`, `team_name`, `team_code`, `team_nickname`, `team_label`, `division` and `team_logo`; `team_id` is Int64, the rest String. A pandas DataFrame when `return_as_pandas` is True.

| col_name | type | description |
|---|---|---|
| `team_name` | String | Team name. |
| `team_id` | Int64 | Unique team identifier. |
| `team_code` | String | Team abbreviation. |
| `team_nickname` | String | Team nickname. |
| `team_label` | String | Short city label. |
| `division` | String | Division identifier. |
| `team_logo` | String | URL to the team logo image. |

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_teams
df = pwhl_teams(season=2025)

# Next step

df.select("team_id", "team_name", "team_code")
```

### pwhl_transactions {#pwhl_transactions}

`pwhl_transactions(return_as_pandas: 'bool' = False) -> 'Any'`

PWHL roster transactions.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `return_as_pandas` | `bool` | `False` | Return a pandas DataFrame instead of polars. |

**Returns**

One row per transaction on the feed's current page (the newest 20): `transaction_date` / `transaction_time`, `transaction_type` / `ttype_text`, `title`, `detail`, `player_id` / `player_name` / `position`, and `team_id` / `team_name` / `team_code` / `team_city` (`player_id` / `team_id` Int64, the rest String). A pandas DataFrame when `return_as_pandas` is True.

**Raises**

| Exception | Description |
|---|---|
| `NoDataError` | The feed answered 404. |
| `AssetFetchError` | The fetch failed: a non-2xx status, an empty or unparseable body, or an `Undefined Tab` / `InvalidView` sentinel. |

**Example**

```python
from sportsdataverse.pwhl import pwhl_transactions
df = pwhl_transactions()

# Next step

df.head()
```

### pwhl_unit_ratings {#pwhl_unit_ratings}

`pwhl_unit_ratings(pbp: 'pl.DataFrame', shifts: 'pl.DataFrame', *, model_dir: "'str | None'" = None, xg_model: 'Any' = None, **kwargs: 'Any') -> "'pl.DataFrame | pd.DataFrame'"`

⑤ PWHL line/pair ratings -- shim over `nhl_unit_ratings` with `league='pwhl'`.

Guards on PWHL shift-chart coverage (see the module docstring).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | a `load_pwhl_pbp` frame (adapted, see the module docstring) or a frame already shaped like `load_nhl_pbp_full`. |
| `shifts` | `DataFrame` |  | the matching `load_pwhl_shifts` frame, or one shaped like `load_nhl_shifts`. |
| `model_dir` | `str \| None` | `None` | booster directory for NHL-shaped input (the borrowed NHL xG boosters). |
| `xg_model` | `Any` | `None` | a fitted `PwhlCoordXGModel` scoring HockeyTech input; `None` fits one on `pbp` (see `sportsdataverse.pwhl.pwhl_xg_proxy.pwhl_shot_xg`). That default is an in-sample fit: the goals in `pbp` shape the xG the ratings are fit to. For a backtest, pass a model fit on prior seasons only. |

**Returns**

Same schema as `nhl_unit_ratings`: `team:Utf8, unit_ids:Utf8, unit_players:Utf8, toi_minutes:Float64, on_ice_xgf:Float64, on_ice_xga:Float64, on_ice_xgf_pct:Float64, summed_rapm:Float64, unit_value:Float64`. A zero-row frame with this schema (+ a `cli_warn`) when PWHL shift coverage is insufficient.

No returns table is published for this function: no capture: since #846 it runs on the published PWHL pbp and shift releases (tested on five real 2025 games) and returns the same schema as its NHL engine, which has no capture of its own.

**Raises**

| Exception | Description |
|---|---|
| `ValueError` | A HockeyTech pbp/shift column the NHL contract needs is missing. |

**Example**

```python
from sportsdataverse.pwhl import load_pwhl_pbp, load_pwhl_shifts
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_unit_ratings
units = pwhl_unit_ratings(load_pwhl_pbp(2025), load_pwhl_shifts(2025))
```
