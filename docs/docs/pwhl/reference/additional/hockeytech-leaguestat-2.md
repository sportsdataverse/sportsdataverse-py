---
title: "PWHL — additional Python functions — HockeyTech / LeagueStat: unit_ratings"
sidebar_label: "HockeyTech / LeagueStat: unit_ratings"
sidebar_position: 3
description: "PWHL — additional Python functions — HockeyTech / LeagueStat: unit_ratings — function reference in sdv-py, the SportsDataverse Python package."
---
# PWHL — additional Python functions — HockeyTech / LeagueStat: unit_ratings

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

**Example**

```python
from sportsdataverse.pwhl import load_pwhl_pbp, load_pwhl_shifts
from sportsdataverse.pwhl.pwhl_player_impact import pwhl_unit_ratings
units = pwhl_unit_ratings(load_pwhl_pbp(2025), load_pwhl_shifts(2025))
```
