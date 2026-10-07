---
title: "NFL — additional Python functions — Models and calculators: opponent_adjusted–team_pressure"
sidebar_label: "Models and calculators: opponent_adjusted–team_pressure"
sidebar_position: 12
description: "NFL — additional Python functions — Models and calculators: opponent_adjusted–team_pressure — function reference in sdv-py, the SportsDataverse Python package."
---
# NFL — additional Python functions — Models and calculators: opponent_adjusted–team_pressure

### opponent_adjusted_ridge {#opponent_adjusted_ridge}

`opponent_adjusted_ridge(plays: 'pl.DataFrame', *, off_col: 'str', def_col: 'str', home_col: 'str', resp_col: 'str', lam: 'float', penalize_home: 'bool' = False, hfa_col: 'str | None' = None) -> 'tuple[pl.DataFrame, float, float]'`

Ridge-regress `resp_col` on offense + defense team indicators + HFA.

League-agnostic (column names are arguments): builds the full
offense/defense-indicator + intercept + home design and solves the
ridge normal equations `beta = (X'X + lam*R)^-1 X'y`. Only team
coefficients are penalised; the intercept (and, unless
`penalize_home`, the home term) is free. Moved (T7.2) from
`sportsdataverse.nfl.nfl_ratings`. Callers: NFL ratings, and CFB
adjusted EPA (`cfb_adjusted_epa._fit_team_strengths`, via `hfa_col`);
`cfb_ratings` uses the different `dropped_level_ridge`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | One row per play. Rows with a null `off_col` / `def_col` / `resp_col` must be filtered by the caller. |
| `off_col` | `str` |  | Column naming the offense (possession) team. |
| `def_col` | `str` |  | Column naming the defense team. |
| `home_col` | `str` |  | Column naming the home team (HFA indicator is `off_col == home_col`). |
| `resp_col` | `str` |  | Numeric response column (e.g. `epa`). |
| `lam` | `float` |  | Ridge penalty applied to the team coefficients. |
| `penalize_home` | `bool` | `False` | Also penalise the home-field coefficient (default False). |
| `hfa_col` | `str \| None` | `None` | Numeric column used as the home regressor as-is (e.g. CFB's `+1` home offense / `0` neutral site / `-1` away), in place of the `off_col == home_col` indicator, which then goes unused. Must not contain nulls (raises `ValueError`). |

**Returns**

A `(frame, intercept, home_coef)` tuple: `frame` has one row per team (`team_id` Utf8, `off_coef` / `def_coef` Float64); `intercept` is the league baseline; `home_coef` the fitted HFA in response units. Zero-row frame + `(0.0, 0.0)` on empty input.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import opponent_adjusted_ridge
frame, intercept, hfa = opponent_adjusted_ridge(
    plays, off_col="posteam", def_col="defteam",
    home_col="home_team", resp_col="epa", lam=200.0,
)
frame.sort("off_coef", descending=True).head()
```

### pressure_pairs {#pressure_pairs}

`pressure_pairs(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per (season, off_team, def_team) dropbacks + pressures (matchup grid).

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  |  |

**Returns**


| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the matchup aggregate. |
| `off_team` | character | Offense team abbreviation. |
| `def_team` | character | Defense team abbreviation. |
| `dropbacks` | integer | Offense dropbacks in the matchup. |
| `pressures` | integer | Sacks plus QB hits in the matchup. |

### special_teams_ratings {#special_teams_ratings}

`special_teams_ratings(plays: 'pl.DataFrame', *, config: 'RatingsConfig | None' = None) -> 'pl.DataFrame'`

One row per team: opponent-adjusted special-teams EPA per play.

Reuses `opponent_adjusted_ridge` (no forked solver) restricted to
`special == 1` plays with `resp_col="epa"`; `adj_st_epa` is the
`off_coef` (the special-teams unit acting as "offense" on the play).
Teams appearing anywhere in `plays` but on no special-teams play get
the documented neutral fill `adj_st_epa = 0.0`.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `plays` | `DataFrame` |  | An `load_nfl_pbp`-schema frame carrying `posteam`, `defteam`, `home_team`, `epa`, `special`. Not pre-filtered -- this function selects the ST plays itself. |
| `config` | `RatingsConfig \| None` | `None` | Tuning knobs (only `ridge_lambda` is consulted); defaults to `RatingsConfig`. |

**Returns**

One row per `team_id` (Utf8) with `adj_st_epa` (Float64). Zero-row, correctly-typed when `plays` is empty.

**Example**

```python
from sportsdataverse.nfl.nfl_ratings import special_teams_ratings
st = special_teams_ratings(pbp)
st.sort("adj_st_epa", descending=True).head()
```

### team_pressure_rates {#team_pressure_rates}

`team_pressure_rates(pbp: 'pl.DataFrame') -> 'pl.DataFrame'`

Per (season, team) raw pressure rates, both sides of the ball.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `pbp` | `DataFrame` |  | nflverse-format pbp with `season` / `posteam` / `defteam` / `qb_dropback` / `sack` / `qb_hit`. |

**Returns**

Per `(season, team)`: `dropbacks_off`, `pressures_allowed`, `pressure_rate_allowed`, `dropbacks_def`, `pressures_generated`, `pressure_rate_generated`. Empty input yields a zero-row frame.

| col_name | type | description |
|---|---|---|
| `season` | integer | Season of the aggregate. |
| `team` | character | Team abbreviation. |
| `dropbacks_off` | integer | Offensive dropbacks (qb_dropback plays). |
| `pressures_allowed` | integer | Sacks plus QB hits allowed on the team's own dropbacks. |
| `pressure_rate_allowed` | double | pressures_allowed / dropbacks_off. |
| `dropbacks_def` | integer | Opponent dropbacks faced on defense. |
| `pressures_generated` | integer | Sacks plus QB hits generated against opponent dropbacks. |
| `pressure_rate_generated` | double | pressures_generated / dropbacks_def. |

**Example**

```python
from sportsdataverse.nfl import load_nfl_pbp
from sportsdataverse.nfl.nfl_line_grades import team_pressure_rates
rates = team_pressure_rates(load_nfl_pbp([2023]))
print(rates.sort("pressure_rate_generated", descending=True).head())
```
