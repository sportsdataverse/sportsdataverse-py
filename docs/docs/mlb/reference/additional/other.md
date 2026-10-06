---
title: "MLB — additional Python functions — Other"
sidebar_label: "Other"
sidebar_position: 7
description: "MLB — additional Python functions — Other — function reference in sdv-py, the SportsDataverse Python package."
---
# MLB — additional Python functions — Other

### build_we_table {#build_we_table}

`build_we_table(states: 'pl.DataFrame', results: 'pl.DataFrame', *, laplace: 'float' = 1.0) -> 'pl.DataFrame'`

Empirical, Laplace-smoothed home win-expectancy table.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `states` | `DataFrame` |  | Output of `pbp_base_out_states`. |
| `results` | `DataFrame` |  | Game-level results with `game_id` (same dtype as `states`), `home_score`, `away_score`. |
| `laplace` | `float` | `1.0` | Additive smoothing constant (default 1.0). |

**Returns**

one row per observed state bucket. | Column | Type | Description | |---|---|---| | inning_capped | Int64 | Inning, capped at 9 | | half | Utf8 | `"top"` or `"bottom"` | | base_state | Utf8 | 3-char base occupancy | | outs_start | Int64 | Outs before the play (0-2) | | score_diff_bucket | Int64 | home - away score, clipped to [-6, 6] | | home_win_exp | Float64 | Laplace-smoothed P(home wins \| state) | | n | Int64 | Plate appearances observed in this bucket |

| col_name | type | description |
|---|---|---|
| `inning_capped` | integer | Inning number, capped at 9 (extra innings pooled with the 9th). |
| `half` | character | Half-inning ("top" or "bottom"). |
| `base_state` | character | 3-char base occupancy code. |
| `outs_start` | integer | Outs before the play (0-2). |
| `score_diff_bucket` | integer | home minus away score, clipped to [-6, 6]. |
| `home_win_exp` | double | Laplace-smoothed empirical P(home team wins \| state bucket). |
| `n` | integer | Plate appearances observed in this state bucket. |

**Example**

```python
from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
from sportsdataverse.mlb.mlb_win_expectancy import build_we_table
states = pbp_base_out_states(pbp)
table = build_we_table(states, results)
```

### espn_mlb_pbp {#espn_mlb_pbp}

`espn_mlb_pbp(game_id: 'int', raw: 'bool' = False, **kwargs) -> 'Dict'`

espn_mlb_pbp - pull the full ESPN game-summary payload for one MLB game.

**Parameters**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `game_id` | `int` |  | ESPN game id (the "event id"). Obtainable from `espn_mlb_schedule`. |
| `raw` | `bool` | `False` | When True, returns the full nested payload unchanged. When False (default), the same payload is returned for now — full parsing into a tidy plays / boxscore dict is **not yet implemented**; see the TODO below. |

**Returns**

The Site v2 summary payload. Top-level keys typically include `header`, `boxscore`, `plays`, `leaders`, `scoringPlays`, `gameInfo`, `winprobability`, `pickcenter`, `news`, `videos`, `standings`, `article`, `seasonseries`, `broadcasts`, `predictor`.

**Example**

```python
from sportsdataverse.mlb import espn_mlb_pbp
game = espn_mlb_pbp(game_id=401569461, raw=True)
sorted(game.keys())
print(game.get("header", {}).get("competitions", [{}])[0].get("date"))

# Iterate the plays array

plays = game.get("plays") or []
print(f"{len(plays)} plays")
for p in plays[:3]:
    print(p.get("text"))
```

### most_recent_mlb_season {#most_recent_mlb_season}

`most_recent_mlb_season() -> 'int'`

most_recent_mlb_season - return the most recent / current MLB season year.

MLB seasons run calendar-year. Before April we still consider the *previous* year
the "most recent" season (since spring training only starts in late February).

**Returns**

The most recent MLB season year (e.g. `2024`).
