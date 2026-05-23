---
title: NHL EDGE parsers
sidebar_label: NHL EDGE parsers
sidebar_position: 3
---

# NHL EDGE parsers

[`sportsdataverse.nhl.nhl_edge_parsers`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/nhl/nhl_edge_parsers.py)
turns raw EDGE `Dict` payloads into tidy polars (or pandas) DataFrames.
The schemas were captured live 2026-05-23 against the 2024-25 season
and are the basis of the parser logic.

## Family parsers (1 wrapper → 1 parser)

| Parser | Wrappers it handles | Output shape |
|---|---|---|
| `parse_edge_top10` | All `*_top_10` (12 wrappers — currently 404) | Multi-row leaderboard |
| `parse_edge_detail` | `*_detail`, `*_5v5_detail`, `*_comparison`, `*_landing` (15 wrappers) | Single row, deep-flattened |
| `parse_edge_shot_location` | `*_shot_location_detail` (3 wrappers) | 17-cell zone grid |
| `parse_edge_zone_time` | `skater_zone_time`, `team_zone_time_details` (2 wrappers) | Per-strength-state rows |
| `parse_edge_payload` | Fallback for any unregistered EDGE wrapper | Best-effort flatten |

## Sub-frame parsers (extract nested lists)

Detail payloads ship rich nested lists *alongside* the single-row entity
summary. `parse_edge_detail` deliberately stringifies them to keep the
output one row per call — the sub-frame parsers unroll them:

| Parser | Source key(s) in payload | Output |
|---|---|---|
| `parse_edge_sog_details` | `sogDetails`, `shotLocationDetails` | 17-cell SOG / save grid |
| `parse_edge_sog_summary` | `sogSummary`, `shotLocationSummary`, `shotLocationTotals` | 4–12 row location-code aggregate |
| `parse_edge_hardest_shots` | `hardestShots` (from `skater_shot_speed_detail`) | 10-row hardest-shots list |

The `EDGE_SUBFRAME_PARSERS` registry maps each detail wrapper to the
tuple of sub-frame parsers that apply.

## Example: full Connor McDavid season

```python
from sportsdataverse.nhl import (
    nhl_edge_skater_detail,
    nhl_edge_skater_shot_speed_detail,
    nhl_edge_skater_zone_time,
    parse_edge_detail,
    parse_edge_hardest_shots,
    parse_edge_sog_details,
    parse_edge_sog_summary,
    parse_edge_zone_time,
)

PLAYER_ID = 8478402  # Connor McDavid
SEASON    = 2025     # end-year of 2024-25

# 1. Top-line summary — one row of ~96 columns
summary = parse_edge_detail(
    nhl_edge_skater_detail(PLAYER_ID, season=SEASON, game_type=2)
)

# 2. Shots-on-goal heat map — 17-cell grid
heatmap_17 = parse_edge_sog_details(
    nhl_edge_skater_detail(PLAYER_ID, season=SEASON, game_type=2)
)

# 3. SOG aggregate by location code — 4-row summary
heatmap_4  = parse_edge_sog_summary(
    nhl_edge_skater_detail(PLAYER_ID, season=SEASON, game_type=2)
)

# 4. Zone-time splits by strength state — 4 rows (all / 5v5 / PP / PK)
zone_time  = parse_edge_zone_time(
    nhl_edge_skater_zone_time(PLAYER_ID, season=SEASON, game_type=2)
)

# 5. 10 hardest shots of the season — 10 rows of per-shot context
hardest    = parse_edge_hardest_shots(
    nhl_edge_skater_shot_speed_detail(PLAYER_ID, season=SEASON, game_type=2)
)
```

## Contract guarantees

Every EDGE parser obeys the same rules as the universal ESPN parsers:

1. **Polars by default**; pandas via `return_as_pandas=True`.
2. **Empty / malformed payloads return a zero-row frame** instead of
   raising.
3. **Output columns snake-cased** (`shootsCatches` → `shoots_catches`,
   `shotSpeed` → `shot_speed_imperial` / `shot_speed_metric` when the
   source is a nested `{imperial, metric}` dict).

## Resolver: `parser_for_edge(fn_name)`

For programmatic dispatch — e.g. "which parser goes with this wrapper
name?" — use `parser_for_edge()`. It always returns a callable (falls
back to `parse_edge_payload` for unregistered names, never returns
`None`):

```python
from sportsdataverse.nhl import (
    nhl_edge_skater_detail,
    parser_for_edge,
)

parser = parser_for_edge(nhl_edge_skater_detail.__name__)
df     = parser(nhl_edge_skater_detail(8478402, season=2025))
```

## Test fixtures

Captured payloads live at
[`tests/fixtures/nhl_edge/`](https://github.com/sportsdataverse/sportsdataverse-py/tree/main/tests/fixtures/nhl_edge).
Offline parser tests run against these fixtures (no live API needed) in
[`tests/test_nhl_edge_parsers.py`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/tests/test_nhl_edge_parsers.py).
