---
title: NHL EDGE
sidebar_label: NHL EDGE
sidebar_position: 2
---

# NHL EDGE

NHL EDGE is the league's player-tracking / Statcast-equivalent surface,
exposing puck and player positional data, shot speed, skating distance/
speed, shot-location heat maps, and zone-time metrics. Wrapped in
[`sportsdataverse.nhl.nhl_edge`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/nhl/nhl_edge.py).

| Property | Value |
|---|---|
| Base URL | `https://api-web.nhle.com/v1/edge/*` |
| OpenAPI spec | `fastRhockey/data-raw/nhl_api_web_openapi.yaml` |
| Functions | **35** wrappers across 4 entity families |
| Season identifier | 8-digit string (`"20242025"`) or 4-digit end-year int (`2025`) |
| Game type | `1` preseason, `2` regular, `3` playoffs |
| `/now` variant | Pass `season=None` |

## Endpoint families

### Skaters (13 wrappers)

| Function | Endpoint |
|---|---|
| `nhl_edge_skater_detail` | `/edge/skater-detail/{playerId}/{season}/{gameType}` |
| `nhl_edge_skater_comparison` | `/edge/skater-comparison/{playerId}/...` |
| `nhl_edge_skater_shot_location_detail` | `/edge/skater-shot-location-detail/{playerId}/...` |
| `nhl_edge_skater_shot_speed_detail` | `/edge/skater-shot-speed-detail/{playerId}/...` |
| `nhl_edge_skater_skating_distance_detail` | `/edge/skater-skating-distance-detail/...` |
| `nhl_edge_skater_skating_speed_detail` | `/edge/skater-skating-speed-detail/...` |
| `nhl_edge_skater_zone_time` | `/edge/skater-zone-time/{playerId}/...` |
| `nhl_edge_skater_landing` | `/edge/skater-landing/{playerId}/...` |
| `nhl_edge_skater_shot_location_top_10` ⚠️ | `/edge/skater-shot-location-top-10/...` |
| `nhl_edge_skater_shot_speed_top_10` ⚠️ | `/edge/skater-shot-speed-top-10/...` |
| `nhl_edge_skater_speed_top_10` ⚠️ | `/edge/skater-speed-top-10/...` |
| `nhl_edge_skater_distance_top_10` ⚠️ | `/edge/skater-distance-top-10/...` |
| `nhl_edge_skater_zone_time_top_10` ⚠️ | `/edge/skater-zone-time-top-10/...` |

### Goalies (9 wrappers)

| Function | Endpoint |
|---|---|
| `nhl_edge_goalie_detail` | `/edge/goalie-detail/{playerId}/...` |
| `nhl_edge_goalie_5v5_detail` | `/edge/goalie-5v5-detail/{playerId}/...` |
| `nhl_edge_goalie_comparison` | `/edge/goalie-comparison/...` |
| `nhl_edge_goalie_save_percentage_detail` | `/edge/goalie-save-percentage-detail/...` |
| `nhl_edge_goalie_shot_location_detail` | `/edge/goalie-shot-location-detail/...` |
| `nhl_edge_goalie_landing` | `/edge/goalie-landing/...` |
| `nhl_edge_goalie_5v5_top_10` ⚠️ | `/edge/goalie-5v5-top-10/...` |
| `nhl_edge_goalie_edge_save_pctg_top_10` ⚠️ | `/edge/goalie-edge-save-pctg-top-10/...` |
| `nhl_edge_goalie_shot_location_top_10` ⚠️ | `/edge/goalie-shot-location-top-10/...` |

### Teams (11 wrappers)

| Function | Endpoint |
|---|---|
| `nhl_edge_team_detail` | `/edge/team-detail/{teamId}/...` |
| `nhl_edge_team_landing` | `/edge/team-landing/{teamId}/...` |
| `nhl_edge_team_shot_location_detail` | `/edge/team-shot-location-detail/...` |
| `nhl_edge_team_shot_speed_detail` | `/edge/team-shot-speed-detail/...` |
| `nhl_edge_team_skating_distance_detail` | `/edge/team-skating-distance-detail/...` |
| `nhl_edge_team_skating_speed_detail` | `/edge/team-skating-speed-detail/...` |
| `nhl_edge_team_zone_time_details` | `/edge/team-zone-time-details/...` |
| `nhl_edge_team_shot_location_top_10` ⚠️ | `/edge/team-shot-location-top-10/...` |
| `nhl_edge_team_skating_distance_top_10` ⚠️ | `/edge/team-skating-distance-top-10/...` |
| `nhl_edge_team_skating_speed_top_10` ⚠️ | `/edge/team-skating-speed-top-10/...` |
| `nhl_edge_team_zone_time_top_10` ⚠️ | `/edge/team-zone-time-top-10/...` |

### Cat (2 wrappers)

| Function | Endpoint |
|---|---|
| `nhl_edge_cat_skater_detail` | `/cat/edge/skater-detail/...` |
| `nhl_edge_cat_goalie_detail` | `/cat/edge/goalie-detail/...` |

## ⚠️ Dead `*_top_10` endpoints

All 12 `*_top_10` URL paths return **HTTP 404** as of 2026-05-23 — the
OpenAPI spec documents them but the endpoints don't exist live. The
wrappers and `parse_edge_top10` parser are kept for forward-compat in
case NHL restores the surface.

The data they'd return is partially available inside the detail
endpoints — `topShotSpeed.percentile`, `topShotSpeed.leagueAvg`, etc.
give per-player league context. Use the detail wrappers instead.

## Parsers

Each EDGE endpoint family has a dedicated parser in
[`sportsdataverse.nhl.nhl_edge_parsers`](./edge-parsers). Detail
payloads ship rich nested lists (`sogDetails`, `hardestShots`, etc.)
which `parse_edge_detail` deliberately stringifies to keep the output
one row per call — call the dedicated sub-frame parser to unroll them
into long-form rows:

```python
from sportsdataverse.nhl import (
    nhl_edge_skater_detail,
    parse_edge_detail,
    parse_edge_sog_details,
)

raw = nhl_edge_skater_detail(8478402, season=2025, game_type=2)  # Connor McDavid

summary = parse_edge_detail(raw)                # 1 row, 96 columns
heatmap = parse_edge_sog_details(raw)           # 17-cell SOG grid
```

## Conventions

- **Season strings**: 8-digit `"YYYYYYYY"` (e.g. `"20242025"` for the
  2024-25 season). Pass a 4-digit int end-year (`2025`) and it's
  auto-expanded. Pass `season=None` to hit the `/now` variant.
- **Game types**: `1` = preseason, `2` = regular season (default),
  `3` = playoffs.
- **Position / strength / sortBy slugs**: pass strings used as-is in
  the URL path (`"all"`, `"5v5"`, `"maxSpeed"`).

## See also

- [NHL api-web](./api-web) — modern game-feed surface (game-center,
  schedule, scoreboard, standings, rosters, leaders, draft).
- [NHL EDGE parsers](./edge-parsers) — schema-grounded parsers for the
  EDGE payload shapes.
- [NHL Stats REST](./stats-rest) — historical aggregates with Cayenne
  filter expressions.
- [NHL Records](./records) — awards, coaches, franchises, HOF, draft.
