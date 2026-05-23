---
title: NHL Stats REST
sidebar_label: NHL Stats REST
sidebar_position: 4
---

# NHL Stats REST

Wrapped in
[`sportsdataverse.nhl.nhl_stats_rest`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/nhl/nhl_stats_rest.py).
This is the **historical / aggregate** stats surface — distinct from
the game-feed `api-web.nhle.com` API and the player-tracking EDGE API.

| Property | Value |
|---|---|
| Base URL | `https://api.nhle.com/stats/rest/` |
| OpenAPI spec | `fastRhockey/data-raw/nhl_stats_rest_openapi.yaml` |
| Functions | **21** wrappers |
| Localization | All functions accept `lang` (default `"en"`) |
| Filter language | Cayenne expressions via `cayenneExp` / `factCayenneExp` |

## Cayenne filter expressions

Stats REST uses a SQL-like filter syntax in the `cayenneExp` query
parameter:

```python
from sportsdataverse.nhl import nhl_stats_rest_report_skater

# Regular-season skater summary for 2024-25, sorted by points desc
df = nhl_stats_rest_report_skater(
    report="summary",
    cayenneExp="seasonId=20242025 and gameTypeId=2",
    sort="points",
    limit=50,
)
```

`factCayenneExp` applies a secondary filter on the fact/aggregate
columns (e.g. `"points >= 100"` to limit to point-per-game producers).

## Endpoint highlights

| Function | Wraps |
|---|---|
| `nhl_stats_rest_config` | `/config` |
| `nhl_stats_rest_glossary` | `/glossary` |
| `nhl_stats_rest_ping` | `/ping` |
| `nhl_stats_rest_componentSeason` | `/componentSeason` |
| `nhl_stats_rest_season` | `/season` |
| `nhl_stats_rest_report_skater` | `/{lang}/skater/{report}` (`summary`, `advanced`, `powerplay`, `penaltykill`, …) |
| `nhl_stats_rest_report_goalie` | `/{lang}/goalie/{report}` |
| `nhl_stats_rest_report_team` | `/{lang}/team/{report}` |
| `nhl_stats_rest_leaders_skaters` | `/{lang}/leaders/skaters/{attribute}` |
| `nhl_stats_rest_leaders_goalies` | `/{lang}/leaders/goalies/{attribute}` |
| `nhl_stats_rest_franchise` | `/franchise` |
| `nhl_stats_rest_country` | `/country` |
| `nhl_stats_rest_draftSource` | `/draftSource` |

## Example: 2024-25 scoring leaders

```python
from sportsdataverse.nhl import nhl_stats_rest_leaders_skaters

raw = nhl_stats_rest_leaders_skaters(attribute="points", limit=10,
                                      cayenneExp="seasonId=20242025 and gameTypeId=2")
# Returns Dict — parse manually or use ENDPOINT_PARSERS-style flatten.
print(raw["data"][0])
```

## Status verified

Live ping confirmed working — `nhl_stats_rest_season()` returns 108 NHL
seasons on the first call.

## See also

- [NHL EDGE](./edge) — player tracking / Statcast surface.
- [NHL Records](./records) — awards, coaches, HOF, attendance.
