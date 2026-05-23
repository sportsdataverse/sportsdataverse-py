---
title: NHL Records
sidebar_label: NHL Records
sidebar_position: 5
---

# NHL Records

Wrapped in
[`sportsdataverse.nhl.nhl_records`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/nhl/nhl_records.py).
This is the **historical records** API — Hall of Fame, awards,
franchise history, draft, and all-star data.

| Property | Value |
|---|---|
| Base URL | `https://records.nhl.com/site/api/` |
| OpenAPI spec | `fastRhockey/data-raw/nhl_records_openapi.yaml` |
| Functions | **50** wrappers |
| Top-level shape | `{"data": [...], "total": N}` |
| Filter kwargs | `cayenneExp`, `factCayenneExp`, `include`, `limit`, `start`, `sort` |

## Endpoint families

### Awards (3)

`nhl_records_award`, `nhl_records_award_recipient`,
`nhl_records_player_awards`

### Coaches (7)

`nhl_records_coach`, `nhl_records_coach_career_records`,
`nhl_records_coach_season_records`,
`nhl_records_coach_regular_season_records`,
`nhl_records_coach_playoff_records`, `nhl_records_head_coaches`,
`nhl_records_assistant_coaches`

### Franchises (7)

`nhl_records_franchise`, `nhl_records_franchise_team_totals`,
`nhl_records_franchise_drafted_players`, …

### Skaters (5) / Goalies (8)

Career & season records, milestones, awards, save-percentage leaders.

### Draft (5)

`nhl_records_draft`, `nhl_records_draft_top_picks`,
`nhl_records_draft_by_season`, `nhl_records_draft_by_franchise`,
`nhl_records_draft_overall`

### All-Star (5) / HOF (2) / GMs (2)

Plus attendance, fastest-goals, team records.

## Example: 40-franchise list

```python
from sportsdataverse.nhl import nhl_records_franchise

raw = nhl_records_franchise()  # returns Dict with {"data": [...], "total": 40}
print(f"{raw['total']} franchises in NHL history")
for f in raw["data"][:5]:
    print(f"  {f['mostRecentTeamName']}: {f['teamCommonName']}")
```

## Status verified

Live test: `nhl_records_franchise()` returned 40 franchises on the
first call against `records.nhl.com/site/api/franchise`.

## See also

- [NHL EDGE](./edge) — player tracking / Statcast surface.
- [NHL Stats REST](./stats-rest) — historical aggregates with Cayenne
  filter expressions.
