---
title: MLB Stats API parsers
sidebar_label: MLB Stats API parsers
sidebar_position: 2
---

# MLB Stats API parsers

[`sportsdataverse.mlb.mlb_api_parsers`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/mlb/mlb_api_parsers.py)
turns the 40 raw-`Dict` `mlb_api_*` wrappers into tidy polars (or
pandas) DataFrames. Schemas captured 2026-05-24 from
`statsapi.mlb.com`.

Mirrors the design of [`sportsdataverse._common_espn_parsers`](../parsers/index):

* Every parser returns `polars.DataFrame` by default; pass
  `return_as_pandas=True` for pandas.
* Empty / malformed payloads return a zero-row frame instead of
  raising — callers can chain without null-checks.
* Output columns are snake-cased via
  `sportsdataverse.dl_utils.underscore`.
* Most parsers use `pandas.json_normalize` for one-pass flattening of
  nested dicts.

## Dedicated parsers (5)

Five parsers have endpoint-specific unrolling logic. Use these for
the high-traffic endpoints where the generic flattener would leave
the data in an awkward shape.

| Parser | Wrappers it handles | Output |
|---|---|---|
| `parse_mlb_api_schedule` | `mlb_api_schedule`, `mlb_api_schedule_postseason` | One row per game — walks `dates[].games[]` and prefixes `schedule_date`. Flattens `teams.home.*` / `teams.away.*` / `venue.*` / `status.*`. |
| `parse_mlb_api_teams` | `mlb_api_teams` | One row per team. |
| `parse_mlb_api_team_roster` | `mlb_api_team_roster` | One row per player from `roster[]` with `person` / `position` / `status` sub-dicts flattened. |
| `parse_mlb_api_standings` | `mlb_api_standings` | One row per (division × team) — walks `records[].teamRecords[]`, prefixes division identifiers as `standings_*` to avoid column collisions with team-record fields like `lastUpdated`. |
| `parse_mlb_api_person_stats` | `mlb_api_person_stats`, `mlb_api_team_stats` | One row per stats split — walks `stats[].splits[]`, prefixes `stats_type` / `stats_group` from the parent block, flattens the inner `stat` block to wide stat columns. |

## Generic fallback (`parse_mlb_api_list`)

For the 20+ list-shape endpoints that don't need extra unrolling, a
single `parse_mlb_api_list` walks common top-level array keys
(`teams`, `venues`, `sports`, `leagues`, `divisions`, `seasons`,
`awards`, `awardRecipients`, `umpires`, `people`, `players`,
`items`, `records`) until one resolves to a non-empty list of dicts,
then flattens.

Covered endpoints:

`venues`, `sports`, `leagues`, `divisions`, `seasons`, `awards`,
`award_recipients`, `umpires`, `people`, `sport_players`, `draft`,
`draft_prospects`, `attendance`, `team_leaders`, `team_alumni`,
`team_affiliates`, `stats`, `stats_leaders`, `stats_streaks`.

## Registry

| Symbol | Description |
|---|---|
| `MLB_API_ENDPOINT_PARSERS` | Dict mapping `mlb_api_*` wrapper name → parser. 26 entries (5 dedicated + 21 generic). |
| `parser_for_mlb_api(fn_name)` | Returns the registered parser; falls back to `parse_mlb_api_list` for any unregistered name. **Never returns `None`** — the caller always gets a DataFrame-returning callable. |

```python
>>> from sportsdataverse.mlb import MLB_API_ENDPOINT_PARSERS, parser_for_mlb_api
>>> parser_for_mlb_api("mlb_api_standings").__name__
'parse_mlb_api_standings'
>>> parser_for_mlb_api("mlb_api_unknown").__name__
'parse_mlb_api_list'
>>> len(MLB_API_ENDPOINT_PARSERS)
26
```

## Examples

### Full season standings → tidy frame

```python
from sportsdataverse.mlb import mlb_api_standings, parse_mlb_api_standings

# 30-row frame (6 divisions × 5 teams) with full division context
# columns + per-team record stats (wins, losses, pct, gamesBack,
# streak, divisionRank, leagueRank, sportRank, etc.).
raw = mlb_api_standings(season=2024)
df  = parse_mlb_api_standings(raw)
df.select(["standings_division_name", "team_id", "wins", "losses",
           "winning_percentage", "games_back"]).head()
```

### Aaron Judge's 2024 hitting line

```python
from sportsdataverse.mlb import mlb_api_person_stats, parse_mlb_api_person_stats

raw = mlb_api_person_stats(person_id=592450,        # Aaron Judge
                            stats="season", season=2024)
df  = parse_mlb_api_person_stats(raw)
# One row with stats_type='Season' + stats_group='hitting' +
# wide stat_* columns (stat_home_runs, stat_avg, stat_slg, stat_obp, ...)
print(df.select(["stats_group", "stat_home_runs", "stat_avg",
                 "stat_slg", "stat_ops"]))
```

### Generic list parser for venues

```python
from sportsdataverse.mlb import mlb_api_venues, parse_mlb_api_list

raw = mlb_api_venues(active=True)
df  = parse_mlb_api_list(raw)   # 1,646-row frame of active venues
```

### Compose against any endpoint via the registry

```python
from sportsdataverse.mlb import mlb_api_teams, parser_for_mlb_api

raw = mlb_api_teams(season=2024)
parser = parser_for_mlb_api(mlb_api_teams.__name__)   # → parse_mlb_api_teams
df = parser(raw)
```

## Test fixtures

Captured 2026-05-24 in
[`tests/fixtures/mlb_api/`](https://github.com/sportsdataverse/sportsdataverse-py/tree/main/tests/fixtures/mlb_api):

- `schedule_2024_09_29.json` — 15 games on the final regular-season day
- `teams_2024.json` — 30 MLB teams
- `team_roster_yankees_2024.json` — NYY full-season roster (~54 players)
- `standings_2024.json` — 6 divisions × 5 teams = 30 rows
- `person_stats_judge_2024.json` — Aaron Judge's 2024 season splits
- `venues_active.json` — 1,646 active venues
- `sports.json` — 20 sport IDs
- `divisions.json` — 61 divisions

17 offline tests in
[`tests/test_mlb_api_parsers.py`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/tests/test_mlb_api_parsers.py)
exercise every parser against these fixtures.

## See also

- [MLB overview](./index) — all 3 MLB data surfaces (ESPN + Stats API + Statcast)
- [Parsers (general overview)](../parsers/index) — the ESPN parser layer + `return_parsed=True` shim
- [Architecture](../architecture/espn-cross-league) — the cross-league factory
