---
title: Baseball Savant (Statcast)
sidebar_label: Statcast
sidebar_position: 3
---

# Baseball Savant — Statcast (`mlb_statcast`)

[`sportsdataverse.mlb.mlb_statcast`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/mlb/mlb_statcast.py)
wraps Baseball Savant — MLB's unofficial Statcast surface at
`baseballsavant.mlb.com`. Unlike the MLB Stats API and the ESPN
cross-league wrappers, **most Savant endpoints return CSV that gets
parsed into a polars DataFrame inline** (rather than `Dict` + a
separate parser), so the wrapper layer IS the parser layer.

| Property | Value |
|---|---|
| Base URL | `https://baseballsavant.mlb.com` |
| Functions | **17** wrappers |
| Return type | `polars.DataFrame` by default; `pandas.DataFrame` via `return_as_pandas=True` |
| Default response format | CSV (parsed inline with `polars.read_csv`) |
| ID space | MLBAM IDs — shared with the MLB Stats API |

## Function families

### Pitch-by-pitch search

| Function | Wraps |
|---|---|
| `statcast_search(start_date, end_date, ...)` | `/statcast_search/csv` |
| `statcast_search_chunked(start_date, end_date, ...)` | Auto-chunked variant for date ranges that exceed the 25k-row response cap |

### Statcast leaderboards (9 endpoints)

| Function | Statcast metric |
|---|---|
| `statcast_leaderboard_custom(year, type_, selections, ...)` | Build-your-own |
| `statcast_leaderboard_expected_statistics(year, ...)` | xBA / xSLG / xwOBA |
| `statcast_leaderboard_sprint_speed(year, ...)` | Sprint speed (ft/sec) |
| `statcast_leaderboard_outs_above_average(year, ...)` | OAA fielding metric |
| `statcast_leaderboard_catch_probability(year, ...)` | Outfielder catch probability |
| `statcast_leaderboard_arm_strength(year, ...)` | Throw velocity |
| `statcast_leaderboard_bat_tracking(year, ...)` | Bat speed + attack angle (2024+) |
| `statcast_leaderboard_poptime(year, ...)` | Catcher pop time |
| `statcast_leaderboard_pitch_arsenal(year, ...)` | Per-pitcher pitch-type breakdown |

### Game-level views (2)

| Function | Wraps |
|---|---|
| `statcast_gamefeed(game_pk, at_bat_number=None)` | `/gf?game_pk={id}` — live game feed JSON |
| `statcast_player_page(player_id, stats=None)` | `/savant-player/{id}` — player HTML page |

## The 25,000-row truncation

`/statcast_search/csv` **caps results at exactly 25,000 rows per
response with no pagination**. There is no `offset` parameter and no
`next` URL. The wrapper handles this in two complementary ways:

### Detect + raise (default, `raise_on_truncation=True`)

```python
from sportsdataverse.mlb import statcast_search

# A month of regular-season pitches usually fits comfortably under 25k,
# but a long range or unfiltered query will hit the cap.
df = statcast_search(
    start_date="2024-04-01",
    end_date="2024-04-30",
    player_type="batter",
)
# If the response is exactly 25,000 rows, RuntimeError is raised with
# a helpful message — silent partial responses would be a bug magnet.
```

Pass `raise_on_truncation=False` only when you're deliberately taking
a partial slice for previewing.

### Auto-chunk + stitch (`statcast_search_chunked`)

```python
from sportsdataverse.mlb import statcast_search_chunked

# Pull the full 2024 regular season in one call — the wrapper
# auto-chunks the date range into 5-day windows and stitches the
# resulting frames client-side.
df = statcast_search_chunked(
    start_date="2024-03-28",
    end_date="2024-09-29",
    chunk_days=5,
)
# Each chunk still runs with raise_on_truncation=True so any single
# chunk hitting 25k surfaces an error rather than silently undercounting.
```

The default chunk size of 5 days is tuned to the regular-season
event density (~3,000-5,000 pitches per day league-wide). For
high-event windows like the postseason where each game has more
pitches, drop to `chunk_days=2` or `3`.

## Statcast coverage windows

| Metric | Available from |
|---|---|
| Pitch F/X velocities (adjusted to out-of-hand) | 2008 – 2016 |
| Statcast velocities (native out-of-hand) | 2017+ |
| Exit velocity + launch angle | 2015+ |
| Hawk-Eye optical tracking | 2020+ |
| Bat tracking (swing speed, attack angle) | 2024+ |

Pre-2008 records exist but lack the velocity/launch-angle dimensions
that the modern Statcast era was built around. The
`statcast_leaderboard_bat_tracking` endpoint is the newest and only
returns data for 2024 and later.

## ID space

Statcast and the MLB Stats API share the same `MLBAM` player ID
space — a `personId` from `mlb_api_person(person_id=592450)` is the
same id as the `batter` / `pitcher` columns in a `statcast_search`
result. This makes chaining trivial:

```python
from sportsdataverse.mlb import (
    mlb_api_person, mlb_api_person_stats,
    parse_mlb_api_person_stats,
    statcast_search,
)

JUDGE = 592450

# 1. Identity + bio from the Stats API
person = mlb_api_person(person_id=JUDGE)
print(person["people"][0]["fullName"])     # Aaron Judge

# 2. Season hitting splits from the Stats API
season_df = parse_mlb_api_person_stats(
    mlb_api_person_stats(person_id=JUDGE, stats="season", season=2024)
)

# 3. Per-pitch detail from Statcast (same id space)
pitches = statcast_search(
    start_date="2024-09-01",
    end_date="2024-09-30",
    batters_lookup=[JUDGE],
)
```

## Example: catcher pop times for a season

```python
from sportsdataverse.mlb import statcast_leaderboard_poptime

# 2024 catcher pop times (sec from pitch-receive to second base)
df = statcast_leaderboard_poptime(year=2024)
df.select(["catcher_name", "pop_2b_sba", "exchange_2b", "n_throws"]).head()
```

## Example: 2024 World Series pitch-by-pitch

```python
from sportsdataverse.mlb import statcast_search

df = statcast_search(
    start_date="2024-10-25",
    end_date="2024-10-31",
    game_type=["W"],           # World Series only
)
# Typically 800-1200 pitches across a 5-7 game series
df.group_by("pitch_type").agg(
    pl.len().alias("pitches"),
    pl.col("release_speed").mean().alias("avg_speed"),
    pl.col("estimated_woba_using_speedangle").mean().alias("xwOBA"),
)
```

## See also

- [MLB overview](./index) — all 3 MLB data surfaces
- [MLB Stats API parsers](./parsers) — `statsapi.mlb.com` parser
  layer that pairs with these Statcast wrappers
- [Parsers (general overview)](../parsers/index) — the broader ESPN
  parser layer
