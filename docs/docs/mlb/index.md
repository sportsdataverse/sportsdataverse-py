---
title: MLB
sidebar_label: MLB
sidebar_position: 1
---

# MLB

`sportsdataverse.mlb` wraps **three independent data surfaces** that
together cover the modern MLB ecosystem end-to-end:

| Surface | Module | Wrappers | Base URL |
|---|---|---:|---|
| **ESPN** (cross-league + MLB-only) | `mlb_espn_ext` (factory-generated) | 113 + 5 originals | `site.api.espn.com`, `sports.core.api.espn.com` |
| **MLB Stats API** (official) | `mlb_api` | 40 | `statsapi.mlb.com/api/v1/` |
| **Baseball Savant** (Statcast) | `mlb_statcast` | 17 | `baseballsavant.mlb.com` |
| **Total MLB surface** | | **175 functions** | |

## ESPN MLB

Same cross-league pattern as every other league — see
[ESPN cross-league architecture](../architecture/espn-cross-league)
for the factory mechanics. MLB-specific additions:

- `espn_mlb_athlete_hotzones(athlete_id)` — pitch-zone heat map
  (Core v2; sparse but reachable).
- All 84 universal wrappers (`espn_mlb_scoreboard`, `espn_mlb_teams_site`,
  `espn_mlb_athlete_overview`, etc.) work with `return_parsed=True` →
  polars DataFrame.

## MLB Stats API (`mlb_api`)

Wraps the official MLB Stats API at `statsapi.mlb.com`. 40 functions
across schedule, standings, teams, players, boxscores, live feed,
leaders, and meta enums. Pair with the parser layer at
[`sportsdataverse.mlb.mlb_api_parsers`](#mlb-stats-api-parser-layer)
for tidy polars DataFrame output.

| Function | Wraps |
|---|---|
| `mlb_api_teams(season=...)` | `/teams?sportId=1&season=...` |
| `mlb_api_schedule(date=...)` | `/schedule?sportId=1&date=...` |
| `mlb_api_boxscore(game_pk=...)` | `/game/{game_pk}/boxscore` |
| `mlb_api_live_feed(game_pk=...)` | `/v1.1/game/{game_pk}/feed/live` |
| `mlb_api_player(person_id=...)` | `/people/{person_id}` |
| `mlb_api_team_roster(team_id=...)` | `/teams/{team_id}/roster` |
| `mlb_api_standings(league_id=...)` | `/standings?leagueId=...` |
| `mlb_api_leaders(...)` | `/stats/leaders` |
| ... | (see source for the full 40) |

### IDs to know

- `sportId=1` is MLB. Minor leagues: 11 (AAA), 12 (AA), 13 (A+), 14
  (A), 16 (Rookie). KBO=32, NPB=31.
- `leagueId`: 103=AL, 104=NL.
- `gameType`: `R` regular, `F` wild card, `D` DS, `L` LCS, `W` WS,
  `S` spring, `A` all-star, `E` exhibition, `PO` postseason.
- **MLBAM IDs** (`personId` / `batter` / `pitcher`) are shared between
  the Stats API and Baseball Savant — same id space.

### Version note

Nearly everything is `/api/v1/`. The only `v1.1` endpoint we wrap is
`/api/v1.1/game/{gamePk}/feed/live` (the live-feed endpoint). There
is no `v2`.

## MLB Stats API parser layer

The 40 `mlb_api_*` wrappers all return raw `Dict`. The parser layer in
[`sportsdataverse.mlb.mlb_api_parsers`](./parsers) turns those payloads
into tidy polars / pandas DataFrames — 5 dedicated parsers for the
high-traffic endpoints (`schedule`, `teams`, `team_roster`,
`standings`, `person_stats`) plus a generic `parse_mlb_api_list`
fallback for the 20+ list-shape endpoints (venues, divisions, awards,
umpires, draft, etc.).

See the dedicated **[MLB Stats API parsers page](./parsers)** for the
full table, registry, and chaining examples.

```python
from sportsdataverse.mlb import mlb_api_standings, parse_mlb_api_standings

# 30-row standings frame (6 divisions × 5 teams) with full division
# context columns + per-team record stats
df = parse_mlb_api_standings(mlb_api_standings(season=2024))
```

## Baseball Savant — Statcast (`mlb_statcast`)

The 17 `statcast_*` wrappers cover Baseball Savant at
`baseballsavant.mlb.com` — pitch-by-pitch search (with auto-chunking
to handle the 25,000-row cap), 9 Statcast leaderboards (xStats,
sprint speed, OAA, catch probability, arm strength, bat tracking,
pop time, pitch arsenal), the per-game feed, and the scraped player
page.

Most Savant endpoints return CSV that's parsed into a polars
DataFrame **inline** — the wrapper layer IS the parser layer, no
separate `parse_*` function needed.

See the dedicated **[Statcast page](./statcast)** for the full
function table, the 25,000-row truncation handling
(`raise_on_truncation` + `statcast_search_chunked`), coverage
windows by metric, the MLBAM ID space shared with the Stats API,
and chaining examples.

```python
from sportsdataverse.mlb import statcast_search_chunked

# Full 2024 regular season — auto-chunked into 5-day windows
df = statcast_search_chunked(start_date="2024-03-28",
                              end_date="2024-09-29")
```

## Example: chain ESPN + Stats API + Statcast

```python
from sportsdataverse.mlb import (
    espn_mlb_scoreboard,
    mlb_api_boxscore,
    statcast_search,
)

# 1. ESPN scoreboard for today
scoreboard = espn_mlb_scoreboard(return_parsed=True)
print(scoreboard.select(["event_id", "home_name", "away_name", "venue_id"]).head())

# 2. MLBAM box score for one game (game_pk is MLBAM's id)
box = mlb_api_boxscore(game_pk=746210)
print(list(box["teams"]["home"]["players"]))

# 3. Statcast pitches for that game's home team on the date
pitches = statcast_search(
    start_date="2024-09-29", end_date="2024-09-29",
    team="LAD", player_type="pitcher",
)
print(pitches.select(["player_name", "pitch_type", "release_speed", "events"]).head())
```

## See also

- [ESPN cross-league architecture](../architecture/espn-cross-league)
- [The parser layer](../parsers/index)
