---
title: NHL api-web (modern game-feed)
sidebar_label: NHL api-web
sidebar_position: 1
---

# NHL api-web (modern game-feed)

The modern NHL game-feed API at [`api-web.nhle.com/v1/`](https://api-web.nhle.com/v1/)
is the canonical source for live and historical NHL data —
game-center, schedules, scores, scoreboards, standings, rosters,
player profiles, leaders, and the draft. Wrapped in
[`sportsdataverse.nhl.nhl_api_web`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/nhl/nhl_api_web.py).

| Property | Value |
|---|---|
| Base URL | `https://api-web.nhle.com/v1/` |
| OpenAPI spec | `fastRhockey/data-raw/nhl_api_web_openapi.yaml` |
| Functions | **26** wrappers |
| Season identifier | 8-digit string (`"20242025"`) or 4-digit end-year int (`2025`) |
| Game type | `1` preseason, `2` regular, `3` playoffs |
| Game ID | 10-digit, e.g. `2023030417` (2024 Stanley Cup Final G7) |
| `/now` variant | Many endpoints accept it for current state |

This page documents the *modern* surface that replaced the deprecated
`statsapi.web.nhl.com`. For the historical / aggregate stats surface
see [NHL Stats REST](./stats-rest); for player-tracking Statcast-style
data see [NHL EDGE](./edge); for records / awards / HOF see
[NHL Records](./records).

## Endpoint families

### Game-center (4 wrappers)

The richest surface — per-game deep dives for live and historical games.

| Function | Endpoint |
|---|---|
| `nhl_web_pbp(game_id)` | `/gamecenter/{game_id}/play-by-play` |
| `nhl_web_boxscore(game_id)` | `/gamecenter/{game_id}/boxscore` |
| `nhl_web_landing(game_id)` | `/gamecenter/{game_id}/landing` |
| `nhl_web_right_rail(game_id)` | `/gamecenter/{game_id}/right-rail` |

### Schedule / score / scoreboard (4)

| Function | Endpoint |
|---|---|
| `nhl_web_schedule(date)` | `/schedule/{date}` (or `/schedule/now`) |
| `nhl_web_score(date)` | `/score/{date}` |
| `nhl_web_scoreboard(date, team)` | `/scoreboard/{date}` |
| `nhl_web_schedule_calendar(date)` | `/schedule-calendar/{date}` |

### Team-scoped (5)

| Function | Endpoint |
|---|---|
| `nhl_web_club_schedule_season(team, season)` | `/club-schedule-season/{team}/{season}` |
| `nhl_web_club_schedule_month(team, month)` | `/club-schedule/{team}/month/{month}` |
| `nhl_web_club_schedule_week(team, date)` | `/club-schedule/{team}/week/{date}` |
| `nhl_web_club_stats(team, season, game_type)` | `/club-stats/{team}/{season}/{game_type}` |
| `nhl_web_club_stats_season(team)` | `/club-stats-season/{team}` |
| `nhl_web_roster(team, season)` | `/roster/{team}/{season}` |
| `nhl_web_roster_season(team)` | `/roster-season/{team}` |

### Standings (2)

| Function | Endpoint |
|---|---|
| `nhl_web_standings(date)` | `/standings/{date}` |
| `nhl_web_standings_season()` | `/standings-season` |

### Player (3)

| Function | Endpoint |
|---|---|
| `nhl_web_player_landing(player_id)` | `/player/{player_id}/landing` |
| `nhl_web_player_game_log(player_id, season, game_type)` | `/player/{player_id}/game-log/{season}/{game_type}` |
| `nhl_web_player_spotlight()` | `/player-spotlight` |

### Leaders (2)

| Function | Endpoint |
|---|---|
| `nhl_web_skater_leaders(season, game_type)` | `/skater-stats-leaders/{season}/{game_type}` |
| `nhl_web_goalie_leaders(season, game_type)` | `/goalie-stats-leaders/{season}/{game_type}` |

### Draft (5)

| Function | Endpoint |
|---|---|
| `nhl_web_draft_picks(year, round_)` | `/draft/picks/{year}/{round}` |
| `nhl_web_draft_rankings(year, category)` | `/draft/rankings/{year}/{category}` |
| `nhl_web_draft_picks_now()` | `/draft/picks/now` |
| `nhl_web_draft_rankings_now()` | `/draft/rankings/now` |
| `nhl_web_draft_tracker_picks_now()` | `/draft-tracker/picks/now` |

### Playoff series (1)

| Function | Endpoint |
|---|---|
| `nhl_web_playoff_series(season, series_letter)` | `/schedule/playoff-series/{season}/{series_letter}` |

## Parser layer

[`sportsdataverse.nhl.nhl_api_web_parsers`](https://github.com/sportsdataverse/sportsdataverse-py/blob/main/sportsdataverse/nhl/nhl_api_web_parsers.py)
turns each raw `Dict` payload into a tidy polars (or pandas) DataFrame.
Schemas captured 2026-05-24.

### Parser table

| Parser | Wrapper(s) | Output shape |
|---|---|---|
| `parse_nhl_web_pbp` | `pbp` | One row per play (~330/game) |
| `parse_nhl_web_boxscore` | `boxscore` | One row per (team × player) — unrolls 6 buckets, tags `home_away` + `position_group` |
| `parse_nhl_web_landing` | `landing` | Single-row game profile |
| `parse_nhl_web_right_rail` | `right_rail` | **Dispatcher** — 6 sub-frames |
| `parse_nhl_web_schedule` | `schedule`, `schedule_calendar` | One row per game, `schedule_date` prefixed |
| `parse_nhl_web_score` | `score` | One row per game for the date |
| `parse_nhl_web_scoreboard` | `scoreboard` | One row per game across days, `scoreboard_date` prefixed |
| `parse_nhl_web_club_schedule` | `club_schedule_*` (×3) | One row per game with `club_timezone` + season context |
| `parse_nhl_web_standings` | `standings` | One row per team (84 stat cols) |
| `parse_nhl_web_standings_season` | `standings_season` | One row per season (108 NHL seasons) |
| `parse_nhl_web_club_stats` | `club_stats`, `club_stats_season` | **Dispatcher** — `{skaters, goalies}` |
| `parse_nhl_web_roster` | `roster`, `roster_season` | One row per player, merges `forwards`/`defensemen`/`goalies` with `position_group` |
| `parse_nhl_web_player_landing` | `player_landing` | Single-row player profile (~130 cols for a regular skater) |
| `parse_nhl_web_player_game_log` | `player_game_log` | One row per game |
| `parse_nhl_web_leaders` | `skater_leaders`, `goalie_leaders` | One row per (category × player), tagged with `category` |
| `parse_nhl_web_draft_picks` | `draft_picks`, `draft_picks_now`, `draft_tracker_picks_now` | One row per pick |

### Dispatchers

Two endpoints ship multi-section payloads handled by dispatchers:

#### `parse_nhl_web_right_rail`

Game-context sub-frames typically rendered alongside the box-score:

| Section key | Shape |
|---|---|
| `season_series` | Head-to-head games between the two teams (~7 rows) |
| `shots_by_period` | Per-period shot totals (3 rows) |
| `team_game_stats` | Per-category team-vs-team stat comparison (~10 rows) |
| `game_info` | Single-row game-info dict (referees, linesmen, awayTeam, homeTeam) |
| `linescore_by_period` | Per-period score breakdown |
| `season_series_wins` | Single-row aggregate of series wins |

```python
from sportsdataverse.nhl import nhl_web_right_rail, parse_nhl_web_right_rail

raw = nhl_web_right_rail(2023030417)            # 2024 Stanley Cup Final G7

# Full dispatch — dict of 6 sub-frames
out = parse_nhl_web_right_rail(raw)
out["team_game_stats"]                          # one row per stat category

# Or grab just one section
team_stats = parse_nhl_web_right_rail(raw, section="team_game_stats")
```

#### `parse_nhl_web_club_stats`

```python
from sportsdataverse.nhl import nhl_web_club_stats, parse_nhl_web_club_stats

raw = nhl_web_club_stats("EDM", season=2024, game_type=2)

out = parse_nhl_web_club_stats(raw)
out["skaters"]   # 27 rows for EDM's regular-season skaters
out["goalies"]   # 3 goalies

# Or grab just one
skaters = parse_nhl_web_club_stats(raw, section="skaters")
```

### Registry

`NHL_API_WEB_ENDPOINT_PARSERS` has 24 entries — every data endpoint
has a parser. `parser_for_nhl_api_web(fn_name)` returns the parser
callable (or `None` for the 3 idiosyncratic endpoints whose payloads
don't fit a reusable pattern: `playoff_series`, `player_spotlight`,
`draft_rankings*`).

## Parser deep-dive

### Boxscore unrolling pattern

`parse_nhl_web_boxscore` is the most non-trivial parser — boxscore
ships **six independent buckets** (away/home × forwards/defense/
goalies) at `playerByGameStats.{awayTeam,homeTeam}.{forwards,defense,
goalies}`. The parser merges all six into one long-form frame and
tags each row with `home_away` ("home" / "away") and
`position_group` ("forwards" / "defense" / "goalies") so callers can
filter without re-walking the original nested dict:

```python
import polars as pl
from sportsdataverse.nhl import nhl_web_boxscore, parse_nhl_web_boxscore

df = parse_nhl_web_boxscore(nhl_web_boxscore(2023030417))

# Both teams' starting goalies
df.filter(pl.col("position_group") == "goalies").select(
    ["home_away", "name", "shots_against", "saves",
     "goals_against", "save_pctg"]
)

# Top 5 scorers across both teams
df.filter(pl.col("position_group") != "goalies") \
  .sort("points", descending=True).head(5)
```

### Dispatchers in detail

#### `parse_nhl_web_right_rail` — 6 sub-frames in one call

Right rail bundles 6 unrelated context views (season series, shot
trends, team comparisons, officials, period scoring, series wins).
With `section=None` (the default) the dispatcher returns a dict; with
`section="<name>"` it returns just that one frame, mirroring the
[`parse_summary`](../parsers/index) pattern for Site v2 ESPN summary
payloads.

```python
from sportsdataverse.nhl import nhl_web_right_rail, parse_nhl_web_right_rail

raw = nhl_web_right_rail(2023030417)
out = parse_nhl_web_right_rail(raw)        # dict of 6 sub-frames

# Per-category team-vs-team comparison from the game
out["team_game_stats"].select(["category", "away_value", "home_value"])

# Per-period shot totals
out["shots_by_period"]                      # 3 rows × {period, away, home}

# Where the two teams stood entering this game
out["season_series_wins"]                   # 1 row × wins / needed_to_win
```

The dispatcher's section name → schema mapping:

| `section=` | Source key in payload | Typical shape |
|---|---|---|
| `"season_series"` | `seasonSeries` | One row per head-to-head game |
| `"shots_by_period"` | `shotsByPeriod` | 3 rows (one per period; 4 for OT) |
| `"team_game_stats"` | `teamGameStats` | One row per stat category |
| `"game_info"` | `gameInfo` | Single-row dict (referees, linesmen, team dicts) |
| `"linescore_by_period"` | `linescore.byPeriod` | Per-period score breakdown |
| `"season_series_wins"` | `seasonSeriesWins` | Single-row series win aggregate |

#### `parse_nhl_web_club_stats` — skaters + goalies split

A simpler 2-section dispatcher. Both sub-frames carry per-player
season totals; goalies have a different stat schema than skaters so
the natural split is to keep them as separate frames rather than
forcing a `position_group` column with mixed schemas.

```python
from sportsdataverse.nhl import nhl_web_club_stats, parse_nhl_web_club_stats

out = parse_nhl_web_club_stats(nhl_web_club_stats("EDM", season=2024, game_type=2))
out["skaters"]   # 27 skaters × 20 cols (G, A, P, +/-, TOI, …)
out["goalies"]   #  3 goalies × 20 cols (W, L, OTL, GAA, SV%, SO, …)
```

### Roster merging pattern

`parse_nhl_web_roster` follows the same merge-with-tag idea as
`parse_nhl_web_boxscore` but for the static roster endpoint —
forwards / defensemen / goalies arrive as three separate arrays
and the parser merges them into one frame with a `position_group`
column:

```python
from sportsdataverse.nhl import nhl_web_roster, parse_nhl_web_roster

df = parse_nhl_web_roster(nhl_web_roster("EDM", season=2024))
df["position_group"].value_counts()
# forwards=11, defensemen=9, goalies=4
```

### Leaders category-keyed payload

The leaders endpoints return a **category-keyed** dict at the top
level (`{points: [...], goals: [...], assists: [...]}` for skaters;
`{wins: [...], savePctg: [...], shutouts: [...]}` for goalies).
`parse_nhl_web_leaders` walks every top-level list-valued key, tags
each row with the category it came from, and concatenates:

```python
from sportsdataverse.nhl import nhl_web_skater_leaders, parse_nhl_web_leaders

# Request multiple categories at once via the URL — parser handles
# the resulting multi-category payload transparently
df = parse_nhl_web_leaders(nhl_web_skater_leaders())
df["category"].unique()    # ['points', 'goals', 'assists', ...]
df.filter(pl.col("category") == "points").head(10)
```

## Full game-center example

```python
from sportsdataverse.nhl import (
    nhl_web_boxscore,
    nhl_web_landing,
    nhl_web_pbp,
    nhl_web_right_rail,
    parse_nhl_web_boxscore,
    parse_nhl_web_landing,
    parse_nhl_web_pbp,
    parse_nhl_web_right_rail,
)

GAME_ID = 2023030417    # 2024 Stanley Cup Final G7 EDM @ FLA

# 1. Game header — one row of context (venue, teams, gameState, clock)
header = parse_nhl_web_landing(nhl_web_landing(GAME_ID))

# 2. Per-player stats — one row per (team × player), 40 players × 36 cols
players = parse_nhl_web_boxscore(nhl_web_boxscore(GAME_ID))
players.filter(pl.col("position_group") == "goalies")   # both goalies' stats

# 3. Play-by-play — ~331 plays per game
pbp = parse_nhl_web_pbp(nhl_web_pbp(GAME_ID))
pbp.filter(pl.col("type_desc_key") == "goal")           # 4 goals in the game

# 4. Context sub-frames (season series, shots-by-period, team stats, ...)
rail = parse_nhl_web_right_rail(nhl_web_right_rail(GAME_ID))
rail["shots_by_period"]    # 3 periods × {periodDescriptor, away, home}
```

## See also

- [NHL EDGE](./edge) — player tracking / Statcast-equivalents.
- [NHL EDGE parsers](./edge-parsers) — schema-grounded parsers for the
  EDGE payload shapes.
- [NHL Stats REST](./stats-rest) — historical aggregates with Cayenne
  filter expressions.
- [NHL Records](./records) — awards, coaches, franchises, draft, HOF.
- [The parser layer (general overview)](../parsers/index).
