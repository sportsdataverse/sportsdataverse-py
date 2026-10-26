---
title: HockeyTech leagues
sidebar_label: HockeyTech leagues
sidebar_position: 1
description: "The PWHL and nineteen North American minor and junior hockey leagues on the HockeyTech feed: one function family per league."
---

# HockeyTech leagues

The PWHL and nineteen North American minor and junior leagues publish their stats
through HockeyTech (LeagueStat): one query-dispatched endpoint behind every league
site. sdv-py wraps it as one shared core, `sportsdataverse.hockeytech`, and one
generated family of functions per league.

## The twenty leagues

`pwhl`, `ahl`, `ohl`, `whl`, `qmjhl`, `echl`, `sphl`, `chl`, `ushl`, `bchl`, `ajhl`,
`sjhl`, `ojhl`, `cchl`, `gojhl`, `mhl`, `nojhl`, `vijhl`, `kijhl` and `mjhl`, each
named by its HockeyTech client code (the QMJHL's is `lhjmq`).

Each league is a module under `sportsdataverse.hockey.<league>`. The PWHL lives at the
top level, `sportsdataverse.pwhl`, with a richer surface: props, xG, ratings and
release loaders. Every function is also exported from the top-level `sportsdataverse`
namespace.

## One family per league

The league registry is the whole surface: each league gets the same thirteen
functions, named after it. For the ECHL:

| Function | Returns |
|---|---|
| `echl_schedule(season=)` | one row per game |
| `echl_standings(season=)` | one row per team |
| `echl_teams(season=)` | the teams |
| `echl_team_roster(team_id, season=)` | one team's roster |
| `echl_player_stats(player_id)` | one player's stats |
| `echl_leaders(season=)` | stat leaders |
| `echl_pbp(game_id)` | one game's play-by-play |
| `echl_game_summary(game_id)` | one game's summary, a dict of frames |
| `echl_game_shifts(game_id)` | one game's shifts |
| `echl_player_toi(game_id)` | time on ice per player |
| `echl_game_corsi(game_id)` | shot attempts per player |
| `echl_season_id()` | the league's HockeyTech season ids |
| `most_recent_echl_season()` | the current season (END year) |

The other nineteen leagues have the same thirteen with their own prefix
(`ohl_schedule`, `ushl_pbp`, ...). `season` is the END year of the season, so
`season=2026` is 2025-26; the family maps it to HockeyTech's season id. Ids are
`Int64`.

```python
import polars as pl
import sportsdataverse as sdv
from sportsdataverse.hockey.ushl import ushl_standings

schedule = sdv.echl_schedule(season=2026)          # 2025-26, one row per game
standings = ushl_standings(season=2025)
game_id = int(schedule["game_id"][0])
pbp = sdv.echl_pbp(game_id)
print(schedule.height, standings.height, pbp.height)
```

## One parser for every league

The HockeyTech payloads are the same shape in every league (the `seasons` and
`scorebar` rows are byte-identical across all twenty), so one parser module drives
every family. The per-league differences are data, not code: the API key (public,
shipped in each league's site; override with `SDV_<LEAGUE>_API_KEY`), the client
code, and the league id the standings query takes.

Some leagues publish less:

- `ushl` and `mjhl` play-by-play has goals, penalties and goalie changes only, with no
  coordinates.
- `mjhl` refuses the game-summary feed, so `mjhl_game_summary` returns empty event
  frames and `mjhl_pbp` returns plays without game metadata.

Errors follow the package vocabulary: a 404 raises `NoDataError`, and any other
unusable reply (HockeyTech reports most errors as an HTTP 200 with an error in the
body) raises `AssetFetchError`.

## The 600 x 300 canvas

Every league reports shot and event locations in `x_coord` / `y_coord` on one
canvas: 600 by 300, origin at the top left, centre ice at (300, 150). The feed puts
the home team's attack toward x = 0 and the visitor's toward x = 600 in every period.
There is no other canvas, whatever the league.

The play-by-play also carries derived frames, ported from fastRhockey's
`pwhl_pbp`, all rotations of a centre-origin frame in feet (x from -100 to 100, y
from -42.5 to 42.5):

| Columns | Frame |
|---|---|
| `x_coord_original`, `y_coord_original` | the raw canvas |
| `x_coord_neutral`, `y_coord_neutral` | the canvas with centre ice at (0, 0) |
| `x_coord_fixed`, `y_coord_fixed` | feet, home team shooting right |
| `x_coord_right`, `y_coord_right` | feet, every team shooting right |
| `x_coord_vertical`, `y_coord_vertical` | feet, every team shooting up |

```python
shots = pbp.filter(pl.col("x_coord").is_not_null()).select(
    "event", "team_id", "x_coord", "y_coord", "x_coord_right", "y_coord_right"
)
print(shots.select(pl.col("x_coord").max(), pl.col("y_coord").max()))   # never past 600 / 300
```

`x_coord_right` / `y_coord_right` (and the vertical pair) are null where the side
is unknown, such as on a faceoff or in a game without its summary.

See the league pages, such as [ECHL](../../echl/index.md) and
[PWHL](../../pwhl/index.md), for every function's parameters and Returns table.
