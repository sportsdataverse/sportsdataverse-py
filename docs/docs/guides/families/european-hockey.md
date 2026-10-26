---
title: European hockey
sidebar_label: European hockey
sidebar_position: 2
description: "The thirteen European hockey leagues under sportsdataverse.hockey (DEL, SHL, Liiga, KHL, SIHF and more) and the conventions they share."
---

# European hockey

Thirteen European hockey leagues each have a home under `sportsdataverse.hockey`.
Unlike the [HockeyTech leagues](hockeytech.md), they do not share a feed: each home
wraps its league's own data source, so the function sets differ from league to
league.

## The homes

| Import from | League | Source |
|---|---|---|
| `sportsdataverse.hockey.del_` | DEL (Germany) | the league's public eSports S3 feed |
| `sportsdataverse.hockey.icehl` | ICE Hockey League (Austria and neighbours) | the same eSports feed family |
| `sportsdataverse.hockey.eihl` | Elite Ice Hockey League (UK) | the same eSports feed family |
| `sportsdataverse.hockey.metal` | Metal Ligaen (Denmark) | the same eSports feed family |
| `sportsdataverse.hockey.elh` | Czech Extraliga | hokej.cz |
| `sportsdataverse.hockey.liiga` | Liiga (Finland) | the Liiga API |
| `sportsdataverse.hockey.shl` | SHL (Sweden) | the SHL site API |
| `sportsdataverse.hockey.ha` | HockeyAllsvenskan (Sweden) | the HockeyAllsvenskan API |
| `sportsdataverse.hockey.nl` | National League (Switzerland) | the National League API |
| `sportsdataverse.hockey.sihf` | Swiss leagues | the Swiss federation's data.sihf.ch |
| `sportsdataverse.hockey.magnus` | Ligue Magnus (France) | the Ligue Magnus API |
| `sportsdataverse.hockey.khl` | KHL | the KHL REST API and its webcaster feed |
| `sportsdataverse.hockey.vhl` | VHL | the VHL site |

**DEL is `del_`.** Its docs pages use the prefix `del`, and its functions are
`del_schedule`, `del_pbp` and so on, but the package is `hockey.del_`: `del` is a
Python keyword, so `import sportsdataverse.hockey.del` would not parse.

## What each home covers

Most homes have a schedule, standings, play-by-play and box scores; several add shot
locations, shifts, rosters and player pages. A sample:

| Home | Example functions |
|---|---|
| `del_` | `del_schedule`, `del_standings`, `del_pbp`, `del_shots`, `del_shifts`, `del_player_box`, `del_goalie_box` |
| `liiga` | `liiga_schedule`, `liiga_standings`, `liiga_game`, `liiga_shotmap`, `liiga_player_game_log` |
| `shl` | `shl_schedule`, `shl_standings`, `shl_pbp`, `shl_player_box`, `shl_roster` |
| `nl` | `nl_schedule`, `nl_standings`, `nl_pbp`, `nl_shots`, `nl_lineups` |
| `khl` | `khl_schedule`, `khl_standings`, `khl_pbp`, `khl_shots`, `khl_webcaster_events` |
| `sihf` | `sihf_results`, `sihf_standings`, `sihf_pbp`, `sihf_game_lineups`, the `sihf_team_*` and `sihf_player_*` leaderboards |

Each home also exports its parsers (`parse_liiga_games`, `parse_esports_pbp`, ...)
for working from raw payloads. The league pages list every function with its
parameters and Returns table: [DEL](../../del/index.md), [Liiga](../../liiga/index.md),
[SHL](../../shl/index.md), [SIHF](../../sihf/index.md) and the rest under
Hockey in the sidebar.

## Shared conventions

- **Seasons are END years.** `season=2026` is 2025-26 in every home; each runtime
  maps it to its provider's own key at the boundary.
- **Polite pacing.** Every runtime leaves at least one second between one answer and
  the next request to the same host, retries included.
- **Tidy by default.** Wrappers return a polars DataFrame; `return_parsed=False`
  gives the raw payload and `return_as_pandas=True` gives pandas.

```python
from sportsdataverse.hockey.del_ import del_schedule
from sportsdataverse.hockey.liiga import liiga_standings

liiga = liiga_standings(season=2026)          # 2025-26
games = del_schedule(season=2026)
print(liiga.height, games.height)
```

The SHL and its siblings sometimes key a call on a provider id rather than a season
(`shl_standings(ssgt_uuid=...)`); the league page shows where that id comes from.
