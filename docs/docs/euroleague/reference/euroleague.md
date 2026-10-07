---
title: EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api)
sidebar_label: EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api)
description: "EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# EUROLEAGUE — EuroLeague APIs (api-live.euroleague.net v2 + v3, live.euroleague.net/api)

`sportsdataverse.euroleague` — 15 endpoints.

## Game

| Function | Summary |
|---|---|
| [euroleague_game_boxscore](euroleague/game.md#euroleague_game_boxscore) | Box score of one game: per-player and team totals per side, by-quarter scores, referees, attendance. |
| [euroleague_game_header](euroleague/game.md#euroleague_game_header) | Game header: teams, codes, coaches, score by quarter, venue, referees. |
| [euroleague_game_pbp](euroleague/game.md#euroleague_game_pbp) | Play-by-play of one game, one array per quarter (FirstQuarter ... ForthQuarter, ExtraTime). |
| [euroleague_game_points](euroleague/game.md#euroleague_game_points) | Shot chart of one game: one row per made/missed FG and made FT, with COORD_X/COORD_Y in cm from the hoop (the shot-chart source). |
| [euroleague_game_report](euroleague/game.md#euroleague_game_report) | Game report: date, round, phase, both clubs with score and last-5 form. |
| [euroleague_game_stats](euroleague/game.md#euroleague_game_stats) | Box score of one game. |

## Other

| Function | Summary |
|---|---|
| [euroleague_clubs](euroleague/other.md#euroleague_clubs) | Clubs in a season. |
| [euroleague_competitions](euroleague/other.md#euroleague_competitions) | Competitions (EuroLeague E, EuroCup U, ...). |
| [euroleague_games](euroleague/other.md#euroleague_games) | Games of a season. |
| [euroleague_people](euroleague/other.md#euroleague_people) | People (players, coaches) in a season. |
| [euroleague_player_stats](euroleague/other.md#euroleague_player_stats) | Season player stats, traditional (box-score totals or per-game averages) or advanced (eFG%, TS%, rebound / assist / turnover rates), one row per player. |
| [euroleague_rounds](euroleague/other.md#euroleague_rounds) | Rounds of a season. |
| [euroleague_seasons](euroleague/other.md#euroleague_seasons) | Seasons of a competition. |
| [euroleague_standings](euroleague/other.md#euroleague_standings) | Standings as of a round: basic (W-L, points, home/away/last-10 records), calendar (per-round result streaks), streaks (longest win/loss runs) or aheadbehind … |
| [euroleague_team_stats](euroleague/other.md#euroleague_team_stats) | Season team stats, traditional (box-score totals or per-game averages) or advanced (eFG%, TS%, four-factor style rates), one row per team. |
