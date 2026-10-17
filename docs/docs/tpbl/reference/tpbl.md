---
title: TPBL — TPBL API (api.tpbl.basketball, Taiwan)
sidebar_label: TPBL API (api.tpbl.basketball, Taiwan)
description: "TPBL — TPBL API (api.tpbl.basketball, Taiwan) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# TPBL — TPBL API (api.tpbl.basketball, Taiwan)

`sportsdataverse.basketball.tpbl` — 16 endpoints.

> **Note:** Unofficial and unsupported: these are the routes the tpbl.basketball site's own front end calls on api.tpbl.basketball (read from its JavaScript bundle). The API is keyless and needs no headers; tpbl.basketball/robots.txt answers 200 with an empty body (no rules, no crawl-delay). `tpbl_runtime` still spaces requests at least 1 s apart. SportsDataverse has not ruled on the site's terms, so payloads are not redistributed as release assets.

> **Note:** Coverage: three seasons so far (2024-25 = season 2025, to 2026-27 = season 2027, whose regular season starts 2026-10-17). There is NO play-by-play route (none of the site's 131 JavaScript chunks calls one); the two shot lists carry court coordinates only, with no shooter, game, quarter or time. Game wrappers serve FINISHED games only (an unfinished game raises AssetFetchError). Names (teams with their sponsor, venues, players, referees) are Traditional Chinese; the description of each code column translates it.

> **Note:** Ids: every route takes TPBL's own ids (season, division, team, game, player); the gohoops_* ids belong to GoHoops, the stats platform behind the league, and are echoed beside them. The box score keys its players by gohoops_roster_id (a person registered with one squad in one season), not by the TPBL player id: join tpbl_box_players to tpbl_roster on gohoops_roster_id. Season-keyed wrappers take the END year and the runtime maps it to the API's season id. Birth dates and contact fields are never returned.

> **Note:** Scope checks: the box-score wrappers fetch the game detail first (one extra request) and refuse a stats body whose teams, final scores, quarter count or player points disagree with it; the season game list, roster and player game log are checked against the object asked for. The two shot lists name no team, player or division in the body, so they cannot be scoped. tpbl_games(verify=True) additionally checks every division's completed games against its standings (falling back to the team-filtered lists for a division whose standings answer empty, as the 2024-25 playoffs do). tpbl_standings raises NoDataError for such a division.

## Box

| Function | Summary |
|---|---|
| [tpbl_box_players](tpbl/box.md#tpbl_box_players) | Whole-game box score lines of every player on either team sheet, keyed by GoHoops roster id. |
| [tpbl_box_player_periods](tpbl/box.md#tpbl_box_player_periods) | Per-quarter box score lines of the players who were on court, overtimes included. |
| [tpbl_box_teams](tpbl/box.md#tpbl_box_teams) | Whole-game team totals of both teams: shooting, rebounds, fouls, assists, turnovers and paint points. |
| [tpbl_box_team_periods](tpbl/box.md#tpbl_box_team_periods) | Per-quarter team lines of both teams: points scored and conceded in the quarter, running totals and stats. |

## Player

| Function | Summary |
|---|---|
| [tpbl_player_shots](tpbl/player.md#tpbl_player_shots) | Every field-goal attempt of one player in one division with its court position. |
| [tpbl_player_games](tpbl/player.md#tpbl_player_games) | A player's game log in one division: one row per game played, with that game's whole stat line. |

## Other

| Function | Summary |
|---|---|
| [tpbl_seasons](tpbl/other.md#tpbl_seasons) | Every TPBL season with its TPBL and GoHoops ids, label, status and date window. |
| [tpbl_divisions](tpbl/other.md#tpbl_divisions) | The divisions (preseason, regular season, play-in, playoffs, finals) of one season. |
| [tpbl_teams](tpbl/other.md#tpbl_teams) | The teams of one season (bracket placeholders flagged). |
| [tpbl_games](tpbl/other.md#tpbl_games) | Every game of one season, all divisions, played and scheduled, with teams, venue, status and score. |
| [tpbl_standings](tpbl/other.md#tpbl_standings) | League table of one division: rank, record, games behind and streak. |
| [tpbl_standings_against](tpbl/other.md#tpbl_standings_against) | Head-to-head record of every team against every other team in one division. |
| [tpbl_game](tpbl/other.md#tpbl_game) | Header of one finished game: teams with scores, venue, status, quarters, tip-off, attendance and links. |
| [tpbl_referees](tpbl/other.md#tpbl_referees) | The officiating crew of one finished game, chief referee first (names only). |
| [tpbl_roster](tpbl/other.md#tpbl_roster) | The registered roster of one team in one division (birth dates omitted). |
| [tpbl_team_shots](tpbl/other.md#tpbl_team_shots) | Every field-goal attempt of one team in one division with its court position (optionally per quarter). |
