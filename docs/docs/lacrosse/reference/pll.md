---
title: Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer)
sidebar_label: Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer)
description: "Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# Lacrosse — Premier Lacrosse League stats API (stats.premierlacrosseleague.com; runtime only, no key, the site's Referer)

`sportsdataverse.lacrosse` — 19 endpoints.

> **Note:** Runtime only: each call sends one GET to `stats.premierlacrosseleague.com/api`, the proxy behind the Premier Lacrosse League's own stats site. sdv-py publishes no release assets from this API and ships no bulk or backfill tooling for it. Requests from one process are spaced at least half a second apart.

> **Note:** No token: the stats routes need no key and the site keeps its upstream key server-side. The proxy answers only requests carrying the stats site's own Referer (the runtime sends it); without it the answer is HTTP 403 "Origin not allowed" (AssetFetchError). No environment variable is read.

> **Note:** Seasons 2019 to 2026 (`year`). The per-game wrappers (game, play-by-play, box scores, insights) work from 2021: the 2019 and 2020 game slugs contain "@" and the proxy refuses them (NoDataError).

> **Note:** Ids are strings. Per-game wrappers take `event_slug` (from `pll_schedule`; the slug format changes by season, so read it, never build it), team wrappers `team_slug` (from `pll_team_season_stats`, e.g. waterdogs, boston-cannons) and player wrappers `player_slug` (from `pll_player_season_stats`). Team codes (WAT) and six-digit player ids are columns, not route keys.

> **Note:** ESPN: `espn_lacrosse_*` (league pll) already carries PLL scoreboards, schedules, standings and team pages, but ESPN's PLL summary has no play-by-play and empty box scores; these wrappers add play-by-play, box scores, season and advanced stats, leaders, rosters and bios. The two share no game id: join on the UTC start time and the team codes (ESPN's abbreviations equal `team_id` here).

## Player

| Function | Summary |
|---|---|
| [pll_player_box](pll/player.md#pll_player_box) | A game's player box score: one row per player (points, 1- and 2-point goals, assists, shots, ground balls, turnovers, faceoffs, saves, penalties, touches, … |
| [pll_player_season_stats](pll/player.md#pll_player_season_stats) | Every player's stat line for a season segment: one row per player (scoring, shooting, faceoffs, goalie, passing and per-touch rates). |
| [pll_player_advanced_stats](pll/player.md#pll_player_advanced_stats) | Advanced player stats for a season: one row per player (settled, fast-break and assisted goals, assist types, left- and right-handed shooting). |
| [pll_player](pll/player.md#pll_player) | One player's profile: team, position, jersey, birth date, height, weight, hometown, school, injury and contract status, seasons played, biography. |
| [pll_player_seasons](pll/player.md#pll_player_seasons) | One player's stat line for every season and segment he played. |

## Team

| Function | Summary |
|---|---|
| [pll_team_box](pll/team.md#pll_team_box) | A game's team box score: one row per team (scoring, shooting, faceoffs, clears, saves, special teams, possessions and time of possession). |
| [pll_team_season_stats](pll/team.md#pll_team_season_stats) | Every team's season totals and rates: one row per team (offense, defense, faceoffs, clears and rides, power play and penalty kill). |
| [pll_team_stats](pll/team.md#pll_team_stats) | One team's record and stat lines for a season: one row per segment it played (regular season, playoffs). |
| [pll_team_roster](pll/team.md#pll_team_roster) | One team's roster: one row per player (jersey, position, bio, injury and contract status). |

## Other

| Function | Summary |
|---|---|
| [pll_schedule](pll/other.md#pll_schedule) | A season's schedule and results: one row per game (teams, score, UTC start time, segment, week, venue, broadcast and the event_slug every per-game wrapper … |
| [pll_game](pll/other.md#pll_game) | One game's header: teams and records, score by period, clock, venue, broadcast and the pregame win probability. |
| [pll_play_by_play](pll/other.md#pll_play_by_play) | A game's play-by-play: one row per logged play (clock, play type, shot value, the players involved, the win probability after it; the score is the running … |
| [pll_game_insights](pll/other.md#pll_game_insights) | The league's written insights on a game: one row per streak, milestone or matchup note. |
| [pll_standings](pll/other.md#pll_standings) | Standings: one row per team (record, scores for and against, conference record; seeds, null until the league sets them). |
| [pll_season_leaders](pll/other.md#pll_season_leaders) | A season's leaderboards in long format: one row per stat and ranked player. |
| [pll_career_leaders](pll/other.md#pll_career_leaders) | All-time PLL leaderboards in long format: one row per stat and ranked player, with games and seasons played. |
| [pll_glg_schedule](pll/other.md#pll_glg_schedule) | Global Lacrosse Games (national teams) schedule and results: one row per game, in the pll_schedule columns. |
| [pll_glg_standings](pll/other.md#pll_glg_standings) | Global Lacrosse Games standings: one row per national team, in the pll_standings columns. |
| [pll_wll_schedule](pll/other.md#pll_wll_schedule) | Women's Lacrosse League schedule and results: one row per game, in the pll_schedule columns (the proxy serves no other WLL route). |
