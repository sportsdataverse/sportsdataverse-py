---
title: EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore)
sidebar_label: EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore)
description: "EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 10
toc_max_heading_level: 2
---
# EASL — EASL sync server (easl.joshricha.com, third-party over Sportradar DataCore)

`sportsdataverse.basketball.easl` — 18 endpoints.

> **Note:** WARNING: UNOFFICIAL THIRD-PARTY INTERMEDIARY. These wrappers read easl.joshricha.com/api/v1, an Express server run by an individual. It is not EASL's and not Sportradar's: it mirrors Sportradar DataCore (organisation b1e0z) into SQLite and serves it keyless, and it can change or vanish without notice. Its routes, field names (steak, facebook_username) and sync freshness are the proxy author's. A first-party DataCore feed needs credentials this package does not have. The host serves no robots.txt (404) and states no terms; the runtime waits at least 1 s before every request, retries included, and honours Retry-After. Payloads are not redistributed as release assets.

> **Note:** Seasons are END years (2026 = EASL 25-26). The server's season key is the START year; easl_runtime reads the season list, maps the END year to the DataCore seasonId and defaults to the ACTIVE season. Four editions exist (2023-24 to 2026-27). The server defaults to the active season on /matches, /standings and /leaders; the runtime always sends the resolved seasonId. A season is two calendar years: group by season_id, not by the year of the tip-off.

> **Note:** Finished games only: the game routes first read the fixture and refuse (AssetFetchError) one that is not CONFIRMED with two scores, a box score whose player points do not add up to the score (the server serves one-team, four-row boxes for some games), a play-by-play that does not end on the final score in its highest period (4 or more, and the last period of the period table), and team totals that disagree with the score. For the ACTIVE season's fixture lists the runtime reads /health: a server clock more than a day from yours, or a game still SCHEDULED more than a day after its tip-off, means the mirror has stalled (AssetFetchError). That proves the mirror kept up with the games played, not that a game is final upstream.

> **Note:** Ids: every DataCore id is a UUID v1 string. team_id = entityId, player_id = personId; fixture_number (YYSSNNN) is null on every 2023-24 fixture; *_row_id columns are the sync server's own integers, not DataCore ids. The fixture's competitors come in arbitrary order: read home_* / away_*, which the parsers take from isHome.

> **Note:** No shot x/y, no game clock and no lineups exist on this server. The play-by-play text always says 'missed' for field goals and 'Free throw K of M made' for free throws, made or not: use points_scored (the change of the event team's running score), which lags on a minority of events. 301 of 2,444 sampled box rows are players who did not play (null minutes and counting stats; points are 0); player_box.played tells them apart. easl_player_stats ignores a season filter: it is the career line over every season. Names: nameFullLocal is the English display name and nameFullLatin the native-script name, so the columns are name and name_native.

## Player

| Function | Summary |
|---|---|
| [easl_player_box](easl/player.md#easl_player_box) | Player box score of a finished game, one row per rostered player (non-players have null counting stats). |
| [easl_players](easl/player.md#easl_players) | Every EASL player of all seasons; with season and team_id a roster that adds jersey number and position. |
| [easl_player](easl/player.md#easl_player) | One player's biography: names, nationality, birth date, height, weight. |
| [easl_player_stats](easl/player.md#easl_player_stats) | A player's cumulative statistics over ALL seasons (the server ignores a season filter). |

## Team

| Function | Summary |
|---|---|
| [easl_team_box](easl/team.md#easl_team_box) | Team totals of a finished game, one row per team, with the opponent's totals as *_against columns. |
| [easl_team_box_by_period](easl/team.md#easl_team_box_by_period) | Team totals of a finished game by period, one row per team and period (overtimes after the fourth). |
| [easl_teams](easl/team.md#easl_teams) | The 12 ACTIVE EASL teams (historical teams resolve only through easl_team). |
| [easl_team](easl/team.md#easl_team) | One team's directory entry; resolves the historical teams easl_teams does not list. |
| [easl_team_schedule](easl/team.md#easl_team_schedule) | A team's fixtures in one season, shaped like easl_schedule. |
| [easl_team_stats](easl/team-2.md#easl_team_stats) | A team's cumulative season statistics: totals, per-game rates and the opponent's totals (*_against). |
| [easl_team_leaders](easl/team-2.md#easl_team_leaders) | A team's top-5 player leaders per category in one season, shaped like easl_leaders. |

## Other

| Function | Summary |
|---|---|
| [easl_health](easl/other.md#easl_health) | Liveness probe of the unofficial sync server: status, message and the server's own timestamp. |
| [easl_seasons](easl/other.md#easl_seasons) | Every configured EASL season (the four editions 2023-24 to 2026-27) with its DataCore season id and END year. |
| [easl_schedule](easl/other.md#easl_schedule) | Every fixture of one season: status, tip-off, stage, pool, venue and both teams with their scores. |
| [easl_game](easl/other.md#easl_game) | Header of one finished fixture: teams, final score, status, stage, pool, venue and attendance. |
| [easl_pbp](easl/other.md#easl_pbp) | Play-by-play of a finished game in delivered order, with the running score and the points each event scored. |
| [easl_standings](easl/other.md#easl_standings) | Group-stage standings of one season: rank within the pool, record, splits, points and the last five results. |
| [easl_leaders](easl/other.md#easl_leaders) | Tournament top-5 player leaders in nine categories (points, rebounds, assists, steals, blocks, shooting). |
