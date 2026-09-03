<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fox Bifrost fixtures](#fox-bifrost-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Fox Bifrost fixtures

Real captures from `api.foxsports.com/bifrost/v1` (public data-tier key),
captured 2026-08-07. Used by `tests/test_fox_wbb_wnba_offline.py`.

| File | URL |
|---|---|
| `wnba_team_3_standings.json` | `/wnba/team/3/standings` (league-wide WNBA standings; team directory source) |
| `wcbk_team_11_standings.json` | `/wcbk/team/11/standings` (Big East standings from the UConn seed) |
| `nba_team_1_standings.json` | `/nba/team/1/standings` (league-wide NBA standings, all 30 teams; team directory source) |

Captured 2026-06-09/12 by the `sdv-internal-refs/fox` crawl, used by
`tests/test_fox_layout.py` (shared layout layer + per-league builder):

| File | URL |
|---|---|
| `cbk_league_teamnav.json` | `/cbk/league/teamnav` (flat `navItems` team directory) |
| `cbk_league_conferences.json` | `/cbk/league/conferences` (`groups[].items[]` conference directory) |
| `cbk_league_scores.json` | `/cbk/league/scores` (`groupList` / `dailyList` nav selections) |
| `cbk_team_roster.json` | `/cbk/team/27/roster` |
| `cbk_team_gamelog.json` | `/cbk/team/27/gamelog` |
| `cbk_event_data_pbp_first_half.json` | `/cbk/event/262052/data`, **trimmed** to `pbp.sections[0].groups[0]` (1ST HALF, 158 plays) |
| `cfb_event_standings.json` | `/cfb/event/{id}/standings` |
| `nfl_league_header.json` | `/nfl/league/header` |
| `nfl_team_header.json` | `/nfl/team/10/header` |
| `nfl_league_playernews.json` | `/nfl/league/playernews` |
| `nfl_league_stats.json` | `/nfl/league/stats` (`leadersSections`) |
| `nba_league_odds.json` | `/nba/league/odds` (`six-pack` modules) |
| `nba_event_matchup.json` | `/nba/event/{id}/matchup` |
| `nba_event_recap.json` | `/nba/event/{id}/recap` (`topPerformers`) |
| `ufl_event_odds.json` | `/ufl/event/{id}/odds` (a populated `sixPack`) |
| `ufl_league_stats_con_team.json` | `/ufl/league/stats-con/team/{cat}/0` |
| `wnba_team_stats.json` | `/wnba/team/3/stats` (`leadersSections`) |
| `soccer_league_scores_segment.json` | `/soccer/league/scores-segment/c1d20260519` |
| `topevents_scoreboard_segment_1.json` | `/topevents/scoreboard/segment/1` |
| `explore_browse_sports_main.json` | `/explore/browse/sports/main` |
| `search_entities.json` | `/search/entities?text=chiefs` |
| `trending_videos.json` | `/general/trending/videos` (feed key), **trimmed** to the first 3 results |

Two files are marked **trimmed**: they are unmodified captured JSON with a
list truncated (the CMS documents and the pbp half are otherwise ~1 MB each).
Every value asserted in the tests comes from the capture as served.

The CFB Fox pbp fixture lives separately at
`tests/cfb/fixtures/fox_cfb_event_41616_data.json`.
