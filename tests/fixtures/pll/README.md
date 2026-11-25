<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Premier Lacrosse League stats fixtures](#premier-lacrosse-league-stats-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Premier Lacrosse League stats fixtures

Captured 2026-11-25 from `https://stats.premierlacrosseleague.com/api/...` through the `pll_*` wrappers with
`return_parsed=False`, so each file is exactly what the runtime getter returns: the response's `data` object with the
request stamped under `_request`. Nothing is trimmed. No key or token is involved; the requests carried the stats
site's own `Referer`.

| File | Route | Arguments |
|---|---|---|
| `schedule_2026.json` | `/api/v4/events` | `year=2026, include_champ_series=True` |
| `game_2026-ev-4.json` | `/api/v4/events/{event_slug}` | `event_slug="2026-ev-4"` (Waterdogs 16, Whipsnakes 14) |
| `play_by_play_2026-ev-4.json` | `/api/v4/events/{event_slug}/playlogs` | `event_slug="2026-ev-4"` |
| `play_by_play_cannons-redwoods-2021-6-04.json` | `/api/v4/events/{event_slug}/playlogs` | a 2021 game: empty shot `details`, running score on every row |
| `player_box_2026-ev-4.json` | `/api/v4/events/{event_slug}/players/stats` | `event_slug="2026-ev-4"` |
| `team_box_2026-ev-4.json` | `/api/v4/events/{event_slug}/teams/stats` | `event_slug="2026-ev-4"` |
| `game_insights_2026-ev-4.json` | `/api/v4/events/{event_slug}/insights` | `event_slug="2026-ev-4"` |
| `standings_2026.json` | `/api/v4/standings` | `year=2026` |
| `team_season_stats_2026_regular.json` | `/api/v4/teams/season-stats` | `year=2026` (segment `regular`) |
| `team_stats_waterdogs_2026.json` | `/api/v4/teams/{team_slug}/stats` | `team_slug="waterdogs", year=2026` |
| `team_roster_waterdogs_2026_regular.json` | `/api/v4/teams/{team_slug}/roster` | `team_slug="waterdogs", year=2026` |
| `player_season_stats_2026_regular.json` | `/api/v4/players/season-stats` | `year=2026` |
| `player_advanced_stats_2026_regular.json` | `/api/v4/players/stats/advanced` | `year=2026` |
| `season_leaders_2026_regular.json` | `/api/v4/players/stats/season-leaders` | `year=2026, limit=5` (every stat) |
| `career_leaders_points_goals_saves.json` | `/api/v4/players/stats/career-leaders` | `stats=["points", "goals", "saves"], limit=5` |
| `player_michael-sowers.json` | `/api/v4/players/{player_slug}` | `player_slug="michael-sowers", year=2026` |
| `player_seasons_michael-sowers.json` | `/api/v4/players/{player_slug}/stats/seasons` | `player_slug="michael-sowers"` |
| `glg_schedule_intlm_2026.json` | `/api/glg/{glg_league}/games` | `glg_league="intlm", year=2026` |
| `glg_standings_intlm_2026.json` | `/api/glg/{glg_league}/standings` | `glg_league="intlm", year=2026` |
| `wll_schedule_2026.json` | `/api/wll/v1/events` | `year=2026` |

The route catalog, field glossary and probes live in sdv-internal-refs `pll/`.
