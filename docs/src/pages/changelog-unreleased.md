---
title: Unreleased changes
---

# Unreleased changes

Merged to `main` since 0.1.5 and not yet released. Released versions are on the [Changelog](/CHANGELOG).

## Unreleased

### Changed

- **Tests:** warnings are errors. A test asserts an expected warning with `pytest.warns(..., match=...)` or
  filters an incidental one by exact message; live tests skip on a timeout or upstream 429/5xx, the
  live job runs on Ubuntu, and the `tests` extra needs pytest >= 8.0. (#726)

### Deprecated

- **NBA / WNBA:** `load_nba_stats_{pbp,possessions,lineups}_v3` and the four `load_wnba_stats_*` shims
  (`lineups`, `player_season_stats`, `standings`, `team_season_stats`) now say they will be removed in
  0.2.0; they named 0.1.0, which has shipped. A test fails if a removal version falls behind again. (#726)

### Fixed

- **CFB:** `get_go_wp` returns NaN for a 4th-down row with no `yards_to_goal` or `distance`, instead of
  a garbage go-for-it value from casting NaN to an integer; other plays are unchanged. (#726)
- **CFB:** scoring zero rows (QBR on a live game's opening drive, `predict_from_card` on an empty frame)
  no longer logs XGBoost's "Empty dataset" warning. (#726)
- **ESPN rankings parsed to an empty frame.** `espn_{cfb,mbb,wbb,mch,wch}_rankings()` ran the Site v2
  payload through `parse_items`, which reads a Core v2 `items` list; the polls sit in a top-level
  `rankings` list, so every league returned zero rows. A dedicated `parse_rankings` now gives one row
  per ranked or vote-receiving team per poll (live 2026-10-08: cfb 208 rows over 5 polls, mbb 77, wbb
  75, mch 35, wch 20), with `poll_id`, `season`, `season_type`, `week` (the week within the season
  type, which ESPN's Core v2 rankings URL uses), `rank` (null on vote-receiving rows), `previous_rank`,
  `points`, `first_place_votes` and the team fields; `team_id` is a string. The college baseball and
  softball wrappers route through it too, but ESPN answers their rankings URL with a 404. (#732)
- **HockeyTech schedules returned other seasons' games.** `<league>_schedule(season=...)` for
  the 19 HockeyTech league families, and `pwhl_schedule`, read `modulekit/scorebar`. That view
  ignores the `season_id` it is sent, returns games oldest-first and stops at its 10,000-row
  limit, so `ahl_schedule(season=2026)` returned 10,000 AHL games from 1995 to 2012 and none
  from 2025-26; OHL and ECHL failed the same way, and PWHL returned every season's games. The
  schedule functions now read `modulekit/schedule&season_id=<id>`, which the feed scopes to one
  season (live 2026-10-08: AHL 2025-26 = 1,152 games, OHL = 682, PWHL = 120, each in that season
  alone), and `parse_schedule(..., season_id=)` drops any row from another season (#727).
  Behaviour changes:
  - With neither `season` nor `season_id`, the schedule is the newest regular season, as in the
    other season-aware functions. It used to be the feed's whole window.
  - `game_status` now reads the schedule view's strings: `Final OT` where scorebar said
    `Final 1st OT`, and the start time (`7:00 pm EST`) for an unplayed game.
  - A season's playoffs and preseason are separate HockeyTech season ids; pass `season_id` for
    them.
  - An empty reply, or a season filter that keeps nothing, is zero rows with the 12 columns.
- **HockeyTech derived coordinates put home-team events off the rink.** `add_coord_transforms`
  (every `<league>_pbp` and `pwhl_pbp`) flipped home events with a 0-200 x 0-85 mirror applied
  to centre-origin feet, so home `x_coord_right` ran 191 to 290 ft and `y_coord_right` 49 to
  125 ft (PWHL game 42), and `y_coord_fixed` applied the y transform twice. The feed puts the
  home team's attack toward x = 0 in every period (320 PWHL games; sdv-internal-refs #50), and
  the columns are now rotations of the feet frame (#51): `x_coord_fixed` / `y_coord_fixed` =
  (-x, -y), home team shooting right; `x_coord_right` / `y_coord_right` = home (-x, -y),
  visitor (x, y); `x_coord_vertical` / `y_coord_vertical` = (-y_right, x_right). The right and
  vertical columns are null when an event's side is unknown (no team, as on faceoffs, or a null
  or empty home team id) instead of defaulting to the visitor's. `shot_distance`, `shot_angle`
  and xG are unchanged. The published PWHL play-by-play releases keep the old values until they
  are rebuilt.
- **HockeyTech empty-net goals from a team's own half were measured to the wrong net.**
  `add_shot_distance_angle` measures to the nearer net (`89 - |x|`), which per-event validation
  showed is right for own-half events with a goalie in net: they are near-net events whose
  coordinates the feed mirrored (own-half quality shots match ordinary ones at 10 / 24 / 35 ft).
  An own-half empty-net goal is a genuine long shot (27 in 320 PWHL games; median 126 ft,
  measured 59 ft). Empty-net goals (`empty_net` "1") now measure to the net their team attacks;
  every other event is unchanged (sdv-internal-refs #52). `shot_distance`, `shot_angle`,
  `scoring_chance` and the xG features of those goals change.
- **NBA:** `logistic_fit_irls` no longer overflows on separable data, and `nba_rapm` fits a single
  possession without a divide-by-zero warning. (#726)
- **NFL:** `get_go_wp` on a full nflverse frame no longer raises pandas `PerformanceWarning`s; output is
  unchanged. (#726)
