---
title: Unreleased changes
---

# Unreleased changes

Merged to `main` since 0.1.5 and not yet released. Released versions are on the [Changelog](/CHANGELOG).

## Unreleased

### Fixed

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
