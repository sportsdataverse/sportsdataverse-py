<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`cfb_2026_big12_partial` — real 2026 Big 12 conference games through week 10](#cfb_2026_big12_partial--real-2026-big-12-conference-games-through-week-10)
  - [Provenance](#provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `cfb_2026_big12_partial` — real 2026 Big 12 conference games through week 10

This fixture holds the real 2026 Big 12 regular-season conference games
completed through week 10 (48 games, 16 teams), with final scores, captured
on 2026-11-09 while the season was still in progress. No completed
2014-2025 Big 12 season has a tie that reaches the `record_vs_common_desc`
rung in a way where the common-opponents-only reading matters. This partial
season does, so it is frozen here as the Big 12 regression case for
sportsdataverse/sportsdataverse-py#1140:

- **Cincinnati, Iowa State, TCU and West Virginia (tied):** the old reading
  compared them against an opponent tier that not all four had played, and
  that seeded TCU first (10th). With only opponents all four played, the
  descent cannot separate them, opponents' conference win % orders them, and
  TCU finishes 13th.
- **Colorado, Kansas and Kansas State (tied):** the same thing happens. The
  old reading put Kansas 14th; it now finishes 16th.

Both engines must give these ranks: sdv-py's
`tests/cfb/test_cfb_standings.py::TestCommonOpponentDescent` and cfbseedR's
`tests/testthat/test-tiebreakers.R`. These are standings as of week 10, not
final standings.

## Provenance

- **Source:** `sportsdataverse.cfb.load_cfb_schedule([2026])` (the
  sportsdataverse-data `cfb_schedules` release), run through cfbseedR's
  `cfb_games_from_schedule()`.
- **Filter:** completed `REG` games with both teams in the Big 12.
- **Files:** `games.csv` holds the game rows (with `season`); `teams.csv`
  holds the 16 teams.
