<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [mbb_ncaa fixtures](#mbb_ncaa-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# mbb_ncaa fixtures

Real `stats.ncaa.org` basketball contest-tab pages, captured via the browser
transport in `sportsdataverse/mbb/mbb_ncaa_fetch.py` (patchright, new-headless,
US residential IP). Consumed by `tests/mbb/test_mbb_ncaa_box_tabs.py` and
`tests/wbb/test_wbb_ncaa_box_tabs.py` (the officials / team-stats / linescore
parsers in `mbb/mbb_ncaa_box_tabs.py`).

Contest 5722355 — Coppin St. @ South Carolina (WBB), 2024-11-14:

| file | tab | source URL |
|---|---|---|
| `bkb_officials_5722355.html` | Officials | <https://stats.ncaa.org/contests/5722355/officials> |
| `bkb_team_stats_5722355.html` | Team Stats (by period) | <https://stats.ncaa.org/contests/5722355/team_stats> |
| `bkb_box_score_5722355.html` | Box Score (linescore + game info) | <https://stats.ncaa.org/contests/5722355/box_score> |

The other basketball tabs are covered elsewhere: `/individual_stats` by
`parse_ncaa_bb_box` (`mbb_ncaa_box_stats`), `/play_by_play` by `mbb_ncaa_game_pbp`.
The pages are league-agnostic, so WBB re-exports the same parsers.
