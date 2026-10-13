<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN NHL summary captures (task 2.15, clock direction)](#espn-nhl-summary-captures-task-215-clock-direction)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN NHL summary captures (task 2.15, clock direction)

Captured 2026-10-13 from `site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=<id>`,
trimmed to the keys `espn_nhl_pbp` reads (`header`, `plays`, `boxscore`, `gameInfo`, `pickcenter`,
`odds`, `format`); `plays` is unmodified. Used by `tests/nhl/test_nhl_pbp.py`.

| File | Game | Why |
|---|---|---|
| `summary_401672983.json` | 2024-05-23 DAL 2, EDM 3 (2OT), WCF G1, season type 3 | playoff overtime is 20:00 (P4 runs 0:00 -> 20:00) |
| `summary_401688272.json` | 2025-01-11 MTL at DAL, shootout, season type 2 | regular-season overtime ends at 5:00; the shootout (P5) clock is 0:00 on every row |

ESPN's NHL `clock.displayValue` is elapsed time in the period; every period opens with a 0:00
`Period Start` row and closes with a `Period End` row at the period's length.
