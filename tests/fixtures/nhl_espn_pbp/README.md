<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN NHL summary captures (task 2.15, clock direction)](#espn-nhl-summary-captures-task-215-clock-direction)
  - [NHL API play-by-play of the same games](#nhl-api-play-by-play-of-the-same-games)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN NHL summary captures (task 2.15, clock direction)

Captured 2026-10-13 from `site.api.espn.com/apis/site/v2/sports/hockey/nhl/summary?event=<id>`,
trimmed to the keys the clock path reads (`header`, `plays`, `boxscore`, `gameInfo`, `pickcenter`,
`odds`, `format`); other parser keys take their defaults. `plays` is unmodified. Used by
`tests/nhl/test_nhl_pbp.py`.

| File | Game | Why |
|---|---|---|
| `summary_401672983.json` | 2024-05-23 EDM 3 at DAL 2 (2OT), WCF G1, season type 3; NHL id 2023030321 | playoff overtime is 20:00 (P4 runs 0:00 -> 20:00) |
| `summary_401688272.json` | 2025-01-11 DAL at MTL (MTL home), shootout, season type 2; NHL id 2024020673 | regular-season overtime ends at 5:00; the shootout (P5) clock is 0:00 on every row |

ESPN's NHL `clock.displayValue` is elapsed time in the period; every period opens with a 0:00
`Period Start` row and closes with a `Period End` row at the period's length.

## NHL API play-by-play of the same games

`nhl_api_pbp_2023030321.json` and `nhl_api_pbp_2024020673.json` are the `pbp_raw` member
(`api-web.nhle.com/v1/gamecenter/<id>/play-by-play`) of `nhl/json/raw/<id>.json` in
`sportsdataverse/fastRhockey-nhl-raw` (commits 161a6eed5, 2026-09-10, and 44cd0765b,
2026-05-29), trimmed to the game header and each play's `eventId`, `periodDescriptor`,
`timeInPeriod`, `timeRemaining` and `typeDescKey`. They are the independent oracle for the
ESPN-derived `start.period_seconds_remaining`; the 2024 SCF G7 counterpart is
`../nhl_api_web/pbp_2024_scf_g7.json`.
