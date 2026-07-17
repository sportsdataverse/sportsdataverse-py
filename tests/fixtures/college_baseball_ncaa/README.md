<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [college_baseball_ncaa fixtures](#college_baseball_ncaa-fixtures)
  - [Box-score tabs (contest 6357953 — Kansas @ A&M-Corpus Christi, 2025-02-14)](#box-score-tabs-contest-6357953--kansas--am-corpus-christi-2025-02-14)
  - [Real softball (WSB) capture (contest 6548848 — Elon @ Saint Joseph's, 2025-04-12)](#real-softball-wsb-capture-contest-6548848--elon--saint-josephs-2025-04-12)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# college_baseball_ncaa fixtures

Real `stats.ncaa.org` college-baseball (NCAA sport code `MBA`) play-by-play game
pages, captured via the browser transport in
`sportsdataverse/mbb/mbb_ncaa_fetch.py` (patchright + `--headless=new` + US
residential IP) from `https://stats.ncaa.org/contests/{id}/play_by_play`.

Consumed by `tests/baseball/test_college_baseball_ncaa_pbp.py` and (as a
structural stand-in) `tests/baseball/test_college_softball_ncaa_pbp.py`.

| file | contest_id | final | captured |
|---|---|---|---|
| `mba_pbp_6357953.html` | 6357953 | 8-5 | 2026-07-17 |
| `mba_pbp_6356679.html` | 6356679 | 12-1 | 2026-07-17 |
| `mba_pbp_6356680.html` | 6356680 | 8-0 | 2026-07-17 |

Source URLs follow `<https://stats.ncaa.org/contests/{contest_id}/play_by_play>`.

## Box-score tabs (contest 6357953 — Kansas @ A&M-Corpus Christi, 2025-02-14)

Consumed by `tests/baseball/test_college_baseball_ncaa_box.py`. The other
stats.ncaa.org tabs of one game, for the box parsers in
`baseball/college_baseball/college_baseball_ncaa_box.py`:

| file | tab | source URL |
|---|---|---|
| `bsb_box_score_6357953.html` | Box Score (linescore + game info) | <https://stats.ncaa.org/contests/6357953/box_score> |
| `bsb_team_stats_6357953.html` | Team Stats (by inning) | <https://stats.ncaa.org/contests/6357953/team_stats> |
| `bsb_individual_stats_6357953.html` | Individual Stats (batting/pitching/fielding) | <https://stats.ncaa.org/contests/6357953/individual_stats> |
| `bsb_situational_stats_6357953.html` | Situational Stats | <https://stats.ncaa.org/contests/6357953/situational_stats> |

The `Umpires` tab (`/umpires`) is a known gap — the fetch fails to resolve and
the `/officials` path carries no umpire data; deferred as a TODO.

## Real softball (WSB) capture (contest 6548848 — Elon @ Saint Joseph's, 2025-04-12)

A genuine softball game, discovered live via the scoreboard route
(`season_divisions/18763/scoreboards`) and used by the softball tests instead of a
baseball stand-in. Softball pbp differs from baseball (last-name-only players, `;`
clause separator, `stole home` runs) — the shared parser handles both.

| file | tab |
|---|---|
| `wsb_pbp_6548848.html` | Play By Play |
| `wsb_team_stats_6548848.html` | Team Stats |
| `wsb_individual_stats_6548848.html` | Individual Stats |
| `wsb_box_score_6548848.html` | Box Score |
