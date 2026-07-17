<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [college_baseball_ncaa fixtures](#college_baseball_ncaa-fixtures)

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
