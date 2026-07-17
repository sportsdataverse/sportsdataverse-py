<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_ncaa fixtures](#cfb_ncaa-fixtures)
  - [Box-score tabs (contest 5362283 — California @ Auburn, 2024-09-07)](#box-score-tabs-contest-5362283--california--auburn-2024-09-07)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_ncaa fixtures

Real `stats.ncaa.org` college-football (NCAA sport code `MFB`) play-by-play
game pages, captured via the browser transport in
`sportsdataverse/mbb/mbb_ncaa_fetch.py` (patchright + `--headless=new` + US
residential IP) from `https://stats.ncaa.org/contests/{id}/play_by_play`.

Consumed by `tests/cfb/test_cfb_ncaa_pbp.py` (offline parser tests).

| file | contest_id | source URL | captured |
|---|---|---|---|
| `cfb_ncaa_pbp_5362535.html` | 5362535 | <https://stats.ncaa.org/contests/5362535/play_by_play> | 2026-07-16 |
| `cfb_ncaa_pbp_5336803.html` | 5336803 | <https://stats.ncaa.org/contests/5336803/play_by_play> | 2026-07-16 |
| `cfb_ncaa_pbp_5361446.html` | 5361446 | <https://stats.ncaa.org/contests/5361446/play_by_play> | 2026-07-16 |

The full 6-game capture corpus lives in the `ncaa-mfb-football-raw` producer repo;
three games are vendored here to prove the parser generalizes across games
(0 unknown play types on every game).

## Box-score tabs (contest 5362283 — California @ Auburn, 2024-09-07)

Consumed by `tests/cfb/test_cfb_ncaa_box.py`. The other stats.ncaa.org tabs of one
game, for the box/drives/officials parsers in `cfb/cfb_ncaa_box.py`:

| file | tab | source URL |
|---|---|---|
| `mfb_drives_5362283.html` | Drives | <https://stats.ncaa.org/contests/5362283/drives> |
| `mfb_team_stats_5362283.html` | Team Stats (by period) | <https://stats.ncaa.org/contests/5362283/team_stats> |
| `mfb_individual_stats_5362283.html` | Individual Stats | <https://stats.ncaa.org/contests/5362283/individual_stats> |
| `mfb_officials_5362283.html` | Officials | <https://stats.ncaa.org/contests/5362283/officials> |
| `mfb_box_score_5362283.html` | Box Score (linescore + game info) | <https://stats.ncaa.org/contests/5362283/box_score> |
