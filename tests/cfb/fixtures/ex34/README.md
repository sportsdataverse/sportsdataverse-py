<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [EX-3.4-a guard captures](#ex-34-a-guard-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# EX-3.4-a guard captures

Two ESPN college-football summary payloads for the quality program's task 3.4 follow-ups (EX-3.4-a); the test is
`tests/cfb/test_cfb_end_state_ex34.py::test_mirror_repair_across_an_admin_row_needs_the_stated_yardage`.

- **Source:** `https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=<game_id>`,
  as stored by the scraper in `cfbfastR-cfb-raw/cfb/json/raw/<game_id>.json`, raw repo at `00f1fae99f`
  (the two files last changed in `b598a0f3c0`, 2026-07-29); copied 2026-10-19.
- **Form:** `<game_id>_trimmed.json.gz`, the `trimmed` form of `../README.md`: the payload reduced to `boxscore`,
  `drives`, `gameInfo`, `header`, `pickcenter` and `scoringPlays`, whole game kept, gzip with mtime 0.
- **Selection:** two of the rows a base-vs-head sweep over 2,591 cfb-raw games found the unguarded EX-3.4-a repair
  moving the wrong way (the end agrees with the play's start, `statYardage` and text; the next real snap after the
  admin row starts at the complement):
  - 401282772 (Western Michigan @ Michigan, 2021): "Cade McNamara run for 2 yds to the WMich 11", then a Michigan
    Timeout, then a snap whose start ESPN files at yardsToEndzone 0 (the processor reads its yardLine 11 as 89).
  - 401761589 (App State @ Charlotte, 2025): "Conner Harrell run for no gain to the CLT 48", then End of 3rd
    Quarter, then a Q4 snap filed 3rd & 6 at 48 from the other side.
- Play ids are ESPN's; the test casts them to Int64.

The names do not start with `summary_`, so the whole-game oracle (`summary_*` in `..`) does not pick them up.
