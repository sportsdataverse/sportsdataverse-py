<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [EX-3.4-a guard captures](#ex-34-a-guard-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# EX-3.4-a guard captures

Seven ESPN college-football summary payloads for the quality program's task 3.4 follow-ups (EX-3.4-a, and the review of #905); the first two are for
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
- **R905-1** (review of #905), same source and form: 401760417 (Utah State @ Fresno State, 2025). Its "Br. Barnes run
  for 0 yds" (401760417125) is a ghost 4th & 1 record with a degenerate end block, sharing sequenceNumber 28 with the
  real 4th & 1 snap by the same team that follows; the test is
  `test_cfb_end_state_ex34.py::test_fourth_down_short_degenerate_end_is_left_alone`, which also reads 401760407
  (UNLV @ Colorado State, 2025): "L. Avant run for 0 yds" (401760407697), the same ghost shape before the same
  team's 4th & 1 punt.
- **R905-3**, same source and form: 401752832 (USC @ Purdue, 2025), whose "pass intercepted ... PENALTY USC ..."
  (401752832104944503) is an interception a flag wiped out, with a degenerate end block, before the same team's
  1st & 10 at the 14; and 400548284 (Utah @ Oregon State, 2014), whose OT1 "run for 4 yds to the OrgSt 19"
  (400548284106000241) is followed by the same drive's 3rd & 4 filed under OT2 (the same-period clause's only EPA
  row in the sweep, pinned as left alone); and 401752915 (Illinois @ Wisconsin, 2025), whose stray "K. Crimmins punt for 0 yds"
  (401752915563) has a degenerate end before Illinois's own snap (the play-type allowlist's killer, pinned as left
  alone).
- Play ids are ESPN's; the test casts them to Int64.

The names do not start with `summary_`, so the whole-game oracle (`summary_*` in `..`) does not pick them up.
