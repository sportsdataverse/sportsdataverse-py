<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Kickoff fair-catch spot captures](#kickoff-fair-catch-spot-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Kickoff fair-catch spot captures

Four ESPN college-football summary payloads for `tests/cfb/test_cfb_kickoff_fair_catch_spot.py`, which also reads
`../supp002/401761641_trimmed.json.gz`.

- **Source:** `https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=<game_id>`,
  as stored by the scraper in `cfbfastR-cfb-raw/cfb/json/raw/<game_id>.json`, raw repo at `8480d4db3f` (the four
  files last changed in `b598a0f3c0` and `c5a4bbbff2`, 2026-07-29); copied 2026-11-01.
- **Form:** `<game_id>_trimmed.json.gz`, the `trimmed` form of `../README.md`: the payload reduced to `boxscore`,
  `drives`, `gameInfo`, `header`, `pickcenter` and `scoringPlays`, whole game kept, gzip with mtime 0.
- **Selection:** from a scan of every fair-caught kickoff in the cfb-raw corpus (2,232 rows, 2004-2026), games with
  verified cases where the next snap starts at the rule's spot:
  - 401411154 (NC State @ Wake Forest, 2022): a catch at the NCSt 30 (next snap at 70) and two inside the 25 (the
    WFrst 1 and 0, next snap at 75), in the 2014-2024 text form.
  - 401757282 (New Mexico State @ Liberty, 2025): a catch at the LU27 with a 15-yard foul enforced from the spot to
    the LU42 (next snap at 58), where ESPN's end is 80.
  - 401114199 (Oregon @ Oregon State, 2019): "OREGON ST Penalty, illegal fair catch signal (10 Yards) to the OrgSt 15",
    a foul and not a catch (next snap at 85, not the 25).
  - 400933913 (Auburn @ Georgia, 2017): fair catches at the Geo 21 and 27 before the 2018 rule, dead at the spot
    (next snaps at 79 and 73).
- Play ids are ESPN's; the test casts them to Int64.
