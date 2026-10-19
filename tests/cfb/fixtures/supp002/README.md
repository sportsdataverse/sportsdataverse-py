<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [supp-002 candidate captures](#supp-002-candidate-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# supp-002 candidate captures

37 ESPN college-football summary payloads for quality program task 3.4(a) (supp-002) and EX-3.4-b; the
test is `tests/cfb/test_cfb_supp002_degenerate_end.py`, which also reads `../summary_401856682.json`.

- **Source:** `https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=<game_id>`,
  as stored by the scraper in `cfbfastR-cfb-raw/cfb/json/raw/<game_id>.json`, raw repo at `98deb3f091`
  (2026-10-11); copied 2026-10-12.
- **Form:** `<game_id>_trimmed.json.gz`, the `trimmed` form of `../README.md`: the payload reduced to
  `boxscore`, `drives`, `gameInfo`, `header`, `pickcenter` and `scoringPlays`, whole game kept, gzip
  with mtime 0.
- **Selection**, three groups:
  - 22 games: every published `cfbfastR-cfb-data` 2019-2026 "Fumble Recovery (Own)" row with
    `end.yardsToEndzone >= 95` and `EPA < -2.5` (the auditor's signature), read from
    `cfb/pbp/parquet/play_by_play_<season>.parquet` on 2026-10-12.
  - 13 games (401635627, 401752791, 401752886, 401754576, 401756934, 401756935, 401762443, 401856814,
    401858248, 401862703, 401862781, 401864432, 401864443) and 401856674: with the 4 signature games above,
    the 18 games holding the 20 rows the rule moves across all 20,719 cfb-raw games (review PR-834's raw
    scan; 6 Fumble Recovery (Own), 6 Rush, 8 Fumble rows). 401752886 also carries the EX-3.4-b Timeout row
    (a defensive timeout between the moved Rush and the next snap). 401856674165 is the retained-fumble
    companion record of 401856674175, which the recovery attribution called lost until EX-3.4-d; it moves
    under the possession predicate (owner ruling R834-7).
  - 401778310: an interception return that fumbles, with a degenerate end block, whose next snap is the
    other team's: the case the rule's same-team guard exists for (R834-3).
- Play ids are ESPN's (10 to 18 digits); the test casts them to Int64.

The names do not start with `summary_`, so the whole-game oracle (`summary_*` in `..`) does not pick them up.
