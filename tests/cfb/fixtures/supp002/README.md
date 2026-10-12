<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [supp-002 candidate captures](#supp-002-candidate-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# supp-002 candidate captures

22 ESPN college-football summary payloads, one per game holding a supp-002 candidate row (quality
program task 3.4(a); the test is `tests/cfb/test_cfb_supp002_degenerate_end.py`, which also reads the
23rd game, `../summary_401856682.json`).

- **Source:** `https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=<game_id>`,
  as stored by the scraper in `cfbfastR-cfb-raw/cfb/json/raw/<game_id>.json`, raw repo at `98deb3f091`
  (2026-10-11); copied 2026-10-12.
- **Form:** `<game_id>_trimmed.json.gz`, the `trimmed` form of `../README.md`: the payload reduced to
  `boxscore`, `drives`, `gameInfo`, `header`, `pickcenter` and `scoringPlays`, whole game kept, gzip
  with mtime 0.
- **Selection:** the games of every published `cfbfastR-cfb-data` 2019-2026 "Fumble Recovery (Own)" row
  with `end.yardsToEndzone >= 95` and `EPA < -2.5`, read from `cfb/pbp/parquet/play_by_play_<season>.parquet`
  on 2026-10-12.
- Play ids are ESPN's (10 to 18 digits); the test casts them to Int64.

The names do not start with `summary_`, so the whole-game oracle (`summary_*` in `..`) does not pick them up.
