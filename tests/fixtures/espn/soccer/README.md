<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN soccer fixtures](#espn-soccer-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN soccer fixtures

Captured 2026-06 from ESPN (auth-free) via sdv-internal-refs espn_capture_league.py.
Leagues: eng.1 (EPL), uefa.champions (UCL knockout+shootout), usa.1 (MLS conferences).
Used by tests/test_espn_soccer_parsers.py.

Added 2026-10-10 (direct `curl` of the Site v2 host, no auth), for the rule-catalog era fixes:

- `usa.nwsl/site-v2/summary_445418.json` -- NWSL 2016-07-01 match: a header, rosters with no
  players, boxscore teams with no statistics, no `keyEvents` / `commentary` (the 2014-2018
  coverage gap, rules/nwsl.yaml#nwsl-2013-espn-season-coverage-start). Exercises the
  empty-detail warning in `parse_soccer_summary`.
- `mex.1/site-v2/scoreboard_20241110.json` -- Liga MX `?dates=20241110`: three events whose
  `season.slug` is `torneo-apertura` (rules/ligamx.yaml#ligamx-1996-two-short-tournaments-per-season).
  Exercises the derived `tournament` column.

The schema introspection in `generate.py` reads only the `<league>/site-v2/<endpoint>.json`
names, so these suffixed files are test-only.
