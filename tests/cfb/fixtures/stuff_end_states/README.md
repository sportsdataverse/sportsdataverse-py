<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [stuff_end_states](#stuff_end_states)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# stuff_end_states

One trimmed ESPN capture for `tests/cfb/test_cfb_pbp_processor_fixes.py`.

| file | game | source | form |
| --- | --- | --- | --- |
| `401856886_trimmed.json.gz` | Auburn Tigers @ Mississippi State Bulldogs, 2026-11-14 | `https://site.api.espn.com/apis/site/v2/sports/football/college-football/summary?event=401856886`, fetched 2026-11-25 with `CFBPlayProcess(gameId=401856886, raw=True).espn_cfb_pbp()` (one request) | trimmed to `boxscore`, `drives`, `gameInfo`, `header`, `pickcenter` and `scoringPlays`; the whole game is kept |

What it holds:

- three rushes behind the line that ESPN types as a fumble recovery (own and opponent) or a safety
  (the stuff and stopped flags);
- a 0-yard rush plus a 13-yard personal foul whose end arrives as `end.yardLine` 101;
- a sack-fumble returned 34 yards whose end stays at the fumble spot.

It is not a `summary_*` file at the top level, so the whole-game oracle does not pick it up.
