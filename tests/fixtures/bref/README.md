<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Basketball-Reference fixtures](#basketball-reference-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Basketball-Reference fixtures

| file | source | notes |
|---|---|---|
| `teams_BOS_1986.html.gz` | `GET https://www.basketball-reference.com/teams/BOS/1986.html` through `sportsdataverse.nba.bref._bref_get` (HTTP 200, `text/html`, 677,082 bytes), captured 2026-10-11 | The full page, gzipped byte-for-byte. The 1985-86 Celtics roster carries Robert Parish's `"00"` jersey, the B-M19 trigger. Used by `tests/nba/test_bref.py` (real page instead of synthetic HTML, B-L93). |

Not committed: a Basketball-Reference **200 interstitial** (rate-limit or challenge page). It
cannot be captured with one request: the 2026-10-11 live check (`audit/live-checks/nba-residential.md`,
gap-B-169) sent three requests 10 s apart and got real pages every time; inducing one needs more
than about 20 requests a minute, which risks an IP ban.
