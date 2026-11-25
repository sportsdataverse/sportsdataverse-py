<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN Golf Fixtures](#espn-golf-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN Golf Fixtures

Captured 2026-11-25 for the offline `parse_golf_*` tests. One subdirectory per ESPN league slug.

| File | URL | Event |
|---|---|---|
| `pga/site-v2/leaderboard.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=pga&event=401811941` | 2026 Masters, 91 players (54 finished, 37 missed the cut) |
| `pga/site-v2/scoreboard.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/pga/scoreboard?dates=20260409` | the same Masters field |
| `lpga/site-v2/scoreboard.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/lpga/scoreboard` | 2026 CME Group Tour Championship, 60 players |

The two scoreboard bodies are trimmed to keep them small. Each round's per-hole `linescores` and its `statistics`
block are removed, since neither parser reads them. Everything else is as captured.
