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
| `pga/site-v2/leaderboard_401580335.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=pga&event=401580335` | 2024 Genesis Invitational, 70 players: Tiger Woods withdrew in round 2 (WD), Jordan Spieth was disqualified (DQ) |
| `pga/site-v2/leaderboard_401580330.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=pga&event=401580330` | 2024 Sony Open, 144 players: three-man playoff (period-5 line), split-tee starts, one WD before round 4 |
| `liv/site-v2/leaderboard_401614916.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=liv&event=401614916` | 2024 LIV Golf Hong Kong, 54 players: three-man playoff after 54 holes (playoff line filed as period 4) |
| `liv/site-v2/leaderboard_401614919.json` | `https://site.api.espn.com/apis/site/v2/sports/golf/leaderboard?league=liv&event=401614919` | 2024 LIV Golf Houston, 54 players: two withdrawals on a no-cut event |

The two scoreboard bodies are trimmed to keep them small. Each round's per-hole `linescores` and its `statistics`
block are removed, since neither parser reads them. Everything else is as captured.

The four `leaderboard_<event id>.json` files were captured 2026-11-26 and are stored as compact JSON; they carried no
per-hole `linescores`, and nothing else is removed. They were found by scanning every 2023-25 PGA TOUR and 2024 LIV
leaderboard for the withdrawn / disqualified status and for `isPlayoff` lines. Things they pin down:

- ESPN files a withdrawal and a disqualification under `STATUS_CUT`, the missed-cut status name. Only
  `status.type.shortDetail` (`WD`, `DQ`, `CUT`, `F`) separates them.
- `status.hole` is the last hole played, not holes completed: a round started on the 10th ends on `hole` 9 with
  `thru` 18. LIV rows carry `thru` and no `hole`.
- Playoff players carry `status.playoff: true` and a `thru` that describes the playoff, not a round (PGA: playoff
  holes played; LIV: 0).

