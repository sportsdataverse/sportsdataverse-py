<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Basketball live-adapter fixtures (BB-D13)](#basketball-live-adapter-fixtures-bb-d13)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Basketball live-adapter fixtures (BB-D13)

Real captures backing `tests/basketball/test_sources_live.py`, which runs the cdn.nba.com / cdn.wnba.com
live adapter and ESPN's own summary of the same game through `helper_<lg>_pbp` + `game_page_frames` and
compares them. The liveData captures for the first two games are the ones already committed for
`tests/nba/test_nba_live.py` / `tests/wnba/test_wnba_live.py` (provenance in their own READMEs).

| File | Source | Captured | Why |
|---|---|---|---|
| `espn_nba_401809243.json.gz` | `espn_nba_pbp(401809243, raw=True)` | 2026-11-03 | 2025-10-21 HOU @ OKC, double overtime; ESPN side of liveData `0022500001` (`tests/nba/fixtures/nba_live/`) |
| `espn_wnba_401856986.json.gz` | `espn_wnba_pbp(401856986, raw=True)` | 2026-11-03 | 2026 IND @ CON; three technical fouls, each with its free throw and substitutions at one clock; ESPN side of liveData `1022600097` (`tests/wnba/fixtures/wnba_live/`) |
| `espn_nba_401809839.json.gz` | `espn_nba_pbp(401809839, raw=True)` | 2026-11-03 | 2025-12-16 NBA Cup final, SA vs NY in Las Vegas: ESPN `neutralSite` True; a coach's technical |
| `playbyplay_0062500001.json.gz` | `https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_0062500001.json` via `nba.nba_live._fetch_live` (curl_cffi, `impersonate="chrome"`) | 2026-11-03 | liveData side of the Cup final |
| `boxscore_0062500001.json.gz` | `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_0062500001.json`, same transport | 2026-11-03 | liveData side of the Cup final |
| `idmap_rows.json` | `basketball.sources.idmap._build_idmap_from_release(lg, [2026])` (the published schedule, team and player crosswalks), filtered to the three games, their teams and every person in their liveData payloads | 2026-11-03 | the ESPN <-> liveData id pairs the adapter rewrites through; liveData person 1628188 (SA's coach) has no pair |

Every JSON is gzipped compact (`separators=(",", ":")`) and otherwise verbatim. The ESPN and liveData
captures of each game were taken minutes apart, long after the final, so they describe the same game state.
