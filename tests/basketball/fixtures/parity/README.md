<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Basketball parity corpus (BB-D15)](#basketball-parity-corpus-bb-d15)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Basketball parity corpus (BB-D15)

Real captures backing `tests/basketball/test_sources_parity.py`, which runs ESPN and the cdn.nba.com /
cdn.wnba.com live adapter through the basketball dispatch leg (`helper_<lg>_pbp` + `game_page_frames`) and
holds every game to the pinned gates in `sportsdataverse/basketball/sources/parity.py`. The corpus is these
24 games plus the three BB-D13 games in `../live_adapter/` (not re-captured): 18 NBA 2025-26 and 9 WNBA 2026
games in all.

Per game, three files, all captured 2026-11-03, long after the final, minutes apart:

| File | Source |
|---|---|
| `espn_<lg>_<espn id>.json.gz` | `espn_nba_pbp(id, raw=True)` / `espn_wnba_pbp(id, raw=True)` |
| `playbyplay_<live id>.json.gz` | `https://cdn.<lg>.com/static/json/liveData/playbyplay/playbyplay_<live id>.json` via `nba.nba_live._fetch_live` (curl_cffi, `impersonate="chrome"`), one request at a time, 1.5 s apart |
| `boxscore_<live id>.json.gz` | `https://cdn.<lg>.com/static/json/liveData/boxscore/boxscore_<live id>.json`, same transport |

`idmap_rows.json.gz` is `basketball.sources.idmap._build_idmap_from_release(lg, [2026])` (the published
schedule, team and player crosswalks), filtered to these games, their teams and every person in their
liveData payloads. Every JSON is gzipped compact (`separators=(",", ":")`) and otherwise verbatim.

The regular-season technical-foul games were picked by fetching ESPN for 24 NBA and 16 WNBA random
regulation finals (seed 15) and keeping the ones with the most technical and flagrant fouls; the ESPN
captures of those four games are from that scan.

| League | ESPN id | liveData id | Date (local) | Game | Why |
|---|---|---|---|---|---|
| NBA | 401809788 | `0022500046` | 2025-11-14 | LAC @ DAL | NBA Cup group play, double overtime |
| NBA | 401809794 | `0022500052` | 2025-11-21 | NO @ DAL | three technicals |
| NBA | 401809837 | `0022501229` | 2025-12-13 | NY @ ORL | NBA Cup semifinal, Las Vegas (neutral) |
| NBA | 401809838 | `0022501230` | 2025-12-13 | SA @ OKC | NBA Cup semifinal, Las Vegas (neutral) |
| NBA | 401810002 | `0022500147` | 2025-11-01 | DAL @ DET | Mexico City game (neutral); FG 34-80 = 42.5% |
| NBA | 401810433 | `0022500578` | 2026-01-15 | MEM @ ORL | Berlin game (neutral) |
| NBA | 401810457 | `0022500602` | 2026-01-18 | ORL @ MEM | London game (neutral) |
| NBA | 401810551 | `0022500696` | 2026-01-30 | DET @ GS | technicals and a flagrant foul |
| NBA | 401810666 | `0022500811` | 2026-02-21 | ORL @ PHX | double overtime |
| NBA | 401810743 | `0022500888` | 2026-03-04 | SA @ PHI | two flagrant fouls |
| NBA | 401859963 | `0042500401` | 2026-06-03 | NY @ SA | NBA Finals Game 1 |
| NBA | 401859967 | `0042500405` | 2026-06-13 | NY @ SA | NBA Finals Game 5 |
| NBA | 401866755 | `0052500111` | 2026-04-14 | MIA @ CHA | play-in, overtime |
| NBA | 401866759 | `0052500211` | 2026-04-17 | GS @ PHX | play-in; five technicals, two ejections, double fouls |
| NBA | 401873197 | `0042500311` | 2026-05-18 | SA @ OKC | West Finals Game 1, double overtime |
| NBA | 401873203 | `0042500317` | 2026-05-30 | SA @ OKC | West Finals Game 7; 3P 17-40 = 42.5% |
| WNBA | 401856921 | `1022600032` | 2026-05-20 | POR @ IND | technicals and flagrant fouls |
| WNBA | 401857031 | `1022600142` | 2026-06-28 | POR @ WSH | four overtimes |
| WNBA | 401857097 | `1022600208` | 2026-07-30 | ATL @ DAL | technicals and flagrant fouls |
| WNBA | 401857136 | `1022600247` | 2026-08-11 | PHX @ LA | neutral site |
| WNBA | 401918020 | `1042600112` | 2026-09-30 | GS @ DAL | first round Game 2, overtime |
| WNBA | 401918300 | `1042600213` | 2026-10-09 | GS @ LV | semifinal Game 3, overtime |
| WNBA | 401918305 | `1042600301` | 2026-10-17 | ATL @ GS | WNBA Finals Game 1 |
| WNBA | 401918310 | `1042600306` | 2026-10-29 | GS @ ATL | WNBA Finals Game 6; an ESPN-listed player with no minutes |
