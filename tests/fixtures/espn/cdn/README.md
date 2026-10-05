<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN CDN page payloads](#espn-cdn-page-payloads)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN CDN page payloads

Captured 2026-10-05 from `https://cdn.espn.com/core/{league}/{page}?xhr=1`
with a plain `requests` GET (default user agent; a desktop-browser user agent
gets an HTTP 202 HTML challenge instead of JSON). Used by `tests/test_espn_cdn.py`.

**Trimmed to what the parsers read.** Each capture was cut down by deleting keys
only, then re-serialized as compact JSON. Every retained value is identical to the
capture, and each parser returns identical frames on the full and the trimmed
payload (checked frame-by-frame when trimming). What was deleted:

- game pages (`playbyplay_*`, `boxscore_*`): every top-level key except `gameId`
  and `gamepackageJSON`, plus the `gamepackageJSON` keys whose removal left
  `parse_cdn_game` output unchanged: `videos`, and the keys that are empty in that
  game (`winprobability`, `broadcasts` for NBA/CFB; `winprobability`, `standings`
  for MLB).
- `schedule_nba.json`: everything except `content.schedule`, and in each day block
  everything except `games`.
- `scoreboard_*`: everything except `content.sbData.events`.
- `rankings_cfb.json`: everything except `content.data.rankings` (3.3 MB to 40 KB;
  the bulk was page `config` and the season/week filter menus).

| File | Request | Notes |
|---|---|---|
| `playbyplay_nba.json` | `nba/playbyplay?gameId=401705127` | 2025-01-15 NBA; `gamepackageJSON` with header, boxscore, 441 plays |
| `playbyplay_cfb.json` | `college-football/playbyplay?gameId=401628551` | 2024 week 12 CFB; football shape: drives + scoringPlays, no top-level plays |
| `boxscore_mlb.json` | `mlb/boxscore?gameId=401696358` | 2025-07-15 MLB All-Star Game; 618 plays |
| `schedule_nba.json` | `nba/schedule?date=20250115` | 7 days from 2025-01-15, 51 games under `content.schedule` |
| `scoreboard_nba.json` | `nba/scoreboard?date=20250115` | 11 games under `content.sbData` (a Site v2 scoreboard) |
| `scoreboard_epl.json` | `eng.1/scoreboard?date=20250201` | 6 Premier League games; soccer answers this page for eng.1, usa.1 and uefa.champions only |
| `rankings_cfb.json` | `college-football/rankings?week=5&year=2024&seasontype=2` | 5 polls (AP, AFCA Coaches, FCS, D-II, D-III) under `content.data.rankings` |
