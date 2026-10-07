<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [EuroLeague fixtures](#euroleague-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# EuroLeague fixtures

Real, trimmed responses from the three keyless EuroLeague APIs -- the Competition Engine
`https://api-live.euroleague.net/v2` and `/v3` (`Accept: application/json`) and the legacy
live API `https://live.euroleague.net/api` -- captured 2026-10-05 (v2) and 2026-10-06 (v3,
live) and copied byte-for-byte from `sdv-internal-refs/euroleague/captures/` (source commit
`ba0860c`, copied 2026-10-06). Season `E2025` (2025-26), game code `1`, round `1`; the two
`U2025__*` files are EuroCup 2025-26 game 1. Every array is cut to its first 3 elements at
every depth. A file name is the route slug, prefixed by the server tail when it is not v2
(`v3__`, `api__`).

| File | Route |
|---|---|
| `competitions.json` | `/competitions` |
| `competitions__E__seasons.json` | `/competitions/E/seasons` |
| `competitions__E__seasons__E2025__rounds.json` | `/competitions/E/seasons/E2025/rounds` |
| `competitions__E__seasons__E2025__clubs.json` | `/competitions/E/seasons/E2025/clubs` |
| `competitions__E__seasons__E2025__people.json` | `/competitions/E/seasons/E2025/people?limit=3` |
| `competitions__E__seasons__E2025__games.json` | `/competitions/E/seasons/E2025/games?limit=3` |
| `competitions__E__seasons__E2025__games__1__stats.json` | `/competitions/E/seasons/E2025/games/1/stats` |
| `v3__competitions__E__seasons__E2025__rounds__1__basicstandings.json` | v3 `/competitions/E/seasons/E2025/rounds/1/basicstandings` |
| `v3__competitions__E__seasons__E2025__rounds__1__calendarstandings.json` | v3 `/competitions/E/seasons/E2025/rounds/1/calendarstandings` |
| `v3__competitions__E__seasons__E2025__rounds__1__streaks.json` | v3 `/competitions/E/seasons/E2025/rounds/1/streaks` |
| `v3__competitions__E__seasons__E2025__rounds__1__aheadbehind.json` | v3 `/competitions/E/seasons/E2025/rounds/1/aheadbehind` |
| `v3__competitions__E__seasons__E2025__games__1__report.json` | v3 `/competitions/E/seasons/E2025/games/1/report` |
| `v3__competitions__E__statistics__players__traditional.json` | v3 `/competitions/E/statistics/players/traditional?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame&limit=3` |
| `v3__competitions__E__statistics__players__advanced.json` | v3 `/competitions/E/statistics/players/advanced?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame&limit=3` |
| `v3__competitions__E__statistics__teams__traditional.json` | v3 `/competitions/E/statistics/teams/traditional?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame&limit=3` |
| `v3__competitions__E__statistics__teams__advanced.json` | v3 `/competitions/E/statistics/teams/advanced?SeasonMode=Single&SeasonCode=E2025&statisticMode=PerGame&limit=3` |
| `api__Points.json` | live `/Points?gamecode=1&seasoncode=E2025` (the shot chart) |
| `api__PlayByPlay.json` | live `/PlayByPlay?gamecode=1&seasoncode=E2025` |
| `api__Boxscore.json` | live `/Boxscore?gamecode=1&seasoncode=E2025` |
| `api__Header.json` | live `/Header?gamecode=1&seasoncode=E2025` |
| `U2025__api__Points.json` | live `/Points?gamecode=1&seasoncode=U2025` (EuroCup; from `captures/U2025/`) |
| `U2025__api__PlayByPlay.json` | live `/PlayByPlay?gamecode=1&seasoncode=U2025` (EuroCup; from `captures/U2025/`) |

List routes answer an envelope with one list (`{"total": n, "data": [...]}`, v3 stats
`{"total": n, "players": [...]}`); the v3 standings are `{"winner": {...}, "teams": [...]}`;
the v2 box score and the v3 report are page objects (one row). Live `Points` is
`{"Rows": [...]}`, `PlayByPlay` one array per quarter, `Boxscore` one `Stats` object per side,
`Header` a flat object; the live API answers an unknown game with an empty 200 body (not
captured: nothing to copy). Regenerate by re-copying from the reference repo; do not hand-edit.
