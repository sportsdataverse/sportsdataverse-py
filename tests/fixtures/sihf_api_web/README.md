<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [SIHF fixtures](#sihf-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# SIHF fixtures

Real responses from the keyless Swiss Ice Hockey Federation statistics API
`https://data.sihf.ch/statistic/api/cms`, captured 2026-10-08 with `language=en` (the two
standings tables were re-captured 2026-10-12, see below). Each table
route below is the exact request `sihf_runtime._get` builds. The table bodies are cut to their
first rows (count below), and each `filters[].entries` list to its first 8 entries. The two
game bodies are whole. Nothing else is edited.
Season `2026` is 2025-26 (SIHF keys a season by its END year); `2027` is the season in progress
on the capture date; league `1` is the National League.

| File | Route | Rows kept |
|---|---|---|
| `table__results__2026__1.json` | `/table?alias=results&searchQuery=1//1&language=en&filterBy=Season,Date&filterQuery=2026/01.07.2025-30.06.2026` | 3 of 414 |
| `table__results__2027__1.json` | `/table?alias=results&searchQuery=1//1&language=en&filterBy=Season,Date&filterQuery=2027/01.07.2026-30.06.2027` | 3 of 364 (current season) |
| `table__standing__2026__1.json` | `/table?alias=standing&searchQuery=1//1&language=en&filterBy=Season,ContentType&filterQuery=2026/All` (captured 2026-10-12) | 14 of 14 |
| `table__standing__2025__1.json` | `/table?alias=standing&searchQuery=1//1&language=en&filterBy=Season,ContentType&filterQuery=2025/All` (captured 2026-10-12) | 4 of 14 |
| `table__player__2026__1.json` | `/table?alias=player&searchQuery=1//1&language=en&filterBy=Season&filterQuery=2026` | 3 of 427 |
| `table__goalkeeper__2026__1.json` | `/table?alias=goalkeeper&searchQuery=1//1&language=en&filterBy=Season&filterQuery=2026` | 8 of 35 (row 7 is the league-average row) |
| `gameoverview__20261105000325.json` | `/gameoverview?alias=gameDetail&searchQuery=20261105000325` | whole (has a goaltender change and a revoked goal) |
| `gameoverview__20261105000009.json` | `/gameoverview?alias=gameDetail&searchQuery=20261105000009` | whole (decided by a shootout) |

Behaviour the fixtures cannot show, measured on the same day (the `sihf_runtime` docstring has the
full contract):

- The league must be the last `searchQuery` segment (`1//<league>`). As a `League` filter
  (`searchQuery=1//&filterBy=Season,League`) it works for past seasons, but the current season
  answers 400 `{"error":"MissingLeagueId"}`.
- Without a `Date` filter, the current season's `results` answer only today's game day (one row).
- An unknown `Season` or `Phase` is answered with another one (`Season=1890` serves the current
  season), visible only in `filters[].selected`. The exception is `results?Phase=0`: it echoes
  `0` back and serves the Playout Final, so only the Phase filter's own `entries` list shows the
  id is unknown. An unknown league answers one `["Keine Tabelle gefunden!"]` row. `Phase=all` is
  answered with ONE phase by `standing` (regular season) and by `player` / `goalkeeper`
  (Playoff Overall), and with every game by `results`. No Phase filter gives the whole-season
  summary.
- `standing` without a `ContentType` filter answers the AWAY-games table and does not echo the
  filter: every 2025-26 National League team shows 26 GP, and HC Davos 50 points, which is exactly
  the away-games table recomputed from that season's 364 regular-season `results` (measured
  2026-10-12). `ContentType=All` (echoed back) answers the full table, 52 GP and HC Davos on 117,
  equal to the table recomputed from every result; `Home` / `Away` answer the split tables. The
  standings fixtures were re-captured with `ContentType=All` on that date (the 2026-10-08 captures
  were the away table).
- `/table` without `searchQuery`, or with an unknown `alias`, answers an empty HTTP 501.
- `/gameoverview` with an unknown game answers 404 `{"error":"GameNotFound"}`, and without
  `searchQuery` it answers 400 `{"error":"MissingSearchQuery"}`.

To regenerate a fixture, re-capture it from the URL above and trim it the same way. Do not hand-edit.
