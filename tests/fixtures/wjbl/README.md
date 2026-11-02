<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [W League fixtures (`api.wjbl.01core.app/api`)](#w-league-fixtures-apiwjbl01coreappapi)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# W League fixtures (`api.wjbl.01core.app/api`)

Every file is a body as the API sends it (HTTP 200 JSON; errors are `[]` or an `{"error": ...}` envelope). The tests parse
them directly and mock the transport with them; nothing reads the maintainers' private reference checkout (CI has none).

| Path | What |
|---|---|
| `full/*` | **Full, untruncated** bodies fetched **2026-10-13** from `https://api.wjbl.01core.app/api` (plain `urllib`, keyless, 1.5 s apart, 10 requests in all). The returns-schema dtypes (`wjbl_parsers.SCHEMAS`) and the dtype tests come from these: the `captures/` samples cut arrays to 3 rows and mistype rates. Game 7987 (2025-26 playoff final game 1, 2026-04-04, Toyota Antelopes 66-75 Denso Iris): `play-schedule__detail@7987`, `play-schedule@7987`, `boxscore@7987`, `pbp__play-by-play@7987` (439 plays, re-saved compact to stay under 300 KB), `pbp__team-stats@7987`, `pbp__player-stats@7987`. Also `leagues` (all 37 editions), `seasons__standings` (every season phase), `teams__base` (69 franchises) and `play-schedules@45_2026-04` (the four playoff games of April 2026). The organiser's `contact` in `play-schedule@7987` was replaced by `[redacted]` (the capture tool's own rule). |
| `captures/*.json` | The maintainers' capture samples (captured **2026-10-12**), arrays cut to 3 rows: games 7987 (bare files), 5533 (2018-19, the first play-by-play season), 7360 (2023-24, one overtime) and 4243 (2013-14, box score only: its `/pbp/*` routes answer `[]`), month pages of league editions 45, 28 and 20, `team-standings` / `team-ranking` (season phases 169, 170), `team__roster` (team 13, phase 169) and `player` (9799). Contact data and blood type are already `[redacted]`. |
| `spec_paths.json` | The spec's 15 routes, written by `tools/codegen/gen_wjbl.py` so the tests check the YAML without the maintainers' private reference checkout. |

Source of the routes and shapes: the maintainers' private WJBL OpenAPI spec, returns notes and capture README.
