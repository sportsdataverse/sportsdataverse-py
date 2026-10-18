# VHL fixtures (`www.vhlru.ru` + `online.vhlru.ru`, server-rendered HTML)

The VHL has no JSON API; every route is a `text/html` page that `vhl_parsers` parses.

**Cut-down pages** (`*.html` in this directory): copied verbatim from `sdv-internal-refs/tests/fixtures/vhl/`
(refs commit `eb57f5d`, fetched 2026-10-12, `www.vhlru.ru` pinned to `188.128.49.29`). They keep the parts the
parsers read: scripts, styles, svg, header, footer and legends dropped; every `<tbody>` cut to 3 rows (player career
4); selects cut to 3 options plus the selected one; the calendar keeps one day (18 March 2026, 3 games); the
broadcast one event per kind and the `match` + period-1 stat tabs. **Birth dates are masked to `01.01.2000`.**

| File | Page |
|---|---|
| `calendar_1374.html` | `https://www.vhlru.ru/en/calendar/1374/0/` (2025/26 regular season) |
| `report_899223.html` | `https://www.vhlru.ru/en/report/1374/?idgame=899223` (game 984, Toros 2:3 Magnitka SO) |
| `online_899223.html` | `https://online.vhlru.ru/online/899223.html` (the same game's broadcast) |
| `standings_1374.html` | `https://www.vhlru.ru/en/standings/league/1374/` |
| `stats_players_1374.html` | `https://www.vhlru.ru/en/stats/players/1374/` |
| `player_34418.html` | `https://www.vhlru.ru/en/players/34418/` (skater, full card) |
| `team_460279.html` | `https://www.vhlru.ru/en/teams/460279/` (Kristall: officials, honours) |

**Full page** (`full/report_899223.html`): `https://www.vhlru.ru/en/report/1374/?idgame=899223`, fetched
untruncated on 2026-10-12 (324 KB). Only the `<script>`, `<style>`, `<svg>` and `<noscript>` blocks were removed
(284 KB), which the parsers never read; every table and row is intact (34 skater and 4 goalie lines). The report
returns-schemas and the dtype tests come from this page: it prints a game's shifts per game (`Sft/G`) as whole
numbers (`22`, where the season tables print `22.8`), which the parsers must still type `Float64`. It carries no
birth dates.

**Season table** (`tournaments.html`): the `<select name="tournament">` of `https://www.vhlru.ru/en/calendar/`, cut
out of the live page (4.3 MB) on 2026-10-12 and wrapped in a minimal document: all 33 season stages, 2010/11 = 189 /
190 to 2026/27 = 1430. The runtime's season -> tn mapping reads this select.

**Refs capture records** (`captures/*.json`): copied verbatim from `sdv-internal-refs/vhl/captures/` (refs commit
`eb57f5d`). These are the parsed records `vhl_runtime._get` returns (`return_parsed=False`), truncated to 3 rows
per array; birth dates masked to `DD.MM.YYYY`. See that directory's README for each page.
