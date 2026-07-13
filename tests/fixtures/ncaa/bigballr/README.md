<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [bigballR / wbigballR port fixtures](#bigballr--wbigballr-port-fixtures)
  - [`html/` — raw page captures (27 pages, 2026-07-12)](#html--raw-page-captures-27-pages-2026-07-12)
  - [`oracle/mbb/` (17 CSVs) and `oracle/wbb/` (15 CSVs)](#oraclembb-17-csvs-and-oraclewbb-15-csvs)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# bigballR / wbigballR port fixtures

Parity fixtures for the bigballR (MBB) + wbigballR (WBB) R→Python port of the
stats.ncaa.org scraper surface (`ncaa_mbb_*` / `ncaa_wbb_*`).

## `html/` — raw page captures (27 pages, 2026-07-12)

Captured once through the accepted transports (`NcaaFetcher.with_browser()` —
Playwright new-headless Chrome, residential-direct; clears the Akamai bm-verify
challenge on game-detail pages). Games chosen to stress edge logic:

| League | Contest | Why |
|---|---|---|
| MBB | 6470186 | Illinois 113–55 Jackson St. (2025-11-03) — blowout / garbage-time path |
| MBB | 6479639 | Illinois 81–77 Texas Tech (2025-11-11) — close regulation |
| MBB | 6479592 | Illinois–Wisconsin (2026-02-10) — 1 OT |
| MBB | 1613299 | Illinois @ Maryland (2019-01-26) — 2019-era markup (legacy id 4690813) |
| WBB | 5722355 | South Carolina 92–60 Coppin St. (2024-11-14) — blowout |
| WBB | 5732292 | South Carolina 68–62 Michigan (2024-11-04) — close, neutral site |
| WBB | 5728709 | Notre Dame 80–70 Texas (2024-12-05) — 1 OT |
| WBB | 5733807 | NC State 104–95 Notre Dame (2025-02-23) — 2 OT |

Per game: `pbp_{id}.html` (`/contests/{id}/play_by_play`), `individual_stats_{id}.html`
(`/contests/{id}/individual_stats`), `box_{id}.html` (`/contests/{id}/box_score`).
Team-level: `team_609554.html` + `roster_609554.html` (Illinois MBB 2025-26),
`team_592003.html` + `roster_592003.html` (South Carolina WBB 2024-25).
Scoreboards: `scoreboard_18703_11-11-2025.html` (MBB), `scoreboard_18423_12-05-2024.html` (WBB).

**Structural fact these captures settle:** WBB play-by-play ships one table per
QUARTER (line score `1 2 3 4 [OT…] S`); MBB ships halves. wbigballR (an older fork
of bigballR) applies the MBB halves clock math to WBB and therefore misreads a
regulation WBB game as having two overtimes — see `oracle/wbb/README.md` for how
parity scope is adjusted.

## `oracle/mbb/` (17 CSVs) and `oracle/wbb/` (15 CSVs)

Golden R-oracle outputs: every bigballR / wbigballR export run from source
(`pkgload::load_all`) on the games above. MBB ran live via bigballR's own chromote
transport; WBB ran FULLY OFFLINE (`use_file=TRUE`) over the exact bytes in `html/`.
Each directory's `README.md` records R version, package git SHA, capture time, and
the known R-side breakages deliberately not ported (box `multi.games=TRUE`
aggregation, men's-table name resolution in wbigballR, `"Tip-In"`/`"Tip In"`
vocabulary bug).

CSV `NA` cells are the literal string `NA` (R `write.csv` default) — readers must
pass `null_values=["NA"]`.
