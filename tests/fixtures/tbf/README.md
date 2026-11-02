<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [tbf fixtures -- provenance](#tbf-fixtures----provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# tbf fixtures -- provenance

Host: `https://miniappapi.tbf.org.tr/webapi-service/api` (the Turkish Basketball Federation's official app backend;
no key, plain GETs, >= 1 s apart; the vendor origin hosts `*.nikayazilim.com` and `www.tbf.org.tr` are never used).

- `captures/` -- copied unchanged from the maintainers' captures, captured live 2026-10-12. Every array is cut to 3
  rows (so `count` / `totalCount` fields disagree with the rows; the runtime tests patch those). Left out: the
  routes with no wrapper (`Match__box-score*`, `Match__get-match-report-with-players*`,
  `Team__get-team-detail-main` -- club contact fields, redacted in the capture -- and `Kupalar__*`) and `_observed.json`.
- `full/` -- FULL, untruncated bodies fetched live 2026-10-13 (9 requests, 1.3 s apart, redirects off), the source of
  every dtype in the parsers:

  | file | URL (relative to the host) |
  |---|---|
  | `header.json` | `/Match/mac-header?matchId=346279` |
  | `stats.json` | `/Match/mac-istatistik?matchId=346279` |
  | `flow.json` | `/Match/game-flow?matchId=346279` |
  | `shots.json` | `/Match/shot-chart?matchId=346279` |
  | `summary.json` | `/Match/mac-ozet?matchId=346279` |
  | `leaders_game.json` | `/Match/mac-leader?matchId=346279` |
  | `standings.json` | `/League/get-standings-table?seasonId=174&leagueId=22214` |
  | `team_stats.json` | `/Team/get-team-detail-statistic?LeagueId=22214&SeasonId=174&TeamProcessId=265995` |
  | `leaders_points.json` | `/Player/get-point-leaders?seasonId=174&leagueId=22214` |

  Game 346279 = BSL 2026-27 week 1, Tofas 90-89 Pizzabulls Bordo Bandirma (Genius id 2913588). The bodies carry no
  e-mail address or phone number (checked); they are stored compact (one line).
- `spec_paths.json` -- the spec's 33 routes (written by `tools/codegen/gen_tbf.py`), so the tests need no private reference checkout.
