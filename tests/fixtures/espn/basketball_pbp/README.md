<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN basketball pbp fixtures](#espn-basketball-pbp-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN basketball pbp fixtures

Real ESPN Site v2 `summary` payloads, copied verbatim (2026-10-05) from the
raw stores the hoopR / wehoop pbp releases are built from
(`<lg>/json/raw/<game_id>.json`, the `espn_<lg>_pbp(raw=True)` shape). Used by
`tests/test_basketball_pbp_offline.py`; the 2026-10-12 rows below are live
`espn_<lg>_pbp(raw=True)` captures used by `tests/nba/test_nba_game_page_leagues.py`. Game dates are the US Eastern local date (ESPN's
header time is UTC, so a night game can carry the next day's UTC date).

| File | Source | Why |
|---|---|---|
| `mbb_401856600.json.gz` | hoopR-mbb-raw, 2026 national championship CONN @ MICH | one pickcenter provider (DraftKings MICH -6.5); a RegularTimeOut |
| `mbb_401830342.json.gz` | hoopR-mbb-raw, 2025-11-15 UTU @ MVSU, double overtime | 2OT end-of-period seconds; RegularTimeOut |
| `nba_260312029.json.gz` | hoopR-nba-raw, 2006-03-12 PHI @ MEM | "Memphis ... timeout" contains "phi" (team-name substring match) |
| `wnba_400927398.json.gz` | wehoop-wnba-raw, 2017-05-14 CHI @ MIN | two team timeouts whose text names no team (" Full timeout") |
| `pickcenter.json` | the `pickcenter` arrays of mbb 401856600 / 400766104 / 400587253 / 330582427 / 401364342, nba 401809238 / 400578293 / 401430219, wnba 401320565, wbb 401468165 | one provider; several providers (401364342: teamrankings and Caesars disagree); a lone record-only entry with no spread; a record-only row sorting ahead of consensus (330582427, 401430219) |
| `nba_401766458.json.gz` | hoopR-nba-raw @ 1fa774d21de, 2025-04-15 ATL @ ORL play-in | Cole Anthony's technical FT keyed late (seq 738, array index 438): ESPN's rows from seq 658 on are never re-based, last row 119-95 vs official 120-95 |
| `nba_400829116.json.gz` | hoopR-nba-raw @ 2db1b380436, 2015-16 regular season | every scoring free throw ships `scoreValue: 0`; ESPN's running score dips 7 times and ends 112-105 vs official 114-105 |
| `nba_401704638.json.gz` | hoopR-nba-raw @ 1fa774d21de, 2024-25 regular season | the scoring plays sum to 104-139 against an official 104-140: the rebuild must be rejected |
| `wnba_240618004.json.gz` | wehoop-wnba-raw @ 9bc2a03e0, 2004-06-18, two halves | ESPN's score falls once and ends 61-69 vs official 63-69; 245 rows short |
| `mbb_401829457.json.gz` | hoopR-mbb-raw @ 11e4542f967, 2025-11-23 CCU vs UND, neutral site | every ESPN row carries the header away team's score in `homeScore` (ends 75-58; official UND home 58, CCU 75) |
| `wbb_400550784.json.gz` | wehoop-wbb-raw @ f6e7d5dc342, 2013-14, two halves | the last two rows were never re-based: ESPN ends 52-54, official 54-56 |
| `wnba_250603003.json.gz` | `espn_wnba_pbp(250603003, raw=True)` captured 2026-10-12, 2005-06-03 NYL @ TUL | WNBA halves era (`format.regulation.periods` 2); one OT (period 3); End Period tied 54-54 at period 2; no pickcenter; not neutral |
| `wbb_293170052.json.gz` | `espn_wbb_pbp(293170052, raw=True)` captured 2026-10-12, 2009-11-13 UNF @ FSU | WBB 2010 halves; box turnovers only under `teamTurnovers` (FSU 23 / UNF 28; `turnovers` and `totalTurnovers` 0); not neutral |
| `wbb_273130043.json.gz` | `espn_wbb_pbp(273130043, raw=True)` captured 2026-10-12, 2007-11-09 STAN @ YALE | WBB 2008 halves; no turnovers under any box key (all 0) |
| `mbb_401812161.json.gz` | `espn_mbb_pbp(401812161, raw=True)` captured 2026-10-12, 2026-02-19 UVM @ UMBC | MBB non-neutral conference game, regulation (max period 2), one pickcenter provider |
| `nba_game_page_golden.json` | `nba_game_page.game_page_frames` output on `summary_nba.json` (league_id "00") and `summary_wnba.json` ("10") at e4894f0817, written before the league generalization | NBA/WNBA byte-identity gate of the league generalization |
| `mbb_401828345.json.gz` | `espn_mbb_pbp(401828345, raw=True)` captured 2026-10-12, 2025-12-31 GMU @ LAS | D12: GMU (2244) committed no turnover (box 0 under `turnovers`, `totalTurnovers`, `teamTurnovers`; no turnover play for 2244); LAS 4. A college 0 after 2009 stays 0 |
| `wbb_283430164.json.gz` | wehoop-wbb-raw @ f6e7d5dc342 (`wbb/json/raw/283430164.json`), copied 2026-10-12, 2008-12-08 UGA @ RUTG, season 2009 | F2: both teams 0 under `turnovers`, `teamTurnovers` and `totalTurnovers` while the plays log 13 RUTG / 10 UGA turnovers: a WBB game total below 5 is "not recorded" (null) in any season |
| `wbb_290232368.json.gz` | wehoop-wbb-raw @ f6e7d5dc342 (`wbb/json/raw/290232368.json`), copied 2026-10-12, 2009-01-23 FAIR @ MRST, season 2009, one OT | F2 re-ruling: game total 1 (FAIR 0 under every key; MRST 1 under `teamTurnovers`) while the plays log 12 FAIR / 16 MRST turnovers: a WBB game total below 5 is "not recorded" (null) |
| `wbb_401364640.json.gz` | wehoop-wbb-raw @ f6e7d5dc342 (`wbb/json/raw/401364640.json`), copied 2026-10-12, 2022-01-15 BOIS @ SDSU | genuine low turnover total 8 (BOIS 2, SDSU 6; the plays agree): at or above the threshold of 5, so the counts stay |
