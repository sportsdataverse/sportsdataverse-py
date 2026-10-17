<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN basketball pbp fixtures](#espn-basketball-pbp-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN basketball pbp fixtures

Real ESPN Site v2 `summary` payloads, copied verbatim (2026-10-05) from the
raw stores the hoopR / wehoop pbp releases are built from
(`<lg>/json/raw/<game_id>.json`, the `espn_<lg>_pbp(raw=True)` shape). Used by
`tests/test_basketball_pbp_offline.py`.

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
