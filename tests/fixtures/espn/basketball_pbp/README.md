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
