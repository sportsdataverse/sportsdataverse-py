<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN basketball pbp fixtures](#espn-basketball-pbp-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN basketball pbp fixtures

Real ESPN Site v2 `summary` payloads, copied verbatim (2026-10-05) from the
raw stores the hoopR / wehoop pbp releases are built from
(`<lg>/json/raw/<game_id>.json`, the `espn_<lg>_pbp(raw=True)` shape). Used by
`tests/test_basketball_pbp_offline.py` and `tests/validation/test_basketball_{clock_seconds,coordinates}.py`.

| File | Source | Why |
|---|---|---|
| `mbb_401856600.json.gz` | hoopR-mbb-raw, 2026 national championship CONN @ MICH | one pickcenter provider (DraftKings MICH -6.5); a RegularTimeOut |
| `mbb_401830342.json.gz` | hoopR-mbb-raw, 2025-11-15 UTU @ MVSU, double overtime | 2OT end-of-period seconds; RegularTimeOut |
| `nba_260312029.json.gz` | hoopR-nba-raw, 2006-03-12 PHI @ MEM | "Memphis ... timeout" contains "phi" (team-name substring match) |
| `published_coordinates.parquet` | the released pbp parquet of each captured game, from the local data-repo checkouts on 2026-10-12: hoopR-nba-data `nba/pbp/parquet/play_by_play_{2006,2024}.parquet` (545f0e9a, d15bb9ce), wehoop-wnba-data `wnba/pbp/parquet/play_by_play_{2017,2024}.parquet` (8fea183e), hoopR-mbb-data `mbb/pbp/parquet/play_by_play_{2026,2024}.parquet` (f24a256d, 013176f8), wehoop-wbb-data `wbb/pbp/parquet/play_by_play_2024.parquet` (6cb2cb4a); columns `league`, `game_id`, `id` (Int64) and `coordinate_{x,y}{,_raw}` (Float64), 3,304 rows, games 260312029 401585607 400927398 401726992 401830342 401856600 401638645 401637613 | the CI oracle for the hoopR / wehoop coordinate transform (R844-8); equal row for row except the MBB / WBB free throws R844-3 moves onto the line |
| `wbb_400509734.json.gz` | wehoop-wbb-raw `wbb/json/raw/400509734.json` at f6e7d5dc34 (captured 2023-04-09), 2013-11-08 ARIZ @ IONA, gzipped, plays untrimmed | 2013-14 halves era (two 20:00 halves) plus an overtime; ESPN ships no coordinates that season (R841-2) |
| `wnba_400927398.json.gz` | wehoop-wnba-raw, 2017-05-14 CHI @ MIN | two team timeouts whose text names no team (" Full timeout") |
| `pickcenter.json` | the `pickcenter` arrays of mbb 401856600 / 400766104 / 400587253 / 330582427 / 401364342, nba 401809238 / 400578293 / 401430219, wnba 401320565, wbb 401468165 | one provider; several providers (401364342: teamrankings and Caesars disagree); a lone record-only entry with no spread; a record-only row sorting ahead of consensus (330582427, 401430219) |
