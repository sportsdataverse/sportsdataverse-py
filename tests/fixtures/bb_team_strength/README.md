<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D19 team-strength corpus](#bb-d19-team-strength-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D19 team-strength corpus

Real captures for `tests/mbb/test_mbb_team_strength.py`, downloaded from the sportsdataverse-data
releases on 2026-11-03 (zstd level 19). Nothing is synthetic or edited: rows are only filtered and
columns only dropped. The NBA/WNBA tests and the college possessions reuse BB-D3's
`../bb_team_season_rates` corpus (README there).

| File | Rows | Release asset | Kept |
|---|---:|---|---|
| `mbb_schedule_2025.parquet` / `wbb_schedule_2025.parquet` | 419 / 401 | `espn_{mens,womens}_college_basketball_schedules/{mbb,wbb}_schedule_2025` | Every ESPN game with one of the 18 Big Ten teams (plus the `game_id`s of `../bb_team_season_rates/{mbb,wbb}_schedule_2025.parquet`, all of which are such games), with the final score. The women's file has 4 games more than BB-D3's: Big Ten games ESPN carries that no NCAA possession row links to. Cols: `game_id, season, neutral_site, home_id, away_id, home_score, away_score, status_type_completed`. |
| `mbb_ratings_2025.parquet` / `wbb_ratings_2025.parquet` | 183 / 188 | `mbb_ratings/mbb_ratings_2025`, `wbb_ratings/wbb_ratings_2025` | Every team in those schedule rows or in BB-D3's possession fixture. All columns. |
