<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NFL projection/draft holdout corpus](#nfl-projectiondraft-holdout-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NFL projection/draft holdout corpus

Committed offline fixtures for the NFL projection & draft model-spine oracle
gates. Captured **2026-07-08** by `dev/nfl_projection/capture_corpus.py`
(untracked scratch script) from the shipped `sportsdataverse.nfl` loaders.
All ids are `Utf8` (`player_id` = nflverse gsis id), `season` is `Int64`,
counting stats are `Float64`. Regular season only (`season_type == "REG"`).

| File | Source loader | Seasons | Notes |
|---|---|---|---|
| `player_stats_2020_2023.parquet` | `load_nfl_player_stats()` | 2020-2023 | nflverse weekly offense schema; `fumbles_lost` = sack + rushing + receiving fumbles lost |
| `realized_2024.parquet` | `load_nfl_player_stats()` | 2024 | same schema — the holdout target |
| `rosters_2020_2023.parquet` | `load_nfl_rosters()` | 2020-2023 | `player_id` = `gsis_id`; `age` derived from `birth_date` at season start (Sep 1); unique per (player_id, season) |
| `snap_counts_2020_2023.parquet` | `load_nfl_snap_counts()` | 2020-2023 | `pfr_player_id` crosswalked to gsis `player_id` via `load_nfl_ff_playerids()` (inner join) |
| `snap_counts_2024.parquet` | `load_nfl_snap_counts([2024])` | 2024 | same crosswalk — the availability holdout target (realized games = distinct weeks with `offense_snaps > 0`) |
| `ff_rankings_2024.parquet` | `load_nfl_ff_rankings(kind="all")` | preseason 2024 | FantasyPros `redraft-overall` ECR, latest pre-kickoff scrape (2024-08-30), fp id crosswalked to gsis via `load_nfl_ff_playerids()`; `ecr` = min across duplicate id matches |
| `draft_matured.parquet` | `load_nfl_draft_picks()` + `load_nfl_combine()` | draft classes 2000-2019 | combine measurables left-joined on `pfr_id`; `ht` parsed `"6-2"` → inches. **`car_av` is sourced from nflverse `w_av`** (PFR weighted career AV) — the upstream `car_av` column is an all-null Boolean; null `w_av` on a drafted player = never accrued AV → filled `0.0` |
| `draft_holdout.parquet` | same | classes 2015-2019 | the oracle-gate slice (matured outcomes); draft-model training must use classes `≤ target_class − 5` |
