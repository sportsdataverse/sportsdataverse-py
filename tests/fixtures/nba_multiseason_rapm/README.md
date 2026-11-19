<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Multi-season RAPM possessions sample](#multi-season-rapm-possessions-sample)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Multi-season RAPM possessions sample

`possessions_sample.parquet` holds every regular-season possession of the first 30 regular-season games
(by `game_id`) of two NBA seasons: 2025-26 (`season` 2026, games `0022500001`-`0022500030`, 6,063
possessions) and 2026-27 (`season` 2027, `0022600001`-`0022600035`, 5,924). The rows come from the
`nba_stats_possessions` release assets, filtered to `season_type == "regular-season"` games through the
`nba_stats_schedules` assets, read 2026-11-18. Only the columns the fit reads are kept: `game_id`,
`season`, `points` and `off_player_1..5` / `def_player_1..5`. Nothing is synthetic or edited.

`tests/nba/test_nba_rapm_variants.py` uses it to check `nba_multiseason_rapm` against plain `nba_rapm`
on the pooled and single-season frames and to check its `n_seasons` count.

The oracle gate (`tools/gate_multiseason_rapm.py`) compares the full 2019-20 to 2026-27 window with
Jeremias Engelmann's 8-season RAPM. His sheet has no license, so it is not committed here.
