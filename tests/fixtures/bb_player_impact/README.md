<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Player-impact league-context corpus](#player-impact-league-context-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Player-impact league-context corpus

Real captures for `tests/nba/test_nba_player_impact.py`, taken on 2026-11-09 from the
sportsdataverse-data releases (`gh release download nba_player_impact` / `wnba_player_impact`,
the assets `load_nba_player_impact(2025)` and `load_wnba_player_impact(2025)` read), zstd level
19. Nothing is synthetic: rows are untouched (both season types), columns only dropped.

| File | Rows | Kept columns |
|---|---:|---|
| `nba_player_impact_2025.parquet` | 788 (569 Regular Season, 219 Playoffs) | `season, season_type, player_id, player_name, team_id, off_poss, def_poss`, the 13 ranked metrics (`o_rapm, d_rapm, rapm, o_adj_rapm, d_adj_rapm, adj_rapm, ospm, dspm, spm, obpm, dbpm, bpm, war`), `min, gp` |
| `wnba_player_impact_2025.parquet` | 272 (181 Regular Season, 91 Playoffs) | the same |

Id dtypes: `player_id` / `team_id` / `season` / `off_poss` / `def_poss` Int64. `season` is the END
year as the releases stamp it.

These are the releases as published before the November 2026 rebuild, so their `o_adj_rapm` /
`d_adj_rapm` still carry the no-intercept offsets (about +11 / -10). The rank tests do not depend
on the values being centred: they check the grammar (gates, direction, percentile formula, cohort).
