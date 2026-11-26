<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Adjusted-RAPM penalty corpus](#adjusted-rapm-penalty-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Adjusted-RAPM penalty corpus

`cv_sse.parquet` holds the game-grouped 10-fold cross-validation error of adjusted RAPM (ridge toward
the previous season's SPM prior). There is one row per league (`nba`, `wnba`), regular season (named by
its end year), prior scale (`k`, the 8 values of `tools/fit_adj_rapm_penalty.py`'s `PRIOR_SCALES`) and
grid penalty (`alpha`, the 71 points of `tools/fit_rapm_penalty.py`'s `GRID`). `sse` is the squared
error of the held-out games' points, summed over the ten folds.

It is the `cv_sse.parquet` that `tools/fit_adj_rapm_penalty.py report` wrote on 2026-11-09. The
`curves` stage behind it read:

- the `nba_stats_possessions` / `wnba_stats_possessions` and `_schedules` release assets (1997-2026
  both);
- the leaguegamelog captures of local hoopR-nba-stats-raw / wehoop-wnba-stats-raw checkouts, which
  give the box logs the SPM prior is trained on.

Nothing is synthetic or edited. Rezipped at zstd level 19.

The SPM prior's own fit (`tools/fit_adj_rapm_penalty.py spm`, run 2026-11-25 on the same inputs):

- `spm_cv.parquet`: one row per league, regular season 1997-2026, and `train_spm` penalty (`alpha`, the
  9 points of `SPM_ALPHAS`): `sse` is the player-grouped 5-fold CV error of the minute-weighted
  `train_spm` on that season's own plain RAPM (offense plus defense squared error, each held-out row
  weighted by its minutes over the training rows' mean); `n` is the season's player count.
- `spm_rows_2024_2025.parquet`: four of the seasons' training rows that stage wrote (NBA 2023-24 and
  2024-25, WNBA 2024 and 2025; 1,479 rows). Columns: `league`, `season`, then `box_features`
  (`player_id`, the 11 per-100 rates, `min`, `gp`) joined to `nba_rapm` (`o_rapm`, `d_rapm`, `rapm`,
  `off_poss`, `def_poss`).

`tests/tools/test_fit_adj_rapm_penalty_spm.py` re-derives `nba_spm.SPM_ALPHA` from `spm_cv.parquet`,
re-runs the CV on a committed season to match its row, and gates the weighting: trained on 2024 and
scored on 2025's box line, the default fit's Spearman and spread against 2025's own plain RAPM sit
above floors that the equal-weight fit it replaced misses.

`tests/tools/test_fit_adj_rapm_penalty.py` checks four things:

- Summed over the training seasons at `k = 2`, the minimum is `nba_adj_rapm.ADJ_RAPM_PENALTY` (NBA
  3,728, WNBA 2,683).
- At `k = 1` the minimum is NBA 3,162 / WNBA 2,276.
- The pooled error at `k = 2` is below the error at `k = 1` (the prior-scale evidence).
- `k = 0` reproduces plain RAPM's frozen penalty, `RAPM_PENALTY`.
