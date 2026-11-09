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

`tests/tools/test_fit_adj_rapm_penalty.py` checks four things:

- Summed over the training seasons at `k = 2`, the minimum is `nba_adj_rapm.ADJ_RAPM_PENALTY` (NBA
  3,728, WNBA 2,683).
- At `k = 1` the minimum is NBA 3,162 / WNBA 2,276.
- The pooled error at `k = 2` is below the error at `k = 1` (the prior-scale evidence).
- `k = 0` reproduces plain RAPM's frozen penalty, `RAPM_PENALTY`.
