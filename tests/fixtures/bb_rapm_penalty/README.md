<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Plain-RAPM penalty corpus](#plain-rapm-penalty-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Plain-RAPM penalty corpus

`cv_sse.parquet` is the game-grouped 10-fold cross-validation error of plain RAPM, one row per league
(`nba`, `wnba`), regular season (named by its end year) and grid penalty (`alpha`, the 71 points of
`tools/fit_rapm_penalty.py`'s `GRID`): `sse` is the squared error of the held-out games' points, summed
over the ten folds. It is the `cv_sse.parquet` that `tools/fit_rapm_penalty.py report` wrote on
2026-11-06 from the `curves` stage, which read the `nba_stats_possessions` / `wnba_stats_possessions`
and `_schedules` release assets (NBA 1997-2027, the last in progress; WNBA 1997-2026). Nothing is
synthetic or edited.

`tests/tools/test_fit_rapm_penalty.py` sums it over the training seasons (NBA 1997-2026, WNBA
1997-2026) and checks that the minimum is `nba_rapm.RAPM_PENALTY` (NBA 2,683, WNBA 1,931), and pins
the as-of pick (pooled over a season and the ones before it).
