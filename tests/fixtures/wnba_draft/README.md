<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WNBA draft/projection oracle corpus (T3.4 Phase 5 re-fit)](#wnba-draftprojection-oracle-corpus-t34-phase-5-re-fit)
  - [Files](#files)
  - [Fitted artifacts (genuine WNBA re-fits, replacing the T3.4 NBA-fit/placeholder seeds)](#fitted-artifacts-genuine-wnba-re-fits-replacing-the-t34-nba-fitplaceholder-seeds)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WNBA draft/projection oracle corpus (T3.4 Phase 5 re-fit)

Captured 2026-07-11. All `player_id` columns are `Utf8` (the raw integer id, stringified via
`.cast(pl.Int64).cast(pl.Utf8)` -- never from a float, per the id/join-key discipline).
`draft_year`/`season` are `Int64`.

**Capture note (transport):** direct residential access to `stats.wnba.com` worked initially
(the bulk `drafthistory` call, plus a burst test of ~70 sequential `playercareerstats` calls)
but degraded partway through the session -- every call started timing out (30s, 0 bytes),
including a re-try of the cheap bulk call that had succeeded easily before. That is a
cumulative rate/IP throttle, not a per-request fluke. The bulk of `season_stats_raw.parquet`
was captured via the ProxyBonanza pool (`dev/wnba_draft/proxy_transport.py`, round-robin over
~50 exit IPs) at `MAX_WORKERS=6` -- see `dev/wnba_draft/capture_corpus.py`'s module docstring
and `.superpowers/sdd/wnba-draft-refit/progress.md` for the full debugging record. Report this
honestly: this corpus needed the proxy, it did not come entirely from the residential IP.

**Determinism note:** the dedup step every fit/test script applies to `season_stats_raw.parquet`
(`.sort("min", descending=True).unique(subset=["player_id","season_id"], keep="first")`, to
collapse mid-season-trade duplicate rows) MUST pass `maintain_order=True` on both calls --
polars' default sort is unstable under threads, so ties in `min` pick a non-deterministic
survivor across separate process runs otherwise (observed: re-running `fit_availability.py`
without the fix gave holdout MAE 0.1847, 0.1890, 0.1913 across 3 runs on the same input file).

## Files

- **`draft_history.parquet`** (1201 rows, 29 classes 1997-2025) -- `wnba_stats_drafthistory()`,
  a single bulk call (no per-year loop needed, unlike the NBA combine capture). Columns include
  `person_id`/`player_id`, `player_name`, `season`/`draft_year`, `round_number`, `round_pick`,
  `overall_pick`, `team_id`, `organization`, `organization_type`. This is the feature source for
  `wnba_draft_model` (`overall_pick`, `round_number`) -- there is no combine-measurement
  equivalent (`wnba_stats_draftcombinestats` returns 0 rows for every WNBA season, confirmed
  live 2026-07-08 and again 2026-07-11).

- **`season_stats_raw.parquet`** (4221 rows, 734 distinct players out of the 1194 non-null
  `person_id`s in `draft_history.parquet`) -- `wnba_stats_playercareerstats(player_id=)`'s
  `SeasonTotalsRegularSeason` result set per drafted player: `player_id`, `season_id`,
  `player_age`, `gp`, `fga`, `fta`, `tov`, `pts`, `reb`, `ast`, `stl`, `blk`, `min`, etc. -- the
  identical 27-column schema `nba_stats_playercareerstats` ships, confirmed live 2026-07-11. The
  ~460 drafted players absent here never played a captured WNBA regular-season game (many late
  picks in a shallower league than the NBA never make a final roster) -- absence, not a zero
  row, is the correct representation; downstream fit scripts left-join and `fill_null(0.0)`.

- **`career_values.parquet`** (734 rows) -- materialized by `dev/wnba_draft/fit_box_value.py`
  from `season_stats_raw.parquet`, applying (not re-fitting) the existing
  `LEAGUE_CONSTANTS["wnba"].box_value_coef`/`replacement` (still NBA-fit -- there is no
  `wnba_bpm` or other independent advanced-metric anchor to ridge-regress WNBA box rates
  against, a permanent, documented caveat, not a pending TODO).

- **`rookie_values.parquet`** (1194 rows, one per `draft_history` player -- zero-filled for
  players absent from `season_stats_raw.parquet`) -- first/second captured season's box value
  per drafted player, also from `fit_box_value.py`.

## Fitted artifacts (genuine WNBA re-fits, replacing the T3.4 NBA-fit/placeholder seeds)

| Artifact | Fit script | Train / holdout | Observed |
|---|---|---|---|
| `wnba_aging_curve.json` | `fit_aging_curve.py` | all 24 observed ages (20-43) | peak_age 29, unimodal, genuinely different curve from `nba_aging_curve.json` |
| `wnba_draft_value.json` | `fit_draft_model.py` | train 1997-2018 (942), holdout 2019-2025 (271 scored) | holdout Spearman 0.228 vs realized `career_value`; `draft_prob` is a documented constant (~0.99), NOT a fitted classifier -- no undrafted/invitee negative class exists in this corpus |
| `wnba_availability.json` | `fit_availability.py` | train seasons \<=2018 (2713 rows), holdout 2019-2025 (1126 rows) | holdout MAE 0.2315 vs career-mean baseline 0.2485 |
| `wnba_rookie_projection.json` | `fit_rookie_residual.py` | train 1997-2018 classes (incl. train-only `rookie_fraction`) | holdout Spearman 0.127 vs realized `rookie_value` (composed with the draft-value + aging-curve artifacts above) |

See `tests/wnba/test_wnba_draft_backtest.py` for the oracle gates that pin these numbers down
(floors set with margin below/above the observed values, never lowered to pass).
