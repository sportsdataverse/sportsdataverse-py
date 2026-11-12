---
title: Model lineage
sidebar_label: Model lineage
sidebar_position: 4
description: "Where every bundled model artifact and fitted constant in sportsdataverse-py comes from, and the script that fitted it."
---

# Model lineage

Every bundled model artifact and every fitted constant in sdv-py should name the
script that produced it, and that script should be one a reader can open. This
page records where that holds and where it does not. `tests/test_model_lineage.py`
keeps it honest: every `dev/**.py` or `tools/**.py` path cited from package source
must be tracked by git or be listed under [Known gaps](#known-gaps) below, a gap
that becomes tracked must be removed from the list, and every bundled `.ubj`
booster must have a row in the booster registry.

A script in another repository is cited as `repo` + path + the commit it was read
at. "Byte-identical" means the bundled file's sha256 equals the GitHub release
asset's `digest` (checked 2026-10-22).

## Bundled boosters (30)

### CFB (`sportsdataverse/cfb/models/`, 11)

The trainers live in
[`cfbfastR-cfb-data`](https://github.com/sportsdataverse/cfbfastR-cfb-data) under
`python/cfb_model_build/` (read at `14419a885`); that repo's `models/REGISTRY.md`
holds each model's gates, retrain date and cadence. Each booster here ships a
`.card.json` beside it.

| artifact | fitting script | bundled in | release byte-identical |
|---|---|---|---|
| `sportsdataverse/cfb/models/ep_model.ubj` | `model_training/train_ep.py` | `05bfcb1641` | no (`espn_cfb_model_artifacts` `ep.ubj` is the older 2026-06-18 fit) |
| `sportsdataverse/cfb/models/wp_spread.ubj` | `model_training/train_wp.py --variant spread` | `05bfcb1641` | no (release asset 2026-06-18) |
| `sportsdataverse/cfb/models/wp_naive.ubj` | `model_training/train_wp.py --variant naive` | `05bfcb1641` | no (release asset 2026-06-18) |
| `sportsdataverse/cfb/models/cfb_cp_model.ubj` | `cpoe/` (`python -m cfb_model_build.cpoe --variant game_state`) | `fbe11c473a` | no |
| `sportsdataverse/cfb/models/cfb_cp_model_air_yards.ubj` | `cpoe/` (air-yards variant) | `4b34e0e81e` | yes |
| `sportsdataverse/cfb/models/fd_model.ubj` | `model_training/fourth_down/` | `fbe11c473a` | no |
| `sportsdataverse/cfb/models/fg_model.ubj` | `model_training/train_fg.py` | `05bfcb1641` | no |
| `sportsdataverse/cfb/models/two_pt_model.ubj` | `model_training/train_two_pt.py` | `05bfcb1641` | no |
| `sportsdataverse/cfb/models/xpass_model.ubj` | `model_training/train_xpass.py` | `05bfcb1641` | no |
| `sportsdataverse/cfb/models/qbr_model.ubj` | `model_training/train_qbr.py` | `291ad943e0` | yes |
| `sportsdataverse/cfb/models/wp_ot_reach.ubj` | sdv-py `tools/fit_cfb_wp_overtime.py` (`408a21e9cb`) | `408a21e9cb` | n/a (bundle only; the card pins the sha256 of the boosters it corrects) |

The seven boosters bundled in `05bfcb1641` ("retrain the model suite on the
corrected corpus") are newer than the `espn_cfb_model_artifacts` assets of the
same name, so the release is not their source; their cards record the training
frame and seasons.

### NFL (`sportsdataverse/nfl/models/`, 9)

The trainers live in [`nfl-data`](https://github.com/sportsdataverse/nfl-data)
under `python/model_training/` (read at `98cfb72`); its `models/REGISTRY.md` holds
the gates and cadence. No NFL booster ships a card in sdv-py.

| artifact | fitting script | bundled in | release byte-identical |
|---|---|---|---|
| `sportsdataverse/nfl/models/ep_model.ubj` | `play_level` at nfl-data `bbe7269` (nflfastR's `cal_data` rows and roof mapping, 1999-2025 `nfl_model_pbp`) | this PR | no (refit after the 09-10 release; `nfl_model_artifacts` is republished when nfl-data#84 merges) |
| `sportsdataverse/nfl/models/wp_naive.ubj` | `play_level` | `c61e0d9ad1` | yes |
| `sportsdataverse/nfl/models/wp_spread.ubj` | `play_level` | `c61e0d9ad1` | yes |
| `sportsdataverse/nfl/models/cp_model.ubj` | `play_level` at nfl-data#88 (trained from 2009) | this PR | no (refit after the 09-10 release; `nfl_model_artifacts` is republished with the nfl-data follow-up) |
| `sportsdataverse/nfl/models/xpass_model.ubj` | `decision_models` | `c61e0d9ad1` | yes |
| `sportsdataverse/nfl/models/fg_model.ubj` | `decision_models` (per the nfl-data registry) | `c61e0d9ad1` | unverifiable (no release asset; bundle only) |
| `sportsdataverse/nfl/models/two_pt_model.ubj` | `decision_models` (per the nfl-data registry) | `c61e0d9ad1` | unverifiable (no release asset; bundle only) |
| `sportsdataverse/nfl/models/qbr_model.ubj` | `akeaswaran/cfb_qbr` `qbr_xgb.R` (`ca94e63`, 2021; a CFB-trained model) | `2e52d0625a` (re-saved with feature names, trees unchanged) | n/a (no release) |
| `sportsdataverse/nfl/models/nfl_playcall.ubj` | sdv-py `tools/models/nfl_scheme/train_playcall.py` (`04b729e6d2`) | `04b729e6d2` | n/a (bundle only) |

### Basketball and baseball (10)

| artifact | fitting script | bundled in |
|---|---|---|
| `sportsdataverse/nba/models/nba_in_game_wp.ubj` | sdv-py `tools/models/nba_prediction/train_in_game_wp.py --league-id 00` (`2413d0bfb2`) | `2413d0bfb2` |
| `sportsdataverse/nba/models/wnba_in_game_wp.ubj` | sdv-py `tools/models/nba_prediction/train_in_game_wp.py --league-id 10 --artifact wnba_in_game_wp.ubj` (`2413d0bfb2`) | `2413d0bfb2` |
| `sportsdataverse/nba/models/nba_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id 00` (train 2022-2024, holdout 2025, confirmation 2026; gates, partition and importance beside it) | #1113 |
| `sportsdataverse/nba/models/wnba_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id 10` (train 2022-2024, holdout 2025, confirmation 2026; dated as-of scoring level; one owner-accepted gate-3 bucket; card, gates, partition and importance beside it) | #1120 |
| `sportsdataverse/mbb/models/mbb_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id mbb` (train 2022-2024, holdout 2025, confirmation 2026; as-of scoring level; gates, partition and importance beside it) | #1118 |
| `sportsdataverse/mbb/models/wbb_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id wbb` (train 2022-2024, holdout 2025, confirmation 2026; as-of scoring level; gates, partition and importance beside it) | #1118 |
| `sportsdataverse/mbb/models/mbb_in_game_wp.ubj` | sdv-py `tools/models/mbb_prediction/train_in_game_wp.py --league mbb` (train 2022, 2023, 2025; holdout 2026; confirmation 2024) | #1104 |
| `sportsdataverse/mbb/models/wbb_in_game_wp.ubj` | sdv-py `tools/models/mbb_prediction/train_in_game_wp.py --league wbb` (same seasons) | #1104 |
| `sportsdataverse/mlb/models/mlb_stuff_plus.ubj` | sdv-py `tools/models/mlb_pitching/fit_stuff_plus.py`, names re-attached by `tools/models/mlb_pitching/relabel_boosters.py` (`9c883ae281`) | `9c883ae281` |
| `sportsdataverse/mlb/models/mlb_command_plus.ubj` | sdv-py `tools/models/mlb_pitching/fit_command_plus.py`, names re-attached by `tools/models/mlb_pitching/relabel_boosters.py` (`9c883ae281`) | `9c883ae281` |

The four in-game WP boosters each ship a `.card.json` beside them, built from the fit report, the holdout
scoring and the booster: NBA and WNBA by sdv-py `tools/models/nba_prediction/wp_model_cards.py`
(`tests/nba/test_nba_wp_model_cards.py`), MBB and WBB by `tools/models/mbb_prediction/wp_model_cards.py`
(`tests/mbb/test_mbb_wp_model_cards.py`, which also holds the college holdout gate). Each test fails on a stale
card. The other five ship none.

The college refit (#1104) has a pre-registered confirmation season, 2027 (owner decision, 2026-11-09): once
it is complete, the 2026 holdout gate (the same rule, blocks and fixed replaced booster) is run on it once,
with nothing re-tuned in between. The reason is the gate's power: the overtime block holds only 326 MBB /
257 WBB holdout games, and 52 gate cells (22 MBB, 30 WBB) at a one-sided 2.5% false-failure rate expect
about 1.3 false failures per refit. The gate keeps its absolute-value form (|calibration in the large|
compared paired); the owner chose "as is" over a signed paired difference. Both cards carry it
(`holdout_gate.preregistered_confirmation`), and the card test pins it.

## Fitted constants

| constant | fitting script | status |
|---|---|---|
| `nba_rapm.RAPM_PENALTY` (NBA 2,683, WNBA 1,931) | `tools/fit_rapm_penalty.py` | tracked; `curves` reads the possessions release assets (network), `report` scores offline and writes the pooled CV error that `tests/fixtures/bb_rapm_penalty` commits, from which `tests/tools/test_fit_rapm_penalty.py` re-derives both values and the as-of picks. The G League key `"20"` is **seeded** with the NBA value, not fitted. Refit policy: re-run after each complete season, and change a value only when the pooled pick moves off it by more than one standard error of the per-season differences (it moved once in the NBA history, 2,276 to 2,683 at 2018-19); a change re-scales every season, so it ships with a republish of every season's `player_impact`. Plain RAPM, `nba_player_impact` / `wnba_player_impact` and `{nba,wnba}_player_factor_rapm` fit before November 2026 used RidgeCV's per-season pick instead and are not comparable across seasons |
| `nba_adj_rapm.ADJ_RAPM_PENALTY` (NBA 3,728, WNBA 2,683) and `ADJ_RAPM_PRIOR_SCALE` (2.0 both) | `tools/fit_adj_rapm_penalty.py` | tracked. `curves` reads the possessions release assets and the -raw stores' leaguegamelog captures (network or local) and chains the SPM prior as the producers do. `report` scores offline and writes the pooled CV error over penalty and prior scale that `tests/fixtures/bb_adj_rapm_penalty` commits, from which `tests/tools/test_fit_adj_rapm_penalty.py` re-derives the penalty at the frozen scale and checks that the scale beats the unscaled prior on held-out games. The scale itself was chosen where next-season R^2 and Spearman peak, which only `report` measures (its `report.json` `forecast` block); the test does not re-derive that peak. The G League key `"20"` is **seeded** with the NBA values. Refit policy: as `RAPM_PENALTY`'s; a change re-scales every season, so it ships with a republish of every season's `player_impact`. Adjusted RAPM fit before #1150 used RidgeCV's per-season pick at the unscaled prior |
| `nba_box_prior.BOX_PRIOR_PAD_POSS` (1,000), `BOX_PRIOR_HALF_LIFE` (4 seasons), `BOX_PRIOR_RIDGE_ALPHA` (1.0), `BOX_PRIOR_AGE_CENTER` (27), `BOX_PRIOR_LEAVE_SEASON_OUT_THROUGH` (2005), `BOX_PRIOR_FOLDS` (5); `nba_reml_rapm.REML_PENALTY_BOUNDS` (10, 1e7); `tools/fit_box_prior_v2.py` `CHRONO_SHARE` (0.6) | none (chosen a priori, documented at the constants) | **not fitted**: set before the gate ran and not tuned on it. `tools/fit_box_prior_v2.py` fits the box prior itself (per season: walk-forward from 2006, leave-one-season-out through 2005, five player folds; nothing is frozen in the package) and gates `nba_reml_rapm` against `nba_adj_rapm` on held-out games and a chronological split; `tests/fixtures/bb_box_prior_v2` commits its `gate.parquet` / `report.json` and `tests/tools/test_fit_box_prior_v2.py` re-derives the gate and tests the harness's leakage wiring. `nba_reml_rapm` defaults to the fixed `ADJ_RAPM_PENALTY` (owner decision 2026-11-10: it beat the per-call REML pick on held-out games in 27 of 29 seasons); that value was fitted for the previous-season SPM prior, so refitting it for the v2 prior is a follow-up. `penalty="reml"` chooses penalties per call. Cadence: TODO, no workflow runs the fitter yet; publishing the season rating (and the 2026-27 prior, whose player game logs are not captured yet) is a follow-up |
| `nba_current_impact.CURRENT_IMPACT_ENTRY_SD` (0.983), `CURRENT_IMPACT_SEASON_SD` (0.417), `CURRENT_IMPACT_TEAM_SD` (0.026), `CURRENT_IMPACT_DRIFT_SD` (0.0115 per sqrt day); a priori: `CURRENT_IMPACT_LEVEL_SD` (0.05), `CURRENT_IMPACT_NOISE_VAR_FALLBACK` (1.3), `CURRENT_IMPACT_PRIOR_WINDOW_DAYS` (365) | `tools/fit_current_impact.py` | tracked. `inputs` builds weekly box priors from the BB-D38 run (`tools/fit_box_prior_v2.py`, local release assets and the -raw store); `fit` chooses the four SDs by Nelder-Mead on the 28-day forward MSE of NBA 2007-2013 (filter from 2006, the first strictly walk-forward prior season; it stopped at its 80-evaluation cap on a surface flat to 1e-6, so the team SD and the drift are not identified: zeroing either moves the gate error by under 0.03 bp); harness settings chosen a priori: `STEP_DAYS` 7, `HORIZON_DAYS` 28, `REF_POSS` 20,000, `BOOT` 10,000, the Nelder-Mead start (1.8, 1.0, 0.5, 0.05) and x1.65 simplex, `BASELINE_PENALTY_GRID`; `gate` / `report` score 2014-2026 against the season-to-date `nba_reml_rapm` (also at the window prior and at a penalty tuned on 2007-2013) with a season-cluster bootstrap. `tests/fixtures/bb_current_impact` commits `fit.json` / `gate.parquet` / `report.json`; `tests/tools/test_fit_current_impact.py` checks the constants equal `fit.json`, re-derives the margins and holds the gate floors. Cadence: TODO, no workflow runs it; publishing a daily current rating is a follow-up |
| `mbb_ncaa_rapm_league.DEFAULT_RIDGE_LAMBDA` (1000.0) | `tools/fit_ncaa_rapm_lambda.py` | tracked (moved from untracked `dev/ncaa_rapm/fit_lambda_league.py`) |
| `nba_shot_value_constants.TALENT_SHRINKAGE_K["00"]` / `["20"]` (70.1) | `tools/fit_nba_shot_shrinkage_k.py` | tracked (moved from untracked `dev/nba_shot_value/fit_shrinkage_k.py`; reruns to 70.1 on the committed fixture) |
| `nba_shot_value_constants.TALENT_SHRINKAGE_K["10"]` (60.0) | none | **seeded placeholder**, not a fit: no WNBA shot fixture is committed |
| `team_luck.PYTHAG_EXPONENT` (nba 14.02, wnba 10.64, mbb 8.07, wbb 6.80) | `tools/fit_team_luck_pythag.py` | tracked; reruns to the frozen values on the committed `tests/fixtures/bb_team_luck` records (`tests/tools/test_fit_team_luck_pythag.py`); refit when `SPANS` is extended |
| `player_war.REPLACEMENT_LEVEL` (nba -1.98, wnba -3.18, mbb -1.66, wbb -1.99) and `player_war.PTS_PER_WIN` (nba 31.56, wnba 31.31, mbb 38.91, wbb 47.10) | `tools/fit_player_war.py` | tracked; reruns to the frozen values on the committed `tests/fixtures/bb_player_war` player-seasons and `bb_team_luck` records (`tests/tools/test_fit_player_war.py`); refit every year after the season, moving `SPANS` forward a season and re-deriving the holdout gates (the replacement level drifts up with roster depth; the college holdout seasons sit 0.23 / 0.42 per 100 above the frozen level) |
| `team_strength.RIDGE_LAMBDA` (nba 1600, wnba 800) and `team_strength.COLLEGE_RIDGE_LAMBDA` (mens 200, womens 50) | `tools/fit_team_strength_ridge.py` | tracked; season-disjoint, game-grouped CV (NBA 2022-2026, WNBA 2021-2025, college 2016-2020 + 2022-2023, kept disjoint from the 2024-2026 method comparison in `tools/compare_college_team_strength.py`); refit when the possessions releases are rebuilt |
| `rate_gates.TEAM_MIN_N` and `rate_gates.ON_OFF_MIN_N` (the rank sample gates of the basketball team, lineup and on/off season tables: each rate's split-half r = 0.5 sample, rounded up to two significant figures) | `tools/fit_rate_gates.py` | tracked; reruns to the frozen values on the committed `tests/fixtures/bb_rate_gates` curves (`tests/tools/test_fit_rate_gates.py`, which also scores the holdout seasons); refit every year after the college season ends (April), moving `SPANS` forward a season, and re-derive the holdout floors. Owner-accepted exceptions (owner decision 2026-11-09): three WNBA gates stay ranked under a third signal on the unseen 2023-2025 seasons (holdout r = gate / (gate + holdout M)): team `pace` 7 games 0.331, on/off `off_rtg_on` 610 possessions 0.267 and `off_rtg_diff` 720 possessions 0.314. Re-measured season by season on the current releases (possessions PIPELINE_VERSION 7; sportsdataverse-py #1142) no data or source defect drives them: the gates refit to the same values (holdout `off_rtg_on` 0.263, `off_rtg_diff` 0.312, `pace` 0.331), on-court possessions tie out to the team's, no fit season predates 2015, and the team offensive rating of both sources drops on 2023-2025 as `off_rtg_on` does; pace's dip is 2025 alone. The figures are the gate fit's inputs: pace is ESPN box possessions per game (the table's own possession-table pace scores 0.44 at the gate), and the on/off halves include playoff games (regular season alone: `off_rtg_on` 0.35, `off_rtg_diff` never half signal). `test_low_holdout_rates_are_the_accepted_exceptions` pins exactly these three |
| `paper_index` weights | `game-on-paper-app` `python/tools/fit_paper_index.py` (`43fd699f`) | tracked in that repo |
| `nfl_prediction_constants.RatingsConfig.ridge_lambda`, `NFL_CONSTANTS`, `PROP_CONSTANTS` | `dev/nfl_prediction/fit_{ridge_lambda,pregame,props}.py` | **lost** (see below) |

## Known gaps

Cited from package source but not tracked. **Lost** means the script is in no
branch's history (`git log --all`) and not on the maintainer's disk: the numbers
it produced are recorded in the source but cannot be re-derived. **Untracked**
means the script exists only in the maintainer's gitignored `dev/` directory;
committing it (under `tools/`, or force-added like the other tracked `dev/`
scripts) closes the gap.

<!-- lineage-gaps:start -->

| cited path | status | cited from |
|---|---|---|
| `dev/nfl_prediction/fit_ridge_lambda.py` | lost | `nfl/nfl_prediction_constants.py` |
| `dev/nfl_prediction/fit_pregame.py` | lost | `nfl/nfl_prediction_constants.py` |
| `dev/nfl_prediction/fit_props.py` | lost | `nfl/nfl_prediction_constants.py` |
| `dev/nfl_projection/fit_availability.py` | lost | `nfl/nfl_availability.py`, `nfl/nfl_projection_constants.py` |
| `dev/nfl_projection/fit_draft_lambda.py` | lost | `nfl/nfl_draft_model.py` |
| `dev/nfl_projection/fit_fantasy_calibration.py` | lost | `nfl/nfl_projection_constants.py` |
| `dev/nfl_projection/fit_shrinkage.py` | lost | `nfl/nfl_projection_constants.py` |
| `dev/nhl_player_impact/fit_goals_per_win.py` | lost | `nhl/nhl_player_impact_constants.py` |
| `dev/mlb_pitching/fit_era_siera.py` | lost | `mlb/mlb_pitching_constants.py` |
| `dev/mbb_prediction/fit_pregame.py` | untracked | `mbb/mbb_prediction_constants.py`, `wbb/wbb_prediction_constants.py` |
| `dev/mbb_player_value/fit_archetypes.py` | untracked | `mbb/models/{mbb,wbb}_archetypes.json` |
| `dev/mbb_player_value/fit_box_bpm.py` | untracked | `mbb/models/{mbb,wbb}_box_bpm.json` |
| `dev/mbb_player_value/fit_draft.py` | untracked | `mbb/models/{mbb,wbb}_draft.json` |
| `dev/mbb_player_value/fit_recruiting.py` | untracked | `mbb/models/{mbb,wbb}_recruiting.json` |
| `dev/mbb_player_value/fit_transfer.py` | untracked | `mbb/models/{mbb,wbb}_transfer.json` |
| `dev/ncaa_rapm/build_name_changes.py` | untracked | `mbb/mbb_ncaa_rapm_input.py` |
| `dev/ncaa_proxy.py` | untracked | `mbb/mbb_ncaa_fetch.py` |
| `dev/t5_xg_reevaluation/xg_model_ideas.py` | untracked | `pwhl/pwhl_xg_proxy.py` |

<!-- lineage-gaps:end -->

Not cited from package source, so outside the test, but still gaps:

- `cfb_cp_model.card.json`, `cfb_cp_model_air_yards.card.json` and
  `fd_model.card.json` carry `training_seasons: null` and `n_training_rows: null`.
- No NFL or MLB booster ships a card; of the basketball boosters, the four in-game WP models and `wnba_epv.ubj` do (`nba_epv.ubj` and the college EPV boosters ship none).
- `nfl-data`'s registry lists the NFL QBR fitting script as TODO; the trainer
  named above (`cfb_qbr/qbr_xgb.R`) is the one the 2.7 feature-order fix traced
  the artifact to, not an NFL-trained replacement.

## Adding a model

Cite the fitting script by a path that is tracked: put a new fitter under
`tools/fit_<what>.py` (the `tools/fit_cfb_wp_overtime.py` pattern) rather than in
the gitignored `dev/`, add the artifact's row above, and label any value that is a
seed rather than a fit as seeded where it is defined.
