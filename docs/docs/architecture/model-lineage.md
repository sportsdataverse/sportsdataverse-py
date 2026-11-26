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

## Bundled boosters (31)

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
| `sportsdataverse/cfb/models/fd_model.ubj` | `model_training/fourth_down/`; kept by the 2026-holdout rebuild gate (`fourth_down/rebuild_gate.py`, result `models/fourth_down/fd_rebuild.gate.json`, cfb-data `895b0e21f`) | `fbe11c473a` | no |
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

### Basketball and baseball (11)

| artifact | fitting script | bundled in |
|---|---|---|
| `sportsdataverse/nba/models/nba_in_game_wp.ubj` | sdv-py `tools/models/nba_prediction/train_in_game_wp.py --league-id 00` (`2413d0bfb2`) | `2413d0bfb2` |
| `sportsdataverse/nba/models/wnba_in_game_wp.ubj` | sdv-py `tools/models/nba_prediction/train_in_game_wp.py --league-id 10 --artifact wnba_in_game_wp.ubj` (`2413d0bfb2`) | `2413d0bfb2` |
| `sportsdataverse/nba/models/nba_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id 00` (train 2022-2024, holdout 2025, confirmation 2026; 8-second transition window since 2026-11-17; gates, partition and importance beside it) | #1113, refit #1216 |
| `sportsdataverse/nba/models/wnba_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id 10` (train 2022-2024, holdout 2025, confirmation 2026; dated as-of scoring level; 8-second transition window since 2026-11-17; card, gates, partition and importance beside it) | #1120, refit #1216 |
| `sportsdataverse/mbb/models/mbb_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id mbb` (train 2022-2024, holdout 2025, confirmation 2026; as-of scoring level; gates, partition and importance beside it) | #1118 |
| `sportsdataverse/mbb/models/wbb_epv.ubj` | sdv-py `tools/models/nba_epv/train_epv.py --league-id wbb` (train 2022-2024, holdout 2025, confirmation 2026; as-of scoring level; gates, partition and importance beside it) | #1118 |
| `sportsdataverse/nba/models/nba_shot_make.ubj` | sdv-py `tools/models/nba_shot_value/train_shot_make.py --league-id 00` (train 2023-2025, holdout 2026, monitor 2027; gated walk-forward 2019-2026 against the zone table it replaces as `score_shot_xpoints`'s NBA default, threes on the zone value; report, partition and importance beside it. Three gate-2 misses, 2019, 2021 and 2023, are owner-accepted exceptions (`OWNER_ACCEPTED`, owner ruling 2026-11-25). The WNBA fit failed its gates and ships no booster: `shot_make_wnba_holdout.json`. Cadence: TODO, no workflow runs the fitter; refit after each complete season, moving the train, holdout and walk seasons forward, then refit `TALENT_SHRINKAGE_K["00"]`) | #1311 |
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
| `nba_spm.SPM_ALPHA` (100, the default equal-weight fit) and `SPM_ALPHA_WEIGHTED` (30), `nba_adj_rapm.ADJ_RAPM_PENALTY` (NBA 3,728, WNBA 2,683), `ADJ_RAPM_PRIOR_SCALE` (2, default) and `ADJ_RAPM_PRIOR_SCALE_WEIGHTED` (1.5) | `tools/fit_adj_rapm_penalty.py` | tracked. The defaults are the published configuration (#1150). The `*_WEIGHTED` constants are the minute-weighted alternative (#1309): opt-in through `train_spm(..., weight="min")`, and not shipped, because as the prior it fails the NBA producer's `r_rapm_adj_min` floor (2026-11-26). `spm` fits the prior's own penalty first: the player-grouped CV error of the minute-weighted `train_spm` on each season's own plain RAPM, pooled over 1997-2026 per league; `tests/fixtures/bb_adj_rapm_penalty` commits that curve (`spm_cv.parquet`) and four seasons' training rows, from which `tests/tools/test_fit_adj_rapm_penalty_spm.py` re-derives `SPM_ALPHA_WEIGHTED` (the NBA pick; the WNBA's own pick, 100, is within 0.3% at 30, so the value is shared) and gates the weighting on a next-season split. `curves --prior-rule equal` into a second OUT plus `report --baseline` pair the refit against the equal-weight prior at its #1150 scale 2 season by season (held-out error on the same games, next-season Spearman / R^2); the fixture's `report.json` carries that block, and `test_fit_adj_rapm_penalty.py` gates the per-season spread band and the paired held-out delta from it. `curves` reads the possessions release assets and the -raw stores' leaguegamelog captures (network or local) and chains the SPM prior as the producers do. `report` scores offline and writes the pooled CV error over penalty and prior scale that `tests/fixtures/bb_adj_rapm_penalty` commits, from which `tests/tools/test_fit_adj_rapm_penalty.py` re-derives the penalty at the frozen scale and checks that the scale beats the unscaled prior on held-out games. The scale itself was chosen where next-season R^2 and Spearman peak and held-out error is at or near its minimum, which only `report` measures (its `report.json` `forecast` block); the test does not re-derive that peak. The G League key `"20"` is **seeded** with the NBA values. Refit policy: as `RAPM_PENALTY`'s; a change re-scales every season, so it ships with a republish of every season's `player_impact`. Adjusted RAPM fit before #1150 used RidgeCV's per-season pick at the unscaled prior |
| `nba_box_prior.BOX_PRIOR_PAD_POSS` (1,000), `BOX_PRIOR_HALF_LIFE` (4 seasons), `BOX_PRIOR_RIDGE_ALPHA` (1.0), `BOX_PRIOR_AGE_CENTER` (27), `BOX_PRIOR_LEAVE_SEASON_OUT_THROUGH` (2005), `BOX_PRIOR_FOLDS` (5); `nba_reml_rapm.REML_PENALTY_BOUNDS` (10, 1e7); `tools/fit_box_prior_v2.py` `CHRONO_SHARE` (0.6) | none (chosen a priori, documented at the constants) | **not fitted**: set before the gate ran and not tuned on it. `tools/fit_box_prior_v2.py` fits the box prior itself (per season: walk-forward from 2006, leave-one-season-out through 2005, five player folds; nothing is frozen in the package) and gates `nba_reml_rapm` against `nba_adj_rapm` on held-out games and a chronological split; `tests/fixtures/bb_box_prior_v2` commits its `gate.parquet` / `report.json` and `tests/tools/test_fit_box_prior_v2.py` re-derives the gate and tests the harness's leakage wiring. `nba_reml_rapm` defaults to the fixed `ADJ_RAPM_PENALTY` (owner decision 2026-11-10: it beat the per-call REML pick on held-out games in 27 of 29 seasons); that value was fitted for the previous-season SPM prior, so refitting it for the v2 prior is a follow-up. `penalty="reml"` chooses penalties per call. Cadence: TODO, no workflow runs the fitter yet; publishing the season rating (and the 2026-27 prior, whose player game logs are not captured yet) is a follow-up |
| `nba_current_impact.CURRENT_IMPACT_ENTRY_SD` (0.983), `CURRENT_IMPACT_SEASON_SD` (0.417), `CURRENT_IMPACT_TEAM_SD` (0.026), `CURRENT_IMPACT_DRIFT_SD` (0.0115 per sqrt day); a priori: `CURRENT_IMPACT_LEVEL_SD` (0.05), `CURRENT_IMPACT_NOISE_VAR_FALLBACK` (1.3), `CURRENT_IMPACT_PRIOR_WINDOW_DAYS` (365) | `tools/fit_current_impact.py` | tracked. `inputs` builds weekly box priors from the BB-D38 run (`tools/fit_box_prior_v2.py`, local release assets and the -raw store); `fit` chooses the four SDs by Nelder-Mead on the 28-day forward MSE of NBA 2007-2013 (filter from 2006, the first strictly walk-forward prior season; it stopped at its 80-evaluation cap on a surface flat to 1e-6, so the team SD and the drift are not identified: zeroing either moves the gate error by under 0.03 bp); harness settings chosen a priori: `STEP_DAYS` 7, `HORIZON_DAYS` 28, `REF_POSS` 20,000, `BOOT` 10,000, the Nelder-Mead start (1.8, 1.0, 0.5, 0.05) and x1.65 simplex, `BASELINE_PENALTY_GRID`; `gate` / `report` score 2014-2026 against the season-to-date `nba_reml_rapm` (also at the window prior and at a penalty tuned on 2007-2013) with a season-cluster bootstrap. `tests/fixtures/bb_current_impact` commits `fit.json` / `gate.parquet` / `report.json`; `tests/tools/test_fit_current_impact.py` checks the constants equal `fit.json`, re-derives the margins and holds the gate floors. Cadence: TODO, no workflow runs it; publishing a daily current rating is a follow-up |
| `mbb_ncaa_rapm_league.DEFAULT_RIDGE_LAMBDA` (1000.0) | `tools/fit_ncaa_rapm_lambda.py` | tracked (moved from untracked `dev/ncaa_rapm/fit_lambda_league.py`) |
| `nba_shot_value_constants.TALENT_SHRINKAGE_K["00"]` (58.1) / `["20"]` (70.1) | `tools/fit_nba_shot_shrinkage_k.py` | tracked (moved from untracked `dev/nba_shot_value/fit_shrinkage_k.py`); fitted once per model the expectation comes from, on the committed NBA 2022-23 fixture (`tests/tools/test_fit_nba_shot_shrinkage_k.py` reruns both): `--model location` gives 58.1, the NBA's (its default model, threes on the zone value, refit 2026-11-25; was 70.1 on the zone table), and `--model zone` gives 70.1, which the G League reuses (scored from the zone table, no G League fixture). Refit `"00"` after every `nba_shot_make.ubj` refit |
| `nba_shot_value_constants.TALENT_SHRINKAGE_K["10"]` (60.0) | none | **seeded placeholder**, not a fit: no WNBA shot fixture is committed |
| `team_luck.PYTHAG_EXPONENT` (nba 14.02, wnba 10.64, mbb 8.07, wbb 6.80) | `tools/fit_team_luck_pythag.py` | tracked; reruns to the frozen values on the committed `tests/fixtures/bb_team_luck` records (`tests/tools/test_fit_team_luck_pythag.py`); refit when `SPANS` is extended |
| `player_war.REPLACEMENT_LEVEL` (nba -1.98, wnba -3.18, mbb -1.66, wbb -1.99) and `player_war.PTS_PER_WIN` (nba 31.56, wnba 31.31, mbb 38.91, wbb 47.10) | `tools/fit_player_war.py` | tracked; reruns to the frozen values on the committed `tests/fixtures/bb_player_war` player-seasons and `bb_team_luck` records (`tests/tools/test_fit_player_war.py`); refit every year after the season, moving `SPANS` forward a season and re-deriving the holdout gates (the replacement level drifts up with roster depth; the college holdout seasons sit 0.23 / 0.42 per 100 above the frozen level) |
| `team_strength.RIDGE_LAMBDA` (nba 1600, wnba 800) and `team_strength.COLLEGE_RIDGE_LAMBDA` (mens 200, womens 50) | `tools/fit_team_strength_ridge.py` | tracked; season-disjoint, game-grouped CV (NBA 2022-2026, WNBA 2021-2025, college 2016-2020 + 2022-2023, kept disjoint from the 2024-2026 method comparison in `tools/compare_college_team_strength.py`); refit when the possessions releases are rebuilt |
| `team_strength.COLLEGE_RIDGE_PRIOR_REGRESSION` (mens 0.7762, womens 0.8858) and `team_strength.COLLEGE_RIDGE_PRIOR_LAMBDA` (mens 200, womens 200), the college possession ridge's last-season prior | `tools/fit_college_ridge_prior.py` | tracked; fit jointly on an as-of walk of 2022-2023 only and scored on 2024-2026 (disjoint from the train seasons and from `COLLEGE_RIDGE_LAMBDA`'s fit seasons; the method comparison in `tools/compare_college_team_strength.py` scored the same seasons before); the fit's `--out` is committed as `tests/fixtures/bb_team_strength/ridge_prior_{mbb,wbb}.json` and `test_the_prior_constants_are_the_committed_fit` pins the constants to it; refit each year after the college season ends, moving the train and holdout seasons forward, and widen the women's penalty grid (the chosen 200 is its top) |
| `rate_gates.TEAM_MIN_N` and `rate_gates.ON_OFF_MIN_N` (the rank sample gates of the basketball team, lineup and on/off season tables: each rate's split-half r = 0.5 sample, rounded up to two significant figures) | `tools/fit_rate_gates.py` | tracked; reruns to the frozen values on the committed `tests/fixtures/bb_rate_gates` curves (`tests/tools/test_fit_rate_gates.py`, which also scores the holdout seasons); refit every year after the college season ends (April), moving `SPANS` forward a season, and re-derive the holdout floors. The on/off gates were refitted on 2026-11-17 with the NBA / WNBA off side over the player's span with the team, one span per season type, as `player_on_off(tenure=True)` counts it (sportsdataverse-py #1218): NBA `off_rtg_diff` 2400 -> 2500, `def_rtg_diff` 3400 -> 3900 and `net_rtg_diff` 7900 -> 7100, WNBA `off_rtg_diff` 720 -> 750; NBA `net_rtg_on` 3200 -> 3100 is release drift since the 2026-11-06 fit. Refitted again on 2026-11-18 on the listed-games span of sportsdataverse-py #1240 (it opens at the first listed game, box-score DNP or inactive list, and closes at the last where every game names everyone who played; `SPAN_VERSION` 3): NBA `def_rtg_diff` 3900 -> 4000 and `net_rtg_diff` 7100 -> 7300, WNBA `off_rtg_diff` 750 -> 780 (holdout 0.351 -> 0.370); the `_on` curves are unchanged. Owner ruling (2026-11-17): WNBA on/off `net_rtg_diff` is never ranked (`math.inf`), not a fitted value: its train M is 2333 (a 2400 gate), but its 2023-2025 span holdout curve never comes near half signal (r -0.04 to 0.37, fitted M past twice the largest sample); `OWNER_NEVER_RANKED` in the test pins it. Owner-accepted exceptions (owner decision 2026-11-09): WNBA gates that stay ranked under a third signal on the unseen 2023-2025 seasons (holdout r = gate / (gate + holdout M)): team `pace` 7 games 0.331 and on/off `off_rtg_on` 610 possessions 0.263 (0.267 on the 2026-11-06 fixture, release drift); on/off `off_rtg_diff` (720, 0.314) left the list at the span refit (750, 0.351). For those two, re-measured season by season on the current releases (possessions PIPELINE_VERSION 7; sportsdataverse-py #1142) no data or source defect drives them: the gates refit to the same values (holdout `off_rtg_on` 0.263, `off_rtg_diff` 0.312, `pace` 0.331), on-court possessions tie out to the team's, no fit season predates 2015, and the team offensive rating of both sources drops on 2023-2025 as `off_rtg_on` does; pace's dip is 2025 alone. The figures are the gate fit's inputs: pace is ESPN box possessions per game (the table's own possession-table pace scores 0.44 at the gate), and the on/off halves include playoff games (regular season alone: `off_rtg_on` 0.35, `off_rtg_diff` never half signal). `test_low_holdout_rates_are_the_accepted_exceptions` pins exactly these |
| `nba_season_sim.HFA_PSEUDO_POSS` (NBA 80,000, WNBA 10,000 non-neutral possessions) | `tools/fit_pro_hfa_shrink.py` | tracked; reads the `{nba,wnba}_stats_possessions` / `_schedules` release assets (network) for the train seasons only (NBA 2018 2019 2022 2023, WNBA 2018 2019 2021) and writes `tests/fixtures/nba_prediction/hfa_shrink_<lg>.json`, which `tests/nba/test_nba_season_sim.py` checks the constants against (no offline re-derivation: the fit needs the releases). The BB-D25 holdout (`tools/models/nba_prediction/wp_model_cards.py`, NBA 2024-2026, WNBA 2024 2025) scored the frozen values. Refit policy: frozen; refit when the train seasons move forward or the possessions releases are rebuilt, and then re-run the BB-D25 holdout and the `rated_priors` backfill |
| `paper_index` weights | `game-on-paper-app` `python/tools/fit_paper_index.py` (`43fd699f`) | tracked in that repo |
| `nfl_prediction_constants.RatingsConfig.ridge_lambda`, `NFL_CONSTANTS`, `PROP_CONSTANTS` | `dev/nfl_prediction/fit_{ridge_lambda,pregame,props}.py` | **lost** (see below) |
| `nhl_player_impact_constants.LEAGUE_CONSTANTS["nhl"].league_xg_rate_pp` / `league_xg_rate_pk` (4.99) | `tools/fit_nhl_special_teams_rates.py` | tracked; reads the `nhl_pbp_full` / `nhl_shifts` release assets (network, about 50 minutes a season), scores them with the published `nhl_xg_models` boosters and pools every special-teams stint's power-play xG per 60. No offline re-derivation test. Refit 2026-11-17 after `build_stints` stopped dropping eventless intervals and stopped carrying a strength across an unexplained skater-count change (was 5.91). Refit after every xG retrain, since the rate is on the boosters' scale. The PWHL values (5.4) are **seeded** |
| `nhl_player_impact_constants.LEAGUE_CONSTANTS["nhl"].replacement_ev_off` / `replacement_ev_def` / `league_xg_rate_ev` (2.26 / 2.91 / 2.55) | `tools/fit_nhl_replacement_level.py` | tracked; reads the `nhl_pbp_full` / `nhl_shifts` release assets (network, about 2.5 hours a season), scores them with the published `nhl_xg_models` boosters, fits the 5v5 RAPM the WAR uses and takes the TOI-weighted RAPM of the skaters below the league-wide top 18 x n_teams by 5v5 TOI as the replacement level (a league-wide cut, since skaters change teams and the shift chart has no positions) (`tests/tools/test_fit_nhl_replacement_level.py` checks the pooling and the pool offline; no offline re-derivation of the values). Refit after every xG retrain. The PWHL values (1.6 / 2.2 / 2.1) are **seeded** |
| `nhl_prediction_constants.LEAGUE_CONSTANTS["nhl"].hfa` / `margin_sd` / `total_scale` / `avg_xgf` (0.0298 / 0.5109 / 2.3189 / 2.00) | `tools/models/nhl_prediction/fit_pregame.py` | tracked; reads the `nhl_pbp_full` / `nhl_schedules` release assets (network, about a minute with a warm cache) and walks the 2022-23 season with as-of-date ratings: `hfa` and `margin_sd` are fitted jointly by Brier, `total_scale` is the OLS slope of the realized total, and `avg_xgf` is the mean of the 2022-23 .. 2025-26 even-strength xG-for per team-game. No offline re-derivation test. Re-run 2026-11-18 on the 2026-11-14 republish: `hfa` / `margin_sd` / `total_scale` reproduce to 4 dp and `avg_xgf` to the shipped 2 dp (the seasons average 1.999). Refit after every xG retrain. `avg_total_goals` (6.05) is a **seeded** published reference, though the script prints the realized mean |

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
Then give the model its `tools/models/model_ledger.json` row, with the lifecycle
fields below.

## Lifecycle: baseline, retire rule, coverage, sign stability

Each `model_ledger.json` row (schema 1.1) carries `baseline` (the model it was
shown to beat or tie on held-out data), `primary_metric` (that comparison's metric
and SE clustering), `adoption_margin` (its paired `model - baseline` difference,
negative = better), `status` (`active`, `baseline_tied` or `retired`; `baseline_tied` marks a
model that ties its baseline by construction) and `lifecycle_note` (the
evidence, or why a field is null). An optional `reserved_seasons` list names seasons another
study holds back (WNBA 2022, 2023 and 2026 for the sim-vs-market spec); the retire rule and
`coverage.py` refuse to score them. Fill them from committed evidence only; a field
the evidence does not hold stays null with the reason in the note.
`uv run python tools/models/lifecycle/retire.py --check-ledger` checks them, and
`tests/tools/test_model_lifecycle.py` runs that check.

Three checks run on shipped models, each reported first; one becomes a gate only by
an owner decision:

- **Retire rule** (`tools/models/lifecycle/retire.py`). After each completed season
  past the holdout, score the model once against its baseline (same metric,
  paired, same clustering, no refit). Two consecutive seasons whose upper 97.5%
  bound of `model - baseline` is at or above 0 (consecutive scored seasons: a
  season with no record between two failures does not break the run), or one season worse than the
  baseline by more than the adoption margin's magnitude (`abs(adoption_margin)`;
  the ledger stores a gain as negative) at 1.96 SE (its lower 97.5% bound above
  it), make it a RETIRE-candidate. The latest
  season failing alone is WATCH; before any season is scored it is UNSCORED. A
  `baseline_tied` row ties every season by construction, so for it only the margin
  trigger runs (a margin of 0 when the ledger states none) and it is never WATCH. The rule
  runs only on a row with both a `baseline` and `holdout_seasons`. Retiring is the owner's call: `status: retired`
  with `retired_reason`, one release of warning through
  `sportsdataverse/_deprecation.py`, then the function returns the baseline. A
  retired model is re-adopted only on a new fit with its own holdout.
- **Win-total coverage** (`tools/models/lifecycle/coverage.py`) for the NBA / WNBA
  season sim: realized wins against the sim's 50 / 80 / 95% intervals at the
  preseason and an in-season cutoff, on seasons outside the fit, clustered by
  season. Only seasons outside every fit on the sim's path count (NBA and WNBA
  2010-2017); NBA 2024-2026 and WNBA 2024-2025 sit inside the rest-and-travel fit or
  the ridge penalty's cross-validation, so they are scored only with `--near-holdout`
  and labelled. It also checks that the rating draw widens the
  preseason 80% intervals at least 1.5 times; if it doesn't, that is a bug.
- **Sign stability** (`tools/models/lifecycle/signs.py`) for a model refit per
  season: a coefficient whose sign flips between folds with both estimates more
  than 2 SE from zero is a finding, and it puts the model on WATCH at its next
  season.
