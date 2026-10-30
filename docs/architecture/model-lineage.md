# Model lineage

> Where every bundled model artifact and fitted constant in sportsdataverse-py comes from, and the script that fitted it.

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

## Bundled boosters (26)

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

### Basketball and baseball (6)

| artifact | fitting script | bundled in |
|---|---|---|
| `sportsdataverse/nba/models/nba_in_game_wp.ubj` | sdv-py `tools/models/nba_prediction/train_in_game_wp.py --league-id 00` (`2413d0bfb2`) | `2413d0bfb2` |
| `sportsdataverse/nba/models/wnba_in_game_wp.ubj` | sdv-py `tools/models/nba_prediction/train_in_game_wp.py --league-id 10 --artifact wnba_in_game_wp.ubj` (`2413d0bfb2`) | `2413d0bfb2` |
| `sportsdataverse/mbb/models/mbb_in_game_wp.ubj` | **gap**: `dev/mbb_prediction/train_in_game_wp.py` exists only on the maintainer's disk (untracked) | `5c3eaba8de` |
| `sportsdataverse/mbb/models/wbb_in_game_wp.ubj` | **gap**: same untracked script | `5c3eaba8de` |
| `sportsdataverse/mlb/models/mlb_stuff_plus.ubj` | sdv-py `tools/models/mlb_pitching/fit_stuff_plus.py`, names re-attached by `tools/models/mlb_pitching/relabel_boosters.py` (`9c883ae281`) | `9c883ae281` |
| `sportsdataverse/mlb/models/mlb_command_plus.ubj` | sdv-py `tools/models/mlb_pitching/fit_command_plus.py`, names re-attached by `tools/models/mlb_pitching/relabel_boosters.py` (`9c883ae281`) | `9c883ae281` |

None of these six ships a card.

## Fitted constants

| constant | fitting script | status |
|---|---|---|
| `mbb_ncaa_rapm_league.DEFAULT_RIDGE_LAMBDA` (1000.0) | `tools/fit_ncaa_rapm_lambda.py` | tracked (moved from untracked `dev/ncaa_rapm/fit_lambda_league.py`) |
| `nba_shot_value_constants.TALENT_SHRINKAGE_K["00"]` / `["20"]` (70.1) | `tools/fit_nba_shot_shrinkage_k.py` | tracked (moved from untracked `dev/nba_shot_value/fit_shrinkage_k.py`; reruns to 70.1 on the committed fixture) |
| `nba_shot_value_constants.TALENT_SHRINKAGE_K["10"]` (60.0) | none | **seeded placeholder**, not a fit: no WNBA shot fixture is committed |
| `paper_index` weights | `game-on-paper-app` `python/tools/fit_paper_index.py` (`43fd699f`) | tracked in that repo |
| `nfl_prediction_constants.RatingsConfig.ridge_lambda`, `NFL_CONSTANTS`, `PROP_CONSTANTS` | `dev/nfl_prediction/fit_{ridge_lambda,pregame,props}.py` | **lost** (see below) |

## Known gaps

Cited from package source but not tracked. **Lost** means the script is in no
branch's history (`git log --all`) and not on the maintainer's disk: the numbers
it produced are recorded in the source but cannot be re-derived. **Untracked**
means the script exists only in the maintainer's gitignored `dev/` directory;
committing it (under `tools/`, or force-added like the other tracked `dev/`
scripts) closes the gap.

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

Not cited from package source, so outside the test, but still gaps:

- `dev/mbb_prediction/train_in_game_wp.py` (untracked) trained `mbb_in_game_wp.ubj`
  and `wbb_in_game_wp.ubj`.
- `cfb_cp_model.card.json`, `cfb_cp_model_air_yards.card.json` and
  `fd_model.card.json` carry `training_seasons: null` and `n_training_rows: null`.
- No NFL, MBB, NBA or MLB booster ships a card.
- `nfl-data`'s registry lists the NFL QBR fitting script as TODO; the trainer
  named above (`cfb_qbr/qbr_xgb.R`) is the one the 2.7 feature-order fix traced
  the artifact to, not an NFL-trained replacement.

## Adding a model

Cite the fitting script by a path that is tracked: put a new fitter under
`tools/fit_<what>.py` (the `tools/fit_cfb_wp_overtime.py` pattern) rather than in
the gitignored `dev/`, add the artifact's row above, and label any value that is a
seed rather than a fit as seeded where it is defined.
