# CFB user-facing model calculators (cfbfastR + sportsdataverse-py)

**Status:** design, approved to spec
**Scope:** CFB only. The other sports, and the other five nflfastR-parity subsystems
(team colors/logos, id decoding, stat aggregators, data dictionaries, per-sport
`clean_*_pbp`), are explicitly out of scope and get their own specs.

## Problem

`nflfastR` exposes `calculate_expected_points(pbp)` -- hand it a data frame with the
right columns and get model output back, whether those rows came from a real game or
were typed by hand to ask a hypothetical. sdv-py has the equivalent surface for **NFL
only** (`calculate_expected_points`, `calculate_win_probability`, `calculate_epa`,
`calculate_wpa`, `calculate_xpass`, `calculate_xyac`, `calculate_completion_probability`).

CFB has none of it, in either language, despite shipping nine trained models.

Two distinct reasons, and the second is the real one:

1. **cfbfastR's `create_epa()` demands the models from the caller.** Its signature is
   `create_epa(play_df, ep_model, fg_model, season)`. It is exported, but a user must
   already hold booster objects to call it, and the loader that produces them
   (`.cfb_model_file()`) is internal. An exported function can still be unusable.

2. **The translation from human columns to model features does not exist as a callable
   unit.** The cards declare *internal* feature names -- `TimeSecsRem`, `down_1..down_4`,
   `ExpScoreDiff_Time_Ratio`, `pos_score_diff_start` -- and the code that builds them is
   inline inside `cfb_pbp.py`'s scoring pass (~L6900), entangled with pbp-only columns
   such as `start.adj_TimeSecsRem`. A user with `down` / `distance` / `yards_to_goal` has
   none of the model's actual inputs.

So this is not a wrapper. It is the extraction of that derivation into a shared,
tested layer, with nine named entry points on top.

## What already exists

The `cfb_model_artifacts` release is a published cross-language contract:

- `MANIFEST.json` -- `model_version`, `ep_class_contract.class_order`, and
  `consumers: ["cfbfastR (R)", "sportsdataverse-py (Python)"]`
- Per-model `*.card.json` carrying an explicit ordered `features` array

| model | objective | features |
|---|---|---|
| `ep_model` | multi:softprob | TimeSecsRem, yards_to_goal, distance, down_1..down_4, pos_score_diff_start |
| `fg_model` | binary:logistic | yards_to_goal, era0..era3 |
| `qbr_model` | reg:squarederror | qbr_epa, sack_epa, pass_epa, rush_epa, pen_epa, spread, era0..era3 |
| `two_pt_model` | binary:logistic | posteam_spread, posteam_total, pos_score_diff, era |
| `wp_naive` | binary:logistic | 12 incl. adj_TimeSecsRem, ExpScoreDiff_Time_Ratio |
| `wp_spread` | binary:logistic | 13, as wp_naive plus spread_time |
| `xpass_model` | binary:logistic | down, distance, yards_to_goal, pos_score_diff, TimeSecsRem, era, period |
| `cfb_cp_model` | -- | **no card published** |
| `fd_model` | -- | **no card published** |

## Prerequisites (cfbfastR-cfb-data, first)

Approved sequencing: complete the contract before building against it.

1. **Publish cards for `cfb_cp_model` and `fd_model`.** Two shipped models with no
   published contract is a gap regardless of this work.
2. **Populate `era_contract` on every era-consuming card.** It is currently `null` on
   all seven, yet `fg_model`/`qbr_model` consume one-hot `era0..era3` and
   `two_pt_model`/`xpass_model` consume ordinal `era`. `_era_contract()` already exists
   in the trainer (cfb-data#71); the bundle simply has not been republished since.

Until (2) lands, era encoding remains a private constant duplicated in each consumer --
which is exactly what caused cfb-data#70, where cfbfastR and sdv-py both drifted to a
2017 cut the trainer never used and scored 2018-2020 an era off. Reading the rule from
the card is the structural fix for that whole class of bug, not just its last instance.

3. Republish the bundle and bump `model_version`.

## Architecture

Three layers, mirrored in both languages.

### 1. Feature derivation -- `derive_model_features(df, model, season)`

Natural user columns in, the card's declared feature matrix out. The single place that
knows how to build one-hot downs, era encodings, `adj_TimeSecsRem`,
`ExpScoreDiff_Time_Ratio` and `spread_time`. Extracted from the inline `cfb_pbp.py`
logic rather than reimplemented, so the pipeline and the calculators cannot diverge.

Accepts either natural names (`down`, `distance`, `yards_to_goal`, `clock`/`TimeSecsRem`,
`pos_score_diff`) or already-derived model names, so a pbp frame passes through
unchanged and a hand-built frame is completed.

### 2. Card-driven predict -- `predict_from_card(df, model)`

Loads the card, asserts every declared feature is present after derivation, builds the
matrix **in the card's declared order**, predicts. Feature order is load-bearing for
XGBoost and the card is what fixes it; nothing restates the list.

### 3. Ten public `calculate_*` functions over nine models

| function | model(s) |
|---|---|
| `calculate_expected_points` | `ep_model` (+ `fg_model` for the FG-weighted adjustment) |
| `calculate_win_probability` | `wp_spread` when a spread is supplied, else `wp_naive` |
| `calculate_epa` | derived from EP; recomputes it when absent |
| `calculate_wpa` | derived from WP; recomputes it when absent |
| `calculate_field_goal_probability` | `fg_model` |
| `calculate_completion_probability` | `cfb_cp_model` |
| `calculate_xpass` | `xpass_model` |
| `calculate_two_point_probability` | `two_pt_model` |
| `calculate_fourth_down` | `fd_model` |
| `calculate_qbr` | `qbr_model` |

Ten functions, nine models: the two WP boosters sit behind one entry point selected by
whether the caller supplies a spread (mirroring how the pipeline already chooses), while
`calculate_epa` / `calculate_wpa` are derivations rather than separate boosters. Each is
a thin wrapper with a full returns table.

**Naming:** `calculate_*` in BOTH languages, matching nflfastR and sdv-py's NFL surface.
cfbfastR's `create_epa()` / `create_wpa_naive()` stay as the low-level layer the new
functions wrap -- layered, not duplicated, and no existing caller breaks.

## Error handling

A frame missing required columns produces ONE error naming exactly which are absent and
which model wanted them, sourced from the card. `cli::cli_abort()` in R (package
convention), `ValueError` in Python. Never a silent empty frame and never a bare
XGBoost shape error -- the failure mode this design exists to remove is a user unable to
tell what their frame is missing.

## Testing

- **Oracle round-trip:** score a committed pbp fixture through the new calculators and
  assert it reproduces what the shipped pipeline already produces. The pipeline is the
  oracle; if the extracted derivation drifts, this fails.
- **Hand-built row:** the hypothetical case -- a one-row frame typed by hand scores
  without touching any pbp machinery.
- **Missing-column contract:** each calculator names the absent columns.
- **Cross-language parity:** one fixture scored in both R and Python, asserted equal
  within tolerance. Both read the same cards, so this is now checkable.
- **Card-order regression:** a card whose `features` order is permuted must change the
  prediction, proving order is honored rather than incidentally correct.

## Out of scope

Other sports; the other five parity subsystems; retraining or changing any model;
`add_qb_epa`-style mutators (these calculators return frames, callers can join).

## Open question deferred to implementation

Whether `calculate_epa` / `calculate_wpa` should recompute EP/WP internally or require
them present. nflfastR recomputes. Recommend matching nflfastR, decided when the
derivation layer is extracted and the cost is visible.
