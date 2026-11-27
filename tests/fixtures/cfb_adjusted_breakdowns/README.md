<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_adjusted_breakdowns fixtures: provenance](#cfb_adjusted_breakdowns-fixtures-provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_adjusted_breakdowns fixtures: provenance

| file | rows | source | notes |
|---|---|---|---|
| `fbs_plays_2025.parquet` | 106,731 | `cfbfastR-cfb-data` @ `bde27a1b2` (2026-11-27): `cfb/pbp/parquet/play_by_play_2025.parquet` + `cfb/cfb_schedules/parquet/cfb_schedules_2025.parquet`, passed through that repo's `cfb_data_build.summaries_input.prepare_plays_input` (FBS vs FBS, pass/rush, kneels dropped), the frame the team summaries are built from | Down-selected to the 19 columns `cfb_adjusted_breakdowns` and `cfb_adjusted_epa` read: `game_id`, `week`, `seasonType`, `pos_team`, `pos_team_id`, `def_pos_team_id`, `home`, `neutral_site`, `EPA`, `pass`, `rush`, `period`, `pos_score_diff_start`, `epa_success`, `EPA_explosive`, `down`, `standard_down`, `passing_down`, `rz_play`. The build input's names are kept (`epa_success`, `down`); the released pbp calls them `EPA_success` and `start.down`, and a test checks both spellings give the same board. |
| `split_half_by_season.csv` | 960 | `tools/fit_cfb_adjusted_breakdowns.py --data-repo <cfbfastR-cfb-data>` on 2014-2025, sdv-py branch `feat/cfb-adjusted-breakdowns` (the module as committed with it), data repo `875e56e83` (2026-11-27; the released 2014-2025 pbp and schedule parquets, unchanged since `bde27a1b2`) | Per season, breakdown, side and value (raw / adj): teams in both halves, split-half `r`, Spearman-Brown `sb`, the halves' covariance and mean variance, the harmonic-mean half play count `h` and the noise per play. `tests/tools/test_fit_cfb_adjusted_breakdowns.py` re-derives `_PUBLISHED` from it. |
