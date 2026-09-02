<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`nfl_ep_wp` fixtures](#nfl_ep_wp-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `nfl_ep_wp` fixtures

Real (not synthetic) nflverse play-by-play rows, captured so the EP / WP / CP
feature-convention regression tests in `tests/nfl/test_nfl_ep_wp_real_rows.py`
run offline against the bundled `sportsdataverse/nfl/models/*.ubj` boosters.

| File | Provenance | Captured |
|---|---|---|
| `kickoff_rows_2014_2025.parquet` | `load_nfl_pbp([season])` for each season 2014–2025 (nflverse `play_by_play_{season}.parquet` releases), rows with `play_type == "kickoff" \| kickoff_attempt == 1`, every k-th row so 40 rows/season spread across games (480 rows). Carries the raw model inputs plus nflverse's own `ep` / `wp` / `vegas_wp` renamed `nflverse_*`, `receive_2h_ko` derived on the FULL game (first non-null `defteam`, as `_add_wp_aux` does), and `next_play_yardline_100` (the following play's `yardline_100` — after a touchback, the spot the RULE actually put the ball on). | 2026-09-01 |
| `pass_rows_2023.parquet` | `load_nfl_pbp([2023])`, rows with `play_type == "pass"`, non-null `air_yards` and `cp`, every k-th row (300 rows). Carries the CP / xYAC model inputs plus nflverse `cp` / `xyac_mean_yardage` renamed `nflverse_*`. | 2026-09-01 |
| `overtime_games.parquet` | `load_nfl_pbp([season])` for 2010 / 2014 / 2019 / 2023 / 2025 — the **whole** first overtime game of each season by sorted `game_id` (`2010_01_ATL_PIT`, `2014_01_BUF_CHI`, `2019_01_DET_ARI`, `2023_01_BUF_NYJ`, `2025_02_NYG_DAL`; 976 rows, 96 of them overtime). One game per overtime rules era, labelled in `rules_era_label`. Whole games, not sampled rows, because the overtime overlay's `First_Drive` is a per-game minimum and `enrich_nfl_pbp`'s feature substitution and non-play fill are game-scoped — a sampled frame would score differently from production. Carries nflverse's own `ep` / `wp` / `vegas_wp` renamed `nflverse_*`. | 2026-09-02 |

Capture recipe: `dev/kickoff_audit.py` in the sdv-py checkout that produced the
2026-09-01 audit (the [50] kickoff-constants item of the model-writeup
improvement program) writes both files as a side effect; the selection is
deterministic (sorted by `game_id, play_id`, every k-th row), so re-running it
on the same nflverse release reproduces the rows byte-for-byte.

Why these rows: kickoff rows are where `_apply_feature_substitution` rewrites
the EP/WP inputs (touchback yardline 80 / 75, down 1, ydstogo 10) — the one
place a kickoff-rule change (2024 dynamic kickoff: touchback to the 30, 2025:
to the 35) could silently desynchronise sdv-py from the nflverse oracle. Pass
rows are where `distance_to_sticks = air_yards - ydstogo` (the nflfastR sign)
feeds the CP model — the sign convention that bit once already. Overtime games are
where nflfastR does not run the WP boosters at all (`add_wp_variables` L820-899
substitutes a closed form off the EP class probabilities and sets `vegas_wp = wp`) —
the branch whose missing port put overtime `wp` MAE at 0.17-0.21 against nflverse
while the all-plays figure stayed at 0.015.
