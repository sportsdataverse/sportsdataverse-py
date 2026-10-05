---
title: MLB — MLB Statcast (Baseball Savant)
sidebar_label: MLB Statcast (Baseball Savant)
description: "MLB — MLB Statcast (Baseball Savant) — endpoint reference in sdv-py, the SportsDataverse Python package."
sidebar_position: 11
toc_max_heading_level: 2
---
# MLB — MLB Statcast (Baseball Savant)

`sportsdataverse.mlb` — 39 endpoints.

## Leaderboard

| Function | Summary |
|---|---|
| [mlb_statcast_leaderboard_expected_stats](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_expected_stats) | GET /leaderboard/expected_statistics — xBA/xSLG/xwOBA/xISO expected-statistics leaderboard. |
| [mlb_statcast_leaderboard_percentile_rankings](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_percentile_rankings) | GET /leaderboard/percentile-rankings — player percentile-ranking sliders (xwOBA/xBA/xSLG/…). |
| [mlb_statcast_leaderboard_sprint_speed](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_sprint_speed) | GET /leaderboard/sprint_speed — sprint-speed (ft/sec) leaderboard. |
| [mlb_statcast_leaderboard_running_splits](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_running_splits) | GET /leaderboard/running_splits — 90-foot running splits leaderboard. |
| [mlb_statcast_leaderboard_bat_tracking](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_bat_tracking) | GET /leaderboard/bat-tracking — bat-tracking (swing speed / squared-up) leaderboard. |
| [mlb_statcast_leaderboard_swing_path](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_swing_path) | GET /leaderboard/bat-tracking/swing-path-attack-angle — swing path & attack-angle leaderboard. |
| [mlb_statcast_leaderboard_swing_timing](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_swing_timing) | GET /leaderboard/bat-tracking/swing-timing-miss-distance — swing timing & miss-distance leaderboard. |
| [mlb_statcast_leaderboard_swing_take](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_swing_take) | GET /leaderboard/swing-take — swing/take run-value leaderboard. |
| [mlb_statcast_leaderboard_exit_velocity_barrels](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_exit_velocity_barrels) | GET /leaderboard/statcast — exit velocity & barrels leaderboard. |
| [mlb_statcast_leaderboard_batted_ball](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_batted_ball) | GET /leaderboard/batted-ball — batted-ball profile leaderboard. |
| [mlb_statcast_leaderboard_home_runs](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_home_runs) | GET /leaderboard/home-runs — Statcast home-runs leaderboard. |
| [mlb_statcast_leaderboard_pitch_arsenals](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_pitch_arsenals) | GET /leaderboard/pitch-arsenals — pitch arsenals (velo/spin/movement) leaderboard. |
| [mlb_statcast_leaderboard_pitch_arsenal_stats](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_pitch_arsenal_stats) | GET /leaderboard/pitch-arsenal-stats — per-pitch-type outcome stats leaderboard. |
| [mlb_statcast_leaderboard_pitch_movement](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_pitch_movement) | GET /leaderboard/pitch-movement — pitch-movement leaderboard. |
| [mlb_statcast_leaderboard_pitch_tempo](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_pitch_tempo) | GET /leaderboard/pitch-tempo — pitch-tempo leaderboard. |
| [mlb_statcast_leaderboard_active_spin](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_active_spin) | GET /leaderboard/active-spin — active-spin leaderboard. |
| [mlb_statcast_leaderboard_spin_direction](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_spin_direction) | GET /leaderboard/spin-direction-pitches — spin-direction (per-pitch) leaderboard. |
| [mlb_statcast_leaderboard_arm_angles](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_arm_angles) | GET /leaderboard/pitcher-arm-angles — pitcher arm-angle leaderboard. |
| [mlb_statcast_leaderboard_pitcher_running_game](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_pitcher_running_game) | GET /leaderboard/pitcher-running-game — pitcher running-game (holding runners) leaderboard. |
| [mlb_statcast_leaderboard_outs_above_average](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_outs_above_average) | GET /leaderboard/outs_above_average — Outs Above Average (OAA) fielding leaderboard. |
| [mlb_statcast_leaderboard_outfield_directional_oaa](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_outfield_directional_oaa) | GET /leaderboard/outfield_directional_outs_above_average — outfield directional OAA leaderboard. |
| [mlb_statcast_leaderboard_outfield_jump](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_outfield_jump) | GET /leaderboard/outfield_jump — outfielder jump leaderboard. |
| [mlb_statcast_leaderboard_catch_probability](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_catch_probability) | GET /leaderboard/catch_probability — outfielder catch-probability leaderboard. |
| [mlb_statcast_leaderboard_arm_strength](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_arm_strength) | GET /leaderboard/arm-strength — fielder arm-strength leaderboard. |
| [mlb_statcast_leaderboard_poptime](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_poptime) | GET /leaderboard/poptime — catcher pop-time leaderboard. |
| [mlb_statcast_leaderboard_catcher_framing](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_catcher_framing) | GET /leaderboard/catcher-framing — catcher framing leaderboard. |
| [mlb_statcast_leaderboard_catcher_blocking](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_catcher_blocking) | GET /leaderboard/catcher-blocking — catcher blocking leaderboard. |
| [mlb_statcast_leaderboard_catcher_throwing](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_catcher_throwing) | GET /leaderboard/catcher-throwing — catcher throwing leaderboard. |
| [mlb_statcast_leaderboard_catcher_stance](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_catcher_stance) | GET /leaderboard/catcher-stance — catcher stance leaderboard. |
| [mlb_statcast_leaderboard_basestealing_run_value](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_basestealing_run_value) | GET /leaderboard/basestealing-run-value — basestealing run-value leaderboard. |
| [mlb_statcast_leaderboard_baserunning_run_value](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_baserunning_run_value) | GET /leaderboard/baserunning-run-value — baserunning run-value leaderboard. |
| [mlb_statcast_leaderboard_baserunning](mlb_statcast/leaderboard.md#mlb_statcast_leaderboard_baserunning) | GET /leaderboard/baserunning — extra-bases-taken run-value leaderboard. |
| [mlb_statcast_leaderboard_year_to_year](mlb_statcast/leaderboard-2.md#mlb_statcast_leaderboard_year_to_year) | GET /leaderboard/statcast-year-to-year — year-to-year metric change leaderboard. |
| [mlb_statcast_leaderboard_timer_infractions](mlb_statcast/leaderboard-2.md#mlb_statcast_leaderboard_timer_infractions) | GET /leaderboard/pitch-timer-infractions — pitch-timer infractions leaderboard. |
| [mlb_statcast_leaderboard_custom](mlb_statcast/leaderboard-2.md#mlb_statcast_leaderboard_custom) | GET /leaderboard/custom — build-your-own metric leaderboard (comma-separated selections). |
| [mlb_statcast_leaderboard_fielding_run_value](mlb_statcast/leaderboard-2.md#mlb_statcast_leaderboard_fielding_run_value) | GET /leaderboard/fielding-run-value — fielding run-value leaderboard (HTML-embedded JSON). |
| [mlb_statcast_leaderboard_park_factors](mlb_statcast/leaderboard-2.md#mlb_statcast_leaderboard_park_factors) | GET /leaderboard/statcast-park-factors — Statcast park-factors leaderboard (HTML-embedded JSON). |

## Other

| Function | Summary |
|---|---|
| [mlb_statcast_gamefeed](mlb_statcast/other.md#mlb_statcast_gamefeed) | GET /gf — Savant per-game JSON feed (pitch-by-pitch tracking). |
| [mlb_statcast_schedule](mlb_statcast/other.md#mlb_statcast_schedule) | GET /schedule — Savant schedule feed (one row per game). |
