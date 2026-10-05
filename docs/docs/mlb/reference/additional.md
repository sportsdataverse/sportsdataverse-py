---
title: MLB — additional Python functions
sidebar_label: Additional functions
description: "MLB — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# MLB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.mlb`
not covered by the generated API-endpoint reference above.

## Fox

| Function | Summary |
|---|---|
| [fox_mlb_event_matchup](additional/fox.md#fox_mlb_event_matchup) | Fox Sports mlb pregame team-stat comparison (one row per stat). |
| [fox_mlb_event_recap](additional/fox.md#fox_mlb_event_recap) | Fox Sports mlb postgame top performers (one row per player). |
| [fox_mlb_event_standings](additional/fox.md#fox_mlb_event_standings) | Fox Sports mlb the two teams' standings context. |
| [fox_mlb_league_conferences](additional/fox.md#fox_mlb_league_conferences) | Fox Sports mlb conference / group directory. |
| [fox_mlb_league_header](additional/fox.md#fox_mlb_league_header) | Fox Sports mlb league header (one row). |
| [fox_mlb_league_leaders](additional/fox.md#fox_mlb_league_leaders) | MLB statistical leaders (`stats-con`); who=player\|team. |
| [fox_mlb_league_odds](additional/fox.md#fox_mlb_league_odds) | Fox Sports mlb league odds board (one row per team per game). |
| [fox_mlb_league_player_news](additional/fox.md#fox_mlb_league_player_news) | Fox Sports mlb league-wide player news feed. |
| [fox_mlb_league_polls](additional/fox.md#fox_mlb_league_polls) | Fox Sports mlb rankings / polls rendered as standings tables. |
| [fox_mlb_league_schedule](additional/fox.md#fox_mlb_league_schedule) | Fox Sports mlb league schedule nav selections. |
| [fox_mlb_league_scores](additional/fox.md#fox_mlb_league_scores) | Fox Sports mlb league scores nav selections. |
| [fox_mlb_league_standings](additional/fox.md#fox_mlb_league_standings) | Fox Sports mlb league-wide standings tables. |
| [fox_mlb_league_stat_leaders](additional/fox.md#fox_mlb_league_stat_leaders) | Fox Sports mlb league stats landing leaders. |
| [fox_mlb_odds](additional/fox.md#fox_mlb_odds) | MLB game odds six-pack (run line / to-win / total per team). |
| [fox_mlb_scoreboard](additional/fox.md#fox_mlb_scoreboard) | Fox Sports mlb scoreboard nav selections (weeks / dates / groups). |
| [fox_mlb_scorechip](additional/fox.md#fox_mlb_scorechip) | Fox Sports mlb compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_mlb_scores_segment](additional/fox.md#fox_mlb_scores_segment) | Fox Sports mlb one row per game in a scoreboard segment. |
| [fox_mlb_standings](additional/fox.md#fox_mlb_standings) | MLB standings for a team's division/league. |
| [fox_mlb_team_gamelog](additional/fox.md#fox_mlb_team_gamelog) | MLB team game log (long: one row per game-stat). |
| [fox_mlb_team_header](additional/fox.md#fox_mlb_team_header) | Fox Sports mlb team header (one row). |
| [fox_mlb_team_roster](additional/fox.md#fox_mlb_team_roster) | MLB team roster (one row per player). |
| [fox_mlb_team_stats](additional/fox.md#fox_mlb_team_stats) | MLB team stat leaders by category. |
| [fox_mlb_teamnav](additional/fox.md#fox_mlb_teamnav) | Fox Sports mlb team directory (one row per team). |

## Mlb

| Function | Summary |
|---|---|
| [mlb_attendance](additional/mlb.md#mlb_attendance) | GET /api/v1/attendance — game attendance figures. |
| [mlb_baserunning_value](additional/mlb.md#mlb_baserunning_value) | Per-runner baserunning runs from extra-bases-taken above expected. |
| [mlb_batter_projection](additional/mlb.md#mlb_batter_projection) | Next-season xwOBA projection (Marcel + delta-method aging) for every batter. |
| [mlb_catcher_blocking](additional/mlb.md#mlb_catcher_blocking) | Per-catcher blocking runs from a dirt-pitch block-probability model. |
| [mlb_catcher_framing](additional/mlb.md#mlb_catcher_framing) | Per-catcher framing runs from a smooth called-strike logistic (Savant method). |
| [mlb_catcher_throwing](additional/mlb.md#mlb_catcher_throwing) | Per-catcher caught-stealing (throwing) value = caught-stealing above average. |
| [mlb_command_plus](additional/mlb.md#mlb_command_plus) | Score pitches with the bundled Command+/Location+ (②) run-value model. |
| [mlb_divisions](additional/mlb.md#mlb_divisions) | GET /api/v1/divisions — list divisions. |
| [mlb_draft_prospects](additional/mlb.md#mlb_draft_prospects) | GET /api/v1/draft/prospects/{year} — draft prospect list for a year. |
| [mlb_expected_home_runs](additional/mlb.md#mlb_expected_home_runs) | Per player-season park-neutral xHR, park-adjusted xHR, and HR-above-expected. |
| [mlb_expected_stats](additional/mlb.md#mlb_expected_stats) | Per player-season xwOBA/xBA/xSLG from an on-the-fly EV x LA empirical grid. |
| [mlb_fielding_oaa](additional/mlb.md#mlb_fielding_oaa) | Per-fielder outs above average from a per-position catch-probability logistic. |
| [mlb_injury_risk](additional/mlb.md#mlb_injury_risk) | Composite pitcher injury-risk index from leakage-safe trailing trends. |
| [mlb_pbp_diff](additional/mlb.md#mlb_pbp_diff) | GET /api/v1/game/{gamePk}/feed/live/diffPatch — JSON-patch diff of the live feed. |
| [mlb_pbp_live](additional/mlb.md#mlb_pbp_live) | GET /api/v1.1/game/{gamePk}/feed/live — live firehose (v1.1). |
| [mlb_person_stats](additional/mlb.md#mlb_person_stats) | GET /api/v1/people/{personId}/stats — player aggregate stats. |
| [mlb_pitch_classify](additional/mlb.md#mlb_pitch_classify) | Per-pitcher Gaussian-mixture pitch reclassification. |
| [mlb_pitch_era](additional/mlb.md#mlb_pitch_era) | Combined xERA + SIERA-like estimator (model ③). |
| [mlb_pitch_tunneling](additional/mlb.md#mlb_pitch_tunneling) | Per-pitch release/plate distance from the previous pitch + tunnel ratio. |
| [mlb_prop_strikeouts](additional/mlb.md#mlb_prop_strikeouts) | Expected pitcher/team strikeouts via a K/9-and-opponent-K-rate blend. |
| [mlb_prop_team_runs](additional/mlb.md#mlb_prop_team_runs) | Expected team runs via a log5-style rate blend. |
| [mlb_props](additional/mlb.md#mlb_props) | Expected team runs + strikeouts for a slate of matchups. |
| [mlb_pythagenpat](additional/mlb.md#mlb_pythagenpat) | Pythagenpat expected win percentage (Smyth-Patriot, run-environment adaptive exponent). |
| [mlb_pythagenpat_table](additional/mlb.md#mlb_pythagenpat_table) | Per-(season, team) pythagenpat table from game-level results. |
| [mlb_run_expectancy_matrix](additional/mlb.md#mlb_run_expectancy_matrix) | Empirical RE24 run-expectancy matrix by base-out state. |
| [mlb_schedule](additional/mlb.md#mlb_schedule) | GET /api/v1/schedule — schedule of games for a date, range, team, or season. |
| [mlb_seasons](additional/mlb.md#mlb_seasons) | GET /api/v1/seasons — list of seasons for a sport. |
| [mlb_sequence_run_value](additional/mlb.md#mlb_sequence_run_value) | Mean run value grouped by the ordered `(prev_pitch_type, pitch_type)` sequence. |
| [mlb_standings](additional/mlb.md#mlb_standings) | GET /api/v1/standings — league standings. |
| [mlb_statcast_player](additional/mlb.md#mlb_statcast_player) | GET /savant-player/{player_id} and parse one embedded table into a tidy frame. |
| [mlb_statcast_search](additional/mlb-2.md#mlb_statcast_search) | Pitch-by-pitch MLB Statcast search (`/statcast_search/csv`), date-chunked. |
| [mlb_statcast_search_minors](additional/mlb-2.md#mlb_statcast_search_minors) | Minor-league Statcast search (`/statcast-search-minors/csv`), date-chunked. |
| [mlb_statcast_search_wbc](additional/mlb-2.md#mlb_statcast_search_wbc) | World Baseball Classic Statcast search (`/statcast-search-world-baseball-classic/csv`). |
| [mlb_stats](additional/mlb-2.md#mlb_stats) | GET /api/v1/stats — generic stats query. |
| [mlb_stats_leaders](additional/mlb-2.md#mlb_stats_leaders) | GET /api/v1/stats/leaders — top-N leaders for a stat category. |
| [mlb_stats_streaks](additional/mlb-2.md#mlb_stats_streaks) | GET /api/v1/stats/streaks — active or historical streaks. |
| [mlb_stolen_base_value](additional/mlb-2.md#mlb_stolen_base_value) | Per-runner stolen-base run value: realized-vs-expected run contribution. |
| [mlb_stuff_plus](additional/mlb-2.md#mlb_stuff_plus) | Score pitches with the bundled Stuff+ (①) run-value model. |
| [mlb_swing_decision](additional/mlb-2.md#mlb_swing_decision) | Per player-season swing/take run value + selective-aggression (SEAGER analog). |
| [mlb_team_elo](additional/mlb-2.md#mlb_team_elo) | As-of-date iterative Elo run-differential rating. |
| [mlb_team_leaders](additional/mlb-2.md#mlb_team_leaders) | GET /api/v1/teams/{teamId}/leaders — team leaders. |
| [mlb_team_projection](additional/mlb-2.md#mlb_team_projection) | Combined pythagenpat + Elo team projection. |
| [mlb_team_stats](additional/mlb-2.md#mlb_team_stats) | GET /api/v1/teams/{teamId}/stats — team-level stats. |
| [mlb_teams](additional/mlb-2.md#mlb_teams) | GET /api/v1/teams — list teams. `sport_id=1` = MLB. |
| [mlb_times_through_order](additional/mlb-2.md#mlb_times_through_order) | Per-pitch fitted times-through-order fatigue adjustment. |
| [mlb_umpire_bias](additional/mlb-2.md#mlb_umpire_bias) | Per-umpire called-strike bias residual (observed minus expected). |
| [mlb_umpire_called_strike_prob](additional/mlb-2.md#mlb_umpire_called_strike_prob) | P(called strike) per pitch from the zone logistic. |
| [mlb_win_expectancy](additional/mlb-2.md#mlb_win_expectancy) | Per-play home win expectancy from the empirical state table. |
| [mlb_win_probability_added](additional/mlb-2.md#mlb_win_probability_added) | Per-play win-probability-added from a `mlb_win_expectancy` frame. |

## Play-by-play, schedule & rosters

| Function | Summary |
|---|---|
| [espn_mlb_game_rosters](additional/play-by-play-schedule-rosters.md#espn_mlb_game_rosters) | espn_mlb_game_rosters - pull the active game rosters for both teams. |
| [espn_mlb_pbp](additional/play-by-play-schedule-rosters.md#espn_mlb_pbp) | espn_mlb_pbp - pull the full ESPN game-summary payload for one MLB game. |
| [espn_mlb_player_stats](additional/play-by-play-schedule-rosters.md#espn_mlb_player_stats) | Pull an MLB athlete's ESPN **season** stat line as one wide row. |
| [espn_mlb_schedule](additional/play-by-play-schedule-rosters.md#espn_mlb_schedule) | espn_mlb_schedule - look up the MLB schedule for a given date or season-year. |

## Other

| Function | Summary |
|---|---|
| [most_recent_mlb_season](additional/other.md#most_recent_mlb_season) | most_recent_mlb_season - return the most recent / current MLB season year. |
| [add_sequence_features](additional/other.md#add_sequence_features) | Add within-game sequence, times-through-order, and workload features. |
| [advancement_opportunities](additional/other.md#advancement_opportunities) | Extract first-to-third / second-to-home / tag-up opportunities and outcomes. |
| [as_of_split](additional/other.md#as_of_split) | Leakage boundary: rows strictly before `cutoff_date` only. |
| [bip_trajectory_features](additional/other.md#bip_trajectory_features) | Add spray angle / hit distance / launch-angle bin / out label / position. |
| [build_we_table](additional/other.md#build_we_table) | Empirical, Laplace-smoothed home win-expectancy table. |
| [called_strike_prob_grid](additional/other.md#called_strike_prob_grid) | Empirical called-strike-probability grid over `(stand, plate_x, pz_norm)`. |
| [catch_prob_surface](additional/other.md#catch_prob_surface) | Empirical catch-probability surface over `(position, distance, spray, launch angle)`. |
| [count_strike_run_value](additional/other.md#count_strike_run_value) | Ball-to-strike run-expectancy delta per count, from `delta_run_exp`. |
| [espn_mlb_teams](additional/other.md#espn_mlb_teams) | espn_mlb_teams - look up MLB teams from ESPN's Site v2 API. |
| [event_run_value](additional/other.md#event_run_value) | Empirical run value of an event set, from mean `delta_run_exp`. |
| [fit_zone_model](additional/other.md#fit_zone_model) | Fit a logistic P(called strike \| zone coordinates) on called pitches. |
| [mae](additional/other.md#mae) | Mean absolute error between two arrays. |
| [pbp_base_out_states](additional/other.md#pbp_base_out_states) | Reconstruct pre-play base-out state from statsapi play-by-play. |
| [pearson_corr](additional/other.md#pearson_corr) | Pearson correlation coefficient between two 1-D arrays. |
| [pitch_features](additional/other.md#pitch_features) | Build the per-pitch feature substrate every pitching model consumes. |
| [pitcher_appearance_trends](additional/other.md#pitcher_appearance_trends) | Leakage-safe per-appearance trailing velocity/workload trends. |
| [predict_sb_success](additional/other.md#predict_sb_success) | As-of-date predictive P(success): the surface is fit on history strictly before `cutoff_date`. |
| [prop_over_prob](additional/other.md#prop_over_prob) | P(realized count > line) under a Poisson(expected) model. |
| [sb_attempts_from_pitches](additional/other.md#sb_attempts_from_pitches) | Extract stolen-base / caught-stealing attempts from pitch-level Statcast rows. |
| [sb_success_surface](additional/other.md#sb_success_surface) | Empirical P(stolen-base success) surface over `(sprint speed, pop time, base)`. |
| [siera_like](additional/other.md#siera_like) | SIERA-like ERA estimator from K%/BB%/GB% (**experimental / provisional**). |
| [spearman_corr](additional/other.md#spearman_corr) | Spearman rank correlation between two arrays. |
| [tto_penalty_table](additional/other.md#tto_penalty_table) | Observed mean run value by times-through-order, with the penalty vs TTO=1. |
