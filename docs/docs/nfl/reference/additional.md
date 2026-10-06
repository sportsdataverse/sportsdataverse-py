---
title: NFL — additional Python functions
sidebar_label: Additional functions
description: "NFL — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# NFL — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nfl`
not covered by the generated API-endpoint reference above.

## Build

| Function | Summary |
|---|---|
| [build_nfl_player_stats](additional/build.md#build_nfl_player_stats) | Build nflverse **player_stats** by aggregating SDV-native play-by-play. |
| [build_nfl_player_stats_def](additional/build.md#build_nfl_player_stats_def) | Build player-level defensive stats from play-by-play (nflfastR parity). |
| [build_nfl_player_stats_kicking](additional/build.md#build_nfl_player_stats_kicking) | Build player-level kicking stats from play-by-play (nflfastR parity). |
| [build_nfl_players](additional/build.md#build_nfl_players) | Build an SDV-native NFL players frame from ESPN's public athletes endpoint. |
| [build_nfl_rosters](additional/build.md#build_nfl_rosters) | Build SDV-native NFL season rosters from the public Shield API. |
| [build_nfl_season](additional/build.md#build_nfl_season) | Compile play-by-play for multiple NFL games into one tidy frame. |
| [build_nfl_team_stats](additional/build.md#build_nfl_team_stats) | Build nflverse **team_stats** by aggregating SDV-native play-by-play. |

## Calculate

| Function | Summary |
|---|---|
| [calculate_completion_probability](additional/calculate.md#calculate_completion_probability) | Compute completion probability (CP) and CPOE for pass plays. |
| [calculate_epa](additional/calculate.md#calculate_epa) | Derive expected points added (EPA) from pre-scored EP point estimates. |
| [calculate_expected_points](additional/calculate.md#calculate_expected_points) | Compute expected points for provided plays. |
| [calculate_nfl_series_conversion_rates](additional/calculate.md#calculate_nfl_series_conversion_rates) | Compute per-team offense + defense series conversion rates. |
| [calculate_nfl_standings](additional/calculate.md#calculate_nfl_standings) | Compute NFL division standings + conference playoff seeds. |
| [calculate_win_probability](additional/calculate.md#calculate_win_probability) | Compute win probability for provided plays. |
| [calculate_wpa](additional/calculate.md#calculate_wpa) | Derive win probability added (WPA) from pre-scored WP point estimates. |
| [calculate_xpass](additional/calculate.md#calculate_xpass) | Compute expected dropback probability (`xpass`) and `pass_oe`. |
| [calculate_xyac](additional/calculate.md#calculate_xyac) | Compute expected yards after catch (xYAC) for intended pass plays. |

## Dataset loaders

| Function | Summary |
|---|---|
| [load_combine](additional/dataset-loaders.md#load_combine) | Load NFL Combine information |
| [load_contracts](additional/dataset-loaders.md#load_contracts) | Load NFL Historical contracts information |
| [load_depth_charts](additional/dataset-loaders.md#load_depth_charts) | Load NFL Depth Chart data for selected seasons |
| [load_draft_picks](additional/dataset-loaders.md#load_draft_picks) | Load NFL Draft picks information |
| [load_espn_qbr](additional/dataset-loaders.md#load_espn_qbr) | Load ESPN Total QBR (Quarterback Rating) data going back to 2006. |
| [load_ff_opportunity](additional/dataset-loaders.md#load_ff_opportunity) | Load NFL fantasy football opportunity data from ffverse/ffopportunity |
| [load_ff_playerids](additional/dataset-loaders.md#load_ff_playerids) | Load fantasy football player IDs from DynastyProcess.com |
| [load_ff_rankings](additional/dataset-loaders.md#load_ff_rankings) | Load fantasy football rankings and projections |
| [load_ftn_charting](additional/dataset-loaders.md#load_ftn_charting) | Load NFL FTN charting data going back to 2022 |
| [load_injuries](additional/dataset-loaders.md#load_injuries) | Load NFL injuries data for selected seasons |
| [load_nextgen_stats](additional/dataset-loaders.md#load_nextgen_stats) | Load NFL NextGen Stats data going back to 2016. |
| [load_nfl_combine](additional/dataset-loaders.md#load_nfl_combine) | Load NFL Combine information |
| [load_nfl_contracts](additional/dataset-loaders.md#load_nfl_contracts) | Load NFL Historical contracts information |
| [load_nfl_draft_picks](additional/dataset-loaders.md#load_nfl_draft_picks) | Load NFL Draft picks information |
| [load_nfl_espn_qbr](additional/dataset-loaders.md#load_nfl_espn_qbr) | Load ESPN Total QBR (Quarterback Rating) data going back to 2006. |
| [load_nfl_ff_opportunity](additional/dataset-loaders-2.md#load_nfl_ff_opportunity) | Load NFL fantasy football opportunity data from ffverse/ffopportunity |
| [load_nfl_ff_playerids](additional/dataset-loaders-2.md#load_nfl_ff_playerids) | Load fantasy football player IDs from DynastyProcess.com |
| [load_nfl_ff_rankings](additional/dataset-loaders-2.md#load_nfl_ff_rankings) | Load fantasy football rankings and projections |
| [load_nfl_fp_curve](additional/dataset-loaders-2.md#load_nfl_fp_curve) | Load the bundled NFL EP-by-yardline curve (no network). |
| [load_nfl_nextgen_stats](additional/dataset-loaders-2.md#load_nfl_nextgen_stats) | Load NFL NextGen Stats data going back to 2016. |
| [load_nfl_ngs_passing](additional/dataset-loaders-2.md#load_nfl_ngs_passing) | Deprecated alias for `load_nfl_nextgen_stats(stat_type='passing')`. |
| [load_nfl_ngs_receiving](additional/dataset-loaders-2.md#load_nfl_ngs_receiving) | Deprecated alias for `load_nfl_nextgen_stats(stat_type='receiving')`. |
| [load_nfl_ngs_rushing](additional/dataset-loaders-2.md#load_nfl_ngs_rushing) | Deprecated alias for `load_nfl_nextgen_stats(stat_type='rushing')`. |
| [load_nfl_officials](additional/dataset-loaders-2.md#load_nfl_officials) | Load NFL Officials information |
| [load_nfl_pfr_advstats](additional/dataset-loaders-2.md#load_nfl_pfr_advstats) | Load Pro-Football Reference advanced statistics going back to 2018. |
| [load_nfl_pfr_def](additional/dataset-loaders-2.md#load_nfl_pfr_def) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='season')`. |
| [load_nfl_pfr_pass](additional/dataset-loaders-2.md#load_nfl_pfr_pass) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='season')`. |
| [load_nfl_pfr_rec](additional/dataset-loaders-2.md#load_nfl_pfr_rec) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='season')`. |
| [load_nfl_pfr_rush](additional/dataset-loaders-2.md#load_nfl_pfr_rush) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='season')`. |
| [load_nfl_pfr_weekly_def](additional/dataset-loaders-2.md#load_nfl_pfr_weekly_def) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='def', summary_level='week')`. |
| [load_nfl_pfr_weekly_pass](additional/dataset-loaders-3.md#load_nfl_pfr_weekly_pass) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='pass', summary_level='week')`. |
| [load_nfl_pfr_weekly_rec](additional/dataset-loaders-3.md#load_nfl_pfr_weekly_rec) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='rec', summary_level='week')`. |
| [load_nfl_pfr_weekly_rush](additional/dataset-loaders-3.md#load_nfl_pfr_weekly_rush) | Deprecated alias for `load_nfl_pfr_advstats(stat_type='rush', summary_level='week')`. |
| [load_nfl_player_stats](additional/dataset-loaders-3.md#load_nfl_player_stats) | Load NFL player stats data |
| [load_nfl_players](additional/dataset-loaders-3.md#load_nfl_players) | Load the nflverse NFL player-identity master. |
| [load_nfl_schedule](additional/dataset-loaders-3.md#load_nfl_schedule) | Load NFL schedule data |
| [load_nfl_team_stats](additional/dataset-loaders-3.md#load_nfl_team_stats) | Load NFL team stats data going back to 1999 |
| [load_nfl_teams](additional/dataset-loaders-3.md#load_nfl_teams) | Load NFL team ID information and logos |
| [load_nfl_trades](additional/dataset-loaders-3.md#load_nfl_trades) | Load NFL trades data |
| [load_officials](additional/dataset-loaders-3.md#load_officials) | Load NFL Officials information |
| [load_participation](additional/dataset-loaders-3.md#load_participation) | Load NFL play-by-play participation data for selected seasons |
| [load_pfr_advstats](additional/dataset-loaders-3.md#load_pfr_advstats) | Load Pro-Football Reference advanced statistics going back to 2018. |
| [load_player_stats](additional/dataset-loaders-3.md#load_player_stats) | Load NFL player stats data |
| [load_players](additional/dataset-loaders-4.md#load_players) | Load the nflverse NFL player-identity master. |
| [load_rosters_weekly](additional/dataset-loaders-4.md#load_rosters_weekly) | Load NFL weekly roster data for the requested seasons. |
| [load_schedules](additional/dataset-loaders-4.md#load_schedules) | Load NFL schedule data |
| [load_snap_counts](additional/dataset-loaders-4.md#load_snap_counts) | Load NFL snap counts data for selected seasons |
| [load_team_stats](additional/dataset-loaders-4.md#load_team_stats) | Load NFL team stats data going back to 1999 |
| [load_teams](additional/dataset-loaders-4.md#load_teams) | Load NFL team ID information and logos |
| [load_trades](additional/dataset-loaders-4.md#load_trades) | Load NFL trades data |

## Fox

| Function | Summary |
|---|---|
| [fox_nfl_boxscore](additional/fox.md#fox_nfl_boxscore) | NFL boxscore (long: one row per player-stat). |
| [fox_nfl_event_matchup](additional/fox.md#fox_nfl_event_matchup) | Fox Sports nfl pregame team-stat comparison (one row per stat). |
| [fox_nfl_event_recap](additional/fox.md#fox_nfl_event_recap) | Fox Sports nfl postgame top performers (one row per player). |
| [fox_nfl_event_standings](additional/fox.md#fox_nfl_event_standings) | Fox Sports nfl the two teams' standings context. |
| [fox_nfl_league_conferences](additional/fox.md#fox_nfl_league_conferences) | Fox Sports nfl conference / group directory. |
| [fox_nfl_league_header](additional/fox.md#fox_nfl_league_header) | Fox Sports nfl league header (one row). |
| [fox_nfl_league_leaders](additional/fox.md#fox_nfl_league_leaders) | NFL statistical leaders (`stats-con`); who=player\|team. |
| [fox_nfl_league_odds](additional/fox.md#fox_nfl_league_odds) | Fox Sports nfl league odds board (one row per team per game). |
| [fox_nfl_league_player_news](additional/fox.md#fox_nfl_league_player_news) | Fox Sports nfl league-wide player news feed. |
| [fox_nfl_league_polls](additional/fox.md#fox_nfl_league_polls) | Fox Sports nfl rankings / polls rendered as standings tables. |
| [fox_nfl_league_schedule](additional/fox.md#fox_nfl_league_schedule) | Fox Sports nfl league schedule nav selections. |
| [fox_nfl_league_scores](additional/fox.md#fox_nfl_league_scores) | Fox Sports nfl league scores nav selections. |
| [fox_nfl_league_standings](additional/fox.md#fox_nfl_league_standings) | Fox Sports nfl league-wide standings tables. |
| [fox_nfl_league_stat_leaders](additional/fox.md#fox_nfl_league_stat_leaders) | Fox Sports nfl league stats landing leaders. |
| [fox_nfl_odds](additional/fox.md#fox_nfl_odds) | NFL game odds six-pack (spread / to-win / total per team). |
| [fox_nfl_pbp](additional/fox.md#fox_nfl_pbp) | NFL play-by-play (one row per play; drive-based). |
| [fox_nfl_scoreboard](additional/fox.md#fox_nfl_scoreboard) | Fox Sports nfl scoreboard nav selections (weeks / dates / groups). |
| [fox_nfl_scorechip](additional/fox.md#fox_nfl_scorechip) | Fox Sports nfl compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_nfl_scores_segment](additional/fox.md#fox_nfl_scores_segment) | Fox Sports nfl one row per game in a scoreboard segment. |
| [fox_nfl_standings](additional/fox.md#fox_nfl_standings) | NFL standings for a team's conference/division. |
| [fox_nfl_team_gamelog](additional/fox.md#fox_nfl_team_gamelog) | NFL team game log (long: one row per game-stat). |
| [fox_nfl_team_header](additional/fox.md#fox_nfl_team_header) | Fox Sports nfl team header (one row). |
| [fox_nfl_team_roster](additional/fox.md#fox_nfl_team_roster) | NFL team roster (one row per player). |
| [fox_nfl_team_stats](additional/fox.md#fox_nfl_team_stats) | NFL team stat leaders by category. |
| [fox_nfl_teamnav](additional/fox.md#fox_nfl_teamnav) | Fox Sports nfl team directory (one row per team). |

## Nfl

| Function | Summary |
|---|---|
| [nfl_availability_projection](additional/nfl.md#nfl_availability_projection) | Empirical-Bayes availability projection: expected fraction of team games. |
| [nfl_clear_token_cache](additional/nfl.md#nfl_clear_token_cache) | Drop the cached `api.nfl.com` token (forces a fresh mint on the next call). |
| [nfl_compute_results](additional/nfl.md#nfl_compute_results) | Compute NFL game results for one week of a season simulation. |
| [nfl_draft_projection](additional/nfl.md#nfl_draft_projection) | Draft outcome projection for one draft class. |
| [nfl_fantasy_projection](additional/nfl.md#nfl_fantasy_projection) | Fantasy-points projection: deterministic scoring of the Marcel component |
| [nfl_game_details](additional/nfl.md#nfl_game_details) | Pull full `api.nfl.com` game details (drives + plays) by game id. |
| [nfl_game_pbp](additional/nfl.md#nfl_game_pbp) | Parsed `api.nfl.com` play-by-play -- one row per play (polars/pandas frame). |
| [nfl_game_schedule](additional/nfl.md#nfl_game_schedule) | List `api.nfl.com` games for a season/week slice (`/football/v2/games`). |
| [nfl_game_script](additional/nfl.md#nfl_game_script) | Team-season pace / PROE / expected-plays engine. |
| [nfl_headers_gen](additional/nfl.md#nfl_headers_gen) | Build the request-header dict expected by `api.nfl.com`. |
| [nfl_kicker_rating](additional/nfl.md#nfl_kicker_rating) | Environment-adjusted kicker FG-over-expected ratings. |
| [nfl_line_grades](additional/nfl.md#nfl_line_grades) | Team-season OL pass-block + DL pass-rush grades (opponent-adjusted, EB-shrunk). |
| [nfl_ngs_gamecenter_overview](additional/nfl.md#nfl_ngs_gamecenter_overview) | NGS gamecenter overview for one game -- one row per player on a side. |
| [nfl_ngs_leaders](additional/nfl.md#nfl_ngs_leaders) | NGS top-N "leaders" board for a single category (one row per leader play). |
| [nfl_ngs_league_schedule](additional/nfl.md#nfl_ngs_league_schedule) | NGS league schedule -- one row per game; source of NGS `gameId` values. |
| [nfl_ngs_league_schedule_current](additional/nfl.md#nfl_ngs_league_schedule_current) | NGS schedule for the *current* week -- one row per game. |
| [nfl_ngs_league_teams](additional/nfl.md#nfl_ngs_league_teams) | NGS team directory -- one row per team. |
| [nfl_ngs_man_zone_rates](additional/nfl.md#nfl_ngs_man_zone_rates) | Descriptive man/zone coverage rates from NGS-charted labels — NOT a trained classifier. |
| [nfl_ngs_microsite_chart](additional/nfl.md#nfl_ngs_microsite_chart) | NGS microsite chart catalogue -- one row per rendered player chart image. |
| [nfl_ngs_microsite_chart_players](additional/nfl.md#nfl_ngs_microsite_chart_players) | NGS microsite chart player index -- one row per player with a chart. |
| [nfl_ngs_play_is_highlight](additional/nfl-2.md#nfl_ngs_play_is_highlight) | Look up whether a single play is an NGS highlight -- one-row frame. |
| [nfl_ngs_ryoe](additional/nfl-2.md#nfl_ngs_ryoe) | Rush yards over expected per rusher-season, stabilised with EB shrinkage. |
| [nfl_ngs_separation_oe](additional/nfl-2.md#nfl_ngs_separation_oe) | Separation over a built context expectation, per receiver-season. |
| [nfl_ngs_statboard](additional/nfl-2.md#nfl_ngs_statboard) | NGS season/week statboard leaderboard for a stat family (one row per player). |
| [nfl_ngs_statboard_leaders](additional/nfl-2.md#nfl_ngs_statboard_leaders) | NGS cross-stat "leaders" board, stacked long with a `category` column. |
| [nfl_ngs_yac_oe](additional/nfl-2.md#nfl_ngs_yac_oe) | YAC over expected per receiver-season, stabilised with EB shrinkage. |
| [nfl_play_call_probabilities](additional/nfl-2.md#nfl_play_call_probabilities) | Score the bundled play-call classifier over offensive plays. |
| [nfl_play_call_tendencies](additional/nfl-2.md#nfl_play_call_tendencies) | Aggregate scored play-call probabilities to team-season tendencies. |
| [nfl_player_projection](additional/nfl-2.md#nfl_player_projection) | Marcel-style next-season player projection with delta-method aging. |
| [nfl_player_props](additional/nfl-2.md#nfl_player_props) | Empirical-Bayes player-prop projections, leakage-safe per week. |
| [nfl_players_crosswalk](additional/nfl-2.md#nfl_players_crosswalk) | Pure-consumer ID crosswalk sliced from `load_nfl_players`. |
| [nfl_predict_games](additional/nfl-2.md#nfl_predict_games) | Vectorized pregame predictions (+ display-only market edge) per game. |
| [nfl_punter_value](additional/nfl-2.md#nfl_punter_value) | Punter net-field-position value over expected. |
| [nfl_ratings](additional/nfl-2.md#nfl_ratings) | One row per team: the native NFL ratings spine (off/def/ST EPA). |
| [nfl_season_standings](additional/nfl-2.md#nfl_season_standings) | Compute NFL standings with the real NFL tiebreaking procedures. |
| [nfl_simulations](additional/nfl-3.md#nfl_simulations) | Simulate an NFL season from a schedule with (partially) missing results. |
| [nfl_special_teams_epa](additional/nfl-3.md#nfl_special_teams_epa) | Special-teams EPA by team-unit. |
| [nfl_token_gen](additional/nfl-3.md#nfl_token_gen) | Return a valid `api.nfl.com` bearer token, minting + caching as needed. |
| [nfl_usage_projection](additional/nfl-3.md#nfl_usage_projection) | Project next-season target share, air-yards share, and WOPR. |
| [nfl_week_games](additional/nfl-3.md#nfl_week_games) | Parsed `api.nfl.com` week schedule -- one row per game (polars/pandas frame). |

## Play-by-play, schedule & rosters

| Function | Summary |
|---|---|
| [espn_nfl_game_rosters](additional/play-by-play-schedule-rosters.md#espn_nfl_game_rosters) | espn_nfl_game_rosters() - Pull the game by id. |
| [espn_nfl_play_participants](additional/play-by-play-schedule-rosters.md#espn_nfl_play_participants) | Pull ESPN per-play participants for an NFL game. |
| [espn_nfl_player_stats](additional/play-by-play-schedule-rosters.md#espn_nfl_player_stats) | Pull an NFL athlete's ESPN **season** stat line as one wide row. |
| [espn_nfl_schedule](additional/play-by-play-schedule-rosters.md#espn_nfl_schedule) | espn_nfl_schedule - look up the NFL schedule for a given season |

## Scrape

| Function | Summary |
|---|---|
| [scrape_ngs_season](additional/scrape.md#scrape_ngs_season) | Scrape a full season of NGS statboard data, shaped like the nflverse parquet. |
| [scrape_ngs_week](additional/scrape.md#scrape_ngs_week) | Scrape one (season, week) NGS statboard slice, shaped like the nflverse parquet. |

## Utilities & helpers

| Function | Summary |
|---|---|
| [NFLPlayProcess](additional/utilities-helpers.md#NFLPlayProcess) | Process ESPN NFL play-by-play feeds into a tidy game-level dictionary. |
| [get_current_nfl_season](additional/utilities-helpers.md#get_current_nfl_season) | Return the current NFL season year. |
| [get_current_nfl_week](additional/utilities-helpers.md#get_current_nfl_week) | Return the current NFL week (1-22). |
| [get_current_season](additional/utilities-helpers.md#get_current_season) | Return the current NFL season year. |
| [get_current_week](additional/utilities-helpers.md#get_current_week) | Return the current NFL week (1-22). |
| [most_recent_nfl_season](additional/utilities-helpers.md#most_recent_nfl_season) | Alias for `get_current_nfl_season()` mirroring nflreadr's |

## Other

| Function | Summary |
|---|---|
| [NflConfig](additional/other.md#NflConfig) | Runtime configuration for sdv-py NFL loaders. |
| [adjust_pressure_pairs](additional/other.md#adjust_pressure_pairs) | Opponent-adjust matchup pressure rates via an additive fixed point. |
| [cached_loader](additional/other.md#cached_loader) | Decorator that adds caching to a `load_nfl_*` function. |
| [clean_nfl_pbp](additional/other.md#clean_nfl_pbp) | Canonicalize names/ids/teams on a play-by-play frame (nflfastR `clean_pbp` port). |
| [clear_cache](additional/other.md#clear_cache) | Clear both memory and filesystem caches. |
| [compose_counting_projection](additional/other.md#compose_counting_projection) | Compose skill and availability into a counting projection. |
| [efficiency_ratings](additional/other.md#efficiency_ratings) | One row per team: opponent-adjusted offense/defense EPA per play. |
| [env_adjusted_make_prob](additional/other.md#env_adjusted_make_prob) | Add `base_make_prob` + environment-adjusted `exp_make_prob`. |
| [espn_nfl_teams](additional/other.md#espn_nfl_teams) | espn_nfl_teams - look up NFL teams |
| [fg_make_probability](additional/other.md#fg_make_probability) | Predict FG make probability from the bundled `fg_model` (public wrapper). |
| [fit_nfl_field_position_ep](additional/other.md#fit_nfl_field_position_ep) | Fit the NFL EP-by-starting-yardline curve from released `espn_nfl_pbp` plays. |
| [get_2pt_probs](additional/other.md#get_2pt_probs) | The PAT-vs-2pt decision surface for post-touchdown states (CFB-shaped). |
| [get_2pt_wp](additional/other.md#get_2pt_wp) | Win probability of the PAT-vs-2pt choice after a touchdown (nfl4th `get_2pt_wp`). |
| [get_4th_down_probs](additional/other.md#get_4th_down_probs) | Full 4th-down decision surface (nfl4th `add_4th_probs`) + recommendation. |
| [get_config](additional/other.md#get_config) | Return the live `NflConfig` singleton. |
| [get_fg_wp](additional/other.md#get_fg_wp) | Expected win probability of attempting a field goal (nfl4th `get_fg_wp`). |
| [get_go_wp](additional/other.md#get_go_wp) | Expected win probability of going for it on 4th down (nfl4th `get_go_wp`). |
| [get_punt_wp](additional/other.md#get_punt_wp) | Expected win probability of punting on 4th down (nfl4th `get_punt_wp`). |
| [opponent_adjusted_ridge](additional/other.md#opponent_adjusted_ridge) | Ridge-regress `resp_col` on offense + defense team indicators + HFA. |
| [playcall_features](additional/other.md#playcall_features) | Build the play-call feature frame (one row per offensive run/pass play). |
| [player_usage_efficiency](additional/other.md#player_usage_efficiency) | Per-player as-of usage + efficiency with empirical-Bayes shrinkage. |
| [predict_margin](additional/other.md#predict_margin) | Expected home scoring margin from two net ratings. |
| [predict_total](additional/other.md#predict_total) | Expected combined point total from the four efficiency components. |
| [pressure_pairs](additional/other.md#pressure_pairs) | Per (season, off_team, def_team) dropbacks + pressures (matchup grid). |
| [reset_config](additional/other.md#reset_config) | Reset the active config to its env-var-derived defaults. |
| [scoreboard_event_parsing](additional/other.md#scoreboard_event_parsing) | Normalize one ESPN scoreboard `event` into a flatter shape. |
| [shield_nfl_pbp](additional/other.md#shield_nfl_pbp) | Build one NFL game's nflverse-shape play-by-play from Shield, at ANY game phase. |
| [shield_to_espn_summary](additional/other.md#shield_to_espn_summary) | Project one Shield game (any phase) onto an ESPN-summary-shaped dict. |
| [special_teams_ratings](additional/other.md#special_teams_ratings) | One row per team: opponent-adjusted special-teams EPA per play. |
| [team_game_pace](additional/other.md#team_game_pace) | Per team-game pace + pass-rate-over-expected. |
| [team_name_fn](additional/other.md#team_name_fn) | Fold historical/relocated team codes onto their current abbreviation. |
| [team_pressure_rates](additional/other.md#team_pressure_rates) | Per (season, team) raw pressure rates, both sides of the ball. |
| [update_config](additional/other.md#update_config) | Update the active config in place. |
| [win_prob_from_margin](additional/other.md#win_prob_from_margin) | Home win probability from an expected margin (Gaussian margin model). |
