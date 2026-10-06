---
title: CFB — additional Python functions
sidebar_label: Additional functions
description: "CFB — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# CFB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.cfb`
not covered by the generated API-endpoint reference above.

## Highlights

| Function | Summary |
|---|---|
| [CFBPlayProcess](additional/highlights.md#CFBPlayProcess) | Process ESPN college-football play-by-play feeds into a tidy game-level dictionary. |
| [cfb_advanced_stats](additional/highlights.md#cfb_advanced_stats) | Team-season CFB advanced stats: efficiency, explosiveness, havoc. |
| [cfb_standings](additional/highlights.md#cfb_standings) | Compute college football standings with conference ranks and champions. |
| [espn_cfb_player_stats](additional/highlights.md#espn_cfb_player_stats) | Pull a college-football athlete's ESPN **season** stat line. |
| [espn_cfb_schedule](additional/highlights.md#espn_cfb_schedule) | espn_cfb_schedule - look up the college football schedule for a given season |
| [get_cfb_teams](additional/highlights.md#get_cfb_teams) | Load college football team ID information and logos |
| [most_recent_cfb_season](additional/highlights.md#most_recent_cfb_season) | Return the most recent college football season year based on today's date. |
| [to_cfbfastr](additional/highlights.md#to_cfbfastr) | cfbfastR-named play frame from the NCAA structural pbp frame. |

## Yahoo Sports Shangrila

| Function | Summary |
|---|---|
| [yahoo_cfb_boxscore](additional/yahoo-sports-shangrila.md#yahoo_cfb_boxscore) | Yahoo CFB box score: team and player stats, one row per entity stat. |
| [yahoo_cfb_player_season_stats](additional/yahoo-sports-shangrila.md#yahoo_cfb_player_season_stats) | Yahoo CFB player season stats (modern; one wide row per player). |
| [yahoo_cfb_player_season_stats_legacy](additional/yahoo-sports-shangrila.md#yahoo_cfb_player_season_stats_legacy) | Yahoo CFB legacy per-category player leaders (one wide row per player). |
| [yahoo_cfb_scoreboard](additional/yahoo-sports-shangrila.md#yahoo_cfb_scoreboard) | Yahoo CFB scoreboard (one row per game). |
| [yahoo_cfb_team_season_stats](additional/yahoo-sports-shangrila.md#yahoo_cfb_team_season_stats) | Yahoo CFB team season stats (modern; one wide row per team). |
| [yahoo_cfb_team_season_stats_legacy](additional/yahoo-sports-shangrila.md#yahoo_cfb_team_season_stats_legacy) | Yahoo CFB legacy per-category team stats (one wide row per team). |
| [yahoo_cfb_teams](additional/yahoo-sports-shangrila.md#yahoo_cfb_teams) | Yahoo CFB team directory (one row per team). |

## Fox Sports API

| Function | Summary |
|---|---|
| [fox_cfb_boxscore](additional/fox-sports-api.md#fox_cfb_boxscore) | Fox Sports CFB boxscore (long: one row per player-stat). |
| [fox_cfb_event_matchup](additional/fox-sports-api.md#fox_cfb_event_matchup) | Fox Sports cfb pregame team-stat comparison (one row per stat). |
| [fox_cfb_event_recap](additional/fox-sports-api.md#fox_cfb_event_recap) | Fox Sports cfb postgame top performers (one row per player). |
| [fox_cfb_event_standings](additional/fox-sports-api.md#fox_cfb_event_standings) | Fox Sports cfb the two teams' standings context. |
| [fox_cfb_league_conferences](additional/fox-sports-api.md#fox_cfb_league_conferences) | Fox Sports cfb conference / group directory. |
| [fox_cfb_league_header](additional/fox-sports-api.md#fox_cfb_league_header) | Fox Sports cfb league header (one row). |
| [fox_cfb_league_leaders](additional/fox-sports-api.md#fox_cfb_league_leaders) | Fox Sports CFB statistical leaders (one row per player/team). |
| [fox_cfb_league_odds](additional/fox-sports-api.md#fox_cfb_league_odds) | Fox Sports cfb league odds board (one row per team per game). |
| [fox_cfb_league_player_news](additional/fox-sports-api.md#fox_cfb_league_player_news) | Fox Sports cfb league-wide player news feed. |
| [fox_cfb_league_polls](additional/fox-sports-api.md#fox_cfb_league_polls) | Fox Sports cfb rankings / polls rendered as standings tables. |
| [fox_cfb_league_schedule](additional/fox-sports-api.md#fox_cfb_league_schedule) | Fox Sports cfb league schedule nav selections. |
| [fox_cfb_league_scores](additional/fox-sports-api.md#fox_cfb_league_scores) | Fox Sports cfb league scores nav selections. |
| [fox_cfb_league_standings](additional/fox-sports-api.md#fox_cfb_league_standings) | Fox Sports cfb league-wide standings tables. |
| [fox_cfb_league_stat_leaders](additional/fox-sports-api.md#fox_cfb_league_stat_leaders) | Fox Sports cfb league stats landing leaders. |
| [fox_cfb_odds](additional/fox-sports-api.md#fox_cfb_odds) | Fox Sports CFB game odds six-pack (spread / to win / total per team). |
| [fox_cfb_pbp](additional/fox-sports-api.md#fox_cfb_pbp) | Fox Sports CFB play-by-play (one row per play). |
| [fox_cfb_play_process](additional/fox-sports-api.md#fox_cfb_play_process) | Build a *processed* CFB play-by-play game from FoxSports as a backup to ESPN. |
| [fox_cfb_schedule](additional/fox-sports-api.md#fox_cfb_schedule) | Fox Sports CFB full-season schedule (one row per game). |
| [fox_cfb_scoreboard](additional/fox-sports-api.md#fox_cfb_scoreboard) | Fox Sports cfb scoreboard nav selections (weeks / dates / groups). |
| [fox_cfb_scorechip](additional/fox-sports-api.md#fox_cfb_scorechip) | Fox Sports cfb compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_cfb_scores_segment](additional/fox-sports-api.md#fox_cfb_scores_segment) | Fox Sports cfb one row per game in a scoreboard segment. |
| [fox_cfb_standings](additional/fox-sports-api.md#fox_cfb_standings) | Fox Sports CFB conference standings for a team's conference. |
| [fox_cfb_team_gamelog](additional/fox-sports-api.md#fox_cfb_team_gamelog) | Fox Sports CFB team game log -- tidy long: one row per (game, stat). |
| [fox_cfb_team_header](additional/fox-sports-api.md#fox_cfb_team_header) | Fox Sports cfb team header (one row). |
| [fox_cfb_team_roster](additional/fox-sports-api.md#fox_cfb_team_roster) | Fox Sports CFB team roster (one row per player). |
| [fox_cfb_team_stats](additional/fox-sports-api.md#fox_cfb_team_stats) | Fox Sports CFB team stat leaders (one row per category leader). |
| [fox_cfb_teamnav](additional/fox-sports-api.md#fox_cfb_teamnav) | Fox Sports cfb team directory (one row per team). |
| [fox_cfb_teams](additional/fox-sports-api.md#fox_cfb_teams) | Fox Sports CFB team directory (one row per team). |
| [fox_to_espn_summary](additional/fox-sports-api.md#fox_to_espn_summary) | Adapt a Fox `cfb/event/{id}/data` payload into the ESPN-summary shape. |

## Models and calculators

| Function | Summary |
|---|---|
| [add_era_columns](additional/models-and-calculators.md#add_era_columns) | Add the era column(s) `model` consumes, using ITS card's cuts. |
| [assert_rating_scale](additional/models-and-calculators.md#assert_rating_scale) | Warn if the ratings have drifted off the scale the constants were fit on. |
| [calculate_completion_probability](additional/models-and-calculators.md#calculate_completion_probability) | Completion probability for each pass attempt. |
| [calculate_epa](additional/models-and-calculators.md#calculate_epa) | Expected points added: the change in EP across a play. |
| [calculate_expected_points](additional/models-and-calculators.md#calculate_expected_points) | Expected points for each row. |
| [calculate_field_goal_probability](additional/models-and-calculators.md#calculate_field_goal_probability) | Field-goal make probability for each row. |
| [calculate_fourth_down](additional/models-and-calculators.md#calculate_fourth_down) | Fourth-down conversion model output for each row. |
| [calculate_qbr](additional/models-and-calculators.md#calculate_qbr) | Model QBR for each row. |
| [calculate_two_point_probability](additional/models-and-calculators.md#calculate_two_point_probability) | Two-point conversion success probability. |
| [calculate_win_probability](additional/models-and-calculators.md#calculate_win_probability) | Win probability for each row. |
| [calculate_wpa](additional/models-and-calculators.md#calculate_wpa) | Win probability added: the change in WP across a play. |
| [calculate_xpass](additional/models-and-calculators.md#calculate_xpass) | Expected pass probability for each row. |
| [cfb_adjusted_epa](additional/models-and-calculators.md#cfb_adjusted_epa) | Season opponent-adjusted per-team EPA from a season's play-by-play. |
| [cfb_adjusted_epa_by_game](additional/models-and-calculators.md#cfb_adjusted_epa_by_game) | Walk-forward (point-in-time) opponent-adjusted EPA, one row per team-game. |
| [cfb_compute_results](additional/models-and-calculators.md#cfb_compute_results) | Default results generator — nflseedR's dynamic ELO model for CFB. |
| [cfb_draft_projection](additional/models-and-calculators.md#cfb_draft_projection) | Project NFL-draft probability per player + expected picks per team. |
| [cfb_field_position](additional/models-and-calculators.md#cfb_field_position) | Team-season field-position value: avg start, drive EP, margin, pts/drive. |
| [cfb_predict_games](additional/models-and-calculators.md#cfb_predict_games) | Predict a whole schedule of games from a ratings frame (vectorized). |
| [cfb_ratings](additional/models-and-calculators.md#cfb_ratings) | One row per team: the full CFB ratings spine (off/def/ST EPA + FEI). |
| [cfb_recruiting_projection](additional/models-and-calculators.md#cfb_recruiting_projection) | Project team wins / scoring margin for a season from preseason roster features. |
| [cfb_roster_talent](additional/models-and-calculators.md#cfb_roster_talent) | Team-talent composite per team-season (247 Team Talent Composite style). |
| [cfb_simulations](additional/models-and-calculators.md#cfb_simulations) | Simulate college football seasons (nflseedR-style week loop). |
| [efficiency_ratings](additional/models-and-calculators.md#efficiency_ratings) | One row per team: opponent-adjusted offensive/defensive efficiency. |
| [fei_ratings](additional/models-and-calculators.md#fei_ratings) | One row per team: opponent-adjusted per-drive efficiency (FEI-style). |
| [fit_field_position_ep](additional/models-and-calculators.md#fit_field_position_ep) | Fit the monotone EP-by-starting-yardline curve from a drives frame. |
| [get_2pt_probs](additional/models-and-calculators.md#get_2pt_probs) | Two-point-conversion decision surface (cfb4th `get_2pt_wp`). |
| [get_4th_down_probs](additional/models-and-calculators.md#get_4th_down_probs) | Full 4th-down decision surface (cfb4th `add_4th_probs`) + recommendation. |
| [get_fg_wp](additional/models-and-calculators.md#get_fg_wp) | Expected win probability of attempting a field goal (cfb4th `get_fg_wp`). |
| [get_go_wp](additional/models-and-calculators.md#get_go_wp) | Expected win probability of going for it on 4th down (cfb4th `get_go_wp`). |
| [get_punt_wp](additional/models-and-calculators.md#get_punt_wp) | Expected win probability of punting on 4th down (cfb4th `get_punt_wp`). |
| [load_draft_outcomes](additional/models-and-calculators.md#load_draft_outcomes) | NFL draft picks with the college of each pick, for the requested draft years. |
| [load_fp_curve](additional/models-and-calculators.md#load_fp_curve) | Load the bundled EP-by-yardline curve (no network, no first-use download). |
| [load_recruit_classes](additional/models-and-calculators.md#load_recruit_classes) | Load recruiting classes as per-recruit rows from the 247 RDB feed. |
| [normalize_pbp_columns](additional/models-and-calculators.md#normalize_pbp_columns) | Add card-named copies of any play-by-play columns `df` already carries. |
| [predict_from_card](additional/models-and-calculators.md#predict_from_card) | Score `df` with `booster`, validated and ordered by the model's card. |
| [predict_margin](additional/models-and-calculators.md#predict_margin) | Expected home scoring margin from the two net ratings. |
| [predict_total](additional/models-and-calculators.md#predict_total) | Expected combined point total from the four efficiency ratings + tempo. |
| [slope_for_games](additional/models-and-calculators.md#slope_for_games) | Points per unit of rating differential, given how many games back it. |
| [special_teams_ratings](additional/models-and-calculators.md#special_teams_ratings) | One row per team: a per-unit special-teams EPA composite. |
| [win_prob_from_margin](additional/models-and-calculators-2.md#win_prob_from_margin) | Home win probability from an expected margin via the Gaussian CDF. |

## Analytics

| Function | Summary |
|---|---|
| [add_play_type_canonical](additional/analytics.md#add_play_type_canonical) | Append `play_type_canonical` (and optionally `play_type_family`). |
| [canonical_play_type_expr](additional/analytics.md#canonical_play_type_expr) | Build the polars expression mapping raw `type.text` to a canonical type. |
| [cfb_adjusted_tempo](additional/analytics.md#cfb_adjusted_tempo) | Team-season situation-neutral, opponent-adjusted tempo / pace. |
| [cfb_games_from_schedule](additional/analytics.md#cfb_games_from_schedule) | Map a `load_cfb_schedule()` frame into the seedr engine `games` schema. |
| [cfb_playoff_seeds](additional/analytics.md#cfb_playoff_seeds) | Assign College Football Playoff seeds (current straight-seeding rule). |
| [cfb_resume](additional/analytics.md#cfb_resume) | Rating-based résumé metrics: SoS, quality wins, game control, wins-above-bubble. |
| [cfb_returning_production](additional/analytics.md#cfb_returning_production) | Returning production per team-season (offense / defense / overall). |
| [cfb_season_odds](additional/analytics.md#cfb_season_odds) | Ratings-driven season Monte Carlo: conference / playoff / championship odds. |
| [cfb_transfer_impact](additional/analytics.md#cfb_transfer_impact) | Net transfer talent and its projected win-total impact per team-season. |
| [cfb_transfer_moves](additional/analytics.md#cfb_transfer_moves) | Transfer moves inferred from year-over-year roster diffs. |
| [create_drive_summary](additional/analytics.md#create_drive_summary) | Build the StatBroadcast-style drive summary, chart, and long-play lists. |
| [create_situational_stats](additional/analytics.md#create_situational_stats) | Build the situational team-stats block from a plays frame. |
| [make_ratings_compute_results](additional/analytics.md#make_ratings_compute_results) | Build a `cfb_simulations` `compute_results` closure from fixed ratings. |
| [play_type_family_expr](additional/analytics.md#play_type_family_expr) | Build the polars expression mapping a canonical type to its phase family. |

## IDs and crosswalks

| Function | Summary |
|---|---|
| [cfb_odds_events_crosswalk](additional/ids-and-crosswalks.md#cfb_odds_events_crosswalk) | Match The Odds API CFB events to ESPN game ids. |
| [cfb_rosters_crosswalk](additional/ids-and-crosswalks.md#cfb_rosters_crosswalk) | Build the ESPN x Fox x Yahoo player-id crosswalk for one team. |
| [cfb_schedule_crosswalk](additional/ids-and-crosswalks.md#cfb_schedule_crosswalk) | Build the ESPN x Fox x Yahoo CFB game-id crosswalk. |
| [cfb_teams_crosswalk](additional/ids-and-crosswalks.md#cfb_teams_crosswalk) | Build the ESPN x Fox x Yahoo CFB team-id crosswalk. |

## Other

| Function | Summary |
|---|---|
| [espn_cfb_game_rosters](additional/other.md#espn_cfb_game_rosters) | espn_cfb_game_rosters() - Pull the game by id. |
| [espn_cfb_play_participants](additional/other.md#espn_cfb_play_participants) | Pull ESPN per-play participants for a college-football game. |
| [espn_cfb_teams](additional/other.md#espn_cfb_teams) | espn_cfb_teams - look up the college football teams |
| [scoreboard_event_parsing](additional/other.md#scoreboard_event_parsing) | Internal helper that flattens an ESPN scoreboard event dict into a shape |
| [load_cfb_betting_lines](additional/other.md#load_cfb_betting_lines) | Load college football betting lines information |
| [load_cfb_rosters_crosswalk](additional/other.md#load_cfb_rosters_crosswalk) | Load the current ESPN x Fox CFB rosters crosswalk (single snapshot). |
| [on3_industry_player_rankings](additional/other.md#on3_industry_player_rankings) | On3 Industry Comparison player rankings (**deprecated** next/data` scrape). |
| [on3_industry_team_rankings](additional/other.md#on3_industry_team_rankings) | On3 Industry Comparison team rankings (**deprecated** next/data` scrape). |
| [on3_player_rankings](additional/other.md#on3_player_rankings) | On3 player rankings for a class year (**deprecated** next/data` scrape). |
| [on3_team_rankings](additional/other.md#on3_team_rankings) | On3 team recruiting-class rankings (**deprecated** next/data` scrape). |
| [check_box_invariants](additional/other.md#check_box_invariants) | Every identity the two aggregates must satisfy; violations as strings. |
