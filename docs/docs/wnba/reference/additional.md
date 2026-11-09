---
title: WNBA — additional Python functions
sidebar_label: Additional functions
description: "WNBA — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# WNBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wnba` that are not
generated API-endpoint wrappers (those have their own reference pages).

## ESPN

| Function | Summary |
|---|---|
| [espn_wnba_calendar](additional/espn.md#espn_wnba_calendar) | espn_wnba_calendar - look up the WNBA calendar for a given season |
| [espn_wnba_draft](additional/espn.md#espn_wnba_draft) | Pull ESPN WNBA draft picks for a season. |
| [espn_wnba_game_officials](additional/espn.md#espn_wnba_game_officials) | Pull the officials assigned to a WNBA game. |
| [espn_wnba_game_rosters](additional/espn.md#espn_wnba_game_rosters) | espn_wnba_game_rosters() - Pull the game by id. |
| [espn_wnba_pbp](additional/espn.md#espn_wnba_pbp) | espn_wnba_pbp() - Pull one game's play-by-play from ESPN's `wnba/summary` endpoint. |
| [espn_wnba_player_stats](additional/espn.md#espn_wnba_player_stats) | Pull a WNBA athlete's ESPN **season** stat line. |
| [espn_wnba_schedule](additional/espn.md#espn_wnba_schedule) | espn_wnba_schedule - look up the WNBA schedule for a given season |
| [espn_wnba_standings](additional/espn.md#espn_wnba_standings) | Pull ESPN WNBA standings for a season. |
| [espn_wnba_team_roster](additional/espn.md#espn_wnba_team_roster) | Pull the current ESPN team roster for a WNBA team. |
| [espn_wnba_team_stats](additional/espn.md#espn_wnba_team_stats) | Pull ESPN team season stats for a WNBA team. |
| [espn_wnba_teams](additional/espn.md#espn_wnba_teams) | espn_wnba_teams - look up WNBA teams |
| [scoreboard_event_parsing](additional/espn.md#scoreboard_event_parsing) | Flatten one ESPN scoreboard event for the schedule frame, in place. |

## sportsdataverse-data releases

| Function | Summary |
|---|---|
| [load_wnba_stats_leaguedash](additional/sportsdataverse-data-releases.md#load_wnba_stats_leaguedash) | Load one asset family of the `wnba_stats_leaguedash` release. |
| [load_wnba_stats_lineups](additional/sportsdataverse-data-releases.md#load_wnba_stats_lineups) | Load season-level WNBA 5-man lineup statistics (deprecated). |
| [load_wnba_stats_player_season_stats](additional/sportsdataverse-data-releases.md#load_wnba_stats_player_season_stats) | Load season-level WNBA player statistics (deprecated). |
| [load_wnba_stats_standings](additional/sportsdataverse-data-releases.md#load_wnba_stats_standings) | Load season-level WNBA standings (deprecated). |
| [load_wnba_stats_team_season_stats](additional/sportsdataverse-data-releases.md#load_wnba_stats_team_season_stats) | Load season-level WNBA team statistics (deprecated). |

## WNBA Stats API

| Function | Summary |
|---|---|
| [wnba_live_boxscore](additional/wnba-stats-api.md#wnba_live_boxscore) | Fetch and parse WNBA cdn.wnba.com liveData boxscore for a game. |
| [wnba_live_pbp](additional/wnba-stats-api.md#wnba_live_pbp) | Fetch and parse WNBA cdn.wnba.com liveData play-by-play for a game. |

## Fox Sports API

| Function | Summary |
|---|---|
| [fox_wnba_boxscore](additional/fox-sports-api.md#fox_wnba_boxscore) | WNBA boxscore (long: one row per player-stat). |
| [fox_wnba_event_matchup](additional/fox-sports-api.md#fox_wnba_event_matchup) | Fox Sports wnba pregame team-stat comparison (one row per stat). |
| [fox_wnba_event_recap](additional/fox-sports-api.md#fox_wnba_event_recap) | Fox Sports wnba postgame top performers (one row per player). |
| [fox_wnba_event_standings](additional/fox-sports-api.md#fox_wnba_event_standings) | Fox Sports wnba the two teams' standings context. |
| [fox_wnba_league_conferences](additional/fox-sports-api.md#fox_wnba_league_conferences) | Fox Sports wnba conference / group directory. |
| [fox_wnba_league_header](additional/fox-sports-api.md#fox_wnba_league_header) | Fox Sports wnba league header (one row). |
| [fox_wnba_league_leaders](additional/fox-sports-api.md#fox_wnba_league_leaders) | WNBA statistical leaders (`stats-con`); who=player\|team. |
| [fox_wnba_league_odds](additional/fox-sports-api.md#fox_wnba_league_odds) | Fox Sports wnba league odds board (one row per team per game). |
| [fox_wnba_league_player_news](additional/fox-sports-api.md#fox_wnba_league_player_news) | Fox Sports wnba league-wide player news feed. |
| [fox_wnba_league_polls](additional/fox-sports-api.md#fox_wnba_league_polls) | Fox Sports wnba rankings / polls rendered as standings tables. |
| [fox_wnba_league_schedule](additional/fox-sports-api.md#fox_wnba_league_schedule) | Fox Sports wnba league schedule nav selections. |
| [fox_wnba_league_scores](additional/fox-sports-api.md#fox_wnba_league_scores) | Fox Sports wnba league scores nav selections. |
| [fox_wnba_league_standings](additional/fox-sports-api.md#fox_wnba_league_standings) | Fox Sports wnba league-wide standings tables. |
| [fox_wnba_league_stat_leaders](additional/fox-sports-api.md#fox_wnba_league_stat_leaders) | Fox Sports wnba league stats landing leaders. |
| [fox_wnba_odds](additional/fox-sports-api.md#fox_wnba_odds) | WNBA game odds six-pack (spread / to-win / total per team). |
| [fox_wnba_pbp](additional/fox-sports-api.md#fox_wnba_pbp) | WNBA play-by-play (one row per play; period-based). |
| [fox_wnba_scoreboard](additional/fox-sports-api.md#fox_wnba_scoreboard) | Fox Sports wnba scoreboard nav selections (weeks / dates / groups). |
| [fox_wnba_scorechip](additional/fox-sports-api.md#fox_wnba_scorechip) | Fox Sports wnba compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_wnba_scores_segment](additional/fox-sports-api.md#fox_wnba_scores_segment) | Fox Sports wnba one row per game in a scoreboard segment. |
| [fox_wnba_standings](additional/fox-sports-api.md#fox_wnba_standings) | WNBA standings for a team's conference/division. |
| [fox_wnba_team_gamelog](additional/fox-sports-api.md#fox_wnba_team_gamelog) | WNBA team game log (long: one row per game-stat). |
| [fox_wnba_team_header](additional/fox-sports-api.md#fox_wnba_team_header) | Fox Sports wnba team header (one row). |
| [fox_wnba_team_roster](additional/fox-sports-api.md#fox_wnba_team_roster) | WNBA team roster (one row per player). |
| [fox_wnba_team_stats](additional/fox-sports-api.md#fox_wnba_team_stats) | WNBA team stat leaders by category. |
| [fox_wnba_teamnav](additional/fox-sports-api.md#fox_wnba_teamnav) | Fox Sports wnba team directory (one row per team). |
| [fox_wnba_teams](additional/fox-sports-api.md#fox_wnba_teams) | WNBA team directory (`fox_team_id` / `fox_team_name` / `fox_section`). |

## Play-by-play processing

| Function | Summary |
|---|---|
| [build_athlete_identity_lookup](additional/play-by-play-processing.md#build_athlete_identity_lookup) | R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters. |
| [wnba_enhanced_pbp](additional/play-by-play-processing.md#wnba_enhanced_pbp) | Return a normalised enhanced play-by-play frame for a WNBA game. |
| [wnba_on_court](additional/play-by-play-processing.md#wnba_on_court) | Return the rotation-keyed on-court player frame for a WNBA game. |
| [wnba_pbp_disk](additional/play-by-play-processing.md#wnba_pbp_disk) | Read a saved ESPN WNBA play-by-play payload from disk. |
| [wnba_play_context](additional/play-by-play-processing.md#wnba_play_context) | Return a WNBA game's possessions with the full CTG play-context surface. |
| [wnba_possessions](additional/play-by-play-processing.md#wnba_possessions) | Return the possession-level lineup stint matrix for a WNBA game. |
| [wnba_rapm_from_games](additional/play-by-play-processing.md#wnba_rapm_from_games) | Compute per-player RAPM estimates over a sequence of WNBA games. |

## Models and calculators

| Function | Summary |
|---|---|
| [build_wnba_season_wp](additional/models-and-calculators.md#build_wnba_season_wp) | A WNBA season's play-by-play with win-probability columns joined in. |
| [wnba_aging_curve](additional/models-and-calculators.md#wnba_aging_curve) | WNBA aging curve -- the NBA core bound to `league="wnba"`. |
| [wnba_career_trajectory](additional/models-and-calculators.md#wnba_career_trajectory) | WNBA career trajectory -- the NBA core bound to `league="wnba"`. |
| [wnba_draft_model](additional/models-and-calculators.md#wnba_draft_model) | Project WNBA prospect career value + draft probability from draft slot. |
| [wnba_in_game_win_prob](additional/models-and-calculators.md#wnba_in_game_win_prob) | WNBA in-game win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_in_game_win_prob. |
| [wnba_predict_games](additional/models-and-calculators.md#wnba_predict_games) | WNBA vectorized pregame predictions (league_id='10'). See sportsdataverse.nba.nba_game_predict.nba_predict_games. |
| [wnba_predict_margin](additional/models-and-calculators.md#wnba_predict_margin) | WNBA expected margin (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_margin. |
| [wnba_predict_total](additional/models-and-calculators.md#wnba_predict_total) | WNBA expected total (league_id='10'). See sportsdataverse.nba.nba_game_predict.predict_total. |
| [wnba_rookie_projection](additional/models-and-calculators.md#wnba_rookie_projection) | WNBA rookie/sophomore projection -- composes the WNBA draft/aging/availability pieces. |
| [wnba_team_ratings](additional/models-and-calculators.md#wnba_team_ratings) | WNBA team ratings (league_id='10'). See sportsdataverse.nba.nba_team_ratings.nba_team_ratings. |
| [wnba_win_prob_from_margin](additional/models-and-calculators.md#wnba_win_prob_from_margin) | WNBA home win probability (league_id='10'). See sportsdataverse.nba.nba_game_predict.win_prob_from_margin. |

## Analytics

| Function | Summary |
|---|---|
| [make_prob_by_context](additional/analytics.md#make_prob_by_context) | Marginal FG% tables by defender distance and by shot clock. |
| [make_prob_joint](additional/analytics.md#make_prob_joint) | Independence-combined defender x shot-clock make probability. |
| [score_shot_xpoints](additional/analytics.md#score_shot_xpoints) | Score each shot with expected points from the league-average baseline. |
| [shooter_talent](additional/analytics.md#shooter_talent) | Regressed shooter true-talent: make%-above-expected, shrunk to the mean. |
| [shot_selection_quality](additional/analytics.md#shot_selection_quality) | Player shot-selection quality: mean expected value vs the league mean. |
| [wnba_availability](additional/analytics.md#wnba_availability) | WNBA availability -- the NBA core bound to `league="wnba"`. |
| [wnba_coach_careers](additional/analytics.md#wnba_coach_careers) | WNBA head-coach careers over the given seasons (league_id='10'). See sportsdataverse.nba.nba_coach_seasons.nba_coach_careers. |
| [wnba_coach_seasons](additional/analytics.md#wnba_coach_seasons) | WNBA head-coach seasons: W-L, ratings and four factors over each coach's games (league_id='10'). See sportsdataverse.nba.nba_coach_seasons.nba_coach_seasons. |
| [wnba_draft_lottery_odds](additional/analytics.md#wnba_draft_lottery_odds) | WNBA draft-lottery odds by slot, two picks drawn (league_id='10'). See sportsdataverse.nba.nba_season_rules.nba_draft_lottery_odds. |
| [wnba_epv_lineup_games](additional/analytics.md#wnba_epv_lineup_games) | Per WNBA five-woman unit and game: the EPV added with the unit on the floor. |
| [wnba_epv_player_games](additional/analytics.md#wnba_epv_player_games) | Per WNBA player and game: EPV added on her own actions and on-court EPV +/-. |
| [wnba_epv_plays](additional/analytics.md#wnba_epv_plays) | EPV and EPV added on every WNBA play-by-play event inside a possession. |
| [wnba_epv_team_games](additional/analytics.md#wnba_epv_team_games) | Per WNBA team and game: EPV added on offense and allowed on defense. |
| [wnba_expected_turnovers](additional/analytics.md#wnba_expected_turnovers) | WNBA expected turnovers / ball-security skill (`league_id="10"`). |
| [wnba_foul_drawing](additional/analytics.md#wnba_foul_drawing) | WNBA foul-drawing / FT-generation (`league_id="10"`). |
| [wnba_game_flow](additional/analytics.md#wnba_game_flow) | WNBA game flow per team and game: runs, droughts, kills, lead changes, time leading, game control (league_id='10'). See … |
| [wnba_game_lineups_used](additional/analytics.md#wnba_game_lineups_used) | WNBA lineups used per game (league_id='10'). See sportsdataverse.nba.nba_game_impact.nba_game_lineups_used. |
| [wnba_game_player_impact](additional/analytics.md#wnba_game_player_impact) | WNBA game impact box (league_id='10'). See sportsdataverse.nba.nba_game_impact.nba_game_player_impact. |
| [wnba_lineup_seasons](additional/analytics-2.md#wnba_lineup_seasons) | WNBA lineup seasons (league_id='10'). See sportsdataverse.nba.nba_lineup_seasons.nba_lineup_seasons. |
| [wnba_matchup_drapm](additional/analytics-2.md#wnba_matchup_drapm) | WNBA matchup defensive RAPM (`league_id="10"`). |
| [wnba_player_factor_rapm](additional/analytics-2.md#wnba_player_factor_rapm) | WNBA four-factor RAPM split (league_id='10'). See sportsdataverse.nba.nba_player_factor_rapm.nba_player_factor_rapm. |
| [wnba_player_on_off](additional/analytics-2.md#wnba_player_on_off) | WNBA player on/off (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_on_off. |
| [wnba_player_props](additional/analytics-2.md#wnba_player_props) | WNBA player props (league_id='10'). See sportsdataverse.nba.nba_player_props.nba_player_props. |
| [wnba_player_war](additional/analytics-2.md#wnba_player_war) | WNBA WAR and VORP per player-season from our BPM, RAPM and BOBAN (league_id='10'). See sportsdataverse.nba.nba_player_war.nba_player_war. |
| [wnba_player_wowy](additional/analytics-2.md#wnba_player_wowy) | WNBA WOWY pairs (league_id='10'). See sportsdataverse.nba.nba_player_on_off.nba_player_wowy. |
| [wnba_playtype_ratings](additional/analytics-2.md#wnba_playtype_ratings) | WNBA Synergy play-type-adjusted offense/defense (`league_id="10"`). |
| [wnba_referee_assignments](additional/analytics-2.md#wnba_referee_assignments) | Fetch and parse WNBA referee assignments for a given date from official.nba.com. |
| [wnba_season_sim](additional/analytics-2.md#wnba_season_sim) | WNBA season sim: projected W-L, seed, playoff and lottery odds (league_id='10'). See sportsdataverse.nba.nba_season_sim.nba_season_sim. |
| [wnba_season_standings](additional/analytics-3.md#wnba_season_standings) | WNBA standings with the WNBA tiebreakers (league_id='10'). See sportsdataverse.nba.nba_season_rules.nba_season_standings. |
| [wnba_shot_value](additional/analytics-3.md#wnba_shot_value) | WNBA one-call shot-value spine (`league_id="10"`). |
| [wnba_situational_lineup](additional/analytics-3.md#wnba_situational_lineup) | WNBA five-man unit splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_lineup. |
| [wnba_situational_player](additional/analytics-3.md#wnba_situational_player) | WNBA player on-floor splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_player. |
| [wnba_situational_team](additional/analytics-3.md#wnba_situational_team) | WNBA team splits by situation (league_id='10'). See sportsdataverse.nba.nba_situational.nba_situational_team. |
| [wnba_team_clutch](additional/analytics-3.md#wnba_team_clutch) | WNBA clutch skill (league_id='10'). See sportsdataverse.nba.nba_clutch.nba_team_clutch. |
| [wnba_team_luck](additional/analytics-3.md#wnba_team_luck) | WNBA luck and close-game record: Pythagorean expected wins vs actual, close-game W-L (league_id='10'). See sportsdataverse.nba.nba_team_luck.nba_team_luck. |
| [wnba_team_season_flow](additional/analytics-3.md#wnba_team_season_flow) | WNBA game flow per team and season, with ranks (league_id='10'). See sportsdataverse.nba.nba_game_flow.nba_team_season_flow. |
| [wnba_team_season_rates](additional/analytics-3.md#wnba_team_season_rates) | WNBA season team rates from possessions (league_id='10'). See sportsdataverse.nba.nba_team_season_rates.nba_team_season_rates. |
| [wnba_team_strength](additional/analytics-3.md#wnba_team_strength) | WNBA team strength: adjusted ratings, SOS, SOV, pace and four factors (league_id='10'). See sportsdataverse.nba.nba_team_strength.nba_team_strength. |
| [wnba_tracking_drive_value](additional/analytics-3.md#wnba_tracking_drive_value) | WNBA drive value + rim-pressure (`league_id="10"` by-reference shim). |
| [wnba_tracking_pass_value](additional/analytics-3.md#wnba_tracking_pass_value) | WNBA expected-assists / passer value (`league_id="10"` by-reference shim). |
| [wnba_tracking_reb_oe](additional/analytics-3.md#wnba_tracking_reb_oe) | WNBA rebounding-over-expected (`league_id="10"` by-reference shim). |
| [wnba_tracking_rim_protect_value](additional/analytics-3.md#wnba_tracking_rim_protect_value) | WNBA rim-protection / shot-defend points-saved (`league_id="10"` |
| [wnba_tracking_shot_diet_value](additional/analytics-4.md#wnba_tracking_shot_diet_value) | WNBA catch-&-shoot vs pull-up points-over-expected (`league_id="10"` |
| [wnba_tracking_touch_value](additional/analytics-4.md#wnba_tracking_touch_value) | WNBA touch / possession-time value (`league_id="10"` by-reference shim). |
| [zone_value_map](additional/analytics-4.md#zone_value_map) | Per-player per-zone value map: points and expected points per shot. |

## Dates and seasons

| Function | Summary |
|---|---|
| [most_recent_wnba_season](additional/dates-and-seasons.md#most_recent_wnba_season) | most_recent_wnba_season - return the most recent (likely-completed) WNBA season year. |

## IDs and crosswalks

| Function | Summary |
|---|---|
| [wnba_crosswalks_from_archive](additional/ids-and-crosswalks.md#wnba_crosswalks_from_archive) | Build the WNBA team, schedule and player crosswalks for a past season, offline. |
| [wnba_player_crosswalk](additional/ids-and-crosswalks.md#wnba_player_crosswalk) | Build the WNBA cross-source player crosswalk (ESPN / WNBA Stats / Fox). |
| [wnba_schedule_crosswalk](additional/ids-and-crosswalks.md#wnba_schedule_crosswalk) | Build the WNBA cross-source schedule crosswalk (ESPN / WNBA Stats / Fox / Yahoo). |
| [wnba_team_crosswalk](additional/ids-and-crosswalks.md#wnba_team_crosswalk) | Build the WNBA cross-source team crosswalk (ESPN / WNBA Stats / Fox / Yahoo). |
