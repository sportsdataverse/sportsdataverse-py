---
title: NBA — additional Python functions
sidebar_label: Additional functions
description: "NBA — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# NBA — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.nba` that are not
generated API-endpoint wrappers (those have their own reference pages).

## Highlights

| Function | Summary |
|---|---|
| [bref_players_stats](additional/highlights.md#bref_players_stats) | Player season statistics for an entire league season. |
| [bref_standings](additional/highlights.md#bref_standings) | Conference standings for a season, both conferences stacked. |
| [bref_teams_stats](additional/highlights.md#bref_teams_stats) | Team season statistics from the league season page. |
| [compile_nba_season](additional/highlights.md#compile_nba_season) | Compile a full season's possession stint matrix (cached + resumable + throttled). |
| [espn_nba_player_stats](additional/highlights.md#espn_nba_player_stats) | Pull an NBA athlete's ESPN **season** stat line as one wide row. |
| [espn_nba_schedule](additional/highlights.md#espn_nba_schedule) | espn_nba_schedule - look up the NBA schedule for a given date from ESPN |
| [espn_nba_teams](additional/highlights.md#espn_nba_teams) | espn_nba_teams - look up NBA teams |
| [most_recent_nba_season](additional/highlights.md#most_recent_nba_season) | Return the most recent NBA season year based on today's date. |

## ESPN

| Function | Summary |
|---|---|
| [espn_nba_calendar](additional/espn.md#espn_nba_calendar) | espn_nba_calendar - look up the NBA calendar for a given season from ESPN |
| [espn_nba_game_all](additional/espn.md#espn_nba_game_all) | espn_nba_game_all() - Pull one game's plays, team box and player box from one ESPN request. |
| [espn_nba_game_rosters](additional/espn.md#espn_nba_game_rosters) | espn_nba_game_rosters() - Pull the game by id. |
| [espn_nba_pbp](additional/espn.md#espn_nba_pbp) | espn_nba_pbp() - Pull one game's play-by-play from ESPN's `nba/summary` endpoint. |
| [espn_nba_player_box](additional/espn.md#espn_nba_player_box) | espn_nba_player_box() - Pull one game's player box score from ESPN's `nba/summary` endpoint. |
| [espn_nba_team_box](additional/espn.md#espn_nba_team_box) | espn_nba_team_box() - Pull one game's team box score from ESPN's `nba/summary` endpoint. |
| [scoreboard_event_parsing](additional/espn.md#scoreboard_event_parsing) | Internal helper that flattens an ESPN NBA scoreboard event dict into a |

## sportsdataverse-data releases

| Function | Summary |
|---|---|
| [load_nba_stats_draft_combine_anthro](additional/sportsdataverse-data-releases.md#load_nba_stats_draft_combine_anthro) | Load draft-combine anthropometric measurements (`draftcombineplayeranthro`). |
| [load_nba_stats_draft_combine_drills](additional/sportsdataverse-data-releases.md#load_nba_stats_draft_combine_drills) | Load draft-combine athletic drill results (`draftcombinedrillresults`). |
| [load_nba_stats_draft_combine_nonstationary_shooting](additional/sportsdataverse-data-releases.md#load_nba_stats_draft_combine_nonstationary_shooting) | Load draft-combine off-the-dribble and on-the-move shooting (`draftcombinenonstationaryshooting`). |
| [load_nba_stats_draft_combine_spot_shooting](additional/sportsdataverse-data-releases.md#load_nba_stats_draft_combine_spot_shooting) | Load draft-combine spot-up shooting drills (`draftcombinespotshooting`). |
| [load_nba_stats_draft_combine_stats](additional/sportsdataverse-data-releases.md#load_nba_stats_draft_combine_stats) | Load the draft-combine summary table (`nba_stats_draft_combine`, `draftcombinestats`). |
| [load_nba_stats_hustle_player](additional/sportsdataverse-data-releases.md#load_nba_stats_hustle_player) | Load player hustle stats (the `nba_stats_hustle` release, player files). |
| [load_nba_stats_hustle_team](additional/sportsdataverse-data-releases.md#load_nba_stats_hustle_team) | Load team hustle stats (the `nba_stats_hustle` release, team files). |
| [load_nba_stats_leaguedash](additional/sportsdataverse-data-releases.md#load_nba_stats_leaguedash) | Load one asset family of the `nba_stats_leaguedash` release. |
| [load_nba_stats_matchups](additional/sportsdataverse-data-releases.md#load_nba_stats_matchups) | Load offensive-vs-defensive player matchups (`nba_stats_matchups`, season files). |
| [load_nba_stats_matchups_rollup](additional/sportsdataverse-data-releases.md#load_nba_stats_matchups_rollup) | Load defensive matchup roll-ups by position (`nba_stats_matchups`, rollup files). |
| [load_nba_stats_synergy](additional/sportsdataverse-data-releases.md#load_nba_stats_synergy) | Load Synergy play-type stats (the `nba_stats_synergy` release). |

## NBA Stats API

| Function | Summary |
|---|---|
| [nba_live_boxscore](additional/nba-stats-api.md#nba_live_boxscore) | Fetch and parse NBA cdn.nba.com liveData boxscore for a game. |
| [nba_live_pbp](additional/nba-stats-api.md#nba_live_pbp) | Fetch and parse NBA cdn.nba.com liveData play-by-play for a game. |

## NBA Official Injury Report

| Function | Summary |
|---|---|
| [nba_injury_report](additional/nba-official-injury-report.md#nba_injury_report) | Fetch and parse an official NBA injury report. |
| [nba_injury_report_slots](additional/nba-official-injury-report.md#nba_injury_report_slots) | Every slot the league's schedule allows on `date`, earliest first. |
| [nba_injury_report_url](additional/nba-official-injury-report.md#nba_injury_report_url) | The archive URL of the report published at `report_time` (US Eastern). |

## Fox Sports API

| Function | Summary |
|---|---|
| [fox_nba_boxscore](additional/fox-sports-api.md#fox_nba_boxscore) | NBA boxscore (long: one row per player-stat). |
| [fox_nba_event_matchup](additional/fox-sports-api.md#fox_nba_event_matchup) | Fox Sports nba pregame team-stat comparison (one row per stat). |
| [fox_nba_event_recap](additional/fox-sports-api.md#fox_nba_event_recap) | Fox Sports nba postgame top performers (one row per player). |
| [fox_nba_event_standings](additional/fox-sports-api.md#fox_nba_event_standings) | Fox Sports nba the two teams' standings context. |
| [fox_nba_league_conferences](additional/fox-sports-api.md#fox_nba_league_conferences) | Fox Sports nba conference / group directory. |
| [fox_nba_league_header](additional/fox-sports-api.md#fox_nba_league_header) | Fox Sports nba league header (one row). |
| [fox_nba_league_leaders](additional/fox-sports-api.md#fox_nba_league_leaders) | NBA statistical leaders (`stats-con`); who=player\|team. |
| [fox_nba_league_odds](additional/fox-sports-api.md#fox_nba_league_odds) | Fox Sports nba league odds board (one row per team per game). |
| [fox_nba_league_player_news](additional/fox-sports-api.md#fox_nba_league_player_news) | Fox Sports nba league-wide player news feed. |
| [fox_nba_league_polls](additional/fox-sports-api.md#fox_nba_league_polls) | Fox Sports nba rankings / polls rendered as standings tables. |
| [fox_nba_league_schedule](additional/fox-sports-api.md#fox_nba_league_schedule) | Fox Sports nba league schedule nav selections. |
| [fox_nba_league_scores](additional/fox-sports-api.md#fox_nba_league_scores) | Fox Sports nba league scores nav selections. |
| [fox_nba_league_standings](additional/fox-sports-api.md#fox_nba_league_standings) | Fox Sports nba league-wide standings tables. |
| [fox_nba_league_stat_leaders](additional/fox-sports-api.md#fox_nba_league_stat_leaders) | Fox Sports nba league stats landing leaders. |
| [fox_nba_odds](additional/fox-sports-api.md#fox_nba_odds) | NBA game odds six-pack (spread / to-win / total per team). |
| [fox_nba_pbp](additional/fox-sports-api.md#fox_nba_pbp) | NBA play-by-play (one row per play; period-based). |
| [fox_nba_scoreboard](additional/fox-sports-api.md#fox_nba_scoreboard) | Fox Sports nba scoreboard nav selections (weeks / dates / groups). |
| [fox_nba_scorechip](additional/fox-sports-api.md#fox_nba_scorechip) | Fox Sports nba compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_nba_scores_segment](additional/fox-sports-api.md#fox_nba_scores_segment) | Fox Sports nba one row per game in a scoreboard segment. |
| [fox_nba_standings](additional/fox-sports-api.md#fox_nba_standings) | NBA standings for a team's conference/division. |
| [fox_nba_team_gamelog](additional/fox-sports-api.md#fox_nba_team_gamelog) | NBA team game log (long: one row per game-stat). |
| [fox_nba_team_header](additional/fox-sports-api.md#fox_nba_team_header) | Fox Sports nba team header (one row). |
| [fox_nba_team_roster](additional/fox-sports-api.md#fox_nba_team_roster) | NBA team roster (one row per player). |
| [fox_nba_team_stats](additional/fox-sports-api.md#fox_nba_team_stats) | NBA team stat leaders by category. |
| [fox_nba_teamnav](additional/fox-sports-api.md#fox_nba_teamnav) | Fox Sports nba team directory (one row per team). |
| [fox_nba_teams](additional/fox-sports-api.md#fox_nba_teams) | NBA team directory (`fox_team_id` / `fox_team_name` / `fox_section`). |

## Basketball-Reference

| Function | Summary |
|---|---|
| [bref_awards](additional/basketball-reference.md#bref_awards) | End-of-season award voting, all awards stacked into one frame. |
| [bref_draft](additional/basketball-reference.md#bref_draft) | NBA draft results with each pick's career totals and advanced metrics. |
| [bref_injuries](additional/basketball-reference.md#bref_injuries) | The current NBA injury report. |
| [bref_player_bios](additional/basketball-reference.md#bref_player_bios) | The player index for one last-name initial -- bios plus the id slugs. |
| [bref_player_game_log](additional/basketball-reference.md#bref_player_game_log) | A player's regular-season game-by-game log. |
| [bref_team_roster](additional/basketball-reference.md#bref_team_roster) | A team's roster for one season. |

## RealGM

| Function | Summary |
|---|---|
| [realgm_close_browser](additional/realgm.md#realgm_close_browser) | Close the cached headless browser, if one is open. |
| [realgm_coaches](additional/realgm.md#realgm_coaches) | Current NBA head coaches. |
| [realgm_draft](additional/realgm.md#realgm_draft) | Results of one past NBA draft. |
| [realgm_draft_prospects](additional/realgm.md#realgm_draft_prospects) | Current NBA draft-prospect statistics. |
| [realgm_early_entry](additional/realgm.md#realgm_early_entry) | The current NBA draft early-entrant and withdrawal list. |
| [realgm_future_free_agents](additional/realgm.md#realgm_future_free_agents) | RealGM's projected future NBA free-agent classes, with each player's agent. |
| [realgm_gms](additional/realgm.md#realgm_gms) | Current NBA general managers. |
| [realgm_individual_games](additional/realgm.md#realgm_individual_games) | The all-time best individual NBA games leaderboard. |
| [realgm_individual_seasons](additional/realgm.md#realgm_individual_seasons) | The all-time best individual NBA seasons leaderboard. |
| [realgm_player_stats](additional/realgm.md#realgm_player_stats) | Season player-statistics leaderboard for one stat family and season segment. |
| [realgm_players](additional/realgm.md#realgm_players) | The active NBA player index from RealGM. |
| [realgm_players_abroad](additional/realgm.md#realgm_players_abroad) | NBA-affiliated players currently playing overseas. |
| [realgm_rookie_scale](additional/realgm.md#realgm_rookie_scale) | The current NBA rookie-scale salary table. |
| [realgm_salary_cap](additional/realgm.md#realgm_salary_cap) | NBA salary-cap history and projections. |
| [realgm_standings](additional/realgm.md#realgm_standings) | Current NBA standings, both conferences stacked. |
| [realgm_team_stats](additional/realgm.md#realgm_team_stats) | Season team statistics for one stat family and season segment. |
| [realgm_teams](additional/realgm.md#realgm_teams) | The NBA team index with division and conference. |
| [realgm_transactions](additional/realgm.md#realgm_transactions) | The NBA league transactions log. |

## Public model datasets

| Function | Summary |
|---|---|
| [load_darko_dpm](additional/public-model-datasets.md#load_darko_dpm) | Parse a DARKO DPM leaderboard CSV (e.g. `2026-darko-dpm-leaderboard.csv`). |
| [load_dunks_threes_stats](additional/public-model-datasets.md#load_dunks_threes_stats) | Parse a Dunks & Threes counting-stats CSV (e.g. `2025_Dunks_&_Threes_Stats.csv`). |
| [load_epm](additional/public-model-datasets.md#load_epm) | Parse a Dunks & Threes EPM CSV (`{season}_EPM_data.csv`). |
| [load_lebron_daily](additional/public-model-datasets.md#load_lebron_daily) | Parse a LEBRON daily-snapshot CSV (e.g. `lebron_daily_2026-07-02.csv`). |
| [load_lebron_season](additional/public-model-datasets.md#load_lebron_season) | Parse a LEBRON season-file CSV (e.g. `lebron-data-2026.csv`). |
| [load_rapm_ryan_davis](additional/public-model-datasets.md#load_rapm_ryan_davis) | Parse a Ryan Davis published RAPM CSV (single-season or multi-year window). |
| [normalize_player_name](additional/public-model-datasets.md#normalize_player_name) | Fold a player display name to a join-safe key. |

## Play-by-play processing

| Function | Summary |
|---|---|
| [build_athlete_identity_lookup](additional/play-by-play-processing.md#build_athlete_identity_lookup) | R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters. |
| [build_nba_player_identity_lookup](additional/play-by-play-processing.md#build_nba_player_identity_lookup) | R `build_identity_lookup(season)`: athlete_id -> identity from the |
| [nba_pbp_disk](additional/play-by-play-processing.md#nba_pbp_disk) | Load a previously cached ESPN NBA summary JSON for a game from disk. |
| [nba_v3_to_v2_pbp](additional/play-by-play-processing.md#nba_v3_to_v2_pbp) | Convert a v3 `playbyplayv3` payload into the full v2-schema pbp frame. |

## Models and calculators

| Function | Summary |
|---|---|
| [AdjRapmModel](additional/models-and-calculators.md#AdjRapmModel) | Prior-informed RAPM: ridge toward a per-player box prior with an RTO posterior. |
| [AgingCurve](additional/models-and-calculators.md#AgingCurve) | Empirical aging deltas: `delta_by_age[a]` = expected rating change aging a -> a+1. |
| [BoxPriorModel](additional/models-and-calculators.md#BoxPriorModel) | A cross-fitted box prior: one offense and one defense linear rule per player fold. |
| [ExternalValidityResult](additional/models-and-calculators.md#ExternalValidityResult) | Concurrent-validity correlation of model ratings against a published oracle metric. |
| [ForecastResult](additional/models-and-calculators.md#ForecastResult) | Forecast-accuracy metrics: predicted-vs-actual next-season rating over held-out transitions. |
| [LeagueConstants](additional/models-and-calculators.md#LeagueConstants) | Per-`league_id` fitted constants for the NBA prediction & market stack. |
| [MeasureSpec](additional/models-and-calculators.md#MeasureSpec) | Per-model column map for a `leaguedashptstats` measure. |
| [NbaBpmModel](additional/models-and-calculators.md#NbaBpmModel) | A `RatingsModel` scoring a fold via faithful BPM 2.0. |
| [NbaSpmModel](additional/models-and-calculators.md#NbaSpmModel) | A `RatingsModel` that scores a fold via fitted SPM coefficients. |
| [RemlFit](additional/models-and-calculators.md#RemlFit) | A prior-informed ridge fit with separate offense / defense penalties. |
| [RidgeRapmModel](additional/models-and-calculators.md#RidgeRapmModel) | 5179.47467923,  13894.95494373,  37275.93720315, 100000.        ])) -> 'None'` |
| [SeasonGram](additional/models-and-calculators.md#SeasonGram) | One season's RAPM sufficient statistics (the design of `build_rapm_design`). |
| [SpmCoefficients](additional/models-and-calculators.md#SpmCoefficients) | Fitted SPM coefficients (box features -> offense/defense RAPM, per-100). |
| [ValidationReport](additional/models-and-calculators.md#ValidationReport) | Holds all oracle results for a single model evaluation run. |
| [WalkForwardResult](additional/models-and-calculators.md#WalkForwardResult) | Oracle 6: walk-forward ("predict tomorrow") retrodiction over a season timeline. |
| [adjust_efficiency](additional/models-and-calculators.md#adjust_efficiency) | Iterative opponent-adjusted rating -> AdjOffRtg / AdjDefRtg / AdjNet per team-season. |
| [adjust_pace](additional/models-and-calculators.md#adjust_pace) | Opponent-adjusted pace (possessions/game) per team-season. |
| [apply_box_prior](additional/models-and-calculators.md#apply_box_prior) | Score box-prior features with each player's out-of-fold rule. |
| [as_of_ratings_split](additional/models-and-calculators.md#as_of_ratings_split) | Filter a results frame to games strictly before a cutoff date (leakage boundary). |
| [box_prior_features](additional/models-and-calculators.md#box_prior_features) | One season's box-prior features: league-relative, padded box rates plus player and lineup context. |
| [box_prior_training_seasons](additional/models-and-calculators.md#box_prior_training_seasons) | The seasons a prior for `season` may learn from: earlier ones, or every other one through 2005. |
| [calibrate_pts_per_win](additional/models-and-calculators.md#calibrate_pts_per_win) | Regress team wins on season point margin; return points-per-marginal-win. |
| [calibrate_replacement_level](additional/models-and-calculators.md#calibrate_replacement_level) | Solve for the `replacement_level` that makes summed league WAR hit a target. |
| [career_rapm_target](additional/models-and-calculators.md#career_rapm_target) | Age-adjusted, time-weighted career RAPM over `seasons`: the box prior's training target. |
| [darko_forecast_accuracy](additional/models-and-calculators.md#darko_forecast_accuracy) | Holdout forecast accuracy: for each transition, forecast N+1 from history <= N vs actual. |
| [decay_weights](additional/models-and-calculators.md#decay_weights) | Exponential time-decay sample weights `w = 0.5 ** (days_ago / half_life)`. |
| [expected_possessions](additional/models-and-calculators.md#expected_possessions) | Expected possessions for a matchup (Pythagorean-tempo blend). |
| [external_validity](additional/models-and-calculators.md#external_validity) | Oracle 5: correlate a model's ratings against a published external metric. |
| [fit_aging_curve](additional/models-and-calculators.md#fit_aging_curve) | Fit the aging curve by the delta method: avg YoY rating change grouped by starting age. |
| [fit_reml_prior_ridge](additional/models-and-calculators.md#fit_reml_prior_ridge) | Ridge toward `prior_mean` with one penalty for offense and one for defense, chosen by REML. |
| [game_excitement](additional/models-and-calculators.md#game_excitement) | How exciting one game was, with no direction: the game excitement index, lead changes and the biggest swing. |
| [game_minutes](additional/models-and-calculators.md#game_minutes) | Minutes played in a game of `periods` periods: the league's regulation length plus five per overtime. |
| [game_page_frames](additional/models-and-calculators.md#game_page_frames) | Everything the Game on Paper basketball game page needs, from one `espn_<league>_pbp` payload. |
| [game_page_tables](additional/models-and-calculators.md#game_page_tables) | Every game-page table for one game, from its basketball pbp / team box / player box frames. |
| [game_regulation_periods](additional/models-and-calculators.md#game_regulation_periods) | Regulation periods of one game: 2 (halves) or 4 (quarters). |
| [get_constants](additional/models-and-calculators.md#get_constants) | Return the `LeagueConstants` for a `league_id`. |
| [get_shrinkage_k](additional/models-and-calculators.md#get_shrinkage_k) | Shooter-talent shrinkage `k` for a league. |
| [home_prior](additional/models-and-calculators.md#home_prior) | Pregame home win probability for the in-game model, and where it came from. |
| [in_game_features](additional/models-and-calculators.md#in_game_features) | Per-play in-game win-probability features from a `load_nba_pbp` frame. |
| [league_pbp_functions](additional/models-and-calculators.md#league_pbp_functions) | The ESPN fetch and parse pair for one league: `(espn_<league>_pbp, helper_<league>_pbp)`. |
| [luck_adjusted_response](additional/models-and-calculators-2.md#luck_adjusted_response) | Attach a per-possession `la_points` expected-points response. |
| [nba_adj_rapm](additional/models-and-calculators-2.md#nba_adj_rapm) | One-shot prior-informed RAPM over a possession frame -> per-player ratings. |
| [nba_aging_curve](additional/models-and-calculators-2.md#nba_aging_curve) | Load the bundled per-age value-multiplier curve. |
| [nba_bpm](additional/models-and-calculators-2.md#nba_bpm) | Faithful BPM 2.0 per player, at season or single-game granularity. |
| [nba_career_trajectory](additional/models-and-calculators-2.md#nba_career_trajectory) | Age-adjust player-season values with the bundled aging curve. |
| [nba_darko](additional/models-and-calculators-2.md#nba_darko) | Project each player's next-season rating via a per-player Kalman filter + aging curve. |
| [nba_decay_rapm](additional/models-and-calculators-2.md#nba_decay_rapm) | Time-decay RAPM: ridge weighted by `0.5 ** (days_ago / half_life_days)`. |
| [nba_draft_model](additional/models-and-calculators-2.md#nba_draft_model) | Project prospect career value + draft probability from combine measurements. |
| [nba_expected_turnovers](additional/models-and-calculators-2.md#nba_expected_turnovers) | Expected TOV + residual ball-security skill from Synergy play-type mix. |
| [nba_four_factor_rapm](additional/models-and-calculators-2.md#nba_four_factor_rapm) | Four-factor RAPM: four independent ridge fits (efg/ftr/orbd/tov) on the SAME design. |
| [nba_in_game_win_prob](additional/models-and-calculators-2.md#nba_in_game_win_prob) | Per-play home win probability from the bundled in-game model. |
| [nba_la_rapm](additional/models-and-calculators-2.md#nba_la_rapm) | Luck-adjusted RAPM: ridge on an expected-points response (high-variance shooting regressed). |
| [nba_matchup_drapm](additional/models-and-calculators-2.md#nba_matchup_drapm) | Matchup-based defensive RAPM (offense-quality-controlled). |
| [nba_player_impact_context](additional/models-and-calculators-2.md#nba_player_impact_context) | Append `<m>_n`, `<m>_rank` and `<m>_pct` to a `player_impact` table. |
| [nba_player_impact_league_averages](additional/models-and-calculators-2.md#nba_player_impact_league_averages) | The league-average rows of a `player_impact` table, one block per season type. |
| [nba_predict_games](additional/models-and-calculators-2.md#nba_predict_games) | Vectorized pregame predictions for a schedule of games. |
| [nba_reml_rapm](additional/models-and-calculators-3.md#nba_reml_rapm) | Season impact rating: RAPM shrunk toward a box prior, offense and defense penalties chosen by REML. |
| [nba_rookie_projection](additional/models-and-calculators-3.md#nba_rookie_projection) | Project rookie/sophomore value by composing draft x aging x availability. |
| [nba_spm](additional/models-and-calculators-3.md#nba_spm) | Apply fitted SPM coefficients to per-100 box features -> OSPM/DSPM/SPM. |
| [nba_team_ratings](additional/models-and-calculators-3.md#nba_team_ratings) | Opponent-adjusted team ratings (AdjOffRtg/AdjDefRtg/AdjNet/AdjPace), as-of-date aware. |
| [nba_war](additional/models-and-calculators-3.md#nba_war) | Points-above-replacement -> wins for each player. |
| [ot_adjusted_seconds](additional/models-and-calculators-3.md#ot_adjusted_seconds) | Tag overtime rows and give them the overtime period clock as their seconds remaining. |
| [player_game_rates](additional/models-and-calculators-3.md#player_game_rates) | True-shooting and usage for every boxed player of one game. |
| [predict_margin](additional/models-and-calculators-3.md#predict_margin) | Expected home-minus-away margin from two adjusted net ratings. |
| [predict_total](additional/models-and-calculators-3.md#predict_total) | Expected total points from adjusted ratings and paces. |
| [raw_game_efficiency](additional/models-and-calculators-3.md#raw_game_efficiency) | Per-team, per-game possessions + raw offensive/defensive rating. |
| [render_report](additional/models-and-calculators-3.md#render_report) | Render a `ValidationReport` as a human-readable markdown validation card. |
| [score_win_prob](additional/models-and-calculators-3.md#score_win_prob) | Per-play home win probability, its play-to-play delta, and the prior, for one game. |
| [season_game_excitement](additional/models-and-calculators-3.md#season_game_excitement) | Every game's excitement index, lead changes and biggest swing, ranked within its season. |
| [season_gram](additional/models-and-calculators-3.md#season_gram) | The `SeasonGram` of a possession frame (rows with a full lineup). |
| [shot_coordinates](additional/models-and-calculators-3.md#shot_coordinates) | Shot locations in the court frame the Game on Paper hex chart uses. |
| [team_game_rates](additional/models-and-calculators-3.md#team_game_rates) | Four factors, possessions, ratings and pace for both teams of one game. |
| [train_box_prior](additional/models-and-calculators-3.md#train_box_prior) | Possession-weighted ridge of the career target on the box-prior features, cross-fit by player. |
| [train_spm](additional/models-and-calculators-3.md#train_spm) | Ridge-fit box features onto `o_rapm` and `d_rapm` (two regressions). |
| [validate_model](additional/models-and-calculators-3.md#validate_model) | Run the selected oracles and assemble a `ValidationReport`. |
| [walk_forward](additional/models-and-calculators-3.md#walk_forward) | Oracle 6: time-ordered "predict tomorrow" retrodiction. |
| [win_prob_from_margin](additional/models-and-calculators-3.md#win_prob_from_margin) | Home win probability from an expected margin (normal-CDF closed form). |

## Analytics

| Function | Summary |
|---|---|
| [add_ctg_shot_zones](additional/analytics.md#add_ctg_shot_zones) | Append CTG's shot-location zone (`ctg_shot_zone`) to an enhanced PBP frame. |
| [add_play_context](additional/analytics.md#add_play_context) | Build possessions and enrich them with the full CTG play-context surface. |
| [add_possession_flags](additional/analytics.md#add_possession_flags) | Append `seconds_to_first_shot`, `is_transition` and `is_garbage_time` to possessions. |
| [add_start_type_detail](additional/analytics.md#add_start_type_detail) | Append the full pbpstats start-type taxonomy to a possession frame. |
| [add_transition](additional/analytics.md#add_transition) | Flag possessions that started in transition, and time their initial play. |
| [box_features](additional/analytics.md#box_features) | Aggregate per-player per-100-possession box features over a set of games. |
| [build_play_context_shots](additional/analytics.md#build_play_context_shots) | Build the per-shot frame carrying CTG's play context. |
| [build_possession_shooting](additional/analytics.md#build_possession_shooting) | Build the per-shooter companion frame from an enhanced play-by-play DataFrame. |
| [clutch_delta](additional/analytics.md#clutch_delta) | Clutch net-rating delta vs a full-game baseline, per (season, team_id). |
| [flag_garbage_time](additional/analytics.md#flag_garbage_time) | Flag CTG garbage time (excluded from CTG stats by default). |
| [flag_heave_possessions](additional/analytics.md#flag_heave_possessions) | Flag CTG's "projected heave possessions" (excluded from CTG stats by default). |
| [hoopshype_salaries](additional/analytics.md#hoopshype_salaries) | League-wide NBA player salaries from HoopsHype. |
| [lineup_play_context](additional/analytics.md#lineup_play_context) | Roll possessions up into a per-5-man-lineup Play-Context table. |
| [make_prob_by_context](additional/analytics.md#make_prob_by_context) | Marginal FG% tables by defender distance and by shot clock. |
| [make_prob_joint](additional/analytics.md#make_prob_joint) | Independence-combined defender x shot-clock make probability. |
| [nba_availability](additional/analytics.md#nba_availability) | Project games-available % for a season (or seasons) from career GP history. |
| [nba_box_logs](additional/analytics.md#nba_box_logs) | Fetch per-player and per-team game logs for a season (bulk, one call each). |
| [nba_coach_careers](additional/analytics.md#nba_coach_careers) | Head-coach careers over `seasons`: `nba_coach_seasons` summed per coach, rates recomputed. |
| [nba_coach_seasons](additional/analytics-2.md#nba_coach_seasons) | Head-coach seasons: W-L, ratings and four factors over the games each coach was on the bench for. |
| [nba_cup_sim](additional/analytics-2.md#nba_cup_sim) | NBA Cup sim as of a date: odds to win the group, take a wild card, reach each round and win. |
| [nba_cup_standings](additional/analytics-2.md#nba_cup_standings) | NBA Cup Group Play standings as of a date, with the official tiebreak order and wild cards. |
| [nba_draft_lottery_odds](additional/analytics-2.md#nba_draft_lottery_odds) | Exact draft-lottery odds by pre-lottery slot: the chance each slot gets each pick. |
| [nba_epv_lineup_games](additional/analytics-2.md#nba_epv_lineup_games) | Per five-man unit and game: the EPV added with the unit on the floor. |
| [nba_epv_player_games](additional/analytics-2.md#nba_epv_player_games) | Per player and game: EPV added on the player's own actions and on-court EPV +/-. |
| [nba_epv_plays](additional/analytics-2.md#nba_epv_plays) | EPV and EPV added on every play-by-play event inside a possession. |
| [nba_epv_team_games](additional/analytics-2.md#nba_epv_team_games) | Per team and game: EPV added on offense and allowed on defense. |
| [nba_foul_drawing](additional/analytics-2.md#nba_foul_drawing) | Expected FTA + residual foul-drawing skill from Synergy play-type mix. |
| [nba_game_flow](additional/analytics-2.md#nba_game_flow) | Game flow per team and game: runs, droughts, kills, lead changes, time leading and game control. |
| [nba_game_lineups_used](additional/analytics-2.md#nba_game_lineups_used) | The lineups used: per five-man unit and game, minutes, possessions, +/- and ratings. |
| [nba_game_on_off](additional/analytics-2.md#nba_game_on_off) | In-game on/off: per player and game, the team with him on the floor and the rest of its game. |
| [nba_game_player_impact](additional/analytics-2.md#nba_game_player_impact) | The game impact box: per player and game, minutes, on-court possessions, +/- and ratings. |
| [nba_game_stints](additional/analytics-2.md#nba_game_stints) | Game stints: per player and game, every stretch on the floor with its clock, points and possessions. |
| [nba_game_units](additional/analytics-2.md#nba_game_units) | In-game units: per 1- to 5-man set of teammates and game, minutes, ratings, four factors and shot zones. |
| [nba_impact_rows](additional/analytics-2.md#nba_impact_rows) | Map the stats.nba.com releases onto the game-impact row frame. |
| [nba_l2m](additional/analytics-3.md#nba_l2m) | Fetch and parse an NBA Last Two Minute report from official.nba.com. |
| [nba_l2m_games](additional/analytics-3.md#nba_l2m_games) | Fetch the list of games with Last Two Minute reports for an NBA season. |
| [nba_lineup_count_rows](additional/analytics-3.md#nba_lineup_count_rows) | Map `nba_stats_possessions` rows onto the lineup-seasons count frame. |
| [nba_lineup_seasons](additional/analytics-3.md#nba_lineup_seasons) | Lineup seasons: 2-, 3- and 5-man units with possessions, minutes, ratings and four factors. |
| [nba_neutral_site_games](additional/analytics-3.md#nba_neutral_site_games) | The curated NBA / WNBA neutral-site games: played abroad, or NBA Cup knockout games in Las Vegas. |
| [nba_official_seasons](additional/analytics-3.md#nba_official_seasons) | NBA official season profiles: games, fouls and FTA per game, pace and home splits. |
| [nba_play_context](additional/analytics-3.md#nba_play_context) | Fetch one game and return its possessions with the full CTG play-context surface. |
| [nba_play_style_features](additional/analytics-3.md#nba_play_style_features) | The play-style clustering features, one row per qualifying player- or team-season. |
| [nba_player_ages](additional/analytics-3.md#nba_player_ages) | Per-player age for a season (bulk), for the DARKO aging curve. |
| [nba_player_factor_rapm](additional/analytics-3.md#nba_player_factor_rapm) | Player RAPM split into Dean Oliver's four factors, offense and defense, per player-season. |
| [nba_player_identity](additional/analytics-3.md#nba_player_identity) | Human-readable identity for every player in a season's box logs. |
| [nba_player_on_off](additional/analytics-4.md#nba_player_on_off) | Player on/off: each player's team with him on the floor and without him, per team-season. |
| [nba_player_play_styles](additional/analytics-4.md#nba_player_play_styles) | Offensive and defensive play-style archetypes per player-season (BB-D20). |
| [nba_player_positions](additional/analytics-4.md#nba_player_positions) | Fetch league-wide listed positions for a season as numeric 1-5. |
| [nba_player_props](additional/analytics-4.md#nba_player_props) | Per-player expected prop lines + team pace projection for a matchup. |
| [nba_player_shot_zones](additional/analytics-4.md#nba_player_shot_zones) | Season shot profile by zone per player: rim / mid / corner 3 / above-the-break 3. |
| [nba_player_war](additional/analytics-4.md#nba_player_war) | WAR and VORP per player-season, from our BPM, RAPM and BOBAN (regular season). |
| [nba_player_wowy](additional/analytics-4.md#nba_player_wowy) | WOWY pairs: a team's season with two teammates together, apart and both off, per pair. |
| [nba_playtype_ratings](additional/analytics-4.md#nba_playtype_ratings) | Season play-type-adjusted offensive/defensive team ratings. |
| [nba_possession_rate_rows](additional/analytics-4.md#nba_possession_rate_rows) | Map `nba_stats_possessions` rows onto the team-season-rates possession frame. |
| [nba_rated_priors](additional/analytics-4.md#nba_rated_priors) | Rated pregame home win probability for every ESPN game of a season, as of each game's date. |
| [nba_ratings_panel](additional/analytics-4.md#nba_ratings_panel) | Player-ratings-through-date long panel: one row per (player_id, date). |
| [nba_raw_store_season_frame](additional/analytics-4.md#nba_raw_store_season_frame) | Read a committed SEASON-LEVEL capture from the raw store, parsed to a frame. |
| [nba_referee_assignments](additional/analytics-4.md#nba_referee_assignments) | Fetch and parse NBA referee assignments for a given date from official.nba.com. |
| [nba_score_rows](additional/analytics-4.md#nba_score_rows) | Map the stats.nba.com play-by-play onto the game-flow score rows. |
| [nba_season_sim](additional/analytics-5.md#nba_season_sim) | Season sim as of a date: projected W-L, division, seed, play-in, playoff and lottery odds. |
| [nba_season_standings](additional/analytics-5.md#nba_season_standings) | Standings with the league's tiebreakers from completed regular-season games. |
| [nba_shot_value](additional/analytics-5.md#nba_shot_value) | One-call shot-value spine: fetch, score, and run all five models. |
| [nba_shot_value_lineups](additional/analytics-5.md#nba_shot_value_lineups) | Scored per-shot frame for one 5-man lineup (`shotchartlineupdetail`). |
| [nba_situation_rows](additional/analytics-5.md#nba_situation_rows) | Map the stats.nba.com releases onto the situational-splits possession frame. |
| [nba_situational_lineup](additional/analytics-5.md#nba_situational_lineup) | Five-man unit splits by situation: the team splits over the possessions each unit started. |
| [nba_situational_player](additional/analytics-5.md#nba_situational_player) | Player on-floor splits by situation: the team splits over the possessions each player started on the floor. |
| [nba_situational_team](additional/analytics-5.md#nba_situational_team) | Team splits by situation: clutch, period, half, garbage time, score, venue, opponent, play type, chance. |
| [nba_stint_rows](additional/analytics-5.md#nba_stint_rows) | Map the stats.nba.com releases onto the stint-row frame. |
| [nba_team_clutch](additional/analytics-5.md#nba_team_clutch) | Opponent-agnostic clutch skill (shrunk clutch net-rating delta) per team. |
| [nba_team_luck](additional/analytics-5.md#nba_team_luck) | Luck and close-game record: Pythagorean expected wins vs actual wins, and close-game W-L. |
| [nba_team_play_styles](additional/analytics-5.md#nba_team_play_styles) | Defensive play-style archetypes per team-season (BB-D20); team offense has none (see the module). |
| [nba_team_season_flow](additional/analytics-5.md#nba_team_season_flow) | Game flow per team and season, with ranks, percentiles and `_n`. |
| [nba_team_season_rates](additional/analytics-6.md#nba_team_season_rates) | Season team rates from possessions: ORtg/DRtg/net, pace and the four factors, with splits. |
| [nba_team_shot_zones](additional/analytics-6.md#nba_team_shot_zones) | Season shot profile by zone per team, for and allowed: rim / mid / corner 3 / above-the-break 3. |
| [nba_team_strength](additional/analytics-6.md#nba_team_strength) | Team strength from possessions: adjusted ratings, SOS, SOV, pace and the four factors. |
| [nba_tracking_drive_value](additional/analytics-6.md#nba_tracking_drive_value) | Drive value over expected + rim-pressure, per player-season. |
| [nba_tracking_pass_value](additional/analytics-6.md#nba_tracking_pass_value) | Expected-assists / passer value: `ast_oe` per player-season. |
| [nba_tracking_reb_oe](additional/analytics-6.md#nba_tracking_reb_oe) | Rebounding-over-expected: `reb_oe` plus OREB/DREB splits, per player-season. |
| [nba_tracking_rim_protect_value](additional/analytics-6.md#nba_tracking_rim_protect_value) | Rim-protection / shot-defend points-saved over expected, per player-season. |
| [nba_tracking_shot_diet_value](additional/analytics-7.md#nba_tracking_shot_diet_value) | Catch-&-shoot vs pull-up points-over-expected, per player-season. |
| [nba_tracking_touch_value](additional/analytics-7.md#nba_tracking_touch_value) | Touch / possession-time value over expected, per player-season. |
| [nbadraft_mock_draft](additional/analytics-7.md#nbadraft_mock_draft) | The current consensus mock draft from NBADraft.net. |
| [player_play_context](additional/analytics-7.md#player_play_context) | Per-player offensive On/Off Play-Context table (CTG's On/Off page, offense half). |
| [player_rates](additional/analytics-7.md#player_rates) | Per-player per-minute rate stats from box logs. |
| [players_on_court_from_pbp](additional/analytics-7.md#players_on_court_from_pbp) | Reconstruct the 5-on-5 on-court lineup from pbp subs + boxscore starters. |
| [players_on_court_from_quarter_boxscores](additional/analytics-7.md#players_on_court_from_quarter_boxscores) | Reconstruct the 5-on-5 on-court lineup, seeding each period exactly where possible. |
| [players_on_court_from_rotation](additional/analytics-7.md#players_on_court_from_rotation) | Reconstruct the 5-on-5 on-court lineup via the rotation (gamerotation) algorithm. |
| [prob_over](additional/analytics-7.md#prob_over) | Probability a stat finishes strictly above `line`. |
| [project_player_line](additional/analytics-7.md#project_player_line) | Project a player's expected counting line from per-minute rates. |
| [prop_distribution](additional/analytics-7.md#prop_distribution) | Distribution family + parameters for a projected stat mean. |
| [ratings_as_of](additional/analytics-7.md#ratings_as_of) | Fit `model` on every possession dated on or before `asof` and return ratings. |
| [rotowire_injuries](additional/analytics-7.md#rotowire_injuries) | The current NBA injury report from RotoWire. |
| [score_shot_xpoints](additional/analytics-7.md#score_shot_xpoints) | Score each shot with expected points from the league-average baseline. |
| [shooter_talent](additional/analytics-7.md#shooter_talent) | Regressed shooter true-talent: make%-above-expected, shrunk to the mean. |
| [shot_selection_quality](additional/analytics-7.md#shot_selection_quality) | Player shot-selection quality: mean expected value vs the league mean. |
| [shrink_clutch](additional/analytics-7.md#shrink_clutch) | Empirical-Bayes / James-Stein shrinkage of `clutch_delta` toward zero. |
| [spotrac_team_cap](additional/analytics-7.md#spotrac_team_cap) | Team salary-cap allocations from Spotrac. |
| [starters_on_court_counts](additional/analytics-7.md#starters_on_court_counts) | Count, per possession, how many **starters** are on the floor across BOTH teams. |
| [team_pace_projection](additional/analytics-7.md#team_pace_projection) | Expected possessions for a matchup (Phase-3 `expected_possessions`). |
| [team_play_context](additional/analytics-7.md#team_play_context) | Roll possessions up into CTG's team Play-Context table. |
| [xpoints_baseline](additional/analytics-7.md#xpoints_baseline) | League-average FG% baseline table keyed by the three shot-zone columns. |
| [zone_value_map](additional/analytics-7.md#zone_value_map) | Per-player per-zone value map: points and expected points per shot. |

## Dates and seasons

| Function | Summary |
|---|---|
| [year_to_season](additional/dates-and-seasons.md#year_to_season) | Convert a season START year (e.g. 2023) to the NBA's hyphenated label |

## IDs and crosswalks

| Function | Summary |
|---|---|
| [nba_crosswalks_from_archive](additional/ids-and-crosswalks.md#nba_crosswalks_from_archive) | Build the NBA team, schedule and player crosswalks for a past season, offline. |
| [nba_player_crosswalk](additional/ids-and-crosswalks.md#nba_player_crosswalk) | Build the NBA cross-source player crosswalk (ESPN / NBA Stats / Fox). |
| [nba_schedule_crosswalk](additional/ids-and-crosswalks.md#nba_schedule_crosswalk) | Build the NBA cross-source schedule crosswalk (ESPN / NBA Stats / Fox). |
| [nba_team_crosswalk](additional/ids-and-crosswalks.md#nba_team_crosswalk) | Build the NBA cross-source team crosswalk (ESPN / NBA Stats / Fox). |
