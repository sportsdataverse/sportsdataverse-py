---
title: WBB — additional Python functions
sidebar_label: Additional functions
description: "WBB — additional Python functions — additional functions in sdv-py, the SportsDataverse Python package."
sidebar_position: 50
---
# WBB — additional Python functions

Hand-written wrappers, loaders, and helpers in `sportsdataverse.wbb`
not covered by the generated API-endpoint reference above.

## ESPN

| Function | Summary |
|---|---|
| [espn_wbb_game_officials](additional/espn.md#espn_wbb_game_officials) | Pull the officials assigned to a women's-college-basketball game. |
| [espn_wbb_game_rosters](additional/espn.md#espn_wbb_game_rosters) | espn_wbb_game_rosters() - Pull the game by id. |
| [espn_wbb_pbp](additional/espn.md#espn_wbb_pbp) | espn_wbb_pbp() - Pull the game by id. Data from API endpoints - `womens-college-basketball/playbyplay`, |
| [espn_wbb_player_stats](additional/espn.md#espn_wbb_player_stats) | Pull a women's-college-basketball athlete's ESPN **season** stat line. |
| [espn_wbb_schedule](additional/espn.md#espn_wbb_schedule) | espn_wbb_schedule - look up the women's college basketball schedule for a given season |
| [espn_wbb_team_stats](additional/espn.md#espn_wbb_team_stats) | Pull ESPN team season stats for a women's-college-basketball team. |
| [espn_wbb_teams](additional/espn.md#espn_wbb_teams) | espn_wbb_teams - look up the women's college basketball teams |
| [scoreboard_event_parsing](additional/espn.md#scoreboard_event_parsing) | _No description available._ |

## stats.ncaa.org

| Function | Summary |
|---|---|
| [BadLineupClump](additional/stats-ncaa-org.md#BadLineupClump) | A run of consecutive bad :class:`~sportsdataverse.mbb.mbb_ncaa_models |
| [FieldAverage](additional/stats-ncaa-org.md#FieldAverage) | League average + estimated HCA for one stat field (`ts:620-625`). |
| [GameBreakEvent](additional/stats-ncaa-org.md#GameBreakEvent) | A break in play (timeout, end of period, etc.) short of the end of |
| [GameEndEvent](additional/stats-ncaa-org.md#GameEndEvent) | The end of the game (`Model.GameEndEvent`, `ExtractorUtils.scala:878-881`). |
| [IterationResult](additional/stats-ncaa-org.md#IterationResult) | Return of `run_iterative_adjustment_with_hca` (`ts:314-317`). |
| [LineupBuildingState](additional/stats-ncaa-org.md#LineupBuildingState) | State for building raw lineup data across a fold over play-by-play |
| [NcaaFetchConfig](additional/stats-ncaa-org.md#NcaaFetchConfig) | Runtime configuration for the stats.ncaa.org fetch layer. |
| [NcaaFetcher](additional/stats-ncaa-org.md#NcaaFetcher) | Cache-first stats.ncaa.org fetcher, proxy-bound per the binding directive. |
| [OtherOpponentEvent](additional/stats-ncaa-org.md#OtherOpponentEvent) | A non-sub event belonging to the opponent (`Model.OtherOpponentEvent`, |
| [OtherTeamEvent](additional/stats-ncaa-org.md#OtherTeamEvent) | A non-sub event belonging to the team under analysis |
| [ParseError](additional/stats-ncaa-org.md#ParseError) | A parse-time error (`ParseError`, `ParseError.scala:9`). |
| [PbpBuilders](additional/stats-ncaa-org.md#PbpBuilders) | One version-era's HTML finder functions (``PlayByPlayParser |
| [PeekableIterator](additional/stats-ncaa-org.md#PeekableIterator) | A stateful iterator with one element of look-ahead, the Python |
| [PossessionSplits](additional/stats-ncaa-org.md#PossessionSplits) | Home/away/neutral possession totals for one team (`ts:143-152`). |
| [ScheduleBuilders](additional/stats-ncaa-org.md#ScheduleBuilders) | One version-era's HTML finder functions (``TeamScheduleParser |
| [ShotEventBuilders](additional/stats-ncaa-org.md#ShotEventBuilders) | The HTML finder-function table (`ShotEventParser.base_builders`, |
| [ShotMapDimensions](additional/stats-ncaa-org.md#ShotMapDimensions) | SVG shot-map pixel<->feet conversion constants, taken from the |
| [StrengthAdjustedResult](additional/stats-ncaa-org.md#StrengthAdjustedResult) | The compute output of `build_strength_adjusted_stats`. |
| [SubInEvent](additional/stats-ncaa-org.md#SubInEvent) | A player subs into the game (`Model.SubInEvent`, `ExtractorUtils.scala:850-853`). |
| [SubOutEvent](additional/stats-ncaa-org.md#SubOutEvent) | A player subs out of the game (`Model.SubOutEvent`, `ExtractorUtils.scala:854-857`). |
| [TeamStrengthAdjusted](additional/stats-ncaa-org.md#TeamStrengthAdjusted) | One team's raw / adjusted / HCA-adjusted rates (`ts:642-648`). |
| [ValidationError](additional/stats-ncaa-org.md#ValidationError) | The 3 ways a lineup can be declared invalid, in Scala declaration |
| [add_missing_players](additional/stats-ncaa-org.md#add_missing_players) | Back-fills a clump whose lineups carry TOO FEW players |
| [add_stats_to_lineups](additional/stats-ncaa-org.md#add_stats_to_lineups) | Enrich a lineup with play-by-play stats for both team and opponent |
| [alias_combos](additional/stats-ncaa-org.md#alias_combos) | Pair each of `combos`' three name variants with a shared alias |
| [analyze_and_fix_clumps](additional/stats-ncaa-org.md#analyze_and_fix_clumps) | Runs the full self-healing fixer pipeline over one bad-lineup clump |
| [attr_regex_filter](additional/stats-ncaa-org.md#attr_regex_filter) | JSoup `[attr~=regex]`: candidates whose `attr` value matches |
| [build_available_team_list](additional/stats-ncaa-org.md#build_available_team_list) | Builds a per-conference team-index JSON fragment for |
| [build_base_event](additional/stats-ncaa-org.md#build_base_event) | Fills in the fields a shot event can borrow straight from the |
| [build_lineup_cli_array](additional/stats-ncaa-org.md#build_lineup_cli_array) | Builds the per-conference team array for `lineups-cli.sh` files |
| [build_lineup_id](additional/stats-ncaa-org.md#build_lineup_id) | Builds a lineup id from a list of players (`ExtractorUtils.scala:602-606`): |
| [build_new_player_list](additional/stats-ncaa-org.md#build_new_player_list) | Builds a player list from the previous (or current, if pre-initialized) |
| [build_partial_lineup_list](additional/stats-ncaa-org.md#build_partial_lineup_list) | Converts a stream of partially parsed events into a list of lineup |
| [build_player_code](additional/stats-ncaa-org.md#build_player_code) | Build a short player code from a name, in any of the NCAA formats |
| [build_strength_adjusted_stats](additional/stats-ncaa-org.md#build_strength_adjusted_stats) | Run the full strength-adjustment compute over a team list. |
| [build_sub_error](additional/stats-ncaa-org.md#build_sub_error) | Build a location-less `ParseError` from id fragments |
| [cached_path](additional/stats-ncaa-org.md#cached_path) | Return the on-disk cache file path for *path*, without touching it. |
| [categorize_bad_lineups](additional/stats-ncaa-org.md#categorize_bad_lineups) | Aggregates bad lineup events for display, by clump-leader player count |
| [clump_bad_lineups](additional/stats-ncaa-org.md#clump_bad_lineups) | Groups consecutive bad lineup events into `BadLineupClump`\ s |
| [combos](additional/stats-ncaa-org.md#combos) | Generate the three name-string variants NCAA sources use for one |
| [compute_league_averages_from_per_game](additional/stats-ncaa-org.md#compute_league_averages_from_per_game) | Possession-weighted league means per field (`computeLeagueAveragesFromPerGame`, `ts:189-221`). |
| [compute_opponent_strengths](additional/stats-ncaa-org.md#compute_opponent_strengths) | Schedule-weighted opponent strength per field (`computeOpponentStrengths`, `ts:253-299`). |
| [compute_possession_splits](additional/stats-ncaa-org.md#compute_possession_splits) | Home/away/neutral possession totals for a team (`computePossessionSplits`, `ts:154-186`). |
| [create_lineup_data](additional/stats-ncaa-org.md#create_lineup_data) | Combines the different methods to build a set of lineup events |
| [create_player_events](additional/stats-ncaa-org.md#create_player_events) | Split a lineup event into one :class:`~sportsdataverse.mbb |
| [create_shot_event_data](additional/stats-ncaa-org.md#create_shot_event_data) | Parses a game page's SVG shot map into a list of :class:`~sportsdataverse |
| [duration_from_period](additional/stats-ncaa-org.md#duration_from_period) | The game duration (minutes elapsed) once `period` has completed |
| [enrich_and_reverse_game_events](additional/stats-ncaa-org.md#enrich_and_reverse_game_events) | Inserts game-break events and turns descending per-row times into |
| [enrich_lineup](additional/stats-ncaa-org.md#enrich_lineup) | Populate `pts`/`plus_minus` from the score delta, then run the |
| [enrich_shot_events_with_pbp](additional/stats-ncaa-org.md#enrich_shot_events_with_pbp) | Enrich each shot with its play-by-play event + on-floor lineup |
| [enrich_stats](additional/stats-ncaa-org-2.md#enrich_stats) | Fold a lineup's raw events into a counting-stat tree (``protected def |
| [ensure_ev_uniqueness](additional/stats-ncaa-org-2.md#ensure_ev_uniqueness) | Nudge each event's `min` by a tiny per-index delta so truly |
| [extract_player_from_ev](additional/stats-ncaa-org-2.md#extract_player_from_ev) | Resolve the player named in `pbp_event` to a |
| [field_keys](additional/stats-ncaa-org-2.md#field_keys) | Off/def stat-key names for a field (`fieldKeys`, `ts:77-79`). |
| [filter_matching_own](additional/stats-ncaa-org-2.md#filter_matching_own) | JSoup `:matchesOwn(regex)` applied to an already-computed candidate |
| [find_lineup](additional/stats-ncaa-org-2.md#find_lineup) | Find the lineup (stint) event on the floor for `shot` |
| [find_missing_subs](additional/stats-ncaa-org-2.md#find_missing_subs) | Trims a clump whose lineups carry TOO MANY players by identifying the |
| [find_pbp_clump](additional/stats-ncaa-org-2.md#find_pbp_clump) | Gather every play-by-play shot/assist event sharing `shot_time` |
| [fix_combos](additional/stats-ncaa-org-2.md#fix_combos) | Pair each of `combos`' three name variants with a shared |
| [fix_possible_score_swap_bug](additional/stats-ncaa-org-2.md#fix_possible_score_swap_bug) | Undo a rare NCAA data bug where the scores get transposed |
| [get_ascending_time](additional/stats-ncaa-org-2.md#get_ascending_time) | Converts the descending in-period clock time to an ascending |
| [get_box_lineup](additional/stats-ncaa-org-2.md#get_box_lineup) | Gets the boxscore lineup from the HTML page (``BoxscoreParser |
| [get_config](additional/stats-ncaa-org-2.md#get_config) | Return the live `NcaaFetchConfig` singleton. |
| [get_game_weight](additional/stats-ncaa-org-2.md#get_game_weight) | Weight for one game/field/side (`getGameWeight`, `ts:119-140`). |
| [get_neutral_games](additional/stats-ncaa-org-2.md#get_neutral_games) | Extracts the set of neutral/away-marked game dates from a saved NCAA |
| [get_per_game_raw](additional/stats-ncaa-org-2.md#get_per_game_raw) | Per-game raw shooting rate from one opponent row (`getPerGameRaw`, `ts:82-116`). |
| [get_sorted_pbp_events](additional/stats-ncaa-org-2.md#get_sorted_pbp_events) | Handy util to return the play-by-play events in chronological order, |
| [get_team_raw_from_per_game](additional/stats-ncaa-org-2.md#get_team_raw_from_per_game) | A team's field rate as the weighted mean of its per-game raws (`getTeamRawFromPerGame`, `ts:224-250`). |
| [get_team_triples](additional/stats-ncaa-org-2.md#get_team_triples) | Extracts `(team, NCAA id, conference)` triples from a saved NCAA |
| [get_unified_ncaa_id](additional/stats-ncaa-org-2.md#get_unified_ncaa_id) | Gets a player's lowest cross-season NCAA id from a saved player page |
| [handle_common_sub_bug](additional/stats-ncaa-org-2.md#handle_common_sub_bug) | Fixes the "2-in-1-out then a compensating 1-out" substitution bug |
| [inject_starting_lineup_into_box](additional/stats-ncaa-org-2.md#inject_starting_lineup_into_box) | Infer the starting five and reorder the box-score roster so they lead |
| [inject_validated_players](additional/stats-ncaa-org-2.md#inject_validated_players) | Validates box players against the roster (if available) and any |
| [is_cached](additional/stats-ncaa-org-2.md#is_cached) | Return whether *path* already has a cache file on disk. |
| [is_end_of_game_fouling_vs_fastbreak](additional/stats-ncaa-org-2.md#is_end_of_game_fouling_vs_fastbreak) | Check for intentional fouling to prolong the game, specifically so it |
| [is_gen2](additional/stats-ncaa-org-2.md#is_gen2) | Detect the new/"gen2" NCAA event format (`EventUtils.is_gen2`, |
| [is_scramble](additional/stats-ncaa-org-2.md#is_scramble) | Figure out if (each event of) the current clump is part of a |
| [is_team_shooting_left_to_start](additional/stats-ncaa-org-2.md#is_team_shooting_left_to_start) | Infers which side of the SVG court the team under analysis shoots |
| [is_women_game](additional/stats-ncaa-org-2.md#is_women_game) | Infers men's vs. women's game from timing evidence |
| [jsoup_text](additional/stats-ncaa-org-2.md#jsoup_text) | JSoup `Element.text()`: all descendant text, whitespace-collapsed. |
| [load_proxybonanza_pool](additional/stats-ncaa-org-2.md#load_proxybonanza_pool) | Resolve a ProxyBonanza package into a list of `http://login:pass@ip:port` URLs. |
| [matching_player](additional/stats-ncaa-org-2.md#matching_player) | Whether the player in `pbp_event` matches `shot`'s shooter |
| [misspellings](additional/stats-ncaa-org-2.md#misspellings) | Team-scoped misspelling map, falling back to the generic map |
| [ncaa_wbb_box_scores](additional/stats-ncaa-org-2.md#ncaa_wbb_box_scores) | Scrape WBB per-player box scores (wbigballR `get_box_scores`/`scrape_box`). |
| [ncaa_wbb_date_games](additional/stats-ncaa-org-2.md#ncaa_wbb_date_games) | Discover every NCAA WBB game played on a date (wbigballR `get_date_games`). |
| [ncaa_wbb_join_pbp_shots](additional/stats-ncaa-org-2.md#ncaa_wbb_join_pbp_shots) | Attach WBB chart shots onto the pbp frame (pure delegation). |
| [ncaa_wbb_player_stats](additional/stats-ncaa-org-2.md#ncaa_wbb_player_stats) | Aggregate WBB play-by-play into per-player box stats (wbigballR `get_player_stats`). |
| [ncaa_wbb_possessions](additional/stats-ncaa-org-2.md#ncaa_wbb_possessions) | Aggregate WBB play-by-play into one row per possession (wbigballR `get_possessions`). |
| [ncaa_wbb_shot_locations](additional/stats-ncaa-org-2.md#ncaa_wbb_shot_locations) | Scrape WBB shot locations for one or more games. |
| [ncaa_wbb_team_roster](additional/stats-ncaa-org-2.md#ncaa_wbb_team_roster) | Scrape a women's team roster from stats.ncaa.org. |
| [ncaa_wbb_team_schedule](additional/stats-ncaa-org-2.md#ncaa_wbb_team_schedule) | Scrape a women's team's season schedule from stats.ncaa.org. |
| [ncaa_wbb_team_stats](additional/stats-ncaa-org-2.md#ncaa_wbb_team_stats) | Aggregate WBB play-by-play into per-team game stats (wbigballR `get_team_stats`). |
| [phase1_shot_event_enrichment](additional/stats-ncaa-org-2.md#phase1_shot_event_enrichment) | The court-geometry enrichment pass: ascending time, coordinate |
| [playwright_transport](additional/stats-ncaa-org-2.md#playwright_transport) | Build the **suggested** stats.ncaa.org game-detail scraping transport. |
| [remove_diacritics](additional/stats-ncaa-org-2.md#remove_diacritics) | Strip diacritical marks, e.g. `"Juhász"` -> `"Juhasz"` |
| [reorder_and_reverse](additional/stats-ncaa-org-2.md#reorder_and_reverse) | Orders same-minute play-by-play events so subs never enclose the plays |
| [reset_config](additional/stats-ncaa-org-2.md#reset_config) | Reset the active config to its env-var-derived defaults. |
| [right_kind_of_shot](additional/stats-ncaa-org-2.md#right_kind_of_shot) | Whether `pbp_event`'s shot type is compatible with `shot`'s |
| [run_iterative_adjustment_with_hca](additional/stats-ncaa-org-2.md#run_iterative_adjustment_with_hca) | KenPom-style SoS + HCA fixed-point solver (`runIterativeAdjustmentWithHCA`, `ts:306-527`). |
| [select_contains](additional/stats-ncaa-org-2.md#select_contains) | JSoup `root.select(sel + ":contains(text)")`: candidates whose full |
| [select_matching](additional/stats-ncaa-org-2.md#select_matching) | JSoup `root.select(sel + ":matches(regex)")`: candidates whose full |
| [select_matching_own](additional/stats-ncaa-org-2.md#select_matching_own) | JSoup `root.select(sel + ":matchesOwn(regex)")`: candidates whose |
| [shot_js_to_html](additional/stats-ncaa-org-2.md#shot_js_to_html) | Converts client-side `addShot(...)` JS calls into parseable |
| [start_time_from_period](additional/stats-ncaa-org-3.md#start_time_from_period) | The game-clock time (minutes elapsed) a period starts at |
| [sum_event_stats](additional/stats-ncaa-org-3.md#sum_event_stats) | Field-wise add two :class:`~sportsdataverse.mbb.mbb_ncaa_models |
| [sum_shot_infos](additional/stats-ncaa-org-3.md#sum_shot_infos) | Field-wise sum a list of :class:`~sportsdataverse.mbb.mbb_ncaa_models |
| [td_at](additional/stats-ncaa-org-3.md#td_at) | JSoup `row >?> element("td:eq(n)")`: the `n`-th `<td>` child. |
| [transform_shot_location](additional/stats-ncaa-org-3.md#transform_shot_location) | Transforms a raw SVG pixel location into feet from the basket, always |
| [update_config](additional/stats-ncaa-org-3.md#update_config) | Update the active config in place. |
| [validate_box_score](additional/stats-ncaa-org-3.md#validate_box_score) | Checks there are no duplicates in the lineup (``BoxscoreParser |
| [validate_lineup](additional/stats-ncaa-org-3.md#validate_lineup) | Flags a lineup stint as internally inconsistent, via 3 independent |

## Fox Sports API

| Function | Summary |
|---|---|
| [fox_wbb_boxscore](additional/fox-sports-api.md#fox_wbb_boxscore) | WBB boxscore (long: one row per player-stat). |
| [fox_wbb_event_matchup](additional/fox-sports-api.md#fox_wbb_event_matchup) | Fox Sports wcbk pregame team-stat comparison (one row per stat). |
| [fox_wbb_event_recap](additional/fox-sports-api.md#fox_wbb_event_recap) | Fox Sports wcbk postgame top performers (one row per player). |
| [fox_wbb_event_standings](additional/fox-sports-api.md#fox_wbb_event_standings) | Fox Sports wcbk the two teams' standings context. |
| [fox_wbb_league_conferences](additional/fox-sports-api.md#fox_wbb_league_conferences) | Fox Sports wcbk conference / group directory. |
| [fox_wbb_league_header](additional/fox-sports-api.md#fox_wbb_league_header) | Fox Sports wcbk league header (one row). |
| [fox_wbb_league_leaders](additional/fox-sports-api.md#fox_wbb_league_leaders) | WBB statistical leaders (`stats-con`); who=player\|team. |
| [fox_wbb_league_odds](additional/fox-sports-api.md#fox_wbb_league_odds) | Fox Sports wcbk league odds board (one row per team per game). |
| [fox_wbb_league_player_news](additional/fox-sports-api.md#fox_wbb_league_player_news) | Fox Sports wcbk league-wide player news feed. |
| [fox_wbb_league_polls](additional/fox-sports-api.md#fox_wbb_league_polls) | Fox Sports wcbk rankings / polls rendered as standings tables. |
| [fox_wbb_league_schedule](additional/fox-sports-api.md#fox_wbb_league_schedule) | Fox Sports wcbk league schedule nav selections. |
| [fox_wbb_league_scores](additional/fox-sports-api.md#fox_wbb_league_scores) | Fox Sports wcbk league scores nav selections. |
| [fox_wbb_league_standings](additional/fox-sports-api.md#fox_wbb_league_standings) | Fox Sports wcbk league-wide standings tables. |
| [fox_wbb_league_stat_leaders](additional/fox-sports-api.md#fox_wbb_league_stat_leaders) | Fox Sports wcbk league stats landing leaders. |
| [fox_wbb_odds](additional/fox-sports-api.md#fox_wbb_odds) | WBB game odds six-pack (spread / to-win / total per team). |
| [fox_wbb_pbp](additional/fox-sports-api.md#fox_wbb_pbp) | WBB play-by-play (one row per play; period-based). |
| [fox_wbb_scoreboard](additional/fox-sports-api.md#fox_wbb_scoreboard) | Fox Sports wcbk scoreboard nav selections (weeks / dates / groups). |
| [fox_wbb_scorechip](additional/fox-sports-api.md#fox_wbb_scorechip) | Fox Sports wcbk compact live score chip (raw dict -- live-only, uncaptured shape). |
| [fox_wbb_scores_segment](additional/fox-sports-api.md#fox_wbb_scores_segment) | Fox Sports wcbk one row per game in a scoreboard segment. |
| [fox_wbb_standings](additional/fox-sports-api.md#fox_wbb_standings) | WBB standings for a team's conference/division. |
| [fox_wbb_team_gamelog](additional/fox-sports-api.md#fox_wbb_team_gamelog) | WBB team game log (long: one row per game-stat). |
| [fox_wbb_team_header](additional/fox-sports-api.md#fox_wbb_team_header) | Fox Sports wcbk team header (one row). |
| [fox_wbb_team_roster](additional/fox-sports-api.md#fox_wbb_team_roster) | WBB team roster (one row per player). |
| [fox_wbb_team_stats](additional/fox-sports-api.md#fox_wbb_team_stats) | WBB team stat leaders by category. |
| [fox_wbb_teamnav](additional/fox-sports-api.md#fox_wbb_teamnav) | Fox Sports wcbk team directory (one row per team). |
| [fox_wbb_teams](additional/fox-sports-api.md#fox_wbb_teams) | WBB team directory (`fox_team_id` / `fox_team_name` / `fox_section`). |
| [fox_wbb_teams_all](additional/fox-sports-api.md#fox_wbb_teams_all) | Full WBB team directory by walking seed ids across conferences. |

## Her Hoop Stats

| Function | Summary |
|---|---|
| [has_herhoopstats_login](additional/her-hoop-stats.md#has_herhoopstats_login) | Whether Her Hoop Stats credentials are set in the environment. |
| [herhoopstats_login](additional/her-hoop-stats.md#herhoopstats_login) | Log into herhoopstats.com and return the authenticated session. |
| [herhoopstats_team_roster](additional/her-hoop-stats.md#herhoopstats_team_roster) | The player roster table from one Her Hoop Stats team page. |
| [herhoopstats_team_stats](additional/her-hoop-stats.md#herhoopstats_team_stats) | Every table on one Her Hoop Stats team page. |
| [herhoopstats_teams](additional/her-hoop-stats.md#herhoopstats_teams) | NCAA women's team single-season summary table. |

## Play-by-play processing

| Function | Summary |
|---|---|
| [build_athlete_identity_lookup](additional/play-by-play-processing.md#build_athlete_identity_lookup) | R `build_athlete_identity_lookup`: athlete_id -> identity from team rosters. |
| [classify_point_value](additional/play-by-play-processing.md#classify_point_value) | 2 or 3 from basket-relative geometry (arc radius + corner band). |
| [classify_zone_geometry](additional/play-by-play-processing.md#classify_zone_geometry) | Shot zone from geometry: `rim \| paint \| mid \| corner3 \| abovebreak3`. |
| [classify_zone_type](additional/play-by-play-processing.md#classify_zone_type) | Collapse a source shot-type label to `rim \| arc3 \| jump`. |
| [espn_shots_to_canonical](additional/play-by-play-processing.md#espn_shots_to_canonical) | ESPN `load_mbb_shots` frame -> the canonical shot frame. |
| [fit_espn_court_scale](additional/play-by-play-processing.md#fit_espn_court_scale) | Fit the ESPN raw-coordinate court scale: `(origin_x, origin_y, feet_per_unit)`. |
| [ncaa_wbb_game_pbp](additional/play-by-play-processing.md#ncaa_wbb_game_pbp) | Scrape one WBB game's play-by-play (wbigballR `scrape_game`, quarters fixed). |
| [ncaa_wbb_play_by_play](additional/play-by-play-processing.md#ncaa_wbb_play_by_play) | Scrape many WBB games' play-by-play (wbigballR `get_play_by_play`, quarters fixed). |
| [shot_events_to_frame](additional/play-by-play-processing.md#shot_events_to_frame) | Flatten NCAA HTML `ShotEvent` objects to the canonical frame. |
| [wbb_pbp_disk](additional/play-by-play-processing.md#wbb_pbp_disk) | _No description available._ |

## Models and calculators

| Function | Summary |
|---|---|
| [AssistEvent](additional/models-and-calculators.md#AssistEvent) | One assist relationship's counts (`LineupEventStats.AssistEvent`, |
| [AssistInfo](additional/models-and-calculators.md#AssistInfo) | Detailed assist info, split into given/received |
| [ConferenceId](additional/models-and-calculators.md#ConferenceId) | CBB conference identifier (`ConferenceId`, ``models/ConferenceId |
| [CutdownShotEvent](additional/models-and-calculators.md#CutdownShotEvent) | A narrowed `ShotEvent`, keeping only the fields needed once a |
| [Direction](additional/models-and-calculators.md#Direction) | Which team is in possession (`RawGameEvent.Direction`, `:119-121`). |
| [FieldGoalStats](additional/models-and-calculators.md#FieldGoalStats) | Field-goal counting stats (`LineupEventStats.FieldGoalStats`, |
| [LeagueConstants](additional/models-and-calculators.md#LeagueConstants) | Per-league fitted constants for the prediction & tournament stack. |
| [LineupEvent](additional/models-and-calculators.md#LineupEvent) | A portion of a game during which a given lineup was on the floor |
| [LineupEventStats](additional/models-and-calculators.md#LineupEventStats) | A lineup event's full counting-stat tree (`LineupEventStats`, |
| [LineupId](additional/models-and-calculators.md#LineupId) | The set of players on the floor, as an opaque id string |
| [LocationType](additional/models-and-calculators.md#LocationType) | Game location (`Game.LocationType`, `Game.scala:36-38`). |
| [PlayerCodeId](additional/models-and-calculators.md#PlayerCodeId) | A player's within-team-season code paired with their full identity |
| [PlayerEvent](additional/models-and-calculators.md#PlayerEvent) | A lineup event's stats, narrowed to one player (`PlayerEvent`, |
| [PlayerId](additional/models-and-calculators.md#PlayerId) | CBB player identifier (`PlayerId`, `PlayerId.scala`, `AnyVal`). |
| [PlayerShotInfo](additional/models-and-calculators.md#PlayerShotInfo) | Per-player shot-quality info, keyed by lineup slot |
| [PlayerValueConstants](additional/models-and-calculators.md#PlayerValueConstants) | Per-league constants for the player-value spine. |
| [PossCalcFragment](additional/models-and-calculators.md#PossCalcFragment) | Running stats needed to calculate possessions for one lineup event, |
| [PossessionEvent](additional/models-and-calculators.md#PossessionEvent) | Decomposes `RawGameEvent`\ s into attacking/defending sides |
| [RapmConfig](additional/models-and-calculators.md#RapmConfig) | Port of `RapmConfig` (`RapmUtils.ts:175-179`). |
| [RapmPlayerContext](additional/models-and-calculators.md#RapmPlayerContext) | Port of `RapmPlayerContext` (`RapmUtils.ts:147-173`). |
| [RapmPreProcDiagnostics](additional/models-and-calculators.md#RapmPreProcDiagnostics) | Port of `RapmPreProcDiagnostics` (`RapmUtils.ts:187-194`) -- the |
| [RapmPriorInfo](additional/models-and-calculators.md#RapmPriorInfo) | Port of `RapmPriorInfo` (`RapmUtils.ts:124-133`). |
| [RapmProcessingInputs](additional/models-and-calculators.md#RapmProcessingInputs) | Port of `RapmProcessingInputs` (`RapmUtils.ts:196-203`). |
| [RawGameEvent](additional/models-and-calculators.md#RawGameEvent) | A single NCAA play-by-play event line (`LineupEvent.RawGameEvent`, |
| [RosterEntry](additional/models-and-calculators.md#RosterEntry) | An entry in an NCAA team roster (`RosterEntry`, ``models/ncaa |
| [ScoreInfo](additional/models-and-calculators.md#ScoreInfo) | Score context at the start/end of a lineup event |
| [ShotClockStats](additional/models-and-calculators.md#ShotClockStats) | Counting stats broken down by shot-clock segment |
| [ShotEvent](additional/models-and-calculators.md#ShotEvent) | Info about one shot taken during a game, all distances in feet |
| [ShotGeo](additional/models-and-calculators.md#ShotGeo) | A shot's synthetic lat/lon, for geo-aware visualization tooling |
| [ShotLocation](additional/models-and-calculators.md#ShotLocation) | A shot's court-relative coordinates, in feet (`ShotEvent.ShotLocation`, |
| [ShotQualityConstants](additional/models-and-calculators.md#ShotQualityConstants) | Per-league rule + fitted constants for the shot-quality spine. |
| [TeamId](additional/models-and-calculators.md#TeamId) | CBB team identifier (`TeamId`, `TeamId.scala`, `AnyVal`). |
| [TeamSeasonId](additional/models-and-calculators.md#TeamSeasonId) | A team's season identifier (`TeamSeasonId`, `TeamSeasonId.scala`). |
| [Year](additional/models-and-calculators.md#Year) | CBB season, named by the year it ends (`Year`, `Year.scala`). |
| [adjust_efficiency](additional/models-and-calculators.md#adjust_efficiency) | Iterative opponent-adjusted efficiency -> AdjO / AdjD / AdjEM per team-season. |
| [adjust_off_rating_stats](additional/models-and-calculators.md#adjust_off_rating_stats) | Apply a missing-possession correction factor to an `ORtgDiagnostics` dict in place. |
| [adjust_tempo](additional/models-and-calculators.md#adjust_tempo) | Opponent-adjusted tempo (possessions/40) per team-season. |
| [aggregate_player_seasons](additional/models-and-calculators.md#aggregate_player_seasons) | Canonical per-player-season counting frame from the boxscore release. |
| [apply_weak_priors](additional/models-and-calculators.md#apply_weak_priors) | Build a closure that nudges ridge-regressed RAPM back towards its weak prior. |
| [as_of_ratings_split](additional/models-and-calculators.md#as_of_ratings_split) | Filter a results frame to games strictly before a cutoff date (leakage boundary). |
| [as_of_season_split](additional/models-and-calculators.md#as_of_season_split) | Rows strictly before `target_season` -- the leakage boundary. |
| [bootstrap_ari](additional/models-and-calculators.md#bootstrap_ari) | Cluster stability: mean ARI between the full fit and bootstrap refits. |
| [brier_score](additional/models-and-calculators.md#brier_score) | Mean squared error between predicted probabilities and binary outcomes. |
| [build_d_rtg](additional/models-and-calculators.md#build_d_rtg) | Individual defensive rating (Dean-Oliver DRtg) + diagnostics. |
| [build_net_points](additional/models-and-calculators.md#build_net_points) | Decompose ORtg/DRtg + RAPM into a Net-Points-like breakdown. |
| [build_o_rtg](additional/models-and-calculators.md#build_o_rtg) | Individual offensive rating (Dean-Oliver ORtg) + diagnostics. |
| [build_player_context](additional/models-and-calculators.md#build_player_context) | Build the context object the RAPM matrix-solve layer consumes. |
| [build_priors](additional/models-and-calculators.md#build_priors) | Build strong/weak per-player RAPM priors for every column. |
| [build_productivity](additional/models-and-calculators.md#build_productivity) | Public port of `RatingUtils.buildProductivity` (`RatingUtils.ts:963-990`). |
| [build_wbb_season_wp](additional/models-and-calculators.md#build_wbb_season_wp) | A WBB season's play-by-play with win-probability columns joined in. |
| [build_weak_prior_from_rapm](additional/models-and-calculators.md#build_weak_prior_from_rapm) | Wrap a flat RAPM-estimate vector into `playersWeak`-shaped dicts. |
| [calc_collinearity_diag](additional/models-and-calculators.md#calc_collinearity_diag) | Multi-collinearity diagnostic between the players in an off/def design matrix. |
| [calc_lineup_outputs](additional/models-and-calculators.md#calc_lineup_outputs) | Build the off/def target vectors the RAPM design matrices are fit against. |
| [calc_player_weights](additional/models-and-calculators-2.md#calc_player_weights) | Build the off/def player-weight (design) matrices for the RAPM solve. |
| [calc_slow_pseudo_inverse](additional/models-and-calculators-2.md#calc_slow_pseudo_inverse) | Per-parameter variance terms for the ridge-regression standard errors. |
| [calculate_predicted_out](additional/models-and-calculators-2.md#calculate_predicted_out) | Predict per-lineup outputs from fitted per-player RAPM values. |
| [calculate_rapm](additional/models-and-calculators-2.md#calculate_rapm) | Apply a regression solver matrix to a target-outputs vector. |
| [calculate_residual_error](additional/models-and-calculators-2.md#calculate_residual_error) | Sum of squared residuals between actual and predicted lineup outputs. |
| [calculate_sd_rapm](additional/models-and-calculators-2.md#calculate_sd_rapm) | Per-player RAPM standard errors. |
| [calibration_table](additional/models-and-calculators-2.md#calibration_table) | Bucket predicted probabilities into bins and compare to actual outcome rates. |
| [fit_shrinkage_k](additional/models-and-calculators-2.md#fit_shrinkage_k) | Fit the talent shrinkage `k` split-half (see module docstring). |
| [get_constants](additional/models-and-calculators-2.md#get_constants) | League constants bundle for the shot-quality spine. |
| [get_player_value_constants](additional/models-and-calculators-2.md#get_player_value_constants) | Return the `PlayerValueConstants` for a league. |
| [in_game_features](additional/models-and-calculators-2.md#in_game_features) | Per-play in-game win-probability features from a `load_mbb_pbp` frame. |
| [inject_rapm_into_players](additional/models-and-calculators-2.md#inject_rapm_into_players) | Write `pick_ridge_regression`'s RAPM predictions back onto each player. |
| [kmeans_fit](additional/models-and-calculators-2.md#kmeans_fit) | Seeded Lloyd's KMeans, best-of-`n_init` by inertia. |
| [load_artifact](additional/models-and-calculators-2.md#load_artifact) | Read a bundled player-value artifact (`mbb/models/<name>.json`). |
| [log_loss_score](additional/models-and-calculators-2.md#log_loss_score) | Binary cross-entropy loss between predicted probabilities and outcomes. |
| [logistic_fit](additional/models-and-calculators-2.md#logistic_fit) | L2-penalized logistic regression via L-BFGS (intercept unpenalized). |
| [mae](additional/models-and-calculators-2.md#mae) | Mean absolute error between two arrays. |
| [pick_ridge_regression](additional/models-and-calculators-2.md#pick_ridge_regression) | Adaptively pick a ridge-regression lambda and blend in the RAPM priors. |
| [player_per100_features](additional/models-and-calculators-2.md#player_per100_features) | Per-100 / rate features for every (player_id, season). |
| [poss_calc_fragment_sum](additional/models-and-calculators-2.md#poss_calc_fragment_sum) | Field-wise add two `PossCalcFragment`\ s |
| [predict_margin](additional/models-and-calculators-2.md#predict_margin) | Women's expected margin. |
| [predict_total](additional/models-and-calculators-2.md#predict_total) | Women's expected total points. |
| [raw_game_efficiency](additional/models-and-calculators-2.md#raw_game_efficiency) | Per-team, per-game possessions + raw offensive/defensive efficiency. |
| [ridge_cv_lambda](additional/models-and-calculators-2.md#ridge_cv_lambda) | Pick lambda by leave-one-group-out CV (groups = seasons/classes). |
| [ridge_fit](additional/models-and-calculators-2.md#ridge_fit) | Closed-form ridge with an unpenalized intercept (coefficient 0). |
| [roc_auc](additional/models-and-calculators-2.md#roc_auc) | Area under the ROC curve via the rank-sum (Mann-Whitney) identity. |
| [save_artifact](additional/models-and-calculators-2.md#save_artifact) | Write a bundled artifact (dev/fitter use -- writes into the source tree). |
| [score_to_tuple](additional/models-and-calculators-2.md#score_to_tuple) | Parse a `"scored-allowed"` score string (`ExtractorUtils.score_to_tuple`, |
| [simulate_game](additional/models-and-calculators-2.md#simulate_game) | Sample one women's game outcome (women's sigma/HFA/em_scale). |
| [slow_regression](additional/models-and-calculators-2.md#slow_regression) | Build the Tikhonov (ridge) regression solver matrix. |
| [spearman_corr](additional/models-and-calculators-2.md#spearman_corr) | Spearman rank correlation between two arrays. |
| [talent_split_mse](additional/models-and-calculators-2.md#talent_split_mse) | Weighted MSE of the k-regressed first half predicting the raw second half. |
| [three_point_radius](additional/models-and-calculators-2.md#three_point_radius) | Three-point arc radius (feet) for a league x season. |
| [transfer_cohort](additional/models-and-calculators-2.md#transfer_cohort) | One row per transfer: same `player_id`, different `team_id` in |
| [wbb_bracket_sim](additional/models-and-calculators-2.md#wbb_bracket_sim) | Women's single-elimination bracket Monte Carlo. |
| [wbb_in_game_win_prob](additional/models-and-calculators-2.md#wbb_in_game_win_prob) | Women's per-play in-game win probability (bundled `wbb_in_game_wp.ubj`). |
| [wbb_predict_games](additional/models-and-calculators-2.md#wbb_predict_games) | Women's vectorized pregame predictions over a schedule. |
| [wbb_season_sim](additional/models-and-calculators-2.md#wbb_season_sim) | Women's remaining-schedule Monte Carlo. |
| [wbb_team_ratings](additional/models-and-calculators-2.md#wbb_team_ratings) | Women's opponent-adjusted team ratings (AdjO/AdjD/AdjEM/AdjTempo). |
| [win_prob_from_margin](additional/models-and-calculators-2.md#win_prob_from_margin) | Women's home win probability from an expected margin. |

## Analytics

| Function | Summary |
|---|---|
| [ConcurrentClump](additional/analytics.md#ConcurrentClump) | A clump of concurrent raw events, together with the lineups that end |
| [PossState](additional/analytics.md#PossState) | Running state threaded through `calculate_possessions_by_event` |
| [apply_relative_positional_overrides](additional/analytics.md#apply_relative_positional_overrides) | Recursively re-shuffle an ordered lineup per `RELATIVE_POSITION_FIXES`. |
| [assign_to_right_lineup](additional/analytics.md#assign_to_right_lineup) | Assign a clump's possessions to the lineup(s) ending in it |
| [build_3p_shot_info](additional/analytics.md#build_3p_shot_info) | 3P-only shot-decomposition wrapper. |
| [build_adjusted_3p](additional/analytics.md#build_adjusted_3p) | 3P-only approx-unassisted/assisted-FG% wrapper. |
| [build_efficiency_margins](additional/analytics.md#build_efficiency_margins) | Derive `off_net` / `off_raw_net` on a stat set, in place. |
| [build_exp_3p](additional/analytics.md#build_exp_3p) | Expected made-3P count given a player's shot-type mix + shooting %s. |
| [build_position](additional/analytics.md#build_position) | Classify a player into a position label + diagnostic trace string. |
| [build_position_confidences](additional/analytics.md#build_position_confidences) | Build the 5-way positional confidence vector for a player. |
| [build_positional_aware_filter](additional/analytics.md#build_positional_aware_filter) | Decompose a search-filter string into positionally-aware +ve/-ve fragments. |
| [calc_def_player_luck_adj](additional/analytics.md#calc_def_player_luck_adj) | Defensive 3P-luck adjustment for a single player. |
| [calc_def_team_luck_adj](additional/analytics.md#calc_def_team_luck_adj) | Defensive 3P-luck adjustment for a team (or lineup). |
| [calc_off_player_luck_adj](additional/analytics.md#calc_off_player_luck_adj) | Offensive 3P-luck adjustment for a single player. |
| [calc_off_team_luck_adj](additional/analytics.md#calc_off_team_luck_adj) | Offensive 3P-luck adjustment for a team (or lineup). |
| [calculate_aggregated_lineup_stats](additional/analytics.md#calculate_aggregated_lineup_stats) | Combine all lineups into a single team stat set. |
| [calculate_possessions](additional/analytics.md#calculate_possessions) | Top-level entry point: calculate team/opponent possessions for a |
| [calculate_possessions_by_event](additional/analytics.md#calculate_possessions_by_event) | Drive the batch loop + per-clump scoring over an already-flattened |
| [calculate_stats](additional/analytics.md#calculate_stats) | Calculate one direction's possession-fragment for one merged clump |
| [complete_weighted_avg](additional/analytics.md#complete_weighted_avg) | Finish a `weighted_avg` accumulator into true weighted averages. |
| [concurrent_event_handler](additional/analytics.md#concurrent_event_handler) | Batch a stream of singleton/boundary clumps into merged |
| [count_matching](additional/analytics.md#count_matching) | Count events on one side matching any of the given parsers. |
| [get_stats_diff](additional/analytics.md#get_stats_diff) | Straight (unweighted) field-by-field diff of two team stat sets. |
| [incorporate_height](additional/analytics.md#incorporate_height) | Reweight positional confidences by height (Bayesian-ish height prior). |
| [inject_luck](additional/analytics.md#inject_luck) | Reversibly mutate a stat set in place with luck-adjustment deltas. |
| [lineup_as_raw_clumps](additional/analytics.md#lineup_as_raw_clumps) | Turn one lineup's raw events into unprocessed singleton clumps, plus a |
| [lineup_balancer](additional/analytics.md#lineup_balancer) | Attribute this clump's possessions to the candidate lineup(s) |
| [lineup_fixer](additional/analytics.md#lineup_fixer) | Clamp obviously-broken possession counts (``PossessionUtils |
| [lineup_to_team_report](additional/analytics.md#lineup_to_team_report) | Build per-player on/off splits out of a team's lineups. |
| [ncaa_wbb_lineups](additional/analytics.md#ncaa_wbb_lineups) | Aggregate WBB play-by-play into per-lineup stats (wbigballR `get_lineups`). |
| [ncaa_wbb_on_off](additional/analytics.md#ncaa_wbb_on_off) | Team stats for every on/off combination of the given WBB players. |
| [ncaa_wbb_player_combos](additional/analytics.md#ncaa_wbb_player_combos) | Team stats for every n-player WBB combination on the court together. |
| [ncaa_wbb_player_lineups](additional/analytics.md#ncaa_wbb_player_lineups) | Filter a WBB lineups frame by on-court player membership. |
| [order_lineup](additional/analytics.md#order_lineup) | Order a 5-man lineup `X1_X2_X3_X4_X5` into PG/SG/SF/PF/C slot order. |
| [pos_class_to_score](additional/analytics.md#pos_class_to_score) | Ordinal "positional weight" for a position class, PG=1000..C=8000. |
| [project_bracket](additional/analytics.md#project_bracket) | Select and seed a tournament field from a per-team résumé frame. |
| [regress_shot_quality](additional/analytics.md#regress_shot_quality) | Shrink a small-sample shot-quality stat toward its positional average. |
| [strength_of_schedule](additional/analytics.md#strength_of_schedule) | Per-team SoS + Quad 1-4 record + WAB from completed games and ratings. |
| [test_positional_aware_filter](additional/analytics.md#test_positional_aware_filter) | Check a positional-aware filter (from `build_positional_aware_filter`) |
| [using_roster_pos](additional/analytics.md#using_roster_pos) | Reconcile a stats-derived position class against roster metadata. |
| [wbb_bracketology](additional/analytics.md#wbb_bracketology) | Women's projected tournament field for a season. |
| [wbb_strength_of_schedule](additional/analytics.md#wbb_strength_of_schedule) | Women's season-level SoS / Quad / WAB résumé. |
| [weighted_avg](additional/analytics.md#weighted_avg) | Merge `obj` into `mutable_acc` with possession weighting. |

## Dates and seasons

| Function | Summary |
|---|---|
| [most_recent_wbb_season](additional/dates-and-seasons.md#most_recent_wbb_season) | Return the most recent women's college basketball season year. |

## IDs and crosswalks

| Function | Summary |
|---|---|
| [FuzzyMatchError](additional/ids-and-crosswalks.md#FuzzyMatchError) | A failed `fuzzy_box_match` resolution (Scala's `Left[String]` |
| [NoSurnameMatch](additional/ids-and-crosswalks.md#NoSurnameMatch) | No candidate surname fragment scored well enough |
| [StrongSurnameMatch](additional/ids-and-crosswalks.md#StrongSurnameMatch) | A surname fragment matched and the whole-name score cleared |
| [TidyPlayerContext](additional/ids-and-crosswalks.md#TidyPlayerContext) | Precomputed box-score lookup tables + resolution cache for |
| [WeakSurnameMatch](additional/ids-and-crosswalks.md#WeakSurnameMatch) | A surname fragment matched, but the whole-name score fell short of |
| [box_aware_compare](additional/ids-and-crosswalks.md#box_aware_compare) | Score how well a single play-by-play candidate name fits a single |
| [build_tidy_player_context](additional/ids-and-crosswalks.md#build_tidy_player_context) | Build the alternative player-code lookup maps for a box-score lineup |
| [code_from_box](additional/ids-and-crosswalks.md#code_from_box) | Resolve a tidied player NAME to the box roster's own `PlayerCodeId`. |
| [convert_from_digits](additional/ids-and-crosswalks.md#convert_from_digits) | Resolve a jersey-number-only name to its box-score player |
| [convert_from_initials](additional/ids-and-crosswalks.md#convert_from_initials) | Resolve a 2-initial name (`"A B"` / `"B, A"`) to the single |
| [display_name_to_roster_key](additional/ids-and-crosswalks.md#display_name_to_roster_key) | Box-score and shot-chart pages render a player as `"Surname, First"`, |
| [fuzzy_box_match](additional/ids-and-crosswalks.md#fuzzy_box_match) | Pick the single unassigned box-score name a mis-spelled play-by-play |
| [ncaa_espn_team_crosswalk](additional/ids-and-crosswalks.md#ncaa_espn_team_crosswalk) | Season-keyed stats.ncaa.org -> ESPN team-id crosswalk. |
| [ncaa_wbb_team_ids](additional/ids-and-crosswalks.md#ncaa_wbb_team_ids) | Women's-basketball `(team, season) -> stats.ncaa.org id` crosswalk. |
| [resolve_ncaa_team_id](additional/ids-and-crosswalks.md#resolve_ncaa_team_id) | Resolve a school name + season to its stats.ncaa.org team id. |
| [tidy_player](additional/ids-and-crosswalks.md#tidy_player) | Resolve a raw play-by-play name to its box-score full name, via an |
| [wbb_player_crosswalk](additional/ids-and-crosswalks.md#wbb_player_crosswalk) | Build the WBB cross-source player crosswalk (ESPN / Fox). |
| [wbb_schedule_crosswalk](additional/ids-and-crosswalks.md#wbb_schedule_crosswalk) | Build the WBB cross-source schedule crosswalk (ESPN / Torvik). |
| [wbb_team_crosswalk](additional/ids-and-crosswalks.md#wbb_team_crosswalk) | Build the WBB cross-source team crosswalk (ESPN / Fox / Torvik). |
