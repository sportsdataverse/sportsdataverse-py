---
title: CFB
sidebar_label: CFB
description: "sdv-py CFB: endpoint references, dataset loaders and parsers for CFB in the SportsDataverse Python package."
---
# CFB (`sportsdataverse.cfb`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [Highlights](reference/additional#highlights) | curated "start here" functions | 8 | — |
| [ESPN](#espn) | `cdn.espn.com`, `site.api.espn.com`, `site.web.api.espn.com` +1 more | 131 | none |
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com`, `raw.githubusercontent.com` | 74 | none |
| [stats.ncaa.org](#stats-ncaa-org) | `stats.ncaa.org` | 1 | none (Terms gate + rate rotation) |
| [On3 Recruit Database](#on3-recruit-database) | `api.on3.com` | 82 | API key (ON3_API_KEY) |
| [247Sports Recruit Database](#247sports-recruit-database) | `247sports.com`, `ipa.247sports.com` | 47 | none |
| [Yahoo Sports Shangrila](#yahoo-sports-shangrila) | `graphite-secure.sports.yahoo.com` | 7 | none |
| [Fox Sports API](#fox-sports-api) | `api.foxsports.com` | 29 | none |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 105 | — |

## ESPN {#espn}

| Reference | Functions |
|---|---:|
| [ESPN site API (v2)](reference/site) | 25 |
| [ESPN web API (v3)](reference/web) | 5 |
| [ESPN core API (v2)](reference/core) | 89 |
| [ESPN FPI API (fitt v3)](reference/fitt) | 1 |
| [ESPN CDN API (cdn.espn.com)](reference/cdn) | 5 |
| [Hand-written wrappers](reference/additional) | 6 |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 70 |
| [sportsdataverse raw data](reference/loaders) | 1 |
| [Hand-written wrappers](reference/additional) | 3 |

## stats.ncaa.org {#stats-ncaa-org}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 1 |

## On3 Recruit Database {#on3-recruit-database}

| Reference | Functions |
|---|---:|
| [On3 Recruit Database (api.on3.com)](reference/on3) | 78 |
| [Hand-written wrappers](reference/additional) | 4 |

## 247Sports Recruit Database {#247sports-recruit-database}

| Reference | Functions |
|---|---:|
| [247Sports Recruit Database (ipa.247sports.com)](reference/sports247) | 12 |
| [247Sports Site Pages (247sports.com)](reference/sports247_site_pages) | 35 |

## Yahoo Sports Shangrila {#yahoo-sports-shangrila}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 7 |

## Fox Sports API {#fox-sports-api}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional) | 29 |
## Tools and helpers

### Play-by-play processing {#play-by-play-processing}

- [`CFBPlayProcess`](reference/additional#CFBPlayProcess)

### Models and calculators {#models-and-calculators}

- [`add_era_columns`](reference/additional#add_era_columns)
- [`assert_rating_scale`](reference/additional#assert_rating_scale)
- [`calculate_completion_probability`](reference/additional#calculate_completion_probability)
- [`calculate_epa`](reference/additional#calculate_epa)
- [`calculate_expected_points`](reference/additional#calculate_expected_points)
- [`calculate_field_goal_probability`](reference/additional#calculate_field_goal_probability)
- [`calculate_fourth_down`](reference/additional#calculate_fourth_down)
- [`calculate_qbr`](reference/additional#calculate_qbr)
- [`calculate_two_point_probability`](reference/additional#calculate_two_point_probability)
- [`calculate_win_probability`](reference/additional#calculate_win_probability)
- [`calculate_wpa`](reference/additional#calculate_wpa)
- [`calculate_xpass`](reference/additional#calculate_xpass)
- [`cfb_adjusted_epa`](reference/additional#cfb_adjusted_epa)
- [`cfb_adjusted_epa_by_game`](reference/additional#cfb_adjusted_epa_by_game)
- [`cfb_compute_results`](reference/additional#cfb_compute_results)
- [`cfb_draft_projection`](reference/additional#cfb_draft_projection)
- [`cfb_field_position`](reference/additional#cfb_field_position)
- [`cfb_predict_games`](reference/additional#cfb_predict_games)
- [`cfb_ratings`](reference/additional#cfb_ratings)
- [`cfb_recruiting_projection`](reference/additional#cfb_recruiting_projection)
- [`cfb_roster_talent`](reference/additional#cfb_roster_talent)
- [`cfb_simulations`](reference/additional#cfb_simulations)
- [`efficiency_ratings`](reference/additional#efficiency_ratings)
- [`fei_ratings`](reference/additional#fei_ratings)
- [`fit_field_position_ep`](reference/additional#fit_field_position_ep)
- [`get_2pt_probs`](reference/additional#get_2pt_probs)
- [`get_4th_down_probs`](reference/additional#get_4th_down_probs)
- [`get_fg_wp`](reference/additional#get_fg_wp)
- [`get_go_wp`](reference/additional#get_go_wp)
- [`get_punt_wp`](reference/additional#get_punt_wp)
- [`load_draft_outcomes`](reference/additional#load_draft_outcomes)
- [`load_fp_curve`](reference/additional#load_fp_curve)
- [`load_recruit_classes`](reference/additional#load_recruit_classes)
- [`normalize_pbp_columns`](reference/additional#normalize_pbp_columns)
- [`predict_from_card`](reference/additional#predict_from_card)
- [`predict_margin`](reference/additional#predict_margin)
- [`predict_total`](reference/additional#predict_total)
- [`slope_for_games`](reference/additional#slope_for_games)
- [`special_teams_ratings`](reference/additional#special_teams_ratings)
- [`win_prob_from_margin`](reference/additional#win_prob_from_margin)

### Analytics {#analytics}

- [`add_play_type_canonical`](reference/additional#add_play_type_canonical)
- [`canonical_play_type_expr`](reference/additional#canonical_play_type_expr)
- [`cfb_adjusted_tempo`](reference/additional#cfb_adjusted_tempo)
- [`cfb_advanced_stats`](reference/additional#cfb_advanced_stats)
- [`cfb_games_from_schedule`](reference/additional#cfb_games_from_schedule)
- [`cfb_playoff_seeds`](reference/additional#cfb_playoff_seeds)
- [`cfb_resume`](reference/additional#cfb_resume)
- [`cfb_returning_production`](reference/additional#cfb_returning_production)
- [`cfb_season_odds`](reference/additional#cfb_season_odds)
- [`cfb_standings`](reference/additional#cfb_standings)
- [`cfb_transfer_impact`](reference/additional#cfb_transfer_impact)
- [`cfb_transfer_moves`](reference/additional#cfb_transfer_moves)
- [`create_drive_summary`](reference/additional#create_drive_summary)
- [`create_situational_stats`](reference/additional#create_situational_stats)
- [`make_ratings_compute_results`](reference/additional#make_ratings_compute_results)
- [`play_type_family_expr`](reference/additional#play_type_family_expr)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_cfb_season`](reference/additional#most_recent_cfb_season)

### IDs and crosswalks {#ids-and-crosswalks}

- [`cfb_odds_events_crosswalk`](reference/additional#cfb_odds_events_crosswalk)
- [`cfb_rosters_crosswalk`](reference/additional#cfb_rosters_crosswalk)
- [`cfb_schedule_crosswalk`](reference/additional#cfb_schedule_crosswalk)
- [`cfb_teams_crosswalk`](reference/additional#cfb_teams_crosswalk)

### Validation {#validation}

- [`check_box_invariants`](reference/additional#check_box_invariants)


## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [CFB tutorial](../tutorials/02_cfb_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`cfbfastR`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.cfb` (Python) | `cfbfastR` (R) |
|---|---|
| [`calculate_completion_probability`](reference/additional/models-and-calculators#calculate_completion_probability) | [`calculate_completion_probability`](https://cfbfastR.sportsdataverse.org/reference/calculate_completion_probability.html) |
| [`calculate_epa`](reference/additional/models-and-calculators#calculate_epa) | [`calculate_epa`](https://cfbfastR.sportsdataverse.org/reference/calculate_epa.html) |
| [`calculate_expected_points`](reference/additional/models-and-calculators#calculate_expected_points) | [`calculate_expected_points`](https://cfbfastR.sportsdataverse.org/reference/calculate_expected_points.html) |
| [`calculate_field_goal_probability`](reference/additional/models-and-calculators#calculate_field_goal_probability) | [`calculate_field_goal_probability`](https://cfbfastR.sportsdataverse.org/reference/calculate_field_goal_probability.html) |
| [`calculate_fourth_down`](reference/additional/models-and-calculators#calculate_fourth_down) | [`calculate_fourth_down`](https://cfbfastR.sportsdataverse.org/reference/calculate_fourth_down.html) |
| [`calculate_qbr`](reference/additional/models-and-calculators#calculate_qbr) | [`calculate_qbr`](https://cfbfastR.sportsdataverse.org/reference/calculate_qbr.html) |
| [`calculate_two_point_probability`](reference/additional/models-and-calculators#calculate_two_point_probability) | [`calculate_two_point_probability`](https://cfbfastR.sportsdataverse.org/reference/calculate_two_point_probability.html) |
| [`calculate_win_probability`](reference/additional/models-and-calculators#calculate_win_probability) | [`calculate_win_probability`](https://cfbfastR.sportsdataverse.org/reference/calculate_win_probability.html) |
| [`calculate_wpa`](reference/additional/models-and-calculators#calculate_wpa) | [`calculate_wpa`](https://cfbfastR.sportsdataverse.org/reference/calculate_wpa.html) |
| [`calculate_xpass`](reference/additional/models-and-calculators#calculate_xpass) | [`calculate_xpass`](https://cfbfastR.sportsdataverse.org/reference/calculate_xpass.html) |
| [`espn_cfb_award`](reference/core/other#espn_cfb_award) | [`espn_cfb_award`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_award.html) |
| [`espn_cfb_awards`](reference/core/other#espn_cfb_awards) | [`espn_cfb_awards`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_awards.html) |
| [`espn_cfb_calendar`](reference/site#espn_cfb_calendar) | [`espn_cfb_calendar`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_calendar.html) |
| [`espn_cfb_coach`](reference/core/other#espn_cfb_coach) | [`espn_cfb_coach`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_coach.html) |
| [`espn_cfb_coach_record`](reference/core/other#espn_cfb_coach_record) | [`espn_cfb_coach_record`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_coach_record.html) |
| [`espn_cfb_franchise`](reference/core/other#espn_cfb_franchise) | [`espn_cfb_franchise`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_franchise.html) |
| [`espn_cfb_franchises`](reference/core/other#espn_cfb_franchises) | [`espn_cfb_franchises`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_franchises.html) |
| [`espn_cfb_futures`](reference/core/other#espn_cfb_futures) | [`espn_cfb_futures`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_futures.html) |
| [`espn_cfb_game_broadcasts`](reference/core/game#espn_cfb_game_broadcasts) | [`espn_cfb_game_broadcasts`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_broadcasts.html) |
| [`espn_cfb_game_leaders`](reference/core/game#espn_cfb_game_leaders) | [`espn_cfb_game_leaders`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_leaders.html) |
| [`espn_cfb_game_odds`](reference/core/game#espn_cfb_game_odds) | [`espn_cfb_game_odds`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_odds.html) |
| [`espn_cfb_game_play`](reference/core/game#espn_cfb_game_play) | [`espn_cfb_game_play`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_play.html) |
| [`espn_cfb_game_powerindex`](reference/core/game#espn_cfb_game_powerindex) | [`espn_cfb_game_powerindex`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_powerindex.html) |
| [`espn_cfb_game_predictor`](reference/core/game#espn_cfb_game_predictor) | [`espn_cfb_game_predictor`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_predictor.html) |
| [`espn_cfb_game_probabilities`](reference/core/game#espn_cfb_game_probabilities) | [`espn_cfb_game_probabilities`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_probabilities.html) |
| [`espn_cfb_game_situation`](reference/core/game#espn_cfb_game_situation) | [`espn_cfb_game_situation`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_situation.html) |
| [`espn_cfb_game_status`](reference/core/game#espn_cfb_game_status) | [`espn_cfb_game_status`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_status.html) |
| [`espn_cfb_game_team_leaders`](reference/core/game#espn_cfb_game_team_leaders) | [`espn_cfb_game_team_leaders`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_leaders.html) |
| [`espn_cfb_game_team_linescores`](reference/core/game#espn_cfb_game_team_linescores) | [`espn_cfb_game_team_linescores`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_linescores.html) |
| [`espn_cfb_game_team_roster`](reference/core/game#espn_cfb_game_team_roster) | [`espn_cfb_game_team_roster`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_roster.html) |
| [`espn_cfb_game_team_statistics`](reference/core/game#espn_cfb_game_team_statistics) | [`espn_cfb_game_team_statistics`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_team_statistics.html) |
| [`espn_cfb_game_teams`](reference/core/game#espn_cfb_game_teams) | [`espn_cfb_game_teams`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_game_teams.html) |
| [`espn_cfb_groups`](reference/core/other#espn_cfb_groups) | [`espn_cfb_groups`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_groups.html) |
| [`espn_cfb_player_career_stats`](reference/core/player#espn_cfb_player_career_stats) | [`espn_cfb_player_career_stats`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_career_stats.html) |
| [`espn_cfb_player_eventlog`](reference/core/player#espn_cfb_player_eventlog) | [`espn_cfb_player_eventlog`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_eventlog.html) |
| [`espn_cfb_player_gamelog`](reference/web#espn_cfb_player_gamelog) | [`espn_cfb_player_gamelog`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_gamelog.html) |
| [`espn_cfb_player_overview`](reference/web#espn_cfb_player_overview) | [`espn_cfb_player_overview`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_overview.html) |
| [`espn_cfb_player_seasons`](reference/core/player#espn_cfb_player_seasons) | [`espn_cfb_player_seasons`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_seasons.html) |
| [`espn_cfb_player_splits`](reference/web#espn_cfb_player_splits) | [`espn_cfb_player_splits`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_splits.html) |
| [`espn_cfb_player_stats`](reference/additional/highlights#espn_cfb_player_stats) | [`espn_cfb_player_stats`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_stats.html) |
| [`espn_cfb_player_stats_v3`](reference/web#espn_cfb_player_stats_v3) | [`espn_cfb_player_stats_v3`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_player_stats_v3.html) |
| [`espn_cfb_position`](reference/core/other#espn_cfb_position) | [`espn_cfb_position`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_position.html) |
| [`espn_cfb_positions`](reference/core/other#espn_cfb_positions) | [`espn_cfb_positions`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_positions.html) |
| [`espn_cfb_rankings`](reference/site#espn_cfb_rankings) | [`espn_cfb_rankings`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_rankings.html) |
| [`espn_cfb_recruits`](reference/core/other#espn_cfb_recruits) | [`espn_cfb_recruits`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_recruits.html) |
| [`espn_cfb_schedule`](reference/additional/highlights#espn_cfb_schedule) | [`espn_cfb_schedule`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_schedule.html) |
| [`espn_cfb_scoreboard`](reference/site#espn_cfb_scoreboard) | [`espn_cfb_scoreboard`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_scoreboard.html) |
| [`espn_cfb_season_info`](reference/core/season#espn_cfb_season_info) | [`espn_cfb_season_info`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_season_info.html) |
| [`espn_cfb_season_types`](reference/core/season#espn_cfb_season_types) | [`espn_cfb_season_types`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_season_types.html) |
| [`espn_cfb_season_weeks`](reference/core/season#espn_cfb_season_weeks) | [`espn_cfb_season_weeks`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_season_weeks.html) |
| [`espn_cfb_seasons`](reference/core/other#espn_cfb_seasons) | [`espn_cfb_seasons`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_seasons.html) |
| [`espn_cfb_standings`](reference/site#espn_cfb_standings) | [`espn_cfb_standings`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_standings.html) |
| [`espn_cfb_team`](reference/site#espn_cfb_team) | [`espn_cfb_team`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team.html) |
| [`espn_cfb_team_leaders`](reference/site#espn_cfb_team_leaders) | [`espn_cfb_team_leaders`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_leaders.html) |
| [`espn_cfb_team_powerindex`](reference/core/other#espn_cfb_team_powerindex) | [`espn_cfb_team_powerindex`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_powerindex.html) |
| [`espn_cfb_team_record`](reference/site#espn_cfb_team_record) | [`espn_cfb_team_record`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_record.html) |
| [`espn_cfb_team_roster`](reference/site#espn_cfb_team_roster) | [`espn_cfb_team_roster`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_roster.html) |
| [`espn_cfb_team_schedule`](reference/site#espn_cfb_team_schedule) | [`espn_cfb_team_schedule`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_team_schedule.html) |
| [`espn_cfb_teams`](reference/additional/other#espn_cfb_teams) | [`espn_cfb_teams`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_teams.html) |
| [`espn_cfb_venue`](reference/core/other#espn_cfb_venue) | [`espn_cfb_venue`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_venue.html) |
| [`espn_cfb_venues`](reference/core/other#espn_cfb_venues) | [`espn_cfb_venues`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_venues.html) |
| [`espn_cfb_week_rankings`](reference/core/other#espn_cfb_week_rankings) | [`espn_cfb_week_rankings`](https://cfbfastR.sportsdataverse.org/reference/espn_cfb_week_rankings.html) |
| [`fox_cfb_boxscore`](reference/additional/fox-sports-api#fox_cfb_boxscore) | [`fox_cfb_boxscore`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_boxscore.html) |
| [`fox_cfb_league_leaders`](reference/additional/fox-sports-api#fox_cfb_league_leaders) | [`fox_cfb_league_leaders`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_league_leaders.html) |
| [`fox_cfb_odds`](reference/additional/fox-sports-api#fox_cfb_odds) | [`fox_cfb_odds`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_odds.html) |
| [`fox_cfb_pbp`](reference/additional/fox-sports-api#fox_cfb_pbp) | [`fox_cfb_pbp`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_pbp.html) |
| [`fox_cfb_standings`](reference/additional/fox-sports-api#fox_cfb_standings) | [`fox_cfb_standings`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_standings.html) |
| [`fox_cfb_team_gamelog`](reference/additional/fox-sports-api#fox_cfb_team_gamelog) | [`fox_cfb_team_gamelog`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_team_gamelog.html) |
| [`fox_cfb_team_roster`](reference/additional/fox-sports-api#fox_cfb_team_roster) | [`fox_cfb_team_roster`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_team_roster.html) |
| [`fox_cfb_team_stats`](reference/additional/fox-sports-api#fox_cfb_team_stats) | [`fox_cfb_team_stats`](https://cfbfastR.sportsdataverse.org/reference/fox_cfb_team_stats.html) |
| [`load_cfb_fpi_weekly`](reference/loaders/other#load_cfb_fpi_weekly) | [`load_cfb_fpi_weekly`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_fpi_weekly.html) |
| [`load_cfb_group_aliases`](reference/loaders/other-2#load_cfb_group_aliases) | [`load_cfb_group_aliases`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_group_aliases.html) |
| [`load_cfb_group_seasons`](reference/loaders/other-2#load_cfb_group_seasons) | [`load_cfb_group_seasons`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_group_seasons.html) |
| [`load_cfb_groups`](reference/loaders/other-2#load_cfb_groups) | [`load_cfb_groups`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_groups.html) |
| [`load_cfb_pbp`](reference/loaders/pbp#load_cfb_pbp) | [`load_cfb_pbp`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_pbp.html) |
| [`load_cfb_ratings`](reference/loaders/other#load_cfb_ratings) | [`load_cfb_ratings`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_ratings.html) |
| [`load_cfb_ratings_weekly`](reference/loaders/other-2#load_cfb_ratings_weekly) | [`load_cfb_ratings_weekly`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_ratings_weekly.html) |
| [`load_cfb_recruiting_proj`](reference/loaders/other#load_cfb_recruiting_proj) | [`load_cfb_recruiting_proj`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_recruiting_proj.html) |
| [`load_cfb_recruits`](reference/loaders/other#load_cfb_recruits) | [`load_cfb_recruits`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_recruits.html) |
| [`load_cfb_returning_production`](reference/loaders/other#load_cfb_returning_production) | [`load_cfb_returning_production`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_returning_production.html) |
| [`load_cfb_rosters`](reference/loaders/rosters#load_cfb_rosters) | [`load_cfb_rosters`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_rosters.html) |
| [`load_cfb_rosters_crosswalk`](reference/additional/other#load_cfb_rosters_crosswalk) | [`load_cfb_rosters_crosswalk`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_rosters_crosswalk.html) |
| [`load_cfb_schedule_crosswalk`](reference/loaders/schedule#load_cfb_schedule_crosswalk) | [`load_cfb_schedule_crosswalk`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_schedule_crosswalk.html) |
| [`load_cfb_team_group_seasons`](reference/loaders/team-5#load_cfb_team_group_seasons) | [`load_cfb_team_group_seasons`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_team_group_seasons.html) |
| [`load_cfb_team_summaries_weekly`](reference/loaders/team-3#load_cfb_team_summaries_weekly) | [`load_cfb_team_summaries_weekly`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_team_summaries_weekly.html) |
| [`load_cfb_team_talent`](reference/loaders/team#load_cfb_team_talent) | [`load_cfb_team_talent`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_team_talent.html) |
| [`load_cfb_teams`](reference/loaders/team#load_cfb_teams) | [`load_cfb_teams`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_teams.html) |
| [`load_cfb_teams_crosswalk`](reference/loaders/team#load_cfb_teams_crosswalk) | [`load_cfb_teams_crosswalk`](https://cfbfastR.sportsdataverse.org/reference/load_cfb_teams_crosswalk.html) |
| [`load_ncaa_mfb_drives`](reference/loaders/ncaa#load_ncaa_mfb_drives) | [`load_ncaa_mfb_drives`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_drives.html) |
| [`load_ncaa_mfb_linescore`](reference/loaders/ncaa#load_ncaa_mfb_linescore) | [`load_ncaa_mfb_linescore`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_linescore.html) |
| [`load_ncaa_mfb_officials`](reference/loaders/ncaa#load_ncaa_mfb_officials) | [`load_ncaa_mfb_officials`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_officials.html) |
| [`load_ncaa_mfb_pbp`](reference/loaders/ncaa#load_ncaa_mfb_pbp) | [`load_ncaa_mfb_pbp`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_pbp.html) |
| [`load_ncaa_mfb_pbp_cfbfastr`](reference/loaders/ncaa#load_ncaa_mfb_pbp_cfbfastr) | [`load_ncaa_mfb_pbp_cfbfastr`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_pbp_cfbfastr.html) |
| [`load_ncaa_mfb_player_stats`](reference/loaders/ncaa#load_ncaa_mfb_player_stats) | [`load_ncaa_mfb_player_stats`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_player_stats.html) |
| [`load_ncaa_mfb_rosters`](reference/loaders/ncaa#load_ncaa_mfb_rosters) | [`load_ncaa_mfb_rosters`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_rosters.html) |
| [`load_ncaa_mfb_schedule`](reference/loaders/ncaa#load_ncaa_mfb_schedule) | [`load_ncaa_mfb_schedule`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_schedule.html) |
| [`load_ncaa_mfb_team_stats`](reference/loaders/ncaa#load_ncaa_mfb_team_stats) | [`load_ncaa_mfb_team_stats`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_team_stats.html) |
| [`load_ncaa_mfb_teams`](reference/loaders/ncaa#load_ncaa_mfb_teams) | [`load_ncaa_mfb_teams`](https://cfbfastR.sportsdataverse.org/reference/load_ncaa_mfb_teams.html) |
| [`yahoo_cfb_boxscore`](reference/additional/yahoo-sports-shangrila#yahoo_cfb_boxscore) | [`yahoo_cfb_boxscore`](https://cfbfastR.sportsdataverse.org/reference/yahoo_cfb_boxscore.html) |
| [`yahoo_cfb_player_season_stats`](reference/additional/yahoo-sports-shangrila#yahoo_cfb_player_season_stats) | [`yahoo_cfb_player_season_stats`](https://cfbfastR.sportsdataverse.org/reference/yahoo_cfb_player_season_stats.html) |
| [`yahoo_cfb_player_season_stats_legacy`](reference/additional/yahoo-sports-shangrila#yahoo_cfb_player_season_stats_legacy) | [`yahoo_cfb_player_season_stats_legacy`](https://cfbfastR.sportsdataverse.org/reference/yahoo_cfb_player_season_stats_legacy.html) |
| [`yahoo_cfb_scoreboard`](reference/additional/yahoo-sports-shangrila#yahoo_cfb_scoreboard) | [`yahoo_cfb_scoreboard`](https://cfbfastR.sportsdataverse.org/reference/yahoo_cfb_scoreboard.html) |
| [`yahoo_cfb_team_season_stats`](reference/additional/yahoo-sports-shangrila#yahoo_cfb_team_season_stats) | [`yahoo_cfb_team_season_stats`](https://cfbfastR.sportsdataverse.org/reference/yahoo_cfb_team_season_stats.html) |
| [`yahoo_cfb_team_season_stats_legacy`](reference/additional/yahoo-sports-shangrila#yahoo_cfb_team_season_stats_legacy) | [`yahoo_cfb_team_season_stats_legacy`](https://cfbfastR.sportsdataverse.org/reference/yahoo_cfb_team_season_stats_legacy.html) |
