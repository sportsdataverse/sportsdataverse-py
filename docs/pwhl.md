# PWHL

> sdv-py PWHL: endpoint references, dataset loaders and parsers for PWHL in the SportsDataverse Python package.

# PWHL (`sportsdataverse.pwhl`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [sportsdataverse-data releases](#sportsdataverse-data-releases) | `github.com` | 27 | none |
| [HockeyTech / LeagueStat](#hockeytech-leaguestat) | `lscluster.hockeytech.com` | 25 | per-league public key (SDV__API_KEY) |
| [Additional functions](reference/additional) | hand-written wrappers & helpers | 45 | — |

## sportsdataverse-data releases {#sportsdataverse-data-releases}

| Reference | Functions |
|---|---:|
| [sportsdataverse-data releases](reference/loaders) | 21 |
| [Hand-written wrappers](reference/additional/sportsdataverse-data-releases) | 6 |

## HockeyTech / LeagueStat {#hockeytech-leaguestat}

| Reference | Functions |
|---|---:|
| [Hand-written wrappers](reference/additional/hockeytech-leaguestat) | 25 |
## Tools and helpers

### Models and calculators {#models-and-calculators}

- [`LeagueConstants`](reference/additional/models-and-calculators#LeagueConstants)
- [`as_of_ratings_split`](reference/additional/models-and-calculators#as_of_ratings_split)
- [`brier_score`](reference/additional/models-and-calculators#brier_score)
- [`calibration_table`](reference/additional/models-and-calculators#calibration_table)
- [`log_loss_score`](reference/additional/models-and-calculators#log_loss_score)
- [`mae`](reference/additional/models-and-calculators#mae)
- [`pwhl_team_ratings`](reference/additional/models-and-calculators#pwhl_team_ratings)
- [`spearman_corr`](reference/additional/models-and-calculators#spearman_corr)

### Analytics {#analytics}

- [`pwhl_game_total`](reference/additional/analytics#pwhl_game_total)
- [`pwhl_in_game_win_prob`](reference/additional/analytics#pwhl_in_game_win_prob)
- [`pwhl_player_props`](reference/additional/analytics#pwhl_player_props)
- [`pwhl_predict_games`](reference/additional/analytics#pwhl_predict_games)

### Dates and seasons {#dates-and-seasons}

- [`most_recent_pwhl_season`](reference/additional/dates-and-seasons#most_recent_pwhl_season)
- [`pwhl_season_id`](reference/additional/dates-and-seasons#pwhl_season_id)

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)
- [PWHL tutorial](../tutorials/10_pwhl_intro.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`fastRhockey`](https://github.com/sportsdataverse). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.pwhl` (Python) | `fastRhockey` (R) |
|---|---|
| [`load_phf_pbp`](reference/loaders#load_phf_pbp) | [`load_phf_pbp`](https://fastRhockey.sportsdataverse.org/reference/load_phf_pbp.html) |
| [`load_pwhl_game_info`](reference/loaders#load_pwhl_game_info) | [`load_pwhl_game_info`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_game_info.html) |
| [`load_pwhl_game_rosters`](reference/loaders#load_pwhl_game_rosters) | [`load_pwhl_game_rosters`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_game_rosters.html) |
| [`load_pwhl_goalie_box`](reference/additional/sportsdataverse-data-releases#load_pwhl_goalie_box) | [`load_pwhl_goalie_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_goalie_box.html) |
| [`load_pwhl_goalie_boxscores`](reference/loaders#load_pwhl_goalie_boxscores) | [`load_pwhl_goalie_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_goalie_boxscores.html) |
| [`load_pwhl_officials`](reference/loaders#load_pwhl_officials) | [`load_pwhl_officials`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_officials.html) |
| [`load_pwhl_pbp`](reference/loaders#load_pwhl_pbp) | [`load_pwhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_pbp.html) |
| [`load_pwhl_penalty_summary`](reference/loaders#load_pwhl_penalty_summary) | [`load_pwhl_penalty_summary`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_penalty_summary.html) |
| [`load_pwhl_player_box`](reference/additional/sportsdataverse-data-releases#load_pwhl_player_box) | [`load_pwhl_player_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_player_box.html) |
| [`load_pwhl_player_boxscores`](reference/loaders#load_pwhl_player_boxscores) | [`load_pwhl_player_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_player_boxscores.html) |
| [`load_pwhl_rosters`](reference/loaders#load_pwhl_rosters) | [`load_pwhl_rosters`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_rosters.html) |
| [`load_pwhl_schedule`](reference/additional/sportsdataverse-data-releases#load_pwhl_schedule) | [`load_pwhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_schedule.html) |
| [`load_pwhl_schedules`](reference/loaders#load_pwhl_schedules) | [`load_pwhl_schedules`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_schedules.html) |
| [`load_pwhl_scoring_summary`](reference/loaders#load_pwhl_scoring_summary) | [`load_pwhl_scoring_summary`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_scoring_summary.html) |
| [`load_pwhl_shifts`](reference/loaders#load_pwhl_shifts) | [`load_pwhl_shifts`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_shifts.html) |
| [`load_pwhl_shootout`](reference/loaders#load_pwhl_shootout) | [`load_pwhl_shootout`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_shootout.html) |
| [`load_pwhl_shots_by_period`](reference/loaders#load_pwhl_shots_by_period) | [`load_pwhl_shots_by_period`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_shots_by_period.html) |
| [`load_pwhl_skater_box`](reference/additional/sportsdataverse-data-releases#load_pwhl_skater_box) | [`load_pwhl_skater_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_skater_box.html) |
| [`load_pwhl_skater_boxscores`](reference/loaders#load_pwhl_skater_boxscores) | [`load_pwhl_skater_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_skater_boxscores.html) |
| [`load_pwhl_team_box`](reference/additional/sportsdataverse-data-releases#load_pwhl_team_box) | [`load_pwhl_team_box`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_team_box.html) |
| [`load_pwhl_team_boxscores`](reference/loaders#load_pwhl_team_boxscores) | [`load_pwhl_team_boxscores`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_team_boxscores.html) |
| [`load_pwhl_three_stars`](reference/loaders#load_pwhl_three_stars) | [`load_pwhl_three_stars`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_three_stars.html) |
| [`load_pwhl_xg_pbp`](reference/loaders#load_pwhl_xg_pbp) | [`load_pwhl_xg_pbp`](https://fastRhockey.sportsdataverse.org/reference/load_pwhl_xg_pbp.html) |
| [`most_recent_pwhl_season`](reference/additional/dates-and-seasons#most_recent_pwhl_season) | [`most_recent_pwhl_season`](https://fastRhockey.sportsdataverse.org/reference/most_recent_pwhl_season.html) |
| [`pwhl_game_corsi`](reference/additional/hockeytech-leaguestat#pwhl_game_corsi) | [`pwhl_game_corsi`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_corsi.html) |
| [`pwhl_game_info`](reference/additional/hockeytech-leaguestat#pwhl_game_info) | [`pwhl_game_info`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_info.html) |
| [`pwhl_game_shifts`](reference/additional/hockeytech-leaguestat#pwhl_game_shifts) | [`pwhl_game_shifts`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_shifts.html) |
| [`pwhl_game_summary`](reference/additional/hockeytech-leaguestat#pwhl_game_summary) | [`pwhl_game_summary`](https://fastRhockey.sportsdataverse.org/reference/pwhl_game_summary.html) |
| [`pwhl_leaders`](reference/additional/hockeytech-leaguestat#pwhl_leaders) | [`pwhl_leaders`](https://fastRhockey.sportsdataverse.org/reference/pwhl_leaders.html) |
| [`pwhl_pbp`](reference/additional/hockeytech-leaguestat#pwhl_pbp) | [`pwhl_pbp`](https://fastRhockey.sportsdataverse.org/reference/pwhl_pbp.html) |
| [`pwhl_player_box`](reference/additional/hockeytech-leaguestat#pwhl_player_box) | [`pwhl_player_box`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_box.html) |
| [`pwhl_player_game_log`](reference/additional/hockeytech-leaguestat#pwhl_player_game_log) | [`pwhl_player_game_log`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_game_log.html) |
| [`pwhl_player_info`](reference/additional/hockeytech-leaguestat#pwhl_player_info) | [`pwhl_player_info`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_info.html) |
| [`pwhl_player_search`](reference/additional/hockeytech-leaguestat#pwhl_player_search) | [`pwhl_player_search`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_search.html) |
| [`pwhl_player_stats`](reference/additional/hockeytech-leaguestat#pwhl_player_stats) | [`pwhl_player_stats`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_stats.html) |
| [`pwhl_player_toi`](reference/additional/hockeytech-leaguestat#pwhl_player_toi) | [`pwhl_player_toi`](https://fastRhockey.sportsdataverse.org/reference/pwhl_player_toi.html) |
| [`pwhl_playoff_bracket`](reference/additional/hockeytech-leaguestat#pwhl_playoff_bracket) | [`pwhl_playoff_bracket`](https://fastRhockey.sportsdataverse.org/reference/pwhl_playoff_bracket.html) |
| [`pwhl_schedule`](reference/additional/hockeytech-leaguestat#pwhl_schedule) | [`pwhl_schedule`](https://fastRhockey.sportsdataverse.org/reference/pwhl_schedule.html) |
| [`pwhl_scorebar`](reference/additional/hockeytech-leaguestat#pwhl_scorebar) | [`pwhl_scorebar`](https://fastRhockey.sportsdataverse.org/reference/pwhl_scorebar.html) |
| [`pwhl_season_id`](reference/additional/dates-and-seasons#pwhl_season_id) | [`pwhl_season_id`](https://fastRhockey.sportsdataverse.org/reference/pwhl_season_id.html) |
| [`pwhl_standings`](reference/additional/hockeytech-leaguestat#pwhl_standings) | [`pwhl_standings`](https://fastRhockey.sportsdataverse.org/reference/pwhl_standings.html) |
| [`pwhl_stats`](reference/additional/hockeytech-leaguestat#pwhl_stats) | [`pwhl_stats`](https://fastRhockey.sportsdataverse.org/reference/pwhl_stats.html) |
| [`pwhl_streaks`](reference/additional/hockeytech-leaguestat#pwhl_streaks) | [`pwhl_streaks`](https://fastRhockey.sportsdataverse.org/reference/pwhl_streaks.html) |
| [`pwhl_team_roster`](reference/additional/hockeytech-leaguestat#pwhl_team_roster) | [`pwhl_team_roster`](https://fastRhockey.sportsdataverse.org/reference/pwhl_team_roster.html) |
| [`pwhl_teams`](reference/additional/hockeytech-leaguestat#pwhl_teams) | [`pwhl_teams`](https://fastRhockey.sportsdataverse.org/reference/pwhl_teams.html) |
| [`pwhl_transactions`](reference/additional/hockeytech-leaguestat#pwhl_transactions) | [`pwhl_transactions`](https://fastRhockey.sportsdataverse.org/reference/pwhl_transactions.html) |
