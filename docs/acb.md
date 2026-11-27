# ACB

> sdv-py ACB: endpoint references, dataset loaders and parsers for ACB in the SportsDataverse Python package.

# ACB (`sportsdataverse.basketball.acb`)

## Data sources

| Source | APIs / hosts | Functions | Auth |
|---|---|---:|---|
| [ACB (Liga Endesa)](#acb-liga-endesa) | `api2.acb.com` | 45 | none |

## ACB (Liga Endesa) {#acb-liga-endesa}

| Reference | Functions |
|---|---:|
| [ACB API (api2.acb.com, Spain)](reference/acb) | 45 |

## Examples

Worked examples — executed notebooks rendered as pages (refreshed weekly against the live APIs):

- [Quickstart](../tutorials/01_quickstart.md)

## Python ↔ R parity

Each `sportsdataverse` function and its equivalent in the sister R package, [`hoopR`](https://hoopR.sportsdataverse.org). Same-named where possible; the R column links the package's pkgdown reference.

| `sportsdataverse.basketball.acb` (Python) | `hoopR` (R) |
|---|---|
| [`acb_box_info`](reference/acb/other#acb_box_info) | [`acb_box_info`](https://hoopR.sportsdataverse.org/reference/acb_box_info.html) |
| [`acb_bracket`](reference/acb/other#acb_bracket) | [`acb_bracket`](https://hoopR.sportsdataverse.org/reference/acb_bracket.html) |
| [`acb_bracket_games`](reference/acb/other#acb_bracket_games) | [`acb_bracket_games`](https://hoopR.sportsdataverse.org/reference/acb_bracket_games.html) |
| [`acb_calendar`](reference/acb/other-2#acb_calendar) | [`acb_calendar`](https://hoopR.sportsdataverse.org/reference/acb_calendar.html) |
| [`acb_career_stints`](reference/acb/other-2#acb_career_stints) | [`acb_career_stints`](https://hoopR.sportsdataverse.org/reference/acb_career_stints.html) |
| [`acb_career_trophies`](reference/acb/other-2#acb_career_trophies) | [`acb_career_trophies`](https://hoopR.sportsdataverse.org/reference/acb_career_trophies.html) |
| [`acb_club_data`](reference/acb/other-2#acb_club_data) | [`acb_club_data`](https://hoopR.sportsdataverse.org/reference/acb_club_data.html) |
| [`acb_competition_current`](reference/acb/competition#acb_competition_current) | [`acb_competition_current`](https://hoopR.sportsdataverse.org/reference/acb_competition_current.html) |
| [`acb_competition_editions`](reference/acb/competition#acb_competition_editions) | [`acb_competition_editions`](https://hoopR.sportsdataverse.org/reference/acb_competition_editions.html) |
| [`acb_competition_rounds`](reference/acb/competition#acb_competition_rounds) | [`acb_competition_rounds`](https://hoopR.sportsdataverse.org/reference/acb_competition_rounds.html) |
| [`acb_competition_weeks`](reference/acb/competition#acb_competition_weeks) | [`acb_competition_weeks`](https://hoopR.sportsdataverse.org/reference/acb_competition_weeks.html) |
| [`acb_current_roster`](reference/acb/other-2#acb_current_roster) | [`acb_current_roster`](https://hoopR.sportsdataverse.org/reference/acb_current_roster.html) |
| [`acb_lead_stats`](reference/acb/other#acb_lead_stats) | [`acb_lead_stats`](https://hoopR.sportsdataverse.org/reference/acb_lead_stats.html) |
| [`acb_lead_tracker`](reference/acb/other#acb_lead_tracker) | [`acb_lead_tracker`](https://hoopR.sportsdataverse.org/reference/acb_lead_tracker.html) |
| [`acb_league_zones`](reference/acb/other-2#acb_league_zones) | [`acb_league_zones`](https://hoopR.sportsdataverse.org/reference/acb_league_zones.html) |
| [`acb_lineup`](reference/acb/other#acb_lineup) | [`acb_lineup`](https://hoopR.sportsdataverse.org/reference/acb_lineup.html) |
| [`acb_match_advanced_stats`](reference/acb/match#acb_match_advanced_stats) | [`acb_match_advanced_stats`](https://hoopR.sportsdataverse.org/reference/acb_match_advanced_stats.html) |
| [`acb_match_header`](reference/acb/match#acb_match_header) | [`acb_match_header`](https://hoopR.sportsdataverse.org/reference/acb_match_header.html) |
| [`acb_match_leaders`](reference/acb/match#acb_match_leaders) | [`acb_match_leaders`](https://hoopR.sportsdataverse.org/reference/acb_match_leaders.html) |
| [`acb_matchlist`](reference/acb/other#acb_matchlist) | [`acb_matchlist`](https://hoopR.sportsdataverse.org/reference/acb_matchlist.html) |
| [`acb_next_matches`](reference/acb/other-2#acb_next_matches) | [`acb_next_matches`](https://hoopR.sportsdataverse.org/reference/acb_next_matches.html) |
| [`acb_pbp`](reference/acb/other#acb_pbp) | [`acb_pbp`](https://hoopR.sportsdataverse.org/reference/acb_pbp.html) |
| [`acb_period_scores`](reference/acb/other#acb_period_scores) | [`acb_period_scores`](https://hoopR.sportsdataverse.org/reference/acb_period_scores.html) |
| [`acb_player_advanced_stats`](reference/acb/player#acb_player_advanced_stats) | [`acb_player_advanced_stats`](https://hoopR.sportsdataverse.org/reference/acb_player_advanced_stats.html) |
| [`acb_player_box`](reference/acb/player#acb_player_box) | [`acb_player_box`](https://hoopR.sportsdataverse.org/reference/acb_player_box.html) |
| [`acb_player_data`](reference/acb/player#acb_player_data) | [`acb_player_data`](https://hoopR.sportsdataverse.org/reference/acb_player_data.html) |
| [`acb_player_matches`](reference/acb/player#acb_player_matches) | [`acb_player_matches`](https://hoopR.sportsdataverse.org/reference/acb_player_matches.html) |
| [`acb_player_stats`](reference/acb/player#acb_player_stats) | [`acb_player_stats`](https://hoopR.sportsdataverse.org/reference/acb_player_stats.html) |
| [`acb_player_totals`](reference/acb/player#acb_player_totals) | [`acb_player_totals`](https://hoopR.sportsdataverse.org/reference/acb_player_totals.html) |
| [`acb_quickfacts`](reference/acb/other#acb_quickfacts) | [`acb_quickfacts`](https://hoopR.sportsdataverse.org/reference/acb_quickfacts.html) |
| [`acb_round_matches`](reference/acb/other#acb_round_matches) | [`acb_round_matches`](https://hoopR.sportsdataverse.org/reference/acb_round_matches.html) |
| [`acb_search`](reference/acb/other-2#acb_search) | [`acb_search`](https://hoopR.sportsdataverse.org/reference/acb_search.html) |
| [`acb_season_leaders`](reference/acb/other#acb_season_leaders) | [`acb_season_leaders`](https://hoopR.sportsdataverse.org/reference/acb_season_leaders.html) |
| [`acb_serie`](reference/acb/other#acb_serie) | [`acb_serie`](https://hoopR.sportsdataverse.org/reference/acb_serie.html) |
| [`acb_shots`](reference/acb/other#acb_shots) | [`acb_shots`](https://hoopR.sportsdataverse.org/reference/acb_shots.html) |
| [`acb_standings`](reference/acb/other#acb_standings) | [`acb_standings`](https://hoopR.sportsdataverse.org/reference/acb_standings.html) |
| [`acb_standings_rounds`](reference/acb/other#acb_standings_rounds) | [`acb_standings_rounds`](https://hoopR.sportsdataverse.org/reference/acb_standings_rounds.html) |
| [`acb_team_box`](reference/acb/team#acb_team_box) | [`acb_team_box`](https://hoopR.sportsdataverse.org/reference/acb_team_box.html) |
| [`acb_team_comparison`](reference/acb/team#acb_team_comparison) | [`acb_team_comparison`](https://hoopR.sportsdataverse.org/reference/acb_team_comparison.html) |
| [`acb_team_leaders`](reference/acb/team#acb_team_leaders) | [`acb_team_leaders`](https://hoopR.sportsdataverse.org/reference/acb_team_leaders.html) |
| [`acb_team_matches`](reference/acb/team#acb_team_matches) | [`acb_team_matches`](https://hoopR.sportsdataverse.org/reference/acb_team_matches.html) |
| [`acb_team_records`](reference/acb/team#acb_team_records) | [`acb_team_records`](https://hoopR.sportsdataverse.org/reference/acb_team_records.html) |
| [`acb_team_stats`](reference/acb/team#acb_team_stats) | [`acb_team_stats`](https://hoopR.sportsdataverse.org/reference/acb_team_stats.html) |
| [`acb_team_totals`](reference/acb/team#acb_team_totals) | [`acb_team_totals`](https://hoopR.sportsdataverse.org/reference/acb_team_totals.html) |
| [`acb_teams`](reference/acb/team#acb_teams) | [`acb_teams`](https://hoopR.sportsdataverse.org/reference/acb_teams.html) |
