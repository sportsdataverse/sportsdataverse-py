# NFL — PFF Developer API (api.pff.com, API key)

> NFL — PFF Developer API (api.pff.com, API key) — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.nfl` — 68 endpoints.

## Facet

| Function | Summary |
|---|---|
| [pff_api_facet_offense_summary](pff_api/facet.md#pff_api_facet_offense_summary) | League-wide offense summary leaderboard |
| [pff_api_facet_offense_blocking](pff_api/facet.md#pff_api_facet_offense_blocking) | League-wide blocking leaderboard |
| [pff_api_facet_offense_pass_blocking](pff_api/facet.md#pff_api_facet_offense_pass_blocking) | League-wide pass-blocking leaderboard |
| [pff_api_facet_offense_run_blocking](pff_api/facet.md#pff_api_facet_offense_run_blocking) | League-wide run-blocking leaderboard |
| [pff_api_facet_passing_allowed_pressure](pff_api/facet.md#pff_api_facet_passing_allowed_pressure) | League-wide pressure-allowed leaderboard |
| [pff_api_facet_passing_concept](pff_api/facet.md#pff_api_facet_passing_concept) | League-wide passing-by-concept leaderboard |
| [pff_api_facet_passing_depth](pff_api/facet-2.md#pff_api_facet_passing_depth) | League-wide passing-by-depth leaderboard |
| [pff_api_facet_passing_detail](pff_api/facet-3.md#pff_api_facet_passing_detail) | League-wide passing detail leaderboard |
| [pff_api_facet_passing_pressure](pff_api/facet-4.md#pff_api_facet_passing_pressure) | League-wide passing-under-pressure leaderboard |
| [pff_api_facet_passing_summary](pff_api/facet-4.md#pff_api_facet_passing_summary) | League-wide passing summary leaderboard |
| [pff_api_facet_receiving_concept](pff_api/facet-4.md#pff_api_facet_receiving_concept) | League-wide receiving-by-concept leaderboard |
| [pff_api_facet_receiving_coverage](pff_api/facet-4.md#pff_api_facet_receiving_coverage) | League-wide receiving-versus-coverage leaderboard |
| [pff_api_facet_receiving_depth](pff_api/facet-5.md#pff_api_facet_receiving_depth) | League-wide receiving-by-depth leaderboard |
| [pff_api_facet_receiving_scheme](pff_api/facet-6.md#pff_api_facet_receiving_scheme) | League-wide receiving-by-scheme leaderboard |
| [pff_api_facet_receiving_summary](pff_api/facet-6.md#pff_api_facet_receiving_summary) | League-wide receiving summary leaderboard |
| [pff_api_facet_rushing_direction](pff_api/facet-6.md#pff_api_facet_rushing_direction) | League-wide rushing-by-direction leaderboard |
| [pff_api_facet_rushing_summary](pff_api/facet-6.md#pff_api_facet_rushing_summary) | League-wide rushing summary leaderboard |
| [pff_api_facet_defense_coverage](pff_api/facet-6.md#pff_api_facet_defense_coverage) | League-wide coverage leaderboard |
| [pff_api_facet_defense_coverage_scheme](pff_api/facet-6.md#pff_api_facet_defense_coverage_scheme) | League-wide coverage-by-scheme leaderboard |
| [pff_api_facet_defense_coverage_matchup](pff_api/facet-6.md#pff_api_facet_defense_coverage_matchup) | League-wide coverage matchup leaderboard |
| [pff_api_facet_defense_pass_rush](pff_api/facet-6.md#pff_api_facet_defense_pass_rush) | League-wide pass-rush leaderboard |
| [pff_api_facet_defense_run](pff_api/facet-6.md#pff_api_facet_defense_run) | League-wide run-defense leaderboard |
| [pff_api_facet_defense_summary](pff_api/facet-6.md#pff_api_facet_defense_summary) | League-wide defense summary leaderboard |
| [pff_api_facet_field_goal_summary](pff_api/facet-6.md#pff_api_facet_field_goal_summary) | League-wide field-goal kicking leaderboard |
| [pff_api_facet_kickoff_summary](pff_api/facet-6.md#pff_api_facet_kickoff_summary) | League-wide kickoff leaderboard |
| [pff_api_facet_punting_summary](pff_api/facet-7.md#pff_api_facet_punting_summary) | League-wide punting leaderboard |
| [pff_api_facet_return_summary](pff_api/facet-7.md#pff_api_facet_return_summary) | League-wide return leaderboard |
| [pff_api_facet_special_summary](pff_api/facet-7.md#pff_api_facet_special_summary) | League-wide special-teams leaderboard |

## Player

| Function | Summary |
|---|---|
| [pff_api_player_seasons](pff_api/player.md#pff_api_player_seasons) | List the seasons a player has data for |
| [pff_api_player_snaps_summary](pff_api/player.md#pff_api_player_snaps_summary) | Snap counts for a player, broken out by position |
| [pff_api_player_position_pivot](pff_api/player.md#pff_api_player_position_pivot) | Player snap counts pivoted by position |
| [pff_api_player_offense_summary](pff_api/player.md#pff_api_player_offense_summary) | Offense summary for one player |
| [pff_api_player_offense_blocking](pff_api/player.md#pff_api_player_offense_blocking) | Blocking report for one player |
| [pff_api_player_offense_pass_blocking](pff_api/player.md#pff_api_player_offense_pass_blocking) | Pass-blocking report for one player |
| [pff_api_player_offense_run_blocking](pff_api/player.md#pff_api_player_offense_run_blocking) | Run-blocking report for one player |
| [pff_api_player_passing_summary](pff_api/player.md#pff_api_player_passing_summary) | Passing summary for one player |
| [pff_api_player_passing_concept](pff_api/player.md#pff_api_player_passing_concept) | Passing by play concept for one player |
| [pff_api_player_passing_depth](pff_api/player-2.md#pff_api_player_passing_depth) | Passing by target depth for one player |
| [pff_api_player_passing_pressure](pff_api/player-3.md#pff_api_player_passing_pressure) | Passing under pressure for one player |
| [pff_api_player_rushing_direction](pff_api/player-3.md#pff_api_player_rushing_direction) | Rushing by direction for one player |
| [pff_api_player_rushing_summary](pff_api/player-3.md#pff_api_player_rushing_summary) | Rushing summary for one player |
| [pff_api_player_receiving_depth](pff_api/player-4.md#pff_api_player_receiving_depth) | Receiving by target depth for one player |
| [pff_api_player_receiving_summary](pff_api/player-5.md#pff_api_player_receiving_summary) | Receiving summary for one player |
| [pff_api_player_defense_summary](pff_api/player-5.md#pff_api_player_defense_summary) | Defense summary for one player |
| [pff_api_player_field_goal_summary](pff_api/player-5.md#pff_api_player_field_goal_summary) | Field-goal kicking for one player |
| [pff_api_player_kickoff_summary](pff_api/player-5.md#pff_api_player_kickoff_summary) | Kickoffs for one player |
| [pff_api_player_punting_summary](pff_api/player-5.md#pff_api_player_punting_summary) | Punting for one player |
| [pff_api_player_return_summary](pff_api/player-5.md#pff_api_player_return_summary) | Kick and punt returns for one player |
| [pff_api_player_special_summary](pff_api/player-5.md#pff_api_player_special_summary) | Special-teams summary for one player |

## Signature

| Function | Summary |
|---|---|
| [pff_api_signature_passing_time_in_pocket](pff_api/signature.md#pff_api_signature_passing_time_in_pocket) | Signature stat: time in pocket |
| [pff_api_signature_pass_blocking_efficiency_line](pff_api/signature.md#pff_api_signature_pass_blocking_efficiency_line) | Signature stat: pass-blocking efficiency, by line |
| [pff_api_signature_defense_outside_pass_rush](pff_api/signature.md#pff_api_signature_defense_outside_pass_rush) | Signature stat: outside pass rush |
| [pff_api_signature_defense_slot_coverage](pff_api/signature.md#pff_api_signature_defense_slot_coverage) | Signature stat: slot coverage |

## Team

| Function | Summary |
|---|---|
| [pff_api_team_list](pff_api/team.md#pff_api_team_list) | List a season's teams, franchise groups and schedule |
| [pff_api_team_overview](pff_api/team.md#pff_api_team_overview) | Season-to-date team report, one row per team |
| [pff_api_team_summary](pff_api/team.md#pff_api_team_summary) | Per-game team report for one franchise, one row per game |
| [pff_api_team_directory](pff_api/team.md#pff_api_team_directory) | The league's teams for a season, with ids, slugs, colours and groups |
| [pff_api_team_stats](pff_api/team.md#pff_api_team_stats) | Team stats table for one category, every value ranked against the scope |
| [pff_api_team_roster](pff_api/team.md#pff_api_team_roster) | A team's depth-chart roster with grades, ranks and snap counts |
| [pff_api_team_schedule](pff_api/team.md#pff_api_team_schedule) | A team's season schedule, with results and strength of schedule |
| [pff_api_team_leaders](pff_api/team-2.md#pff_api_team_leaders) | A team's leaders for one position group, with rank and percentile |
| [pff_api_team_rushing_direction](pff_api/team-2.md#pff_api_team_rushing_direction) | A team's rushing by direction, one row per rusher and gap, plus totals |
| [pff_api_team_report](pff_api/team-3.md#pff_api_team_report) | One of nineteen player reports for a team, one row per player |

## Other

| Function | Summary |
|---|---|
| [pff_api_ref_leagues](pff_api/other.md#pff_api_ref_leagues) | List the leagues you can read, with their seasons and weeks |
| [pff_api_ref_games](pff_api/other.md#pff_api_ref_games) | List game results for a league, season and week |
| [pff_api_ref_players](pff_api/other.md#pff_api_ref_players) | Search the player directory by name or id |
| [pff_api_whoami](pff_api/other.md#pff_api_whoami) | Show what this API believes about the current credential |
| [pff_api_position_report](pff_api/other-2.md#pff_api_position_report) | One of nineteen player reports for the whole league, one row per player |
