# NHL — NHL Records API

> NHL — NHL Records API — endpoint reference in sdv-py, the SportsDataverse Python package.

`sportsdataverse.nhl` — 44 endpoints.

## Allstar

| Function | Summary |
|---|---|
| [nhl_records_allstar_skater_career](nhl_records/allstar.md#nhl_records_allstar_skater_career) | All-Star Game career statistics for skaters. |
| [nhl_records_allstar_goalie_career](nhl_records/allstar.md#nhl_records_allstar_goalie_career) | All-Star Game career statistics for goaltenders. |
| [nhl_records_allstar_coach_career](nhl_records/allstar.md#nhl_records_allstar_coach_career) | All-Star Game career records for coaches. |
| [nhl_records_allstar_skater_game](nhl_records/allstar.md#nhl_records_allstar_skater_game) | All-Star Game single-game scoring records for skaters. |
| [nhl_records_allstar_goalie_game](nhl_records/allstar.md#nhl_records_allstar_goalie_game) | All-Star Game single-game stats for goaltenders. |

## Coach

| Function | Summary |
|---|---|
| [nhl_records_coach_career](nhl_records/coach.md#nhl_records_coach_career) | Coach career-records (regular season). |
| [nhl_records_coach_career_with_playoffs](nhl_records/coach.md#nhl_records_coach_career_with_playoffs) | Coach career records inclusive of regular season + playoffs. |
| [nhl_records_coach_franchise](nhl_records/coach.md#nhl_records_coach_franchise) | Coach records scoped to individual franchise stints. |
| [nhl_records_coach_stanley_cup](nhl_records/coach.md#nhl_records_coach_stanley_cup) | Coach Stanley Cup Final win streak and consecutive-cup records. |

## Franchise

| Function | Summary |
|---|---|
| [nhl_records_franchises](nhl_records/franchise.md#nhl_records_franchises) | List all NHL franchises (historical and active). |
| [nhl_records_franchise_detail](nhl_records/franchise.md#nhl_records_franchise_detail) | Franchise detail records (extended metadata per franchise). |
| [nhl_records_franchise_team_totals](nhl_records/franchise.md#nhl_records_franchise_team_totals) | All-time team totals per franchise (regular season). |
| [nhl_records_franchise_season_results](nhl_records/franchise.md#nhl_records_franchise_season_results) | Season-by-season results for each franchise. |
| [nhl_records_franchise_playoff_appearances](nhl_records/franchise.md#nhl_records_franchise_playoff_appearances) | Franchise playoff appearance counts and streak information. |
| [nhl_records_franchise_totals](nhl_records/franchise.md#nhl_records_franchise_totals) | League-wide franchise totals (all-time aggregate per franchise). |

## Goalie

| Function | Summary |
|---|---|
| [nhl_records_goalie_career_stats](nhl_records/goalie.md#nhl_records_goalie_career_stats) | Goaltender career statistics (regular season). |
| [nhl_records_goalie_career_stats_with_playoffs](nhl_records/goalie.md#nhl_records_goalie_career_stats_with_playoffs) | Goaltender career stats inclusive of regular season and playoffs. |
| [nhl_records_goalie_season_stats](nhl_records/goalie.md#nhl_records_goalie_season_stats) | Goaltender single-season statistics. |
| [nhl_records_goalie_win_streak](nhl_records/goalie.md#nhl_records_goalie_win_streak) | Goaltenders with the longest consecutive-win streaks. |
| [nhl_records_goalie_shutout_streak](nhl_records/goalie.md#nhl_records_goalie_shutout_streak) | Goaltenders with the longest consecutive-shutout streaks. |
| [nhl_records_goalie_win_plateaus](nhl_records/goalie.md#nhl_records_goalie_win_plateaus) | Goaltenders who reached each win plateau (100, 200, 300 …). |
| [nhl_records_goalie_playoff_streak](nhl_records/goalie.md#nhl_records_goalie_playoff_streak) | Goaltender consecutive playoff-win streaks. |
| [nhl_records_goalie_undefeated_streak](nhl_records/goalie.md#nhl_records_goalie_undefeated_streak) | Goaltender longest undefeated streaks (wins + ties). |

## Other

| Function | Summary |
|---|---|
| [nhl_records_awards](nhl_records/other.md#nhl_records_awards) | List all NHL award / trophy records. |
| [nhl_records_awards_by_franchise](nhl_records/other.md#nhl_records_awards_by_franchise) | List award records for a single franchise. |
| [nhl_records_awards_trophy_season](nhl_records/other.md#nhl_records_awards_trophy_season) | Retrieve the trophy winner for a specific season. |
| [nhl_records_coaches](nhl_records/other.md#nhl_records_coaches) | List NHL head coaches. |
| [nhl_records_coach](nhl_records/other.md#nhl_records_coach) | Retrieve one coach by their numeric ID. |
| [nhl_records_all_time_record_vs_franchise](nhl_records/other.md#nhl_records_all_time_record_vs_franchise) | All-time head-to-head records between every franchise pairing. |
| [nhl_records_skater_career_stats](nhl_records/other.md#nhl_records_skater_career_stats) | Skater career statistics (all-time, regular season). |
| [nhl_records_skater_career_leaders](nhl_records/other.md#nhl_records_skater_career_leaders) | All-time skater career leaderboards. |
| [nhl_records_consecutive_100pt_seasons](nhl_records/other.md#nhl_records_consecutive_100pt_seasons) | Skaters with the most consecutive 100-point seasons. |
| [nhl_records_draft](nhl_records/other.md#nhl_records_draft) | Retrieve NHL Entry Draft picks. |
| [nhl_records_draft_by_team](nhl_records/other.md#nhl_records_draft_by_team) | All draft picks made by a single team. |
| [nhl_records_draft_prospect](nhl_records/other.md#nhl_records_draft_prospect) | Draft prospect records. |
| [nhl_records_draft_lottery_odds](nhl_records/other.md#nhl_records_draft_lottery_odds) | Draft lottery odds (current year or filtered by season). |
| [nhl_records_expansion_draft_picks](nhl_records/other.md#nhl_records_expansion_draft_picks) | Expansion draft picks (e.g. Vegas 2017, Seattle 2021). |
| [nhl_records_attendance](nhl_records/other.md#nhl_records_attendance) | NHL arena attendance records. |
| [nhl_records_hof_players](nhl_records/other.md#nhl_records_hof_players) | Hockey Hall of Fame player inductees. |
| [nhl_records_hof_players_by_office](nhl_records/other.md#nhl_records_hof_players_by_office) | Hall of Fame players for a specific induction office/category. |
| [nhl_records_gm_career](nhl_records/other.md#nhl_records_gm_career) | General Manager career records. |
| [nhl_records_gm_franchise](nhl_records/other.md#nhl_records_gm_franchise) | General Manager records scoped to franchise stints. |
| [nhl_records_home_team_record](nhl_records/other.md#nhl_records_home_team_record) | League-wide home-team win/loss record by season. |
| [nhl_records_away_team_record](nhl_records/other.md#nhl_records_away_team_record) | League-wide away-team win/loss record by season. |
