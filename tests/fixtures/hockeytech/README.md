<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [HockeyTech fixtures](#hockeytech-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# HockeyTech fixtures

Captured JSON payloads from `lscluster.hockeytech.com` / `cluster.leaguestat.com`
(JSONP `angular.callbacks._N(...)` wrapper already stripped). Provenance:

| stem | league | endpoint | game/season |
|------|--------|----------|-------------|
| pwhl_schedule_2025 | pwhl | modulekit/scorebar | season_id 5 |
| pwhl_pbp_42 | pwhl | statviewfeed/gameCenterPlayByPlay | game_id 42 |
| pwhl_gameshifts_42 | pwhl | modulekit/gameshifts | game_id 42 |
| pwhl_seasons | pwhl | modulekit/seasons | all |
| pwhl_standings_5 | pwhl | statviewfeed/teams | season_id 5 |
| pwhl_teams_5 | pwhl | modulekit/teamsbyseason | season_id 5 |
| pwhl_roster_1_5 | pwhl | modulekit/roster | team 1 season 5 |
| pwhl_player_stats_27 | pwhl | modulekit/player seasonstats | player 27 |
| pwhl_leaders_5 | pwhl | statviewfeed/leadersExtended | season_id 5 |
| pwhl_game_summary_42 | pwhl | gc/gamesummary | game_id 42 |
| ahl_pbp\_\* / ohl_pbp\_\* / whl_pbp\_\* / qmjhl_pbp\_\* | (juniors) | gameCenterPlayByPlay (dialect b) | per league |

Refresh: re-run `tests/fixtures/hockeytech/_capture.py` (committed in task A1.3)
against a completed game.
