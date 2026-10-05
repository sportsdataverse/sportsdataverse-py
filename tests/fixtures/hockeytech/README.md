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
| ahl_seasons | ahl | modulekit/seasons | all; sdv-internal-refs `hockeytech/captures/samples/ahl/seasons.json` (b78eb2c, live 2026-07-12), trim marker dropped, key redacted |
| ohl / whl / qmjhl / echl / sphl / chl / ushl / bchl / ajhl / sjhl / ojhl / cchl / gojhl / mhl / nojhl / vijhl / kijhl / mjhl `_seasons` | (18 leagues) | modulekit/seasons | all; live 2026-10-05 via `hockeytech_api(lg, "modulekit", "seasons", {})`, untrimmed, key redacted. `tests/hockeytech/test_season_names.py` reads every league's season names from these |
| ahl_pbp\_\* / ohl_pbp\_\* / whl_pbp\_\* / qmjhl_pbp\_\* | (juniors) | gameCenterPlayByPlay (dialect b) | per league |

HTTP-200 reply bodies that are not data, used by `tests/hockeytech/test_client.py`
to pin `hockeytech_api`'s error vocabulary:

| file | league | endpoint | provenance |
|------|--------|----------|------------|
| mjhl_gamesummary_7301_access_denied.txt | mjhl | gc/gamesummary, game 7301 | live 2026-10-05 (sdv-js T22a, `analytics/live-2026-10-05/mjhl_summary_7301.txt`); plain text, the key has no gamecenter access |
| pwhl_streaks_undefined_tab.json | pwhl | modulekit/streaks | live 2026-07-12, sdv-internal-refs `hockeytech/captures/samples/pwhl/streaks.json` (b78eb2c), key redacted |
| pwhl_svf_streaks_invalidview.json | pwhl | statviewfeed/streaks | live 2026-07-12, sdv-internal-refs `hockeytech/captures/samples/pwhl/svf_streaks.json` (b78eb2c) |

Refresh: re-run `tests/fixtures/hockeytech/_capture.py` (committed in task A1.3)
against a completed game.
