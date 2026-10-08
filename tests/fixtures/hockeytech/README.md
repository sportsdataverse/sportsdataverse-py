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
| pwhl_schedule_2025 | pwhl | modulekit/scorebar | sent season_id 5; holds 200 rows across 6 seasons (90 in season 5), because scorebar ignores season_id |
| pwhl_schedule_8 | pwhl | modulekit/schedule | season_id 8 (2025-26 regular season), all 120 games; live 2026-10-08 via `hockeytech_api`, key redacted |
| pwhl_pbp_42 | pwhl | statviewfeed/gameCenterPlayByPlay | game_id 42 |
| pwhl_gameshifts_42 | pwhl | modulekit/gameshifts | game_id 42 |
| pwhl_seasons | pwhl | modulekit/seasons | all as committed 2026-06-09 (#95): ids 1-10, ending at the "2026-27 Pre-Season" (the live feed added id 11, the 2026-27 regular season, later). Kept as a real preseason-before-regular-season snapshot |
| pwhl_standings_5 | pwhl | statviewfeed/teams | season_id 5 |
| pwhl_teams_5 | pwhl | modulekit/teamsbyseason | season_id 5 |
| pwhl_roster_1_5 | pwhl | modulekit/roster | team 1 season 5 |
| pwhl_player_stats_27 | pwhl | modulekit/player seasonstats | player 27 |
| pwhl_leaders_5 | pwhl | statviewfeed/leadersExtended | season_id 5 |
| pwhl_game_summary_42 | pwhl | gc/gamesummary | game_id 42 |
| pwhl_transactions | pwhl | modulekit/transactions | newest page (20 of 171); sdv-internal-refs `hockeytech/captures/samples/pwhl/transactions.json` (b78eb2c, live 2026-07-12), key redacted |
| pwhl_brackets_9 | pwhl | modulekit/brackets | season_id 9 (2 rounds, 3 series); sdv-internal-refs `samples/pwhl/brackets.json` (b78eb2c), key redacted |
| pwhl_player_gamebygame | pwhl | modulekit/player gamebygame | player 12 season 10, a real reply with `games: []`; sdv-internal-refs `samples/pwhl/player_gamebygame.json` (b78eb2c), key redacted |
| pwhl_player_gamebygame_27_5 | pwhl | modulekit/player gamebygame | player 27 season 5, live 2026-10-07 via `hockeytech_api`, trimmed to 5 of 9 games, key redacted |
| ahl_seasons | ahl | modulekit/seasons | all; sdv-internal-refs `hockeytech/captures/samples/ahl/seasons.json` (b78eb2c, live 2026-07-12), trim marker dropped, key redacted |
| ohl / whl / qmjhl / echl / sphl / chl / ushl / bchl / ajhl / sjhl / ojhl / cchl / gojhl / mhl / nojhl / vijhl / kijhl / mjhl `_seasons` | (18 leagues) | modulekit/seasons | all; live 2026-10-05 via `hockeytech_api(lg, "modulekit", "seasons", {})`, untrimmed, key redacted. `tests/hockeytech/test_season_names.py` reads every league's season names from these and from `ahl_seasons` / `pwhl_seasons` |
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
