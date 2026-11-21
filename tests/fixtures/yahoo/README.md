<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Yahoo Sports fixtures](#yahoo-sports-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Yahoo Sports fixtures

| file | source | notes |
|---|---|---|
| `shangrila_team_season_stats_ncaaf_2024.json` | `GET https://graphite-secure.sports.yahoo.com/v1/query/shangrila/leagueStatsByTeam?lang=en-US&region=US&tz=America%2FChicago&leagues=ncaaf&season=2024&count=200&leagueStructureId=ncaaf.struct.div.1` through `yahoo_cfb_team_season_stats(season=2024, return_parsed=False)`, captured 2026-10-11 | HTTP 200, 2.9 MB; trimmed to the first 3 of 134 teams in `data.leagues[0].footballStats` (Clemson, Duke, Florida St.), every other key kept. Used by `tests/cfb/test_cfb_yahoo_ext.py` (B-L50). |
| `shangrila_league_teams_ncaaf.json` | `GET .../shangrila/leagueTeams?lang=en-US&region=US&tz=America%2FChicago&league=ncaaf&season=2025`, captured 2026-11-20 | HTTP 200, 845 KB; trimmed to 3 of 539 teams (Clemson FBS, Montana FCS, Bowie State D2) with the four logo keys dropped. Used by `tests/cfb/test_cfb_yahoo_ext.py`. |
| `shangrila_league_game_ids_by_date_ncaaf_2025_w5.json` | `GET .../shangrila/leagueGameIdsByDate?...&leagues=ncaaf&season=2025&week=5`, captured 2026-11-20 | HTTP 200; trimmed to the first 3 of 107 game ids. Used by `test_cfb_yahoo_ext.py` and `test_cfb_crosswalk.py`. |
| `shangrila_scoreboard_game_ncaaf_2025_w5.json` | `GET .../shangrila/scoreboardGame?...&gameId=<the first 5 week-5 ids, comma-joined>`, captured 2026-11-20 | HTTP 200, 105 KB; kept the 3 games in the id fixture and dropped each game's `bets`, `gameOddsSummary`, `brief` and the all-null `teams`. Same users. |
| `shangrila_playbook_boxscore_ncaaf.json` | `GET .../shangrila/playbookBoxscore?...&gameId=ncaaf.g.202509270116&isFootball=true`, captured 2026-11-20 | HTTP 200, 90 KB; each lineup trimmed to 2 players with stats + 1 without, `footballStatTypes` to the stats used, `footballPositions` to 3; drives, play-by-play, win probability, odds, recap videos, series history and the teams' `lastGames` / `injuredPlayers` / `rankPolls` / `teamStandings` / `playoffSeeds` dropped. Used by `test_cfb_yahoo_ext.py`. |

`...` is `https://graphite-secure.sports.yahoo.com/v1/query/shangrila` plus `lang=en-US&region=US&tz=America%2FChicago`.
Yahoo retired its editorial feed on 2026-11-20 (HTTP 404 for every league). The `editorial_*` captures went with the
`yahoo_editorial_*` wrappers and `parse_yahoo_editorial`.

The other files in this directory came with #452 (`407db46cd8`, 2026-09-03) and predate this README.
