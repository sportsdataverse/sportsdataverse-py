<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Archive crosswalk fixtures](#archive-crosswalk-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Archive crosswalk fixtures

Real, trimmed inputs for `tests/test_crosswalk_basketball_archive.py`, copied 2026-10-11.

| Path | Source |
|---|---|
| `nba_stats/json/boxscoretraditionalv3/2015/0041400405.json`, `0041400406.json` | `hoopR-nba-stats-raw` (stats.nba.com, 2015 NBA Finals games 5 and 6), unmodified |
| `nba_stats/json/boxscoresummaryv2/2015/0041400405.json` | `hoopR-nba-stats-raw`, unmodified. Game 6 has no summary on purpose, so its date comes from the game log. |
| `nba_stats/json/leaguegamelog/2015/playoffs.json` | `hoopR-nba-stats-raw`, `rowSet` trimmed to game `0041400406` |
| `wnba_stats/json/boxscoretraditionalv3/2015/1041500305.json`, `boxscoresummaryv2/2015/1041500305.json` | `wehoop-wnba-stats-raw` (stats.wnba.com, 2015 WNBA Finals game 5), unmodified |
| `espn/{nba,wnba}_schedule_2015.parquet` | `hoopR-nba-data` / `wehoop-wnba-data` `schedules/parquet/*_schedule_2015.parquet`, filtered to those games and the columns the builder reads |
| `espn/{nba,wnba}_game_rosters_2015.parquet` | `*/game_rosters/parquet/game_rosters_2015.parquet`, filtered the same way |
| `fox_nba_scores_segment_20150614.json`, `fox_nba_scores_segment_20150616.json` | `api.foxsports.com/bifrost/v1/nba/league/scores-segment/{date}` (the same two Finals games), captured 2026-10-11; image, sort and favorite keys dropped |
| `yahoo_wnba_league_teams.json` | Yahoo shangrila `leagueTeams?league=wnba`, captured 2026-10-11 (today's 15 teams); logo, color, alias, division, conference and standings keys dropped |
| `yahoo_wnba_game_ids_20151014.json` | Yahoo shangrila `leagueGameIdsByDate?leagues=wnba&startRange=2015-10-14&endRange=2015-10-14&season=2015` (2015 WNBA Finals game 5), captured 2026-10-11, unmodified |
