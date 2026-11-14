<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Season producer fixtures (supp-013)](#season-producer-fixtures-supp-013)
  - [College season-stats counts (`tests/mbb/test_mbb_season_stats_counts.py`)](#college-season-stats-counts-testsmbbtest_mbb_season_stats_countspy)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Season producer fixtures (supp-013)

Raw ESPN standings payloads from the raw repos and the R-released rows built from them: the oracle for
`helper_nba_standings` (`sportsdataverse/nba/nba_season_producers.py`) and `helper_wbb_standings`
(`sportsdataverse/wbb/wbb_season_producers.py`). Read by `tests/nba/test_nba_season_producers.py` and
`tests/wbb/test_wbb_season_producers.py`. Built 2026-10-11.

| file | source |
|---|---|
| `nba_standings_2024.json.gz` | `hoopR-nba-raw` `nba/standings/json/2024.json` @ `4ce184e3366` (2026-05-30), whole payload, gzip |
| `nba_standings_2024_release.parquet` | the R producer's release asset `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_nba_standings/standings_2024.parquet` (690 rows, `load_nba_standings([2024])`) |
| `wbb_standings_2026_trimmed.json.gz` | `wehoop-wbb-raw` `wbb/standings/json/2026.json` @ `bca4b5bbf53` (2026-05-12), trimmed to the top-level `uid/id/name/abbreviation/shortName/season` plus the first two `children` (America East `1`, American `62`); the full file is 6.8 MB |
| `wbb_standings_2026_release.parquet` | `https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_womens_college_basketball_standings/standings_2026.parquet` (`load_wbb_standings([2026])`) filtered to `group_id` in `{1, 62}` (1,848 rows) |
| `wnba_player_season_stats_2025_3917450.json` | `wehoop-wnba-raw` `wnba/player_season_stats/json/2025/3917450.json` @ `880bc368b` (2026-05-07), whole payload: Napheesa Collier, seven Lynx seasons. Career `totals` gp 193 / avgPoints 18.4; the 2025 `statistics[]` row gp 33 / avgPoints 22.9 (#1087) |
| `wnba_player_season_stats_2025_1107.json` | `wehoop-wnba-raw` `wnba/player_season_stats/json/2025/1107.json` @ `880bc368b` (2026-05-07), whole payload: traded in 2025 (Sparks, Fever, plus a `2025 Totals` row); career gp 131, 2025 Totals gp 15 (#1087) |

## College season-stats counts (`tests/mbb/test_mbb_season_stats_counts.py`)

Captured 2026-11-13. Nothing is synthetic.

| file | source |
|---|---|
| `mbb_player_season_stats_4432243.json` | `hoopR-mbb-raw` `mbb/player_season_stats/json/4432243.json` @ `e65c30ac4c0` (2026-05-30), whole payload: a Texas guard whose 2025-26 totals carry `freeThrowsMade-freeThrowsAttempted` as `"83-111"` |
| `mbb_team_stats_2026_150_site.json.gz` | `https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams/150/statistics?season=2026` (Duke): `requestedSeason.type` 2, 34 games, 973 FGM, gzip |
| `mbb_team_stats_2026_150_core_type0.json.gz` | `https://sports.core.api.espn.com/v2/sports/basketball/leagues/mens-college-basketball/seasons/2026/types/0/teams/150/statistics` (Duke): the whole season, 38 games, 1,079 FGM, 612 / 840 FT, gzip |
| `mbb_team_box_2026_duke_games.parquet` | `load_mbb_team_boxscore([2026])` (the `espn_mens_college_basketball_team_boxscores` release) filtered to Duke's 38 `game_id`s: 76 rows, both teams of each game, every column, zstd 19 |
