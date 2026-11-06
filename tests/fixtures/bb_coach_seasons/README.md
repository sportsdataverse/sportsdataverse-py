<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D8 coach-seasons corpus](#bb-d8-coach-seasons-corpus)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D8 coach-seasons corpus

Real captures for `tests/nba/test_nba_coach_seasons.py` and `tests/mbb/test_mbb_coach_seasons.py`,
built on 2026-11-04 (zstd level 19). Nothing is synthetic: every row is a release row, an API
answer or a parsed page, with columns dropped.

| File | Rows | Source | Kept |
|---|---:|---|---|
| `{nba,wnba}_schedule.parquet` | 14,128 / 2,307 | `nba_stats_schedules` 2016-2026, `wnba_stats_schedules` 2016-2025 (`load_nba_stats_schedules`, `load_wnba_stats_schedules`) | regular season and playoffs; `game_id, season, season_type, game_date, {home,away}_team_id, {home,away}_team_abbreviation, {home,away}_pts` |
| `{nba,wnba}_stats_coaches.parquet` | 2,587 / 166 | `nba_stats_coaches` files 2015-2026, `wnba_stats_coaches` 2015-2025 (one file before each season: the opener) | `team_id, season, coach_id, coach_name, coach_type, sort_sequence` (assistants kept: `head_coaches` filters them) |
| `{nba,wnba}_espn_coach_records.parquet` | 256 / 26 | ESPN core API `seasons/{season}/coaches` -> each coach -> its `types/2` records, via `nba_coach_seasons.espn_coach_records` | `season, espn_team_id, coach, wins, losses` |
| `{nba,wnba}_team_crosswalk.parquet` | 330 / 121 | `nba_crosswalk/nba_team_crosswalk_{season}`, `wnba_crosswalk/...` releases | `season, espn_team_id, team_id` (stats.nba.com id) |
| `bbref_coach_records.parquet` | 360 NBA + 129 WNBA | Basketball-Reference season coach pages (`leagues/NBA_{season}_coaches.html`, `wnba/years/{season}_coaches.html`), the public oracle | `league, season, coach, team` (BBRef abbreviation: BRK, CHO, PHO), `g, w, l`: the coach's regular-season games and W-L that season |
| `ncaa_{mbb,wbb}_coaches.parquet` | 6,006 / 5,627 | every committed stats.ncaa.org roster page of `ncaa-mbb-hoops-raw` (2010-2026) / `ncaa-wbb-hoops-raw` (2011-2026), via `ncaa_team_coaches` | `NCAA_COACHES_SCHEMA`: the coach card, the NCAA team-season id and the ESPN team id from `{league}/teams/json/{season}.json` |
| `ncaa_tree/mbb/...` | 2 pages | `ncaa-mbb-hoops-raw/mbb/rosters/html/2018/{110938,110939}.html` and their two rows of `mbb/teams/json/2018.json` | the pages as captured (git normalizes their line endings) |
| `mbb_schedule_2025.parquet` | 419 | `espn_mens_college_basketball_schedules/mbb_schedule_2025` (`load_mbb_schedule(2025)`) | the games of BB-D3's Big Ten possessions fixture, with `game_date, status_type_completed, neutral_site, {home,away}_{id,location,score}` |
| `sports_reference_college_changes.parquet` | 8 | the same pages for four seasons that changed coach (Ole Miss 2017-18, Syracuse 2015-16, Saint Mary's 2013-14, Hawaii 2019-20): both coaches' W-L | `league, season, team_id` (ESPN), `coach, w, l` |
| `sports_reference_college_sample.parquet` | 16 | Sports-Reference school-season pages (`cbb/schools/{school}/{men,women}/{season}.html`): the "Coach:" line and the "Record:" | `league, season, team` (NCAA name), `coach, w, l` |

Also read: BB-D3's `../bb_team_season_rates/nba_possessions_{2024,2026}.parquet` and the college
2025 possessions / team box, and BB-D22's `../bb_team_luck/{mbb,wbb}_season_records.parquet`
(ESPN final scores per team-season, the check on the card records).

Id dtypes: `team_id` Int64 (stats.nba.com franchise id in the pros, ESPN team id in college);
`coach_id` Int64 (stats.nba.com coach id; NCAA person id in college); `game_id` String
(stats.nba.com ids keep their leading zeros, `0022300001`).

The oracle is scored by placing BBRef's own counts with `coach_of_record`, so a game is compared
to the coach BBRef credits; 492 of 26,418 NBA games (136 of 4,244 WNBA) are in team-seasons whose
BBRef counts do not fit the win sequence (an acting coach for a game or two) and are not scored.
BBRef's abbreviations differ from stats.nba.com's for BRK, CHO, PHO (NBA) and SAS, PHO (WNBA);
the tests map them and assert every BBRef team-season maps. A second, count-level gate joins our
per-coach games and wins to BBRef's directly, with nothing on the BBRef side placed.
