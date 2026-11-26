<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [PGA TOUR GraphQL fixtures](#pga-tour-graphql-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# PGA TOUR GraphQL fixtures

Captured 2026-11-25 from `POST https://orchestrator.pgatour.com/graphql`, sending the query document in
`sportsdataverse/golf/pgatour_queries/` with the variables below. Each file is that operation's `data` value as the
runtime getter returns it. For a `*Compressed*` operation, that means the base64 + gzip `payload` is already decoded.

| File | Operation | Variables |
|---|---|---|
| `schedule_R_2026.json` | `schedule` | `{"tourCode": "R", "year": "2026"}` |
| `leaderboard_R2026014.json` | `leaderboardCompressedV3` | `{"id": "R2026014"}` (2026 Masters) |
| `scorecard_R2026014_28237.json` | `scorecardCompressedV3` | `{"tournamentId": "R2026014", "playerId": "28237"}` |
| `tee_times_R2026014.json` | `teeTimesCompressedV2` | `{"id": "R2026014"}` |
| `player_28237.json` | `player` | `{"id": "28237"}` |
| `player_directory_R.json` | `playerDirectory` | `{"tourCode": "R", "active": true}` |
| `stat_catalog_R_2026.json` | `statOverview` | `{"tourCode": "R", "year": 2026}` |
| `stat_details_120_R_2026.json` | `statDetails` | `{"tourCode": "R", "statId": "120", "year": 2026}` (scoring average) |
| `tour_cup_02671_R_2025.json` | `tourCupCombined` | `{"tourCode": "R", "id": "02671", "year": 2025}` (FedExCup) |
| `shot_details_R2026006_59095_r4.json` | `shotDetailsV4Compressed` | `{"tournamentId": "R2026006", "playerId": "59095", "round": 4, "includeRadar": false}` |
| `schedule_years_R.json` | `scheduleYears` | `{"tourCode": "R"}` |
| `tournaments_R2026014.json` | `tournaments` | `{"ids": ["R2026014"]}` |
| `past_champions_R2026014.json` | `tournamentOverview` | `{"tournamentId": "R2026014"}` |
| `hole_by_hole_R2026014_r4.json` | `leaderboardHoleByHole` | `{"tournamentId": "R2026014", "round": 4}` |
| `leaderboard_stats_R2026014_PROBABILITY.json` | `leaderboardStats` | `{"id": "R2026014", "statsType": "PROBABILITY"}` |
| `field_R2026014.json` | `field` | `{"id": "R2026014", "includeWithdrawn": true}` |
| `past_results_R2026014_20250.json` | `tournamentPastResults` | `{"id": "R2026014", "year": 20250}` (the 2025 edition, `R2025014`) |
| `course_stats_R2026014.json` | `courseStats` | `{"tournamentId": "R2026014"}` |
| `hole_details_R2026014_014_12.json` | `holeDetails` | `{"tournamentId": "R2026014", "courseId": "014", "hole": 12}` |
| `scorecard_stats_R2026014_28237.json` | `scorecardStatsV3Compressed` | `{"id": "R2026014", "playerId": "28237"}` |
| `player_results_28237_R.json` | `playerProfileTournamentResults` | `{"playerId": "28237", "tourCode": "R"}` |

The leaderboard, scorecard and tee-times captures come from full-selection documents. Their selection is
`{ id payload }`, the same as the shipped document. The 2026 FedExCup (`02671`, `year: 2026`) answered with an empty
player list on the capture date, so the fixture is the 2025 race.

The eleven captures from `schedule_years` down were sent with the shipped documents. `tournamentPastResults` takes
an edition as its five-digit `availableSeasons[].year` (`20250`): `year: 2025` answered the current (2026) edition, not
an error, which is why the runtime sends a four-digit year as `year * 10`. The Masters has no ShotLink data, so its
`leaderboardStats` `STROKES_GAINED` table is empty and `scorecardStatsV3Compressed` carries the scoring group only.
