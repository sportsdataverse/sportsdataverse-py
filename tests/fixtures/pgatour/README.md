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

The leaderboard, scorecard and tee-times captures come from full-selection documents. Their selection is
`{ id payload }`, the same as the shipped document. The 2026 FedExCup (`02671`, `year: 2026`) answered with an empty
player list on the capture date, so the fixture is the 2025 race.
