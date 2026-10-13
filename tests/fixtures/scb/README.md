# SCB fixtures (BBL `api.basketball-bundesliga.de`, DBBL `api-1.dbbl.scb.world`)

**Per-route captures** (`bbl/*.json`, `dbbl/*.json`): copied verbatim from `sdv-internal-refs/scb/captures/`
(refs commit `cb7a95a`, captured 2026-10-12). They are truncated (every array cut to 3 elements; a box score
keeps three representative actions). Seasons are START years: `seasons__seasonId__2026.json` is 2026-27, which
the wrappers call `season=2027`. File names are the route with `/` -> `__` and braces dropped, plus the season or
game variant; see `sdv-internal-refs/scb/captures/README.md` for the example ids. No auth value is stored here.

**Full bodies** (`full/`): fetched untruncated on 2026-10-12 through `scb_runtime._get` itself (key derived at run
time, 1 s apart, 6 requests in all), then re-serialised compact. Dtypes and the returns-schemas of the box score,
the games page and the season stat tables are derived from these, not from the 3-row captures, because the API
sends a rate as an integer when it is whole (`threePointShotSuccessPercent: 50`, `fieldGoalsMadePerGame: 6`) and a
count as a string in the team line (`"12000"`).

| File | Request | Notes |
|---|---|---|
| `full/bbl_game_stats_2006920.json` | BBL `/games/2006920/stats` | complete box score and 516 actions of ALBA BERLIN - NINERS Chemnitz, 2026-09-18 (179 KB) |
| `full/dbbl_player_statistics_2025.json` | DBBL `/seasons/2025/player-statistics?displayType=MAIN_ROUND` | 138 player rows, 2025-26 (245 KB) |
| `full/dbbl_team_statistics_2025.json` | DBBL `/seasons/2025/team-statistics?displayType=MAIN_ROUND` | 10 team rows, 2025-26 |
| `full/dbbl_games_2024_first40.json` | DBBL `/games?seasonId=2024&currentPage=1&pageSize=500` | first 40 of 189 games of 2024-25 (`totalPages` 1); 4 of them not yet played (`PRE`) |

`spec_paths.json` is the spec's route list, written by `tools/codegen/gen_scb.py`, so the tests need no refs checkout.
