<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D17 college season corpus](#bb-d17-college-season-corpus)
  - [Matching WarrenNolan to ESPN](#matching-warrennolan-to-espn)
  - [Division I membership](#division-i-membership)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D17 college season corpus

Real captures for `tests/mbb/test_mbb_college_season.py`, made on 2026-11-03 with
`uv run python dev/bb_college_season/capture.py` (gitignored, like `dev/mbb_prediction/`).
Rows are only filtered and columns only projected; nothing is synthetic or edited. Team and game
ids are Utf8 (`Int -> Int64 -> Utf8`). The 2024 team box comes from `../mbb_prediction/` and
`../wbb_prediction/`, and so do the actual 2024 fields and seeds (`ncaa_tourney_2024`).

| File | Rows | Source | Notes |
|---|---:|---|---|
| `mbb_schedule_2024.parquet` / `wbb_schedule_2024.parquet` | 6,249 / 5,923 | `load_{mbb,wbb}_schedule([2024])` (ESPN, sportsdataverse-data) through `normalize_college_schedule` | Every game, completed or not, with the conference, tournament-type and note columns the season products read. |
| `mbb_schedule_2025.parquet` / `wbb_schedule_2025.parquet` | 6,299 / 5,648 | the same, 2025 | Out-of-sample season. The women's release stops carrying conference-tournament games after 2025-03-10 (184 of about 300). |
| `mbb_team_box_2025.parquet` / `wbb_team_box_2025.parquet` | 12,572 / 11,252 | `load_{mbb,wbb}_team_boxscore([2025])` | Cols: `game_id, season, team_id, team_score, field_goals_attempted, offensive_rebounds, turnovers, total_turnovers, team_turnovers, free_throws_attempted`. |
| `mbb_ncaa_seeds_2025.parquet` / `wbb_ncaa_seeds_2025.parquet` | 68 / 68 | `espn_{mbb,wbb}_scoreboard` on the first four tournament dates (8 calls, 2 s apart) | `team_id, seed`: ESPN's `curatedRank.current`, which carries the committee seed during the tournament. Four per line plus two extra at 11 and 16. |
| `mbb_warrennolan_2024.parquet` / `wbb_warrennolan_2024.parquet` | 362 / 360 | WarrenNolan, `https://www.warrennolan.com/basketball{,w}/2024/rpi-live` and `.../net` (4 pages, downloaded once) | End of season (postseason included). `rpi_rank`, `net_rank`, the Division I `record`, home / road / neutral records and the RPI-quadrant records `q1`..`q4`. WarrenNolan computes the RPI with the NCAA formula; its quadrants use the men's NET ranges for both leagues. |
| `mbb_warrennolan_2025.parquet` / `wbb_warrennolan_2025.parquet` | 364 / 362 | the same pages, `.../2025/...` | Out of sample. Records agree for every men's team but only 122 of 362 women's: the 2025 women's ESPN release is missing games, so the women's 2025 RPI is not gated. |
| `mbb_schedule_2023_nec.parquet` | 79 | `load_mbb_schedule([2023])` through `normalize_college_schedule`, captured 2026-11-06 by `dev/bb_ineligible/capture_nec_2023.py` | The 2023 men's NEC games between members (nine teams). Merrimack, reclassifying, won the final over Fairleigh Dickinson 67-66; FDU took the auto-bid. The 2023 release has no `TRNMNT` type, so the final is found by its note. |

## Matching WarrenNolan to ESPN

WarrenNolan names teams, so the capture assigns each name to an ESPN Division I team in one
linear assignment over name similarity plus a bonus for an identical Division I record, with
`FAU` / `FIU` aliased. Every team is matched (362 men's, 360 women's). The records agree for all
men's teams and 342 of 360 women's: ESPN's women's schedule carries one or two extra or missing
games for 18 teams. Those games are why the women's RPI gates are looser.

## Division I membership

`college_teams` reads it from the schedule (a team ESPN gives a conference id). ESPN strips every
conference field from Dayton's 2024 women's games, so the last fallback is the conference a team
played ten or more games against. The women's standings release has no 2024 season, so it is
not used.
