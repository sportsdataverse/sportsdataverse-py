<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D37 NCAA <-> ESPN player crosswalk fixtures](#bb-d37-ncaa---espn-player-crosswalk-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D37 NCAA <-> ESPN player crosswalk fixtures

Real captures for `tests/mbb/test_ncaa_espn_player_crosswalk.py`, made on 2026-11-09 from the
published sportsdataverse-data releases. Season 2023-24 (`2024`), every Big Ten and SWAC team in
both leagues: a high-major and a low-major conference, whose rosters differ most in jersey and
name hygiene. Rows are only filtered and columns only projected; nothing is synthetic or edited.

| File (`{mbb,wbb}_2024_*.parquet`) | Rows (mbb / wbb) | Source | Notes |
|---|---:|---|---|
| `team_crosswalk` | 26 / 26 | `ncaa_espn_team_crosswalk(league)` | `season, ncaa_team_id, espn_team_id, conference_id`, season `"2023-24"`, conference `{lg}:big-ten` / `{lg}:swac`. |
| `ncaa_team_rosters` | 423 / 371 | `load_ncaa_{lg}_team_rosters(seasons=2024)` | `season, team_id, player_id, clean_name, jersey` for those teams (ids as published, String). |
| `espn_athletes` | 447 / 384 | `espn_player_xwalk.espn_athlete_side(lg, 2024)` (`load_{lg}_player_boxscore`, one row per team + athlete, modal jersey) | `espn_team_id, id, name, jersey`, all String. |
| `player_aliases` | 422 / 371 | `load_{lg}_player_aliases()`, `source == "ncaa"`, window covering 2024, `source_player_id` in the roster | `source_player_id, athlete_id, match_method, match_confidence`: the published oracle the test checks against. |

To recapture, run the capture script from the BB-D37 PR description with
`uv run python <capture.py> tests/fixtures/ncaa_espn_player_crosswalk`.
