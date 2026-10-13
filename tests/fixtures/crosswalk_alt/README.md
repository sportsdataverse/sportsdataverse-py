<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [crosswalk_alt fixtures](#crosswalk_alt-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# crosswalk_alt fixtures

Real payloads and real ESPN rows for `tests/test_crosswalk_alt.py`, copied 2026-10-12.

- `raw/` mirrors the `*-alt-raw` layout written by `sportsdataverse._crosswalk_history`:
  - `yahoo/nba/playbook_boxscore/2024/nba.g.2023110619.json.gz`: Yahoo `playbookBoxscore`,
    Dallas @ Orlando 2023-11-06 (hoopR-nba-alt-raw commit "NBA Yahoo Raw Update (Start: 2024 End: 2024)").
  - `fox/nba/event_data/2002/85726.json.gz`: Fox Bifrost `event/85726/data`, Orlando @ San Antonio
    2001-11-06 (hoopR-nba-alt-raw "NBA Fox Raw Update (Start: 2002 End: 2002)").
  - `cbs/nfl/game_boxscore/2024/29376790.json.gz` + `cbs/nfl/scoreboard/2024/regular_17.json`: CBS NAPI
    `game/boxscore` and the week-17 scoreboard page, Kansas City @ Pittsburgh 2024-12-25 (nfl-alt-raw
    "NFL Cbs Raw Update (Start: 2024 End: 2024)").
  - `cbs/cfb/game_boxscore/2017/2843743.json.gz`: the `boxScores` of a CBS NAPI `game/boxscore` answer
    for a CFB game id that came back as a basketball box (`playerStats` is a list), each list cut to
    two players (cfbfastR-cfb-alt-raw "CFB Cbs Raw Update (Start: 2017 End: 2017)").
- `espn/`: the same games on the ESPN side.
  - `nba_schedule_{season}_{game_id}.parquet` / `nba_game_rosters_{season}_{game_id}.parquet`: rows
    filtered from hoopR-nba-data `nba/schedules/parquet/nba_schedule_{season}.parquet` and
    `nba/game_rosters/parquet/game_rosters_{season}.parquet`.
  - `nfl_2024_401671799.json`: the nfl-raw `nfl/espn/crosswalk/games.json` row and the box-score
    athletes (`parse_summary_boxscore_player`) of `nfl/espn/raw/2024/401671799.json.gz`.
