<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [bb_game_lines fixtures (BB-D33)](#bb_game_lines-fixtures-bb-d33)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# bb_game_lines fixtures (BB-D33)

Real slices of the SportsDataverse odds store (the odds-data checkout's `odds/` tree, the same files
the sdv-db `odds` schema ingests), captured 2026-11-10, with the ESPN schedules of the same games from
the sportsdataverse-data `load_<league>_schedule` releases.

| file | source | slice |
|---|---|---|
| `<lg>_lines_checkpoints.parquet` | `odds/lines_checkpoints_<year>.parquet` | the events below; columns trimmed to what `game_lines` reads |
| `<lg>_espn_crosswalk.parquet` | `odds/espn_crosswalk.parquet` | those events |
| `<lg>_schedule.parquet` | `load_<lg>_schedule` | those games: `game_id`, `season`, `home_id`, `away_id`, `neutral_site`, scores, `status_type_completed` |
| `<lg>_props_checkpoints.parquet` | `odds/checkpoints/props_<yyyy>_<mm>_<lg>.parquet` | the five main markets of the props window |
| `<lg>_props_player_xwalk.parquet` | `odds/<lg>/props_player_xwalk_<season>.parquet` | the names in the props slice |

Windows (commence time, UTC): NBA 2024-10-22..2024-11-11 plus 401766458 (props 2024-10-22..25);
WNBA 2025-07-01..07-19 (props 07-08..10); MBB 2025-11-21..11-23 plus 401856600 and 401812161
(401829457 is a `flipped` crosswalk match); WBB (`basketball_wncaab`) 2026-02-12..02-15.
