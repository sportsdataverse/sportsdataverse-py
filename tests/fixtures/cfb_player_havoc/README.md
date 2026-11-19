<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB player havoc fixture](#cfb-player-havoc-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB player havoc fixture

These are real rows from two `sportsdataverse-data` releases, read on **2026-11-18**. They back
`tests/cfb/test_cfb_player_havoc.py`. The fixture covers every 2025 game Pittsburgh (ESPN team
221) played: 13 games, with both teams' players in each.

| File | Rows | Source |
|---|---:|---|
| `player_box_2025_pitt.parquet` | 649 | `espn_cfb_player_box` `player_box_2025.parquet` (`load_cfb_player_box`). Only the `defensive`, `interceptions` and `passing` category rows of those games are kept. `passing` is there so the test can check that a quarterback's thrown interceptions are never counted. Columns that are null in every kept row are dropped. |
| `adv_defensive_players_2025_pitt.parquet` | 136 | `espn_cfb_adv_defensive_players` `adv_defensive_players_2025.parquet` (`load_cfb_adv_defensive_players`): the same games, untrimmed. |

The oracle is ESPN's season line for Rasheem Biles (athlete 4922981), from
`espn_cfb_player_stats(4922981, 2025)` on 2026-11-18. It reads 9 games, 2.5 sacks, 3 passes
defended, 2 interceptions and 1 forced fumble. That line leaves out his 10th box game, the bowl
401778316, where he had 2 sacks and 1 pass defended. The forced fumble is not named in ESPN's
play text, so the play-by-play count is 0 (the documented undercount).
