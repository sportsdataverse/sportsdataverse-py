<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [player_core fixtures](#player_core-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# player_core fixtures

Real ESPN core-v2 `/athletes/{id}` captures backing `tests/test_player_core.py`.
Captured 2026-07-16 from the committed `{lg}/player_core/json/` raw trees
(themselves scraped from `sports.core.api.espn.com/v2/sports/basketball/leagues/{lg}/athletes/{id}`).

| file | league | athlete | why this one |
|---|---|---|---|
| `cap_player_core_nba_1966.json` | nba | LeBron James | richest record: draft + headshot + experience; **no `college`** (prep-to-pro) — the legitimately-null case |
| `cap_player_core_mbb_4433176.json` | mbb | RJ Davis | `college_id == current_team_id` (in college ball the team *is* the college) |
| `cap_player_core_wnba_1002.json` | wnba | Jessica Breland | draft + college populated; pro-league shape |
| `cap_player_core_wbb_9617.json` | wbb | id 9617 | sparsest WBB record on disk — pins schema stability when most fields are absent |

Coverage is **era-dependent by nature** (headshots only exist for modern players;
`college`/`dateOfBirth` decline the other way), so these deliberately span the extremes.
