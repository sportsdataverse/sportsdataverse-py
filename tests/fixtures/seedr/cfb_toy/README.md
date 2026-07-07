<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [seedr cfb_toy fixture](#seedr-cfb_toy-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# seedr cfb_toy fixture

Cross-validation toy fixture from the shared nflseedR-port spec (2026-07-03).
Two conferences (Alpha A1-A4, Beta B1-B4) + one independent (I1), 15 REG games
plus two week-15 `CONF_CHAMP` games. Designed so the Alpha 3-way conference tie
(A1/A2/A3 all 2-1) has circular head-to-head and tied common-opponent records,
resolving deterministically down the cascade (A1 +25 > A2 +10 > A3 +3 on
conference point differential under `tiebreaker_depth="POINTS"`).

The same CSVs are consumed by the R `cfbseedR` port; both engines' sorted
`cfb_standings(..., tiebreaker_depth="POINTS")` outputs are diffed by the
orchestrator and must agree.

- `toy_games.csv` — engine `games` schema (`sim`, `week`, `game_type`,
  `home_team`, `away_team`, `result` = home margin, `neutral`).
- `toy_teams.csv` — `team`, `conference` (`FBS Independents` marks I1).
