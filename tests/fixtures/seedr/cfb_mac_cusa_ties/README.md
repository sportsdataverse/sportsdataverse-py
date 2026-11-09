<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [`cfb_mac_cusa_ties` — real MAC and Conference USA conference games](#cfb_mac_cusa_ties--real-mac-and-conference-usa-conference-games)
  - [Provenance](#provenance)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# `cfb_mac_cusa_ties` — real MAC and Conference USA conference games

Real regular-season conference games with final scores: the MAC in 2023, 2025
and 2026 (through week 10), and Conference USA in 2025 and 2026 (through week
10). They pin the dated tiebreaker epochs of
sportsdataverse/sportsdataverse-py#1145 (MAC) and #1146 (Conference USA). No
ratings, CFP rankings or APR are supplied, so every tie the tests check is
settled by a games-based rung:

- **MAC 2023:** Northern Illinois over Bowling Green on the order-of-finish
  step (opponents' win % would pick Bowling Green), under the 2024 cascade and
  under the 2025 order alike.
- **MAC 2025:** Toledo over Ohio on the order-of-finish step.
- **MAC 2026:** opponents' conference win % seeds Buffalo first of Buffalo,
  Toledo and Western Michigan.
- **CUSA 2025:** record vs common opponents orders FIU, Louisiana Tech and
  Missouri State. Keyed as 2024, the same games take the 2024 cascade (CFP
  clause, composite, APR), which has no games-based rung after head-to-head.
- **CUSA 2026:** opponents' conference win % cuts a six-team tie to FIU and
  Kennesaw State, and the common-opponent descent puts Kennesaw State first.

Both engines read these files: sdv-py's
`tests/cfb/test_cfb_standings.py::TestMacCusaEpochs` and cfbseedR's
`tests/testthat/test-tiebreakers.R`. The CSVs are byte-identical to
cfbseedR's `tests/testthat/fixtures/cfb_mac_cusa_ties/`.

## Provenance

- **Source:** the cfbfastR / sportsdataverse-data `cfb_schedules` release
  (pulled 2026-11-09), run through cfbseedR's `cfb_games_from_schedule()`.
- **Filter:** completed `REG` games with both teams in the same conference,
  that conference being the MAC (`Mid-American`) or Conference USA.
- **Files:** `games.csv` holds the game rows (with `season`, so each season
  takes its own epoch). `teams.csv` holds each season's members (`season`,
  `team`, `conference`), because membership changes between seasons (UMass
  joins the MAC in 2025, Sacramento State in 2026; Northern Illinois, Louisiana
  Tech and UTEP leave after 2025). Filter both files to one season before
  calling `cfb_standings()`.
