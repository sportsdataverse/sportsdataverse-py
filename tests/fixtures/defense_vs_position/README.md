<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [defense_vs_position fixtures](#defense_vs_position-fixtures)
  - [`cfb_pbp_2024_psu3_uga2_def.parquet` (398 rows)](#cfb_pbp_2024_psu3_uga2_defparquet-398-rows)
  - [`cfb_rosters_2024_slice.parquet` (51 rows)](#cfb_rosters_2024_sliceparquet-51-rows)
  - [`nfl_model_pbp_2024_phi_def_wk1_3.parquet` (229 rows)](#nfl_model_pbp_2024_phi_def_wk1_3parquet-229-rows)
  - [`nfl_rosters_2024_slice.parquet` (23 rows)](#nfl_rosters_2024_sliceparquet-23-rows)
  - [Hand-computed cells](#hand-computed-cells)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# defense_vs_position fixtures

Real released 2024 rows for `sportsdataverse/defense_vs_position.py`, read from the
`sportsdataverse-data` release assets by URL on 2026-10-03 (no repo checkout).

## `cfb_pbp_2024_psu3_uga2_def.parquet` (398 rows)

Every play Penn State (`def_pos_team_id` 213) defended in its first three 2024
regular-season games (401628457 at West Virginia, 401628470 Bowling Green,
401628493 Kent State) and Georgia (61) defended in its first two (401628323 Clemson,
401628339 Tennessee Tech), special teams and penalties included so the population
filter has real work.

- Source: `espn_cfb_pbp/play_by_play_2024.parquet`
  (sha256 `9b4bfe6a8241cf496f7c43096c305269abc32431fc96f080e4a12302009994e3`).
- Columns: `PBP_COLUMNS["cfb"]` plus `pos_team_id`, `pos_team`, `def_pos_team`,
  `type.text`, `text` and `passer_player_id` for reading the plays by hand.
- Selection (polars): the semi-join of the asset on those `(def_pos_team_id,
  game_id)` pairs, sorted by `game_id, game_play_number`.

## `cfb_rosters_2024_slice.parquet` (51 rows)

The 2024 roster rows (`season`, `team_id`, `athlete_id`, `display_name`,
`position`, `position_abbreviation`) of every rusher, receiver and passer id in the
pbp fixture: 52 ids, 51 rows. The missing id, `-5650`, is ESPN's TEAM placeholder
(Clemson "TEAM run for a loss of 1 yard", 401628323 play 74), so that carry counts
in no group.

- Source: `espn_cfb_rosters/cfb_rosters_2024.parquet`
  (sha256 `5d0f008bc4490ecd4ae0cd9c5224d5da4abc93914bd43321aa9cb854c168ba34`).

## `nfl_model_pbp_2024_phi_def_wk1_3.parquet` (229 rows)

Every play the Eagles defended (`defteam == "PHI"`) in weeks 1-3 of 2024
(`2024_01_GB_PHI`, `2024_02_ATL_PHI`, `2024_03_PHI_NO`).

- Source: `nfl_model_pbp/model_pbp_2024.parquet`
  (sha256 `94a72afbcd45c4ae06e43d10fa4756eed0b0af124ce21e8fd96190650c4b1d6c`).
- Columns: `PBP_COLUMNS["nfl"]` plus `posteam`, `qb_scramble` and `desc`.

## `nfl_rosters_2024_slice.parquet` (23 rows)

`load_nfl_rosters([2024])` rows (`season`, `team`, `gsis_id`, `full_name`,
`position`) for the 23 rusher and receiver gsis ids in the NFL fixture; every one
has a row.

## Hand-computed cells

Counted row by row in plain Python from the fixture (not through the function).

**CFB 2024, Penn State, TE** -- 14 targets to roster TEs, all completions: West
Virginia's Kole Taylor (plays 6, 60) and Treylan Davis (40), Bowling Green's Harold
Fannin Jr. (plays 2, 7, 17, 42, 43, 53, 67, 125, 143, 157, 160); none vs Kent State.

| plays | games | EPA sum | EPA/play | successes | explosive | receiving yards |
|---|---|---|---|---|---|---|
| 14 | 2 | 12.263268947601318 | 0.8759477819715228 | 10 | 1 (play 42, EPA 2.53) | 165 (11.7857 per target) |

`games` = 2, so the cell is not `qualified`; Penn State's QB, RB and WR cells span 3
games and are.

**NFL 2024, PHI, RB** -- 70 RB carries (366 rushing yards) and 18 RB targets over 3
games: 88 plays, EPA sum -3.1620409803072107 (-0.03593228386712739 per play), 37
successes, 2 explosive, 5.228571 yards per carry. QB: 95 dropbacks with 4 sacks plus
one Kirk Cousins aborted-snap carry = 96 plays.
