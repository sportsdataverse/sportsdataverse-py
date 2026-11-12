<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [BB-D36 shot-lineup corpus](#bb-d36-shot-lineup-corpus)
  - [Coverage on the full releases (measured 2026-11-12)](#coverage-on-the-full-releases-measured-2026-11-12)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# BB-D36 shot-lineup corpus

Real captures for `tests/basketball/test_shot_lineups.py`, read from the Data API on 2026-11-12. The college
rows are `{mbb,wbb}.ncaa_shots` and `ncaa_pbp`; the pbp is projected to the columns the builder reads. The
pro rows are `{nba,wnba}_stats.shots` and `possessions`. Nothing is synthetic or edited: rows are only
filtered (whole games) and columns only dropped.

| File | Games | Rows |
|---|---|---:|
| `mbb_shots.parquet` / `mbb_pbp.parquet` | stats.ncaa.org contests 6607173 (Lindenwood vs East-West U., a non-D-I side), 6633802, 6644911 (2026-27) | 377 / 1,592 |
| `wbb_shots.parquet` / `wbb_pbp.parquet` | contests 6636813, 6648297 (2026-27, one with overtime) | 273 / 1,171 |
| `nba_stats_shots.parquet` / `nba_stats_poss.parquet` | 0022600023 (2026-27) | 171 / 188 |
| `wnba_stats_shots.parquet` / `wnba_stats_poss.parquet` | 1022600220 (2026) | 136 / 158 |

## Coverage on the full releases (measured 2026-11-12)

`eligible` means both teams carry an id and at least one of the game's shots matched. That match stands in for "the game has lineups". It's the gate's denominator (see `shot_lineup_coverage`).

| Season | Shots | Matched | Eligible full five a side | Shooter in his five |
|---|---:|---:|---:|---:|
| MBB 2026-27 (to Nov 10) | 53,046 | 0.9991 | 0.9839 | 0.9824 |
| WBB 2026-27 (to Nov 10) | 69,680 | 0.9987 | 0.9827 | 0.9852 |
| MBB/WBB 2025-26 and earlier | 740,861 / 708,038 | 0 | none eligible | — |
| NBA 1996-97 .. 2026-27 (eight seasons sampled) | 28k-235k | 0.914-0.9998 | 0.9955-0.9998 | 0.9775-0.9905 |
| WNBA 1997 .. 2026 (six seasons sampled) | 14k-48k | 0.9977-0.9999 | 0.9978-0.9999 | 0.9782-0.9862 |

The college releases before 2026-27 carry no shot-chart clock (`period` and `sec_left` are null), so no shot
can be placed on its event. NBA 2026-27's 0.914 matched is the in-season possessions release lagging the
shots by a few games; those games have no lineups and aren't eligible.
