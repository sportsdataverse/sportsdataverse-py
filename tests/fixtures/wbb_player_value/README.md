<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WBB player-value oracle fixtures](#wbb-player-value-oracle-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WBB player-value oracle fixtures

Women's mirror of `tests/fixtures/mbb_player_value/` (see that README for
the shared conventions). Captured **2026-07-07** by
`dev/mbb_player_value/capture_oracle.py` with `PRED_LEAGUE=womens`; consumed
by `tests/wbb/test_wbb_player_value_oracle.py`.

**Era note (differs from MBB):** the women's release floors are
`wbb_player_boxscore` 2025+, but `wbb_shots` and `wbb_rosters` **2026+** —
`aggregate_player_seasons` therefore uses the per-season box-derived
shot-split fallback for 2025 (rim share unavailable that season).

| File | Rows | Source / provenance |
|---|---:|---|
| `barttorvik_bpm_2025.parquet` | 4,729 | `barttorvik.com/ncaaw/getadvstats.php?year=2025` — the women's endpoint returns a JSON array-of-arrays **without** `csv=1` (`csv=1` returns an empty body); same auto-layout detection + additive `bpm == obpm + dbpm` identity (verified on all rows). |
| `barttorvik_bpm_2026.parquet` | 4,682 | Same, `year=2026` (capture-only today -- the gate pins 2025; reserved for the next re-fit). |
| `recruits_2025_2026.parquet` | 233 | ESPN Core v2 women's recruiting, full tracked classes 2024+2025 (`limit=1000`); all rows graded. ESPN tracks far fewer women's recruits than men's. |
| `draft_2025_2026.parquet` | 83 | WNBA Core v2 drafts 2025+2026 (3 rounds × 13; Bueckers #1 2025, Fudd #1 2026 verified at capture). |
| `rosters_2025_2026.parquet` | 9,778 | `wbb_rosters` — **2026 only** (release floor; 2025 skipped). |
| `player_seasons_2025.parquet` | 7,841 | `aggregate_player_seasons([2025], league="womens")` frozen for offline gates (box-fallback shot splits — see era note). |
| `player_seasons_2026.parquet` | 8,305 | Same, 2026 (real shots-classified splits). |
| `team_ratings_2025.parquet` | 618 | `mbb_team_ratings([2025], league="womens")`, frozen. |
| `team_ratings_2026.parquet` | 663 | Same, 2026. |
| `archetype_labeled.parquet` | 9 | Hand-labeled role-certain 2025 player-seasons (Watkins/Bueckers/Hidalgo/M. Williams = shot creator; Betts/Iriafen/Kitts = midrange big; Miles = lead guard; Garzon = spot-up shooter). |

**Barttorvik team-name crosswalk:** 4,580/4,729 rows (96.8%) matched to an
ESPN `team_id`; the 11 unmatched schools mirror the men's list (one-off name
irregulars, dropped by the gate's `team_id.is_not_null()` filter).

**WNBA pick-head data floor (documented, gate not lowered):** only 65 of 83
picks match college data (internationals excluded), age/class is
eligibility-constant, and roster height is unavailable for departed players
— the pick head's pooled LOSO Spearman is 0.52 vs the 0.55 gate, asserted
as an `xfail` so a future data expansion XPASSes visibly. The prob head
passes decisively (AUC 0.978/0.984).
