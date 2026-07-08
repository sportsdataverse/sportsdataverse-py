<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WBB shot-quality fixtures](#wbb-shot-quality-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WBB shot-quality fixtures

Women's mirror of `tests/fixtures/mbb_shot_quality/` (see that README for
conventions). Captured **2026-07-08** by `dev/mbb_shot_quality/
capture_shots.py` with `PRED_LEAGUE=womens`. **Season 2026** — the
`wbb_shots` release floors at 2026 (no 2025), so the women's gates and the
Barttorvik anchors (`BART_NATIONAL_SPLITS["womens"]`, year=2026 JSON
endpoint) are era-matched to 2026.

| File | Rows | Provenance |
|---|---:|---|
| `espn_shots_2026_train.parquet` | 367,591 | `load_wbb_shots([2026])` -> `espn_shots_to_canonical`; temporal split at the median game date (2026-01-10), train = on/before. |
| `espn_shots_2026_holdout.parquet` | 339,825 | Same capture, games after the cut. |

No women's NCAA HTML sample — the T1.1 residential scrape cache holds
men's contests only; the NCAA path is proven by the men's sample +
league-agnostic parser tests.

Re-capture: `PRED_LEAGUE=womens uv run python dev/mbb_shot_quality/capture_shots.py`.
