<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [MBB shot-quality fixtures](#mbb-shot-quality-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# MBB shot-quality fixtures

Canonical-shot-frame captures consumed by the shot-quality oracle gates.
Captured **2026-07-08** by `dev/mbb_shot_quality/capture_shots.py`
(gitignored working script). All id columns `Utf8`; schema =
`mbb_shots_adapter.CANONICAL_SHOT_SCHEMA`.

| File | Rows | Provenance |
|---|---:|---|
| `espn_shots_2025_train.parquet` | 98,466 | `load_mbb_shots([2025])` -> `espn_shots_to_canonical` (fitted court scale). **Temporal split at the median game date (2025-02-27, from `load_mbb_schedule`): train = games on/before the cut** -- the Phase-1 calibration gate fits here and scores the holdout, respecting the leakage boundary. |
| `espn_shots_2025_holdout.parquet` | 93,934 | Same capture, games after 2025-02-27. |
| `ncaa_shots_sample.parquet` | 462 | Four real stats.ncaa.org shot-chart pages (contests 1613299, 5722328, 5722337, 5722338; browser transport per the T1.1-approved residential pattern, cache-first) -> `create_shot_event_data` -> `shot_events_to_frame`. ONE perspective per game (the event list covers both teams; two perspectives would double-count). Proves the NCAA path end-to-end on current markup. |

Notes:

- The ESPN release's `coordinate_{x,y}_raw` grid is basket-anchored
  half-court, ~1 unit = 1 ft (fitted, not assumed: origin = median rim-make
  coordinates, scale = arc radius / median made-three distance).
- ~76% of raw ESPN shot rows lack coordinates (or carry int32-sentinel
  garbage) and are dropped at canonicalization; 192,400 of the season's
  shots survive.
- Zone make rates in the train split (rim .569, paint .385, mid .379,
  corner3 .365, abovebreak3 .339) sit near the published Hoop-Math/Torvik
  baselines in `mbb_shot_quality_constants.PUBLISHED_ZONE_BASELINES`.

Re-capture: `uv run python dev/mbb_shot_quality/capture_shots.py`.
