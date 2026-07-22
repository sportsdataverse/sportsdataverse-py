<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Calibration-at-scale artifacts](#calibration-at-scale-artifacts)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Calibration-at-scale artifacts

Committed reliability curves for the NBA possession-sim WP surface
(`sportsdataverse/nba/nba_possession_sim/wp_surface.py`).

- `nba_wp_calibration.json` — built deterministically from the three
  committed `tests/fixtures/nba_engine/*/playbyplayv3.json` captures: a
  240-path train / 160-path held-out self-calibration walk (the Markov
  self-consistency check) plus the three realized games' score paths
  walked through the same surface. Floats rounded to 10 dp.

Everything is seeded, so `tests/nba/test_nba_wp_calibration.py` enforces
exact equality against a rebuild. Regenerate deliberately after an engine /
shelf / surface change:

```sh
uv run python -m tools.calibration.build
```

The pinned parameters live in `tools/calibration/build.py::PARAMS`.
