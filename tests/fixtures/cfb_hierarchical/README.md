<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_hierarchical fixtures](#cfb_hierarchical-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_hierarchical fixtures

- `schedule_2023_2024.parquet`: every 2023-2024 game with an FBS team from the `cfb_schedules` release, read
  2026-11-27 with `load_cfb_schedule([2023, 2024])`. Only the columns `cfb_hierarchical_ratings` reads are kept.
- `expected_ratings.parquet`: the reference model's as-of strengths on that schedule, for (2023, t=6), (2024, t=10)
  and (2024, t=101, the bowls). The reference is cfbfastR-cfb-data `cfb_higher_models.hierarchical`:
  `build_corpus` and `fit_asof(method="eb")`.
  - The corpus is the completed games before the slot, with the carryover chain started at 2023.
  - It was produced on sportsdataverse-py `bb1f71d3f9` (the schedule loader only).

`tests/cfb/test_cfb_hierarchical.py` asserts that the port reproduces it to 1e-6.
