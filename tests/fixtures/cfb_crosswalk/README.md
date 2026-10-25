<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [cfb_crosswalk fixtures](#cfb_crosswalk-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# cfb_crosswalk fixtures

- `cfb_schedule_crosswalk_2025_head25.parquet`: the first 25 rows of the published
  `cfb_schedule_crosswalk` release asset for season 2025, read with
  `sportsdataverse.cfb.load_cfb_schedule_crosswalk([2025])` on 2026-10-25 and written
  unchanged (`.head(25)`). Every row carries both a `fox_game_id` and a `yahoo_game_id`.

Used by `tests/test_shared_private_bodies.py` to pin the one CFB crosswalk cache the Fox and
Yahoo adapters share (B-GB-11).
