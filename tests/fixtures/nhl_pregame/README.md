<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NHL pregame snapshot fixture](#nhl-pregame-snapshot-fixture)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NHL pregame snapshot fixture

`nhl_pregame_snapshots_2027.parquet` is fastRhockey-nhl-data's `nhl_data_build.pregame` output for
2026-11-25 (14 games, 128 state intervals), compiled from nine real ESPN Site v2 probes of the NHL
scoreboard and injury list taken every 15 minutes from 2026-11-26T00:10:47Z to 02:11:06Z, bridged
against that repo's 2026-27 schedule and rosters. It stands in for the `nhl_pregame_snapshots`
release asset in `tests/nhl/test_nhl_pregame_snapshots.py`. Ids are Int64; `valid_from`, `valid_to`
and `start_time_utc` are `Datetime("us", "UTC")`.
