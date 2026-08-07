<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA Stats fixtures](#nba-stats-fixtures)
  - [Trimming](#trimming)
  - [Re-capturing](#re-capturing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA Stats fixtures

Real captured response bodies from `stats.nba.com`. No synthetic fixtures —
three Statcast parsers previously shipped wrong because their hand-written
fixtures did not match the live payload.

Per-family subdirectories carry their own README (see `tracking/`).

| File | Endpoint | Captured | Notes |
|---|---|---|---|
| `scheduleleaguev2_2025_26.json` | `nba_stats_scheduleleaguev2(season="2025-26", return_parsed=False)` | 2026-08-07 | 2025-26 season |

## Trimming

`scheduleleaguev2_2025_26.json` is the real body with
`leagueSchedule.gameDates` reduced to the **first 4 and last 4 dates**
(14 games) and the unused `leagueSchedule.weeks` block dropped — the full body is
~4.7 MB. Every retained game object is byte-for-byte as served, and the
first/last split keeps more than one `game_id` season-type prefix in the fixture
so `season_type_description` derivation is exercised.

## Re-capturing

From a residential IP (`stats.nba.com` hangs on datacenter IPs):

```sh
SDV_PY_NBA_STATS_LIVE=1 uv run python -c "
import json
from sportsdataverse.nba.nba_stats import nba_stats_scheduleleaguev2
raw = nba_stats_scheduleleaguev2(season='2025-26', return_parsed=False)
gd = raw['leagueSchedule']['gameDates']
raw['leagueSchedule']['gameDates'] = gd[:4] + gd[-4:]
raw['leagueSchedule'].pop('weeks', None)
json.dump(raw, open('tests/fixtures/nba_stats/scheduleleaguev2_2025_26.json', 'w'), indent=1)
"
```

`tests/test_crosswalk_basketball_sources.py` derives its expected row count from
the fixture itself, so a re-capture does not need a matching test edit unless the
column contract moved.
