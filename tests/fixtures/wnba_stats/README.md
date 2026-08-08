<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WNBA Stats fixtures](#wnba-stats-fixtures)
  - [Trimming](#trimming)
  - [Re-capturing](#re-capturing)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WNBA Stats fixtures

Real captured response bodies from `stats.wnba.com`. No synthetic fixtures —
three Statcast parsers previously shipped wrong because their hand-written
fixtures did not match the live payload.

| File | Endpoint | Captured | Notes |
|---|---|---|---|
| `scheduleleaguev2_2026.json` | `wnba_stats_scheduleleaguev2(season="2026", return_parsed=False)` | 2026-08-07 | 2026 season |

## Trimming

`scheduleleaguev2_2026.json` is the real body with `leagueSchedule.gameDates`
reduced to the **first 4 and last 4 dates** (25 games) and the unused
`leagueSchedule.weeks` block dropped — the full body is ~900 KB. Every retained
game object is byte-for-byte as served, and the first/last split keeps more than
one `game_id` season-type prefix in the fixture so
`season_type_description` derivation is exercised.

## Re-capturing

From a residential IP (`stats.wnba.com` hangs on datacenter IPs):

```sh
SDV_PY_NBA_STATS_LIVE=1 uv run python -c "
import json
from sportsdataverse.wnba.wnba_stats import wnba_stats_scheduleleaguev2
raw = wnba_stats_scheduleleaguev2(season='2026', return_parsed=False)
gd = raw['leagueSchedule']['gameDates']
raw['leagueSchedule']['gameDates'] = gd[:4] + gd[-4:]
raw['leagueSchedule'].pop('weeks', None)
json.dump(raw, open('tests/fixtures/wnba_stats/scheduleleaguev2_2026.json', 'w'), indent=1)
"
```

`tests/test_crosswalk_basketball_sources.py` derives its expected row count from
the fixture itself, so a re-capture does not need a matching test edit unless the
column contract moved.
