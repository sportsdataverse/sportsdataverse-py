<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA Stats fixtures](#nba-stats-fixtures)
  - [Trimming](#trimming)
  - [Re-capturing](#re-capturing)
  - [Per-endpoint captures](#per-endpoint-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA Stats fixtures

Real captured response bodies from `stats.nba.com`. No synthetic fixtures —
three Statcast parsers previously shipped wrong because their hand-written
fixtures did not match the live payload.

Per-family subdirectories carry their own README (see `tracking/`).

| File | Endpoint | Captured | Notes |
|---|---|---|---|
| `scheduleleaguev2_2025_26.json` | `nba_stats_scheduleleaguev2(season="2025-26", return_parsed=False)` | 2026-08-07 | 2025-26 season |
| `commonteamroster_1610612747_2023_24.json` | `nba_stats_commonteamroster(team_id="1610612747", season="2023-24", return_parsed=False)` | 2026-07-08 | Lakers 2023-24, untrimmed; copied from `sdv-internal-refs/nba/captures/_sample/00/commonteamroster.json` |
| `leaguestandingsv3_2023_24.json` | `nba_stats_leaguestandingsv3(season="2023-24", return_parsed=False)` | 2026-07-08 | 30 teams, untrimmed; copied from `sdv-internal-refs/nba/captures/_sample/00/leaguestandingsv3.json` |
| `leaguegamelog_team_2023_24.json` | `nba_stats_leaguegamelog(league_id="00", season="2023-24", return_parsed=False)` | 2026-07-08 | Team game log, trimmed (see below); from `sdv-internal-refs/nba/captures/_sample/00/leaguegamelog.json` |

## Trimming

`scheduleleaguev2_2025_26.json` is the real body with
`leagueSchedule.gameDates` reduced to the **first 4 and last 4 dates**
(14 games) and the unused `leagueSchedule.weeks` block dropped — the full body is
~4.7 MB. Every retained game object is byte-for-byte as served, and the
first/last split keeps more than one `game_id` season-type prefix in the fixture
so `season_type_description` derivation is exercised.

`leaguegamelog_team_2023_24.json` keeps only the **first row per `TEAM_ID`** of
the real 2,460-row body (30 rows, one per team, ~390 KB -> ~5 KB). The
crosswalk reads it only for each team's tricode, so one row per team exercises
the whole mapping; retained rows are unchanged, the envelope is as served.

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

## Per-endpoint captures

`endpoints/<slug>.json` holds one real body per `nba_stats_*` wrapper (125 of
128). They are the 2026-08 capture sweep's samples,
`sdv-internal-refs/nba/captures/_sample/00/<slug>.json` (commit `3d754b8`), copied
by `tools/codegen/vendor_captures.py`. The returns-table schemas under
`tools/codegen/schemas/native/nba_stats/` are generated from what
`parse_nba_stats_result_sets` emits on them (`tools/codegen/gen_nba_stats.py`).
`tests/codegen/test_stats_on3_schemas_match_parser.py` checks that the schemas
still match.

The three video endpoints are not in the directory, because their sweep samples
are empty. Their schema comes from the pilot captures in `tests/nba/fixtures/`
instead (`gen_nba_stats.CAPTURE_OVERRIDES`).

Each body is trimmed in the same way: every record list keeps its first 2
records, plus every later record that adds a `(field path, value type)` pair the
kept records lack. Parser dtypes depend only on that set of pairs, so the vendor
script asserts that the trimmed body parses to the same columns, in the same
order and with the same dtypes, as the full one. Headers, result-set lists and
scalar rows are never cut. Re-vendor (this needs the private repo):

```sh
SDV_INTERNAL_REFS_REPO=<path> uv run python tools/codegen/vendor_captures.py
uv run python tools/codegen/gen_nba_stats.py
```
