<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [G League Stats fixtures](#g-league-stats-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# G League Stats fixtures

Real captured response bodies from `stats.nba.com` with `LeagueID=20` (the NBA G League). No
synthetic fixtures.

`endpoints/<slug>.json` holds one capture per `nbagl_stats_<slug>` wrapper: a trimmed copy of the
2026-08 capture sweep sample `sdv-internal-refs/nba/captures/_sample/20/<slug>.json`, written by
`tools/codegen/vendor_captures.py`. Trimming keeps the first two records of every record list plus
each later record that adds a new `(field path, value type)` pair, so the trimmed capture parses to
the same columns and dtypes as the full one (the script asserts it). The returns schemas under
`tools/codegen/schemas/native/nbagl_stats/` are built from what `parse_nba_stats_result_sets` emits
on these files (`tools/codegen/gen_nba_stats.py`).

The sweep took most of them with the G League sample entities in `sdv-internal-refs/nba/tools/sample_params.py`
(`GameID` 2022500137, `PlayerID` 1642443, team 1612709914, season `2023-24` with a newer-season ladder), but
not all of them, and each file's `parameters` block is the record of what it sent:

- `teamdetails.json` was taken with `TeamID` 1610612747 (the Lakers, an NBA team); its columns do not depend
  on the team, but a capture with a G League team (1612709914) should replace it.
- Ten team endpoints were taken with `TeamID` 0 (`cumestatsteam`, `franchiseleaders`, `leagueplayerondetails`,
  `teamdashboardbygeneralsplits`, `teamdashboardbyshootingsplits`, `teamdashlineups`, `teamplayerdashboard`,
  `teamplayeronoffdetails`, `teamplayeronoffsummary`, `teamvsplayer`).
- `VsPlayerID` was sent as 201939; the wrapper default (1631457) is a player from the 2022500137 box score.
- `playbyplayv3.json` is a `{meta, game: {actions}}` body, not a `resultSets` envelope, so the parser emits no
  columns and that schema is `unverified` (the raw payload is there with `return_parsed=False`).

None of the wrapper defaults has been called live: stats.nba.com hangs on datacenter IPs and is only probed
from a residential one with `SDV_PY_NBA_STATS_LIVE=1`.

Re-vendor (needs the private repo checkout):

    SDV_INTERNAL_REFS_REPO=<path> uv run python tools/codegen/vendor_captures.py
