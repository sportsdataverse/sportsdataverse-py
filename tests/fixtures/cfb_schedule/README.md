<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [ESPN calendar fixtures](#espn-calendar-fixtures)
  - [Why there is no captured drift span here](#why-there-is-no-captured-drift-span-here)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# ESPN calendar fixtures

Real captured `leagues[0].calendar` payloads backing the
`espn_cfb_calendar` / `espn_nfl_calendar` concat regression test
(`tests/cfb/test_calendar_concat.py`).

| File | Provenance | Captured |
|---|---|---|
| `cfb_calendar_2024.json` | `site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates=2024&groups=80` | 2026-08-11 |
| `nfl_calendar_2024.json` | `site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?dates=2024` | 2026-08-11 |

Both are trimmed to `{"leagues": [{"calendar": [...]}]}` — the only part
either loader reads.

## Why there is no captured drift span here

Unlike the NFL release parquets (see `tests/fixtures/nfl_loaders/README.md`,
where 2016 vs 2023 genuinely differ), the calendar blocks were **probed and
found uniform**: 20 season probes across both leagues (2002-2025) all
produced a single distinct column set and a single distinct dtype signature
per season. There is no real drift span to capture.

The concat is still not identical-schema *by construction*: each season-type
block is flattened by its own
`pandas.json_normalize(..., errors="ignore")` call, so the column set is
decided by the payload. A key absent from every entry in one block silently
drops that column from that block only, and `how="vertical"` then raises.
`test_calendar_concat.py` reproduces exactly that by deleting one optional key
from one block of the captured payload — a labelled mutation of real data, not
a captured drift. The move to `diagonal_relaxed` is therefore **defensive
hardening, not a fix for an observed failure**.
