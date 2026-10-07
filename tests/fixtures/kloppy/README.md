<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [kloppy fixtures](#kloppy-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# kloppy fixtures

A real, trimmed StatsBomb open-data match for `tests/soccer/test_soccer_events.py`, loaded
offline through `kloppy.statsbomb.load(event_data=..., lineup_data=...)`.

| File | Source | Trim |
|---|---|---|
| `statsbomb_8658_events.json` | `https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/8658.json` | first 50 events + every `Half Start` / `Half End` + the first `Shot` (57 of 2,978) |
| `statsbomb_8658_lineups.json` | `https://raw.githubusercontent.com/statsbomb/open-data/master/data/lineups/8658.json` | whole file |

Match 8658 is France v Croatia, the 2018 FIFA World Cup final. Captured 2026-10-06 by
`trim_statsbomb_8658.py` (re-run it to re-capture; do not hand-edit). kloppy refuses a plain
head-trim ("Failed to determine start and end time of periods"), which is why both boundary
events of each half are kept.

**License.** StatsBomb open data is published at <https://github.com/statsbomb/open-data> under
StatsBomb's non-commercial public data license (`LICENSE.pdf` in that repo): free for research
and non-commercial use with attribution to StatsBomb; no commercial use. These files are a
test fixture only.
