<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Event-level lineup captures](#event-level-lineup-captures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Event-level lineup captures

Real stats.ncaa.org pages for `tests/mbb/test_mbb_ncaa_event_lineups.py`, copied unedited from the
ncaa-mbb-hoops-raw bundles (`mbb/raw/2026/<id>.json.gz`, `pages.play_by_play` and
`pages.individual_stats`), captured 2026-07-14.

| Contest | Files | What it pins |
|---|---|---|
| `6416178` (MBB 2025-26, Lipscomb vs Jacksonville) | `pbp_6416178.html`, `individual_stats_6416178.html` | At 39:49 the page lists Allen Udemadu's `Leaves Game` before his own two free throws. A free thrower cannot be replaced (Rule 3-6.2.a), so he is on the floor for both, and the sub takes effect after them. |
| `6424024` (MBB 2025-26) | `pbp_6424024.html`, `individual_stats_6424024.html` | One sub row for `UNKNOWN.TEAM`. bigballR's invalid-substitution bailout shipped the whole game with null lineups; the event-level parser skips that row. |

`individual_stats_*.html` carries the box score's minutes played (`MP`), the independent oracle for
the on-floor minutes the lineups imply.
