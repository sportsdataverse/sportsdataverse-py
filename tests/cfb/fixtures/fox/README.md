<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Fox (Bifrost) college-football fixtures](#fox-bifrost-college-football-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Fox (Bifrost) college-football fixtures

Real captures from `api.foxsports.com/bifrost/v1/cfb/event/{id}/data`, trimmed by
`background-research/2026-09-17-football-sources-program/s2-fox/make_fixtures.py`. Nothing here
is synthetic.

| file | provenance |
|---|---|
| `fox_cfb_401856679.json` | Fox event **43065**, Oklahoma @ Michigan, 2026 week 2 (ESPN 401856679). Captured 2026-09-16. Carries a made field goal and the drive-regroup case: the receiving team's first row (a pre-snap penalty) filed under the punting team's drive group. |
| `fox_cfb_no_coverage.json` | an **FCS-hosted** game (Fox lists it, Fox serves HTTP 200 with a real header and **no plays**, `"pbp": {"sections": []}`) — the shape Fox, CBS and Yahoo all answer with for FCS-hosted games in every era. |
| `fox_cfb_segment_2025_bowls.json` | `cfb/league/scores-segment/2025-bowls-2?groupId=2`, the 2025 postseason as `fox_cfb_schedule` fetches it (47 games). Captured 2026-10-19, trimmed to the fields `_parse_fox_segment` reads. |
| `fox_cfb_segment_2025_cfp.json` | `cfb/league/scores-segment/2025-cfp-2?groupId=cfp`, the 2025 College Football Playoff (11 games). Captured 2026-10-19, same trim. Every game in it is also in the bowls segment. |
