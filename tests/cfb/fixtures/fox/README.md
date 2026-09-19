# Fox (Bifrost) college-football fixtures

Real captures from `api.foxsports.com/bifrost/v1/cfb/event/{id}/data`, trimmed by
`background-research/2026-09-17-football-sources-program/s2-fox/make_fixtures.py`. Nothing here
is synthetic.

| file | provenance |
|---|---|
| `fox_cfb_401856679.json` | Fox event **43065**, Oklahoma @ Michigan, 2026 week 2 (ESPN 401856679). Captured 2026-09-16. Carries a made field goal and the drive-regroup case: the receiving team's first row (a pre-snap penalty) filed under the punting team's drive group. |
| `fox_cfb_no_coverage.json` | an **FCS-hosted** game (Fox lists it, Fox serves HTTP 200 with a real header and **no `pbp` key**) — the shape Fox, CBS and Yahoo all answer with for FCS-hosted games in every era. |
