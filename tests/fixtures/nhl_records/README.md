# NHL Records fixture payloads

Captured 2026-05-24 from `https://records.nhl.com/site/api/`. Used by
`tests/test_nhl_aux_parsers.py` to exercise the `parse_nhl_records`
parser offline.

| File | Endpoint | Notes |
|---|---|---|
| `records_franchise.json`              | `/franchise`                          | 40 franchises |
| `records_franchise_team_totals.json`  | `/franchise-team-totals?limit=10`     | Franchise career-team totals |
| `records_coach.json`                  | `/coach?limit=10`                     | Coach bios (574 total) |
| `records_draft.json`                  | `/draft?limit=10`                     | Draft picks (13,152 total) |
| `records_player_records.json`         | `/player?limit=10`                    | Player records (23,313 total) |
| `records_attendance.json`             | `/attendance`                         | 80 years of attendance data |

Every endpoint ships the same `{data: [...], total: N}` shape so a
single parser handles all of them.
