<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB PFF-grade oracle fixtures](#cfb-pff-grade-oracle-fixtures)
  - [Provenance](#provenance)
  - [Auth (how this was captured)](#auth-how-this-was-captured)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB PFF-grade oracle fixtures

`pff_team_grades_2023.parquet` — PFF team grades for the T2.1 `cfb_ratings`
external-validity oracle (`tests/cfb/test_cfb_ratings_pff_oracle.py`).

## Provenance

Captured 2026-07-10 from **`premium.pff.com/api/v1/teams/overview?league=ncaa&season=2023`**
(PFF Premium Stats 2.0, the surface wrapped by `pff_teams_overview(league="ncaa")`).
The 479-team payload was parsed with the shipped `parse_pff_report`, filtered to
FBS, and **name-bridged to the ESPN `team_id`** via `load_cfb_team_info` +
`cfb_crosswalk._norm_team` on the school+mascot key (PFF ships the full team name
"Abilene Christian Wildcats"; ESPN `school` is school-only, so the bridge keys on
`school + " " + mascot`). 132 FBS teams matched. Rebuild:
`dev/pff_oracle/build_fixture.py`.

| col | type | source |
|---|---|---|
| `team_id` | Utf8 | ESPN team id (bridged from PFF name) |
| `pff_overall` | Float64 | PFF `grades_overall` |
| `pff_offense` | Float64 | PFF `grades_offense` |
| `pff_defense` | Float64 | PFF `grades_defense` (higher = better) |

## Auth (how this was captured)

`premium.pff.com` is Clerk-authenticated with a short-lived `__session` JWT
(~60s) plus the Phoenix `_premium_key` entitlement cookie. A **saved Playwright
`storage_state`** (Clerk `__client` + localStorage) lets a headless browser
auto-refresh `__session` and re-establish `_premium_key` with no re-login — a
one-time manual sign-in (`dev/pff_auth/login_capture.py --headed --manual`)
seeds it, and `dev/pff_auth/reuse_test.py` confirmed a fresh headless context
reused it against a *different* season. All of `dev/pff_auth/` is gitignored
(it holds live session state); only the derived, de-identified grade parquet is
committed here.
