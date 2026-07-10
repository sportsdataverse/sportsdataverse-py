<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [CFB PFF-grade oracle fixtures](#cfb-pff-grade-oracle-fixtures)
  - [Provenance](#provenance)
  - [Full NCAA facet surface verified](#full-ncaa-facet-surface-verified)
  - [Auth (how this was captured)](#auth-how-this-was-captured)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# CFB PFF-grade oracle fixtures

`pff_team_grades_2023.parquet` — PFF team grades for the T2.1 `cfb_ratings`
external-validity oracle (`tests/cfb/test_cfb_ratings_pff_oracle.py`).

## Provenance

Captured 2026-07-10 from **`premium.pff.com/api/v1/teams/overview?league=ncaa&season=2023`**
(PFF Premium Stats 2.0, the surface wrapped by `pff_teams_overview(league="ncaa")`).
The full 479-team payload is committed here as `teams_overview_ncaa_2023.json`
(the tracked rebuild input); `build_fixture.py` parses it with the shipped
`parse_pff_report`, filters to FBS, and **name-bridges to the ESPN `team_id`** via
`load_cfb_team_info` + `cfb_crosswalk._norm_team` on the school+mascot key (PFF
ships the full team name "Abilene Christian Wildcats"; ESPN `school` is school-only,
so the bridge keys on `school + " " + mascot`). **132 FBS teams match** — the
`pff_team_grades_2023.parquet` row count and the number the oracle test pins.

**Reproduce from tracked inputs** (no network, no PFF session needed — the raw
payload is committed): `uv run python dev/pff_oracle/build_fixture.py`. The
separate `tests/fixtures/pff/ncaa_teams_overview.json` (12 teams) and
`ncaa_facet_passing_summary.json` (15 players) are trimmed *parser* fixtures for
`test_pff_parsers.py`, not the oracle rebuild source.

| col | type | source |
|---|---|---|
| `team_id` | Utf8 | ESPN team id (bridged from PFF name) |
| `pff_overall` | Float64 | PFF `grades_overall` |
| `pff_offense` | Float64 | PFF `grades_offense` |
| `pff_defense` | Float64 | PFF `grades_defense` (higher = better) |

## Full NCAA facet surface verified

Beyond `teams/overview`, the player-grade facets
(`/api/v1/facet/{offense,defense,passing,rushing,receiving}/summary?league=ncaa`)
all return real NCAA data through the same session and parse cleanly via
`parse_pff_report` (a trimmed passing/summary sample is committed at
`tests/fixtures/pff/ncaa_facet_passing_summary.json`). The payloads are large
(offense ~7 MB, defense ~14 MB), so capture them **one endpoint at a time with a
short delay** — firing several rapid large fetches back-to-back outruns the ~60 s
`__session` window and the later ones fail (that, not any endpoint issue, is why
an initial batched grab dropped the facets).

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
