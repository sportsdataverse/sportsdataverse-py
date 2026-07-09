<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [On3 rankings fixture payloads](#on3-rankings-fixture-payloads)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# On3 rankings fixture payloads

Two families live here:

1. **Legacy `_next/data` rankings** — captured 2026-07-07 from the on3.com
   Next.js data routes (buildId `6bd409f28d869b30da255ab5bd16dcd632c84d31`),
   **trimmed** to the first 3 `list` entries per payload. These back the
   deprecated rankings shim (`sportsdataverse/cfb/on3_rankings.py`) and its
   tests in `tests/test_on3_parsers.py`.
2. **Recruit Database (RDB)** — captured 2026-07-08 from the auth-free public
   gateway `api.on3.com/public/rdb/v{1,2}` (see
   `sdv-internal-refs/on3/captures/manifest.csv`), trimmed to a few `list`
   entries. These back `parse_on3_rdb` in `tests/test_on3_rdb.py`.

| File | Route | Notes |
|---|---|---|
| `on3_player_rankings.json` | `/_next/data/{buildId}/rivals/rankings/player/football/2026.json?rankingType=player&sport=football&year=2026` | 3 of 301 entries; nested `person` (rating/consensus/status/high school), `ratings[]`, `nilValue` |
| `on3_team_rankings.json` | `/_next/data/{buildId}/rivals/rankings/team/football/2026.json?rankingType=team&sport=football&year=2026` | 3 of 200 entries under `pageProps.teamData.list` |
| `team_ranking_team_rankings.json` | `GET /public/rdb/v1/team-ranking/football-2025/team-rankings?page=1&pageSize=5` | RDB **paged** envelope (`relatedModel`/`pagination`/`list`); 3 of 50 org rows |
| `player_profile.json` | `GET /public/rdb/v1/player/89617/profile` | RDB **single-object** envelope (`On3PlayerProfileLive`); 1 row |
| `player_all_rankings.json` | `GET /public/rdb/v1/player/89617/all-rankings` | RDB **bare-array** envelope; 3 of 6 ranking rows |
| `filters_status.json` | `GET /public/rdb/v1/filters/status` | RDB bare array of status strings |

Provenance notes:

- The data route **requires** the `rankingType`/`sport`/`year` query params —
  it returns the Next.js 404 page without them.
- The page route pattern is `/rivals/rankings/[rankingType]/[sport]/[year]`
  even though the public URLs live under `/db/rankings/...` (post
  On3–Rivals-merger rewrite).
- `{buildId}` rotates on every On3 deploy; re-capture by pulling the current
  id from any on3.com page's `__NEXT_DATA__` blob (the runtime in
  `sportsdataverse/cfb/on3_runtime.py` automates this).

To refresh, re-capture the same URLs with a current buildId and re-trim to 3
entries. The parser tests are payload-agnostic apart from the trimmed row
count.
