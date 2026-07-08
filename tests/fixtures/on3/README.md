<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [On3 rankings fixture payloads](#on3-rankings-fixture-payloads)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# On3 rankings fixture payloads

Captured 2026-07-07 from the on3.com Next.js data routes (buildId
`6bd409f28d869b30da255ab5bd16dcd632c84d31`) and **trimmed** to the first 3
`list` entries per payload — the surrounding envelope (`relatedModel`,
`pagination`) is intact and every kept entry is byte-identical to the live
capture. Used by `tests/test_on3_parsers.py`.

| File | Route | Notes |
|---|---|---|
| `on3_player_rankings.json` | `/_next/data/{buildId}/rivals/rankings/player/football/2026.json?rankingType=player&sport=football&year=2026` | 3 of 301 entries; nested `person` (rating/consensus/status/high school), `ratings[]`, `nilValue` |
| `on3_team_rankings.json` | `/_next/data/{buildId}/rivals/rankings/team/football/2026.json?rankingType=team&sport=football&year=2026` | 3 of 200 entries under `pageProps.teamData.list` |

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
