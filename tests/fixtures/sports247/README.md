<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [247Sports RDB fixture payloads](#247sports-rdb-fixture-payloads)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 247Sports RDB fixture payloads

Captured 2026-07-07 from `ipa.247sports.com` (the 247Sports Recruit Database —
note the host is **`ipa.`**, not `api.`) via curl_cffi `impersonate="chrome"`.
Used by `tests/test_sports247_parsers.py`.

| File | Endpoint | Notes |
|---|---|---|
| `sports247_teams_football.json` | `GET /rdb/v1/teams/?sportKey=1` | full football team directory (139 teams), bare JSON array |
| `sports247_institution_rankings_fb_2026.json` | `GET /rdb/v1/rankings/1/2026/institutionrankings/?pagesize=5` | page 1 of 244 institutions; `{pagination, list}` envelope; 247 + composite ranks/ratings |

Provenance notes:

- The Fastly edge **fingerprint-blocks plain `requests`** (0-byte 403 on every
  route) — the same block class as stats.nba.com. Re-capture with curl or
  curl_cffi Chrome impersonation, never plain requests.
- Slash-less paths 301-redirect to their trailing-slash form; capture with the
  trailing slash.
- Only these two `/rdb/v1/*` GET routes are public; the other ~23 (recruits,
  player rankings, transfers, coaches, crystal-ball predictions, ...) return
  401 without an internal CBSi bearer token.
- `sportKey`: 1 = football, 2 = basketball.

To refresh, re-capture the same URLs. The parser tests are payload-agnostic
apart from the pagesize-5 row count on the rankings fixture.
