<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [247Sports RDB fixture payloads](#247sports-rdb-fixture-payloads)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# 247Sports RDB fixture payloads

Captured 2026-07-07 from `ipa.247sports.com` (the 247Sports Recruit Database —
note the host is **`ipa.`**, not `api.`) via curl_cffi `impersonate="chrome"`.
Used by `tests/test_sports247_parsers.py`.

**Auth-free public routes:**

| File | Endpoint | Notes |
|---|---|---|
| `sports247_teams_football.json` | `GET /rdb/v1/teams/?sportKey=1` | full football team directory (139 teams), bare JSON array |
| `sports247_institution_rankings_fb_2026.json` | `GET /rdb/v1/rankings/1/2026/institutionrankings/?pagesize=5` | page 1 of 244 institutions; `{pagination, list}` envelope; 247 + composite ranks/ratings |

**Guest-JWT-gated routes** (trimmed to 3 rows each; `GET https://247sports.com/`
mints a guest `JWT` cookie with no login, passed as `Authorization: Bearer`):

| File | Endpoint | Shape |
|---|---|---|
| `sports247_recruits_fb_2026.json` | `/rdb/v1/recruits/?sportKey=1&year=2026` | `{pagination, players}` — individual recruit DB |
| `sports247_transfers_fb_2026.json` | `/rdb/v1/transfers/?sportKey=1&year=2026` | `{lastUpdated, pagination, players}` — rows nested under `player` |
| `sports247_coaches_fb_2026.json` | `/rdb/v1/coaches/?sportKey=1&year=2026` | `{pagination, results}` |
| `sports247_transfer_portal_player_feed_fb_2026.json` | `/rdb/v1/rankings/1/2026/transferPortalPlayerfeed/` | `{rankings}` |
| `sports247_composite_team_ranking_feed_fb_2026.json` | `/rdb/v1/rankings/1/2026/compositeTeamRankingFeed/` | bare array |
| `sports247_transfer_portal_only_team_feed_fb_2026.json` | `/rdb/v1/rankings/1/2026/transferPortalOnlyTeamFeed/` | bare array |
| `sports247_current_target_predictions_fb_2026.json` | `/rdb/v1/sites/1/years/2026/sports/1/currentTargetPredictions/` | bare array — "crystal ball" |
| `sports247_sports_year_fb.json` | `/rdb/v1/sports/1/year/` | bare array of scalar years |
| `sports247_tags_autocomplete.json` | `/rdb/v1/tags/autocomplete/?defaultName=smith` | bare array |
| `sports247_positions_fb_2026.json` | `/rdb/v1/positions/?sportKey=1&year=2026` | bare array (16 rows) — position lookup (`group`, `groupKey`, `name`, `label`, `value`); probe-confirmed guest-usable 2026-07-08 |

Provenance notes:

- The Fastly edge **fingerprint-blocks plain `requests`** (0-byte 403 on every
  route) — the same block class as stats.nba.com. Re-capture with curl or
  curl_cffi Chrome impersonation, never plain requests.
- Slash-less paths 301-redirect to their trailing-slash form; capture with the
  trailing slash.
- The gated routes need a **guest bearer JWT** — `GET https://247sports.com/`
  sets a `JWT` cookie (no login; ~12 h TTL, `sub`/`iss` `247sports.com`,
  `fastly: true`); pass it as `Authorization: Bearer <jwt>`. The runtime
  (`sportsdataverse/cfb/sports247_runtime.py`) mints/caches/refreshes it
  automatically. The remaining 13 `/rdb/v1/*` GET routes (`biggestMovers`,
  `archivedPlayerRankings`, `playerSportRankings`, `unrankedRecruits`,
  `rankings`, `sports`, `year`, `institutionGroups`, the `tags/.../photos`
  pair, ...) stay 403 even with the guest token — they need a
  logged-in/premium session.
- `sportKey`: 1 = football, 2 = basketball.

To refresh, re-capture the same URLs and re-trim enveloped payloads to 3 rows.
The parser tests are payload-agnostic apart from the row counts asserted in
`tests/test_sports247_parsers.py`.
