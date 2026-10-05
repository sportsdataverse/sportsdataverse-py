<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Real error bodies for the flat-API getter tests](#real-error-bodies-for-the-flat-api-getter-tests)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Real error bodies for the flat-API getter tests

Bodies captured live on 2026-10-05 with `curl` (one request each, 3 s apart), saved
byte-for-byte (the ESPN 400 was gunzipped). `tests/codegen/test_runtime_errors.py`
serves them through the real `dl_utils.download()` to prove a failed fetch raises
instead of reaching a parser as data.

| File | Request | Status | Content-Type |
|---|---|---|---|
| `fox_bifrost_401_invalid_apikey.json` | `GET https://api.foxsports.com/bifrost/v1/nfl/league/teamnav?apikey=<invalid>&api-version=1.1` | 401 | `application/json` |
| `espn_site_roster_400.json` | `GET https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/99999/roster` | 400 | `application/json;charset=UTF-8` |
| `espn_site_summary_404.json` | `GET https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event=1` | 404 | `application/json;charset=UTF-8` |
| `espn_core_event_404.json` | `GET https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/1` | 404 | `application/json;charset=utf-8` |
| `cbs_napi_404.json` | `GET https://api.cbssports.com/napi/resource/notarealresource` | 404 | `application/json; charset=utf-8` |
| `nhl_api_web_404.html` | `GET https://api-web.nhle.com/v1/gamecenter/1/play-by-play` | 404 | `text/html;charset=iso-8859-1` |

ESPN answered the summary with a real HTTP 404 here; the legacy 200-with-`code: 404`
envelope could not be reproduced live, so that test serves the real
`espn_site_summary_404.json` body with a 200 status. No 5xx or non-JSON 200 could be
provoked on demand; those tests use short synthetic bodies.
