<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [NBA cdn.nba.com liveData fixtures](#nba-cdnnbacom-livedata-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# NBA cdn.nba.com liveData fixtures

Real captures backing `tests/nba/test_nba_live.py`.

| File | URL | Captured | Transport |
|---|---|---|---|
| `playbyplay_0022500001.json` | `https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_0022500001.json` | 2026-09-26 | curl_cffi (`impersonate="chrome"`) via `sportsdataverse.nba.nba_stats_runtime._curl_transport` |
| `boxscore_0022500001.json` | `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_0022500001.json` | 2026-09-26 | curl_cffi (`impersonate="chrome"`) via `sportsdataverse.nba.nba_stats_runtime._curl_transport` |

Game `0022500001` is the 2025-26 regular-season opener. `sportsdataverse.dl_utils.download`
(plain `requests`, Chrome UA + `Origin`/`Referer: https://www.nba.com`, matching hoopR's
`.nba_cdn_headers()`) returned a 403 HTML body for both URLs during the transport spike;
switching to `curl_cffi` with Chrome TLS impersonation returned 200 JSON for both.
