<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [WNBA cdn.wnba.com liveData fixtures](#wnba-cdnwnbacom-livedata-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# WNBA cdn.wnba.com liveData fixtures

Real captures backing `tests/wnba/test_wnba_live.py`.

| File | URL | Captured | Transport |
|---|---|---|---|
| `playbyplay_1022600097.json` | `https://cdn.wnba.com/static/json/liveData/playbyplay/playbyplay_1022600097.json` | 2026-09-26 | curl_cffi (`impersonate="chrome"`) via `sportsdataverse.nba.nba_stats_runtime._curl_transport` |
| `boxscore_1022600097.json` | `https://cdn.wnba.com/static/json/liveData/boxscore/boxscore_1022600097.json` | 2026-09-26 | curl_cffi (`impersonate="chrome"`) via `sportsdataverse.nba.nba_stats_runtime._curl_transport` |

Game `1022600097` (IND @ CON, 2026-06-13) was taken from the `wnba` block of
`tests/nba/fixtures/official_nba/referee_assignments_2026-06-13.json`. Headers mirror
`nba_live`'s but with `Origin`/`Referer` set to `https://www.wnba.com/` (mirroring wehoop's
`.wnba_cdn_headers()`). Plain `requests` (`dl_utils.download`) 403'd on `cdn.wnba.com` the
same way it did on `cdn.nba.com`; `curl_cffi` Chrome impersonation returned 200 JSON.
