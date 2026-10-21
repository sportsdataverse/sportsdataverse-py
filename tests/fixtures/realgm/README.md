<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [RealGM fixtures](#realgm-fixtures)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# RealGM fixtures

| file | source | notes |
|---|---|---|
| `just_a_moment.html` | `https://basketball.realgm.com/nba/players`, fetched once through the package's own headless-Chromium path (`sportsdataverse.nba.realgm._playwright_html("/nba/players")`, default `SDV_PY_REALGM_WAIT=25`) from a residential IP; captured 2026-10-11 | The Cloudflare challenge page the browser was still showing at the deadline (`<title>Just a moment...</title>`, 28,912 bytes as served; 26,292 after redaction). Byte-for-byte except the per-request Cloudflare identifiers, replaced with `REDACTED` the same way the NCAA Akamai reference ids are: the Ray ID (`cRay` and the `Ray ID:` footer, which Cloudflare maps back to the request and so to the client), the challenge tokens (`__cf_chl_tk`, `__cf_chl_f_tk`, `__cf_chl_rt_tk`) and the `cH`, `cN`, `md`, `mdrd` and `cITimeS` challenge values. The tests read only the title and the absence of tables. The challenge did not clear, the same result as the 2026-10-11 live check (`audit/live-checks/nba-residential.md`, gap-B-165). Every `realgm_*` function reads it as a zero-row answer today (B-M8, strict xfail in `tests/nba/test_realgm.py` until task 4.4). |

Regenerate: `python -c "from sportsdataverse.nba.realgm import _playwright_html; open('just_a_moment.html','w',encoding='utf-8').write(_playwright_html('/nba/players'))"`; keep it only if the title is still the challenge.
