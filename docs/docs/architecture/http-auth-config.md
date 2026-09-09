---
title: HTTP, auth & proxy configuration
sidebar_label: HTTP, auth & proxy config
sidebar_position: 2
---

# HTTP, auth & proxy configuration

Every HTTP call in `sportsdataverse-py` ultimately funnels through one of a
small number of gateways, and each gateway has its own env-var / kwarg
surface for overriding headers, proxies, credentials, timeouts and retries.
This page is the single index of that surface — organized by mechanism, not
by league, since the same handful of patterns repeat across ~15 independent
data sources.

**Precedence is consistent everywhere on this page unless noted otherwise:
explicit keyword argument > source-specific environment variable > a
package-wide environment variable (where one exists) > a hardcoded default.**
Most of this surface reads its environment variables at *call time*, not
import time, so setting one after the package is imported (e.g. mid-test,
mid-notebook) still takes effect. **The one exception is NFL** (§8): its
`SDV_PY_NFL_*` variables are read once into a module-level `NflConfig`
singleton at import time, and stay at whatever they were until
`reset_config()` or an explicit `update_config()` -- changing the env var
after import has no effect on an already-imported process.

## 1. The core gateway: `dl_utils.download()`

[`sportsdataverse.dl_utils.download()`](https://sportsdataverse-py.sportsdataverse.org)
is the canonical HTTP entry point — every ESPN wrapper, every codegen-generated
flat-API wrapper (NHL api-web/edge/stats-rest/records, MLB Stats API,
`mlb_statcast`, `pff`, `nfl_api`, `sports247`, `on3`, HockeyTech's client,
KenPom/Her Hoop Stats via `_subscription_http`, and more) and most
hand-written scrapers call it, directly or through a thin per-source getter.

```python
download(url, params=None, headers=None, proxy=None, timeout=_UNSET,
         num_retries=_UNSET, session=None, logger=None, cache_ttl=None,
         retry_statuses=_RETRYABLE_STATUS)
```

| Parameter | Env fallback | Default | Notes |
|---|---|---|---|
| `timeout` | `SDV_PY_HTTP_TIMEOUT` | `30` | Pass `None` explicitly for *no* timeout — that bypasses the env lookup entirely. |
| `num_retries` | `SDV_PY_HTTP_RETRIES` | `15` | `None` is treated as "omitted" (there's no "infinite retries" reading). |
| `headers` | — | `{}` | Merged by the *caller*, not by `download()` itself — see §6 for who merges vs. replaces. |
| `proxy` | — | `None` | **No env fallback here.** `download()` takes whatever `proxies=` dict it's handed; env-var proxy resolution is implemented independently by the handful of callers listed in §2. |
| `session` | — | shared pooled `requests.Session` | Pass your own for isolated cookies/auth/proxy lifecycle (the shared session's cookie jar is otherwise global). |
| `retry_statuses` | — | `{403, 408, 429, 500, 502, 503, 504}` | Narrow this for an authenticated endpoint where a 403 is a real "forbidden," not host overload. |
| `cache_ttl` | — | source-specific | Overrides the on-disk response cache TTL for one call; see §7. |

**Because every codegen-generated wrapper function accepts `**kwargs` that
flow straight into its getter, `headers=`, `proxy=`, `timeout=`,
`num_retries=`, `session=`, and `cache_ttl=` work as override kwargs on
essentially every public function in the package** (all ~800 ESPN
cross-league wrappers plus every flat-API family), whether or not that
function's own docstring calls them out individually.

> **Exception: HockeyTech.** `hockeytech_api()` (the shared client behind
> PWHL + the 19 minor/junior leagues, §7) accepts `**kwargs` but never
> forwards them to `download()` -- it calls the gateway with a fixed
> `headers=`/`timeout=`/`num_retries=` and nothing else. Passing `proxy=`
> (or any other override kwarg) to a HockeyTech wrapper is silently a no-op.

Example:

```python
from sportsdataverse.nba import espn_nba_scoreboard

df = espn_nba_scoreboard(
    headers={"X-My-Header": "1"},
    proxy={"http": "http://user:pw@proxy:8080", "https": "http://user:pw@proxy:8080"},
    timeout=10,
    num_retries=3,
)
```

## 2. Proxy resolution, by source

`SDV_PY_PROXY` is **not** a universal fallback baked into `download()` — it's
only honored by the sources below that explicitly call a proxy-resolution
helper. Everything else needs `proxy=` (or its source-specific equivalent)
passed on every call, or a source-specific env var.

| Source | Per-call kwarg | Source-specific env | Falls back to `SDV_PY_PROXY`? |
|---|---|---|---|
| KenPom | `proxy=` | `SDV_PY_KENPOM_PROXY` | **Yes** |
| Her Hoop Stats | `proxy=` | `SDV_PY_HERHOOPSTATS_PROXY` | **Yes** |
| RealGM | `proxy=` | `SDV_PY_REALGM_PROXY` | **Yes** |
| Basketball-Reference | `proxy=` | — | No — pass `proxy=` explicitly every call |
| stats.nba.com / stats.wnba.com | `proxy_url=` (note the different name) | — | No |
| stats.ncaa.org (`ncaa_mbb_*`/`ncaa_wbb_*`/`cfb_ncaa_*`/college baseball) | via `update_config(proxy_url=...)` | `SDV_PY_NCAA_PROXY_URL`, plus a managed rotating pool via `SDV_PY_PROXYBONANZA_KEY`/`SDV_PY_PROXYBONANZA_PKG` | No |
| Every ESPN cross-league wrapper + other flat-API families | `proxy=` → `download()` | — | No |

The KenPom/Her Hoop Stats/RealGM three are the only sources where setting one
env var (`SDV_PY_PROXY`) transparently covers every call with no per-call
kwarg needed.

## 3. Username/password, session-cookie auth (KenPom, Her Hoop Stats)

Both sit behind a plain login form (no API), served via the shared
[`sportsdataverse._subscription_http`](https://sportsdataverse-py.sportsdataverse.org)
layer. Every wrapper accepts `email=`, `password=`, `proxy=`, `session=`
(an already-authenticated `requests.Session` to reuse verbatim), and
`headers=` (merged over the defaults, not replaced).

| | KenPom | Her Hoop Stats |
|---|---|---|
| Email env (checked in order) | `KENPOM_EMAIL`, `KP_USER` (hoopR compat), `SDV_PY_KENPOM_EMAIL` | `HERHOOPSTATS_EMAIL`, `SDV_PY_HERHOOPSTATS_EMAIL` |
| Password env | `KENPOM_PW`, `KP_PW`, `SDV_PY_KENPOM_PW` | `HERHOOPSTATS_PW`, `SDV_PY_HERHOOPSTATS_PW` |
| Proxy env | `SDV_PY_KENPOM_PROXY` → `SDV_PY_PROXY` | `SDV_PY_HERHOOPSTATS_PROXY` → `SDV_PY_PROXY` |
| Login helper | `kenpom_login()` | `herhoopstats_login()` |
| Credential-check helper | `has_kenpom_login()` | `has_herhoopstats_login()` |

Successful logins are cached in-process per `(site, email, proxy)` for 30
minutes (`clear_session_cache()` to force a fresh login — useful after
rotating a password or proxy). A rejected login raises `RuntimeError`
immediately rather than silently scraping the logged-out (free-tier) page.

```python
from sportsdataverse.mbb import kenpom_ratings
df = kenpom_ratings(y=2025, email="you@example.com", password="...",
                     proxy="http://user:pw@proxy.example:8080")
```

## 4. Cookie-jar auth (PFF Premium)

`premium.pff.com` is not TLS/JA3-gated — the only gate is session cookies.
`_get()` (and every `pff_*` wrapper) accepts `cookies: Dict[str, str]`
directly. Resolution, in order:

1. an explicit `cookies=` dict on the call,
2. environment — a raw cookie string `SDV_PY_PFF_COOKIES` (`"k=v; k=v"`), or
   the pair `SDV_PY_PFF_PREMIUM_KEY` (the `_premium_key` entitlement cookie) +
   optional `SDV_PY_PFF_SESSION` (the Clerk `__session` JWT),
3. `SDV_PY_PFF_STORAGE_STATE` — a path to a saved Playwright `storage_state`
   JSON (captured once from a headed login). Replayed **headlessly** so Clerk
   re-mints a fresh `__session`; the result is cached in-process for
   `SDV_PY_PFF_STORAGE_STATE_TTL` seconds (default `300`). Needs the optional
   `playwright` extra (`pip install sportsdataverse[pff]` +
   `playwright install chromium`).
4. otherwise `RuntimeError` with setup instructions.

Both the HTTP call (`transport=`) and the browser refresh (`refresher=` on
the internal `_cookies_from_storage_state`) are injectable for offline tests.

## 5. Bearer-token / OAuth auth (NFL.com)

Two independent NFL.com surfaces, each with its own token flow:

**Shield API** (`api.nfl.com`, `nfl_games.py` / `nfl_token_gen()`):

| Precedence | Source |
|---|---|
| 1 | `NFL_ACCESS_TOKEN` env — wins outright *unless* `client_key=`/`client_secret=` are passed explicitly |
| 2 | `client_key=`/`client_secret=` kwargs, else `NFL_CLIENT_KEY`/`NFL_CLIENT_SECRET` env, else the bundled public `WEB_DESKTOP` web-app pair |

Tokens are minted and cached in-process (keyed on the resolved key/secret
pair); pass `force_refresh=True` to bypass the cache.

**NFL Pro** (`pro.nfl.com/api/secured/*`, `nflpro_runtime.py` /
`nflpro_token()` — needs an NFL+ Premium subscription):

| Precedence | Source |
|---|---|
| 1 | `token=` kwarg, else `NFLPRO_TOKEN` env — validated for both **entitlement** (must carry an active `NFL_PLUS_*` plan; a client-credentials token that isn't user-bound raises `NFLProAuthError`) and **freshness** (expired raises too) |
| 2 | `email=`/`password=` kwargs, else `NFLPRO_EMAIL`/`NFLPRO_PW` env — drives a browser login, cached per-account |

## 6. TLS/JA3-fingerprint impersonation (`curl_cffi`)

Some hosts block plain `requests` at the TLS handshake level (a silent
timeout, not an HTTP error), so these sources use `curl_cffi` with Chrome
impersonation instead of the shared `requests`-based `download()`.

| Source | Runtime | `headers=` behavior | `impersonate=` override? | Other tunables |
|---|---|---|---|---|
| stats.nba.com / stats.wnba.com | `nba/nba_stats_runtime.py` | **Replaces** the default `stats_headers(host)` wholesale — pass a full header set, not a delta | No — hardcoded `"chrome"` | `proxy_url=`, `transport=` (injectable); `SDV_PY_NBA_STATS_TIMEOUT` (default `30`), `SDV_PY_NBA_STATS_RETRIES` (default `0`), `SDV_PY_NBA_STATS_BACKOFF` (default `1.5`) |
| stats.ncaa.org (`ncaa_mbb_*`/`ncaa_wbb_*`/`cfb_ncaa_*`/college baseball) | `mbb/mbb_ncaa_fetch.py` | via the shared NCAA transport | **Yes** — `SDV_PY_NCAA_IMPERSONATE` (default `"chrome"`) | See §8 for the full `NcaaFetchConfig` surface |
| 247Sports (recruiting DB + site-pages) | `cfb/sports247_runtime.py`, `sports247_site_pages_runtime.py` | fixed headers | No — hardcoded `"chrome"` | `SDV_PY_247_RETRIES`/`_DELAY`/`_BACKOFF` (pacing only, see §9) |

## 7. API keys

| Source | Kwarg | Env var |
|---|---|---|
| The Odds API (`odds/the_odds_api.py`, `toa_*` functions) | `api_key=` | `ODDS_API_KEY` |
| HockeyTech (PWHL + 19 minor/junior leagues) | — | `SDV_<LEAGUE>_API_KEY` (e.g. `SDV_PWHL_API_KEY`) overrides that league's public default key for every view; wins even over the PBP-specific key override some leagues carry |
| Fox Sports Bifrost | — | `SDV_PY_FOX_DATA_KEY`, `SDV_PY_FOX_FEED_KEY` — override the publicly-shipped default API keys baked into the Fox web app |

## 8. Per-family config objects

Two sources expose a full `get_config()` / `update_config()` / `reset_config()`
singleton (mutate at runtime, or set env vars before first use):

**NFL** (`sportsdataverse.nfl`, `NflConfig`):

| Env var | Field | Default |
|---|---|---|
| `SDV_PY_NFL_CACHE` | `cache_mode` (`memory`/`filesystem`/`off`) | `memory` |
| `SDV_PY_NFL_CACHE_DIR` | `cache_dir` | `~/.cache/sportsdataverse/nfl` |
| `SDV_PY_NFL_CACHE_DURATION` | `cache_duration` (seconds) | `86400` |
| `SDV_PY_NFL_VERBOSE` | `verbose` | `True` |
| `SDV_PY_NFL_TIMEOUT` | `timeout` (seconds) | `30` |
| `SDV_PY_NFL_USER_AGENT` | `user_agent` | `"sportsdataverse-py-nfl"` |

> **Known gap:** `NflConfig.user_agent` (and therefore `SDV_PY_NFL_USER_AGENT`)
> is stored on the config object but is not currently applied to any outgoing
> request header — setting it has no observable effect yet. Use the universal
> `headers={"User-Agent": ...}` kwarg (§1) on individual NFL calls instead
> until this is wired up.

**stats.ncaa.org** (`sportsdataverse.mbb.mbb_ncaa_fetch`, `NcaaFetchConfig`):

| Env var | Field | Default |
|---|---|---|
| `SDV_PY_NCAA_CACHE_DIR` | `cache_dir` | `~/.sportsdataverse/ncaa_cache` |
| `SDV_PY_NCAA_PROXY_URL` | `proxy_url` | `None` |
| `SDV_PY_PROXYBONANZA_KEY` / `SDV_PY_PROXYBONANZA_PKG` | `proxybonanza_key` / `proxybonanza_pkg` | `None` — managed rotating-proxy pool credentials |
| `SDV_PY_NCAA_TIMEOUT` | `timeout` (seconds) | `45` |
| `SDV_PY_NCAA_IMPERSONATE` | `impersonate` | `"chrome"` |
| `SDV_PY_NCAA_ROTATION_BACKOFF` | `rotation_backoff` (seconds) | `1.0` |
| `SDV_PY_NCAA_ROTATE_EVERY` | `rotate_every` (proactive rotation after N successful fetches; `0` = never) | `200` |

`NcaaFetchConfig.__repr__` redacts `proxy_url` credentials and the
ProxyBonanza key — safe to print/log.

## 9. Rate-limiting / pacing env vars

Not auth, but frequently need overriding alongside it (a backfill against an
old season, or a host that's rate-limiting you):

| Env var | Source | Default |
|---|---|---|
| `SDV_PY_247_RETRIES` / `SDV_PY_247_DELAY` / `SDV_PY_247_BACKOFF` | 247Sports roster/recruiting scrape (`cfb/cfb_roster_talent.py`) | `3` / `0.5`s / `2.0`s |
| `SDV_PY_HOOPSHYPE_DELAY` | HoopsHype salary scrape (`nba/nba_salary_draft.py`) | `0.5`s |
| `SDV_PY_BREF_RATE_DELAY` | Basketball-Reference (`nba/bref.py`) | source default |
| `SDV_PY_REALGM_TTL` / `_DELAY` / `_WAIT` / `_POLL` | RealGM (Cloudflare-challenge browser session pacing, `nba/realgm.py`) | source defaults |
| `SDV_PY_NBA_STATS_TIMEOUT` / `_RETRIES` / `_BACKOFF` | stats.nba.com / stats.wnba.com | `30`s / `0` / `1.5` |

## 10. Cache / storage directories (adjacent, not auth — easy to confuse)

| Env var | Controls |
|---|---|
| `SDV_PY_CACHE_DIR` | Generic on-disk response cache root (`~/.cache/sportsdataverse/` default) used by `dl_utils.download()`'s cache layer |
| `SDV_PY_NBA_CACHE_DIR` | NBA season-compile cache (`nba/nba_season_compile.py`) |
| `SDV_PY_NBA_RAW_JSON_DIR` (+ per-endpoint `SDV_PY_NBA_RAW_JSON_DIR_{ENDPOINT}`) / `SDV_PY_NBA_RAW_JSON_READONLY` / `SDV_PY_NBA_RAW_JSON_HTTP_TIMEOUT` | NBA read-through raw JSON store (`readonly=1` means fully offline — a miss raises rather than falling back to the network) |
| `SDV_PY_WNBA_RAW_JSON_DIR` / `SDV_PY_WNBA_RAW_JSON_READONLY` | WNBA counterpart of the above |
| `SDV_PY_CFB_MODEL_DIR` | Override the bundled CFB fourth-down model artifact directory |
| `NHL_XG_MODEL_DIR` | Override the bundled NHL xG model artifact directory (note: no `SDV_PY_` prefix) |

## 11. Publishing / GitHub token (`sportsdataverse.release`)

Release-asset publish/download helpers shell out to the `gh` CLI. Its
subprocess environment resolves `GH_TOKEN`, falling back to `GITHUB_PAT` if
unset (mirrors the R-side `.Renviron` convention, where `GITHUB_PAT` is the
long-standing name). Upload retry pacing is dot-separated (an R-convention
holdover, not `SDV_PY_`-prefixed): `SPORTSDATAVERSE.UPLOAD.INSIST` (default
`"true"`), `SPORTSDATAVERSE.UPLOAD.PAUSE_BASE` (`0.05`s),
`SPORTSDATAVERSE.UPLOAD.PAUSE_MIN` (`1`s), `SPORTSDATAVERSE.UPLOAD.MAX_TIMES`
(`20`, ignored when `INSIST` is false).

## 12. User-Agent overrides

Most sources hardcode a realistic browser UA string with no dedicated env
var — override it per-call via the universal `headers={"User-Agent": ...}`
kwarg (§1) instead. Two narrow exceptions carry their own env var:
`SDV_PY_NFL_USER_AGENT` (currently inert — see the §8 callout) and
`SDV_PY_USER_AGENT`, which is read in exactly one place (`nhl/nhl_xg.py`'s
model-artifact download), not package-wide.

## Quick reference: "I need to..."

- **...route one call through a proxy.** Use `proxy=` (or `proxy_url=` for
  `nba_stats`/`wnba_stats`) on the call. For KenPom/Her Hoop Stats/RealGM,
  setting `SDV_PY_PROXY` once covers every call instead.
- **...supply my own subscription login.** `email=`/`password=` on any
  KenPom/Her Hoop Stats call, or the matching env vars in §3.
- **...authenticate against PFF Premium without Playwright.** Set
  `SDV_PY_PFF_PREMIUM_KEY` + `SDV_PY_PFF_SESSION` (copy the two cookies from
  a logged-in browser's dev tools) — see §4.
- **...use my own NFL Pro / NFL.com token.** `NFLPRO_TOKEN` / `NFL_ACCESS_TOKEN`
  — see §5.
- **...raise a timeout for a slow historical season.** The universal
  `timeout=` kwarg (§1), or `SDV_PY_NBA_STATS_TIMEOUT` for stats.nba.com
  specifically (§6).
- **...add a custom header package-wide.** There's no package-wide header
  injection point — pass `headers=` per call (§1), merged over defaults for
  most sources but **replaced wholesale** for `nba_stats`/`wnba_stats` (§6).
