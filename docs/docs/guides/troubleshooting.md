---
title: Troubleshooting and FAQ
sidebar_label: Troubleshooting & FAQ
sidebar_position: 6
---

# Troubleshooting and FAQ

The failures people hit most often, what each one means, and what to do about it.

## Which error did I get?

Every error the package raises subclasses `sportsdataverse.errors.SportsDataverseError`,
so one `except` clause catches any of them. The useful distinction is between the
subclasses.

| Exception | Meaning | What to do |
|---|---|---|
| `NoDataError` | The fetch **succeeded** and the answer is "nothing here": a 404 from any host, or ESPN's 200 with a `code: 404` body. | Skip it. The game, season or asset does not exist. |
| `AssetFetchError` | The fetch **failed** and the answer is **unknown**: a 403, a rate limit, a 5xx, an exhausted retry budget, a blank or non-JSON body. | Retry later. Never record it as empty. |
| `SeasonNotFoundError` | You asked a loader for a season before its first published season. | Ask for a later season; the message names the floor. |
| `RawStoreMissError` | A read-only NBA / WNBA raw store has no capture for the game you asked for. | Capture it, or run with the store writable. |
| `ValueError` | The request itself is wrong: a 400 / 422 from the host, a bad argument, an unknown cache mode. | Fix the call; retrying cannot help. |

`NoDataError` and `AssetFetchError` are kept apart on purpose: a rate-limited
season recorded as an empty one is silent data loss. Neither subclasses the other.
`NoESPNDataError` is the old name of `NoDataError` and is the same class, so
existing `except NoESPNDataError` code keeps working.

```python
from sportsdataverse.cfb import espn_cfb_game_rosters, load_cfb_schedule
from sportsdataverse.errors import AssetFetchError, NoDataError

try:
    rosters = espn_cfb_game_rosters(game_id=1)       # not a real ESPN game id
except NoDataError:
    rosters = None                                   # nothing there: skip it

try:
    schedule = load_cfb_schedule([2024])
except AssetFetchError as exc:
    raise RuntimeError(f"fetch failed, retry later: {exc}") from exc
```

A season below a loader's floor fails fast, before any request:

```python
from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.nba import load_nba_rosters

try:
    load_nba_rosters([2024])
except SeasonNotFoundError as exc:
    print(exc)          # season cannot be less than 2025
```

### A read-only raw store misses a game

The NBA and WNBA possession and season-compile paths can read stats.nba.com payloads
through a raw JSON store (`SDV_PY_NBA_RAW_JSON_DIR`, or `raw_store_dir=`). Read-only
mode (`SDV_PY_NBA_RAW_JSON_READONLY=1`, or `raw_store_readonly=True`) means offline:
a capture in the store is served, and a miss raises `RawStoreMissError` naming the
endpoint and game instead of quietly going to the network. Per-game compile loops
catch it, log the game and move on.

```python
# Not run here: the store falls through to stats.nba.com when writable.
from sportsdataverse.errors import RawStoreMissError
from sportsdataverse.nba.nba_possessions import nba_possessions

try:
    poss = nba_possessions("0022300001", raw_store_readonly=True)
except RawStoreMissError as exc:
    print(f"not in the raw store: {exc}")
```

### SchemaDriftWarning and the other warnings

Parsers never raise on a payload of the wrong shape; they return a zero-row frame.
To keep that frame from reading as "no data", they warn `SchemaDriftWarning`, naming
the parser, the payload path and the exception. An id column the Int64 cast refuses
also warns it. In a pipeline that must never publish a drifted parse, turn it into an
error:

```python
import warnings

from sportsdataverse.errors import SchemaDriftWarning

warnings.filterwarnings("error", category=SchemaDriftWarning)
```

The other package warnings, all `UserWarning` subclasses in `sportsdataverse.errors`:

- `EmptyResponseWarning`: a stats.nba.com / stats.wnba.com call came back 2xx with
  an empty object. The usual cause is a missing or invalid parameter, most often
  `Season`.
- `EraCoverageWarning`: NFL model features were built for a season past the last
  validated era.
- `AsOfCutWarning`: a pregame projection could not place its target game in time.
- `UnsupportedPageFormatWarning`: a scraped page is in a layout its parser does not
  read.

## stats.nba.com and stats.wnba.com

### My `nba_stats_*` call hangs

stats.nba.com and stats.wnba.com fingerprint the TLS handshake and also judge the IP
address. From a datacenter or cloud address (CI runners, Colab, most cloud VMs) a
request does not fail: it stalls until it times out. Run these wrappers from a
residential connection. The per-request timeout is `SDV_PY_NBA_STATS_TIMEOUT`
(seconds, default 30); raise it for slow historical endpoints. Retries are set with
`SDV_PY_NBA_STATS_RETRIES`.

```python
# Not run here: stats.nba.com stalls on cloud IPs.
from sportsdataverse.nba import nba_stats

df = nba_stats.nba_stats_leaguedashplayerstats(league_id="00")      # NBA
gleague = nba_stats.nba_stats_leaguedashplayerstats(league_id="20") # G League
```

### `ImportError: ... require curl_cffi`

Plain `requests` is fingerprint-blocked by stats.nba.com, so the runtime uses
[curl_cffi](https://github.com/lexiforest/curl_cffi) with Chrome impersonation. It is
an optional dependency (in the `tests` and `all` extras), imported only when a live
call is made:

```bash
pip install curl_cffi
# or
pip install "sportsdataverse[all]"
```

A few international basketball sites (live FIBA game pages, P. LEAGUE+) need it too.

### Why do the stats.nba.com tests skip?

The test suite gates live tests with `SDV_PY_LIVE_TESTS=1`. The stats.nba.com /
stats.wnba.com tests use a separate gate, `SDV_PY_NBA_STATS_LIVE=1`, which no CI
workflow sets, because those hosts hang on CI runners. To run them, from a
residential IP:

```bash
SDV_PY_NBA_STATS_LIVE=1 uv run pytest tests/nba -k nba_stats
```

## stats.ncaa.org

stats.ncaa.org bans IP addresses quickly, so the package never fetches it directly.
Every request goes through the shared NCAA fetch layer (`mbb_ncaa_fetch` /
`wbb_ncaa_fetch`), which:

- requires a proxy: without one it raises `RuntimeError` (a setup error, not a
  fetch failure);
- caches every page it fetches under `~/.sportsdataverse/ncaa_cache` and serves the
  cache first, so a page is fetched once;
- raises `NoDataError` on a real 404 and `AssetFetchError` for a banned or exhausted
  proxy pool, an uncleared bm-verify challenge or the Terms gate.

Configure it with environment variables (`SDV_PY_NCAA_PROXY_URL`,
`SDV_PY_NCAA_CACHE_DIR`, `SDV_PY_NCAA_TIMEOUT`, `SDV_PY_NCAA_ROTATE_EVERY`, ...) or
programmatically. Never write your own requests against stats.ncaa.org; reuse this
layer.

```python
# Not run here: stats.ncaa.org bans cloud IPs.
from sportsdataverse.mbb import ncaa_mbb_play_by_play
from sportsdataverse.mbb.mbb_ncaa_fetch import update_config

update_config(proxy_url="http://user:password@203.0.113.10:8080")
pbp = ncaa_mbb_play_by_play([5254137, 5254138], errors="skip")   # scrape_status says why a game dropped
```

With the default `errors="raise"`, a failed game raises `AssetFetchError` rather than
being dropped, so a ban never reads as a smaller season.

## My NFL data looks stale

The NFL loaders (`load_nfl_*`) cache their frames: in memory by default, or on disk
with `cache_mode="filesystem"`, for `cache_duration` seconds (24 hours by default).
The cache key is the loader's name and arguments. **It does not include the URL**, so
if a release file is republished under the same name, or the package changes the path
a loader reads, you keep getting the cached frame until it expires. Clear it, or turn
caching off while you debug:

```python
from sportsdataverse.nfl import clear_cache, update_config

clear_cache()                       # drops the memory and filesystem entries
update_config(cache_mode="off")     # no caching for the rest of the session
```

The `SDV_PY_NFL_CACHE` environment variable sets the starting mode. The package-wide
HTTP response cache (`sdv cache ...`, `sportsdataverse.cache`) is a different cache,
off by default; see the [cache recipe](recipes.md#nfl-cache-and-configuration).

## My tests fail on warnings

If your test suite turns warnings into errors (pytest's `-W error` or
`filterwarnings = error`), these are the package warnings you are likely to meet:

- `DeprecationWarning` from a moved or deprecated import path:
  `sportsdataverse.parsed.*` (call `sportsdataverse.<league>` directly) or a league
  that moved, such as `sportsdataverse.epl` (now `sportsdataverse.soccer.epl`). Fix the
  import; the message names the new path.
- `SchemaDriftWarning`, `EmptyResponseWarning` and the others above. Each says
  something real about the data, so assert it (`pytest.warns(SchemaDriftWarning)`)
  where you expect it, or filter that one message where it is incidental. Avoid
  ignoring a whole category.

`import sportsdataverse` itself emits no warnings.
