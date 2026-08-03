"""Vendor-agnostic proxy canary for stats.ncaa.org bm-verify.

Runs the SAME small MBB probe (default 10 known-good contests x 2 game-detail
pages) through every proxy vendor listed in a TOML config, scoring each fetch
with the PRODUCTION classifier from :mod:`sportsdataverse.mbb.mbb_ncaa_fetch`
(``_ban_check`` / ``_is_challenge`` / the 1 KB size floor). A "clean" here is a
"clean" in the real backfill, so the winner is chosen on measured bm-verify
solve rate -- not on a vendor's marketing.

Why this exists: the ProxyBonanza pool was ASN-confirmed **datacenter**
(QuickPacket / Hivelocity / ReliableSite), which is exactly what Akamai
bm-verify + IP reputation defeats. Before spending on a residential/ISP/mobile
or managed-unblocker vendor, prove one actually solves the challenge on this
host. See ``docs/SCRAPING_NOTES.md``.

Two adapter classes, both reusing the existing :class:`NcaaFetcher` (so the
probe's fetch path == production's):

* ``proxy_browser``   -- raw residential/ISP/mobile proxies + the Playwright
  new-headless solver (NetNut, Decodo, mobile). ``NcaaFetcher.with_browser``.
* ``unblocker_zyte``  -- Zyte API (``api.zyte.com``): URL in, solved HTML out.
* ``unblocker_proxy`` -- a managed-unblocker PROXY endpoint that returns solved
  HTML for a plain GET (Bright Data Web Unlocker, Oxylabs Web Unblocker).

Config: copy ``canary_vendors.toml.example`` -> ``canary_vendors.toml`` (gitignored)
and fill in whichever trial creds you have. Vendors with placeholder/empty creds
are skipped with a note, so you can add trials one at a time.

Run via ``scripts/run_canary.sh`` (wires PYTHONPATH + the venv python).
"""

from __future__ import annotations

import argparse
import base64
import statistics
import sys
import tempfile
import time
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlsplit

from sportsdataverse.mbb.mbb_ncaa_fetch import (
    _MIN_CONTENT_BYTES,
    _RAW_FETCH_JS,
    NcaaFetchConfig,
    NcaaFetcher,
    _ban_check,
    _browser_response_unsolved,
    _is_challenge,
)

# Known-good captured MBB contests (present in mbb/json/) -- valid games, so any
# non-clean result is unambiguously a vendor/challenge failure, not a bad id.
DEFAULT_CONTEST_IDS = [
    "6388769",
    "6388905",
    "6388907",
    "6388965",
    "6388997",
    "6389038",
    "6389078",
    "6389091",
    "6389095",
    "6389099",
]

# Two bm-verify-gated game-detail pages per game (the ones curl_cffi can't clear).
PAGES = ("play_by_play", "individual_stats")

# Substrings that mark a config value as an unfilled placeholder -> skip vendor.
_PLACEHOLDERS = (
    "your_",
    "user:pass",
    "customer-xxx",
    "xxxxxx",
    "<",
    "changeme",
    "example",
)

CLEAN, STUB, CHALLENGE, BAN, TIMEOUT, ERROR = (
    "clean",
    "stub",
    "challenge",
    "ban",
    "timeout",
    "error",
)
_CATS = (CLEAN, STUB, CHALLENGE, BAN, TIMEOUT, ERROR)


def classify(text: str) -> str:
    """Classify a returned body the way production does -- ban > challenge > size floor.

    Order is load-bearing: a ban page can be small, and the markerless XHR stub
    (15-411 B observed) is only caught by the size floor, so it must come last.
    """
    if _ban_check(text) != "clean":
        return BAN
    if _is_challenge(text):
        return CHALLENGE
    if len(text) < _MIN_CONTENT_BYTES:
        return STUB
    return CLEAN


def classify_error(exc: Exception) -> str:
    """Bucket a fetch exception (NcaaFetcher raises loudly on ban/unsolved)."""
    msg = str(exc).lower()
    if "every proxy" in msg or "banned" in msg or "403" in msg:
        return BAN
    if "bm-verify" in msg or "challenge" in msg:
        return CHALLENGE
    if "timeout" in msg or "timed out" in msg:
        return TIMEOUT
    return ERROR


def _has_placeholder(value: str) -> bool:
    low = value.lower()
    return not value.strip() or any(p in low for p in _PLACEHOLDERS)


def _vendor_ready(vendor: dict) -> Optional[str]:
    """Return a skip-reason string if the vendor's creds are unfilled, else None."""
    vtype = vendor.get("type", "")
    if vtype in ("proxy_browser", "proxy_patchright"):
        proxies = vendor.get("proxies") or []
        if not proxies or any(_has_placeholder(p) for p in proxies):
            return "no proxies configured (placeholder/empty)"
    elif vtype == "unblocker_zyte":
        if _has_placeholder(vendor.get("api_key", "")):
            return "no api_key configured (placeholder/empty)"
    elif vtype == "unblocker_proxy":
        if _has_placeholder(vendor.get("proxy", "")):
            return "no proxy endpoint configured (placeholder/empty)"
    elif vtype == "managed_cdp":
        if _has_placeholder(vendor.get("cdp_url", "")):
            return "no cdp_url configured (placeholder/empty)"
    else:
        return f"unknown vendor type {vtype!r}"
    return None


# Real Chrome UA -- MUST override the browser's default, which leaks
# "HeadlessChrome" in new-headless mode. That leak is the single tell that made
# earlier patchright runs fail; with it fixed the solve clears bm-verify (proven
# live 2026-07-16: 200 + 135 KB real PBP on the first attempt).
_REAL_CHROME_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)


class _PatchrightTransport:
    """Local anti-detect (patchright) Chromium solver, proxy-bound like production.

    Drives **patchright** (a drop-in Playwright fork that closes the
    ``Runtime.enable`` CDP leak + ``navigator.webdriver`` tells) in **new-headless**
    mode (``--headless=new`` -> real GPU/ANGLE, not SwiftShader) with a **real
    Chrome UA** (no ``HeadlessChrome`` leak). This exact combination clears
    stats.ncaa.org bm-verify from a US residential IP where vanilla Playwright and
    a leaky-UA patchright both failed. Requires a host with a real GPU.
    """

    def __init__(
        self,
        *,
        challenge_wait_ms: int = 8000,
        nav_timeout_ms: int = 45000,  # residential + multi-round-trip solve needs headroom
        solve_attempts: int = 3,
        user_agent: str = _REAL_CHROME_UA,
    ) -> None:
        self.user_agent = user_agent
        self.challenge_wait_ms = challenge_wait_ms
        self.nav_timeout_ms = nav_timeout_ms
        self.solve_attempts = max(1, solve_attempts)
        self._pw: Any = None
        self._ctx: Any = None
        self._page: Any = None
        self._current_proxy: Optional[str] = None

    def _ensure(self, proxies: "dict[str, str]") -> None:
        proxy = proxies.get("http") or proxies.get("https") or None
        if self._page is not None:
            if proxy == self._current_proxy:
                return
            self.close()  # proxy rotated -> relaunch (context is proxy-bound at launch)
        import atexit

        from patchright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        udd = tempfile.mkdtemp(prefix="patchright_udd_")
        launch: "dict[str, Any]" = {
            "user_data_dir": udd,
            # new-headless (real GPU/ANGLE) + real Chrome UA -- the config that
            # actually clears bm-verify. headless=False + --headless=new is the
            # Playwright idiom for new-headless.
            "headless": False,
            "args": ["--headless=new"],
            "user_agent": self.user_agent,
            "no_viewport": True,
        }
        if proxy:
            parts = urlsplit(proxy)
            username = parts.username
            if username and "-session-" in username:
                # Sticky-session vendors (Decodo) pin the egress IP to the
                # session id in the username. Re-mint it on every LAUNCH so a
                # relaunch after a failed solve lands on a fresh IP -- an aged
                # session (~70min ceiling) stops clearing bm-verify, and
                # retrying it on the same IP fails forever.
                import re as _re
                import uuid as _uuid

                username = _re.sub(r"(-session-)\w+", rf"\g<1>{_uuid.uuid4().hex[:12]}", username)
            launch["proxy"] = {
                "server": f"{parts.scheme}://{parts.hostname}:{parts.port}",
                **({"username": username} if username else {}),
                **({"password": parts.password} if parts.password else {}),
            }
        self._ctx = self._pw.chromium.launch_persistent_context(**launch)
        self._page = self._ctx.pages[0] if self._ctx.pages else self._ctx.new_page()
        self._current_proxy = proxy
        atexit.register(self.close)

    def __call__(self, url: str, proxies: "dict[str, str]", headers: "dict[str, str]") -> "tuple[int, str]":
        self._ensure(proxies)
        # Navigate ONCE (this triggers + solves the challenge), then poll the raw
        # in-page fetch until _abck is minted -- the proven pattern. Re-navigating
        # per attempt through a slow residential proxy is what timed the solve out.
        self._page.goto(url, wait_until="domcontentloaded", timeout=self.nav_timeout_ms)
        status, text = 0, ""
        for _ in range(self.solve_attempts):
            self._page.wait_for_timeout(self.challenge_wait_ms)
            result = self._page.evaluate(_RAW_FETCH_JS, url)
            status, text = int(result["status"]), str(result["text"])
            if not _browser_response_unsolved(text):
                return status, text
        # Retire the (likely aged-out) session so the caller's retry relaunches
        # on a freshly-minted sticky id instead of re-failing the same IP.
        self.close()
        raise RuntimeError(
            f"patchright: bm-verify not passed after {self.solve_attempts} attempts ({len(text)}-byte body)"
        )

    def close(self) -> None:
        for obj, meth in ((self._ctx, "close"), (self._pw, "stop")):
            try:
                if obj is not None:
                    getattr(obj, meth)()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        self._pw = self._ctx = self._page = None
        self._current_proxy = None

    def __enter__(self) -> "_PatchrightTransport":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _ManagedCdpTransport:
    """Drive a MANAGED remote browser over CDP (Bright Data Scraping Browser,
    Browserbase, Zyte browser). The provider owns the hardened fingerprint,
    residential egress, and Akamai solve; Playwright only steers it. Same shape
    as sdv-py's ncaa.fetch.ManagedBrowserTransport, kept self-contained here so
    the canary doesn't depend on that (currently uncommitted) module.
    """

    def __init__(
        self,
        endpoint_url: str,
        *,
        challenge_wait_ms: int = 8000,
        nav_timeout_ms: int = 120000,
        solve_attempts: int = 2,
    ) -> None:
        self._endpoint = endpoint_url
        self.challenge_wait_ms = challenge_wait_ms
        self.nav_timeout_ms = nav_timeout_ms
        self.solve_attempts = max(1, solve_attempts)
        self._pw: Any = None
        self._browser: Any = None
        self._page: Any = None

    def _ensure(self) -> None:
        if self._page is not None:
            return
        from playwright.sync_api import sync_playwright

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.connect_over_cdp(self._endpoint, timeout=self.nav_timeout_ms)
        self._page = self._browser.new_page()

    def __call__(self, url: str, proxies: "dict[str, str]", headers: "dict[str, str]") -> "tuple[int, str]":
        self._ensure()
        status, text = 0, ""
        for _ in range(self.solve_attempts):
            self._page.goto(url, wait_until="domcontentloaded", timeout=self.nav_timeout_ms)
            self._page.wait_for_timeout(self.challenge_wait_ms)
            result = self._page.evaluate(_RAW_FETCH_JS, url)
            status, text = int(result["status"]), str(result["text"])
            if not _browser_response_unsolved(text):
                return status, text
        raise RuntimeError(
            f"managed_cdp: bm-verify not passed after {self.solve_attempts} attempts ({len(text)}-byte body)"
        )

    def close(self) -> None:
        for obj, meth in ((self._browser, "close"), (self._pw, "stop")):
            try:
                if obj is not None:
                    getattr(obj, meth)()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
        self._pw = self._browser = self._page = None

    def __enter__(self) -> "_ManagedCdpTransport":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _make_zyte_transport(api_key: str, render: str) -> Callable[..., "tuple[int, str]"]:
    import requests

    def _t(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        key = "browserHtml" if render == "browserHtml" else "httpResponseBody"
        r = requests.post(
            "https://api.zyte.com/v1/extract",
            auth=(api_key, ""),
            json={"url": url, key: True},
            timeout=120,
        )
        if r.status_code != 200:
            return r.status_code, r.text
        data = r.json()
        status = int(data.get("statusCode") or 200)
        if "browserHtml" in data:
            return status, str(data["browserHtml"])
        body = data.get("httpResponseBody")
        return (status, base64.b64decode(body).decode("utf-8", "replace")) if body else (status, "")

    return _t


def _make_unblocker_proxy_transport(proxy: str) -> Callable[..., "tuple[int, str]"]:
    # Bright Data Web Unlocker / Oxylabs Web Unblocker: plain GET through their
    # proxy endpoint returns already-solved HTML. verify=False because these MITM
    # TLS (their CA isn't in the trust store); the "auth" is the proxy creds.
    import curl_cffi

    def _t(url: str, proxies: dict, headers: dict) -> "tuple[int, str]":
        r = curl_cffi.get(
            url,
            headers=headers,
            proxies={"http": proxy, "https": proxy},
            timeout=120,
            impersonate="chrome",
            verify=False,
        )
        return r.status_code, r.text

    return _t


def build_fetcher(vendor: dict, cache_dir: Path) -> NcaaFetcher:
    """Construct the right NcaaFetcher for a vendor (all context-managed)."""
    vtype = vendor["type"]
    if vtype == "proxy_browser":
        cfg = NcaaFetchConfig(cache_dir=cache_dir)
        # nav_timeout_ms lower than the prod default so a dead/typo'd proxy host
        # fails a game in ~20s instead of ~40s; a real IP resolves well under it.
        return NcaaFetcher.with_browser(cfg, proxy_pool=list(vendor["proxies"]), nav_timeout_ms=20000)
    if vtype == "proxy_patchright":
        cfg = NcaaFetchConfig(cache_dir=cache_dir, transport=_PatchrightTransport())
        return NcaaFetcher(cfg, proxy_pool=list(vendor["proxies"]))
    if vtype == "managed_cdp":
        cfg = NcaaFetchConfig(cache_dir=cache_dir, transport=_ManagedCdpTransport(vendor["cdp_url"]))
        return NcaaFetcher(cfg, proxy_pool=None)
    if vtype == "unblocker_zyte":
        transport = _make_zyte_transport(vendor["api_key"], vendor.get("render", "browserHtml"))
    elif vtype == "unblocker_proxy":
        transport = _make_unblocker_proxy_transport(vendor["proxy"])
    else:  # pragma: no cover - guarded by _vendor_ready
        raise ValueError(f"unknown vendor type {vtype!r}")
    cfg = NcaaFetchConfig(cache_dir=cache_dir, transport=transport)
    return NcaaFetcher(cfg, proxy_pool=None)


def _fetch_page(fetcher: NcaaFetcher, cid: str, page: str) -> str:
    if page == "play_by_play":
        return fetcher.fetch_game_pbp(cid, force=True)
    return fetcher.fetch_game_individual_stats(cid, force=True)


def probe_vendor(vendor: dict, contest_ids: list[str], delay: float) -> list[dict]:
    """Run one vendor over all contests x pages. Returns per-fetch result rows."""
    rows: list[dict] = []
    cache_dir = Path(tempfile.mkdtemp(prefix=f"ncaa_canary_{vendor['name']}_"))
    with build_fetcher(vendor, cache_dir) as fetcher:
        for game_idx, cid in enumerate(contest_ids, 1):
            for page in PAGES:
                t0 = time.monotonic()
                try:
                    text = _fetch_page(fetcher, cid, page)
                    cat, nbytes = classify(text), len(text)
                except Exception as exc:  # noqa: BLE001 - a probe records failures, never aborts
                    cat, nbytes = classify_error(exc), 0
                rows.append(
                    {
                        "cid": cid,
                        "page": page,
                        "cat": cat,
                        "bytes": nbytes,
                        "sec": round(time.monotonic() - t0, 1),
                        "game_idx": game_idx,
                    }
                )
                print(
                    f"    [{vendor['name']}] game {game_idx:>2}/{len(contest_ids)} "
                    f"{page:<16} {cat:<9} {nbytes:>7}B {rows[-1]['sec']:>5}s",
                    flush=True,
                )
                if delay:
                    time.sleep(delay)
    return rows


def scorecard(vendor: dict, rows: list[dict], n_games: int) -> str:
    """Aggregate a vendor's rows into a markdown scorecard section."""
    counts = {c: sum(1 for r in rows if r["cat"] == c) for c in _CATS}
    total = len(rows)
    clean_pct = 100 * counts[CLEAN] / total if total else 0.0
    # A game is clean only if BOTH its pages are clean (production needs both).
    by_game: dict[str, list[str]] = {}
    for r in rows:
        by_game.setdefault(r["cid"], []).append(r["cat"])
    clean_games = sum(1 for cats in by_game.values() if all(c == CLEAN for c in cats))
    # First non-clean fetch's game index = IP-lifetime signal.
    first_fail = next((r["game_idx"] for r in rows if r["cat"] != CLEAN), None)
    clean_bytes = [r["bytes"] for r in rows if r["cat"] == CLEAN]
    med_bytes = int(statistics.median(clean_bytes)) if clean_bytes else 0
    verdict = "PASS" if clean_games >= 0.9 * n_games else "FAIL"

    lines = [
        f"### {vendor['name']}  ({vendor['type']}) -- **{verdict}**",
        "",
        f"- clean pages: **{counts[CLEAN]}/{total}** ({clean_pct:.0f}%)",
        f"- clean games (both pages): **{clean_games}/{n_games}**",
        f"- first failure at game #: {first_fail if first_fail else 'none'}",
        f"- median clean page size: {med_bytes:,} B",
        "- breakdown: " + ", ".join(f"{c}={counts[c]}" for c in _CATS if counts[c]),
        "",
    ]
    return "\n".join(lines)


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="canary_vendors.toml", help="TOML vendor config")
    ap.add_argument("--games", type=int, default=0, help="limit to first N contests (0=all)")
    ap.add_argument("--delay", type=float, default=1.0, help="seconds between fetches (be gentle)")
    ap.add_argument("--out", default="canary_out", help="output directory for the scorecard")
    args = ap.parse_args(argv)

    cfg_path = Path(args.config)
    if not cfg_path.exists():
        print(
            f"config not found: {cfg_path}\ncopy canary_vendors.toml.example -> {cfg_path} and fill in trial creds.",
            file=sys.stderr,
        )
        return 2
    conf = tomllib.loads(cfg_path.read_text(encoding="utf-8"))
    contest_ids = [str(c) for c in conf.get("contest_ids", DEFAULT_CONTEST_IDS)]
    if args.games:
        contest_ids = contest_ids[: args.games]
    vendors = conf.get("vendor", [])
    if not vendors:
        print("no [[vendor]] entries in config.", file=sys.stderr)
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"canary_{stamp}.md"
    header = (
        f"# NCAA proxy canary -- {stamp}\n\n"
        f"{len(contest_ids)} MBB contests x {len(PAGES)} pages "
        f"(play_by_play + individual_stats). PASS = >=90% clean games.\n\n"
    )
    out_file.write_text(header, encoding="utf-8")
    print(header)

    summary: list[str] = []
    for vendor in vendors:
        name = vendor.get("name", "?")
        skip = _vendor_ready(vendor)
        if skip:
            note = f"### {name}  ({vendor.get('type', '?')}) -- SKIPPED: {skip}\n\n"
            out_file.write_text(out_file.read_text(encoding="utf-8") + note, encoding="utf-8")
            print(f"  {name}: SKIPPED ({skip})", flush=True)
            summary.append(f"{name}: skipped")
            continue
        print(f"  probing {name} ({vendor['type']}) ...", flush=True)
        try:
            rows = probe_vendor(vendor, contest_ids, args.delay)
            section = scorecard(vendor, rows, len(contest_ids))
        except Exception as exc:  # noqa: BLE001 - one bad vendor must not sink the run
            section = f"### {name}  ({vendor['type']}) -- ERROR building/running: {exc}\n\n"
        # Append incrementally so a later hang never loses earlier results.
        out_file.write_text(out_file.read_text(encoding="utf-8") + section + "\n", encoding="utf-8")
        first_line = section.splitlines()[0].replace("### ", "")
        print(f"  -> {first_line}", flush=True)
        summary.append(first_line)

    print("\n=== canary summary ===")
    for s in summary:
        print(f"  {s}")
    print(f"\nfull scorecard -> {out_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
