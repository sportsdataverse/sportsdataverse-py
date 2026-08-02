"""Round-robin ProxyBonanza proxy pool for the shared stats.nba.com / stats.wnba.com request budget.

Port of the R side's ``R/utils.R::get_proxy_ips`` / ``next_proxy``. Credentials
(``PROXY_ENDPOINT`` / ``PROXY_KEY`` / ``PROXY_PKG``) are read from the process
environment at call time -- never hardcoded, never logged in cleartext (use
:func:`redact` before putting a proxy URL in a log line or error message).
"""

import io
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Optional


def classify(status: Optional[int], text: str, error: Optional[str]) -> str:
    """Bucket a fetch outcome for health/observability.

    - ``transport_err``: the request raised (timeout / connection / proxy dead).
    - ``blocked``: HTTP 403 / 429 — the IP was rate-limited / blocked.
    - ``server_err``: HTTP 5xx — the request REACHED the stats host and the server
      erred (e.g. gamerotation 500s on pre-tracking games). The proxy is fine, so
      this is NOT a proxy-health signal and never quarantines.
    - ``notfound``: HTTP 400 / 404 — endpoint genuinely absent (expected for old
      seasons; benign).
    - ``blank``: HTTP 200 with an empty body (mild throttle signal).
    - ``ok``: HTTP 200 with a body.

    Only ``transport_err`` / ``blocked`` / ``blank`` are quarantine-worthy (the
    proxy or the IP is the problem). ``server_err`` and ``notfound`` never count
    against a proxy.
    """
    if error is not None:
        return "transport_err"
    if status == 200:
        return "ok" if (text or "").strip() else "blank"
    if status in (400, 404):
        return "notfound"
    if status is not None and 500 <= status < 600:
        return "server_err"
    return "blocked"


_QUARANTINE_CATS = ("transport_err", "blocked", "blank")
_ERROR_CATS = (
    "transport_err",
    "blocked",
    "blank",
    "server_err",
)  # non-benign, worth logging


class ProxyHealth:
    """Thread-safe per-proxy + global fetch-outcome registry.

    Drives three things: per-proxy quarantine (consecutive bad outcomes), the
    heartbeat's health summary, and the degradation WARN. Keyed by the redacted
    ``host:port`` so credentials never enter the counters or a log line.
    """

    _CAT_KEYS = ("ok", "blank", "notfound", "blocked", "server_err", "transport_err")

    def __init__(
        self,
        quarantine_fails: int = 5,
        quarantine_secs: float = 120.0,
        error_log: Optional[str] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._per: dict[str, dict[str, Any]] = {}
        self.quarantine_fails = quarantine_fails
        # Quarantine is a COOLDOWN, not a death sentence: a proxy that trips the
        # consecutive-fault threshold is benched for quarantine_secs then retried,
        # so a transient block storm can't sideline good IPs forever.
        self.quarantine_secs = quarantine_secs
        self.cat: dict[str, int] = {k: 0 for k in self._CAT_KEYS}
        # Aggregate outcomes by endpoint too, so "which requests error and why"
        # is answerable: endpoint -> {category: count}.
        self.endpoint_cat: dict[str, dict[str, int]] = {}
        # Optional append-only JSONL of every non-ok outcome (the queryable
        # by-endpoint/type/resource drill-down). One line: ts, endpoint,
        # resource (game_id/season), status, cat, latency, proxy.
        self._elog: Optional[io.TextIOBase] = None
        if error_log:
            try:
                self._elog = open(error_log, "a", encoding="utf-8")  # noqa: SIM115
            except OSError:
                self._elog = None

    def record(
        self,
        proxy_url: Optional[str],
        category: str,
        latency_ms: float = 0.0,
        endpoint: str = "?",
        resource: str = "",
        status: Optional[int] = None,
        params: Optional[dict[str, Any]] = None,
        session_id: Optional[int] = None,
        session_req: Optional[int] = None,
    ) -> None:
        key = redact(proxy_url) if proxy_url else "direct"
        with self._lock:
            self.cat[category] = self.cat.get(category, 0) + 1
            ec = self.endpoint_cat.setdefault(endpoint, {k: 0 for k in self._CAT_KEYS})
            ec[category] = ec.get(category, 0) + 1
            d = self._per.setdefault(
                key,
                {
                    "req": 0,
                    "consec_err": 0,
                    "quar_until": 0.0,
                    "lat_ms": 0.0,
                    "ok": 0,
                    "blank": 0,
                    "notfound": 0,
                    "blocked": 0,
                    "server_err": 0,
                    "transport_err": 0,
                },
            )
            d["req"] += 1
            d[category] = d.get(category, 0) + 1
            d["lat_ms"] = latency_ms
            if category in _QUARANTINE_CATS:
                d["consec_err"] += 1
                if d["consec_err"] >= self.quarantine_fails:  # (re-)bench for a cooldown
                    d["quar_until"] = time.monotonic() + self.quarantine_secs
            else:  # ok / notfound / server_err: the proxy delivered, so rehabilitate it
                d["consec_err"] = 0
                d["quar_until"] = 0.0
            if self._elog is not None and category in _ERROR_CATS:
                entry = {
                    "ts": round(time.time(), 3),
                    "endpoint": endpoint,
                    "resource": resource,
                    "status": status,
                    "cat": category,
                    "lat_ms": round(latency_ms, 1),
                    "proxy": key,
                }
                # Full request params make same-endpoint/different-variant errors
                # individually distinguishable (e.g. leaguedashteamshotlocations's
                # 28 param variants). session_id + session_req pinpoint WHICH
                # session and WHICH request-in-the-sequence failed (throttle-after-N).
                if params:
                    entry["params"] = params
                if session_id is not None:
                    entry["session_id"] = session_id
                if session_req is not None:
                    entry["session_req"] = session_req
                self._elog.write(json.dumps(entry) + "\n")
                self._elog.flush()

    def is_quarantined(self, proxy_url: Optional[str]) -> bool:
        key = redact(proxy_url) if proxy_url else "direct"
        with self._lock:
            d = self._per.get(key)
            return bool(d and d["quar_until"] > time.monotonic())

    def reset_quarantine(self) -> None:
        with self._lock:
            for d in self._per.values():
                d["consec_err"] = 0
                d["quar_until"] = 0.0

    def snapshot(self) -> dict:
        with self._lock:
            healthy = degraded = quar = 0
            worst = []
            now = time.monotonic()
            for k, d in self._per.items():
                if d["quar_until"] > now:
                    quar += 1
                    worst.append((k, d["consec_err"]))
                elif d["consec_err"] >= 2:
                    degraded += 1
                    worst.append((k, d["consec_err"]))
                else:
                    healthy += 1
            worst.sort(key=lambda x: -x[1])
            return {
                "cat": dict(self.cat),
                "healthy": healthy,
                "degraded": degraded,
                "quar": quar,
                "used": len(self._per),
                "worst": worst[:3],
            }

    def endpoint_summary(self, min_errors: int = 1) -> list[tuple]:
        """(endpoint, err_total, {cat: count}) for every endpoint with >=
        min_errors non-benign outcomes, worst first. Feeds the season/final
        breakdown and answers 'which requests error, and how'."""
        with self._lock:
            rows = []
            for ep, ec in self.endpoint_cat.items():
                errs = sum(ec.get(c, 0) for c in _ERROR_CATS)
                if errs >= min_errors:
                    rows.append((ep, errs, dict(ec)))
            rows.sort(key=lambda r: -r[1])
            return rows

    def top_error_endpoints(self, n: int = 3) -> str:
        """Compact 'gamerotation(5xx682) hustle(b88)' fragment for the heartbeat."""
        frag = []
        for ep, _errs, ec in self.endpoint_summary()[:n]:
            parts = []
            if ec.get("transport_err"):
                parts.append(f"t{ec['transport_err']}")
            if ec.get("blocked"):
                parts.append(f"b{ec['blocked']}")
            if ec.get("server_err"):
                parts.append(f"5xx{ec['server_err']}")
            if ec.get("blank"):
                parts.append(f"z{ec['blank']}")
            frag.append(f"{ep}({' '.join(parts)})")
        return " ".join(frag) or "none"

    def close(self) -> None:
        if self._elog is not None:
            try:
                self._elog.close()
            except OSError:
                pass


def load_proxies() -> list[dict[str, Any]]:
    """Fetch the configured ProxyBonanza proxy list.

    Reads ``PROXY_ENDPOINT`` / ``PROXY_KEY`` / ``PROXY_PKG`` from the process
    environment at call time. GETs ``{PROXY_ENDPOINT}/{PROXY_PKG}.json`` with
    an ``Authorization: {PROXY_KEY}`` header, mirroring the R
    ``get_proxy_ips()`` response shape (``data.login`` / ``data.password`` /
    ``data.ippacks[].ip`` / ``data.ippacks[].port_http``, broadcast into one
    dict per IP pack). Never raises: returns ``[]`` if any of the three env
    vars is unset, the request fails, or the payload doesn't match the
    expected shape.

    Returns:
        A list of dicts with ``ip`` / ``port`` / ``login`` / ``password``
        keys (consumable by :class:`RoundRobin`), or ``[]`` when
        unconfigured / unreachable.
    """
    endpoint = os.environ.get("PROXY_ENDPOINT")
    key = os.environ.get("PROXY_KEY")
    pkg = os.environ.get("PROXY_PKG")
    if not endpoint or not key or not pkg:
        return []
    if not endpoint.lower().startswith(("http://", "https://")):
        return []  # refuse file:// / ftp:// etc. -- keeps the S310 suppression honest

    url = f"{endpoint.rstrip('/')}/{pkg}.json"
    req = urllib.request.Request(url, headers={"Authorization": key})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:  # noqa: S310 - fixed https proxy-vendor endpoint
            payload = json.loads(resp.read().decode("utf-8"))
    except (OSError, json.JSONDecodeError, ValueError):
        return []

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return []
    ippacks = data.get("ippacks")
    if not isinstance(ippacks, list):
        return []
    login = data.get("login")
    password = data.get("password")
    return [
        {
            "ip": pack["ip"],
            "port": pack["port_http"],
            "login": login,
            "password": password,
        }
        for pack in ippacks
        if isinstance(pack, dict) and pack.get("ip") and pack.get("port_http")
    ]


class RoundRobin:
    """Random-start round-robin proxy rotation.

    Mirrors ``next_proxy()``: shuffles a fixed visiting order once (spreads
    load evenly across IPs instead of ``select_proxy()``'s sampling with
    replacement) and walks it forever.

    Args:
        proxies: List of proxy dicts (``ip`` / ``port`` / ``login`` /
            ``password``), e.g. from :func:`load_proxies`.
    """

    def __init__(self, proxies: list[dict[str, Any]], health: "Optional[ProxyHealth]" = None):
        self._proxies = list(proxies)
        self._order = list(range(len(self._proxies)))
        random.shuffle(self._order)
        self._pos = 0
        self._health = health
        self._lock = threading.Lock()  # 14 workers call next() concurrently

    def _url_at(self, idx: int) -> str:
        p = self._proxies[self._order[idx % len(self._order)]]
        return f"http://{p['login']}:{p['password']}@{p['ip']}:{p['port']}"

    def next(self) -> Optional[str]:
        """Return the next non-quarantined proxy URL, or ``None`` if the pool is empty.

        Skips proxies the health tracker has quarantined (too many consecutive
        timeouts / blocks). If every proxy is quarantined, clears the quarantine
        and hands one back anyway rather than stalling the sweep.
        """
        if not self._proxies:
            return None
        with self._lock:
            n = len(self._order)
            for _ in range(n):
                url = self._url_at(self._pos)
                self._pos += 1
                if self._health is None or not self._health.is_quarantined(url):
                    return url
            # whole pool quarantined — reset and fall back so work never stalls
            if self._health is not None:
                self._health.reset_quarantine()
            url = self._url_at(self._pos)
            self._pos += 1
            return url


def redact(url: str) -> str:
    """Strip ``login:password@`` userinfo from a proxy URL for safe logging.

    Args:
        url: A proxy URL, e.g. ``"http://user:pass@1.2.3.4:8000"``.

    Returns:
        The URL with credentials removed (``"scheme://host:port"``). Returns
        the input unchanged if there is no ``@`` to strip.
    """
    scheme_sep = url.find("://")
    if scheme_sep == -1 or "@" not in url:
        return url
    scheme = url[: scheme_sep + 3]
    rest = url[scheme_sep + 3 :]
    _, _, hostport = rest.rpartition("@")
    return f"{scheme}{hostport}"
