"""ProxyBonanza-routed transport for stats.wnba.com (blocker fallback, T3.4 Phase 5).

Direct residential access to ``stats.wnba.com`` worked for the first ~1250 requests today
(2026-07-11) then started timing out (30s, 0 bytes) on every call, including the cheap single
bulk ``drafthistory`` call that had succeeded easily earlier -- a cumulative rate/IP throttle,
not a per-request fluke (retried 3x with no success). Per the ops rule for exactly this
situation, fall back to the ProxyBonanza pool (see ``dev/ncaa_proxy.py`` for the proven pattern
against a different host). Creds live in ``~/.Renviron`` / ``~/Documents/.Renviron``
(``PROXYBONANZA_API_KEY``/``PROXY_KEY`` + ``PROXY_PKG``), read AT CALL TIME here -- never an OS
env var, never hardcoded, never logged.

Unlike the NCAA client, this file doesn't need its own HTTP session -- the ``wnba_stats_*``
wrappers already accept a ``proxy_url=`` kwarg that threads through to
``nba_stats_runtime._get`` -> ``_curl_transport``, so this module only needs to hand back a
rotating ``login:password@ip:port`` string per call.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import curl_cffi

_RENVIRONS = [Path.home() / ".Renviron", Path.home() / "Documents" / ".Renviron"]


def _read_renviron() -> dict[str, str]:
    out: dict[str, str] = {}
    for p in _RENVIRONS:
        if not p.exists():
            continue
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                out.setdefault(k.strip(), v.strip().strip('"').strip("'").strip())
    return out


def load_proxy_pool() -> tuple[str, str, list[dict[str, Any]]]:
    c = _read_renviron()
    api_key = c.get("PROXYBONANZA_API_KEY") or c.get("PROXY_KEY")
    pkg = c.get("PROXY_PKG")
    if not api_key or not pkg:
        raise RuntimeError("ProxyBonanza creds absent from .Renviron (PROXYBONANZA_API_KEY/PROXY_KEY + PROXY_PKG)")
    r = curl_cffi.get(
        f"https://api.proxybonanza.com/v1/userpackages/{pkg}.json",
        headers={"Authorization": api_key},
        timeout=30,
        impersonate="chrome",
    )
    r.raise_for_status()
    d = r.json()["data"]
    return d["login"], d["password"], d["ippacks"]


class ProxyRotator:
    """Round-robins the ProxyBonanza pool, handing back one ``proxy_url`` string per call."""

    def __init__(self) -> None:
        self._login, self._pwd, self._pool = load_proxy_pool()
        self._i = 0
        print(f"  [proxy] pool loaded: {len(self._pool)} ips")

    def next_url(self) -> str:
        pk = self._pool[self._i % len(self._pool)]
        self._i += 1
        return f"http://{self._login}:{self._pwd}@{pk['ip']}:{pk['port_http']}"
