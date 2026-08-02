"""Thread-local sticky-session curl_cffi transport for stats.nba.com / stats.wnba.com sweeps.

More prudent than rotating the proxy on every request: each worker thread keeps a
persistent ``curl_cffi.Session`` (keep-alive) bound to ONE proxy, so the TLS/JA3
handshake is paid once and reused across a burst of requests — faster, and a
coherent per-IP session reads as more legitimate than per-request IP hopping. A
rotation policy bounds per-IP exposure and self-heals:

    * rotate after SESSION_MAX_REQUESTS requests (default 120),
    * or SESSION_MAX_SECS seconds (default 300),
    * or immediately on a proxy fault (timeout / connection / 403 / 429),

and it never binds a quarantined proxy (it draws from the same quarantine-aware
``RoundRobin``). Injected via the wrapper ``transport=`` hook, so sdv-py is
unchanged. Every fetch is recorded to ``ProxyHealth`` with its endpoint + full
params + status + latency + proxy + session id + session request number, so
same-endpoint/different-param requests are individually distinguishable in the
error log.

Transient server-side 500s (the stats hosts intermittently 500 an individual
historical game/endpoint even though the data exists) are retried in-session on
the SAME proxy up to SESSION_SERVER_ERR_RETRIES (default 2) — the IP is healthy,
so rebinding would be wasteful. Timeouts / blocks are NOT retried here: they stay
single-shot and rotate, preserving the fast-on-faults contract.
"""

import itertools
import os
import threading
import time
from typing import Optional

from .proxy import ProxyHealth, RoundRobin, classify

_ROTATE_ON = ("transport_err", "blocked")  # proxy/IP faults -> rebind the session


class SessionTransport:
    def __init__(
        self,
        rr: RoundRobin,
        health: ProxyHealth,
        max_requests: int = 120,
        max_secs: float = 300.0,
    ) -> None:
        self.rr = rr
        self.health = health
        self.max_requests = int(os.environ.get("SESSION_MAX_REQUESTS", str(max_requests)))
        self.max_secs = float(os.environ.get("SESSION_MAX_SECS", str(max_secs)))
        self.timeout = float(os.environ.get("SDV_PY_NBA_STATS_TIMEOUT", "30"))
        # the stats hosts 500 intermittently on individual historical games even
        # though the data exists (measured: the same game/endpoint recovers on an
        # immediate re-request). Retry the CHEAP server_err class in-session;
        # timeouts/blocks stay single-shot (the fast-on-faults contract).
        self.server_err_retries = int(os.environ.get("SESSION_SERVER_ERR_RETRIES", "2"))
        self._tls = threading.local()
        self._ids = itertools.count(1)
        self._id_lock = threading.Lock()

    def _next_id(self) -> int:
        with self._id_lock:
            return next(self._ids)

    def _bind(self, st: threading.local) -> None:
        """(Re)bind this worker to a fresh Session on a new (non-quarantined) proxy."""
        from curl_cffi import requests as creq

        old = getattr(st, "sess", None)
        if old is not None:
            try:
                old.close()
            except Exception:  # noqa: BLE001 - a close failure must not stall the sweep
                pass
        st.proxy = self.rr.next()
        st.sess = creq.Session(impersonate="chrome")  # one JA3 handshake, then reuse
        if st.proxy:
            st.sess.proxies = {"http": st.proxy, "https": st.proxy}
        st.n = 0
        st.t0 = time.monotonic()
        st.sid = self._next_id()

    def __call__(self, url: str, params: dict, headers: dict, proxy_url: Optional[str]) -> tuple:
        # ``proxy_url`` is the shared wrapper-transport hook signature; this
        # transport OWNS proxy selection (round-robin + quarantine) and
        # deliberately ignores an explicit proxy.
        st = self._tls
        if (
            getattr(st, "sess", None) is None
            or st.n >= self.max_requests
            or (time.monotonic() - st.t0) >= self.max_secs
        ):
            self._bind(st)

        endpoint = url.rsplit("/stats/", 1)[-1].split("?")[0] if "/stats/" in url else url
        resource = str((params or {}).get("GameID") or (params or {}).get("Season") or "")
        st_req = st.n + 1  # ordinal of this request within the current session
        t0 = time.monotonic()
        try:
            r = st.sess.get(url, params=params, headers=headers, timeout=self.timeout)
            status, text = r.status_code, r.text
        except Exception:
            lat = (time.monotonic() - t0) * 1000
            self.health.record(
                st.proxy,
                "transport_err",
                lat,
                endpoint=endpoint,
                resource=resource,
                status=None,
                params=params,
                session_id=st.sid,
                session_req=st_req,
            )
            try:
                self._bind(st)  # a dead/slow proxy: drop the session and move on
            except Exception:  # noqa: BLE001 - must not mask the original transport error
                st.sess = None  # rebind lazily on the next call instead
            raise  # preserve the "timeout propagates" contract for the miss count

        st.n += 1
        lat = (time.monotonic() - t0) * 1000
        cat = classify(status, text, None)
        # Recover transient server-side 500s on the SAME proxy (the IP is fine, the
        # server erred). Only the final outcome is recorded, so a recovered 500 logs
        # nothing and a persistent one logs a single server_err.
        tries = 0
        while cat == "server_err" and tries < self.server_err_retries:
            tries += 1
            st.n += 1
            st_req = st.n  # the retry's ordinal -- keep throttle-after-N analysis honest
            t0 = time.monotonic()
            try:
                r = st.sess.get(url, params=params, headers=headers, timeout=self.timeout)
                status, text = r.status_code, r.text
            except Exception:
                lat = (time.monotonic() - t0) * 1000
                self.health.record(
                    st.proxy,
                    "transport_err",
                    lat,
                    endpoint=endpoint,
                    resource=resource,
                    status=None,
                    params=params,
                    session_id=st.sid,
                    session_req=st_req,
                )
                try:
                    self._bind(st)
                except Exception:  # noqa: BLE001 - must not mask the original transport error
                    st.sess = None
                raise
            lat = (time.monotonic() - t0) * 1000
            cat = classify(status, text, None)
        self.health.record(
            st.proxy,
            cat,
            lat,
            endpoint=endpoint,
            resource=resource,
            status=status,
            params=params,
            session_id=st.sid,
            session_req=st_req,
        )
        if cat in _ROTATE_ON:  # the IP was throttled/blocked -> rebind to a fresh one
            self._bind(st)
        return status, text
