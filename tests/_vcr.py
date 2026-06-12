"""VCR-style record/replay harness for live-path integration tests.

Why this exists
---------------
Most parser / wrapper tests monkeypatch the provider wrappers with synthetic
frames -- fast and offline, but they never exercise the *real* call path
(URL construction -> :func:`sportsdataverse.dl_utils.download` retry loop ->
:func:`sportsdataverse.errors.no_espn_data` -> the parser). This harness closes
that gap: record a real API interaction **once** into a committed JSON
"cassette", then replay it offline forever. CI then runs the genuine code path
against recorded bytes with zero network.

Interception layer
-------------------
We patch ``requests.sessions.Session.request`` -- the single method that
``session.get`` (the shared pooled session in ``dl_utils``), the module-level
``requests.get`` / ``requests.post`` (the ``nfl_games`` Shield calls and the
direct-``requests`` fallbacks), and everything else ultimately funnel through.
One patch covers every HTTP exit in the package.

Secret hygiene (CRITICAL)
-------------------------
Cassettes are committed fixtures. Any API key carried in a request URL or query
param (e.g. The Odds API ``?apiKey=...``) MUST NOT be persisted. Every URL and
param dict is run through the scrubber before it touches disk, replacing
known-secret keys with ``"<redacted>"``. The *same* scrub is applied when
building the replay match key, so a redacted recording still matches a live
request that carries the real secret.

Modes
-----
* **replay** (default): ``Session.request`` is fully replaced; a cassette miss
  raises :class:`CassetteMiss` -- it never falls through to the network, so a
  replay run is hermetic by construction.
* **record** (``SDV_PY_RECORD=1``): the real ``Session.request`` runs, each
  interaction is captured, and the cassette is (re)written on context exit.
  Recording requires real network, so the calling test must also be
  live-gated (``SDV_PY_LIVE_TESTS=1``).

Usage::

    from tests._vcr import use_cassette

    def test_real_call_path():
        with use_cassette("espn_cfb_team_basic"):
            resp = download("https://site.api.espn.com/.../teams/194")
            assert resp.json()["team"]["displayName"] == "Ohio State Buckeyes"
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests

CASSETTE_DIR = Path(__file__).parent / "cassettes"

# Query-param / header keys whose VALUE is a credential. Matched case-insensitively.
# Extend this set rather than scattering ad-hoc redactions -- it is the single
# source of truth for "what must never reach a committed cassette".
_SECRET_KEYS = frozenset(
    {
        "apikey",
        "api_key",
        "key",
        "token",
        "access_token",
        "auth",
        "client_secret",
        "secret",
        "password",
    }
)
# URL-safe sentinel (no chars urlencode would escape) so a scrubbed query param
# reads as ``apiKey=REDACTED`` -- obvious to a reviewer grepping a cassette --
# rather than a cryptic ``%3C...%3E`` blob.
_REDACTED = "REDACTED"

#: True when ``SDV_PY_RECORD=1`` -- cassettes are (re)written from live calls.
RECORDING: bool = os.environ.get("SDV_PY_RECORD") == "1"


class CassetteMiss(LookupError):
    """Raised in replay mode when no recorded interaction matches a request."""


# ---------------------------------------------------------------------------
# Scrubbing + match-key construction (pure, unit-tested offline)
# ---------------------------------------------------------------------------


def _is_secret(key: str) -> bool:
    return key.lower() in _SECRET_KEYS


def _scrub_url(url: str) -> str:
    """Redact secret query params embedded in a URL.

    Applied both to the *request* URL and to the echoed ``response.url`` (which
    for a GET reflects the full query string, secrets included) before either is
    written to disk.
    """
    parts = urlsplit(url)
    if not parts.query:
        return url
    pairs = [(k, _REDACTED if _is_secret(k) else v) for k, v in parse_qsl(parts.query, keep_blank_values=True)]
    return urlunsplit(parts._replace(query=urlencode(pairs)))


def _key(method: str, url: str, params: Any) -> Tuple[str, str, str]:
    """Stable match key: ``(METHOD, base-url, scrubbed-sorted-params-json)``.

    The URL's own query string and the explicit ``params=`` kwarg are folded
    into one canonical param map (so ``url?a=1`` + ``params={b:2}`` matches
    ``url?a=1&b=2``), secrets are redacted, ``None`` values dropped, and the
    result sorted -- making the key invariant to param ordering and credential
    rotation.
    """
    parts = urlsplit(url)
    merged: Dict[str, str] = dict(parse_qsl(parts.query, keep_blank_values=True))
    if params:
        items = params.items() if isinstance(params, dict) else params
        for k, v in items:
            if v is not None:
                merged[str(k)] = str(v)
    scrubbed = {k: (_REDACTED if _is_secret(k) else v) for k, v in merged.items()}
    base = urlunsplit(parts._replace(query=""))
    return (method.upper(), base, json.dumps(dict(sorted(scrubbed.items())), sort_keys=True))


def _build_response(rec: Dict[str, Any]) -> requests.Response:
    """Synthesize a real ``requests.Response`` from a recorded interaction.

    A genuine ``Response`` (not a duck-typed shim) means the replayed object
    flows through ``no_espn_data`` / ``.raise_for_status`` / ``.json`` exactly
    as a live one would.
    """
    resp = requests.Response()
    resp.status_code = int(rec["status_code"])
    resp._content = rec["body"].encode("utf-8")  # type: ignore[attr-defined]
    resp.url = rec["url"]
    resp.encoding = "utf-8"
    resp.headers["Content-Type"] = rec.get("content_type", "application/json")
    return resp


# ---------------------------------------------------------------------------
# Cassette container
# ---------------------------------------------------------------------------


class _Cassette:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.interactions: List[Dict[str, Any]] = []
        self._index: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            self.interactions = data.get("interactions", [])
            for it in self.interactions:
                req = it["request"]
                self._index[(req["method"], req["url"], req["params"])] = it["response"]

    def find(self, method: str, url: str, params: Any) -> Optional[Dict[str, Any]]:
        return self._index.get(_key(method, url, params))

    def record(self, method: str, url: str, params: Any, response: requests.Response) -> None:
        k = _key(method, url, params)
        if k in self._index:  # first writer wins -> deterministic cassettes
            return
        rec_response = {
            "status_code": response.status_code,
            "url": _scrub_url(response.url),
            "body": response.text,
            "content_type": response.headers.get("Content-Type", "application/json"),
        }
        self.interactions.append({"request": {"method": k[0], "url": k[1], "params": k[2]}, "response": rec_response})
        self._index[k] = rec_response

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": 1, "interactions": self.interactions}
        self.path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8", newline="\n")


# ---------------------------------------------------------------------------
# Public context manager
# ---------------------------------------------------------------------------


@contextmanager
def use_cassette(name: Union[str, Path], *, record: Optional[bool] = None) -> Iterator[_Cassette]:
    """Patch ``Session.request`` to record or replay HTTP interactions.

    Args:
        name: Cassette stem (resolved to ``tests/cassettes/<name>.json``) or an
            explicit ``Path`` (used as-is -- handy for ``tmp_path`` in tests).
        record: Force record (``True``) / replay (``False``). Defaults to the
            ``SDV_PY_RECORD`` env var via :data:`RECORDING`.

    Yields:
        The active :class:`_Cassette` (mostly for assertions in tests).
    """
    path = name if isinstance(name, Path) else CASSETTE_DIR / f"{name}.json"
    recording = RECORDING if record is None else record
    cassette = _Cassette(path)

    # Force the generic response cache off so download() can't short-circuit
    # before Session.request -- replay must be driven purely by the cassette.
    from sportsdataverse import cache as _cache

    prev_mode = _cache.get_cache_mode()
    _cache.set_cache_mode("off")

    real_request = requests.sessions.Session.request

    if recording:

        def _patched(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
            resp = real_request(self, method, url, **kwargs)
            cassette.record(method, url, kwargs.get("params"), resp)
            return resp

    else:

        def _patched(self, method, url, **kwargs):  # type: ignore[no-untyped-def]
            rec = cassette.find(method, url, kwargs.get("params"))
            if rec is None:
                raise CassetteMiss(
                    f"No recorded interaction for {method.upper()} {url} "
                    f"params={kwargs.get('params')!r} in cassette {path.name!r}. "
                    f"Re-record with: SDV_PY_RECORD=1 SDV_PY_LIVE_TESTS=1 pytest <test>"
                )
            return _build_response(rec)

    requests.sessions.Session.request = _patched  # type: ignore[method-assign]
    try:
        yield cassette
    finally:
        requests.sessions.Session.request = real_request  # type: ignore[method-assign]
        _cache.set_cache_mode(prev_mode)
        if recording:
            cassette.save()
