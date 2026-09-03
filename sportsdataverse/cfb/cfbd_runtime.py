"""Authenticated getter for the College Football Data API (``api.collegefootballdata.com``).

CFBD is a free-but-keyed service: every route needs an ``Authorization: Bearer``
header, and an unauthenticated request comes back ``401`` rather than a partial
payload. The generated ``cfbd`` wrappers import :func:`_get` from here instead of
the shared no-auth getter for exactly that reason.

Credential resolution happens **at call time**, never at import, and the key is
never logged:

1. an explicit ``headers=`` dict (the wrappers expose one) wins outright;
2. else ``CFBD_API_KEY`` from the process environment;
3. else ``CFBD_API_KEY`` read out of ``~/.Renviron`` / ``~/Documents/.Renviron``.

Step 3 matters on this machine specifically: the SportsDataverse keys live in
``.Renviron``, which **R reads at startup and Python does not**, so a key that is
plainly "set" for the R packages is invisible to `os.environ` here. Resolving it
lazily keeps `import sportsdataverse` working with no key present, and keeps the
key out of any traceback.

Example:
    Fetch one endpoint directly (needs ``CFBD_API_KEY``)::

        from sportsdataverse.cfb.cfbd_runtime import _get

        payload = _get("/teams", params={"year": 2024})
        print(len(payload))
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from sportsdataverse.dl_utils import download

__all__ = ["_get", "resolve_key", "CFBD_HOST"]

CFBD_HOST = "https://api.collegefootballdata.com"

_ENV_VAR = "CFBD_API_KEY"
_RENVIRON_LINE = re.compile(r'^\s*CFBD_API_KEY\s*=\s*"?([^"\s#]+)"?')


def _from_renviron() -> Optional[str]:
    """``CFBD_API_KEY`` out of ``.Renviron``, or ``None``.

    Returns the first match found; never raises on a missing or unreadable file,
    because an absent key is a normal state (a user without a CFBD subscription).
    """
    for candidate in (Path.home() / ".Renviron", Path.home() / "Documents" / ".Renviron"):
        try:
            if not candidate.is_file():
                continue
            for line in candidate.read_text(encoding="utf-8", errors="ignore").splitlines():
                match = _RENVIRON_LINE.match(line)
                if match:
                    return match.group(1)
        except OSError:
            continue
    return None


def resolve_key() -> str:
    """The CFBD API key, or a ``RuntimeError`` naming every place that was checked.

    Raises:
        RuntimeError: When no key is available. The message lists the env var and
            the ``.Renviron`` fallback so the fix is obvious; it never echoes a
            partial key.
    """
    key = os.environ.get(_ENV_VAR) or _from_renviron()
    if not key:
        raise RuntimeError(
            "no CFBD API key: set the CFBD_API_KEY environment variable, add it to "
            "~/.Renviron, or pass headers={'Authorization': 'Bearer <key>'}. "
            "Free keys: https://collegefootballdata.com/key"
        )
    return key


def _get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> Union[Dict[str, Any], List[Any]]:
    """GET one CFBD route and return its decoded JSON.

    Args:
        url: **Fully-qualified** CFBD URL, built by the generated wrapper (the
            flat-API contract every ``getter_module`` follows -- the wrapper owns
            the host, the runtime owns auth). A bare ``/teams`` path is accepted
            too and gets the host prefixed, so the module is usable by hand.
        params: Query parameters. ``None`` values are dropped, so an unset
            optional wrapper argument does not become ``?year=None``.
        headers: Complete header dict. When supplied it is used as-is and no key
            is resolved -- the escape hatch for a caller holding its own token.
        **kwargs: Forwarded to :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The decoded JSON: a ``list`` for the record endpoints (most of them), a
        ``dict`` for the few that return an object.

    Raises:
        RuntimeError: When no API key can be resolved.
    """
    if headers is None:
        headers = {"Authorization": f"Bearer {resolve_key()}", "Accept": "application/json"}
    target = url if url.startswith(("http://", "https://")) else f"{CFBD_HOST}{url}"
    query = {k: v for k, v in (params or {}).items() if v is not None}
    response = download(target, params=query or None, headers=headers, **kwargs)
    return response.json()
