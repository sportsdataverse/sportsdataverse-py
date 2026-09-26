"""NBA officiating data from official.nba.com: Last Two Minute reports and referee assignments.

Port of the scraping logic in atlhawksfanatic/L2M (MIT, (c) 2019 atlhawksfanatic).
official.nba.com is S3 behind Akamai Bot Manager: a browser User-Agent is required, and a
403 means two different things -- an S3 XML ``AccessDenied`` body is "no such report"
(``NoDataError``) while an Akamai HTML page is a blocked fetch (``AssetFetchError``).
"""

from __future__ import annotations

import requests

from sportsdataverse.dl_utils import download
from sportsdataverse.errors import AssetFetchError, NoDataError

__all__: list[str] = []

_OFFICIAL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
    "Referer": "https://official.nba.com/",
}
# 403 is NOT transient here: it is either "no object" or a WAF block, both definitive.
_RETRY_NO_403 = frozenset({408, 429, 500, 502, 503, 504})


def _official_get(url: str, *, proxy: dict | None = None) -> requests.Response:
    """Fetch a URL from ``official.nba.com`` with the browser UA it requires, classifying 403s.

    Args:
        url: Full ``official.nba.com`` URL to fetch.
        proxy: Optional proxy dict passed through to
            :func:`sportsdataverse.dl_utils.download`.

    Returns:
        The successful (200) ``requests.Response``.

    Raises:
        NoDataError: The response is a 403 with an S3 ``AccessDenied`` XML body --
            official.nba.com's way of saying no report exists for the request.
        AssetFetchError: The response is any other non-200 status, including an
            Akamai WAF block (403 HTML) or a 5xx that outlived the retry budget.

    Example:
        Fetch a Last Two Minute report::

            from sportsdataverse.nba.nba_officiating import _official_get
            resp = _official_get("https://official.nba.com/l2m/json/0042500405.json")
            payload = resp.json()
    """
    resp = download(url, headers=_OFFICIAL_HEADERS, proxy=proxy, retry_statuses=_RETRY_NO_403)
    if resp.status_code == 200:
        return resp
    if resp.status_code == 403 and "<Code>AccessDenied</Code>" in resp.text[:500]:
        raise NoDataError(f"official.nba.com has no object at {url}")
    ctype = resp.headers.get("Content-Type", "")
    raise AssetFetchError(f"official.nba.com fetch failed ({resp.status_code}, {ctype!r}) for {url}")
