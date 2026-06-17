"""Hand-written NHL api-web wrappers the codegen URL-builder can't express.

These live alongside the generated :mod:`sportsdataverse.nhl.nhl_api_web`
(``tools/codegen/endpoints/nhl_api_web.yaml``). Functions here use request
shapes the single-URL-builder template can't represent faithfully -- currently
just ``nhl_scoreboard`` (three mutually-exclusive URL forms). Listed in
``tests/codegen/test_parity_native.py::_IRREGULAR``.
"""

from __future__ import annotations

from typing import Dict, Optional

from sportsdataverse.dl_utils import download
from sportsdataverse.nhl.nhl_api_web_parsers import parse_nhl_web_scoreboard

_API_WEB_BASE = "https://api-web.nhle.com"

__all__ = ["nhl_scoreboard"]


def nhl_scoreboard(
    date: Optional[str] = None,
    team: Optional[str] = None,
    *,
    return_parsed: bool = True,
    return_as_pandas: bool = False,
    **kwargs,
) -> Dict:
    """In-game scoreboard payload (renamed from ``nhl_web_scoreboard``).

    Picks among three mutually-exclusive NHL api-web forms (kept hand-written
    because the URL-builder codegen can't represent the 3-way branch):

    * ``GET /v1/scoreboard/{team}/now`` -- team-scoped now (when ``team`` set),
    * ``GET /v1/scoreboard/{date}`` -- league-wide on a date,
    * ``GET /v1/scoreboard/now`` -- league-wide now (both args None).

    Args:
        date: ``YYYY-MM-DD``; ``None`` -> ``/now``. Mutually exclusive with ``team``.
        team: 3-letter abbreviation; takes precedence over ``date``.
        return_parsed: dispatch the raw payload through ``parse_nhl_web_scoreboard``.
        return_as_pandas: with ``return_parsed``, return pandas instead of polars.

    Returns:
        A polars/pandas DataFrame by default; the raw JSON ``Dict`` when
        ``return_parsed=False``.

    Example:
        Quick start::

            nhl_scoreboard(date="2024-03-01")
    """
    if team is not None:
        path = f"/v1/scoreboard/{team}/now"
    else:
        suffix = "now" if date is None else date
        path = f"/v1/scoreboard/{suffix}"
    resp = download(url=f"{_API_WEB_BASE}{path}", **kwargs)
    try:
        raw: Dict = {} if resp is None else resp.json()
    except Exception:
        raw = {}
    if return_parsed:
        return parse_nhl_web_scoreboard(raw, return_as_pandas=return_as_pandas)
    return raw
