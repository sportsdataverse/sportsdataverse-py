"""WNBA officiating data from official.nba.com: shim over the unified NBA/WNBA endpoint."""

from __future__ import annotations

import datetime as _dt
from typing import Any

from sportsdataverse.nba.nba_officiating import nba_referee_assignments

__all__ = ["wnba_referee_assignments"]


def wnba_referee_assignments(
    date: str | _dt.date, *, raw: bool = False, return_as_pandas: bool = False, proxy: dict | None = None
) -> dict[str, Any]:
    """Fetch and parse WNBA referee assignments for a given date from official.nba.com.

    Retrieves the referee crew assignments and replay center officials for all WNBA
    games on a given date. The ``crew_position`` column (1–4) represents the feed's
    slot order; slot 1 is inferred to be the crew chief. The ``season`` column is
    the WNBA single-year season (feed year converted as-is). This is a thin shim
    over :func:`sportsdataverse.nba.nba_officiating.nba_referee_assignments` that
    sets ``league="wnba"``.

    Args:
        date: The date to fetch assignments for (str in "YYYY-MM-DD" format or datetime.date).
        raw: If True, return the raw JSON payload (dict) with all three leagues instead of parsed DataFrames.
        return_as_pandas: If True, return pandas DataFrames instead of polars.
        proxy: Optional proxy dict passed through to the HTTP layer.

    Returns:
        A dict with keys ``"officials"`` and ``"replay_center"`` mapping to DataFrames.
        If ``raw=True``, returns the full three-league JSON payload instead.

    Raises:
        AssetFetchError: The fetch failed (network error, rate limit, or Akamai WAF block).

    Note:
        A date with no WNBA games is not an error -- the endpoint always returns a
        200 with an empty ``rows`` list for the ``wnba`` block, so both frames come
        back zero-row rather than raising ``NoDataError``.

    Example:
        Fetch WNBA referee assignments for a date::

            from sportsdataverse.wnba.wnba_officiating import wnba_referee_assignments
            result = wnba_referee_assignments("2026-06-13")
            officials = result["officials"]
            print(f"Found {officials.height} official slots")

        See Also:
            * `wehoop`_ -- R package for WNBA data access and visualization

            .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return nba_referee_assignments(date, league="wnba", raw=raw, return_as_pandas=return_as_pandas, proxy=proxy)
