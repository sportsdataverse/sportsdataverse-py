"""Division I programs barred from the NCAA tournament, by season (men's and women's).

ESPN does not flag them, so the college season products read this bundled, source-cited list:
programs in the Division I reclassification period, and postseason bans (APR, NCAA infractions,
self-imposed). The rows, their sources and how to maintain them are in
``sportsdataverse/mbb/data/ncaa_tournament_ineligible.md``.

Example:
    Quick start::

        from sportsdataverse.mbb import mbb_tournament_ineligible
        barred = mbb_tournament_ineligible([2024], league="womens")
"""

from __future__ import annotations

import io
from functools import lru_cache
from importlib.resources import files
from typing import Literal, overload

import pandas as pd
import polars as pl

__all__ = ["mbb_tournament_ineligible"]

INELIGIBLE_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "league": pl.Utf8(),
    "team_id": pl.Utf8(),
    "team": pl.Utf8(),
    "reason": pl.Utf8(),
    "detail": pl.Utf8(),
    "source_url": pl.Utf8(),
    "confidence": pl.Utf8(),
}
REASONS = frozenset({"reclassifying", "apr_ban", "infractions_ban", "self_imposed_ban", "other"})
LEAGUES = ("mens", "womens")


@lru_cache(maxsize=1)
def _bundled() -> pl.DataFrame:
    raw = (files("sportsdataverse.mbb") / "data" / "ncaa_tournament_ineligible.csv").read_bytes()
    return pl.read_csv(io.BytesIO(raw), schema=INELIGIBLE_SCHEMA)


@overload
def mbb_tournament_ineligible(
    seasons: list[int] | None = None, *, league: str | None = None, return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def mbb_tournament_ineligible(
    seasons: list[int] | None = None, *, league: str | None = None, return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def mbb_tournament_ineligible(
    seasons: list[int] | None = None, *, league: str | None = None, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Division I basketball programs that could not play in the NCAA tournament, per season.

    A bundled, hand-curated list (2010-2027): programs reclassifying to Division I (four seasons
    from Division II until 2025, three since the NCAA's January 2025 rule; St. Thomas came from
    Division III), and APR, NCAA-infractions and self-imposed postseason bans. Every row cites
    its source. The college season products (bracketology, season sim, strength of schedule) use
    it by default to set ``eligible``.

    Args:
        seasons: Seasons to keep, as the end year (``2024`` is 2023-24). ``None`` keeps all.
        league: ``"mens"``, ``"womens"``, or ``None`` for both.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ineligible ``(season, league, team_id)``: ``season, league, team_id`` (ESPN
        team id, Utf8), ``team``, ``reason`` (``reclassifying``, ``apr_ban``,
        ``infractions_ban``, ``self_imposed_ban`` or ``other``), ``detail``, ``source_url`` and
        ``confidence`` (``high`` / ``medium``).

    Raises:
        ValueError: If ``league`` is not ``"mens"``, ``"womens"`` or ``None``.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_tournament_ineligible
            barred = mbb_tournament_ineligible([2023], league="mens")

        Pipeline next step (one line)::

            barred.filter(pl.col("reason") == "reclassifying").get_column("team").to_list()

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    if league is not None and league not in LEAGUES:
        raise ValueError(f"league must be 'mens', 'womens' or None, not {league!r}")
    out = _bundled()
    if seasons is not None:
        out = out.filter(pl.col("season").is_in(list(seasons)))
    if league is not None:
        out = out.filter(pl.col("league") == league)
    return out.to_pandas() if return_as_pandas else out


def _ineligible_team_ids(season: int, league: str) -> set[str]:
    """The ESPN team ids barred from one season's NCAA tournament (:func:`mbb_tournament_ineligible`)."""
    return set(mbb_tournament_ineligible([season], league=league).get_column("team_id").to_list())
