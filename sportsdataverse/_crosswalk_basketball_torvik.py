"""Checks on caller-supplied Barttorvik frames before a crosswalk build uses them."""

from __future__ import annotations

import polars as pl


def check_bart_games(bart_games: pl.DataFrame, season: int) -> pl.DataFrame:
    """Validate a caller-supplied Torvik super schedule before a schedule build uses it.

    A snapshot read back from CSV loses the ``Date`` dtype, and one from another
    season would join against the wrong team crosswalk without an error, so both
    are rejected here instead of producing a wrong crosswalk.

    Args:
        bart_games: A :func:`bart_super_sked` frame (``muid``, ``game_date``,
            ``team1``, ``team2``, ``winner``, ``year``).
        season: The season the crosswalk is being built for.

    Returns:
        ``bart_games`` unchanged.

    Raises:
        ValueError: A column is missing, ``game_date`` is not ``pl.Date``, or a
            row's ``year`` is not ``season``.

    Example:
        Quick start::

            from sportsdataverse._crosswalk_basketball_sources import bart_super_sked
            from sportsdataverse._crosswalk_basketball_torvik import check_bart_games
            games = check_bart_games(bart_super_sked("mbb", 2026), 2026)
    """
    missing = [c for c in ("muid", "game_date", "team1", "team2", "winner", "year") if c not in bart_games.columns]
    if missing:
        raise ValueError(f"bart_games is missing {missing}; pass the bart_super_sked() frame")
    if bart_games.schema["game_date"] != pl.Date:
        raise ValueError(f"bart_games.game_date is {bart_games.schema['game_date']}, expected Date")
    years = sorted(set(bart_games["year"].cast(pl.Int64, strict=False).to_list()) - {season})
    if years:
        raise ValueError(f"bart_games holds season(s) {years}, not {season}; the snapshot is stale")
    return bart_games
