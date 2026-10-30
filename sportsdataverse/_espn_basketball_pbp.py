"""Pieces shared by the ESPN basketball pbp producers (nba / wnba / mbb / wbb)."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
import polars as pl

# ESPN play types for a timeout a team called (NCAA 578/579, NBA/WNBA 15/16/17/283).
# Official / TV timeouts (580, 581, 19) belong to no team and are left out. The
# timeouts map built from these holds timeouts CALLED as ESPN logs them, not
# timeouts CHARGED: a coach's challenge outcome is not applied, because ESPN logs
# the challenge's own timeout too inconsistently to correct for (a team timeout
# precedes 90% of charged and 56% of retained NBA 2020-26 challenges, 84% / 22% in
# the WNBA 2023-26, about 5% in NCAA 2025-26). Challenge plays are not timeouts.
TEAM_TIMEOUT_TYPES = ["RegularTimeOut", "ShortTimeOut", "Full Timeout", "Short Timeout", "No Timeout", "Reset Timeout"]

# A free-throw play type: NBA / WNBA "Free Throw 1 of 2", MBB / WBB "MadeFreeThrow".
FREE_THROW = r"(?i)free ?throw"


def pickcenter_odds(
    pickcenter: Any, default_over_under: float | None, default_spread: float | None = 2.5
) -> dict[str, Any]:
    """Spread, over/under and home favorite from an ESPN summary ``pickcenter`` array.

    Providers are read in ``str(provider.id)`` order, which puts teamrankings
    ("1002") ahead of consensus ("1004") and of Caesars ("45"). That order is kept
    on purpose: where teamrankings and consensus disagree, teamrankings matches the
    winner more often (MBB 23 of 37 games, NBA 39 of 60), and an integer sort would
    move whole seasons (MBB 2021-22) to Caesars' line. The spread and the home
    favorite come from the same row, the first with a spread (a record-only
    teamrankings row has none, and its favorite flag is False for both teams).
    The over/under is the first non-null one. One provider is enough; with no
    spread anywhere the defaults (``default_spread``, home favored, unavailable) stand.

    ``default_spread=None`` is hoopR's behaviour instead: a game with no line gets a
    null spread, over/under (the provider's own, if it has one) and home favorite,
    never an invented line. MBB uses it.
    """
    pc = pd.json_normalize(pickcenter or [])
    if "spread" not in pc.columns or not pc["spread"].notnull().any():
        if default_spread is None:
            ou = pc["overUnder"].dropna() if "overUnder" in pc.columns else pd.Series(dtype=float)
            over_under = float(ou.iloc[0]) if len(ou) else default_over_under
            return {"gameSpread": None, "overUnder": over_under, "homeFavorite": None, "gameSpreadAvailable": False}
        return {
            "gameSpread": default_spread,
            "overUnder": default_over_under,
            "homeFavorite": True,
            "gameSpreadAvailable": False,
        }
    if "provider.id" in pc.columns:
        pc = pc.sort_values(by="provider.id", key=lambda ids: ids.astype(str), kind="stable")
    row = pc[pc["spread"].notnull()].iloc[0]
    spread = float(row["spread"])
    # ESPN's spread is the home team's line, so a negative spread is a home favorite
    favorite = row.get("homeTeamOdds.favorite")
    home_favorite = spread < 0 if spread != 0 else (True if pd.isna(favorite) else bool(favorite))
    over_unders = pc["overUnder"].dropna() if "overUnder" in pc.columns else pd.Series(dtype=float)
    return {
        "gameSpread": spread,
        "overUnder": float(over_unders.iloc[0]) if len(over_unders) else default_over_under,
        "homeFavorite": home_favorite,
        "gameSpreadAvailable": True,
    }


def team_timeout_called(columns: list[str], init: dict[str, Any], side: str) -> pl.Expr:
    """True on a team-timeout play the ``side`` ("home" / "away") team called.

    The play's own ``team.id`` decides. Only a play without one falls back to the
    team's abbreviation / location / mascot appearing as a whole word in the text
    (a bare substring test credits "Memphis" to PHI and "timeout" to ME).
    """
    team_id = pl.col("team.id").cast(pl.Int64, strict=False) if "team.id" in columns else pl.lit(None, dtype=pl.Int64)
    names = {str(init[f"{side}Team{part}"]) for part in ("Abbrev", "Name", "Mascot", "NameAlt")} - {"", "None"}
    if names:
        alternation = "|".join(re.escape(n) for n in sorted(names))
        name_match = pl.col("text").str.contains(rf"(?i)(?:^|\W)(?:{alternation})(?:\W|$)")
    else:
        name_match = pl.lit(False)
    called = pl.col("type.text").is_in(TEAM_TIMEOUT_TYPES) & pl.coalesce(team_id == init[f"{side}TeamId"], name_match)
    return called.fill_null(False)


def period_end_seconds(period_scope: str, scopes: tuple[str, ...]) -> list[pl.Expr]:
    """``end.<scope>_seconds_remaining`` with the period boundary on the row that ends the period.

    ``end.*`` is the next row's ``start.*``. The last row of a period (by sequence: the
    next row's ``period.number`` differs) has its next row in a period whose clock
    starts over, so it ends where its own period ends, at 0:00 on the period clock:
    ``start.<scope>`` less the ``start.<period_scope>`` seconds the period still had.
    Every other row, a period's first included, ends where the next row starts. The
    game's last row has no next row and stays null.

    The boundary used to be filled on a period's FIRST row instead, setting its end to
    the full period length, so a first play logged after the opening second ended later
    than it started (WNBA 400927398 Q2 at 9:41: start 1781, end 1800).

    Args:
        period_scope: The scope counting down one period ("period" for MBB, "quarter"
            for the others, where it is the period clock in a halves era too).
        scopes: The wider scopes ("half", "game") filled the same way.

    Returns:
        One expression per scope, ``period_scope`` first, each aliased to its
        ``end.<scope>_seconds_remaining`` column.
    """
    ends_period = pl.col("period.number") != pl.col("period.number").shift(-1)
    left_in_period = pl.col(f"start.{period_scope}_seconds_remaining")
    return [
        pl.when(ends_period)
        .then(pl.col(f"start.{s}_seconds_remaining") - left_in_period)
        .otherwise(pl.col(f"end.{s}_seconds_remaining"))
        .alias(f"end.{s}_seconds_remaining")
        for s in (period_scope, *scopes)
    ]


def hoopr_coordinates(df: pl.DataFrame, home_team_id: int | None) -> pl.DataFrame:
    """ESPN shot coordinates in the hoopR / wehoop convention, the raw pair kept as ``coordinate.{x,y}.raw``.

    Port of the ``helper_espn_<lg>_pbp`` block in hoopR (``espn_nba_data.R``, ``espn_mbb_data.R``)
    and wehoop (``espn_wnba_data.R``, ``espn_wbb_data.R``), the convention the published
    basketball pbp files carry. ESPN reports feet with the hoop at ``(25, 0)``. A free throw is
    pinned to ``(25, 13.75)``, the line 13.75 feet from the hoop: in the raw pair only on R's
    case-sensitive ``"Free Throw"`` match, in the transformed pair on any free throw (below). Then
    ``coordinate.x = y - 41.75`` and ``coordinate.y = x - 25``, both negated on the home team's
    plays, so the two teams shoot at opposite ends of one full court. A play with no ``team.id``
    is not flipped (R's ``case_when`` reads ``NA == id`` as false).

    One deliberate divergence from R: a free throw is any ``type.text`` matching
    ``(?i)free ?throw``. From 2014 through 2026 ESPN types every MBB / WBB free throw, made or
    missed, "MadeFreeThrow"; R's case-sensitive ``str_detect(., "Free Throw")`` never matches
    it, so those published free throws keep ESPN's coordinates (the earlier "Free Throw 1 of 1"
    types were pinned). Here they sit on the line like NBA / WNBA's (owner ruling R844-3) in the
    transformed pair only: their raw pair keeps ESPN's values, as R's and the published files' do.

    ESPN marks a play it did not locate with a large negative sentinel (about -2.1e8 in either
    raw column). The transform carries it through, so ``coordinate.x`` / ``coordinate.y`` read
    about +-2.1e8 on those plays; filter on the raw pair, e.g. ``coordinate_x_raw >
    nba_game_page.COORD_SENTINEL``, before plotting.

    The raw pair takes the original columns' places and the transformed pair is appended,
    R's column order. A frame that already carries ``coordinate.x.raw`` (a processed payload
    read back by the release converters) is transformed again from its raw pair, so the
    function is idempotent. Frames without both coordinate columns pass through unchanged.

    Args:
        df: Plays with ESPN's dotted keys (``coordinate.x``, ``coordinate.y``, ``type.text``,
            ``team.id``).
        home_team_id: The home team's ESPN id.

    Returns:
        ``df`` with ``coordinate.x`` / ``coordinate.y`` transformed (Float64, full-court frame,
        home plays negated) and ``coordinate.x.raw`` / ``coordinate.y.raw`` holding ESPN's
        values with R's free throws (``"Free Throw"``, case-sensitive) pinned to ``(25, 13.75)``
        (Float64).
    """
    if "coordinate.x" not in df.columns or "coordinate.y" not in df.columns:
        return df
    if "coordinate.x.raw" in df.columns and "coordinate.y.raw" in df.columns:
        df = df.drop("coordinate.x", "coordinate.y").rename(
            {"coordinate.x.raw": "coordinate.x", "coordinate.y.raw": "coordinate.y"}
        )
    ft = pl.col("type.text").str.contains(FREE_THROW) if "type.text" in df.columns else pl.lit(False)
    # R844-3 moves only the transformed pair: a free throw hoopR's case-sensitive "Free Throw" misses
    # ("MadeFreeThrow") keeps ESPN's values in the raw pair, as the published files do
    keep_espn = (
        (ft & ~pl.col("type.text").str.contains("Free Throw", literal=True)).fill_null(False)
        if "type.text" in df.columns
        else pl.lit(False)
    )
    df = df.with_columns(
        pl.when(ft).then(pl.lit(25.0)).otherwise(pl.col("coordinate.x").cast(pl.Float64)).alias("coordinate.x"),
        pl.when(ft).then(pl.lit(13.75)).otherwise(pl.col("coordinate.y").cast(pl.Float64)).alias("coordinate.y"),
        pl.col("coordinate.x").cast(pl.Float64).alias("_espn_x"),
        pl.col("coordinate.y").cast(pl.Float64).alias("_espn_y"),
    )
    team = pl.col("team.id").cast(pl.Int64, strict=False) if "team.id" in df.columns else pl.lit(None, dtype=pl.Int64)
    is_home = pl.lit(False) if home_team_id is None else (team == home_team_id).fill_null(False)
    df = df.with_columns(
        pl.when(is_home)
        .then(-1 * (pl.col("coordinate.y") - 41.75))
        .otherwise(pl.col("coordinate.y") - 41.75)
        .alias("coordinate_x_transformed"),
        pl.when(is_home)
        .then(-1 * (pl.col("coordinate.x") - 25))
        .otherwise(pl.col("coordinate.x") - 25)
        .alias("coordinate_y_transformed"),
    ).rename(
        {
            "coordinate.x": "coordinate.x.raw",
            "coordinate.y": "coordinate.y.raw",
            "coordinate_x_transformed": "coordinate.x",
            "coordinate_y_transformed": "coordinate.y",
        }
    )
    return df.with_columns(
        pl.when(keep_espn).then(pl.col("_espn_x")).otherwise(pl.col("coordinate.x.raw")).alias("coordinate.x.raw"),
        pl.when(keep_espn).then(pl.col("_espn_y")).otherwise(pl.col("coordinate.y.raw")).alias("coordinate.y.raw"),
    ).drop("_espn_x", "_espn_y")
