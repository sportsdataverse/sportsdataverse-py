"""NFL injury/availability model: season availability rates from snap counts and
an empirical-Bayes availability projection (compute-on-demand, no artifacts).

Availability is a standalone [0, 1] output — it is combined with skill
projections only at composition time via :func:`compose_counting_projection`
(the spine's skill-vs-availability separation rule).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

from sportsdataverse.nfl.nfl_loaders import load_nfl_ff_playerids, load_nfl_rosters, load_nfl_snap_counts
from sportsdataverse.nfl.nfl_projection_constants import _season_rosters, as_of_season_split, get_position_constants

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

# Fitted by dev/nfl_projection/fit_availability.py (2026-07-08, fold-only
# revision after the oracle-gate review): EB_PRIOR_SEASONS selected by
# leave-one-fold-out games MAE on as-of folds (targets 2022 + 2023, snap-count
# history only, "recent regulars" population = players with >= 8 available
# games in the most recent visible season): k grid 0.1-1.5, LOFO MAE monotone
# in k, minimum at k=0.1 (3.7030). AVAIL_RECAL = (a, b) linear recalibration of
# the raw EB rate, numpy.polyfit pooled over both folds at the selected k.
# The 2024 holdout was NOT touched during selection; single out-of-sample 2024
# evaluation: games MAE 3.5436, max decile calibration gap 0.0494 (gate 0.05).
# See the POSITION_CONSTANTS comment for the fitted base rates + the QB/RB
# base-order finding (crosswalk verified; QB availability folds benching churn).
EB_PRIOR_SEASONS: float = 0.1
AVAIL_RECAL: tuple = (0.2040, 0.6821)

_SEASON_SCHEMA: dict = {
    "player_id": pl.Utf8,
    "season": pl.Int64,
    "position": pl.Utf8,
    "age": pl.Float64,
    "games_available": pl.Int64,
    "availability_rate": pl.Float64,
    "injury_weeks": pl.Float64,
}


#: share of regular-season skill-position snap rows (QB/RB/WR/TE with offense snaps) the PFR -> gsis bridge must
#: place in every season; 2020-2023 match at 0.9972-0.9986 by season (lowest season-position cell 0.991, 2023 TE),
#: so a lower rate means the crosswalk broke
SNAP_BRIDGE_MIN_MATCH = 0.98


def _check_bridge_rate(reg: pl.DataFrame, pfr_ids: pl.Series) -> None:
    """Raise when a season's skill-position snap rows find a gsis id less often than ``SNAP_BRIDGE_MIN_MATCH``."""
    skill = reg.filter(pl.col("position").is_in(["QB", "RB", "WR", "TE"]) & (pl.col("offense_snaps") > 0))
    if skill.height == 0:
        return
    rates = skill.group_by("season").agg(pl.col("pfr_player_id").is_in(pfr_ids.implode()).mean().alias("rate"))
    worst = rates.sort("rate").row(0, named=True)
    if worst["rate"] < SNAP_BRIDGE_MIN_MATCH:
        raise ValueError(
            f"only {worst['rate']:.1%} of {worst['season']} skill-position snap rows map to a gsis id "
            f"(floor {SNAP_BRIDGE_MIN_MATCH:.0%}): the PFR -> gsis crosswalk is broken"
        )


def _snaps_by_gsis(snaps: pl.DataFrame, player_ids: pl.DataFrame) -> pl.DataFrame:
    """Regular-season snap counts keyed by gsis ``player_id``, bridged from PFR ids as the fitted corpus was.

    ``tests/fixtures/nfl_projection/snap_counts_2020_2023.parquet`` (the sample ``EB_PRIOR_SEASONS``,
    ``AVAIL_RECAL`` and the position ``base_availability`` were fit on) is ``load_nfl_snap_counts`` regular-season
    rows inner-joined to ``load_nfl_ff_playerids()`` on ``pfr_player_id == pfr_id``. Unlike that plain join, a
    mapping that is ambiguous either way (one PFR id to two gsis ids, or two PFR ids to one gsis id) is dropped
    rather than crediting or doubling a player; the 2026-11-26 crosswalk has none over the 2020-2023 snap ids.

    Raises:
        ValueError: On non-empty snaps with neither ``player_id`` nor ``pfr_player_id``, or when a season's
            skill-position rows find a gsis id less often than ``SNAP_BRIDGE_MIN_MATCH``.
        TypeError: When ``pfr_player_id`` / ``pfr_id`` are not both text.
    """
    if snaps.height == 0:
        return snaps
    reg = snaps.filter(pl.col("game_type") == "REG") if "game_type" in snaps.columns else snaps
    if "player_id" in reg.columns:
        return reg
    if "pfr_player_id" not in reg.columns:
        raise ValueError(f"snap counts carry neither player_id nor pfr_player_id: {snaps.columns}")
    if not (reg.schema["pfr_player_id"] == player_ids.schema["pfr_id"] == pl.Utf8):
        raise TypeError(
            f"pfr_player_id is {reg.schema['pfr_player_id']} and pfr_id is {player_ids.schema['pfr_id']}; both must be "
            "String (cast the raw ids at the loader, never a float)"
        )
    bridge = (
        player_ids.select("pfr_id", pl.col("gsis_id").cast(pl.Utf8))
        .drop_nulls()
        .unique()
        .filter((pl.len().over("pfr_id") == 1) & (pl.len().over("gsis_id") == 1))
    )
    _check_bridge_rate(reg, bridge["pfr_id"])
    return (
        reg.join(bridge, left_on="pfr_player_id", right_on="pfr_id", how="inner")
        .rename({"gsis_id": "player_id"})
        .with_columns(pl.col("season").cast(pl.Int64), pl.col("week").cast(pl.Int64))
    )


def season_availability(
    snap_counts: pl.DataFrame,
    rosters: pl.DataFrame,
    *,
    team_games: int = 17,
    injuries: pl.DataFrame | None = None,
    player_ids: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Season availability rate per player from snap counts.

    A game counts as available when the player logged at least one offensive
    snap. ``availability_rate = games_available / team_games``.

    Args:
        snap_counts (pl.DataFrame): Weekly snap counts (``player_id, season,
            week, offense_snaps``), or :func:`sportsdataverse.nfl.load_nfl_snap_counts` rows
            (keyed by ``pfr_player_id``) together with ``player_ids``.
        rosters (pl.DataFrame): Season rosters: :func:`sportsdataverse.nfl.load_nfl_rosters`
            (``gsis_id``, ``season``, ``position``, ``birth_date``) or a frame carrying
            ``player_id, season, position, age``.
        team_games (int): Regular-season team games (17 from 2021).
        injuries (Optional[pl.DataFrame]): Optional weekly injury report frame
            (``player_id, season, week``); counted into ``injury_weeks``.
        player_ids (Optional[pl.DataFrame]): :func:`sportsdataverse.nfl.load_nfl_ff_playerids` rows; when
            given, PFR-keyed snap counts are bridged to gsis ids (regular season only).

    Returns:
        pl.DataFrame: One row per (player_id, season): ``player_id:Utf8,
        season:Int64, position:Utf8, age:Float64, games_available:Int64,
        availability_rate:Float64, injury_weeks:Float64``. Empty input returns
        a zero-row frame with that schema.

    Raises:
        ValueError: When ``snap_counts`` is keyed by ``pfr_player_id`` and no ``player_ids`` is given, or the
            bridge places fewer than ``SNAP_BRIDGE_MIN_MATCH`` of a season's skill-position rows.
        TypeError: When the PFR id columns are not both text.

    Example:
        Quick start::

            import sportsdataverse.nfl as nfl
            from sportsdataverse.nfl.nfl_availability import season_availability
            avail = season_availability(
                nfl.load_nfl_snap_counts([2023]),
                nfl.load_nfl_rosters([2023]),
                player_ids=nfl.load_nfl_ff_playerids(),
            )

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    required = {"player_id", "season", "week", "offense_snaps"}
    if snap_counts.height and "player_id" not in snap_counts.columns and "pfr_player_id" in snap_counts.columns:
        if player_ids is None:
            # load_nfl_snap_counts is keyed by PFR id; counting it as empty silently projected nobody
            raise ValueError("snap counts keyed by pfr_player_id: pass player_ids=load_nfl_ff_playerids()")
        snap_counts = _snaps_by_gsis(snap_counts, player_ids)
    if snap_counts.height == 0 or not required.issubset(snap_counts.columns):
        return pl.DataFrame(schema=_SEASON_SCHEMA)
    sc = snap_counts.with_columns(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    )
    agg = (
        sc.filter(pl.col("offense_snaps") > 0)
        .group_by("player_id", "season")
        .agg(pl.col("week").n_unique().cast(pl.Int64).alias("games_available"))
        .with_columns((pl.col("games_available") / float(team_games)).cast(pl.Float64).alias("availability_rate"))
    )
    ros = _season_rosters(rosters)
    if ros is not None:
        assert agg.schema["player_id"] == ros.schema["player_id"]
        agg = agg.join(ros, on=["player_id", "season"], how="left")
    else:
        agg = agg.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("position"),
            pl.lit(None, dtype=pl.Float64).alias("age"),
        )
    if injuries is not None and {"player_id", "season", "week"}.issubset(injuries.columns) and injuries.height > 0:
        inj = (
            injuries.with_columns(pl.col("player_id").cast(pl.Utf8), pl.col("season").cast(pl.Int64))
            .group_by("player_id", "season")
            .agg(pl.col("week").n_unique().cast(pl.Float64).alias("injury_weeks"))
        )
        assert agg.schema["player_id"] == inj.schema["player_id"]
        agg = agg.join(inj, on=["player_id", "season"], how="left").with_columns(pl.col("injury_weeks").fill_null(0.0))
    else:
        agg = agg.with_columns(pl.lit(0.0).alias("injury_weeks"))
    return agg.select(list(_SEASON_SCHEMA.keys()))


_PROJ_SCHEMA: dict = {
    "player_id": pl.Utf8,
    "target_season": pl.Int64,
    "position": pl.Utf8,
    "proj_availability": pl.Float64,
    "proj_games": pl.Float64,
    "proj_games_missed": pl.Float64,
}


@overload
def nfl_availability_projection(
    seasons: list[int], target_season: int, *, team_games: int = ..., return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...


@overload
def nfl_availability_projection(
    seasons: list[int], target_season: int, *, team_games: int = ..., return_as_pandas: Literal[True]
) -> pd.DataFrame: ...


def nfl_availability_projection(
    seasons: list[int], target_season: int, *, team_games: int = 17, return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """Empirical-Bayes availability projection: expected fraction of team games.

    Shrinks each player's historical availability toward the fitted position
    base rate::

        proj = (avail_games + k * base * team_games)
               / (played_seasons * team_games + k * team_games)

    with ``k = EB_PRIOR_SEASONS`` pseudo-seasons of prior weight. As-of-date
    clean: only seasons strictly before ``target_season`` contribute.

    Args:
        seasons (List[int]): History seasons to load snap counts/rosters for.
        target_season (int): The season being projected.
        team_games (int): Regular-season team games.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: ``player_id:Utf8, target_season:Int64, position:Utf8,
        proj_availability:Float64, proj_games:Float64,
        proj_games_missed:Float64``. Empty history returns a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_availability import nfl_availability_projection
            avail = nfl_availability_projection([2021, 2022, 2023], 2024)
            avail.sort("proj_games").head()

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    snaps = load_nfl_snap_counts(list(seasons))
    rosters = load_nfl_rosters(list(seasons))
    season_avail = season_availability(snaps, rosters, team_games=team_games, player_ids=load_nfl_ff_playerids())
    if snaps.height and season_avail.height == 0:
        raise ValueError(f"{snaps.height} snap-count rows gave no season availability: columns {snaps.columns}")
    hist = as_of_season_split(season_avail, target_season)
    if hist.height == 0:
        result = pl.DataFrame(schema=_PROJ_SCHEMA)
        return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result
    agg = (
        hist.sort("season")
        .group_by("player_id")
        .agg(
            pl.col("games_available").sum().alias("_avail_games"),
            pl.len().cast(pl.Float64).alias("_played_seasons"),
            pl.col("position").drop_nulls().last().alias("position"),
        )
    )
    bases = pl.DataFrame(
        {
            "position": [p for p in agg["position"].unique().to_list()],
        }
    ).with_columns(
        pl.col("position")
        .map_elements(
            lambda p: get_position_constants(p if p is not None else "DEFAULT").base_availability,
            return_dtype=pl.Float64,
        )
        .alias("_base")
    )
    assert agg.schema["position"] == bases.schema["position"]
    k = EB_PRIOR_SEASONS
    recal_a, recal_b = AVAIL_RECAL
    out = (
        agg.join(bases, on="position", how="left")
        .with_columns(
            (
                (pl.col("_avail_games") + k * pl.col("_base") * team_games)
                / (pl.col("_played_seasons") * team_games + k * team_games)
            )
            .cast(pl.Float64)
            .alias("_eb_rate")
        )
        .with_columns(
            (recal_a + recal_b * pl.col("_eb_rate")).clip(0.0, 1.0).cast(pl.Float64).alias("proj_availability")
        )
        .with_columns(
            (pl.col("proj_availability") * float(team_games)).cast(pl.Float64).alias("proj_games"),
        )
        .with_columns((float(team_games) - pl.col("proj_games")).cast(pl.Float64).alias("proj_games_missed"))
        .with_columns(pl.lit(target_season, dtype=pl.Int64).alias("target_season"))
    )
    result = out.select(list(_PROJ_SCHEMA.keys()))
    return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result


def compose_counting_projection(
    rate_proj: pl.DataFrame,
    avail_proj: pl.DataFrame,
    *,
    rate_col: str = "proj_rate",
    volume_col: str = "proj_volume",
) -> pl.DataFrame:
    """Compose skill and availability into a counting projection.

    The **only** place skill (rate x volume) and availability meet:
    ``proj_counting = rate * volume * proj_availability``, joined on
    ``player_id`` (dtype-asserted).

    Args:
        rate_proj (pl.DataFrame): Skill projection carrying ``player_id`` +
            ``rate_col`` + ``volume_col``.
        avail_proj (pl.DataFrame): Availability projection carrying
            ``player_id`` + ``proj_availability``.
        rate_col (str): Rate column name in ``rate_proj``.
        volume_col (str): Volume column name in ``rate_proj``.

    Returns:
        pl.DataFrame: ``rate_proj`` columns plus ``proj_availability`` and
        ``proj_counting:Float64``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_availability import compose_counting_projection
            out = compose_counting_projection(rate_frame, avail_frame)
    """
    left = rate_proj.with_columns(pl.col("player_id").cast(pl.Utf8))
    right = avail_proj.select(pl.col("player_id").cast(pl.Utf8), pl.col("proj_availability"))
    assert left.schema["player_id"] == right.schema["player_id"]
    return left.join(right, on="player_id", how="inner").with_columns(
        (pl.col(rate_col) * pl.col(volume_col) * pl.col("proj_availability")).cast(pl.Float64).alias("proj_counting")
    )
