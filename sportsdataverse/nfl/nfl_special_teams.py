"""NFL special-teams EPA by unit ④ + punter net-over-expected.

Reads EPA straight from the pbp (never recomputed) and the shipped punt
landing distribution (``nfl_fourth_down._load_punt_data``) for expected punt
net.  nflverse semantics note: on kickoffs ``posteam`` is the RECEIVING
team, so the kicking-team unit on a kickoff is credited to ``defteam``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

_ST_SCHEMA: dict = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "unit": pl.Utf8,
    "plays": pl.Int64,
    "epa": pl.Float64,
    "epa_per_play": pl.Float64,
}

_PUNTER_SCHEMA: dict = {
    "season": pl.Int64,
    "punter_player_id": pl.Utf8,
    "punts": pl.Int64,
    "gross_avg": pl.Float64,
    "net_avg": pl.Float64,
    "exp_net_avg": pl.Float64,
    "net_over_expected": pl.Float64,
    "epa": pl.Float64,
}

#: yards charged for a touchback when computing punt net.
_TOUCHBACK_YARDS = 20.0


def _special_teams_epa_from_pbp(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per (season, team, unit) special-teams EPA from an in-memory pbp."""
    df = pbp.filter(
        pl.col("play_type").is_in(["punt", "kickoff", "field_goal", "extra_point"])
        & pl.col("epa").is_not_null()
        & pl.col("posteam").is_not_null()
    )
    if df.height == 0:
        return pl.DataFrame(schema=_ST_SCHEMA)
    df = df.with_columns(pl.col("posteam").cast(pl.Utf8), pl.col("defteam").cast(pl.Utf8))

    # kicking-team side: posteam except on kickoffs (where posteam receives)
    kick = df.with_columns(
        pl.when(pl.col("play_type") == "kickoff").then(pl.col("defteam")).otherwise(pl.col("posteam")).alias("team"),
        pl.when(pl.col("play_type") == "kickoff").then(-pl.col("epa")).otherwise(pl.col("epa")).alias("unit_epa"),
        pl.col("play_type").alias("unit"),
    )
    # return side exists only for punt / kickoff
    ret = df.filter(pl.col("play_type").is_in(["punt", "kickoff"])).with_columns(
        pl.when(pl.col("play_type") == "kickoff").then(pl.col("posteam")).otherwise(pl.col("defteam")).alias("team"),
        pl.when(pl.col("play_type") == "kickoff").then(pl.col("epa")).otherwise(-pl.col("epa")).alias("unit_epa"),
        (pl.col("play_type") + pl.lit("_return")).alias("unit"),
    )
    long = pl.concat(
        [
            kick.select("season", "team", "unit", "unit_epa"),
            ret.select("season", "team", "unit", "unit_epa"),
        ]
    )
    assert long.schema["team"] == pl.Utf8
    return (
        long.group_by("season", "team", "unit")
        .agg(
            pl.len().cast(pl.Int64).alias("plays"),
            pl.col("unit_epa").sum().alias("epa"),
            pl.col("unit_epa").mean().alias("epa_per_play"),
        )
        .with_columns(pl.col("season").cast(pl.Int64))
        .select(list(_ST_SCHEMA.keys()))
        .sort("season", "team", "unit")
    )


def nfl_special_teams_epa(
    seasons: Union[int, List[int]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Special-teams EPA by team-unit.

    Units: ``punt`` / ``punt_return`` / ``kickoff`` / ``kickoff_return`` /
    ``field_goal`` / ``extra_point``.  On each punt/kickoff the kicking
    team's unit carries the play EPA signed to the kicking team and the
    return team's unit its negation, so a team's units sum to its total
    ST-play EPA.

    Args:
        seasons: Season or list of seasons.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(season, team, unit)``: ``plays``, ``epa``, ``epa_per_play``.
        Empty seasons yield a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_special_teams import nfl_special_teams_epa
            st = nfl_special_teams_epa([2023])
            print(st.filter(pl.col("unit") == "punt").sort("epa", descending=True).head())

        See Also:
            * `nflfastR`_ -- EPA source columns.

        .. _nflfastR: https://www.nflfastr.com
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp

    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    if not season_list:
        out: pl.DataFrame = pl.DataFrame(schema=_ST_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    out = _special_teams_epa_from_pbp(load_nfl_pbp(season_list))
    return out.to_pandas() if return_as_pandas else out


def _punter_value_from(pbp: pl.DataFrame, punt_data: pl.DataFrame) -> pl.DataFrame:
    """Per (season, punter) net-over-expected from pbp + the punt distribution.

    ``net = kick_distance - return_yards - 20 * touchback``; expected net at a
    line of scrimmage is ``yardline_100 - E[yardline_after]`` under the shipped
    landing distribution (``yardline_after`` is in the punting team's
    coordinate).
    """
    punts = pbp.filter(
        (pl.col("play_type") == "punt")
        & pl.col("punter_player_id").is_not_null()
        & pl.col("kick_distance").is_not_null()
    )
    if punts.height == 0:
        return pl.DataFrame(schema=_PUNTER_SCHEMA)

    exp_after = (
        punt_data.group_by("yardline_100")
        .agg(((pl.col("yardline_after") * pl.col("pct")).sum() / pl.col("pct").sum()).alias("exp_yardline_after"))
        .with_columns(pl.col("yardline_100").cast(pl.Float64))
    )
    punts = punts.with_columns(pl.col("yardline_100").cast(pl.Float64))
    assert punts.schema["yardline_100"] == exp_after.schema["yardline_100"]
    punts = punts.join(exp_after, on="yardline_100", how="left").with_columns(
        (pl.col("yardline_100") - pl.col("exp_yardline_after")).alias("exp_net"),
        (
            pl.col("kick_distance")
            - pl.col("return_yards").fill_null(0.0)
            - _TOUCHBACK_YARDS * pl.col("touchback").fill_null(0.0)
        ).alias("net"),
    )
    # the shipped punt table only covers yardline_100 ~31..99; punts outside it get
    # a null exp_net, which .mean() would skip in exp_net_avg while net_avg kept the
    # row -- restrict BOTH aggregates to the supported punt set so NOE is unbiased
    punts = punts.filter(pl.col("exp_net").is_not_null())
    return (
        punts.group_by("season", "punter_player_id")
        .agg(
            pl.len().cast(pl.Int64).alias("punts"),
            pl.col("kick_distance").mean().alias("gross_avg"),
            pl.col("net").mean().alias("net_avg"),
            pl.col("exp_net").mean().alias("exp_net_avg"),
            pl.col("epa").sum().alias("epa"),
        )
        .with_columns(
            (pl.col("net_avg") - pl.col("exp_net_avg")).alias("net_over_expected"),
            pl.col("season").cast(pl.Int64),
            pl.col("punter_player_id").cast(pl.Utf8),
        )
        .select(list(_PUNTER_SCHEMA.keys()))
        .sort("season", "net_over_expected", descending=[False, True])
    )


def nfl_punter_value(
    seasons: Union[int, List[int]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Punter net-field-position value over expected.

    Expected net comes from the shipped punt landing distribution
    (``nfl_fourth_down._load_punt_data``) evaluated at each punt's line of
    scrimmage; realized net is ``kick_distance - return_yards - 20*touchback``.

    Args:
        seasons: Season or list of seasons.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(season, punter_player_id)``: ``punts``, ``gross_avg``,
        ``net_avg``, ``exp_net_avg``, ``net_over_expected``, ``epa``.
        Empty seasons yield a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_special_teams import nfl_punter_value
            pv = nfl_punter_value([2023])
            print(pv.head())

        See Also:
            * `nflfastR`_ -- punt pbp columns (kick_distance, return_yards).

        .. _nflfastR: https://www.nflfastr.com
    """
    from sportsdataverse.nfl.nfl_fourth_down import _load_punt_data
    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp

    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    punt_data = _load_punt_data()
    if not season_list or punt_data is None:
        out: pl.DataFrame = pl.DataFrame(schema=_PUNTER_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    out = _punter_value_from(load_nfl_pbp(season_list), punt_data)
    return out.to_pandas() if return_as_pandas else out
