"""SDV-native NFL **player stats** aggregated from play-by-play.

:func:`build_nfl_player_stats` is a faithful polars port of nflfastR's
``calculate_player_stats`` (``aggregate_game_stats.R``) computed by aggregating
the SDV-native enriched play-by-play (``load_nfl_pbp(..., source="sdv")``) into
the nflverse **player_stats** schema -- offense (passing / rushing / receiving)
plus special-teams touchdowns and fantasy points. The output column set matches
the published ``load_nfl_player_stats`` schema
(``tools/codegen/schemas/autodoc/nfl/load_nfl_player_stats.yaml``).

SDV-PBP column gaps vs nflfastR (and how they are handled here):

- ``qb_epa`` is absent from SDV pbp. nflfastR computes ``passing_epa`` as
  ``sum(qb_epa)`` (QB-credited EPA). We fall back to ``epa``; this produces a
  small ``passing_epa`` (and ``dakota``) parity difference on plays where a
  receiver fumbles after a completed catch -- on the vast majority of dropbacks
  ``qb_epa == epa`` so the correlation stays very high.
- ``two_point_conv_result`` is absent. We derive a successful two-point
  conversion as ``two_point_attempt == 1 & sp == 1`` (``sp`` = scoring play;
  a successful 2pt try scores). The passer / rusher / receiver player ids on
  that play attribute the conversion.
- ``special`` is absent. We derive it from
  ``play_type in {extra_point, field_goal, kickoff, punt}`` and credit
  ``special_teams_tds`` to ``td_player_id`` on those plays.
- ``fumbled_1_player_id`` / ``fumble_recovery_1_team`` are absent, so fumbles
  cannot be attributed to the specific fumbler/recovering team. We attribute a
  ``fumble`` / ``fumble_lost`` on the play to the relevant role player
  (passer on a sack, rusher on a run, receiver on a completed catch). This
  matches nflfastR for the common case (the role player IS the fumbler) and
  diverges only on rarer strip/lateral plays.
- ``first_down`` is absent but ``first_down_pass`` / ``first_down_rush`` are
  present and used directly (the nflfastR derivation).
- Lateral columns (``lateral_*``) are absent; laterals are not modeled (a
  negligible effect on season totals).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Literal, overload

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

# Special-teams play types used to derive the nflfastR ``special`` flag.
_SPECIAL_PLAY_TYPES: tuple[str, ...] = ("extra_point", "field_goal", "kickoff", "punt")

# Canonical output column order (matches the published player_stats schema).
_OUTPUT_COLUMNS: tuple[str, ...] = (
    "player_id",
    "player_name",
    "player_display_name",
    "position",
    "position_group",
    "headshot_url",
    "recent_team",
    "season",
    "week",
    "season_type",
    "opponent_team",
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "sacks",
    "sack_yards",
    "sack_fumbles",
    "sack_fumbles_lost",
    "passing_air_yards",
    "passing_yards_after_catch",
    "passing_first_downs",
    "passing_epa",
    "passing_2pt_conversions",
    "pacr",
    "dakota",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "rushing_fumbles",
    "rushing_fumbles_lost",
    "rushing_first_downs",
    "rushing_epa",
    "rushing_2pt_conversions",
    "receptions",
    "targets",
    "receiving_yards",
    "receiving_tds",
    "receiving_fumbles",
    "receiving_fumbles_lost",
    "receiving_air_yards",
    "receiving_yards_after_catch",
    "receiving_first_downs",
    "receiving_epa",
    "receiving_2pt_conversions",
    "racr",
    "target_share",
    "air_yards_share",
    "wopr",
    "special_teams_tds",
    "fantasy_points",
    "fantasy_points_ppr",
)

# Columns whose NA should NOT be coerced to 0 (rate / model stats).
_RATE_COLUMNS: tuple[str, ...] = (
    "passing_epa",
    "rushing_epa",
    "receiving_epa",
    "dakota",
    "racr",
    "target_share",
    "air_yards_share",
    "wopr",
    "pacr",
)


def _i(col: str) -> pl.Expr:
    """``pl.col(col)`` coerced to Int64 with nulls -> 0 (counting-stat helper)."""
    return pl.col(col).cast(pl.Int64, strict=False).fill_null(0)


def _f(col: str) -> pl.Expr:
    """``pl.col(col)`` coerced to Float64 with nulls -> 0 (yardage-stat helper)."""
    return pl.col(col).cast(pl.Float64, strict=False).fill_null(0.0)


def _prepare_pbp(pbp: pl.DataFrame) -> pl.DataFrame:
    """Add the derived ``special`` flag and normalize integer flag columns."""
    return pbp.with_columns(pl.col("play_type").is_in(_SPECIAL_PLAY_TYPES).cast(pl.Int64).alias("special"))


def _passing_frame(data: pl.DataFrame, two_pts: pl.DataFrame) -> pl.DataFrame:
    """Per-(player, week, season) passing stats keyed on ``passer_player_id``."""
    pdf = (
        data.filter(pl.col("play_type").is_in(["pass", "qb_spike"]))
        .filter(pl.col("passer_player_id").is_not_null())
        .group_by(["passer_player_id", "week", "season"])
        .agg(
            pl.first("passer_player_name").alias("name_pass"),
            pl.first("posteam").alias("team_pass"),
            pl.first("defteam").alias("opp_pass"),
            ((_f("passing_yards") - _f("air_yards")) * _i("complete_pass")).sum().alias("passing_yards_after_catch"),
            _f("passing_yards").sum().alias("passing_yards"),
            ((_i("touchdown") == 1) & (pl.col("td_team") == pl.col("posteam")) & (_i("complete_pass") == 1))
            .sum()
            .alias("passing_tds"),
            _i("interception").sum().alias("interceptions"),
            ((_i("complete_pass") == 1) | (_i("incomplete_pass") == 1) | (_i("interception") == 1))
            .sum()
            .alias("attempts"),
            (_i("complete_pass") == 1).sum().alias("completions"),
            # fumble attribution fallback: fumble on a sack credited to passer
            ((_i("fumble") == 1) & (_i("sack") == 1)).sum().alias("sack_fumbles"),
            ((_i("fumble_lost") == 1) & (_i("sack") == 1)).sum().alias("sack_fumbles_lost"),
            _f("air_yards").sum().alias("passing_air_yards"),
            _i("sack").sum().alias("sacks"),
            (-1.0 * (_f("yards_gained") * _i("sack")).sum()).alias("sack_yards"),
            _i("first_down_pass").sum().alias("passing_first_downs"),
            _f("epa").sum().alias("passing_epa"),  # qb_epa absent -> epa fallback
            _f("cpoe").mean().alias("passing_cpoe"),
        )
        .rename({"passer_player_id": "player_id"})
    )

    pass_two = (
        two_pts.filter(pl.col("pass_attempt") == 1)
        .filter(pl.col("passer_player_id").is_not_null())
        .group_by(["passer_player_id", "week", "season"])
        .agg(pl.len().cast(pl.Int64).alias("passing_2pt_conversions"))
        .rename({"passer_player_id": "player_id"})
    )

    return (
        pdf.join(pass_two, on=["player_id", "week", "season"], how="full", coalesce=True)
        .with_columns(pl.col("passing_2pt_conversions").fill_null(0))
        .filter(pl.col("player_id").is_not_null())
    )


def _rushing_frame(data: pl.DataFrame, two_pts: pl.DataFrame) -> pl.DataFrame:
    """Per-(player, week, season) rushing stats keyed on ``rusher_player_id``."""
    rdf = (
        data.filter(pl.col("play_type").is_in(["run", "qb_kneel"]))
        .filter(pl.col("rusher_player_id").is_not_null())
        .group_by(["rusher_player_id", "week", "season"])
        .agg(
            pl.first("rusher_player_name").alias("name_rush"),
            pl.first("posteam").alias("team_rush"),
            pl.first("defteam").alias("opp_rush"),
            _f("rushing_yards").sum().alias("rushing_yards"),
            (pl.col("td_player_id") == pl.col("rusher_player_id")).sum().alias("rushing_tds"),
            pl.len().cast(pl.Int64).alias("carries"),
            (_i("fumble") == 1).sum().alias("rushing_fumbles"),
            (_i("fumble_lost") == 1).sum().alias("rushing_fumbles_lost"),
            _i("first_down_rush").sum().alias("rushing_first_downs"),
            _f("epa").sum().alias("rushing_epa"),
        )
        .rename({"rusher_player_id": "player_id"})
    )

    rush_two = (
        two_pts.filter(pl.col("rush_attempt") == 1)
        .filter(pl.col("rusher_player_id").is_not_null())
        .group_by(["rusher_player_id", "week", "season"])
        .agg(pl.len().cast(pl.Int64).alias("rushing_2pt_conversions"))
        .rename({"rusher_player_id": "player_id"})
    )

    return (
        rdf.join(rush_two, on=["player_id", "week", "season"], how="full", coalesce=True)
        .with_columns(pl.col("rushing_2pt_conversions").fill_null(0))
        .filter(pl.col("player_id").is_not_null())
    )


def _receiving_frame(data: pl.DataFrame, two_pts: pl.DataFrame) -> pl.DataFrame:
    """Per-(player, week, season) receiving stats keyed on ``receiver_player_id``.

    Includes the per-game team-denominator shares (``target_share`` /
    ``air_yards_share`` / ``wopr``).
    """
    rec = (
        data.filter(pl.col("receiver_player_id").is_not_null())
        .group_by(["receiver_player_id", "week", "season"])
        .agg(
            pl.first("receiver_player_name").alias("name_receiver"),
            pl.first("posteam").alias("team_receiver"),
            pl.first("defteam").alias("opp_receiver"),
            _f("receiving_yards").sum().alias("receiving_yards"),
            (_i("complete_pass") == 1).sum().alias("receptions"),
            pl.len().cast(pl.Int64).alias("targets"),
            (pl.col("td_player_id") == pl.col("receiver_player_id")).sum().alias("receiving_tds"),
            # fumble attribution fallback: fumble on a completed catch -> receiver
            ((_i("fumble") == 1) & (_i("complete_pass") == 1)).sum().alias("receiving_fumbles"),
            ((_i("fumble_lost") == 1) & (_i("complete_pass") == 1)).sum().alias("receiving_fumbles_lost"),
            _f("air_yards").sum().alias("receiving_air_yards"),
            _f("yards_after_catch").sum().alias("receiving_yards_after_catch"),
            _i("first_down_pass").sum().alias("receiving_first_downs"),
            _f("epa").sum().alias("receiving_epa"),
        )
        .rename({"receiver_player_id": "player_id"})
    )

    # Team receiving denominators (per posteam-week) for WOPR.
    rec_team = (
        data.filter(pl.col("receiver_player_id").is_not_null())
        .group_by(["posteam", "week", "season"])
        .agg(
            pl.len().cast(pl.Int64).alias("team_targets"),
            _f("air_yards").sum().alias("team_air_yards"),
        )
    )

    rec_df = (
        rec.join(
            rec_team,
            left_on=["team_receiver", "week", "season"],
            right_on=["posteam", "week", "season"],
            how="left",
        )
        .with_columns(
            (pl.col("targets") / pl.col("team_targets")).alias("target_share"),
            (pl.col("receiving_air_yards") / pl.col("team_air_yards")).alias("air_yards_share"),
            pl.when(pl.col("receiving_air_yards") == 0)
            .then(0.0)
            .otherwise(pl.col("receiving_yards") / pl.col("receiving_air_yards"))
            .alias("racr"),
        )
        .with_columns((1.5 * pl.col("target_share") + 0.7 * pl.col("air_yards_share")).alias("wopr"))
    )

    rec_two = (
        two_pts.filter(pl.col("pass_attempt") == 1)
        .filter(pl.col("receiver_player_id").is_not_null())
        .group_by(["receiver_player_id", "week", "season"])
        .agg(pl.len().cast(pl.Int64).alias("receiving_2pt_conversions"))
        .rename({"receiver_player_id": "player_id"})
    )

    return (
        rec_df.join(rec_two, on=["player_id", "week", "season"], how="full", coalesce=True)
        .with_columns(pl.col("receiving_2pt_conversions").fill_null(0))
        .filter(pl.col("player_id").is_not_null())
    )


def _special_teams_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per-(player, week, season) special-teams touchdowns keyed on ``td_player_id``."""
    return (
        pbp.filter((pl.col("special") == 1) & pl.col("td_player_id").is_not_null())
        .group_by(["td_player_id", "week", "season"])
        .agg(
            pl.first("td_player_name").alias("name_st"),
            pl.first("td_team").alias("team_st"),
            pl.first("defteam").alias("opp_st"),
            _i("touchdown").sum().alias("special_teams_tds"),
        )
        .rename({"td_player_id": "player_id"})
    )


def _empty_player_stats(*, weekly: bool, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Return a zero-row frame carrying the canonical schema."""
    cols = list(_OUTPUT_COLUMNS)
    if not weekly:
        cols = [c for c in cols if c not in ("week", "season_type", "opponent_team")]
        cols.insert(cols.index("recent_team") + 1, "games")
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    str_cols = {
        "player_id",
        "player_name",
        "player_display_name",
        "position",
        "position_group",
        "headshot_url",
        "recent_team",
        "season_type",
        "opponent_team",
    }
    int_cols = {"season", "week", "games"}
    for c in cols:
        if c in str_cols:
            schema[c] = pl.Utf8
        elif c in int_cols:
            schema[c] = pl.Int64
        else:
            schema[c] = pl.Float64
    out = pl.DataFrame(schema=schema)
    if return_as_pandas:
        return out.to_pandas()
    return out


@overload
def build_nfl_player_stats(
    seasons: List[int],
    *,
    summary_level: str = ...,
    season_type: str = ...,
    source: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def build_nfl_player_stats(
    seasons: List[int],
    *,
    summary_level: str = ...,
    season_type: str = ...,
    source: str = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def build_nfl_player_stats(
    seasons: List[int],
    *,
    summary_level: str = "week",
    season_type: str = "REG",
    source: str = "sdv",
    return_as_pandas: bool = False,
) -> pl.DataFrame | "pd.DataFrame":
    """Build nflverse **player_stats** by aggregating SDV-native play-by-play.

    A faithful polars port of nflfastR's ``calculate_player_stats``
    (``aggregate_game_stats.R``): per-player passing / rushing / receiving frames
    are full-outer-joined on the group keys, special-teams touchdowns and fantasy
    points are added, and player metadata is joined from
    :func:`sportsdataverse.nfl.load_nfl_players`. See the module docstring for the
    SDV-PBP column-gap handling (notably ``passing_epa`` falls back to ``epa``
    because ``qb_epa`` is absent).

    Args:
        seasons: Four-digit NFL seasons to aggregate (e.g. ``[2023]``).
        summary_level: ``"week"`` (group on season + week + player_id, with
            ``opponent_team``) or ``"season"`` (group on season + player_id, with
            ``recent_team`` = last team and ``games`` = distinct game count).
        season_type: ``"REG"``, ``"POST"``, or ``"REG+POST"``. Pre-filters the
            play-by-play before aggregation.
        source: Play-by-play release passed to :func:`load_nfl_pbp`. Defaults to
            ``"sdv"`` (the SDV-native enriched release).
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A polars (or pandas) DataFrame in the published ``load_nfl_player_stats``
        schema. At ``summary_level="season"`` the ``week`` / ``season_type`` /
        ``opponent_team`` columns are replaced by a ``games`` column.

    Raises:
        ValueError: If ``summary_level`` is not ``"week"``/``"season"`` or
            ``season_type`` is not ``"REG"``/``"POST"``/``"REG+POST"``.

    Example:
        Weekly player stats for 2023::

            from sportsdataverse.nfl import build_nfl_player_stats
            wk = build_nfl_player_stats([2023], summary_level="week")
            print(wk.shape)

        Season totals as pandas::

            df_pd = build_nfl_player_stats([2023], summary_level="season",
                                           return_as_pandas=True)

        Pipeline next step (one line)::

            wk.filter(pl.col("attempts") >= 5).sort("passing_epa", descending=True).head()

    See Also:
        * `nflfastR <https://www.nflfastr.com>`_ -- the ``calculate_player_stats`` source
        * `nflreadpy <https://github.com/nflverse/nflreadpy>`_ -- nflverse loaders (Python)
    """
    if summary_level not in ("week", "season"):
        raise ValueError(f"Invalid summary_level {summary_level!r}; expected 'week' or 'season'.")
    if season_type not in ("REG", "POST", "REG+POST"):
        raise ValueError(f"Invalid season_type {season_type!r}; expected 'REG', 'POST', or 'REG+POST'.")

    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp, load_nfl_players  # noqa: PLC0415

    pbp = load_nfl_pbp(seasons, source=source)

    # season_type pre-filter (PBP carries REG / POST / etc.).
    if season_type in ("REG", "POST"):
        pbp = pbp.filter(pl.col("season_type") == season_type)
    if pbp.height == 0:
        return _empty_player_stats(weekly=summary_level == "week", return_as_pandas=return_as_pandas)

    pbp = _prepare_pbp(pbp)

    # 1. "normal" plays counting toward official stats (down present).
    data = pbp.filter(pl.col("down").is_not_null() & pl.col("play_type").is_in(["pass", "qb_kneel", "qb_spike", "run"]))
    # 2. successful two-point conversions (two_point_attempt rows have null down,
    #    excluded from ``data``). Success derived from sp == 1.
    two_pts = pbp.filter((pl.col("two_point_attempt") == 1) & (pl.col("sp") == 1))

    # Cast the join keys to stable dtypes so empty sub-frames (all-null
    # player_id, inferred as Null dtype) don't break the full-outer joins.
    def _keys(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.col("player_id").cast(pl.Utf8, strict=False),
            pl.col("week").cast(pl.Int64, strict=False),
            pl.col("season").cast(pl.Int64, strict=False),
        )

    pass_df = _keys(_passing_frame(data, two_pts))
    rush_df = _keys(_rushing_frame(data, two_pts))
    rec_df = _keys(_receiving_frame(data, two_pts))
    st_tds = _keys(_special_teams_frame(pbp))

    # season_type per (season, week). A given (season, week) is REG xor POST, so
    # this is 1-to-1 with the player-week grain (deduped to be safe).
    s_type = pbp.select(["season", "week", "season_type"]).unique(subset=["season", "week"], keep="first")

    player_df = (
        pass_df.join(rush_df, on=["player_id", "week", "season"], how="full", coalesce=True)
        .join(rec_df, on=["player_id", "week", "season"], how="full", coalesce=True)
        .join(st_tds, on=["player_id", "week", "season"], how="full", coalesce=True)
        .join(s_type, on=["season", "week"], how="left")
    )

    player_df = player_df.with_columns(
        pl.coalesce(["name_pass", "name_rush", "name_receiver", "name_st"]).alias("player_name"),
        pl.coalesce(["team_pass", "team_rush", "team_receiver", "team_st"]).alias("recent_team"),
        pl.coalesce(["opp_pass", "opp_rush", "opp_receiver", "opp_st"]).alias("opponent_team"),
    )

    # Counting / yardage columns -> fill 0; rate columns keep nulls.
    count_yard_cols = [
        "completions",
        "attempts",
        "passing_yards",
        "passing_tds",
        "interceptions",
        "sacks",
        "sack_yards",
        "sack_fumbles",
        "sack_fumbles_lost",
        "passing_air_yards",
        "passing_yards_after_catch",
        "passing_first_downs",
        "passing_2pt_conversions",
        "carries",
        "rushing_yards",
        "rushing_tds",
        "rushing_fumbles",
        "rushing_fumbles_lost",
        "rushing_first_downs",
        "rushing_2pt_conversions",
        "receptions",
        "targets",
        "receiving_yards",
        "receiving_tds",
        "receiving_fumbles",
        "receiving_fumbles_lost",
        "receiving_air_yards",
        "receiving_yards_after_catch",
        "receiving_first_downs",
        "receiving_2pt_conversions",
        "special_teams_tds",
    ]
    player_df = player_df.with_columns([pl.col(c).fill_null(0) for c in count_yard_cols if c in player_df.columns])

    # pacr (passing) computed at the week-row level.
    player_df = player_df.with_columns(
        pl.when(pl.col("passing_air_yards") <= 0)
        .then(0.0)
        .otherwise(pl.col("passing_yards") / pl.col("passing_air_yards"))
        .alias("pacr"),
        # dakota = 0.816*(passing_epa/attempts) + 0.184*passing_cpoe (linear approx)
        pl.when(pl.col("attempts") > 0)
        .then(0.816 * (pl.col("passing_epa") / pl.col("attempts")) + 0.184 * pl.col("passing_cpoe").fill_null(0.0))
        .otherwise(None)
        .alias("dakota"),
    )

    player_df = player_df.filter(pl.col("player_id").is_not_null())

    if summary_level == "season":
        player_df = _collapse_to_season(player_df, season_type=season_type)

    player_df = _add_fantasy(player_df)
    player_df = _join_player_meta(player_df, load_nfl_players())
    player_df = _finalize(player_df, weekly=summary_level == "week")

    if return_as_pandas:
        return player_df.to_pandas()
    return player_df


def _collapse_to_season(player_df: pl.DataFrame, *, season_type: str) -> pl.DataFrame:
    """Collapse week-level rows to season totals grouped on (season, player_id)."""
    # capture targets / air yards before they are summed (for share recomputation)
    pre = player_df.with_columns(
        pl.col("target_share").alias("_ts"),
        pl.col("air_yards_share").alias("_ays"),
    )
    summed = (
        pre.group_by(["season", "player_id"])
        .agg(
            pl.len().alias("games"),
            pl.last("recent_team").alias("recent_team"),
            pl.first("player_name").alias("player_name"),
            *[
                pl.col(c).sum().alias(c)
                for c in (
                    "completions",
                    "attempts",
                    "passing_yards",
                    "passing_tds",
                    "interceptions",
                    "sacks",
                    "sack_yards",
                    "sack_fumbles",
                    "sack_fumbles_lost",
                    "passing_air_yards",
                    "passing_yards_after_catch",
                    "passing_first_downs",
                    "passing_2pt_conversions",
                    "carries",
                    "rushing_yards",
                    "rushing_tds",
                    "rushing_fumbles",
                    "rushing_fumbles_lost",
                    "rushing_first_downs",
                    "rushing_2pt_conversions",
                    "receptions",
                    "targets",
                    "receiving_yards",
                    "receiving_tds",
                    "receiving_fumbles",
                    "receiving_fumbles_lost",
                    "receiving_air_yards",
                    "receiving_yards_after_catch",
                    "receiving_first_downs",
                    "receiving_2pt_conversions",
                    "special_teams_tds",
                )
            ],
            pl.col("passing_epa").sum().alias("passing_epa"),
            pl.col("rushing_epa").sum().alias("rushing_epa"),
            pl.col("receiving_epa").sum().alias("receiving_epa"),
            pl.col("passing_cpoe").mean().alias("passing_cpoe"),
        )
        .with_columns(
            pl.when(pl.col("passing_air_yards") <= 0)
            .then(0.0)
            .otherwise(pl.col("passing_yards") / pl.col("passing_air_yards"))
            .alias("pacr"),
            pl.when(pl.col("receiving_air_yards") == 0)
            .then(0.0)
            .otherwise(pl.col("receiving_yards") / pl.col("receiving_air_yards"))
            .alias("racr"),
            pl.when(pl.col("attempts") > 0)
            .then(0.816 * (pl.col("passing_epa") / pl.col("attempts")) + 0.184 * pl.col("passing_cpoe").fill_null(0.0))
            .otherwise(None)
            .alias("dakota"),
        )
    )

    # target_share / air_yards_share at season level recomputed against team
    # totals reconstructed from the per-week shares (nflfastR's approach):
    #   season_share = sum(player_x) / sum(player_x / week_share)
    team_denoms = (
        pre.with_columns(
            pl.when(pl.col("_ts") > 0).then(pl.col("targets") / pl.col("_ts")).otherwise(None).alias("_team_tgt"),
            pl.when(pl.col("_ays") != 0)
            .then(pl.col("receiving_air_yards") / pl.col("_ays"))
            .otherwise(None)
            .alias("_team_ay"),
        )
        .group_by(["season", "player_id"])
        .agg(
            pl.col("targets").sum().alias("_tgt_sum"),
            pl.col("_team_tgt").sum().alias("_team_tgt_sum"),
            pl.col("receiving_air_yards").sum().alias("_ay_sum"),
            pl.col("_team_ay").sum().alias("_team_ay_sum"),
        )
        .with_columns(
            pl.when(pl.col("_team_tgt_sum") > 0)
            .then(pl.col("_tgt_sum") / pl.col("_team_tgt_sum"))
            .otherwise(None)
            .alias("target_share"),
            pl.when(pl.col("_team_ay_sum") != 0)
            .then(pl.col("_ay_sum") / pl.col("_team_ay_sum"))
            .otherwise(None)
            .alias("air_yards_share"),
        )
        .select(["season", "player_id", "target_share", "air_yards_share"])
    )

    return summed.join(team_denoms, on=["season", "player_id"], how="left").with_columns(
        (1.5 * pl.col("target_share").fill_null(0.0) + 0.7 * pl.col("air_yards_share").fill_null(0.0)).alias("wopr")
    )


def _add_fantasy(player_df: pl.DataFrame) -> pl.DataFrame:
    """Add ``fantasy_points`` (standard) + ``fantasy_points_ppr``."""
    return player_df.with_columns(
        (
            (1.0 / 25.0) * pl.col("passing_yards")
            + 4.0 * pl.col("passing_tds")
            + -2.0 * pl.col("interceptions")
            + (1.0 / 10.0) * (pl.col("rushing_yards") + pl.col("receiving_yards"))
            + 6.0 * (pl.col("rushing_tds") + pl.col("receiving_tds") + pl.col("special_teams_tds"))
            + 2.0
            * (
                pl.col("passing_2pt_conversions")
                + pl.col("rushing_2pt_conversions")
                + pl.col("receiving_2pt_conversions")
            )
            + -2.0 * (pl.col("sack_fumbles_lost") + pl.col("rushing_fumbles_lost") + pl.col("receiving_fumbles_lost"))
        ).alias("fantasy_points")
    ).with_columns((pl.col("fantasy_points") + pl.col("receptions")).alias("fantasy_points_ppr"))


def _join_player_meta(player_df: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Join player identity metadata from :func:`load_nfl_players`."""
    meta = players.select(
        pl.col("gsis_id").alias("player_id"),
        pl.col("display_name").alias("player_display_name"),
        pl.col("short_name").alias("meta_player_name"),
        pl.col("position"),
        pl.col("position_group"),
        pl.col("headshot").alias("headshot_url"),
    ).unique(subset=["player_id"], keep="first")
    joined = player_df.join(meta, on="player_id", how="left")
    # Prefer the players-master short name; fall back to the pbp name.
    return joined.with_columns(pl.coalesce(["meta_player_name", "player_name"]).alias("player_name")).drop(
        "meta_player_name"
    )


def _finalize(player_df: pl.DataFrame, *, weekly: bool) -> pl.DataFrame:
    """Select the canonical column order + cast id/numeric types."""
    cols = list(_OUTPUT_COLUMNS)
    if not weekly:
        cols = [c for c in cols if c not in ("week", "season_type", "opponent_team")]
        cols.insert(cols.index("recent_team") + 1, "games")
    # ensure every expected column exists
    for c in cols:
        if c not in player_df.columns:
            player_df = player_df.with_columns(pl.lit(None).alias(c))
    out = player_df.select(cols).filter(pl.col("player_id").is_not_null() & pl.col("player_name").is_not_null())
    sort_keys = ["player_id", "season"] + (["week"] if weekly else [])
    return out.sort(sort_keys)
