"""SDV-native NFL **player + team stats** aggregated from play-by-play.

:func:`build_nfl_player_stats` is a faithful polars port of nflfastR's
``calculate_player_stats`` (``aggregate_game_stats.R``) computed by aggregating
the SDV-native enriched play-by-play (``load_nfl_pbp(..., source="sdv")``) into
the nflverse **player_stats** schema -- offense (passing / rushing / receiving)
plus special-teams touchdowns and fantasy points. The output column set matches
the published ``load_nfl_player_stats`` schema
(``tools/codegen/schemas/autodoc/nfl/load_nfl_player_stats.yaml``).

:func:`build_nfl_team_stats` is the team-level analogue (a polars port of
nflfastR's ``calculate_stats(stat_type = "team")`` / the ``aggregate_game_stats*``
family). It aggregates the same SDV-native play-by-play into the nflverse
**team_stats** schema (~102 columns: offense + ``def_*`` defense + kicking +
misc / returns / turnovers), matching the published ``load_nfl_team_stats`` schema
(``tools/codegen/schemas/autodoc/nfl/load_nfl_team_stats.yaml``).

team_stats grouping (validated in the SDV-PBP "Phase B" build, which matched the
nflverse play-by-play columns exactly):

- **Offense** is keyed on ``posteam`` -- the team-level sum of the same per-play
  passing / rushing / receiving events the player-stats builder uses.
- **Defense** is keyed on the **tackler's team** (the per-play ``*_team`` slot
  tags), NOT ``defteam`` -- a defensive stat is credited to the team of the
  player who made the play, which diverges from ``defteam`` on return plays.
  Counting per team-tagged slot and grouping on the slot team reproduces
  ``def_tackles_solo`` / ``def_tackles_with_assist`` / ``def_tackle_assists``
  exactly. Slots with no ``*_team`` companion (``sack`` / ``qb_hit`` /
  ``pass_defense`` / ``tackle_for_loss`` / ``forced_fumble`` / ``interception``)
  credit the defensive player, i.e. ``defteam``.
- **Kicking** is keyed on ``posteam`` (the kicking team on a FG / PAT).
- **Returns** are keyed on ``return_team``; **penalties** on ``penalty_team``;
  **timeouts** on ``timeout_team``.
- Per-week filtering on ``defteam`` alone double-counts a team that appears as
  the opponent across multiple games in the same week, so the team-tagged-slot /
  ``return_team`` / ``penalty_team`` keying is used throughout (the play-level
  team tag is unambiguous and matches nflverse at the game grain).

SDV-PBP gaps for team_stats (documented, small):

- ``passing_epa`` / ``rushing_epa`` / ``receiving_epa`` use ``epa`` (``qb_epa``
  is absent), as in :func:`build_nfl_player_stats`.
- ``def_sacks`` / ``def_sack_yards`` use the ``sack_player_id`` (full) +
  ``half_sack_{1,2}`` (0.5 each) attribution -- exact vs nflverse.
- ``def_tds`` / ``def_fumbles`` / ``fumble_recovery_*`` / ``def_safeties`` /
  ``misc_yards`` are derived from the per-play fumble-recovery / return /
  touchdown / safety columns; a handful of lateral / multi-recovery edge plays
  diverge negligibly from the playstats-derived nflverse baseline.

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


# ---------------------------------------------------------------------------
# team_stats
# ---------------------------------------------------------------------------

# Canonical team_stats output column order (matches the published schema in
# tools/codegen/schemas/autodoc/nfl/load_nfl_team_stats.yaml).
_TEAM_OUTPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "week",
    "team",
    "season_type",
    "opponent_team",
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "sack_yards_lost",
    "sack_fumbles",
    "sack_fumbles_lost",
    "passing_air_yards",
    "passing_yards_after_catch",
    "passing_first_downs",
    "passing_epa",
    "passing_cpoe",
    "passing_2pt_conversions",
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
    "special_teams_tds",
    "def_tackles_solo",
    "def_tackles_with_assist",
    "def_tackle_assists",
    "def_tackles_for_loss",
    "def_tackles_for_loss_yards",
    "def_fumbles_forced",
    "def_sacks",
    "def_sack_yards",
    "def_qb_hits",
    "def_interceptions",
    "def_interception_yards",
    "def_pass_defended",
    "def_tds",
    "def_fumbles",
    "def_safeties",
    "misc_yards",
    "fumble_recovery_own",
    "fumble_recovery_yards_own",
    "fumble_recovery_opp",
    "fumble_recovery_yards_opp",
    "fumble_recovery_tds",
    "penalties",
    "penalty_yards",
    "timeouts",
    "punt_returns",
    "punt_return_yards",
    "kickoff_returns",
    "kickoff_return_yards",
    "fg_made",
    "fg_att",
    "fg_missed",
    "fg_blocked",
    "fg_long",
    "fg_pct",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60_",
    "fg_missed_0_19",
    "fg_missed_20_29",
    "fg_missed_30_39",
    "fg_missed_40_49",
    "fg_missed_50_59",
    "fg_missed_60_",
    "fg_made_list",
    "fg_missed_list",
    "fg_blocked_list",
    "fg_made_distance",
    "fg_missed_distance",
    "fg_blocked_distance",
    "pat_made",
    "pat_att",
    "pat_missed",
    "pat_blocked",
    "pat_pct",
    "gwfg_made",
    "gwfg_att",
    "gwfg_missed",
    "gwfg_blocked",
    "gwfg_distance",
)

# team_stats rate columns whose null must NOT be coerced to 0.
_TEAM_RATE_COLUMNS: tuple[str, ...] = (
    "passing_epa",
    "passing_cpoe",
    "rushing_epa",
    "receiving_epa",
    "fg_pct",
    "pat_pct",
    "fg_long",
)

# team_stats string columns (FG distance lists).
_TEAM_STR_COLUMNS: tuple[str, ...] = ("fg_made_list", "fg_missed_list", "fg_blocked_list")


def _cast_team_keys(frame: pl.DataFrame) -> pl.DataFrame:
    """Coerce the (season, week, team) join keys to stable dtypes.

    Empty / all-null sub-frames infer their ``team`` column as the Null dtype,
    which breaks the full-outer joins; force Utf8 / Int64 so every sub-frame
    composes cleanly.
    """
    return frame.with_columns(
        pl.col("season").cast(pl.Int64, strict=False),
        pl.col("week").cast(pl.Int64, strict=False),
        pl.col("team").cast(pl.Utf8, strict=False),
    )


def _team_offense_frame(data: pl.DataFrame, two_pts: pl.DataFrame) -> pl.DataFrame:
    """Team offense aggregated on ``posteam`` per (season, week)."""
    pass_plays = data.filter(pl.col("play_type").is_in(["pass", "qb_spike"]))
    run_plays = data.filter(pl.col("play_type").is_in(["run", "qb_kneel"]))
    rec_plays = pass_plays  # air-yards / receiving sums span all pass plays, not just receiver-present ones

    passing = pass_plays.group_by(["posteam", "week", "season"]).agg(
        (_i("complete_pass") == 1).sum().alias("completions"),
        ((_i("complete_pass") == 1) | (_i("incomplete_pass") == 1) | (_i("interception") == 1)).sum().alias("attempts"),
        _f("passing_yards").sum().alias("passing_yards"),
        ((_i("touchdown") == 1) & (pl.col("td_team") == pl.col("posteam")) & (_i("complete_pass") == 1))
        .sum()
        .alias("passing_tds"),
        _i("interception").sum().alias("passing_interceptions"),
        _i("sack").sum().alias("sacks_suffered"),
        # nflverse stores sack_yards_lost as a negative number.
        ((_f("yards_gained") * _i("sack")).sum()).alias("sack_yards_lost"),
        ((_i("fumble") == 1) & (_i("sack") == 1)).sum().alias("sack_fumbles"),
        ((_i("fumble_lost") == 1) & (_i("sack") == 1)).sum().alias("sack_fumbles_lost"),
        _f("air_yards").sum().alias("passing_air_yards"),
        ((_f("passing_yards") - _f("air_yards")) * _i("complete_pass")).sum().alias("passing_yards_after_catch"),
        _i("first_down_pass").sum().alias("passing_first_downs"),
        _f("epa").sum().alias("passing_epa"),  # qb_epa absent -> epa fallback
        # cpoe is recorded on dropbacks; mean over pass plays (NA-safe).
        pl.col("cpoe").mean().alias("passing_cpoe"),
    )

    rushing = run_plays.group_by(["posteam", "week", "season"]).agg(
        pl.len().cast(pl.Int64).alias("carries"),
        _f("rushing_yards").sum().alias("rushing_yards"),
        ((_i("touchdown") == 1) & (pl.col("td_team") == pl.col("posteam"))).sum().alias("rushing_tds"),
        (_i("fumble") == 1).sum().alias("rushing_fumbles"),
        (_i("fumble_lost") == 1).sum().alias("rushing_fumbles_lost"),
        _i("first_down_rush").sum().alias("rushing_first_downs"),
        _f("epa").sum().alias("rushing_epa"),
    )

    receiving = rec_plays.group_by(["posteam", "week", "season"]).agg(
        (_i("complete_pass") == 1).sum().alias("receptions"),
        pl.col("receiver_player_id").is_not_null().sum().cast(pl.Int64).alias("targets"),
        _f("receiving_yards").sum().alias("receiving_yards"),
        ((_i("touchdown") == 1) & (pl.col("td_team") == pl.col("posteam")) & (_i("complete_pass") == 1))
        .sum()
        .alias("receiving_tds"),
        ((_i("fumble") == 1) & (_i("complete_pass") == 1)).sum().alias("receiving_fumbles"),
        ((_i("fumble_lost") == 1) & (_i("complete_pass") == 1)).sum().alias("receiving_fumbles_lost"),
        _f("air_yards").sum().alias("receiving_air_yards"),
        _f("yards_after_catch").sum().alias("receiving_yards_after_catch"),
        _i("first_down_pass").sum().alias("receiving_first_downs"),
        # receiving_epa sums epa only over targeted (receiver-present) plays.
        (_f("epa") * pl.col("receiver_player_id").is_not_null().cast(pl.Float64)).sum().alias("receiving_epa"),
    )

    two = two_pts.group_by(["posteam", "week", "season"]).agg(
        ((_i("pass_attempt") == 1) & pl.col("passer_player_id").is_not_null()).sum().alias("passing_2pt_conversions"),
        ((_i("rush_attempt") == 1) & pl.col("rusher_player_id").is_not_null()).sum().alias("rushing_2pt_conversions"),
        ((_i("pass_attempt") == 1) & pl.col("receiver_player_id").is_not_null())
        .sum()
        .alias("receiving_2pt_conversions"),
    )

    out = (
        passing.join(rushing, on=["posteam", "week", "season"], how="full", coalesce=True)
        .join(receiving, on=["posteam", "week", "season"], how="full", coalesce=True)
        .join(two, on=["posteam", "week", "season"], how="full", coalesce=True)
        .rename({"posteam": "team"})
        .filter(pl.col("team").is_not_null())
    )
    return out


def _slot_team_long(pbp: pl.DataFrame, prefix: str, n_slots: int, *, value_name: str) -> pl.DataFrame:
    """Melt team-tagged player slots into a long (season, week, team, value) count.

    For each ``{prefix}_{k}_player_id`` slot, the credited team is the slot's
    own ``{prefix}_{k}_team`` companion when present, otherwise ``defteam``
    (the player is a defender). Returns one (season, week, team) row per slot
    occurrence so a downstream ``len()`` reproduces nflfastR's per-stat-event count.
    """
    frames: list[pl.DataFrame] = []
    for k in range(1, n_slots + 1):
        id_col = f"{prefix}_{k}_player_id"
        if id_col not in pbp.columns:
            continue
        team_col = f"{prefix}_{k}_team"
        team_expr = pl.col(team_col) if team_col in pbp.columns else pl.col("defteam")
        sub = (
            pbp.filter(pl.col(id_col).is_not_null())
            .select(["season", "week", team_expr.alias("team")])
            .filter(pl.col("team").is_not_null())
        )
        frames.append(sub)
    if not frames:
        return pl.DataFrame(schema={"season": pl.Int64, "week": pl.Int64, "team": pl.Utf8, value_name: pl.Int64})
    return pl.concat(frames).group_by(["season", "week", "team"]).agg(pl.len().cast(pl.Int64).alias(value_name))


def _team_defense_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """Team defense aggregated on the tackler's team (slot team tags)."""
    # Tackle stats credited to the player's own team (slot _team tag).
    solo = _slot_team_long(pbp, "solo_tackle", 2, value_name="def_tackles_solo")
    twa = _slot_team_long(pbp, "tackle_with_assist", 2, value_name="def_tackles_with_assist")
    asst = _slot_team_long(pbp, "assist_tackle", 4, value_name="def_tackle_assists")
    # tackle_for_loss / forced_fumble / qb_hit / pass_defense have no _team
    # companion -> credit defteam (defensive player).
    tfl = _slot_team_long(pbp, "tackle_for_loss", 2, value_name="def_tackles_for_loss")
    ff = _slot_team_long(pbp, "forced_fumble_player", 2, value_name="def_fumbles_forced")
    qbh = _slot_team_long(pbp, "qb_hit", 2, value_name="def_qb_hits")
    pdef = _slot_team_long(pbp, "pass_defense", 2, value_name="def_pass_defended")

    # Per-play defense credited to defteam.
    by_def = (
        pbp.group_by(["season", "week", "defteam"])
        .agg(
            # full sacks (sack_player_id) + 0.5 per half-sack slot
            (
                pl.col("sack_player_id").is_not_null().cast(pl.Float64)
                + 0.5 * pl.col("half_sack_1_player_id").is_not_null().cast(pl.Float64)
                + 0.5 * pl.col("half_sack_2_player_id").is_not_null().cast(pl.Float64)
            )
            .sum()
            .alias("def_sacks"),
            # nflverse stores def_sack_yards as positive yards lost by the offense.
            (
                -1.0 * _f("yards_gained") * pl.col("sack_player_id").is_not_null().cast(pl.Float64)
                + -0.5 * _f("yards_gained") * pl.col("half_sack_1_player_id").is_not_null().cast(pl.Float64)
                + -0.5 * _f("yards_gained") * pl.col("half_sack_2_player_id").is_not_null().cast(pl.Float64)
            )
            .sum()
            .alias("def_sack_yards"),
            _i("interception").sum().alias("def_interceptions"),
            (_f("return_yards") * _i("interception")).sum().alias("def_interception_yards"),
            # defensive TD: scoring play credited to defteam (return / interception / fumble TD)
            ((_i("return_touchdown") == 1) & (pl.col("td_team") == pl.col("defteam"))).sum().alias("def_tds"),
            # def_fumbles: a fumble committed by a defensive player (e.g. a muffed
            # recovery) -- credited to defteam via the fumbled-slot team tags below.
            _i("safety").sum().alias("def_safeties"),
        )
        .rename({"defteam": "team"})
    )

    # def_fumbles keyed on the fumbling team == defteam (defender's own fumble).
    fumbled_frames: list[pl.DataFrame] = []
    for k in (1, 2):
        fumbled_team = f"fumbled_{k}_team"
        if fumbled_team not in pbp.columns:
            continue
        sub = pbp.filter(pl.col(fumbled_team) == pl.col("defteam")).select(
            "season", "week", pl.col("defteam").alias("team")
        )
        fumbled_frames.append(sub)
    if fumbled_frames:
        def_fumbles = (
            pl.concat(fumbled_frames)
            .group_by(["season", "week", "team"])
            .agg(pl.len().cast(pl.Int64).alias("def_fumbles"))
        )
    else:
        def_fumbles = pl.DataFrame(
            schema={"season": pl.Int64, "week": pl.Int64, "team": pl.Utf8, "def_fumbles": pl.Int64}
        )

    out = _cast_team_keys(by_def)
    for frame in (solo, twa, asst, tfl, ff, qbh, pdef, def_fumbles):
        out = out.join(_cast_team_keys(frame), on=["season", "week", "team"], how="full", coalesce=True)
    out = out.filter(pl.col("team").is_not_null())
    return out


def _team_kicking_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """Team kicking aggregated on ``posteam`` (FG + PAT, distance buckets)."""
    fg = pbp.filter(_i("field_goal_attempt") == 1)
    made = (pl.col("field_goal_result") == "made") & (_i("field_goal_attempt") == 1)
    miss = (pl.col("field_goal_result") == "missed") & (_i("field_goal_attempt") == 1)
    blk = (pl.col("field_goal_result") == "blocked") & (_i("field_goal_attempt") == 1)
    dist = _f("kick_distance")

    def _bucket(flag: pl.Expr, lo: int, hi: int | None) -> pl.Expr:
        cond = flag & (dist >= lo) if hi is None else flag & (dist >= lo) & (dist <= hi)
        return cond.sum()

    fg_agg = fg.group_by(["posteam", "week", "season"]).agg(
        made.sum().alias("fg_made"),
        _i("field_goal_attempt").sum().alias("fg_att"),
        miss.sum().alias("fg_missed"),
        blk.sum().alias("fg_blocked"),
        pl.when(made.any()).then(dist.filter(made).max()).otherwise(None).alias("fg_long"),
        _bucket(made, 0, 19).alias("fg_made_0_19"),
        _bucket(made, 20, 29).alias("fg_made_20_29"),
        _bucket(made, 30, 39).alias("fg_made_30_39"),
        _bucket(made, 40, 49).alias("fg_made_40_49"),
        _bucket(made, 50, 59).alias("fg_made_50_59"),
        _bucket(made, 60, None).alias("fg_made_60_"),
        _bucket(miss, 0, 19).alias("fg_missed_0_19"),
        _bucket(miss, 20, 29).alias("fg_missed_20_29"),
        _bucket(miss, 30, 39).alias("fg_missed_30_39"),
        _bucket(miss, 40, 49).alias("fg_missed_40_49"),
        _bucket(miss, 50, 59).alias("fg_missed_50_59"),
        _bucket(miss, 60, None).alias("fg_missed_60_"),
        dist.filter(made).cast(pl.Int64).cast(pl.Utf8).str.join(";").alias("fg_made_list"),
        dist.filter(miss).cast(pl.Int64).cast(pl.Utf8).str.join(";").alias("fg_missed_list"),
        dist.filter(blk).cast(pl.Int64).cast(pl.Utf8).str.join(";").alias("fg_blocked_list"),
        dist.filter(made).sum().cast(pl.Int64).alias("fg_made_distance"),
        dist.filter(miss).sum().cast(pl.Int64).alias("fg_missed_distance"),
        dist.filter(blk).sum().cast(pl.Int64).alias("fg_blocked_distance"),
    )

    pat = pbp.filter(_i("extra_point_attempt") == 1)
    pat_agg = pat.group_by(["posteam", "week", "season"]).agg(
        (pl.col("extra_point_result") == "good").sum().alias("pat_made"),
        _i("extra_point_attempt").sum().alias("pat_att"),
        (pl.col("extra_point_result") == "failed").sum().alias("pat_missed"),
        (pl.col("extra_point_result") == "blocked").sum().alias("pat_blocked"),
    )

    out = (
        fg_agg.join(pat_agg, on=["posteam", "week", "season"], how="full", coalesce=True)
        .rename({"posteam": "team"})
        .filter(pl.col("team").is_not_null())
    )
    return out


def _team_misc_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """Returns / penalties / timeouts / fumble-recovery / misc per the play team tag."""
    # Punt + kickoff returns keyed on return_team.
    punts = pbp.filter(
        (_i("punt_attempt") == 1)
        & pl.col("punt_returner_player_id").is_not_null()
        & (_i("punt_fair_catch") == 0)
        & (_i("punt_downed") == 0)
        & (_i("punt_out_of_bounds") == 0)
        & pl.col("return_team").is_not_null()
    )
    punt_ret = punts.group_by(["season", "week", "return_team"]).agg(
        pl.len().cast(pl.Int64).alias("punt_returns"),
        _f("return_yards").sum().cast(pl.Int64).alias("punt_return_yards"),
    )

    kos = pbp.filter(
        (_i("kickoff_attempt") == 1)
        & pl.col("kickoff_returner_player_id").is_not_null()
        & (_i("kickoff_fair_catch") == 0)
        & (_i("kickoff_out_of_bounds") == 0)
        & pl.col("return_team").is_not_null()
    )
    ko_ret = kos.group_by(["season", "week", "return_team"]).agg(
        pl.len().cast(pl.Int64).alias("kickoff_returns"),
        _f("return_yards").sum().cast(pl.Int64).alias("kickoff_return_yards"),
    )

    # Penalties keyed on penalty_team.
    pen = (
        pbp.filter((_i("penalty") == 1) & pl.col("penalty_team").is_not_null())
        .group_by(["season", "week", "penalty_team"])
        .agg(
            pl.len().cast(pl.Int64).alias("penalties"),
            _f("penalty_yards").sum().cast(pl.Int64).alias("penalty_yards"),
        )
        .rename({"penalty_team": "team"})
    )

    # Timeouts keyed on timeout_team.
    to = (
        pbp.filter((_i("timeout") == 1) & pl.col("timeout_team").is_not_null())
        .group_by(["season", "week", "timeout_team"])
        .agg(pl.len().cast(pl.Int64).alias("timeouts"))
        .rename({"timeout_team": "team"})
    )

    # Fumble recoveries (own vs opponent) keyed on the recovering team. "own"
    # means the recovering team also fumbled (recovered its own fumble), matched
    # via the per-slot ``fumbled_{k}_team`` tag.
    fr_frames: list[pl.DataFrame] = []
    for k in (1, 2):
        rec_team = f"fumble_recovery_{k}_team"
        rec_yds = f"fumble_recovery_{k}_yards"
        fumbled_team = f"fumbled_{k}_team"
        if rec_team not in pbp.columns:
            continue
        fumbled_expr = pl.col(fumbled_team) if fumbled_team in pbp.columns else pl.col("posteam")
        sub = pbp.filter(pl.col(rec_team).is_not_null()).select(
            "season",
            "week",
            pl.col(rec_team).alias("rteam"),
            (pl.col(rec_team) == fumbled_expr).alias("is_own"),
            _f(rec_yds).alias("ryds") if rec_yds in pbp.columns else pl.lit(0.0).alias("ryds"),
            ((_i("return_touchdown") == 1) | (_i("touchdown") == 1)).cast(pl.Int64).alias("rtd"),
        )
        fr_frames.append(sub)
    if fr_frames:
        fr_all = pl.concat(fr_frames)
        fr = (
            fr_all.group_by(["season", "week", "rteam"])
            .agg(
                (pl.col("is_own") == True).sum().cast(pl.Int64).alias("fumble_recovery_own"),  # noqa: E712
                pl.col("ryds").filter(pl.col("is_own") == True).sum().cast(pl.Int64).alias("fumble_recovery_yards_own"),  # noqa: E712
                (pl.col("is_own") == False).sum().cast(pl.Int64).alias("fumble_recovery_opp"),  # noqa: E712
                pl.col("ryds")
                .filter(pl.col("is_own") == False)
                .sum()
                .cast(pl.Int64)
                .alias("fumble_recovery_yards_opp"),  # noqa: E712
                pl.col("rtd").sum().cast(pl.Int64).alias("fumble_recovery_tds"),
            )
            .rename({"rteam": "team"})
        )
    else:
        fr = pl.DataFrame(
            schema={
                "season": pl.Int64,
                "week": pl.Int64,
                "team": pl.Utf8,
                "fumble_recovery_own": pl.Int64,
                "fumble_recovery_yards_own": pl.Int64,
                "fumble_recovery_opp": pl.Int64,
                "fumble_recovery_yards_opp": pl.Int64,
                "fumble_recovery_tds": pl.Int64,
            }
        )

    # misc_yards: combined return yards on fumble + lateral recovery plays
    # credited to the recovering team. Approximated as fumble-recovery return yards.
    misc = fr.select(
        "season",
        "week",
        "team",
        (pl.col("fumble_recovery_yards_own") + pl.col("fumble_recovery_yards_opp")).alias("misc_yards"),
    )

    out = _cast_team_keys(punt_ret.rename({"return_team": "team"})).join(
        _cast_team_keys(ko_ret.rename({"return_team": "team"})),
        on=["season", "week", "team"],
        how="full",
        coalesce=True,
    )
    for frame in (pen, to, fr, misc):
        out = out.join(_cast_team_keys(frame), on=["season", "week", "team"], how="full", coalesce=True)
    return out.filter(pl.col("team").is_not_null())


def _empty_team_stats(*, weekly: bool, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Return a zero-row team_stats frame carrying the canonical schema."""
    cols = list(_TEAM_OUTPUT_COLUMNS)
    if not weekly:
        cols = [c for c in cols if c not in ("week", "season_type", "opponent_team")]
        cols.insert(cols.index("team") + 1, "games")
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    str_cols = {"team", "season_type", "opponent_team", *_TEAM_STR_COLUMNS}
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
def build_nfl_team_stats(
    seasons: List[int],
    *,
    summary_level: str = ...,
    season_type: str = ...,
    source: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def build_nfl_team_stats(
    seasons: List[int],
    *,
    summary_level: str = ...,
    season_type: str = ...,
    source: str = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def build_nfl_team_stats(
    seasons: List[int],
    *,
    summary_level: str = "week",
    season_type: str = "REG",
    source: str = "sdv",
    return_as_pandas: bool = False,
) -> pl.DataFrame | "pd.DataFrame":
    """Build nflverse **team_stats** by aggregating SDV-native play-by-play.

    A faithful polars port of nflfastR's ``calculate_stats(stat_type = "team")``
    (the ``aggregate_game_stats*`` family). Offense is keyed on ``posteam``,
    defense on the tackler's team (per-play ``*_team`` slot tags -- NOT
    ``defteam``, which double-counts on return plays), kicking on ``posteam``,
    and returns / penalties / timeouts on the relevant play team tag. See the
    module docstring for the full grouping + SDV-PBP gap notes (notably
    ``passing_epa`` falls back to ``epa`` because ``qb_epa`` is absent).

    Args:
        seasons: Four-digit NFL seasons to aggregate (e.g. ``[2023]``).
        summary_level: ``"week"`` (group on season + week + team, with
            ``opponent_team``) or ``"season"`` (group on season + team, with a
            ``games`` distinct-game count replacing week / season_type /
            opponent_team).
        season_type: ``"REG"``, ``"POST"``, or ``"REG+POST"``. Pre-filters the
            play-by-play before aggregation.
        source: Play-by-play release passed to :func:`load_nfl_pbp`. Defaults to
            ``"sdv"`` (the SDV-native enriched release).
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A polars (or pandas) DataFrame in the published ``load_nfl_team_stats``
        schema (~102 columns). At ``summary_level="season"`` the ``week`` /
        ``season_type`` / ``opponent_team`` columns are replaced by a ``games``
        column.

    Raises:
        ValueError: If ``summary_level`` is not ``"week"``/``"season"`` or
            ``season_type`` is not ``"REG"``/``"POST"``/``"REG+POST"``.

    Example:
        Weekly team stats for 2023::

            from sportsdataverse.nfl import build_nfl_team_stats
            wk = build_nfl_team_stats([2023], summary_level="week")
            print(wk.shape)

        Season totals as pandas::

            df_pd = build_nfl_team_stats([2023], summary_level="season",
                                         return_as_pandas=True)

        Pipeline next step (one line)::

            wk.sort("def_sacks", descending=True).head()

    See Also:
        * `nflfastR <https://www.nflfastr.com>`_ -- the ``calculate_stats`` source
        * `nflreadpy <https://github.com/nflverse/nflreadpy>`_ -- nflverse loaders (Python)
    """
    if summary_level not in ("week", "season"):
        raise ValueError(f"Invalid summary_level {summary_level!r}; expected 'week' or 'season'.")
    if season_type not in ("REG", "POST", "REG+POST"):
        raise ValueError(f"Invalid season_type {season_type!r}; expected 'REG', 'POST', or 'REG+POST'.")

    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp  # noqa: PLC0415

    pbp = load_nfl_pbp(seasons, source=source)
    # The SDV-native PBP (source="sdv") omits a few special-teams return-detail
    # flags the full nflverse PBP carries; default them to 0 so the return-count
    # logic still runs. Return YARDS are unaffected; on source="sdv" the punt /
    # kickoff return COUNTS may include fair catches / downed punts — a documented
    # minor limitation (those flags are not in the Shield-built nfl_model_pbp).
    _missing_st = [
        c
        for c in (
            "punt_fair_catch",
            "punt_downed",
            "punt_out_of_bounds",
            "kickoff_fair_catch",
            "kickoff_out_of_bounds",
        )
        if c not in pbp.columns
    ]
    if _missing_st:
        pbp = pbp.with_columns([pl.lit(0, dtype=pl.Int64).alias(c) for c in _missing_st])

    if season_type in ("REG", "POST"):
        pbp = pbp.filter(pl.col("season_type") == season_type)
    if pbp.height == 0:
        return _empty_team_stats(weekly=summary_level == "week", return_as_pandas=return_as_pandas)

    pbp = _prepare_pbp(pbp)

    # Plays counting toward official offensive stats (down present).
    data = pbp.filter(pl.col("down").is_not_null() & pl.col("play_type").is_in(["pass", "qb_kneel", "qb_spike", "run"]))
    two_pts = pbp.filter((pl.col("two_point_attempt") == 1) & (pl.col("sp") == 1))

    def _keys(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.col("team").cast(pl.Utf8, strict=False),
            pl.col("week").cast(pl.Int64, strict=False),
            pl.col("season").cast(pl.Int64, strict=False),
        )

    offense = _keys(_team_offense_frame(data, two_pts))
    defense = _keys(_team_defense_frame(pbp))
    kicking = _keys(_team_kicking_frame(pbp))
    misc = _keys(_team_misc_frame(pbp))

    # special-teams TDs per (team, week) -- reuse the player-level helper keyed on td_team.
    st_tds = (
        pbp.filter((pl.col("special") == 1) & (_i("touchdown") == 1) & pl.col("td_team").is_not_null())
        .group_by(["season", "week", "td_team"])
        .agg(_i("touchdown").sum().alias("special_teams_tds"))
        .rename({"td_team": "team"})
    )
    st_tds = _keys(st_tds)

    team_df = offense
    for frame in (defense, kicking, misc, st_tds):
        team_df = team_df.join(frame, on=["team", "week", "season"], how="full", coalesce=True)

    # opponent_team + season_type per (season, week, team) from the schedule grain.
    sched = (
        pbp.filter(pl.col("posteam").is_not_null() & pl.col("defteam").is_not_null())
        .select(
            ["season", "week", "season_type", pl.col("posteam").alias("team"), pl.col("defteam").alias("opponent_team")]
        )
        .unique(subset=["season", "week", "team"], keep="first")
    )
    team_df = team_df.join(sched, on=["season", "week", "team"], how="left")

    team_df = team_df.filter(pl.col("team").is_not_null())

    # Fill counting columns to 0; keep rate columns null.
    fill_cols = [
        c
        for c in _TEAM_OUTPUT_COLUMNS
        if c not in _TEAM_RATE_COLUMNS
        and c not in _TEAM_STR_COLUMNS
        and c not in ("season", "week", "team", "season_type", "opponent_team")
        and c in team_df.columns
    ]
    team_df = team_df.with_columns([pl.col(c).fill_null(0) for c in fill_cols])
    # String lists default to empty string.
    team_df = team_df.with_columns([pl.col(c).fill_null("") for c in _TEAM_STR_COLUMNS if c in team_df.columns])

    # FG / PAT percentages (avoid 0/0).
    team_df = team_df.with_columns(
        pl.when(pl.col("fg_att") > 0)
        .then(pl.col("fg_made") / pl.col("fg_att"))
        .otherwise(None)
        .round(3)
        .alias("fg_pct"),
        pl.when(pl.col("pat_att") > 0).then(pl.col("pat_made") / pl.col("pat_att")).otherwise(None).alias("pat_pct"),
    )

    # gwfg_* columns are not derivable from PBP (no game-winning-FG flag); ship 0.
    for c in ("gwfg_made", "gwfg_att", "gwfg_missed", "gwfg_blocked", "gwfg_distance"):
        if c not in team_df.columns:
            team_df = team_df.with_columns(pl.lit(0).cast(pl.Int64).alias(c))

    if summary_level == "season":
        team_df = _collapse_team_to_season(team_df, season_type=season_type)

    return _finalize_team(team_df, weekly=summary_level == "week", return_as_pandas=return_as_pandas)


def _collapse_team_to_season(team_df: pl.DataFrame, *, season_type: str) -> pl.DataFrame:
    """Collapse week-level team rows to season totals grouped on (season, team)."""
    sum_cols = [
        c
        for c in _TEAM_OUTPUT_COLUMNS
        if c not in _TEAM_RATE_COLUMNS
        and c not in _TEAM_STR_COLUMNS
        and c not in ("season", "week", "team", "season_type", "opponent_team", "fg_long")
        and c in team_df.columns
    ]
    agg = [pl.len().alias("games")]
    agg += [pl.col(c).sum().alias(c) for c in sum_cols]
    agg += [
        pl.col("passing_epa").sum().alias("passing_epa"),
        pl.col("rushing_epa").sum().alias("rushing_epa"),
        pl.col("receiving_epa").sum().alias("receiving_epa"),
        pl.col("passing_cpoe").mean().alias("passing_cpoe"),
        pl.col("fg_long").max().alias("fg_long"),
    ]
    agg += [pl.col(c).str.join(";").alias(c) for c in _TEAM_STR_COLUMNS if c in team_df.columns]

    summed = (
        team_df.group_by(["season", "team"])
        .agg(agg)
        .with_columns(
            pl.when(pl.col("fg_att") > 0)
            .then(pl.col("fg_made") / pl.col("fg_att"))
            .otherwise(None)
            .round(3)
            .alias("fg_pct"),
            pl.when(pl.col("pat_att") > 0)
            .then(pl.col("pat_made") / pl.col("pat_att"))
            .otherwise(None)
            .alias("pat_pct"),
        )
    )
    return summed


def _finalize_team(team_df: pl.DataFrame, *, weekly: bool, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Select the canonical team_stats column order + sort."""
    cols = list(_TEAM_OUTPUT_COLUMNS)
    if not weekly:
        cols = [c for c in cols if c not in ("week", "season_type", "opponent_team")]
        cols.insert(cols.index("team") + 1, "games")
    for c in cols:
        if c not in team_df.columns:
            team_df = team_df.with_columns(pl.lit(None).alias(c))
    out = team_df.select(cols).filter(pl.col("team").is_not_null())
    sort_keys = ["team", "season"] + (["week"] if weekly else [])
    out = out.sort(sort_keys)
    if return_as_pandas:
        return out.to_pandas()
    return out
