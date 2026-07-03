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

- ``passing_epa`` uses the exact ``qb_epa`` (QB-credited EPA, matching
  nflfastR's ``sum(qb_epa)``); ``rushing_epa`` / ``receiving_epa`` use ``epa``
  (correct per nflfastR). ``passing_epa`` (and the linear ``dakota`` it feeds)
  still carries the intrinsic EP-model drift (~0.99 corr) but the credited-EPA
  logic now matches nflfastR exactly.
- ``def_sacks`` / ``def_sack_yards`` use the ``sack_player_id`` (full) +
  ``half_sack_{1,2}`` (0.5 each) attribution -- exact vs nflverse.
- ``def_tackles_for_loss`` (+ ``_yards``) sum the exact per-play stat-id-402
  count/yards on defteam; ``def_tds`` uses the exact td-stat-id count tagged to
  defteam (``special != 1``), capturing fumble-return / blocked-kick TDs;
  ``misc_yards`` uses the exact stat-id-63:64 column; ``fumble_recovery_yards``
  include the lateral-recovery yards (stat-ids 57:58 / 61:62).
- ``gwfg_*`` mirror nflfastR ``is_gwfg_attempt`` via ``fixed_drive`` (FG attempt
  in the posteam's final drive, trailing by <=2, defense scores no more after).

SDV-PBP column gaps vs nflfastR (and how they are handled here):
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


_NEW_PBP_COLS: tuple[str, ...] = (
    "qb_epa",
    "fixed_drive",
    "def_tackles_for_loss",
    "def_tackles_for_loss_yards",
    "td_ids_touchdown",
    "misc_yards",
    "fumble_recovery_own_lateral_yards",
    "fumble_recovery_opp_lateral_yards",
)


def _ensure_new_cols(pbp: pl.DataFrame) -> pl.DataFrame:
    """Backfill the newer SDV-PBP columns when an older/partial source omits them.

    ``qb_epa`` falls back to ``epa`` (its value on the vast majority of plays);
    ``fixed_drive`` backfills to NULL (NOT 0) so that a source lacking it does
    not make every row look like the game's final drive — ``_team_gwfg_frame``
    keys GWFG off ``fixed_drive == max(fixed_drive)``, and an all-zero column
    would flag every qualifying FG attempt as a game-winner; a null column makes
    ``max`` null so no row qualifies (GWFG degrades to empty, not over-counted).
    The remaining count/yardage columns default to 0 so the exact-stat-id logic
    degrades gracefully to a zero contribution rather than raising.
    """
    add: list[pl.Expr] = []
    for c in _NEW_PBP_COLS:
        if c in pbp.columns:
            continue
        if c == "qb_epa":
            src = pl.col("epa") if "epa" in pbp.columns else pl.lit(0.0)
            add.append(src.cast(pl.Float64, strict=False).alias("qb_epa"))
        elif c == "fixed_drive":
            add.append(pl.lit(None, dtype=pl.Int64).alias("fixed_drive"))
        else:
            add.append(pl.lit(0, dtype=pl.Int64).alias(c))
    return pbp.with_columns(add) if add else pbp


def _prepare_pbp(pbp: pl.DataFrame) -> pl.DataFrame:
    """Add the derived ``special`` flag and backfill newer PBP columns."""
    pbp = _ensure_new_cols(pbp)
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
            _f("qb_epa").sum().alias("passing_epa"),  # exact qb_epa (QB-credited EPA), matches nflfastR
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
    SDV-PBP column-gap handling (``passing_epa`` uses the exact ``qb_epa``;
    ``rushing_epa`` / ``receiving_epa`` use plain ``epa`` per nflfastR).

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
        # dakota = 0.816*(passing_epa/attempts) + 0.184*passing_cpoe (linear approx;
        # passing_epa = sum(qb_epa), so dakota recomputes off the exact credited EPA)
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
        _f("qb_epa").sum().alias("passing_epa"),  # exact qb_epa (QB-credited EPA), matches nflfastR
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
    # forced_fumble / qb_hit / pass_defense have no _team companion -> credit
    # defteam (defensive player). tackle_for_loss is now summed exactly per-play
    # (stat_id-402 count) on defteam in ``by_def`` below.
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
            # exact per-play tackle-for-loss count + yards (stat_id 402), credited to defteam.
            _f("def_tackles_for_loss").sum().cast(pl.Int64).alias("def_tackles_for_loss"),
            _f("def_tackles_for_loss_yards").sum().cast(pl.Int64).alias("def_tackles_for_loss_yards"),
            # defensive TD: any TD credited to defteam (return / interception / fumble-return /
            # blocked-kick), excluding special-teams plays. Uses the exact td-stat-id count.
            (_i("td_ids_touchdown") * ((pl.col("td_team") == pl.col("defteam")) & (_i("special") != 1)).cast(pl.Int64))
            .sum()
            .alias("def_tds"),
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
    for frame in (solo, twa, asst, ff, qbh, pdef, def_fumbles):
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


def _team_gwfg_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """Game-winning field-goal attempts aggregated on ``posteam`` per (season, week).

    Mirrors nflfastR ``is_gwfg_attempt`` (calculate_stats.R): a FG attempt that is
    in the posteam's final drive of the game (``fixed_drive == max(fixed_drive)``
    over the game for that posteam), where the posteam trailed by 2 points or less
    prior to the kick (``score_differential`` in ``[-2, 0]``), and the defense did
    not score afterward (per-play ``defteam_score`` equals the game-final defteam
    score). gwfg_made/att/missed/blocked come from ``field_goal_result`` on those
    attempts; gwfg_distance sums ``kick_distance``.
    """
    empty = pl.DataFrame(
        schema={
            "season": pl.Int64,
            "week": pl.Int64,
            "team": pl.Utf8,
            "gwfg_made": pl.Int64,
            "gwfg_att": pl.Int64,
            "gwfg_missed": pl.Int64,
            "gwfg_blocked": pl.Int64,
            "gwfg_distance": pl.Int64,
        }
    )
    needed = {
        "game_id",
        "posteam",
        "defteam",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
        "fixed_drive",
        "score_differential",
        "defteam_score",
        "field_goal_attempt",
    }
    if not needed.issubset(pbp.columns):
        return empty

    # final defteam score == away_score when posteam is home, else home_score.
    work = pbp.filter(pl.col("posteam").is_not_null() & pl.col("defteam").is_not_null()).with_columns(
        pl.when(pl.col("posteam") == pl.col("home_team"))
        .then(pl.col("away_score"))
        .otherwise(pl.col("home_score"))
        .alias("_final_defteam_score"),
    )
    # max fixed_drive over the game for each posteam.
    work = work.with_columns(
        pl.col("fixed_drive").max().over(["game_id", "posteam"]).alias("_max_fixed_drive"),
    )
    gwfg = work.filter(
        (_i("field_goal_attempt") == 1)
        & (pl.col("fixed_drive") == pl.col("_max_fixed_drive"))
        & (_f("score_differential") >= -2)
        & (_f("score_differential") <= 0)
        & (_f("defteam_score") == _f("_final_defteam_score"))
    )
    if gwfg.height == 0:
        return empty

    made = pl.col("field_goal_result") == "made"
    miss = pl.col("field_goal_result") == "missed"
    blk = pl.col("field_goal_result") == "blocked"
    out = (
        gwfg.group_by(["posteam", "week", "season"])
        .agg(
            made.sum().cast(pl.Int64).alias("gwfg_made"),
            pl.len().cast(pl.Int64).alias("gwfg_att"),
            miss.sum().cast(pl.Int64).alias("gwfg_missed"),
            blk.sum().cast(pl.Int64).alias("gwfg_blocked"),
            _f("kick_distance").sum().cast(pl.Int64).alias("gwfg_distance"),
        )
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
        base_yds = _f(rec_yds) if rec_yds in pbp.columns else pl.lit(0.0)
        sub = pbp.filter(pl.col(rec_team).is_not_null()).select(
            "season",
            "week",
            pl.col(rec_team).alias("rteam"),
            (pl.col(rec_team) == fumbled_expr).alias("is_own"),
            base_yds.alias("ryds"),
            ((_i("return_touchdown") == 1) | (_i("touchdown") == 1)).cast(pl.Int64).alias("rtd"),
        )
        fr_frames.append(sub)
    if fr_frames:
        fr_all = pl.concat(fr_frames)
        fr = (
            fr_all.group_by(["season", "week", "rteam"])
            .agg(
                (pl.col("is_own") == True).sum().cast(pl.Int64).alias("fumble_recovery_own"),  # noqa: E712
                pl.col("ryds").filter(pl.col("is_own") == True).sum().alias("fumble_recovery_yards_own"),  # noqa: E712
                (pl.col("is_own") == False).sum().cast(pl.Int64).alias("fumble_recovery_opp"),  # noqa: E712
                pl.col("ryds").filter(pl.col("is_own") == False).sum().alias("fumble_recovery_yards_opp"),  # noqa: E712
                pl.col("rtd").sum().cast(pl.Int64).alias("fumble_recovery_tds"),
            )
            .rename({"rteam": "team"})
        )
        # Lateral-recovery yards (stat_ids 57:58 own / 61:62 opp) are play-level
        # totals already split own/opp, credited to the recovering team
        # (``fumble_recovery_1_team``). Add them ONCE (not per slot) to match
        # nflfastR's fumble_recovery_yards_{own,opp}.
        if "fumble_recovery_own_lateral_yards" in pbp.columns and "fumble_recovery_1_team" in pbp.columns:
            lat = (
                pbp.filter(pl.col("fumble_recovery_1_team").is_not_null())
                .group_by(["season", "week", "fumble_recovery_1_team"])
                .agg(
                    _f("fumble_recovery_own_lateral_yards").sum().alias("_own_lat"),
                    _f("fumble_recovery_opp_lateral_yards").sum().alias("_opp_lat"),
                )
                .rename({"fumble_recovery_1_team": "team"})
            )
            fr = (
                fr.join(lat, on=["season", "week", "team"], how="full", coalesce=True)
                .with_columns(
                    (pl.col("fumble_recovery_yards_own").fill_null(0) + pl.col("_own_lat").fill_null(0)).alias(
                        "fumble_recovery_yards_own"
                    ),
                    (pl.col("fumble_recovery_yards_opp").fill_null(0) + pl.col("_opp_lat").fill_null(0)).alias(
                        "fumble_recovery_yards_opp"
                    ),
                )
                .drop("_own_lat", "_opp_lat")
            )
        fr = fr.with_columns(
            pl.col("fumble_recovery_yards_own").cast(pl.Int64),
            pl.col("fumble_recovery_yards_opp").cast(pl.Int64),
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

    # misc_yards (stat_ids 63:64): mostly yards gained after blocked punts / FGs,
    # credited to the team that returned the block (the kicking team's opponent ==
    # defteam on these kick plays). Uses the exact per-play ``misc_yards`` column.
    if "misc_yards" in pbp.columns:
        misc = (
            pbp.filter((_f("misc_yards") != 0) & pl.col("defteam").is_not_null())
            .group_by(["season", "week", "defteam"])
            .agg(_f("misc_yards").sum().cast(pl.Int64).alias("misc_yards"))
            .rename({"defteam": "team"})
        )
    else:
        misc = pl.DataFrame(schema={"season": pl.Int64, "week": pl.Int64, "team": pl.Utf8, "misc_yards": pl.Int64})

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
    module docstring for the full grouping + SDV-PBP gap notes (``passing_epa``
    uses the exact ``qb_epa``; ``gwfg_*`` derive from ``fixed_drive``).

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
    # The SDV-native nfl_model_pbp now carries the special-teams return-detail
    # flags (punt/kickoff fair_catch/downed/out_of_bounds), so the punt/kickoff
    # return-count exclusion filters run with the real flags and the counts are
    # exact. This guard is a harmless fallback for any older/partial PBP source
    # missing one of these columns.
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
    gwfg = _keys(_team_gwfg_frame(pbp))
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
    for frame in (defense, kicking, gwfg, misc, st_tds):
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

    # gwfg_* columns come from _team_gwfg_frame; ensure they exist for the
    # no-attempt edge case (fill_null(0) below zero-fills weeks without one).
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


# ---------------------------------------------------------------------------
# player_stats_def (nflfastR ``calculate_player_stats_def`` parity)
# ---------------------------------------------------------------------------
#
# Unlike :func:`build_nfl_player_stats` / :func:`build_nfl_team_stats` (which
# load their own PBP via ``load_nfl_pbp``), these two builders take a
# caller-supplied ``pbp`` frame directly -- matching the (deprecated, but
# still the parity target) nflfastR functions ``calculate_player_stats_def()``
# and ``calculate_player_stats_kicking()``, which both accept a ``pbp``
# argument rather than loading data themselves.
#
# IMPORTANT season-collapse quirk (transcribed verbatim from the R source):
# both R functions group the ``weekly=FALSE`` aggregate on ``(player_id,
# team)`` ONLY -- *not* ``season`` -- so passing multiple seasons of PBP with
# ``weekly=False`` folds them into a single row per player/team. This matches
# nflfastR's shipped (if surprising) behavior for these specific deprecated
# functions; callers who need a season column should pre-filter ``pbp`` to one
# season before calling with ``weekly=False``.

_DEF_OUTPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "week",
    "season_type",
    "player_id",
    "player_name",
    "player_display_name",
    "position",
    "position_group",
    "headshot_url",
    "team",
    "def_tackles",
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
    "def_fumble_recovery_own",
    "def_fumble_recovery_yards_own",
    "def_fumble_recovery_opp",
    "def_fumble_recovery_yards_opp",
    "def_safety",
    "def_penalty",
    "def_penalty_yards",
)

_DEF_JOIN_KEYS: tuple[str, str, str, str] = ("season", "week", "team", "player_id")

_DEF_EMPTY_KEY_SCHEMA: dict[str, type[pl.DataType] | pl.DataType] = {
    "season": pl.Int64,
    "week": pl.Int64,
    "team": pl.Utf8,
    "player_id": pl.Utf8,
}


def _empty_player_stats_def(*, weekly: bool, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Return a zero-row def-stats frame carrying the canonical schema."""
    cols = list(_DEF_OUTPUT_COLUMNS)
    if not weekly:
        # Season grain drops season too (grouped on player_id/team only).
        cols = [c for c in cols if c not in ("season", "week", "season_type")]
        cols.insert(cols.index("player_display_name") + 1, "games")
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    str_cols = {
        "player_id",
        "player_name",
        "player_display_name",
        "position",
        "position_group",
        "headshot_url",
        "team",
        "season_type",
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


def _def_empty(value_cols: dict[str, type[pl.DataType] | pl.DataType]) -> pl.DataFrame:
    """A zero-row (season, week, team, player_id) frame plus the given extra value columns."""
    schema = dict(_DEF_EMPTY_KEY_SCHEMA)
    schema.update(value_cols)
    return pl.DataFrame(schema=schema)


def _def_cast_keys(frame: pl.DataFrame) -> pl.DataFrame:
    """Cast the four def join-key columns to their canonical dtypes.

    Root-cause guard: a synthetic/real pbp column that is entirely null (e.g.
    a slot-2 id column with no occurrences in a given batch) infers as
    polars ``Null`` dtype rather than ``Utf8``/``Int64``. That ``Null``
    survives through ``filter``/``group_by`` on an otherwise-empty frame and
    then breaks a downstream ``.join(..., how="full")`` against a sibling
    frame whose keys did get real values (``SchemaError: datatypes of join
    keys don't match``). Casting at the single join choke point
    (:func:`_def_join`) rather than chasing every sub-frame constructor keeps
    every def sub-frame's keys pinned to the schema documented in
    ``_DEF_EMPTY_KEY_SCHEMA``.
    """
    return frame.with_columns(
        pl.col("season").cast(pl.Int64, strict=False),
        pl.col("week").cast(pl.Int64, strict=False),
        pl.col("team").cast(pl.Utf8, strict=False),
        pl.col("player_id").cast(pl.Utf8, strict=False),
    )


def _def_join(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
    """Full-outer join two def sub-frames on ``_DEF_JOIN_KEYS``, key dtypes guaranteed."""
    return _def_cast_keys(left).join(_def_cast_keys(right), on=list(_DEF_JOIN_KEYS), how="full", coalesce=True)


def _def_slot_frame(data: pl.DataFrame, id_col: str) -> pl.DataFrame:
    """One row per non-null ``id_col`` occurrence, keyed on (season, week, team=defteam, player_id)."""
    if id_col not in data.columns:
        return _def_empty({})
    return (
        data.filter(pl.col(id_col).is_not_null())
        .select("season", "week", pl.col("defteam").alias("team"), pl.col(id_col).alias("player_id"))
        .filter(pl.col("team").is_not_null())
    )


def _def_count(frames: List[pl.DataFrame], *, value_name: str) -> pl.DataFrame:
    """Count rows per (season, week, team, player_id) across the union of ``frames``."""
    present = [f for f in frames if f.width > 0]
    if not present:
        return _def_empty({value_name: pl.Float64})
    return _def_cast_keys(
        pl.concat(present, how="vertical_relaxed")
        .group_by(list(_DEF_JOIN_KEYS))
        .agg(pl.len().cast(pl.Float64).alias(value_name))
    )


def _def_tackle_frame(data: pl.DataFrame) -> pl.DataFrame:
    """Tackle-family counts, transcribed from nflfastR's ``tackle_vars`` pivot.

    Per the R ``calculate_player_stats_def`` source, ``tackle_vars`` only lists
    slot 1 for ``tackle_with_assist`` and ``tackle_for_loss`` (the count -- see
    :func:`_def_tackle_for_loss_yards` for the yards, which DOES use both
    slots), while ``solo_tackle``, ``assist_tackle``, and
    ``forced_fumble_player`` count both slots 1 and 2. This intentionally
    diverges from the team-level :func:`_team_defense_frame` (which counts up
    to 4 ``assist_tackle`` slots and both ``tackle_with_assist`` slots to match
    the real data shape) -- this function instead transcribes the literal,
    narrower ``tackle_vars`` list from the deprecated ``calculate_player_stats_def``
    R source being ported here.
    """
    solo = _def_count(
        [_def_slot_frame(data, "solo_tackle_1_player_id"), _def_slot_frame(data, "solo_tackle_2_player_id")],
        value_name="def_tackles_solo",
    )
    twa = _def_count([_def_slot_frame(data, "tackle_with_assist_1_player_id")], value_name="def_tackles_with_assist")
    asst = _def_count(
        [_def_slot_frame(data, "assist_tackle_1_player_id"), _def_slot_frame(data, "assist_tackle_2_player_id")],
        value_name="def_tackle_assists",
    )
    tfl = _def_count([_def_slot_frame(data, "tackle_for_loss_1_player_id")], value_name="def_tackles_for_loss")
    ff = _def_count(
        [
            _def_slot_frame(data, "forced_fumble_player_1_player_id"),
            _def_slot_frame(data, "forced_fumble_player_2_player_id"),
        ],
        value_name="def_fumbles_forced",
    )

    out = solo
    for frame in (twa, asst, tfl, ff):
        out = _def_join(out, frame)
    fill_cols = [
        "def_tackles_solo",
        "def_tackles_with_assist",
        "def_tackle_assists",
        "def_tackles_for_loss",
        "def_fumbles_forced",
    ]
    out = out.with_columns([pl.col(c).fill_null(0.0) for c in fill_cols])
    return out.with_columns((pl.col("def_tackles_solo") + pl.col("def_tackles_with_assist")).alias("def_tackles"))


def _def_tackle_for_loss_yards(data: pl.DataFrame) -> pl.DataFrame:
    """``def_tackles_for_loss_yards`` = ``sum(-yards_gained)`` over TFL plays, both slots.

    Gated to ``tackled_for_loss==1 & fumble==0 & sack==0`` per the R source
    (a TFL that's also a fumble or sack is excluded here to avoid double
    counting with the sack-yards / fumble-yards ledgers).
    """
    needed = {"tackled_for_loss", "fumble", "sack", "yards_gained"}
    if not needed.issubset(data.columns):
        return _def_empty({"def_tackles_for_loss_yards": pl.Float64})
    base = data.filter((_i("tackled_for_loss") == 1) & (_i("fumble") == 0) & (_i("sack") == 0))
    frames = []
    for k in (1, 2):
        col = f"tackle_for_loss_{k}_player_id"
        if col not in base.columns:
            continue
        frames.append(
            base.filter(pl.col(col).is_not_null()).select(
                "season",
                "week",
                pl.col("defteam").alias("team"),
                pl.col(col).alias("player_id"),
                (-1.0 * _f("yards_gained")).alias("def_tackles_for_loss_yards"),
            )
        )
    if not frames:
        return _def_empty({"def_tackles_for_loss_yards": pl.Float64})
    return (
        pl.concat(frames, how="vertical_relaxed")
        .group_by(list(_DEF_JOIN_KEYS))
        .agg(pl.col("def_tackles_for_loss_yards").sum())
    )


def _def_sack_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_sacks`` / ``def_sack_yards`` -- full sack weight 1.0, each half-sack slot 0.5."""
    parts = []
    if "sack_player_id" in data.columns:
        parts.append(
            data.filter(pl.col("sack_player_id").is_not_null()).select(
                "season",
                "week",
                pl.col("defteam").alias("team"),
                pl.col("sack_player_id").alias("player_id"),
                pl.lit(1.0).alias("n"),
                (-1.0 * _f("yards_gained")).alias("sack_yards"),
            )
        )
    for k in (1, 2):
        col = f"half_sack_{k}_player_id"
        if col in data.columns:
            parts.append(
                data.filter(pl.col(col).is_not_null()).select(
                    "season",
                    "week",
                    pl.col("defteam").alias("team"),
                    pl.col(col).alias("player_id"),
                    pl.lit(0.5).alias("n"),
                    (-0.5 * _f("yards_gained")).alias("sack_yards"),
                )
            )
    if not parts:
        return _def_empty({"def_sacks": pl.Float64, "def_sack_yards": pl.Float64})
    return (
        pl.concat(parts, how="vertical_relaxed")
        .group_by(list(_DEF_JOIN_KEYS))
        .agg(pl.col("n").sum().alias("def_sacks"), pl.col("sack_yards").sum().alias("def_sack_yards"))
    )


def _def_int_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_interceptions`` / ``def_pass_defended`` / ``def_interception_yards``."""
    int_count = _def_count([_def_slot_frame(data, "interception_player_id")], value_name="def_interceptions")
    pdef = _def_count(
        [_def_slot_frame(data, "pass_defense_1_player_id"), _def_slot_frame(data, "pass_defense_2_player_id")],
        value_name="def_pass_defended",
    )
    if "interception_player_id" in data.columns and "return_yards" in data.columns:
        int_yards = (
            data.filter(pl.col("interception_player_id").is_not_null())
            .group_by(
                ["season", "week", pl.col("defteam").alias("team"), pl.col("interception_player_id").alias("player_id")]
            )
            .agg(_f("return_yards").sum().alias("def_interception_yards"))
        )
    else:
        int_yards = _def_empty({"def_interception_yards": pl.Float64})
    out = _def_join(int_count, pdef)
    return _def_join(out, int_yards)


def _def_safety_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_safety`` -- count of ``safety==1`` rows keyed on ``safety_player_id``."""
    if not {"safety", "safety_player_id"}.issubset(data.columns):
        return _def_empty({"def_safety": pl.Float64})
    return (
        data.filter((_i("safety") == 1) & pl.col("safety_player_id").is_not_null())
        .group_by(["season", "week", pl.col("defteam").alias("team"), pl.col("safety_player_id").alias("player_id")])
        .agg(pl.len().cast(pl.Float64).alias("def_safety"))
    )


def _def_fumble_own_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_fumbles`` + ``def_fumble_recovery_own`` (the defense fumbled its own ball).

    Ported from nflfastR's ``fumble_df_own``: restricted to rows where the
    defense itself fumbled (``defteam`` matches either ``fumbled_{1,2}_team``
    slot), then split per-slot into "who fumbled" (``def_fumbles``) and "who on
    ``defteam`` recovered it back" (``def_fumble_recovery_own``), each slot
    independently gated on its own ``_team`` match. The R source only
    conditionally nulls the *slot-1* ``fumbled_1_player_id`` in this block
    (leaving ``fumbled_2_player_id`` unguarded) -- an asymmetry NOT called out
    as a required-replicate gotcha in the reference (unlike the flagged
    ``fumble_yds_opp_data`` guard in :func:`_def_fumble_yards_opp_frame`
    below); transcribed here as the documented, slot-symmetric formula-table
    semantics instead, for maintainability.
    """
    fumbled_cols = [f"fumbled_{k}_team" for k in (1, 2) if f"fumbled_{k}_team" in data.columns]
    if not fumbled_cols:
        return _def_empty({"def_fumbles": pl.Float64, "def_fumble_recovery_own": pl.Float64})

    own_cond = pl.lit(False)
    for col in fumbled_cols:
        own_cond = own_cond | (pl.col("defteam") == pl.col(col))
    own_fum = data.filter(((_i("fumble") == 1) | (_i("fumble_lost") == 1)) & own_cond)

    fumbled_frames = []
    recovery_frames = []
    for k in (1, 2):
        f_team, f_id = f"fumbled_{k}_team", f"fumbled_{k}_player_id"
        if f_team in own_fum.columns and f_id in own_fum.columns:
            fumbled_frames.append(
                own_fum.filter((pl.col(f_team) == pl.col("defteam")) & pl.col(f_id).is_not_null()).select(
                    "season", "week", pl.col("defteam").alias("team"), pl.col(f_id).alias("player_id")
                )
            )
        r_team, r_id = f"fumble_recovery_{k}_team", f"fumble_recovery_{k}_player_id"
        if r_team in own_fum.columns and r_id in own_fum.columns:
            recovery_frames.append(
                own_fum.filter((pl.col(r_team) == pl.col("defteam")) & pl.col(r_id).is_not_null()).select(
                    "season", "week", pl.col("defteam").alias("team"), pl.col(r_id).alias("player_id")
                )
            )

    fumbles = _def_count(fumbled_frames, value_name="def_fumbles")
    recov_own = _def_count(recovery_frames, value_name="def_fumble_recovery_own")
    return _def_join(fumbles, recov_own)


def _def_fumble_yards_own_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_fumble_recovery_yards_own`` -- sum of recovery yards for the own-fumble rows.

    Deviation from the verbatim R source (mirroring :func:`_def_fumble_own_frame`'s
    documented choice): each recovery slot is additionally gated on
    ``fumble_recovery_{k}_team == defteam``, where R's ``fumble_yds_own_df``
    groups on the raw recovery team with no such guard -- the formula-table
    semantics ("restricted to the own recovery rows") rather than the R quirk.
    """
    fumbled_cols = [f"fumbled_{k}_team" for k in (1, 2) if f"fumbled_{k}_team" in data.columns]
    if not fumbled_cols:
        return _def_empty({"def_fumble_recovery_yards_own": pl.Float64})
    own_cond = pl.lit(False)
    for col in fumbled_cols:
        own_cond = own_cond | (pl.col("defteam") == pl.col(col))
    own_fum = data.filter(((_i("fumble") == 1) | (_i("fumble_lost") == 1)) & own_cond)

    frames = []
    for k in (1, 2):
        r_team, r_id, r_yds = (
            f"fumble_recovery_{k}_team",
            f"fumble_recovery_{k}_player_id",
            f"fumble_recovery_{k}_yards",
        )
        if r_team in own_fum.columns and r_id in own_fum.columns:
            yds_expr = _f(r_yds) if r_yds in own_fum.columns else pl.lit(0.0)
            frames.append(
                own_fum.filter((pl.col(r_team) == pl.col("defteam")) & pl.col(r_id).is_not_null()).select(
                    "season",
                    "week",
                    pl.col("defteam").alias("team"),
                    pl.col(r_id).alias("player_id"),
                    yds_expr.alias("yds"),
                )
            )
    if not frames:
        return _def_empty({"def_fumble_recovery_yards_own": pl.Float64})
    return (
        pl.concat(frames, how="vertical_relaxed")
        .group_by(list(_DEF_JOIN_KEYS))
        .agg(pl.col("yds").sum().alias("def_fumble_recovery_yards_own"))
    )


def _def_fumble_opp_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_fumble_recovery_opp`` -- ``defteam`` recovered an opponent (non-own) fumble.

    Symmetric per-slot guard, mirroring nflfastR's ``fumble_df_opp`` mutate
    (which nulls each ``fumble_recovery_{k}_player_id`` unless ``defteam !=
    fumbled_{k}_team``) -- this count IS slot-symmetric in the R source
    itself (unlike the yards variant below).
    """
    base = data.filter((_i("fumble") == 1) | (_i("fumble_lost") == 1))
    frames = []
    for k in (1, 2):
        r_team, r_id, f_team = f"fumble_recovery_{k}_team", f"fumble_recovery_{k}_player_id", f"fumbled_{k}_team"
        if r_team not in base.columns or r_id not in base.columns:
            continue
        not_own = (pl.col(f_team) != pl.col("defteam")) if f_team in base.columns else pl.lit(True)
        frames.append(
            base.filter((pl.col(r_team) == pl.col("defteam")) & not_own & pl.col(r_id).is_not_null()).select(
                "season", "week", pl.col("defteam").alias("team"), pl.col(r_id).alias("player_id")
            )
        )
    if not frames:
        return _def_empty({"def_fumble_recovery_opp": pl.Float64})
    return _def_count(frames, value_name="def_fumble_recovery_opp")


def _def_fumble_yards_opp_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_fumble_recovery_yards_opp`` -- REPLICATES the flagged asymmetric R guard.

    nflfastR's ``fumble_yds_opp_data`` filters ONLY on slot-1 conditions
    (``defteam == fumble_recovery_1_team & defteam != fumbled_1_team``), then
    reuses that SAME slot-1-filtered row set for BOTH the slot-1 AND slot-2
    yards group-bys -- slot 2's own conditions are never independently
    re-checked. This is exactly as shipped (4+ years) in nflfastR; replicated
    verbatim here rather than "fixed" to be symmetric, per the reference's
    explicit instruction to preserve this exact (possibly slightly
    under/over-inclusive on a rare double-fumble-recovery play) behavior.
    """
    if not {"fumble_recovery_1_team", "fumbled_1_team"}.issubset(data.columns):
        return _def_empty({"def_fumble_recovery_yards_opp": pl.Float64})
    base = data.filter(
        ((_i("fumble") == 1) | (_i("fumble_lost") == 1))
        & (pl.col("fumble_recovery_1_team") == pl.col("defteam"))
        & (pl.col("defteam") != pl.col("fumbled_1_team"))
    )
    if base.height == 0:
        return _def_empty({"def_fumble_recovery_yards_opp": pl.Float64})
    frames = []
    for k in (1, 2):
        r_team, r_id, r_yds = (
            f"fumble_recovery_{k}_team",
            f"fumble_recovery_{k}_player_id",
            f"fumble_recovery_{k}_yards",
        )
        if r_team not in base.columns or r_id not in base.columns:
            continue
        yds_expr = _f(r_yds) if r_yds in base.columns else pl.lit(0.0)
        frames.append(
            base.filter(pl.col(r_id).is_not_null()).select(
                "season", "week", pl.col(r_team).alias("team"), pl.col(r_id).alias("player_id"), yds_expr.alias("yds")
            )
        )
    if not frames:
        return _def_empty({"def_fumble_recovery_yards_opp": pl.Float64})
    return (
        pl.concat(frames, how="vertical_relaxed")
        .group_by(list(_DEF_JOIN_KEYS))
        .agg(pl.col("yds").sum().alias("def_fumble_recovery_yards_opp"))
    )


def _def_penalty_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """``def_penalty`` / ``def_penalty_yards`` -- uses the FULL ``pbp`` (not the down-restricted ``data``)."""
    if not {"penalty", "penalty_team", "penalty_player_id", "penalty_yards"}.issubset(pbp.columns):
        return _def_empty({"def_penalty": pl.Float64, "def_penalty_yards": pl.Float64})
    return (
        pbp.filter(
            (_i("penalty") == 1)
            & pl.col("penalty_player_id").is_not_null()
            & (pl.col("defteam") == pl.col("penalty_team"))
        )
        .group_by(["season", "week", pl.col("defteam").alias("team"), pl.col("penalty_player_id").alias("player_id")])
        .agg(pl.len().cast(pl.Float64).alias("def_penalty"), _f("penalty_yards").sum().alias("def_penalty_yards"))
    )


def _def_td_frame(data: pl.DataFrame) -> pl.DataFrame:
    """``def_tds`` -- touchdowns credited to ``defteam`` (returns / pick-sixes / fumble-return TDs)."""
    if not {"touchdown", "td_team", "td_player_id"}.issubset(data.columns):
        return _def_empty({"def_tds": pl.Float64})
    return (
        data.filter(
            (_i("touchdown") == 1) & (pl.col("defteam") == pl.col("td_team")) & pl.col("td_player_id").is_not_null()
        )
        .group_by(["season", "week", pl.col("td_team").alias("team"), pl.col("td_player_id").alias("player_id")])
        .agg(_i("touchdown").sum().cast(pl.Float64).alias("def_tds"))
    )


def _collapse_def_to_season(player_df: pl.DataFrame) -> pl.DataFrame:
    """Season collapse per the R source: grouped on ``(player_id, team)`` ONLY (no season)."""
    sum_cols = [c for c in _DEF_OUTPUT_COLUMNS if c.startswith("def_") and c in player_df.columns]
    return player_df.group_by(["player_id", "team"]).agg(
        pl.len().alias("games"),
        # pl.first is equivalent to R's custom_mode here: all five meta
        # columns come solely from the deduped players-master join, so they
        # are constant within a (player_id, team) group.
        pl.first("player_name").alias("player_name"),
        pl.first("player_display_name").alias("player_display_name"),
        pl.first("position").alias("position"),
        pl.first("position_group").alias("position_group"),
        pl.first("headshot_url").alias("headshot_url"),
        *[pl.col(c).sum().alias(c) for c in sum_cols],
    )


def _finalize_def(player_df: pl.DataFrame, *, weekly: bool) -> pl.DataFrame:
    """Select the canonical def-stats column order + cast + sort.

    The season grain drops ``season`` too, not just ``week``/``season_type`` --
    R's ``group_by(player_id, team) |> summarise(...)`` consumes them all.
    """
    cols = list(_DEF_OUTPUT_COLUMNS)
    if not weekly:
        cols = [c for c in cols if c not in ("season", "week", "season_type")]
        cols.insert(cols.index("player_display_name") + 1, "games")
    for c in cols:
        if c not in player_df.columns:
            player_df = player_df.with_columns(pl.lit(None).alias(c))
    out = player_df.select(cols).filter(pl.col("player_id").is_not_null())
    sort_keys = ["player_id", "season", "week"] if weekly else ["player_id"]
    return out.sort(sort_keys)


@overload
def build_nfl_player_stats_def(
    pbp: pl.DataFrame,
    *,
    weekly: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def build_nfl_player_stats_def(
    pbp: pl.DataFrame,
    *,
    weekly: bool = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def build_nfl_player_stats_def(
    pbp: pl.DataFrame,
    *,
    weekly: bool = False,
    return_as_pandas: bool = False,
) -> pl.DataFrame | "pd.DataFrame":
    """Build player-level defensive stats from play-by-play (nflfastR parity).

    A faithful polars port of nflfastR's deprecated
    ``calculate_player_stats_def()`` (``aggregate_game_stats_def.R``). Tackle,
    sack (half-sack = 0.5 weighting), pass-defense, interception, safety,
    fumble (own/opponent recovery), penalty, and touchdown sub-frames are each
    aggregated on ``(season, week, team=defteam, player_id)`` and full-outer
    joined together, then player metadata is joined from
    :func:`sportsdataverse.nfl.load_nfl_players`.

    Unlike :func:`build_nfl_player_stats`, this function takes a
    caller-supplied ``pbp`` frame directly rather than loading one -- matching
    the R function's own signature.

    Args:
        pbp: Play-by-play frame carrying the wide nflverse defensive columns
            (``solo_tackle_1_player_id``, ``sack_player_id``,
            ``half_sack_{1,2}_player_id``, ``interception_player_id``,
            ``pass_defense_{1,2}_player_id``, ``fumbled_{1,2}_team`` /
            ``fumble_recovery_{1,2}_team``, etc. -- the same columns
            :func:`sportsdataverse.nfl.load_nfl_pbp` serves).
        weekly: If ``True`` return one row per (season, week, player); if
            ``False`` collapse to one row per ``(player_id, team)`` -- note
            this does NOT retain a ``season`` column even if ``pbp`` spans
            multiple seasons (see the module-level note above), matching the
            R source's own ``group_by(player_id, team)`` (no ``season``).
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A polars (or pandas) DataFrame with the ``def_*`` column set
        documented in the nflfastR-parity reference (weekly grain carries
        ``season``/``week``/``season_type``; the season collapse replaces
        those with ``games``).

    Example:
        Weekly defensive stats from an already-loaded PBP frame::

            from sportsdataverse.nfl import build_nfl_player_stats_def, load_nfl_pbp
            pbp = load_nfl_pbp([2023])
            wk = build_nfl_player_stats_def(pbp, weekly=True)
            print(wk.shape)

        Season totals (one season's worth of ``pbp`` at a time)::

            season = build_nfl_player_stats_def(pbp, weekly=False)

        Pipeline next step (one line)::

            wk.sort("def_sacks", descending=True).head()

    See Also:
        * `nflfastR <https://www.nflfastr.com>`_ -- the ``calculate_player_stats_def`` source
        * `nflreadpy <https://github.com/nflverse/nflreadpy>`_ -- nflverse loaders (Python)
    """
    if pbp.height == 0:
        return _empty_player_stats_def(weekly=weekly, return_as_pandas=return_as_pandas)

    pbp = _prepare_pbp(pbp)
    data = pbp.filter(pl.col("down").is_not_null() & pl.col("play_type").is_in(["pass", "qb_kneel", "qb_spike", "run"]))

    frames = [
        _def_tackle_frame(data),
        _def_tackle_for_loss_yards(data),
        _def_sack_frame(data),
        _def_int_frame(data),
        _def_safety_frame(data),
        _def_fumble_own_frame(data),
        _def_fumble_yards_own_frame(data),
        _def_fumble_opp_frame(data),
        _def_fumble_yards_opp_frame(data),
        _def_penalty_frame(pbp),
        _def_td_frame(data),
    ]
    # def_qb_hits shares the slot-count helper (kept separate from the sack
    # frame since it has no yards component).
    frames.append(
        _def_count(
            [_def_slot_frame(data, "qb_hit_1_player_id"), _def_slot_frame(data, "qb_hit_2_player_id")],
            value_name="def_qb_hits",
        )
    )

    player_df = _def_cast_keys(frames[0])
    for frame in frames[1:]:
        player_df = _def_join(player_df, frame)

    player_df = player_df.filter(pl.col("player_id").is_not_null())

    count_cols = [c for c in _DEF_OUTPUT_COLUMNS if c.startswith("def_") and c in player_df.columns]
    player_df = player_df.with_columns([pl.col(c).fill_null(0.0) for c in count_cols])

    # season_type per (season, week), joined from the down-restricted grain.
    s_type = data.select(["season", "week", "season_type"]).unique(subset=["season", "week"], keep="first")
    player_df = player_df.join(s_type, on=["season", "week"], how="left")

    from sportsdataverse.nfl.nfl_loaders import load_nfl_players  # noqa: PLC0415

    # Unlike build_nfl_player_stats, the def-stats source never derives a
    # player_name from pbp itself (per the R source, player_name/
    # player_display_name/position/position_group/headshot_url all come
    # exclusively from the load_players() join) -- satisfy _join_player_meta's
    # coalesce-with-pbp-name contract with an all-null placeholder so the
    # players-master short_name always wins.
    if "player_name" not in player_df.columns:
        player_df = player_df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("player_name"))
    player_df = _join_player_meta(player_df, load_nfl_players())

    if not weekly:
        player_df = _collapse_def_to_season(player_df)

    player_df = _finalize_def(player_df, weekly=weekly)

    if return_as_pandas:
        return player_df.to_pandas()
    return player_df


# ---------------------------------------------------------------------------
# player_stats_kicking (nflfastR ``calculate_player_stats_kicking`` parity)
# ---------------------------------------------------------------------------

_KICK_OUTPUT_COLUMNS: tuple[str, ...] = (
    "season",
    "week",
    "season_type",
    "player_id",
    "team",
    "player_name",
    "player_display_name",
    "position",
    "position_group",
    "headshot_url",
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
    "gwfg_att",
    "gwfg_distance",
    "gwfg_made",
    "gwfg_missed",
    "gwfg_blocked",
)

_KICK_LIST_COLUMNS: tuple[str, ...] = ("fg_made_list", "fg_missed_list", "fg_blocked_list")

_KICK_INT_COLUMNS: tuple[str, ...] = (
    "fg_made",
    "fg_att",
    "fg_missed",
    "fg_blocked",
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
    "fg_made_distance",
    "fg_missed_distance",
    "fg_blocked_distance",
    "pat_made",
    "pat_att",
    "pat_missed",
    "pat_blocked",
    "gwfg_att",
    "gwfg_made",
    "gwfg_missed",
    "gwfg_blocked",
)


def _kick_season_columns(cols: List[str]) -> List[str]:
    """Season-grain column list: drop season/week/season_type, insert games, rename gwfg_distance.

    ``season`` is dropped too -- the R season grain groups on
    ``(player_id, team)`` only, so its output carries no season column.
    """
    out = [c for c in cols if c not in ("season", "week", "season_type")]
    out.insert(out.index("player_display_name") + 1, "games")
    return ["gwfg_distance_list" if c == "gwfg_distance" else c for c in out]


def _empty_player_stats_kicking(*, weekly: bool, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Return a zero-row kicking-stats frame carrying the canonical schema."""
    cols = _kick_season_columns(list(_KICK_OUTPUT_COLUMNS)) if not weekly else list(_KICK_OUTPUT_COLUMNS)
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    str_cols = {
        "player_id",
        "team",
        "player_name",
        "player_display_name",
        "position",
        "position_group",
        "headshot_url",
        "season_type",
        *_KICK_LIST_COLUMNS,
        "gwfg_distance_list",
    }
    int_cols = {"season", "week", "games", *_KICK_INT_COLUMNS}
    for c in cols:
        if c == "gwfg_distance":
            schema[c] = pl.List(pl.Float64)
        elif c in str_cols:
            schema[c] = pl.Utf8
        elif c in int_cols:
            schema[c] = pl.Int64
        else:
            schema[c] = pl.Float64
    out = pl.DataFrame(schema=schema)
    if return_as_pandas:
        return out.to_pandas()
    return out


_KICK_BASE_SCHEMA: dict[str, type[pl.DataType] | pl.DataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "week": pl.Int64,
    "season_type": pl.Utf8,
    "team": pl.Utf8,
    "player_name": pl.Utf8,
    "player_id": pl.Utf8,
    "dist": pl.Float64,
    "field_goal_attempt": pl.Int64,
    "fg_res": pl.Utf8,
    "extra_point_attempt": pl.Int64,
    "pat_res": pl.Utf8,
    "fixed_drive": pl.Int64,
    "score_differential": pl.Float64,
}


def _kick_base_frame(pbp: pl.DataFrame) -> pl.DataFrame:
    """The nflfastR ``df_fg_or_pat`` base frame: FG/PAT attempts + the kicking team's final drive."""
    needed = {"game_id", "posteam", "kicker_player_id", "field_goal_attempt", "extra_point_attempt", "fixed_drive"}
    if not needed.issubset(pbp.columns):
        return pl.DataFrame(schema=_KICK_BASE_SCHEMA)

    work = pbp.with_columns(pl.col("fixed_drive").max().over(["game_id", "posteam"]).alias("_max_fixed_drive"))
    base = work.filter(
        (_i("field_goal_attempt") == 1)
        | (_i("extra_point_attempt") == 1)
        | (pl.col("fixed_drive") == pl.col("_max_fixed_drive"))
    ).filter(pl.col("kicker_player_id").is_not_null())
    if base.height == 0:
        return pl.DataFrame(schema=_KICK_BASE_SCHEMA)

    name_expr = pl.col("kicker_player_name") if "kicker_player_name" in base.columns else pl.lit(None, dtype=pl.Utf8)
    season_type_expr = pl.col("season_type") if "season_type" in base.columns else pl.lit(None, dtype=pl.Utf8)
    score_diff_expr = (
        _f("score_differential") if "score_differential" in base.columns else pl.lit(None, dtype=pl.Float64)
    )
    return base.select(
        pl.col("game_id").cast(pl.Utf8, strict=False),
        pl.col("season").cast(pl.Int64, strict=False),
        pl.col("week").cast(pl.Int64, strict=False),
        season_type_expr.alias("season_type"),
        pl.col("posteam").cast(pl.Utf8, strict=False).alias("team"),
        name_expr.alias("player_name"),
        pl.col("kicker_player_id").cast(pl.Utf8, strict=False).alias("player_id"),
        _f("kick_distance").alias("dist"),
        _i("field_goal_attempt").alias("field_goal_attempt"),
        pl.col("field_goal_result").alias("fg_res"),
        _i("extra_point_attempt").alias("extra_point_attempt"),
        pl.col("extra_point_result").alias("pat_res"),
        pl.col("fixed_drive").cast(pl.Int64, strict=False),
        score_diff_expr.alias("score_differential"),
    )


def _kick_grp_cols(weekly: bool) -> List[str]:
    return ["season", "week", "season_type", "player_id", "team"] if weekly else ["player_id", "team"]


def _kick_field_goals(base: pl.DataFrame, grp: List[str]) -> pl.DataFrame:
    """``fg_made``/``fg_att``/... + the 6 made/missed distance buckets + long/pct/lists."""
    fg = base.filter(pl.col("field_goal_attempt") == 1)
    made = pl.col("fg_res") == "made"
    miss = pl.col("fg_res") == "missed"
    blk = pl.col("fg_res") == "blocked"
    dist = pl.col("dist")

    def _bucket(flag: pl.Expr, lo: int, hi: "int | None") -> pl.Expr:
        cond = (flag & (dist >= lo)) if hi is None else (flag & (dist >= lo) & (dist <= hi))
        return cond.sum().cast(pl.Int64)

    if fg.height == 0:
        return pl.DataFrame(
            schema={
                **{c: pl.Utf8 for c in grp if c in ("season_type", "player_id", "team")},
                **{c: pl.Int64 for c in grp if c in ("season", "week")},
                "fg_made": pl.Int64,
                "fg_att": pl.Int64,
                "fg_missed": pl.Int64,
                "fg_blocked": pl.Int64,
                "fg_long": pl.Float64,
                "fg_pct": pl.Float64,
                "fg_made_0_19": pl.Int64,
                "fg_made_20_29": pl.Int64,
                "fg_made_30_39": pl.Int64,
                "fg_made_40_49": pl.Int64,
                "fg_made_50_59": pl.Int64,
                "fg_made_60_": pl.Int64,
                "fg_missed_0_19": pl.Int64,
                "fg_missed_20_29": pl.Int64,
                "fg_missed_30_39": pl.Int64,
                "fg_missed_40_49": pl.Int64,
                "fg_missed_50_59": pl.Int64,
                "fg_missed_60_": pl.Int64,
                "fg_made_list": pl.Utf8,
                "fg_missed_list": pl.Utf8,
                "fg_blocked_list": pl.Utf8,
                "fg_made_distance": pl.Int64,
                "fg_missed_distance": pl.Int64,
                "fg_blocked_distance": pl.Int64,
            }
        )

    return (
        fg.group_by(grp)
        .agg(
            made.sum().cast(pl.Int64).alias("fg_made"),
            pl.col("field_goal_attempt").sum().cast(pl.Int64).alias("fg_att"),
            miss.sum().cast(pl.Int64).alias("fg_missed"),
            blk.sum().cast(pl.Int64).alias("fg_blocked"),
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
            dist.filter(made).drop_nulls().cast(pl.Int64).cast(pl.Utf8).str.join(";").alias("fg_made_list"),
            dist.filter(miss).drop_nulls().cast(pl.Int64).cast(pl.Utf8).str.join(";").alias("fg_missed_list"),
            dist.filter(blk).drop_nulls().cast(pl.Int64).cast(pl.Utf8).str.join(";").alias("fg_blocked_list"),
            dist.filter(made).sum().cast(pl.Int64).alias("fg_made_distance"),
            dist.filter(miss).sum().cast(pl.Int64).alias("fg_missed_distance"),
            dist.filter(blk).sum().cast(pl.Int64).alias("fg_blocked_distance"),
        )
        .with_columns(
            pl.when(pl.col("fg_att") > 0)
            .then((pl.col("fg_made") / pl.col("fg_att")).round(3))
            .otherwise(None)
            .alias("fg_pct")
        )
    )


def _kick_pat(base: pl.DataFrame, grp: List[str]) -> pl.DataFrame:
    """``pat_made``/``pat_att``/``pat_missed``/``pat_blocked``/``pat_pct``."""
    pat = base.filter(pl.col("extra_point_attempt") == 1)
    if pat.height == 0:
        return pl.DataFrame(
            schema={
                **{c: pl.Utf8 for c in grp if c in ("season_type", "player_id", "team")},
                **{c: pl.Int64 for c in grp if c in ("season", "week")},
                "pat_made": pl.Int64,
                "pat_att": pl.Int64,
                "pat_missed": pl.Int64,
                "pat_blocked": pl.Int64,
                "pat_pct": pl.Float64,
            }
        )
    return (
        pat.group_by(grp)
        .agg(
            (pl.col("pat_res") == "good").sum().cast(pl.Int64).alias("pat_made"),
            pl.col("extra_point_attempt").sum().cast(pl.Int64).alias("pat_att"),
            (pl.col("pat_res") == "failed").sum().cast(pl.Int64).alias("pat_missed"),
            (pl.col("pat_res") == "blocked").sum().cast(pl.Int64).alias("pat_blocked"),
        )
        .with_columns(
            pl.when(pl.col("pat_att") > 0)
            .then((pl.col("pat_made") / pl.col("pat_att")).round(3))
            .otherwise(None)
            .alias("pat_pct")
        )
    )


def _kick_gwfg(base: pl.DataFrame, grp: List[str], *, weekly: bool) -> pl.DataFrame:
    """Game-winning FG attempts: last fixed_drive of the game, trailing by <=2 points.

    Mirrors the R ``game_winners`` block exactly (no ``defteam_score``
    cross-check, unlike the team-level :func:`_team_gwfg_frame` which adds one
    as a deliberate improvement) -- this player-level builder is the literal
    ``calculate_player_stats_kicking`` port.
    """
    dist_name = "gwfg_distance" if weekly else "gwfg_distance_list"
    empty_schema: dict[str, type[pl.DataType] | pl.DataType] = {
        **{c: pl.Utf8 for c in grp if c in ("season_type", "player_id", "team")},
        **{c: pl.Int64 for c in grp if c in ("season", "week")},
        "gwfg_att": pl.Int64,
        dist_name: pl.List(pl.Float64) if weekly else pl.Utf8,
        "gwfg_made": pl.Int64,
        "gwfg_missed": pl.Int64,
        "gwfg_blocked": pl.Int64,
    }
    if base.height == 0:
        return pl.DataFrame(schema=empty_schema)

    work = base.with_columns(pl.col("fixed_drive").max().over(["game_id", "team"]).alias("_max_fd"))
    gwfg = work.filter(
        (pl.col("fixed_drive") == pl.col("_max_fd"))
        & (pl.col("field_goal_attempt") == 1)
        & (pl.col("score_differential") >= -2)
        & (pl.col("score_differential") <= 0)
    )
    if gwfg.height == 0:
        return pl.DataFrame(schema=empty_schema)

    dist_agg = (
        pl.col("dist").alias(dist_name)
        if weekly
        else pl.col("dist").drop_nulls().cast(pl.Int64).cast(pl.Utf8).str.join(";").alias(dist_name)
    )
    return gwfg.group_by(grp).agg(
        pl.len().cast(pl.Int64).alias("gwfg_att"),
        dist_agg,
        (pl.col("fg_res") == "made").sum().cast(pl.Int64).alias("gwfg_made"),
        (pl.col("fg_res") == "missed").sum().cast(pl.Int64).alias("gwfg_missed"),
        (pl.col("fg_res") == "blocked").sum().cast(pl.Int64).alias("gwfg_blocked"),
    )


def _kick_games(base: pl.DataFrame, grp: List[str]) -> pl.DataFrame:
    """``games`` -- distinct ``game_id`` count over FG-or-PAT attempts (GWFG is a subset of FG)."""
    qualifying = base.filter((pl.col("field_goal_attempt") == 1) | (pl.col("extra_point_attempt") == 1))
    if qualifying.height == 0:
        empty_schema: dict[str, type[pl.DataType] | pl.DataType] = {
            **{c: pl.Utf8 for c in grp if c in ("season_type", "player_id", "team")},
            **{c: pl.Int64 for c in grp if c in ("season", "week")},
            "games": pl.Int64,
        }
        return pl.DataFrame(schema=empty_schema)
    return qualifying.group_by(grp).agg(pl.col("game_id").n_unique().cast(pl.Int64).alias("games"))


def _finalize_kicking(kick_df: pl.DataFrame, *, weekly: bool) -> pl.DataFrame:
    """Select the canonical kicking-stats column order + sort."""
    cols = list(_KICK_OUTPUT_COLUMNS) if weekly else _kick_season_columns(list(_KICK_OUTPUT_COLUMNS))
    for c in cols:
        if c not in kick_df.columns:
            kick_df = kick_df.with_columns(pl.lit(None).alias(c))
    out = kick_df.select(cols).filter(pl.col("player_id").is_not_null())
    sort_keys = ["player_id", "season", "week"] if weekly else ["player_id"]
    return out.sort(sort_keys)


@overload
def build_nfl_player_stats_kicking(
    pbp: pl.DataFrame,
    *,
    weekly: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def build_nfl_player_stats_kicking(
    pbp: pl.DataFrame,
    *,
    weekly: bool = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def build_nfl_player_stats_kicking(
    pbp: pl.DataFrame,
    *,
    weekly: bool = False,
    return_as_pandas: bool = False,
) -> pl.DataFrame | "pd.DataFrame":
    """Build player-level kicking stats from play-by-play (nflfastR parity).

    A faithful polars port of nflfastR's deprecated
    ``calculate_player_stats_kicking()`` (``aggregate_game_stats_kicking.R``).
    Field goals (made-distance buckets, ``fg_long``, ``fg_pct``, ``;``-joined
    distance lists), extra points, and game-winning-FG attempts (last drive of
    the game, trailing by 2 or fewer points) are each aggregated on the kicker
    and full-outer joined together, then player metadata is joined from
    :func:`sportsdataverse.nfl.load_nfl_players`.

    Unlike :func:`build_nfl_team_stats`, this function takes a caller-supplied
    ``pbp`` frame directly rather than loading one -- matching the R
    function's own signature.

    Args:
        pbp: Play-by-play frame carrying ``kicker_player_id`` /
            ``kicker_player_name``, ``field_goal_attempt`` /
            ``field_goal_result`` / ``kick_distance``, ``extra_point_attempt``
            / ``extra_point_result``, ``fixed_drive``, and
            ``score_differential`` (the same columns
            :func:`sportsdataverse.nfl.load_nfl_pbp` serves).
        weekly: If ``True`` return one row per (season, week, player) with a
            ``gwfg_distance`` list column; if ``False`` collapse to one row
            per ``(player_id, team)`` with a ``games`` column and a
            ``;``-joined ``gwfg_distance_list`` string column in place of
            ``gwfg_distance`` (the R source's own deliberate column-name
            change based on the ``weekly`` flag). Note this does NOT retain a
            ``season`` column even if ``pbp`` spans multiple seasons (see the
            module-level note above :func:`build_nfl_player_stats_def`).
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A polars (or pandas) DataFrame with the ``fg_*``/``pat_*``/``gwfg_*``
        column set documented in the nflfastR-parity reference.

    Example:
        Weekly kicking stats from an already-loaded PBP frame::

            from sportsdataverse.nfl import build_nfl_player_stats_kicking, load_nfl_pbp
            pbp = load_nfl_pbp([2023])
            wk = build_nfl_player_stats_kicking(pbp, weekly=True)
            print(wk.shape)

        Season totals (one season's worth of ``pbp`` at a time)::

            season = build_nfl_player_stats_kicking(pbp, weekly=False)

        Pipeline next step (one line)::

            wk.filter(pl.col("fg_att") >= 1).sort("fg_pct", descending=True).head()

    See Also:
        * `nflfastR <https://www.nflfastr.com>`_ -- the ``calculate_player_stats_kicking`` source
        * `nflreadpy <https://github.com/nflverse/nflreadpy>`_ -- nflverse loaders (Python)
    """
    if pbp.height == 0:
        return _empty_player_stats_kicking(weekly=weekly, return_as_pandas=return_as_pandas)

    base = _kick_base_frame(pbp)
    if base.height == 0:
        return _empty_player_stats_kicking(weekly=weekly, return_as_pandas=return_as_pandas)

    grp = _kick_grp_cols(weekly)
    fg = _kick_field_goals(base, grp)
    pat = _kick_pat(base, grp)
    gwfg = _kick_gwfg(base, grp, weekly=weekly)
    games = _kick_games(base, grp)
    names = base.group_by(grp).agg(pl.first("player_name").alias("player_name"))

    kick_df = fg.join(pat, on=grp, how="full", coalesce=True)
    kick_df = kick_df.join(gwfg, on=grp, how="full", coalesce=True)
    kick_df = kick_df.join(names, on=grp, how="full", coalesce=True)
    kick_df = kick_df.join(games, on=grp, how="left")

    kick_df = kick_df.filter(pl.col("player_id").is_not_null())

    # Attempt counts never null (0 when a player attempted none of that kick type).
    for c in ("fg_att", "pat_att", "gwfg_att"):
        if c in kick_df.columns:
            kick_df = kick_df.with_columns(pl.col(c).fill_null(0))
        else:
            kick_df = kick_df.with_columns(pl.lit(0).cast(pl.Int64).alias(c))
    for c in _KICK_INT_COLUMNS:
        if c not in ("fg_att", "pat_att", "gwfg_att") and c in kick_df.columns:
            kick_df = kick_df.with_columns(pl.col(c).fill_null(0))

    dist_name = "gwfg_distance" if weekly else "gwfg_distance_list"
    if dist_name not in kick_df.columns:
        kick_df = kick_df.with_columns(pl.lit(None).alias(dist_name))

    # Blanket "" -> null across the ``;``-joined list columns (mirrors the R
    # source's ``replace(.x, nchar(.x)==0 | is.nan(.x), NA)`` cleanup, applied
    # per-dtype here rather than the R blanket-stringify approach).
    str_list_cols = [c for c in (*_KICK_LIST_COLUMNS, "gwfg_distance_list") if c in kick_df.columns]
    if str_list_cols:
        kick_df = kick_df.with_columns(
            [pl.when(pl.col(c).str.len_chars() == 0).then(None).otherwise(pl.col(c)).alias(c) for c in str_list_cols]
        )

    from sportsdataverse.nfl.nfl_loaders import load_nfl_players  # noqa: PLC0415

    kick_df = _join_player_meta(kick_df, load_nfl_players())

    kick_df = _finalize_kicking(kick_df, weekly=weekly)

    if return_as_pandas:
        return kick_df.to_pandas()
    return kick_df
