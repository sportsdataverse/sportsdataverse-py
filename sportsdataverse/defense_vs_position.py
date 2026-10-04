"""Defense vs position: what each defense allowed to QBs, RBs, WRs and TEs.

One function, :func:`defense_vs_position`, turns a season of released plays plus
that season's roster into one row per ``(season, team_id, position_group)`` for
the DEFENSE: EPA per play, success rate and explosive rate allowed, with
per-group extras (QB dropbacks + sack rate, RB carries + yards per carry, WR/TE
targets + yards per target) and the ``qualified`` games floor. Percentiles are
the producer's job (among qualifiers); this module only derives the rows.

Which plays a group gets:

* every **dropback** (pass attempt, sack, and in the NFL a scramble) is the QB's;
* a **carry** goes to the rusher's roster group (a designed QB run is QB, a jet
  sweep is WR; in CFB, which has no scramble flag, a scramble is a QB carry);
* a **target** goes to the receiver's roster group.

So a completed pass to a tight end counts once for QB and once for TE. Groups are
the F5 producers' (``POSITION_GROUPS``: QB; RB = RB + FB; WR; TE), read from the
season roster. A carrier or receiver with no roster row, an ``other`` position
(OL, DB, ATH, ...) or two different groups in one season is counted in no group;
so is a target with no receiver id (ESPN CFB names no receiver on most
incompletions and every interception).

Example:
    Quick start::

        import polars as pl
        from sportsdataverse.cfb import load_cfb_pbp, load_cfb_rosters
        from sportsdataverse.defense_vs_position import PBP_COLUMNS, defense_vs_position

        pbp = load_cfb_pbp(2024).select(PBP_COLUMNS["cfb"])
        dvp = defense_vs_position(pbp, load_cfb_rosters(2024), "cfb")
        dvp.filter(pl.col("position_group") == "TE").sort("epa_per_play_allowed").head()
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.metric_curves import _NFLFASTR_SEASON_TYPES
from sportsdataverse.rolling_windows import COUNTED_SEASON_TYPES, _as_id

__all__ = [
    "MIN_GAMES",
    "OUTPUT_SCHEMA",
    "PBP_COLUMNS",
    "POSITION_GROUPS",
    "defense_vs_position",
]

#: a (season, team, group) row with fewer games than this is not ``qualified``
MIN_GAMES = 3

#: roster position -> position group, the F5 producers' map (cfb-data
#: ``team_summaries._POSITION_GROUPS``, nfl-data ``build._POSITION_GROUPS``);
#: any other listed position is ``other`` and ESPN's ``-`` is unknown
POSITION_GROUPS = {"QB": "QB", "RB": "RB", "FB": "RB", "WR": "WR", "TE": "TE"}

#: the EPA a dropback / carry must reach to be explosive (cfb_pbp ``EPA_explosive``)
_EXPLOSIVE_PASS_EPA = 2.4
_EXPLOSIVE_RUSH_EPA = 1.8

#: the released pbp columns each league's adapter reads (project to these)
PBP_COLUMNS: dict[str, tuple[str, ...]] = {
    "cfb": (
        "season",
        "seasonType",
        "game_id",
        "game_play_number",
        "down",
        "EPA",
        "EPA_scrimmage",
        "pass",
        "rush",
        "target",
        "sack",
        "yds_rushed",
        "statYardage",
        "yds_receiving",
        "def_pos_team_id",
        "rusher_player_id",
        "receiver_player_id",
    ),
    "nfl": (
        "season",
        "season_type",
        "game_id",
        "play_id",
        "play_type",
        "down",
        "epa",
        "pass",
        "rush",
        "sack",
        "complete_pass",
        "rushing_yards",
        "receiving_yards",
        "yards_gained",
        "defteam",
        "rusher_player_id",
        "receiver_player_id",
    ),
}

#: roster id column per league (the pbp player ids' namespace)
_ROSTER_ID = {"cfb": "athlete_id", "nfl": "gsis_id"}

OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Utf8(),
    "position_group": pl.Utf8(),
    "plays": pl.Int64(),
    "games": pl.Int64(),
    "epa_per_play_allowed": pl.Float64(),
    "success_rate_allowed": pl.Float64(),
    "explosive_rate_allowed": pl.Float64(),
    "dropbacks": pl.Int64(),
    "sack_rate_allowed": pl.Float64(),
    "carries": pl.Int64(),
    "rush_yards_per_carry_allowed": pl.Float64(),
    "targets": pl.Int64(),
    "yards_per_target_allowed": pl.Float64(),
    "qualified": pl.Boolean(),
}

#: per-group extra -> the groups whose plays make it real (null for the others)
_EXTRAS: dict[str, tuple[str, ...]] = {
    "dropbacks": ("QB",),
    "sack_rate_allowed": ("QB",),
    "carries": ("RB",),
    "rush_yards_per_carry_allowed": ("RB",),
    "targets": ("WR", "TE"),
    "yards_per_target_allowed": ("WR", "TE"),
}


def _flag(col: str) -> pl.Expr:
    """A 0/1 or bool play flag as a non-null bool."""
    return pl.col(col).cast(pl.Boolean).fill_null(False)


def _plays(pbp: pl.DataFrame, league: str) -> pl.DataFrame:
    """Scrimmage plays on a numbered down, one shape for both leagues."""
    if league == "cfb":
        keep = (
            pl.col("EPA_scrimmage").is_not_null()
            & pl.col("down").is_between(1, 4)
            & pl.col("EPA").is_not_null()
            & pl.col("def_pos_team_id").is_not_null()
        )
        if "seasonType" in pbp.columns:
            keep = keep & pl.col("seasonType").cast(pl.Int64, strict=False).is_in(COUNTED_SEASON_TYPES)
        df = pbp.filter(keep)
        dropback, carry, sack = _flag("pass"), _flag("rush"), _flag("sack")
        target = _flag("target") & pl.col("receiver_player_id").is_not_null()
        cols = dict(team="def_pos_team_id", play="game_play_number", epa="EPA")
        # ESPN leaves yds_rushed null on ~0.2% of carries (most of them 0-yard rushes);
        # statYardage carries the play's yards there, so a null is never skipped by the mean
        rush_yds = pl.col("yds_rushed").fill_null(pl.col("statYardage"))
        rec_yds = pl.col("yds_receiving").fill_null(0)
    else:
        keep = (
            pl.col("play_type").is_in(["pass", "run"])
            & pl.col("down").is_between(1, 4)
            & pl.col("epa").is_not_null()
            & pl.col("defteam").is_not_null()
        )
        if "season_type" in pbp.columns:
            keep = keep & pl.col("season_type").is_in(_NFLFASTR_SEASON_TYPES)
        df = pbp.filter(keep)
        # nflfastR: pass = dropback (sacks and scrambles in), rush = designed run
        dropback, carry, sack = _flag("pass"), _flag("rush"), _flag("sack")
        target = dropback & (sack == False) & pl.col("receiver_player_id").is_not_null()  # noqa: E712
        cols = dict(team="defteam", play="play_id", epa="epa")
        rush_yds = pl.col("rushing_yards").fill_null(pl.col("yards_gained"))
        # nflfastR leaves receiving_yards null on an incompletion or interception
        complete_yds = pl.col("receiving_yards").fill_null(pl.col("yards_gained"))
        rec_yds = pl.when(_flag("complete_pass") == True).then(complete_yds).otherwise(0)
    epa = pl.col(cols["epa"]).cast(pl.Float64)
    return df.select(
        season=pl.col("season").cast(pl.Int64),
        team_id=_as_id(df, cols["team"]),
        game_id=_as_id(df, "game_id"),
        play=pl.col(cols["play"]).cast(pl.Int64),
        epa=epa,
        success=epa > 0,
        explosive=(dropback & (epa >= _EXPLOSIVE_PASS_EPA)) | (carry & (epa >= _EXPLOSIVE_RUSH_EPA)),
        dropback=dropback,
        carry=carry,
        target=target,
        sack=sack,
        rush_yds=pl.when(carry == True).then(rush_yds.cast(pl.Float64)),  # noqa: E712
        rec_yds=pl.when(target == True).then(rec_yds.cast(pl.Float64)),  # noqa: E712
        rusher_id=_as_id(df, "rusher_player_id"),
        receiver_id=_as_id(df, "receiver_player_id"),
    )


def _roster_groups(rosters: pl.DataFrame, league: str) -> pl.DataFrame:
    """``(season, player_id) -> position_group``; an id under two groups in a season is dropped."""
    pos = pl.col("position")
    if league == "cfb" and "position_abbreviation" in rosters.columns:
        # current loader: abbreviation in position_abbreviation, full name in position;
        # the older shape carries the abbreviation in position itself (sdv-db #124)
        pos = pl.coalesce("position_abbreviation", "position")
    return (
        rosters.select(
            season=pl.col("season").cast(pl.Int64),
            player_id=_as_id(rosters, _ROSTER_ID[league]),
            position_group=pl.when(pos.is_not_null() & (pos != "-")).then(
                pos.replace_strict(POSITION_GROUPS, default="other")
            ),
        )
        .drop_nulls()
        .unique()
        .filter(pl.struct("season", "player_id").is_unique())
    )


def _assign(plays: pl.DataFrame, groups: pl.DataFrame) -> pl.DataFrame:
    """One row per (play, position group): dropbacks to QB, carries and targets by roster."""
    frames = [plays.filter(pl.col("dropback") == True).with_columns(position_group=pl.lit("QB"))]  # noqa: E712
    for flag, id_col in (("carry", "rusher_id"), ("target", "receiver_id")):
        right = groups.rename({"player_id": id_col})
        left = plays.filter(pl.col(flag) == True)  # noqa: E712
        for k in ("season", id_col):
            assert left.schema[k] == right.schema[k], f"{k}: pbp {left.schema[k]} != roster {right.schema[k]}"
        frames.append(left.join(right, on=["season", id_col], how="inner", validate="m:1"))
    return (
        pl.concat(frames)
        .filter(pl.col("position_group") != "other")
        .unique(subset=["game_id", "play", "position_group"], keep="first", maintain_order=True)
    )


def defense_vs_position(pbp: pl.DataFrame, rosters: pl.DataFrame, league: str) -> pl.DataFrame:
    """EPA/play, success and explosive rate each defense allowed to QBs, RBs, WRs and TEs.

    Population: plays from scrimmage on a numbered down (1-4) with an EPA, in the
    regular season or postseason -- CFB ``EPA_scrimmage`` not null and ``seasonType``
    2/3 (the :mod:`sportsdataverse.rolling_windows` population), NFL ``play_type``
    pass/run and ``season_type`` REG/POST. Filter season types first to narrow it.

    A dropback (``pass``: attempts and sacks, plus NFL scrambles) is the QB's; a
    carry (``rush``) goes to the rusher's roster group and a target to the
    receiver's, so one completion counts for QB and for its receiver's group. The
    roster is matched on ``(season, player id)``: CFB ``athlete_id`` with
    ``position_abbreviation`` (or ``position`` when that is the abbreviation, the
    older shape), NFL ``gsis_id`` with ``position``. A carrier or receiver with no
    roster row, an ``other`` position or two different groups that season counts
    in no group, and so does a target with no receiver id. CFB roster positions
    are usable from 2014; the 2004-2013 releases list nearly every player as
    unknown (``-``), so those seasons get QB dropbacks and little else.

    Rates: ``success`` is EPA > 0; ``explosive`` is a dropback with EPA >= 2.4 or
    a carry with EPA >= 1.8 (cfb_pbp's ``EPA_explosive``). ``sack_rate_allowed``
    is sacks per dropback, ``rush_yards_per_carry_allowed`` the carries' rushing
    yards (CFB ``yds_rushed``, ``statYardage`` where it is null), ``yards_per_target_allowed`` receiving yards per target (0 on an
    incompletion or interception). Extras are null on groups whose plays do not
    make them real. ``games`` counts the games with at least one of the cell's
    plays, and ``qualified`` is ``games >= MIN_GAMES``.

    Args:
        pbp (pl.DataFrame): released plays, any number of seasons -- CFB
            ``load_cfb_pbp`` or NFL ``load_nfl_model_pbp`` (project to
            ``PBP_COLUMNS[league]``).
        rosters (pl.DataFrame): the same seasons' rosters -- CFB ``load_cfb_rosters``
            (``season``, ``athlete_id``, ``position_abbreviation`` / ``position``),
            NFL ``load_nfl_rosters`` (``season``, ``gsis_id``, ``position``).
        league (str): ``"cfb"`` or ``"nfl"``.

    Returns:
        pl.DataFrame: one row per ``(season, team_id, position_group)`` the defense
        faced, ``OUTPUT_SCHEMA``, sorted by those keys. ``team_id`` is the defense's
        ESPN team id as text (CFB ``def_pos_team_id``) or its nflverse abbreviation
        (NFL ``defteam``). Empty pbp gives an empty frame with the schema.

    Raises:
        ValueError: ``league`` is not ``"cfb"`` or ``"nfl"``.
        TypeError: a team, game or player id column (pbp or roster) is float.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import load_cfb_pbp, load_cfb_rosters
            from sportsdataverse.defense_vs_position import PBP_COLUMNS, defense_vs_position

            pbp = load_cfb_pbp(2024).select(PBP_COLUMNS["cfb"])
            dvp = defense_vs_position(pbp, load_cfb_rosters(2024), "cfb")

        The NFL twin (gsis ids, nflverse team abbreviations)::

            from sportsdataverse.nfl import load_nfl_model_pbp, load_nfl_rosters

            pbp = load_nfl_model_pbp([2024]).select(PBP_COLUMNS["nfl"])
            dvp_nfl = defense_vs_position(pbp, load_nfl_rosters([2024]), "nfl")

        Pipeline next step (one line)::

            dvp.filter((pl.col("position_group") == "TE") & (pl.col("qualified") == True)).sort("epa_per_play_allowed")

    See Also:
        * `nflfastR`_ -- the dropback / success conventions the NFL adapter reads.
        * `cfbfastR`_ -- the CFB pbp and rosters this reads.

    .. _nflfastR: https://www.nflfastr.com
    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    if league not in PBP_COLUMNS:
        raise ValueError(f"league must be 'cfb' or 'nfl', got {league!r}")
    if pbp.height == 0:
        return pl.DataFrame(schema=OUTPUT_SCHEMA)
    long = _assign(_plays(pbp, league), _roster_groups(rosters, league))
    g = pl.col("position_group")
    out = long.group_by("season", "team_id", "position_group").agg(
        plays=pl.len(),
        games=pl.col("game_id").n_unique(),
        epa_per_play_allowed=pl.col("epa").mean(),
        success_rate_allowed=pl.col("success").mean(),
        explosive_rate_allowed=pl.col("explosive").mean(),
        dropbacks=pl.col("dropback").sum(),
        sack_rate_allowed=pl.col("sack").filter(pl.col("dropback") == True).mean(),  # noqa: E712
        carries=pl.col("carry").sum(),
        rush_yards_per_carry_allowed=pl.col("rush_yds").mean(),
        targets=pl.col("target").sum(),
        yards_per_target_allowed=pl.col("rec_yds").mean(),
    )
    return (
        out.with_columns(
            *[pl.when(g.is_in(groups)).then(pl.col(c)).alias(c) for c, groups in _EXTRAS.items()],
            qualified=pl.col("games") >= MIN_GAMES,
        )
        .select(list(OUTPUT_SCHEMA))
        .cast(OUTPUT_SCHEMA)
        .sort("season", "team_id", "position_group")
    )
