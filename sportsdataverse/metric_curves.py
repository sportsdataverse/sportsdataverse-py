"""Rate curves along a continuous axis: FG% by kick distance, completion% / EPA by air yards,
4th-down conversion by yards to go, success by down x distance, FG% by shot distance.

One league-agnostic core, :func:`metric_curves`, over a long *attempts* frame
(one row per attempt, ``ATTEMPT_SCHEMA``), and the adapters that build it:
:func:`football_attempts` from released ``espn_{cfb,nfl}_pbp`` plays,
:func:`nflfastr_attempts` from ``nfl_model_pbp`` (the nflfastR shape -- the ESPN NFL
pbp has no air yards) and :func:`shot_attempts` from ``{nba,wnba}_stats_shots``.

Every attempt is binned by the fixed :data:`BUCKET_EDGES` of its metric (``x_lo``
inclusive, ``x_hi`` exclusive; an attempt outside every bucket is dropped). Each
populated bucket carries attempts, successes, ``rate = successes / attempts`` and the
mean EPA per attempt, for the league, every team and every credited player; an empty
bucket is absent, never a zero row.

Example:
    Quick start::

        import polars as pl
        from sportsdataverse.cfb import load_cfb_pbp
        from sportsdataverse.metric_curves import FOOTBALL_ATTEMPT_COLUMNS, football_attempts, metric_curves

        pbp = load_cfb_pbp(2024).select(FOOTBALL_ATTEMPT_COLUMNS)
        curves = metric_curves(football_attempts(pbp), "cfb")
        curves.filter((pl.col("metric") == "fg_pct_by_distance") & (pl.col("entity_type") == "league"))
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.football.usage_box import _standing_scrimmage
from sportsdataverse.rolling_windows import _as_id

__all__ = [
    "ATTEMPT_SCHEMA",
    "BUCKET_EDGES",
    "FOOTBALL_ATTEMPT_COLUMNS",
    "ID_SOURCE",
    "NFLFASTR_ATTEMPT_COLUMNS",
    "OUTPUT_SCHEMA",
    "SHOT_ATTEMPT_COLUMNS",
    "football_attempts",
    "metric_curves",
    "nflfastr_attempts",
    "shot_attempts",
]

#: fixed bucket edges per metric: ``x_lo`` inclusive, ``x_hi`` exclusive
BUCKET_EDGES: dict[str, tuple[float, ...]] = {
    "fg_pct_by_distance": (15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 80),
    "cmp_pct_by_air_yards": (-10, 0, 5, 10, 15, 20, 30, 40, 70),
    "epa_by_air_yards": (-10, 0, 5, 10, 15, 20, 30, 40, 70),
    "fourth_conv_by_ytg": (1, 2, 3, 4, 6, 11, 100),
    "success_by_down_distance": (1, 2, 4, 7, 11, 100),
    "fg_pct_by_shot_distance": tuple(range(36)) + (50, 95),
}

#: what the ids in a league's attempts are; the NFL producer re-keys players to ESPN
ID_SOURCE: dict[str, str] = {"cfb": "espn", "nfl": "gsis", "nba": "nba_stats", "wnba": "wnba_stats"}

ATTEMPT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64,
    "metric": pl.Utf8,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "team_id": pl.Utf8,
    "team_name": pl.Utf8,
    "down": pl.Int64,
    "x": pl.Float64,
    "success": pl.Boolean,
    "epa": pl.Float64,
    "id_source": pl.Utf8,
}

OUTPUT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64,
    "entity_type": pl.Utf8,
    "entity_id": pl.Utf8,
    "entity_name": pl.Utf8,
    "team_id": pl.Utf8,
    "id_source": pl.Utf8,
    "metric": pl.Utf8,
    "down": pl.Int64,
    "x_lo": pl.Float64,
    "x_hi": pl.Float64,
    "attempts": pl.Int64,
    "successes": pl.Int64,
    "rate": pl.Float64,
    "epa_per_att": pl.Float64,
}

#: the released ``espn_{cfb,nfl}_pbp`` columns :func:`football_attempts` reads (project to these)
FOOTBALL_ATTEMPT_COLUMNS: tuple[str, ...] = (
    "season",
    "seasonType",
    "down",
    "distance",
    "EPA",
    "EPA_success",
    "pass",
    "rush",
    "pass_attempt",
    "completion",
    "air_yards",
    "passer_player_id",
    "passer_player_name",
    "pos_team_id",
    "pos_team",
    "yds_fg",
    "fg_attempt",
    "fg_made",
    "fg_kicker_player_id",
    "fg_kicker_player_name",
    "first_down_created",
    "touchdown",
    "defense_score_play",
    "penalty_no_play",
    "scrimmage_play",
)

#: the ``nfl_model_pbp`` columns :func:`nflfastr_attempts` reads (project to these)
NFLFASTR_ATTEMPT_COLUMNS: tuple[str, ...] = (
    "season",
    "season_type",
    "posteam",
    "play_type",
    "down",
    "ydstogo",
    "pass_attempt",
    "sack",
    "air_yards",
    "complete_pass",
    "epa",
    "first_down_rush",
    "first_down_pass",
    "touchdown",
    "td_team",
    "field_goal_attempt",
    "field_goal_result",
    "kick_distance",
    "kicker_player_id",
    "kicker_player_name",
    "passer_player_id",
    "passer_player_name",
)

#: the ``{nba,wnba}_stats_shots`` columns :func:`shot_attempts` reads (project to these)
SHOT_ATTEMPT_COLUMNS: tuple[str, ...] = (
    "season",
    "season_type_id",
    "team_id",
    "team_tricode",
    "person_id",
    "player_name",
    "shot_result",
    "shot_distance",
)

#: ESPN season types that count: regular season (2) and postseason (3)
_ESPN_SEASON_TYPES = (2, 3)
#: nflfastR season types that count
_NFLFASTR_SEASON_TYPES = ("REG", "POST")
#: stats.nba season types that count: regular season (2) and playoffs (4); play-in (5) and the Cup final (6) do not
_STATS_SEASON_TYPES = ("2", "4")
_NO_PLAYER = pl.lit(None, dtype=pl.Utf8)
_NO_DOWN = pl.lit(None, dtype=pl.Int64)
_NO_EPA = pl.lit(None, dtype=pl.Float64)


def _attempts(
    df: pl.DataFrame,
    metric: str,
    *,
    x: pl.Expr,
    success: pl.Expr,
    player_id: pl.Expr,
    player_name: pl.Expr,
    team_id: pl.Expr,
    team_name: pl.Expr,
    id_source: str,
    down: pl.Expr = _NO_DOWN,
    epa: pl.Expr = _NO_EPA,
) -> pl.DataFrame:
    """One metric's rows of ``ATTEMPT_SCHEMA`` from an already-filtered play frame."""
    return df.select(
        pl.col("season").cast(pl.Int64),
        pl.lit(metric).alias("metric"),
        player_id.cast(pl.Utf8).alias("player_id"),
        player_name.cast(pl.Utf8).alias("player_name"),
        team_id.cast(pl.Utf8).alias("team_id"),
        team_name.cast(pl.Utf8).alias("team_name"),
        down.cast(pl.Int64).alias("down"),
        x.cast(pl.Float64).alias("x"),
        success.cast(pl.Boolean).alias("success"),
        epa.cast(pl.Float64).alias("epa"),
        pl.lit(id_source).alias("id_source"),
    )


def _finish(frames: list[pl.DataFrame]) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame(schema=ATTEMPT_SCHEMA)
    return pl.concat(frames).filter(pl.col("x").is_not_null() & pl.col("success").is_not_null()).cast(ATTEMPT_SCHEMA)


def football_attempts(pbp: pl.DataFrame) -> pl.DataFrame:
    """Field-goal, air-yards, fourth-down and down-x-distance attempts from released ``espn_{cfb,nfl}_pbp``.

    Populations (regular season + postseason):

    * ``fg_pct_by_distance``: ``fg_attempt`` with a ``yds_fg``; success = ``fg_made``;
      player = the kicker.
    * ``cmp_pct_by_air_yards`` / ``epa_by_air_yards``: ``pass_attempt`` with
      ``air_yards``; success = ``completion`` / ``EPA_success``; player = the passer.
      CFB carries air yards from 2025 only (41% of attempts), so earlier seasons
      yield no rows.
    * ``fourth_conv_by_ytg``: fourth-down rushes and passes that stood (no nullifying
      penalty); success = a first down or the offense's own touchdown -- the
      ``usage_box._standing_scrimmage`` semantics. No player rows.
    * ``success_by_down_distance``: every standing scrimmage play on downs 1-4 with
      a distance; success = ``EPA_success``; ``down`` is the second axis. No player
      rows. Distance follows ``_standing_scrimmage`` (clipped to 1-25, so ESPN's rare
      ``distance == 0`` lands in the 1-yard bucket).

    Args:
        pbp: released pbp plays, any number of seasons (project to ``FOOTBALL_ATTEMPT_COLUMNS``).

    Returns:
        pl.DataFrame: one row per attempt x metric, ``ATTEMPT_SCHEMA``; ids are text.

    Raises:
        TypeError: an id column is float (it would stringify as ``"123.0"``).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import load_cfb_pbp
            from sportsdataverse.metric_curves import FOOTBALL_ATTEMPT_COLUMNS, football_attempts

            pbp = load_cfb_pbp(2024).select(FOOTBALL_ATTEMPT_COLUMNS)
            att = football_attempts(pbp)
            att.filter(pl.col("metric") == "fg_pct_by_distance").head()

        Pipeline next step (one line)::

            att.group_by("metric").agg(pl.len(), pl.col("success").mean())

        See Also:
            * `cfbfastR`_ -- CFB pbp this adapter reads.
            * `nflfastR`_ -- the success / air-yards conventions mirrored here.

        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _nflfastR: https://www.nflfastr.com
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=ATTEMPT_SCHEMA)
    # project first: load_cfb_pbp also carries ESPN's raw start.down / start.distance,
    # which _standing_scrimmage would prefer over the repaired down / distance.
    plays = pbp.select([c for c in FOOTBALL_ATTEMPT_COLUMNS if c in pbp.columns])
    if "seasonType" in plays.columns:
        plays = plays.filter(pl.col("seasonType").cast(pl.Int64, strict=False).is_in(_ESPN_SEASON_TYPES))
    team = dict(team_id=_as_id(plays, "pos_team_id"), team_name=pl.col("pos_team"), id_source="espn")
    frames = []
    fg = plays.filter((pl.col("fg_attempt") == True) & pl.col("yds_fg").is_not_null())  # noqa: E712
    frames.append(
        _attempts(
            fg,
            "fg_pct_by_distance",
            x=pl.col("yds_fg"),
            success=pl.col("fg_made").fill_null(False),
            player_id=_as_id(fg, "fg_kicker_player_id"),
            player_name=pl.col("fg_kicker_player_name"),
            epa=pl.col("EPA"),
            **team,
        )
    )
    passes = plays.filter((pl.col("pass_attempt") == True) & pl.col("air_yards").is_not_null())  # noqa: E712
    for metric, success in (("cmp_pct_by_air_yards", "completion"), ("epa_by_air_yards", "EPA_success")):
        frames.append(
            _attempts(
                passes,
                metric,
                x=pl.col("air_yards"),
                success=pl.col(success),
                player_id=_as_id(passes, "passer_player_id"),
                player_name=pl.col("passer_player_name"),
                epa=pl.col("EPA"),
                **team,
            )
        )
    standing = _standing_scrimmage(plays).filter(
        pl.col("td_down").is_between(1, 4) & pl.col("td_distance").is_not_null()
    )
    fourth = standing.filter((pl.col("td_down") == 4) & ((pl.col("u_rush") == True) | (pl.col("u_pass") == True)))  # noqa: E712
    frames.append(
        _attempts(
            fourth,
            "fourth_conv_by_ytg",
            x=pl.col("td_distance"),
            success=pl.col("converted"),
            player_id=_NO_PLAYER,
            player_name=_NO_PLAYER,
            epa=pl.col("u_EPA"),
            **team,
        )
    )
    frames.append(
        _attempts(
            standing,
            "success_by_down_distance",
            x=pl.col("td_distance"),
            success=pl.col("u_EPA_success"),
            player_id=_NO_PLAYER,
            player_name=_NO_PLAYER,
            down=pl.col("td_down"),
            epa=pl.col("u_EPA"),
            **team,
        )
    )
    return _finish(frames)


def nflfastr_attempts(pbp: pl.DataFrame) -> pl.DataFrame:
    """The same attempts from ``nfl_model_pbp`` (the nflfastR shape), which carries air yards.

    Populations (``REG`` + ``POST``):

    * ``fg_pct_by_distance``: ``field_goal_attempt`` with a ``kick_distance``; success =
      ``field_goal_result == "made"``; player = the kicker.
    * ``cmp_pct_by_air_yards`` / ``epa_by_air_yards``: ``pass_attempt == 1 & sack == 0``
      with ``air_yards``; success = ``complete_pass`` / ``epa > 0`` (nflfastR's success);
      player = the passer.
    * ``fourth_conv_by_ytg``: fourth-down ``play_type`` ``pass`` / ``run`` (a nullified
      play is ``no_play``); success = a rushing or passing first down or the
      offense's own touchdown. No player rows.
    * ``success_by_down_distance``: every ``pass`` / ``run`` play on downs 1-4 with a
      ``ydstogo``; success = ``epa > 0``. No player rows.

    Player ids are nflfastR gsis ids; the producer re-keys them to ESPN through the
    players master and keeps ``gsis_id`` beside ``entity_id``. Teams are nflfastR
    abbreviations.

    Args:
        pbp: ``load_nfl_model_pbp`` plays, any number of seasons (project to ``NFLFASTR_ATTEMPT_COLUMNS``).

    Returns:
        pl.DataFrame: one row per attempt x metric, ``ATTEMPT_SCHEMA``.

    Raises:
        TypeError: an id column is float.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl import load_nfl_model_pbp
            from sportsdataverse.metric_curves import NFLFASTR_ATTEMPT_COLUMNS, metric_curves, nflfastr_attempts

            pbp = load_nfl_model_pbp([2024]).select(NFLFASTR_ATTEMPT_COLUMNS)
            curves = metric_curves(nflfastr_attempts(pbp), "nfl")
            curves.filter((pl.col("metric") == "cmp_pct_by_air_yards") & (pl.col("entity_type") == "league"))

        See Also:
            * `nflfastR`_ -- the pbp shape and success definition this adapter reads.

        .. _nflfastR: https://www.nflfastr.com
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=ATTEMPT_SCHEMA)
    plays = pbp
    if "season_type" in plays.columns:
        plays = plays.filter(pl.col("season_type").is_in(_NFLFASTR_SEASON_TYPES))
    team = dict(team_id=_as_id(plays, "posteam"), team_name=pl.col("posteam"), id_source="gsis")
    frames = []
    fg = plays.filter((pl.col("field_goal_attempt") == 1) & pl.col("kick_distance").is_not_null())
    frames.append(
        _attempts(
            fg,
            "fg_pct_by_distance",
            x=pl.col("kick_distance"),
            success=pl.col("field_goal_result") == "made",
            player_id=_as_id(fg, "kicker_player_id"),
            player_name=pl.col("kicker_player_name"),
            epa=pl.col("epa"),
            **team,
        )
    )
    passes = plays.filter((pl.col("pass_attempt") == 1) & (pl.col("sack") == 0) & pl.col("air_yards").is_not_null())
    for metric, success in (
        ("cmp_pct_by_air_yards", pl.col("complete_pass") == 1),
        ("epa_by_air_yards", pl.col("epa") > 0),
    ):
        frames.append(
            _attempts(
                passes,
                metric,
                x=pl.col("air_yards"),
                success=success,
                player_id=_as_id(passes, "passer_player_id"),
                player_name=pl.col("passer_player_name"),
                epa=pl.col("epa"),
                **team,
            )
        )
    standing = plays.filter(
        pl.col("play_type").is_in(["pass", "run"]) & pl.col("down").is_between(1, 4) & pl.col("ydstogo").is_not_null()
    )
    fourth = standing.filter(pl.col("down") == 4)
    frames.append(
        _attempts(
            fourth,
            "fourth_conv_by_ytg",
            x=pl.col("ydstogo"),
            success=(pl.col("first_down_rush") == 1)
            | (pl.col("first_down_pass") == 1)
            | ((pl.col("touchdown") == 1) & (pl.col("td_team") == pl.col("posteam"))),
            player_id=_NO_PLAYER,
            player_name=_NO_PLAYER,
            epa=pl.col("epa"),
            **team,
        )
    )
    frames.append(
        _attempts(
            standing,
            "success_by_down_distance",
            x=pl.col("ydstogo"),
            success=pl.col("epa") > 0,
            player_id=_NO_PLAYER,
            player_name=_NO_PLAYER,
            down=pl.col("down"),
            epa=pl.col("epa"),
            **team,
        )
    )
    return _finish(frames)


def shot_attempts(shots: pl.DataFrame, league: str = "nba") -> pl.DataFrame:
    """``fg_pct_by_shot_distance`` attempts from released ``{nba,wnba}_stats_shots``.

    Regular-season (``season_type_id`` ``"2"``) and playoff (``"4"``) shots with a
    ``shot_distance`` (feet); success = ``shot_result == "Made"``; player = the
    shooter (stats.nba ``person_id``); no EPA.

    Args:
        shots: ``load_{nba,wnba}_stats_shots`` rows, any number of seasons (project to ``SHOT_ATTEMPT_COLUMNS``).
        league: ``"nba"`` (default) or ``"wnba"`` -- which stats site the ids come from; stamps
            ``id_source`` ``nba_stats`` / ``wnba_stats``.

    Returns:
        pl.DataFrame: one row per shot, ``ATTEMPT_SCHEMA``; ``season`` is the asset's key (END year for the NBA).

    Raises:
        TypeError: an id column is float.
        ValueError: ``league`` is not ``"nba"`` or ``"wnba"``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba import load_nba_stats_shots
            from sportsdataverse.metric_curves import SHOT_ATTEMPT_COLUMNS, metric_curves, shot_attempts

            shots = load_nba_stats_shots([2024]).select(SHOT_ATTEMPT_COLUMNS)
            curves = metric_curves(shot_attempts(shots), "nba")
            curves.filter((pl.col("entity_type") == "player") & (pl.col("entity_id") == "201939"))

        See Also:
            * `hoopR`_ -- the stats.nba shot chart loaders this mirrors.

        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    if league not in ("nba", "wnba"):
        raise ValueError(f"league must be 'nba' or 'wnba', got {league!r}")
    if shots.height == 0:
        return pl.DataFrame(schema=ATTEMPT_SCHEMA)
    made = shots.filter(
        pl.col("season_type_id").cast(pl.Utf8).is_in(_STATS_SEASON_TYPES)
        & pl.col("shot_distance").is_not_null()
        & pl.col("shot_result").is_in(["Made", "Missed"])
    )
    return _finish(
        [
            _attempts(
                made,
                "fg_pct_by_shot_distance",
                x=pl.col("shot_distance"),
                success=pl.col("shot_result") == "Made",
                player_id=_as_id(made, "person_id"),
                player_name=pl.col("player_name"),
                team_id=_as_id(made, "team_id"),
                team_name=pl.col("team_tricode"),
                id_source=ID_SOURCE[league],
            )
        ]
    )


def _edges() -> pl.DataFrame:
    rows = [(m, float(lo), float(hi)) for m, e in BUCKET_EDGES.items() for lo, hi in zip(e[:-1], e[1:])]
    return pl.DataFrame(rows, schema={"metric": pl.Utf8, "x_lo": pl.Float64, "x_hi": pl.Float64}, orient="row")


_BUCKET = ["season", "metric", "down", "x_lo", "x_hi"]


def _curve(df: pl.DataFrame, entity_type: str, by: str | None) -> pl.DataFrame:
    """One entity type's rows: the bucket aggregates plus the entity's labels."""
    player = entity_type == "player"
    none = pl.lit(None, dtype=pl.Utf8)
    name = pl.col("player_name" if player else "team_name").drop_nulls().first() if by else none
    return (
        df.group_by([*_BUCKET, *([by] if by else [])])
        .agg(
            entity_name=name,
            # a traded player's row is labelled with the team of most of their attempts
            _team=pl.col("team_id").drop_nulls().mode().sort().first() if player else none,
            attempts=pl.len().cast(pl.Int64),
            successes=pl.col("success").cast(pl.Int64).sum(),
            epa_per_att=pl.col("epa").mean(),
        )
        .select(
            *_BUCKET,
            pl.lit(entity_type).alias("entity_type"),
            (pl.col(by) if by else none).alias("entity_id"),
            "entity_name",
            pl.col("_team").alias("team_id"),
            "attempts",
            "successes",
            "epa_per_att",
        )
    )


def metric_curves(attempts: pl.DataFrame, league: str) -> pl.DataFrame:
    """League, team and player rate curves from an ``ATTEMPT_SCHEMA`` frame.

    Args:
        attempts: one row per attempt x metric (from an adapter), any number of seasons.
        league: ``"cfb"``, ``"nfl"``, ``"nba"`` or ``"wnba"``. The curves are league-agnostic;
            ``league`` only supplies the default ``id_source`` (:data:`ID_SOURCE`) when the
            attempts frame carries no ``id_source`` column. An adapter's column wins, so the
            ESPN adapter on NFL pbp keeps ``espn`` whatever ``league`` says.

    Returns:
        pl.DataFrame: one row per (season, entity, metric, down, bucket), ``OUTPUT_SCHEMA``:

        * ``entity_type`` ``league`` (``entity_id`` null), ``team`` and ``player`` (only
          attempts credited to a player; ``team_id`` is the team of most of them).
        * ``x_lo`` / ``x_hi``: the attempt's bucket, inclusive / exclusive.
        * ``attempts``, ``successes``, ``rate = successes / attempts`` (exact),
          ``epa_per_att`` (mean EPA of the attempts that have one, else null).
        * ``down``: only set for ``success_by_down_distance``.

        A bucket with no attempt has no row.

    Raises:
        TypeError: ``player_id`` / ``team_id`` is float.
        ValueError: unknown ``league``, a ``metric`` with no bucket edges, or an attempts frame
            that mixes id namespaces (more than one ``id_source``).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb import load_cfb_pbp
            from sportsdataverse.metric_curves import FOOTBALL_ATTEMPT_COLUMNS, football_attempts, metric_curves

            pbp = load_cfb_pbp(2024).select(FOOTBALL_ATTEMPT_COLUMNS)
            curves = metric_curves(football_attempts(pbp), "cfb")
            curves.filter((pl.col("metric") == "fg_pct_by_distance") & (pl.col("entity_type") == "league"))

        Pipeline next step (one line)::

            curves.filter((pl.col("entity_type") == "player") & (pl.col("attempts") >= 10)).sort("rate", descending=True)

        See Also:
            * :func:`football_attempts`, :func:`nflfastr_attempts`, :func:`shot_attempts` -- the
              adapters that build ``attempts`` and stamp its ``id_source``.
            * :func:`sportsdataverse.rolling_windows.rolling_windows` -- the sibling event-window
              derivation with the same id discipline and entity labels.
            * `nflfastR`_ -- the success and air-yards conventions the football curves mirror.

        .. _nflfastR: https://www.nflfastr.com
    """
    if league not in ID_SOURCE:
        raise ValueError(f"league must be one of {sorted(ID_SOURCE)}, got {league!r}")
    if attempts.height == 0:
        return pl.DataFrame(schema=OUTPUT_SCHEMA)
    id_source = ID_SOURCE[league]
    if "id_source" in attempts.columns:
        sources = sorted(attempts["id_source"].drop_nulls().unique().to_list())
        if len(sources) > 1:
            raise ValueError(f"attempts mix id namespaces: {sources}")
        id_source = sources[0] if sources else id_source
    cols = {k: v for k, v in ATTEMPT_SCHEMA.items() if k != "id_source"}
    att = attempts.select(
        *[c for c in cols if c not in ("player_id", "team_id")],
        _as_id(attempts, "player_id").alias("player_id"),
        _as_id(attempts, "team_id").alias("team_id"),
    ).cast(cols)
    unknown = sorted(set(att["metric"].unique()) - set(BUCKET_EDGES))
    if unknown:
        raise ValueError(f"metric(s) without bucket edges: {unknown}")
    edges = _edges()
    assert att.schema["metric"] == edges.schema["metric"]
    binned = att.join(edges, on="metric", how="inner").filter(
        (pl.col("x") >= pl.col("x_lo")) & (pl.col("x") < pl.col("x_hi"))
    )
    parts = [
        _curve(binned, "league", None),
        _curve(binned.filter(pl.col("team_id").is_not_null()), "team", "team_id"),
        _curve(binned.filter(pl.col("player_id").is_not_null()), "player", "player_id"),
    ]
    out = pl.concat(parts).with_columns(
        rate=pl.col("successes") / pl.col("attempts"),
        id_source=pl.lit(id_source),
    )
    return (
        out.select(list(OUTPUT_SCHEMA))
        .cast(OUTPUT_SCHEMA)
        .sort(["metric", "entity_type", "entity_id", "season", "down", "x_lo"], nulls_last=False)
    )
