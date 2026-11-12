"""The ten players on the floor for every shot (BB-D36): the core behind ``{lg}.shot_lineups``.

One row per field-goal attempt of the shots release, with the shooting side's five (``off_player_1``
.. ``off_player_5``) and the defending side's five (``def_player_1`` .. ``def_player_5``):

* **NBA / WNBA** (:func:`pro_shot_lineups`): a ``{lg}_stats`` shot takes the lineups of the
  ``{lg}_stats`` possession of the shooting team that holds its clock (same game and period, the
  team on offense, ``end_seconds_remaining <= clock <= start_seconds_remaining``). A shot on the
  boundary of two such possessions takes the later one, the possession it opens.
* **College** (:func:`ncaa_shot_lineups`): a stats.ncaa.org shot-chart shot takes the lineup of its
  play-by-play field-goal event: same contest, period, seconds left, shooting team, result and
  value, the n-th shot of that key in chart order to the n-th event in game order. Under a minute
  the chart's clock has tenths and the play-by-play's whole seconds, so the key is the chart's
  seconds floored. The chart carries the clock from the 2026-27 parse on; a release shot with no
  clock is unmatched.

``matched`` says a possession or event was found; ``full_lineup`` that all ten ids are known (a
college non-D-I side has no stats.ncaa.org ids, so its shots are never full). Ids keep the
release's types: Int64 person ids for the pros, String stats.ncaa.org player ids for college.
Coordinates are tenths of a foot from the hoop (the ``x_legacy`` / ``y_legacy`` frame; college
feet are scaled as ``{lg}.shot_rows`` does, ``floor(10 * x + 0.5)``).

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package.
"""

from __future__ import annotations

import polars as pl

SLOTS = tuple(f"{side}_player_{i}" for side in ("off", "def") for i in range(1, 6))


def shot_lineup_schema(player: pl.DataType) -> dict[str, pl.DataType]:
    """The ``shot_lineups`` columns; ``player`` is the id dtype (Int64 pro, String college)."""
    return {
        "season": pl.Int64(),
        "game_id": pl.Utf8(),
        "espn_game_id": pl.Int64(),
        "period": pl.Int64(),
        "clock_seconds": pl.Float64(),
        "team_id": pl.Int64(),
        "opp_team_id": pl.Int64(),
        "shooter_id": player,
        "shot_value": pl.Int64(),
        "made": pl.Boolean(),
        "x_legacy": pl.Int64(),
        "y_legacy": pl.Int64(),
        **dict.fromkeys(SLOTS, player),
        "matched": pl.Boolean(),
        "full_lineup": pl.Boolean(),
    }


def _finish(df: pl.DataFrame, player: pl.DataType) -> pl.DataFrame:
    schema = shot_lineup_schema(player)
    full = pl.all_horizontal([pl.col(c).is_not_null() for c in SLOTS])
    return df.with_columns(pl.col("matched").fill_null(False), full.alias("full_lineup")).select(
        [pl.col(c).cast(t) for c, t in schema.items()]
    )


def _int(col: str) -> pl.Expr:
    """An id or count that may arrive as text ("1", "1171.0") or a number."""
    return pl.col(col).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64)


def pro_shot_lineups(shots: pl.DataFrame, possessions: pl.DataFrame) -> pl.DataFrame:
    """Each NBA / WNBA shot with the ten players of the possession it was taken in.

    Args:
        shots: ``load_nba_stats_shots`` / ``load_wnba_stats_shots`` rows (``game_id``, ``season``,
            ``period``, ``clock`` as ``PT11M36.00S``, ``team_id``, ``person_id``, ``shot_value``,
            ``shot_result``, ``x_legacy``, ``y_legacy``).
        possessions: ``load_nba_stats_possessions`` / ``load_wnba_stats_possessions`` rows
            (``game_id``, ``period``, ``offense_team_id``, ``defense_team_id``,
            ``start_seconds_remaining``, ``end_seconds_remaining``, ``off_player_1..5``,
            ``def_player_1..5``).

    Returns:
        One row per shot in :func:`shot_lineup_schema` (Int64 ids); ``espn_game_id`` is null.
    """
    clock = pl.col("clock").str.extract_groups(r"^PT(\d+)M([\d.]+)S$")
    s = shots.with_row_index("_row").select(
        "_row",
        pl.col("season").cast(pl.Int64),
        pl.col("game_id").cast(pl.Utf8),
        pl.lit(None, pl.Int64).alias("espn_game_id"),
        pl.col("period").cast(pl.Int64),
        (clock.struct.field("1").cast(pl.Float64) * 60 + clock.struct.field("2").cast(pl.Float64)).alias(
            "clock_seconds"
        ),
        pl.col("team_id").cast(pl.Int64),
        pl.col("person_id").cast(pl.Int64).alias("shooter_id"),
        pl.col("shot_value").cast(pl.Int64),
        (pl.col("shot_result") == "Made").alias("made"),
        pl.col("x_legacy").cast(pl.Int64),
        pl.col("y_legacy").cast(pl.Int64),
    )
    p = possessions.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("period").cast(pl.Int64),
        pl.col("offense_team_id").cast(pl.Int64).alias("team_id"),
        pl.col("defense_team_id").cast(pl.Int64).alias("opp_team_id"),
        pl.col("start_seconds_remaining").cast(pl.Float64).alias("_start"),
        pl.col("end_seconds_remaining").cast(pl.Float64).alias("_end"),
        *[pl.col(c).cast(pl.Int64) for c in SLOTS],
    )
    assert s.schema["game_id"] == p.schema["game_id"] and s.schema["team_id"] == p.schema["team_id"]
    hit = (
        s.join(p, on=["game_id", "period", "team_id"], how="inner")
        .filter(pl.col("clock_seconds").is_between(pl.col("_end"), pl.col("_start")))
        .sort("_row", "_start")  # the boundary shot takes the later possession (lowest clock at start)
        .unique("_row", keep="first", maintain_order=True)
        .select("_row", *SLOTS, pl.lit(True).alias("matched"))
    )
    opp = p.select("game_id", "team_id", "opp_team_id").unique(["game_id", "team_id"])
    return _finish(
        s.join(hit, on="_row", how="left").join(opp, on=["game_id", "team_id"], how="left").sort("_row"), pl.Int64()
    )


def ncaa_shot_lineups(shots: pl.DataFrame, pbp: pl.DataFrame) -> pl.DataFrame:
    """Each stats.ncaa.org shot-chart shot with the ten players of its play-by-play event.

    Args:
        shots: ``load_ncaa_mbb_shots`` / ``load_ncaa_wbb_shots`` rows in chart order (``season``,
            ``contest_id``, ``espn_game_id``, ``team_id`` (the team's name), ``espn_team_id``,
            ``shooter_player_id``, ``period``, ``sec_left``, ``made``, ``point_value``, ``shot_x``,
            ``shot_y``).
        pbp: ``load_ncaa_mbb_pbp`` / ``load_ncaa_wbb_pbp`` rows in game order (``contest_id``,
            ``period``, ``clock`` as ``MM:SS``, ``event_team``, ``event_result``, ``shot_value``,
            ``home``, ``home_espn_team_id``, ``away_espn_team_id``, ``home_k_player_id`` /
            ``away_k_player_id``).

    Returns:
        One row per shot in :func:`shot_lineup_schema` (String ids); ``game_id`` is the contest id.
    """
    key = ["game_id", "period", "_sec", "_team", "made", "shot_value"]
    sec_left = pl.col("sec_left").cast(pl.Utf8).cast(pl.Float64, strict=False)
    s = shots.with_row_index("_row").select(
        "_row",
        pl.col("season").cast(pl.Int64),
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        _int("espn_game_id").alias("espn_game_id"),
        _int("period").alias("period"),
        sec_left.alias("clock_seconds"),
        # under a minute the chart has tenths (59.7) and the pbp clock whole seconds (0:59)
        sec_left.floor().alias("_sec"),
        pl.col("team_id").cast(pl.Utf8).alias("_team"),
        _int("espn_team_id").alias("team_id"),
        pl.col("shooter_player_id").cast(pl.Utf8).alias("shooter_id"),
        pl.col("point_value").cast(pl.Int64).alias("shot_value"),
        pl.col("made").cast(pl.Boolean),
        (10 * pl.col("shot_x").cast(pl.Float64) + 0.5).floor().cast(pl.Int64).alias("x_legacy"),
        (10 * pl.col("shot_y").cast(pl.Float64) + 0.5).floor().cast(pl.Int64).alias("y_legacy"),
    )
    mmss = pl.col("clock").str.extract_groups(r"^(\d+):(\d+)$")
    home = pl.col("event_team") == pl.col("home")
    side = {"off": ("home", "away"), "def": ("away", "home")}
    e = (
        pbp.filter(pl.col("shot_value").cast(pl.Int64) >= 2, pl.col("event_result").is_in(["made", "missed"]))
        .select(
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            pl.col("period").cast(pl.Int64),
            (mmss.struct.field("1").cast(pl.Float64) * 60 + mmss.struct.field("2").cast(pl.Float64)).alias("_sec"),
            pl.col("event_team").cast(pl.Utf8).alias("_team"),
            (pl.col("event_result") == "made").alias("made"),
            pl.col("shot_value").cast(pl.Int64),
            *[
                pl.when(home)
                .then(pl.col(f"{h}_{i}_player_id"))
                .otherwise(pl.col(f"{a}_{i}_player_id"))
                .cast(pl.Utf8)
                .alias(f"{k}_player_{i}")
                for k, (h, a) in side.items()
                for i in range(1, 6)
            ],
        )
        .with_columns(pl.int_range(pl.len()).over(key).alias("_n"), pl.lit(True).alias("matched"))
    )
    s = s.with_columns(pl.int_range(pl.len()).over(key).alias("_n"))
    assert all(s.schema[c] == e.schema[c] for c in [*key, "_n"])
    # the opponent of every shot, matched or not: each contest's two sides from its pbp header columns
    g = pbp.select(
        pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
        pl.col("home").cast(pl.Utf8),
        pl.col("away").cast(pl.Utf8),
        _int("home_espn_team_id").alias("h"),
        _int("away_espn_team_id").alias("a"),
    ).unique(["game_id"])
    opp = pl.concat(
        [
            g.select("game_id", pl.col("home").alias("_team"), pl.col("a").alias("opp_team_id")),
            g.select("game_id", pl.col("away").alias("_team"), pl.col("h").alias("opp_team_id")),
        ]
    ).unique(["game_id", "_team"])
    out = s.join(e, on=[*key, "_n"], how="left").join(opp, on=["game_id", "_team"], how="left").sort("_row")
    return _finish(out, pl.Utf8())


def shot_lineup_coverage(df: pl.DataFrame) -> pl.DataFrame:
    """Per season: shots, the share matched, and the share with a full five a side.

    The gate's denominator is the ``eligible`` shots: both teams carry an id (college: both sides
    D-I) and at least one of the game's shots matched, the stand-in for "the game has lineups" (a
    pro game missing from the possessions release, or a college season whose chart has no clock,
    has no matched shot). A matched shot can still lack an id, so it is not always full.
    ``shooter_in_five_share`` is the share of eligible matched shots whose shooter is one of his
    side's five.

    Returns:
        One row per season: ``season``, ``shots``, ``matched_share``, ``eligible_shots``,
        ``eligible_full_share`` (null with no eligible shot) and ``shooter_in_five_share``.
    """
    has_lineups = pl.col("matched").any().over("game_id")
    eligible = pl.col("team_id").is_not_null() & pl.col("opp_team_id").is_not_null() & has_lineups
    in_five = pl.any_horizontal([pl.col(f"off_player_{i}") == pl.col("shooter_id") for i in range(1, 6)])
    return (
        df.with_columns(eligible.alias("_eligible"), in_five.fill_null(False).alias("_in_five"))
        .group_by("season")
        .agg(
            pl.len().alias("shots"),
            pl.col("matched").mean().alias("matched_share"),
            pl.col("_eligible").sum().alias("eligible_shots"),
            pl.col("full_lineup").filter(pl.col("_eligible")).mean().alias("eligible_full_share"),
            pl.col("_in_five")
            .filter(pl.col("_eligible") & pl.col("matched") & pl.col("shooter_id").is_not_null())
            .mean()
            .alias("shooter_in_five_share"),
        )
        .sort("season")
    )
