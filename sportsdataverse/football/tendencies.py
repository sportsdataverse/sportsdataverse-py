"""Team / coach tendencies from released play-by-play: pace, run-pass, efficiency, finishing, fourth downs.

League-agnostic (the CFB and NFL processors emit the same columns). One
function, :func:`tendencies`, turns any slice of released ``espn_{league}_pbp``
plays into one row per group key -- a team-season by default, a coach-season
when the caller has joined a ``coach`` column onto the plays -- carrying
COUNTS and the rates built from them, so :func:`aggregate_tendencies` can sum
seasons into play-weighted careers without averaging averages.

Column families (offense unless prefixed ``def_``, which is the same metric
allowed while the group's DEFENSE was on the field):

* volume -- ``games``, ``plays``, ``drives``, ``plays_per_game``,
  ``plays_per_drive``, ``drives_per_game``
* pace -- drive-level: ESPN's own elapsed drive clock over its own offensive
  play count (``sec_per_play``), and the same on situation-neutral drives
  (``sec_per_play_neutral``); ``pace_coverage`` is the share of drives with
  a usable clock, so a season whose clock is sparse can be excluded.
* run / pass -- ``pass_rate`` overall, ``pass_rate_neutral`` (win probability
  20-80%, first four quarters, outside the last two minutes of a half),
  by down (``pass_rate_d1..d4``), early / standard / passing downs, and by
  score state (``pass_rate_leading`` / ``_tied`` / ``_trailing``), each with
  its play count.
* efficiency -- EPA per play, success rate, yards per play, explosive rate,
  with rush / pass splits; early-down EPA; third downs converted and over
  expected (the league's bundled distance curve).
* finishing -- red-zone and scoring-opportunity trips, touchdown rate,
  conversion rate (TD or FG), points per trip; scripted (first two drives
  of each half) vs non-scripted EPA per play, success rate, points per drive.
* fourth down -- decisions (a fourth-down rush, pass, punt or field goal
  that stood; timeouts and penalties are not decisions), went (rush / pass),
  model go, go rate, go rate when the model says go, go rate when it says
  kick, agreement rate, and the win probability left on the table per
  decision (``go_boost`` on the decisions that went against the model).

Example:
    Team-seasons from one released season::

        import polars as pl
        from sportsdataverse.football.tendencies import tendencies
        pbp = pl.read_parquet(".../play_by_play_2024.parquet")
        teams = tendencies(pbp, league="nfl")
        teams.sort("pass_rate_neutral", descending=True).select("pos_team", "pass_rate_neutral").head()

    Coach-seasons, once a ``coach`` (and ``def_coach``) column is on the plays::

        coaches = tendencies(
            pbp.with_columns(coach=..., def_coach=...),
            group_cols=("season", "pos_team", "coach"),
            def_group_cols=("season", "def_pos_team", "def_coach"),
        )
"""

from __future__ import annotations

from typing import Optional

import polars as pl

from sportsdataverse.football.usage_box import load_third_down_curve

__all__ = ["aggregate_tendencies", "tendencies"]

#: (rate column, numerator count, denominator count) -- the contract that lets
#: careers be summed then re-rated
RATES: tuple[tuple[str, str, str], ...] = (
    ("plays_per_game", "plays", "games"),
    ("plays_per_drive", "plays", "drives"),
    ("drives_per_game", "drives", "games"),
    ("sec_per_play", "drive_seconds", "drive_plays"),
    ("sec_per_play_neutral", "drive_seconds_neutral", "drive_plays_neutral"),
    ("pace_coverage", "drives_with_clock", "drives"),
    ("pass_rate", "passes", "plays"),
    ("pass_rate_neutral", "passes_neutral", "plays_neutral"),
    ("pass_rate_d1", "passes_d1", "plays_d1"),
    ("pass_rate_d2", "passes_d2", "plays_d2"),
    ("pass_rate_d3", "passes_d3", "plays_d3"),
    ("pass_rate_d4", "passes_d4", "plays_d4"),
    ("pass_rate_early_down", "passes_early_down", "plays_early_down"),
    ("pass_rate_standard_down", "passes_standard_down", "plays_standard_down"),
    ("pass_rate_passing_down", "passes_passing_down", "plays_passing_down"),
    ("pass_rate_leading", "passes_leading", "plays_leading"),
    ("pass_rate_tied", "passes_tied", "plays_tied"),
    ("pass_rate_trailing", "passes_trailing", "plays_trailing"),
    ("pass_rate_first_half", "passes_first_half", "plays_first_half"),
    ("pass_rate_second_half", "passes_second_half", "plays_second_half"),
    ("epa_per_play", "epa", "plays"),
    ("epa_per_rush", "epa_rush", "rushes"),
    ("epa_per_pass", "epa_pass", "passes"),
    ("epa_per_play_early_down", "epa_early_down", "plays_early_down"),
    ("epa_per_play_neutral", "epa_neutral", "plays_neutral"),
    ("success_rate", "successes", "plays"),
    ("success_rate_rush", "successes_rush", "rushes"),
    ("success_rate_pass", "successes_pass", "passes"),
    ("ypp", "yards", "plays"),
    ("ypp_rush", "yards_rush", "rushes"),
    ("ypp_pass", "yards_pass", "passes"),
    ("explosive_rate", "explosives", "plays"),
    ("explosive_rate_rush", "explosives_rush", "rushes"),
    ("explosive_rate_pass", "explosives_pass", "passes"),
    ("third_down_rate", "third_down_conversions", "third_down_opportunities"),
    ("rz_trip_rate", "rz_trips", "drives"),
    ("rz_td_rate", "rz_tds", "rz_trips"),
    ("rz_conversion_rate", "rz_scores", "rz_trips"),
    ("rz_pts_per_trip", "rz_points", "rz_trips"),
    ("so_trip_rate", "so_trips", "drives"),
    ("so_td_rate", "so_tds", "so_trips"),
    ("so_conversion_rate", "so_scores", "so_trips"),
    ("so_pts_per_trip", "so_points", "so_trips"),
    ("pts_per_drive", "drive_points", "drives"),
    ("scripted_epa_per_play", "scripted_epa", "scripted_plays"),
    ("scripted_success_rate", "scripted_successes", "scripted_plays"),
    ("scripted_pts_per_drive", "scripted_points", "scripted_drives"),
    ("non_scripted_epa_per_play", "non_scripted_epa", "non_scripted_plays"),
    ("non_scripted_success_rate", "non_scripted_successes", "non_scripted_plays"),
    ("non_scripted_pts_per_drive", "non_scripted_points", "non_scripted_drives"),
    ("go_rate", "fourth_went", "fourth_decisions"),
    ("go_rate_when_model_says_go", "fourth_went_when_go", "fourth_model_go"),
    ("go_rate_when_model_says_kick", "fourth_went_when_kick", "fourth_model_kick"),
    ("fourth_agreement_rate", "fourth_agreed", "fourth_decisions"),
    ("fourth_wp_left_per_decision", "fourth_wp_left", "fourth_decisions"),
    ("fourth_conversion_rate", "fourth_converted", "fourth_went"),
)
_DIFF_RATES: tuple[tuple[str, str, str], ...] = (
    ("third_down_over_expected", "third_down_conversions", "third_down_expected"),
)

_DRIVE_PTS = (
    pl.when(pl.col("drive.result") == "TD").then(7.0).when(pl.col("drive.result") == "FG").then(3.0).otherwise(0.0)
)
_SCRIPTED_DRIVES_PER_HALF = 2


def _col(df: pl.DataFrame, *names: str) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None


def _clock_seconds(expr: pl.Expr) -> pl.Expr:
    """``"7:13"`` -> 433.0; unparsable -> null."""
    parts = expr.cast(pl.Utf8).str.split(":")
    return (
        pl.when(parts.list.len() == 2)
        .then(parts.list.get(0).cast(pl.Float64, strict=False) * 60 + parts.list.get(1).cast(pl.Float64, strict=False))
        .otherwise(None)
    )


def _prepare(plays: pl.DataFrame, curve: Optional[pl.DataFrame]) -> pl.DataFrame:
    """Type and flag every play once; the aggregations only read the ``t_*`` columns."""
    df = plays
    b = lambda c: pl.col(c).cast(pl.Boolean, strict=False).fill_null(False) if c in df.columns else pl.lit(False)  # noqa: E731
    f = lambda c: pl.col(c).cast(pl.Float64, strict=False) if c in df.columns else pl.lit(None, dtype=pl.Float64)  # noqa: E731
    down = _col(df, "start.down", "down")
    dist = _col(df, "start.distance", "distance")
    period = _col(df, "period", "period.number")
    exprs: list[pl.Expr] = [
        b("scrimmage_play").alias("t_scrimmage"),
        b("penalty_no_play").alias("t_no_play"),
        b("rush").alias("t_rush"),
        b("pass").alias("t_pass"),
        b("punt").alias("t_punt"),
        b("fg_attempt").alias("t_fg"),
        b("EPA_success").alias("t_success"),
        b("EPA_explosive").alias("t_explosive"),
        b("first_down_created").alias("t_first_down"),
        b("touchdown").alias("t_touchdown"),
        b("rz_play").alias("t_rz"),
        b("scoring_opp").alias("t_so"),
        b("standard_down").alias("t_standard_down"),
        b("passing_down").alias("t_passing_down"),
        f("EPA").alias("t_epa"),
        f("statYardage").alias("t_yards"),
        f("wp_before").alias("t_wp"),
        f("pos_score_diff").alias("t_score_diff"),
        f("go_boost").alias("t_go_boost"),
        (pl.col(down).cast(pl.Int64, strict=False) if down else pl.lit(None, dtype=pl.Int64)).alias("t_down"),
        (pl.col(dist).cast(pl.Float64, strict=False) if dist else pl.lit(None, dtype=pl.Float64)).alias("t_distance"),
        (pl.col(period).cast(pl.Int64, strict=False) if period else pl.lit(None, dtype=pl.Int64)).alias("t_period"),
        (
            pl.col("fourth_down_recommendation").cast(pl.Utf8)
            if "fourth_down_recommendation" in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        ).alias("t_rec"),
        (pl.col("drive.id").cast(pl.Utf8) if "drive.id" in df.columns else pl.lit(None, dtype=pl.Utf8)).alias(
            "t_drive"
        ),
        (
            _clock_seconds(pl.col("drive.timeElapsed.displayValue"))
            if "drive.timeElapsed.displayValue" in df.columns
            else pl.lit(None, dtype=pl.Float64)
        ).alias("t_drive_seconds"),
        f("drive.offensivePlays").alias("t_drive_plays"),
        (_DRIVE_PTS if "drive.result" in df.columns else pl.lit(0.0)).alias("t_drive_pts"),
        (
            (
                pl.col("clock.minutes").cast(pl.Float64, strict=False) * 60
                + pl.col("clock.seconds").cast(pl.Float64, strict=False)
            )
            if {"clock.minutes", "clock.seconds"} <= set(df.columns)
            else pl.lit(None, dtype=pl.Float64)
        ).alias("t_clock"),
    ]
    df = df.with_columns(exprs)
    half = pl.when(pl.col("t_period") <= 2).then(1).when(pl.col("t_period") <= 4).then(2).otherwise(3)
    last_two = pl.col("t_clock").is_not_null() & (pl.col("t_clock") <= 120) & pl.col("t_period").is_in([2, 4])
    neutral = (pl.col("t_wp").is_between(0.2, 0.8) & (pl.col("t_period") <= 4) & ~last_two.fill_null(False)).fill_null(
        False
    )
    df = df.with_columns(
        t_half=half,
        t_neutral=neutral,
        t_standing=(pl.col("t_scrimmage") & ~pl.col("t_no_play")),
        t_leading=(pl.col("t_score_diff") > 0).fill_null(False),
        t_tied=(pl.col("t_score_diff") == 0).fill_null(False),
        t_trailing=(pl.col("t_score_diff") < 0).fill_null(False),
        t_early_down=pl.col("t_down").is_in([1, 2]).fill_null(False),
        t_third=(pl.col("t_down") == 3).fill_null(False),
        t_converted=(pl.col("t_first_down") | pl.col("t_touchdown")),
        # a fourth-down DECISION: the offense ran, threw, punted or kicked (and it stood)
        t_fourth_decision=(
            (pl.col("t_down") == 4)
            & ~pl.col("t_no_play")
            & (pl.col("t_rush") | pl.col("t_pass") | pl.col("t_punt") | pl.col("t_fg"))
        ).fill_null(False),
        t_fourth_went=((pl.col("t_down") == 4) & (pl.col("t_rush") | pl.col("t_pass"))).fill_null(False),
    )
    if curve is not None and curve.height and "t_distance" in df.columns:
        c = curve.select(pl.col("distance").cast(pl.Int64).alias("t_dist_key"), pl.col("rate").alias("t_expected"))
        df = df.with_columns(t_dist_key=pl.col("t_distance").clip(1, 25).round(0).cast(pl.Int64)).join(
            c, on="t_dist_key", how="left"
        )
    else:
        df = df.with_columns(t_expected=pl.lit(None, dtype=pl.Float64))
    return df


def _drive_frame(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    """One row per (keys, game, drive) from standing scrimmage plays."""
    d = df.filter(pl.col("t_standing") & pl.col("t_drive").is_not_null())
    if d.height == 0:
        return pl.DataFrame()
    order = _col(d, "game_play_number", "sequenceNumber", "id")
    drv = (
        d.with_columns(_ord=pl.col(order).cast(pl.Int64, strict=False) if order else pl.int_range(pl.len()))
        .group_by([*keys, "game_id", "t_drive"], maintain_order=True)
        .agg(
            half=pl.col("t_half").min(),
            first_play=pl.col("_ord").min(),
            plays=pl.len(),
            epa=pl.col("t_epa").sum(),
            successes=pl.col("t_success").sum(),
            neutral_start=pl.col("t_neutral").first(),
            rz=pl.col("t_rz").any(),
            so=pl.col("t_so").any(),
            touchdown=pl.col("t_touchdown").any(),
            pts=pl.col("t_drive_pts").first(),
            clock_seconds=pl.col("t_drive_seconds").first(),
            clock_plays=pl.col("t_drive_plays").first(),
        )
        .sort([*keys, "game_id", "half", "first_play"])
    )
    return drv.with_columns(
        drive_index=pl.int_range(pl.len()).over([*keys, "game_id", "half"]),
    ).with_columns(
        scripted=pl.col("drive_index") < _SCRIPTED_DRIVES_PER_HALF,
        has_clock=(pl.col("clock_seconds").is_not_null() & (pl.col("clock_plays") > 0)),
    )


def _offense_counts(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    p = df.filter(pl.col("t_standing"))
    rush, pas = pl.col("t_rush"), pl.col("t_pass")

    def _n(mask: pl.Expr) -> pl.Expr:
        return mask.sum()

    def _split(name: str, mask: pl.Expr) -> list[pl.Expr]:
        return [_n(mask).alias(f"plays_{name}"), _n(mask & pas).alias(f"passes_{name}")]

    aggs: list[pl.Expr] = [
        pl.col("game_id").n_unique().alias("games"),
        pl.len().alias("plays"),
        _n(rush).alias("rushes"),
        _n(pas).alias("passes"),
        pl.col("t_epa").sum().alias("epa"),
        pl.col("t_epa").filter(rush).sum().alias("epa_rush"),
        pl.col("t_epa").filter(pas).sum().alias("epa_pass"),
        pl.col("t_epa").filter(pl.col("t_early_down")).sum().alias("epa_early_down"),
        pl.col("t_epa").filter(pl.col("t_neutral")).sum().alias("epa_neutral"),
        _n(pl.col("t_success")).alias("successes"),
        _n(pl.col("t_success") & rush).alias("successes_rush"),
        _n(pl.col("t_success") & pas).alias("successes_pass"),
        pl.col("t_yards").sum().alias("yards"),
        pl.col("t_yards").filter(rush).sum().alias("yards_rush"),
        pl.col("t_yards").filter(pas).sum().alias("yards_pass"),
        _n(pl.col("t_explosive")).alias("explosives"),
        _n(pl.col("t_explosive") & rush).alias("explosives_rush"),
        _n(pl.col("t_explosive") & pas).alias("explosives_pass"),
        _n(pl.col("t_third")).alias("third_down_opportunities"),
        _n(pl.col("t_third") & pl.col("t_converted")).alias("third_down_conversions"),
        pl.when(pl.col("t_expected").filter(pl.col("t_third")).drop_nulls().len() > 0)
        .then(pl.col("t_expected").filter(pl.col("t_third")).sum())
        .otherwise(None)
        .alias("third_down_expected"),
        *_split("neutral", pl.col("t_neutral")),
        *_split("d1", pl.col("t_down") == 1),
        *_split("d2", pl.col("t_down") == 2),
        *_split("d3", pl.col("t_down") == 3),
        *_split("d4", pl.col("t_down") == 4),
        *_split("early_down", pl.col("t_early_down")),
        *_split("standard_down", pl.col("t_standard_down")),
        *_split("passing_down", pl.col("t_passing_down")),
        *_split("leading", pl.col("t_leading")),
        *_split("tied", pl.col("t_tied")),
        *_split("trailing", pl.col("t_trailing")),
        *_split("first_half", pl.col("t_half") == 1),
        *_split("second_half", pl.col("t_half") == 2),
    ]
    return p.group_by(keys, maintain_order=True).agg(aggs)


def _fourth_counts(df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    d = df.filter(pl.col("t_fourth_decision"))
    went, rec = pl.col("t_fourth_went"), pl.col("t_rec")
    says_go = rec == "go"
    says_kick = rec.is_in(["punt", "field_goal"])
    kicked = ~went
    against = (says_go & kicked) | (says_kick & went)
    # go_boost is go minus the best kick, so it is what a kick left when the
    # model said go and what a go left (negated) when the model said kick
    left = (
        pl.when(says_go & kicked)
        .then(pl.col("t_go_boost"))
        .when(says_kick & went)
        .then(-pl.col("t_go_boost"))
        .otherwise(0.0)
    )
    return d.group_by(keys, maintain_order=True).agg(
        fourth_decisions=pl.len(),
        fourth_went=went.sum(),
        fourth_converted=(went & pl.col("t_converted")).sum(),
        fourth_model_go=says_go.sum(),
        fourth_model_kick=says_kick.sum(),
        fourth_went_when_go=(says_go & went).sum(),
        fourth_went_when_kick=(says_kick & went).sum(),
        fourth_agreed=((says_go & went) | (says_kick & kicked)).sum(),
        fourth_wp_left=left.clip(lower_bound=0.0).fill_null(0.0).filter(against).sum(),
    )


def _drive_counts(drv: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
    if drv.height == 0:
        return pl.DataFrame()
    return drv.group_by(keys, maintain_order=True).agg(
        drives=pl.len(),
        drives_with_clock=pl.col("has_clock").sum(),
        drive_seconds=pl.col("clock_seconds").filter(pl.col("has_clock")).sum(),
        drive_plays=pl.col("clock_plays").filter(pl.col("has_clock")).sum(),
        drive_seconds_neutral=pl.col("clock_seconds").filter(pl.col("has_clock") & pl.col("neutral_start")).sum(),
        drive_plays_neutral=pl.col("clock_plays").filter(pl.col("has_clock") & pl.col("neutral_start")).sum(),
        drive_points=pl.col("pts").sum(),
        rz_trips=pl.col("rz").sum(),
        rz_tds=(pl.col("rz") & pl.col("touchdown")).sum(),
        rz_scores=(pl.col("rz") & (pl.col("pts") > 0)).sum(),
        rz_points=pl.col("pts").filter(pl.col("rz")).sum(),
        so_trips=pl.col("so").sum(),
        so_tds=(pl.col("so") & pl.col("touchdown")).sum(),
        so_scores=(pl.col("so") & (pl.col("pts") > 0)).sum(),
        so_points=pl.col("pts").filter(pl.col("so")).sum(),
        scripted_drives=pl.col("scripted").sum(),
        scripted_plays=pl.col("plays").filter(pl.col("scripted")).sum(),
        scripted_epa=pl.col("epa").filter(pl.col("scripted")).sum(),
        scripted_successes=pl.col("successes").filter(pl.col("scripted")).sum(),
        scripted_points=pl.col("pts").filter(pl.col("scripted")).sum(),
        non_scripted_drives=(~pl.col("scripted")).sum(),
        non_scripted_plays=pl.col("plays").filter(~pl.col("scripted")).sum(),
        non_scripted_epa=pl.col("epa").filter(~pl.col("scripted")).sum(),
        non_scripted_successes=pl.col("successes").filter(~pl.col("scripted")).sum(),
        non_scripted_points=pl.col("pts").filter(~pl.col("scripted")).sum(),
    )


def _apply_rates(df: pl.DataFrame, prefix: str = "") -> pl.DataFrame:
    exprs = []
    have = set(df.columns)
    for rate, num, den in RATES:
        n, d = f"{prefix}{num}", f"{prefix}{den}"
        if {n, d} <= have:
            exprs.append(pl.when(pl.col(d) > 0).then(pl.col(n) / pl.col(d)).otherwise(None).alias(f"{prefix}{rate}"))
    for rate, a, b_ in _DIFF_RATES:
        if {f"{prefix}{a}", f"{prefix}{b_}"} <= have:
            exprs.append((pl.col(f"{prefix}{a}") - pl.col(f"{prefix}{b_}")).alias(f"{prefix}{rate}"))
    return df.with_columns(exprs) if exprs else df


def _join_all(frames: list[pl.DataFrame], keys: list[str]) -> pl.DataFrame:
    out: Optional[pl.DataFrame] = None
    for f in frames:
        if f is None or f.height == 0:
            continue
        out = f if out is None else out.join(f, on=keys, how="full", coalesce=True)
    return out if out is not None else pl.DataFrame()


def tendencies(
    plays: pl.DataFrame,
    *,
    league: str = "cfb",
    group_cols: tuple[str, ...] = ("season", "pos_team"),
    def_group_cols: tuple[str, ...] = ("season", "def_pos_team"),
    third_down_curve: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """Tendency counts and rates per group from released plays.

    Args:
        plays: plays in the released ``espn_{league}_pbp`` shape (any number
            of games / seasons), optionally with extra grouping columns joined
            on (a ``coach`` on the offense side, a ``def_coach`` on the defense).
        league: ``"cfb"`` or ``"nfl"`` -- selects the bundled third-down curve.
        group_cols: the offense grouping (a team-season by default).
        def_group_cols: the defense grouping, one column per ``group_cols``
            entry in the same order; each is renamed onto its offense twin so
            the defense-allowed columns land on the same row
            (``def_pos_team`` -> ``pos_team``, ``def_coach`` -> ``coach``).
        third_down_curve: override the bundled curve.

    Returns:
        One row per group with the volume, pace, run-pass, efficiency,
        finishing and fourth-down columns (counts and rates) and their
        ``def_``-prefixed defense-allowed twins. Empty input -> empty frame.

    Example:
        Quick start::

            from sportsdataverse.football.tendencies import tendencies
            out = tendencies(pbp, league="cfb")
            out.select("pos_team", "sec_per_play_neutral", "pass_rate_neutral", "go_rate_when_model_says_go")
    """
    if not isinstance(plays, pl.DataFrame) or plays.height == 0:
        return pl.DataFrame()
    keys, dkeys = list(group_cols), list(def_group_cols)
    missing = [k for k in [*keys, *dkeys, "game_id"] if k not in plays.columns]
    if missing:
        raise ValueError(f"plays lack grouping columns: {missing}")
    curve = third_down_curve
    if curve is None:
        try:
            curve = load_third_down_curve(league)
        except (FileNotFoundError, OSError, ValueError):
            curve = None
    df = _prepare(plays, curve)

    off = _join_all(
        [_offense_counts(df, keys), _fourth_counts(df, keys), _drive_counts(_drive_frame(df, keys), keys)], keys
    )
    if off.height == 0:
        return pl.DataFrame()
    off = _apply_rates(off)

    # defense-allowed: the same offense aggregations grouped by the defending key
    d_off = _join_all([_offense_counts(df, dkeys), _drive_counts(_drive_frame(df, dkeys), dkeys)], dkeys)
    if d_off.height:
        d_off = d_off.rename({c: f"def_{c}" for c in d_off.columns if c not in dkeys})
        d_off = _apply_rates(d_off, prefix="def_")
        # every defending key maps positionally onto its offense key (def_pos_team
        # -> pos_team, def_coach -> coach), so any number of keys joins
        d_off = d_off.rename({d: k for d, k in zip(dkeys, keys) if d != k})
        off = off.join(d_off, on=list(keys), how="left")
    # counts read 0 when a group had nothing to count; expected third downs stay
    # null when no curve was available, so the over-expected rate stays null too
    count_cols = [
        c
        for c in off.columns
        if c not in keys and off.schema[c].is_numeric() and not _is_rate(c) and not c.endswith("third_down_expected")
    ]
    return off.with_columns([pl.col(c).fill_null(0) for c in count_cols]).sort(keys)


_RATE_NAMES = {r for r, _, _ in RATES} | {r for r, _, _ in _DIFF_RATES}


def _is_rate(col: str) -> bool:
    base = col[4:] if col.startswith("def_") else col
    return base in _RATE_NAMES


def aggregate_tendencies(frames: list[pl.DataFrame], keys: tuple[str, ...] = ("coach",)) -> pl.DataFrame:
    """Sum tendency rows over seasons (play-weighted) and recompute every rate.

    Args:
        frames: outputs of :func:`tendencies` (any seasons / groups).
        keys: the identity to sum on (``("coach",)`` for careers,
            ``("pos_team",)`` for a franchise across seasons).

    Returns:
        One row per key with summed counts, ``seasons`` / ``first_season`` /
        ``last_season`` when a ``season`` column was present, and the rates
        recomputed from the sums. Empty input -> empty frame.

    Example:
        Careers from per-season coach rows::

            from sportsdataverse.football.tendencies import aggregate_tendencies
            careers = aggregate_tendencies([season_rows], keys=("coach",))
    """
    keep = [f for f in frames if f is not None and f.height]
    if not keep:
        return pl.DataFrame()
    ks = list(keys)
    for frame in keep:  # every frame, or a diagonal concat would invent a null identity
        absent = [k for k in ks if k not in frame.columns]
        if absent:
            raise ValueError(f"frames lack aggregation keys: {absent}")
    df = pl.concat(keep, how="diagonal_relaxed")
    # ids are never counts: a summed team id is a number that means nothing
    counts = [
        c
        for c in df.columns
        if c not in ks and c != "season" and df.schema[c].is_numeric() and not _is_rate(c) and not c.endswith("_id")
    ]
    # expected third downs are null when no curve was available; a plain sum
    # would read 0 and turn "over expected" into the raw conversion count
    aggs = [
        (
            pl.when(pl.col(c).is_not_null().any()).then(pl.col(c).sum()).otherwise(None).alias(c)
            if c.endswith("third_down_expected")
            else pl.col(c).sum()
        )
        for c in counts
    ]
    if "season" in df.columns:
        aggs += [
            pl.col("season").n_unique().alias("seasons"),
            pl.col("season").min().alias("first_season"),
            pl.col("season").max().alias("last_season"),
        ]
    g = df.group_by(ks, maintain_order=True).agg(aggs)
    g = _apply_rates(g)
    return _apply_rates(g, prefix="def_")
