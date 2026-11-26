"""Starting-QB layer for the NFL pregame spine (opt-in; ACC-2026-0016).

The native ratings (:func:`sportsdataverse.nfl.nfl_ratings.nfl_ratings`) credit every
play to the team, so when the quarterback changes the rating still carries the
departed QB. This layer prices the expected starter in, and widens the margin when
that starter is uncertain:

* **QB value** -- career EPA per dropback, shrunk toward a prior:
  ``v = (n * e + k * prior) / (n + k)``, with a replacement-level prior for QBs
  with fewer than ``new_qb_dropbacks`` career dropbacks and the league mean
  otherwise.
* **Rating shift** -- ``adj_net' = adj_net + gamma * s_db * (v_starter - v_rated)``,
  where ``v_rated`` is the dropback-weighted value of the QBs whose plays fed the
  team's rating and ``s_db`` the team's dropback share of its plays (EPA per play,
  the unit of ``adj_net``).
* **Margin mixture** -- each team starts its expected QB with probability ``p``
  (by his final injury-report status), else the backup; the margin is the mixture
  of up to four Normals with the spine's ``margin_sd``.

Non-market (binding, as for the rest of the spine): no line or market column is
read. Every input is as of kickoff: plays from games strictly before the game's
date, a depth-chart snapshot timestamped strictly before kickoff, and the final
injury report of that week.

The constants live in ``QB_LAYER_CONSTANTS`` and are fitted by
``tools/models/nfl_prediction/fit_qb_layer.py`` on seasons 2015-2025. The
pre-registered out-of-sample test (2026 weeks 5-18) decides whether the layer is
ever wired into :func:`sportsdataverse.nfl.nfl_market.nfl_predict_games`.
"""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.nfl.datasets import team_abbr_mapping
from sportsdataverse.nfl.nfl_prediction_constants import (
    PredictConfig,
    QBLayerConfig,
    RatingsConfig,
    get_constants,
    get_qb_layer_constants,
)
from sportsdataverse.nfl.nfl_ratings import efficiency_ratings

__all__ = [
    "nfl_expected_starters",
    "nfl_qb_predict_games",
    "nfl_qb_values",
    "nfl_qb_dropbacks",
    "nfl_qb_margin_mixture",
]

#: injury-report statuses the layer keeps; anything else (null, Probable, absent) is "none"
STATUSES: tuple[str, ...] = ("none", "Questionable", "Doubtful", "Out")
_Z80 = float(norm.ppf(0.90))

_DROPBACK_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "game_date": pl.Date,
    "team": pl.Utf8,
    "qb_id": pl.Utf8,
    "qb_epa": pl.Float64,
    "is_dropback": pl.Boolean,
}

_VALUES_SCHEMA: dict[str, pl.PolarsDataType] = {
    "qb_id": pl.Utf8,
    "dropbacks": pl.Int64,
    "epa_per_dropback": pl.Float64,
    "qb_value": pl.Float64,
}

_STARTER_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "team": pl.Utf8,
    "kickoff": pl.Datetime("us", "UTC"),
    "chart_dt": pl.Datetime("us", "UTC"),
    "expected_qb_id": pl.Utf8,
    "backup_qb_id": pl.Utf8,
    "starter_source": pl.Utf8,
    "status": pl.Utf8,
    "p_start": pl.Float64,
}

_PREDICT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "week": pl.Int64,
    "home_team": pl.Utf8,
    "away_team": pl.Utf8,
    "neutral_site": pl.Boolean,
    "base_margin": pl.Float64,
    "base_win_prob": pl.Float64,
    "base_lo80": pl.Float64,
    "base_hi80": pl.Float64,
    "qb_margin": pl.Float64,
    "qb_win_prob": pl.Float64,
    "qb_lo80": pl.Float64,
    "qb_hi80": pl.Float64,
    "home_qb_id": pl.Utf8,
    "away_qb_id": pl.Utf8,
    "home_status": pl.Utf8,
    "away_status": pl.Utf8,
    "home_p_start": pl.Float64,
    "away_p_start": pl.Float64,
    "home_shift": pl.Float64,
    "away_shift": pl.Float64,
    "home_layer_applied": pl.Boolean,
    "away_layer_applied": pl.Boolean,
}


def _team(col: str) -> pl.Expr:
    # one code per franchise in every input: the play-by-play already relocates (STL -> LA, SD -> LAC,
    # OAK -> LV) while the schedule, injury and depth-chart files keep the code of the season
    return pl.col(col).cast(pl.Utf8).replace(team_abbr_mapping)


def _to_date(col: str) -> pl.Expr:
    return pl.col(col).cast(pl.Utf8).str.slice(0, 10).str.to_date("%Y-%m-%d")


def nfl_qb_dropbacks(pbp: pl.DataFrame) -> pl.DataFrame:
    """One row per offensive pass or run play, with the QB on dropbacks.

    The shared input of the layer: QB values read the dropback rows, and each
    team's dropback share reads all of them.

    Args:
        pbp: nflverse-schema play-by-play (:func:`sportsdataverse.nfl.load_nfl_pbp`)
            with ``game_id``, ``season``, ``game_date``, ``posteam``, ``play_type``,
            ``qb_dropback``, ``qb_epa``, ``passer_player_id`` and ``rusher_player_id``.

    Returns:
        ``game_id`` / ``team`` / ``qb_id`` (Utf8; ``qb_id`` null off dropbacks),
        ``season`` (Int64), ``game_date`` (Date), ``qb_epa`` (Float64),
        ``is_dropback`` (Boolean). Plays with ``play_type`` other than ``pass`` /
        ``run`` are dropped; a dropback with no QB id or no ``qb_epa`` is kept as a
        play but not as a dropback.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_qb_layer import nfl_qb_dropbacks
            db = nfl_qb_dropbacks(load_nfl_pbp([2024]))
    """
    if pbp.height == 0:
        return pl.DataFrame(schema=_DROPBACK_SCHEMA)
    qb = pl.coalesce(pl.col("passer_player_id").cast(pl.Utf8), pl.col("rusher_player_id").cast(pl.Utf8))
    out = (
        pbp.filter(pl.col("play_type").is_in(["pass", "run"]) & pl.col("posteam").is_not_null())
        .select(
            pl.col("game_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
            _to_date("game_date").alias("game_date"),
            _team("posteam").alias("team"),
            qb.alias("qb_id"),
            pl.col("qb_epa").cast(pl.Float64),
            (pl.col("qb_dropback").cast(pl.Float64).fill_null(0.0) == 1.0).alias("_db"),
        )
        .with_columns(is_dropback=(pl.col("_db") & pl.col("qb_id").is_not_null() & pl.col("qb_epa").is_not_null()))
        .with_columns(qb_id=pl.when(pl.col("is_dropback")).then(pl.col("qb_id")))
        .drop("_db")
    )
    return out.select(*_DROPBACK_SCHEMA.keys())


def _shrink(n: pl.Expr, total: pl.Expr, cfg: QBLayerConfig) -> pl.Expr:
    prior = pl.when(n < cfg.new_qb_dropbacks).then(pl.lit(cfg.replacement)).otherwise(pl.lit(cfg.league))
    return (total + cfg.k * prior) / (n + cfg.k)


@overload
def nfl_qb_values(
    dropbacks: pl.DataFrame,
    *,
    as_of_date: datetime.date,
    config: QBLayerConfig | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def nfl_qb_values(
    dropbacks: pl.DataFrame,
    *,
    as_of_date: datetime.date,
    config: QBLayerConfig | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


def nfl_qb_values(
    dropbacks: pl.DataFrame,
    *,
    as_of_date: datetime.date,
    config: QBLayerConfig | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Shrunk career EPA per dropback for every QB, as of a date.

    ``qb_value = (n * e + k * prior) / (n + k)`` over the QB's dropbacks in games
    strictly before ``as_of_date`` (seasons from ``config.history_start_season``),
    with ``prior`` the replacement level below ``new_qb_dropbacks`` career
    dropbacks and the league mean from there on.

    Args:
        dropbacks: :func:`nfl_qb_dropbacks` output.
        as_of_date: Exclusive cutoff (the game's date).
        config: Layer constants; defaults to ``QB_LAYER_CONSTANTS["modern"]``.
        return_as_pandas: If True, returns a pandas DataFrame.

    Returns:
        One row per QB: ``qb_id`` (Utf8), ``dropbacks`` (Int64),
        ``epa_per_dropback`` (Float64, raw), ``qb_value`` (Float64, shrunk).
        A QB absent from the frame has no row; callers value him at the
        replacement level.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_qb_layer import nfl_qb_values, nfl_qb_dropbacks
            db = nfl_qb_dropbacks(load_nfl_pbp(list(range(2006, 2025))))
            values = nfl_qb_values(db, as_of_date=dt.date(2024, 10, 1))
    """
    cfg = config or get_qb_layer_constants()
    past = dropbacks.filter(
        (pl.col("is_dropback") == True)  # noqa: E712
        & (pl.col("game_date") < as_of_date)
        & (pl.col("season") >= cfg.history_start_season)
    )
    out = (
        past.group_by("qb_id")
        .agg(dropbacks=pl.len().cast(pl.Int64), _sum=pl.col("qb_epa").sum())
        .with_columns(
            epa_per_dropback=pl.col("_sum") / pl.col("dropbacks"),
            qb_value=_shrink(pl.col("dropbacks"), pl.col("_sum"), cfg),
        )
        .select(*_VALUES_SCHEMA.keys())
        .sort("qb_id")
    )
    if out.height == 0:
        out = pl.DataFrame(schema=_VALUES_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


def _exposure(dropbacks: pl.DataFrame, values: pl.DataFrame, season: int, as_of: datetime.date) -> pl.DataFrame:
    """Per team: dropback share and dropback-weighted value of the QBs in the rating window."""
    window = dropbacks.filter((pl.col("season") == season) & (pl.col("game_date") < as_of))
    if window.height == 0:
        return pl.DataFrame(schema={"team": pl.Utf8, "s_db": pl.Float64, "v_rated": pl.Float64})
    share = window.group_by("team").agg(
        s_db=(pl.col("is_dropback") == True).sum() / pl.len()  # noqa: E712
    )
    assert window.schema["qb_id"] == values.schema["qb_id"]
    rated = (
        window.filter(pl.col("is_dropback") == True)  # noqa: E712
        .group_by("team", "qb_id")
        .agg(n=pl.len())
        .join(values.select("qb_id", "qb_value"), on="qb_id", how="left")
        .group_by("team")
        .agg(v_rated=(pl.col("n") * pl.col("qb_value")).sum() / pl.col("n").sum())
    )
    return share.join(rated, on="team", how="left").with_columns(pl.col("s_db").cast(pl.Float64))


def _kickoff_utc() -> pl.Expr:
    return (
        pl.concat_str([pl.col("gameday").cast(pl.Utf8).str.slice(0, 10), pl.lit(" "), pl.col("gametime")])
        .str.to_datetime("%Y-%m-%d %H:%M", time_zone="America/New_York")
        .dt.convert_time_zone("UTC")
    )


def _team_games(schedule: pl.DataFrame) -> pl.DataFrame:
    """One row per team-game, with the team's previous realized starter (same season)."""
    if schedule["gametime"].null_count():
        # a guessed kickoff could admit a depth-chart snapshot taken after the real one
        raise ValueError(f"{schedule['gametime'].null_count()} games have no gametime; kickoff is unknown")
    base = schedule.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("week").cast(pl.Int64),
        _kickoff_utc().alias("kickoff"),
    )
    sides = [
        base.select(
            "game_id",
            "season",
            "week",
            "kickoff",
            _team(f"{side}_team").alias("team"),
            pl.col(f"{side}_qb_id").cast(pl.Utf8).alias("realized_qb_id"),
        )
        for side in ("home", "away")
    ]
    return (
        pl.concat(sides)
        .sort("team", "kickoff")
        .with_columns(prev_qb_id=pl.col("realized_qb_id").shift(1).over("team", "season"))
    )


def _check_codes(codes: pl.Series, schedule_codes: pl.Series, what: str) -> None:
    unknown = sorted(set(codes.drop_nulls().unique().to_list()) - set(schedule_codes.unique().to_list()))
    if unknown:
        raise ValueError(f"{what} team codes not in the schedule: {unknown}")


def _chart_starters(team_games: pl.DataFrame, depth_charts: pl.DataFrame) -> pl.DataFrame:
    """Rule 1: QB1/QB2 from the last timestamped depth-chart snapshot strictly before kickoff."""
    empty = pl.DataFrame(
        schema={
            "game_id": pl.Utf8,
            "team": pl.Utf8,
            "chart_dt": pl.Datetime("us", "UTC"),
            "chart_qb1": pl.Utf8,
            "chart_qb2": pl.Utf8,
        }
    )
    if depth_charts.height == 0:
        return empty
    if "dt" not in depth_charts.columns:
        raise ValueError("depth charts without a dt timestamp cannot be placed before kickoff (pass None)")
    qbs = (
        depth_charts.filter(pl.col("pos_abb") == "QB")
        .select(
            pl.col("dt").cast(pl.Utf8).str.to_datetime("%Y-%m-%dT%H:%M:%SZ", time_zone="UTC").alias("chart_dt"),
            _team("team"),
            pl.col("gsis_id").cast(pl.Utf8),
            pl.col("pos_rank").cast(pl.Int64),
        )
        .filter(pl.col("gsis_id").is_not_null() & pl.col("chart_dt").is_not_null())
        .group_by("team", "chart_dt", "gsis_id")
        .agg(pl.col("pos_rank").min())
        .sort("team", "chart_dt", "pos_rank", "gsis_id")
    )
    if qbs.height == 0:
        return empty
    ranked = qbs.group_by("team", "chart_dt", maintain_order=True).agg(
        chart_qb1=pl.col("gsis_id").first(),
        chart_qb2=pl.col("gsis_id").get(1, null_on_oob=True),
    )
    assert team_games.schema["team"] == ranked.schema["team"]
    _check_codes(ranked["team"], team_games["team"], "depth chart")
    joined = (
        team_games.select("game_id", "team", "kickoff")
        .sort("kickoff")
        .join_asof(
            ranked.sort("chart_dt"),
            left_on="kickoff",
            right_on="chart_dt",
            by="team",
            strategy="backward",
            allow_exact_matches=False,
            check_sortedness=False,  # sorted on kickoff above, so sorted within every team
        )
    )
    return joined.select("game_id", "team", "chart_dt", "chart_qb1", "chart_qb2")


@overload
def nfl_expected_starters(
    schedule: pl.DataFrame,
    *,
    depth_charts: pl.DataFrame | None = ...,
    injuries: pl.DataFrame | None = ...,
    config: QBLayerConfig | None = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def nfl_expected_starters(
    schedule: pl.DataFrame,
    *,
    depth_charts: pl.DataFrame | None = ...,
    injuries: pl.DataFrame | None = ...,
    config: QBLayerConfig | None = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


def nfl_expected_starters(
    schedule: pl.DataFrame,
    *,
    depth_charts: pl.DataFrame | None = None,
    injuries: pl.DataFrame | None = None,
    config: QBLayerConfig | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Pregame expected starting QB, backup, report status and start probability per team-game.

    1. A timestamped depth chart (nflverse's ``dt`` format, 2025 on): QB1 and QB2 of the
       team's last snapshot strictly before kickoff (``gameday`` + ``gametime``,
       US Eastern).
    2. Otherwise the team's realized starter in its previous game of the season, with no
       known backup. A team's first game with no snapshot has no expected starter.

    ``status`` is the expected starter's ``report_status`` on that week's injury report
    (Out / Doubtful / Questionable; null, Probable or absent is ``none``), and ``p_start``
    the fitted chance he starts at that status. The realized starter columns of the
    schedule (``home_qb_id`` / ``away_qb_id``) are read only for rule 2's previous game.

    Args:
        schedule: :func:`sportsdataverse.nfl.load_nfl_schedule` rows (``game_id``,
            ``season``, ``week``, ``gameday``, ``gametime``, ``home_team``,
            ``away_team``, ``home_qb_id``, ``away_qb_id``).
        depth_charts: :func:`sportsdataverse.nfl.load_nfl_depth_charts` rows in the
            timestamped (``dt``) format. ``None`` uses rule 2 only.
        injuries: :func:`sportsdataverse.nfl.load_nfl_injuries` rows (``season``,
            ``week``, ``team``, ``gsis_id``, ``report_status``). ``None`` gives every
            starter status ``none``.
        config: Layer constants; defaults to ``QB_LAYER_CONSTANTS["modern"]``.
        return_as_pandas: If True, returns a pandas DataFrame.

    Returns:
        One row per team-game: ``game_id`` / ``team`` / ``expected_qb_id`` /
        ``backup_qb_id`` / ``starter_source`` (``depth_chart`` / ``previous_start`` /
        null) / ``status`` (Utf8), ``kickoff`` / ``chart_dt`` (UTC Datetime; ``chart_dt`` is the
        snapshot used, null under rule 2), ``p_start`` (Float64). Zero-row on an empty schedule. ``team`` is the
        franchise's current code (``OAK`` -> ``LV``, ``SD`` -> ``LAC``, ``STL`` -> ``LA``), as in the play-by-play.

    Raises:
        ValueError: On a game with no ``gametime``, depth charts without ``dt``, an injury or
            depth-chart team code the schedule does not have, or a ``p_start`` missing a status.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_depth_charts, load_nfl_injuries, load_nfl_schedule
            from sportsdataverse.nfl.nfl_qb_layer import nfl_expected_starters
            starters = nfl_expected_starters(
                load_nfl_schedule([2025]),
                depth_charts=load_nfl_depth_charts([2025]),
                injuries=load_nfl_injuries([2025]),
            )
    """
    cfg = config or get_qb_layer_constants()
    if schedule.height == 0:
        empty = pl.DataFrame(schema=_STARTER_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty
    tg = _team_games(schedule)
    chart = _chart_starters(tg, depth_charts if depth_charts is not None else pl.DataFrame())
    assert tg.schema["game_id"] == chart.schema["game_id"]
    out = tg.join(chart, on=["game_id", "team"], how="left").with_columns(
        expected_qb_id=pl.coalesce("chart_qb1", "prev_qb_id"),
        backup_qb_id=pl.when(pl.col("chart_qb1").is_not_null()).then(pl.col("chart_qb2")),
        starter_source=pl.when(pl.col("chart_qb1").is_not_null())
        .then(pl.lit("depth_chart"))
        .when(pl.col("prev_qb_id").is_not_null())
        .then(pl.lit("previous_start")),
    )
    out = _with_report_status(out, injuries)
    missing = [s for s in STATUSES if s not in cfg.p_start]
    if missing:
        raise ValueError(f"QBLayerConfig.p_start has no value for {missing}")
    p_map = {s: float(cfg.p_start[s]) for s in STATUSES}
    out = out.with_columns(
        status=pl.when(pl.col("_status").is_in(["Questionable", "Doubtful", "Out"]))
        .then(pl.col("_status"))
        .otherwise(pl.lit("none"))
    ).with_columns(
        p_start=pl.when(pl.col("expected_qb_id").is_null())
        .then(pl.lit(1.0))
        .otherwise(pl.col("status").replace_strict(p_map, return_dtype=pl.Float64))
    )
    out = out.select(*_STARTER_SCHEMA.keys())
    return out.to_pandas() if return_as_pandas else out


def _with_report_status(out: pl.DataFrame, injuries: pl.DataFrame | None) -> pl.DataFrame:
    """Join the expected starter's report status (``_status``) for that season, week and team."""
    if injuries is not None and injuries.height > 0:
        rep = (
            injuries.select(
                pl.col("season").cast(pl.Int64),
                pl.col("week").cast(pl.Int64),
                _team("team"),
                pl.col("gsis_id").cast(pl.Utf8).alias("expected_qb_id"),
                pl.col("report_status").cast(pl.Utf8).alias("_status"),
            )
            .filter(pl.col("expected_qb_id").is_not_null())
            .unique(["season", "week", "team", "expected_qb_id"], keep="last", maintain_order=True)
        )
        for k in ("season", "week", "team", "expected_qb_id"):
            assert out.schema[k] == rep.schema[k]
        _check_codes(
            rep.filter(pl.col("season").is_in(out["season"].unique().to_list()))["team"], out["team"], "injury"
        )
        out = out.join(rep, on=["season", "week", "team", "expected_qb_id"], how="left")
    else:
        out = out.with_columns(_status=pl.lit(None, dtype=pl.Utf8))
    return out


def nfl_qb_margin_mixture(margins: Sequence[float], weights: Sequence[float], *, sigma: float) -> dict[str, float]:
    """Mean, win probability and 80% interval of a Normal mixture of home margins.

    Args:
        margins: Component means (points, home minus away).
        weights: Component weights (non-negative; renormalised to sum to 1).
        sigma: Common component standard deviation (the spine's ``margin_sd``).

    Returns:
        ``{"mean", "win_prob", "lo80", "hi80"}``: the mixture mean, ``P(margin > 0)``, and
        its 0.10 / 0.90 quantiles (bisection to 1e-6 points). One component reduces to
        ``m +/- 1.2816 * sigma``.

    Raises:
        ValueError: On empty or mismatched inputs, or weights that do not sum to a
            positive number.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_qb_layer import nfl_qb_margin_mixture
            nfl_qb_margin_mixture([3.0, -2.0], [0.7, 0.3], sigma=13.02)
    """
    m: np.ndarray = np.asarray(margins, dtype=float)
    w: np.ndarray = np.asarray(weights, dtype=float)
    if m.size == 0 or m.shape != w.shape or not np.all(w >= 0) or w.sum() <= 0:
        raise ValueError("margins and weights must be non-empty, equal length, with positive total weight")
    w = w / w.sum()
    mean = float((w * m).sum())
    win = float((w * norm.sf(0.0, loc=m, scale=sigma)).sum())
    if m.size == 1 or np.allclose(m, m[0]):
        return {"mean": mean, "win_prob": win, "lo80": mean - _Z80 * sigma, "hi80": mean + _Z80 * sigma}

    def quantile(q: float) -> float:
        lo, hi = float(m.min()) - 10 * sigma, float(m.max()) + 10 * sigma
        while hi - lo > 1e-6:
            mid = 0.5 * (lo + hi)
            if float((w * norm.cdf(mid, loc=m, scale=sigma)).sum()) < q:
                lo = mid
            else:
                hi = mid
        return 0.5 * (lo + hi)

    return {"mean": mean, "win_prob": win, "lo80": quantile(0.10), "hi80": quantile(0.90)}


_RIDGE_COLS = (
    "game_id",
    "posteam",
    "defteam",
    "home_team",
    "epa",
    "wp",
    "special",
    "qb_kneel",
    "qb_spike",
    "play_type",
)


def nfl_qb_predict_games(
    schedule: pl.DataFrame,
    pbp: pl.DataFrame,
    *,
    starters: pl.DataFrame | None = None,
    starter: Literal["expected", "realized"] = "expected",
    era: str = "modern",
    config: QBLayerConfig | None = None,
    ratings_config: RatingsConfig | None = None,
) -> pl.DataFrame:
    """Base spine and QB-layer pregame margin, win probability and 80% interval per game.

    For each game date the spine's efficiency ratings are fitted on that season's plays
    from games strictly before the date (as :func:`sportsdataverse.nfl.nfl_ratings`
    with ``as_of_date``), QB values on career dropbacks before the date, and each
    team's dropback exposure on the same window. ``base_*`` is the shipped spine;
    ``qb_*`` adds the starter shift and the start-probability mixture.

    Args:
        schedule: The games to price (:func:`sportsdataverse.nfl.load_nfl_schedule`
            rows; ``location == "Neutral"`` marks a neutral site).
        pbp: nflverse play-by-play covering the priced seasons and the QB history
            (from ``config.history_start_season``).
        starters: :func:`nfl_expected_starters` output, required for
            ``starter="expected"``.
        starter: ``"expected"`` (the pregame arm) or ``"realized"`` (the schedule's
            ``home_qb_id`` / ``away_qb_id`` with ``p = 1``: an upper bound, never a
            pregame input).
        era: Spine constants era (``hfa``, ``points_per_net``, ``margin_sd``).
        config: Layer constants; defaults to ``QB_LAYER_CONSTANTS["modern"]``.
        ratings_config: Ridge constants; defaults to :class:`RatingsConfig`.

    Returns:
        One row per priced game with a rating for both teams (games before either team has a
        rated play are dropped): ``game_id`` / ``home_team`` / ``away_team`` (Utf8, team codes
        relocated as in :func:`nfl_expected_starters`), ``season`` / ``week`` (Int64),
        ``neutral_site`` (Boolean); ``base_margin`` / ``base_win_prob`` / ``base_lo80`` /
        ``base_hi80`` (the shipped spine) and ``qb_margin`` / ``qb_win_prob`` / ``qb_lo80`` /
        ``qb_hi80`` (the layer's mixture mean, P(home wins) and 80% interval), all Float64;
        per side ``*_qb_id`` / ``*_status`` (Utf8), ``*_p_start`` and ``*_shift`` (Float64, the
        starter's shift to ``adj_net`` in EPA per play) and ``*_layer_applied`` (Boolean, False
        when the side had no starter or no rated dropbacks and the layer left it unchanged).

    Raises:
        ValueError: If ``starter="expected"`` and ``starters`` is None.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp, load_nfl_schedule
            from sportsdataverse.nfl.nfl_qb_layer import nfl_expected_starters, nfl_qb_predict_games
            sched = load_nfl_schedule([2025]).filter(pl.col("game_type") == "REG")
            pbp = load_nfl_pbp(list(range(2006, 2026)))
            preds = nfl_qb_predict_games(sched, pbp, starters=nfl_expected_starters(sched))
    """
    if starter == "expected" and starters is None:
        raise ValueError('starter="expected" needs the nfl_expected_starters frame')
    cfg = config or get_qb_layer_constants()
    spine = get_constants(era)
    rcfg = ratings_config or RatingsConfig()
    if schedule.height == 0:
        return pl.DataFrame(schema=_PREDICT_SCHEMA)
    sched, plays = _prep_games_and_plays(schedule, pbp)
    db = nfl_qb_dropbacks(pbp)
    # (game_id, team) -> (expected QB, backup, status, p)
    picks: dict[tuple[str, str], tuple[str | None, str | None, str, float]] = {}
    if starter == "expected" and starters is not None:
        for r in starters.iter_rows(named=True):
            picks[(r["game_id"], r["team"])] = (r["expected_qb_id"], r["backup_qb_id"], r["status"], r["p_start"])
    rows: list[dict[str, object]] = []
    for (season_key, day_key), games in sched.group_by("season", "_date", maintain_order=True):
        season, as_of = int(season_key), day_key
        assert isinstance(as_of, datetime.date)
        window = plays.filter((pl.col("season") == season) & (pl.col("gameday") < as_of))
        if window.height == 0:
            continue
        eff = efficiency_ratings(window, config=rcfg)
        net = dict(zip(eff["team_id"].to_list(), eff["adj_net"].to_list(), strict=True))
        values = nfl_qb_values(db, as_of_date=as_of, config=cfg)
        vmap = dict(zip(values["qb_id"].to_list(), values["qb_value"].to_list(), strict=True))
        expo = _exposure(db, values, season, as_of)
        emap = {r["team"]: (r["s_db"], r["v_rated"]) for r in expo.iter_rows(named=True)}
        for g in games.iter_rows(named=True):
            if g["home_team"] not in net or g["away_team"] not in net:
                continue
            sides = []
            for name in ("home", "away"):
                team = g[f"{name}_team"]
                pick = (
                    (g[f"{name}_qb_id"], None, "none", 1.0)
                    if starter == "realized"
                    else picks.get((g["game_id"], team), (None, None, "none", 1.0))
                )
                sides.append(_price_side(pick, emap.get(team, (None, None)), vmap, cfg))
            rows.append(_price_game(g, season, net, sides[0], sides[1], spine))
    return pl.DataFrame(rows, schema=_PREDICT_SCHEMA) if rows else pl.DataFrame(schema=_PREDICT_SCHEMA)


def _prep_games_and_plays(schedule: pl.DataFrame, pbp: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Typed schedule (relocated codes, game date, neutral flag) and the ridge plays dated by game."""
    sched = schedule.with_columns(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("week").cast(pl.Int64),
        _to_date("gameday").alias("_date"),
        (pl.col("location").cast(pl.Utf8) == "Neutral").fill_null(False).alias("neutral_site"),
        _team("home_team"),
        _team("away_team"),
    )
    plays = pbp.select(
        *_RIDGE_COLS, pl.col("season").cast(pl.Int64), _to_date("game_date").alias("gameday")
    ).with_columns(pl.col("game_id").cast(pl.Utf8), *(_team(c) for c in ("posteam", "defteam", "home_team")))
    return sched, plays


_Side = tuple[str | None, str, float, float, float, bool]


def _price_side(
    pick: tuple[str | None, str | None, str, float],
    exposure: tuple[float | None, float | None],
    vmap: dict[str, float],
    cfg: QBLayerConfig,
) -> _Side:
    """(qb, status, p, starter shift, backup shift, applied) for one team: shifts in EPA per play."""
    qb, bk, status, p = pick
    s_db, v_rated = exposure
    if qb is None or s_db is None or v_rated is None:
        return (qb, status, 1.0, 0.0, 0.0, False)  # nothing to price: the layer is a no-op
    shift_s = cfg.gamma * s_db * (vmap.get(qb, cfg.replacement) - v_rated)
    shift_b = cfg.gamma * s_db * ((vmap.get(bk, cfg.replacement) if bk else cfg.replacement) - v_rated)
    return (qb, status, p, shift_s, shift_b, True)


def _price_game(g: dict, season: int, net: dict[str, float], home: _Side, away: _Side, spine: PredictConfig) -> dict:
    """One output row: the base spine and the start-probability mixture over both teams' starters."""
    base = spine.points_per_net * (net[g["home_team"]] - net[g["away_team"]]) + (
        0.0 if g["neutral_site"] else spine.hfa
    )
    hq, hst, hp, hs, hb, happ = home
    aq, ast, ap, as_, ab, aapp = away
    margins, weights = [], []
    for h_shift, hw in ((hs, hp), (hb, 1.0 - hp)):
        for a_shift, aw in ((as_, ap), (ab, 1.0 - ap)):
            if hw * aw > 0:
                margins.append(base + spine.points_per_net * (h_shift - a_shift))
                weights.append(hw * aw)
    mix = nfl_qb_margin_mixture(margins, weights, sigma=spine.margin_sd)
    return {
        "game_id": g["game_id"],
        "season": season,
        "week": g["week"],
        "home_team": g["home_team"],
        "away_team": g["away_team"],
        "neutral_site": g["neutral_site"],
        "base_margin": base,
        "base_win_prob": float(norm.sf(0.0, loc=base, scale=spine.margin_sd)),
        "base_lo80": base - _Z80 * spine.margin_sd,
        "base_hi80": base + _Z80 * spine.margin_sd,
        "qb_margin": mix["mean"],
        "qb_win_prob": mix["win_prob"],
        "qb_lo80": mix["lo80"],
        "qb_hi80": mix["hi80"],
        "home_qb_id": hq,
        "away_qb_id": aq,
        "home_status": hst,
        "away_status": ast,
        "home_p_start": hp,
        "away_p_start": ap,
        "home_shift": hs,
        "away_shift": as_,
        "home_layer_applied": happ,
        "away_layer_applied": aapp,
    }
