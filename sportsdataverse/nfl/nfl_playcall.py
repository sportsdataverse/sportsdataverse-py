"""NFL play-call classifier ① — feature build, artifact scorer, team tendencies.

Consumes nflverse-format pbp (with the shipped ``xpass`` / ``pass_oe``
columns from :func:`sportsdataverse.nfl.ep_wp.calculate_xpass`) plus the
optional nflverse participation frame, and scores the bundled
``multi:softprob`` classifier (``nfl/models/nfl_playcall.ubj``, trained on
2016-2021 by ``dev/nfl_scheme/train_playcall.py`` — seasons strictly before
the 2022-2023 evaluation window, the as-of leakage boundary).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_scheme_constants import PLAYCALL_ARTIFACT

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

#: The 5 play-call families, in the fixed class order the bundled model was
#: trained with (``dev/nfl_scheme/train_playcall.py``).
FAMILIES: List[str] = ["inside_run", "outside_run", "short_pass", "deep_pass", "scramble"]

#: Always-present pre-snap feature columns (nflverse pbp).
PLAYCALL_CORE_FEATURES: List[str] = [
    "down",
    "ydstogo",
    "yardline_100",
    "score_differential",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "wp",
    "shotgun",
    "no_huddle",
    "xpass",
]

#: Full model feature order (core + participation-derived).
PLAYCALL_FEATURE_ORDER: List[str] = PLAYCALL_CORE_FEATURES + [
    "n_rb",
    "n_te",
    "n_wr",
    "has_participation",
]

_KEY_COLS: List[str] = ["game_id", "play_id", "season", "week", "posteam"]

_PROB_COLS: List[str] = [f"p_{f}" for f in FAMILIES]

_SCORER_SCHEMA: dict = {
    **{k: pl.Utf8 if k in ("game_id", "posteam") else pl.Int64 for k in _KEY_COLS},
    **{c: pl.Float64 for c in _PROB_COLS},
    "p_pass": pl.Float64,
    "pred_family": pl.Utf8,
    "pass_oe_model": pl.Float64,
}


def parse_personnel(expr: pl.Expr) -> Tuple[pl.Expr, pl.Expr, pl.Expr]:
    """Extract ``(n_rb, n_te, n_wr)`` from an ``offense_personnel`` string.

    Parses strings like ``"1 RB, 2 TE, 2 WR"`` with lookaround-free regexes
    (polars/Rust regex has no lookahead).

    Args:
        expr: A polars expression yielding the personnel string.

    Returns:
        Three ``Float64`` expressions: RB, TE, WR counts (null when absent).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_playcall import parse_personnel
            rb, te, wr = parse_personnel(pl.col("offense_personnel"))
            df = pl.DataFrame({"offense_personnel": ["1 RB, 2 TE, 2 WR"]})
            df.select(rb.alias("n_rb"))
    """
    rb = expr.str.extract(r"(\d+)\s*RB", 1).cast(pl.Float64)
    te = expr.str.extract(r"(\d+)\s*TE", 1).cast(pl.Float64)
    wr = expr.str.extract(r"(\d+)\s*WR", 1).cast(pl.Float64)
    return rb, te, wr


def playcall_features(pbp: pl.DataFrame, participation: Optional[pl.DataFrame] = None) -> pl.DataFrame:
    """Build the play-call feature frame (one row per offensive run/pass play).

    Filters to plays with ``pass == 1`` or ``rush == 1``, derives the 5-class
    ``family`` label (scramble > deep/short pass > inside/outside run), and
    left-joins the optional participation frame for personnel counts.

    Args:
        pbp: nflverse-format pbp with the pre-snap feature columns +
            ``pass`` / ``rush`` / ``qb_scramble`` / ``pass_length`` /
            ``run_location`` / ``run_gap`` and ``xpass``.
        participation: Optional nflverse participation frame with
            ``game_id`` / ``play_id`` / ``offense_personnel``.

    Returns:
        Keys + :data:`PLAYCALL_FEATURE_ORDER` columns + ``family`` +
        ``is_pass``.  Personnel columns are null (``has_participation=0``)
        when no participation row matches.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_xpass
            from sportsdataverse.nfl.nfl_playcall import playcall_features
            feat = playcall_features(calculate_xpass(load_nfl_pbp([2023])))
            print(feat["family"].value_counts())
    """
    df = pbp.filter((pl.col("pass") == 1) | (pl.col("rush") == 1))
    df = df.with_columns(
        pl.when(pl.col("qb_scramble") == 1)
        .then(pl.lit("scramble"))
        .when(pl.col("pass") == 1)
        .then(pl.when(pl.col("pass_length") == "deep").then(pl.lit("deep_pass")).otherwise(pl.lit("short_pass")))
        .otherwise(
            pl.when(pl.col("run_gap").is_in(["guard", "center"]) | (pl.col("run_location") == "middle"))
            .then(pl.lit("inside_run"))
            .otherwise(pl.lit("outside_run"))
        )
        .alias("family"),
        pl.col("pass").cast(pl.Int64).alias("is_pass"),
    )

    if participation is not None and participation.height > 0:
        part = participation
        if "game_id" not in part.columns and "nflverse_game_id" in part.columns:
            part = part.rename({"nflverse_game_id": "game_id"})
        part = part.with_columns(pl.col("game_id").cast(pl.Utf8), pl.col("play_id").cast(pl.Int64)).unique(
            subset=["game_id", "play_id"], keep="first"
        )
        df = df.with_columns(pl.col("game_id").cast(pl.Utf8), pl.col("play_id").cast(pl.Int64))
        assert df.schema["play_id"] == part.schema["play_id"]
        assert df.schema["game_id"] == part.schema["game_id"]
        df = df.join(
            part.select("game_id", "play_id", "offense_personnel"),
            on=["game_id", "play_id"],
            how="left",
        )
        rb, te, wr = parse_personnel(pl.col("offense_personnel"))
        df = df.with_columns(
            rb.alias("n_rb"),
            te.alias("n_te"),
            wr.alias("n_wr"),
            pl.col("offense_personnel").is_not_null().cast(pl.Int8).alias("has_participation"),
        )
    else:
        df = df.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("n_rb"),
            pl.lit(None, dtype=pl.Float64).alias("n_te"),
            pl.lit(None, dtype=pl.Float64).alias("n_wr"),
            pl.lit(0, dtype=pl.Int8).alias("has_participation"),
        )

    return df.select(*_KEY_COLS, *PLAYCALL_FEATURE_ORDER, "family", "is_pass")


def _load_playcall_booster(models_dir: Optional[str] = None):  # type: ignore[no-untyped-def]
    """Load the bundled play-call booster (or from ``models_dir``)."""
    from xgboost import Booster

    if models_dir is not None:
        path = f"{models_dir}/{PLAYCALL_ARTIFACT}"
    else:
        from importlib.resources import files

        path = str(files("sportsdataverse.nfl.models").joinpath(PLAYCALL_ARTIFACT))
    booster = Booster()
    booster.load_model(path)
    return booster


def nfl_play_call_probabilities(
    pbp: pl.DataFrame,
    participation: Optional[pl.DataFrame] = None,
    *,
    models_dir: Optional[str] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Score the bundled play-call classifier over offensive plays.

    Args:
        pbp: nflverse-format pbp (must carry ``xpass``; run
            :func:`sportsdataverse.nfl.ep_wp.calculate_xpass` first if not).
        participation: Optional participation frame for personnel features.
        models_dir: Optional directory holding ``nfl_playcall.ubj`` (defaults
            to the bundled package artifact; no first-use download).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Keys + per-family probabilities ``p_inside_run`` / ``p_outside_run`` /
        ``p_short_pass`` / ``p_deep_pass`` / ``p_scramble``, ``p_pass``
        (pass-family sum), ``pred_family`` (argmax) and ``pass_oe_model``
        (``100 * (is_pass - p_pass)``).  Empty input yields a zero-row frame
        with this schema.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_xpass
            from sportsdataverse.nfl.nfl_playcall import nfl_play_call_probabilities
            out = nfl_play_call_probabilities(calculate_xpass(load_nfl_pbp([2023])))
            print(out.select("p_pass", "pred_family").head())

        Pipeline next step::

            out.group_by("posteam").agg(pl.col("p_pass").mean()).sort("p_pass")

        See Also:
            * `nflfastR`_ -- ships the run/pass ``xpass`` baseline this model extends.

        .. _nflfastR: https://www.nflfastr.com
    """
    feat = playcall_features(pbp, participation) if pbp.height > 0 else None
    if feat is None or feat.height == 0:
        out: pl.DataFrame = pl.DataFrame(schema=_SCORER_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    from xgboost import DMatrix

    booster = _load_playcall_booster(models_dir)
    x = feat.select(PLAYCALL_FEATURE_ORDER).to_numpy().astype(np.float32)
    probs = booster.predict(DMatrix(x, feature_names=PLAYCALL_FEATURE_ORDER))
    probs = np.asarray(probs, dtype=np.float64)

    out = feat.select(*_KEY_COLS, "is_pass")
    out = out.with_columns([pl.Series(_PROB_COLS[i], probs[:, i]) for i in range(len(FAMILIES))]).with_columns(
        (pl.col("p_short_pass") + pl.col("p_deep_pass") + pl.col("p_scramble")).alias("p_pass"),
    )
    pred_idx = probs.argmax(axis=1)
    out = out.with_columns(
        pl.Series("pred_family", [FAMILIES[i] for i in pred_idx], dtype=pl.Utf8),
        (100.0 * (pl.col("is_pass") - pl.col("p_pass"))).alias("pass_oe_model"),
    ).drop("is_pass")
    out = out.select(list(_SCORER_SCHEMA.keys()))
    return out.to_pandas() if return_as_pandas else out


def nfl_play_call_tendencies(
    pbp: pl.DataFrame,
    participation: Optional[pl.DataFrame] = None,
    *,
    models_dir: Optional[str] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Aggregate scored play-call probabilities to team-season tendencies.

    Args:
        pbp: nflverse-format pbp (with ``xpass``).
        participation: Optional participation frame.
        models_dir: Optional directory holding ``nfl_playcall.ubj``.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(season, posteam)``: ``plays``, ``mean_p_pass``, ``pass_rate``,
        ``proe`` (``100 * (pass_rate - mean_p_pass)``) and the family mix
        shares ``share_<family>``.  Empty input yields a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_xpass
            from sportsdataverse.nfl.nfl_playcall import nfl_play_call_tendencies
            t = nfl_play_call_tendencies(calculate_xpass(load_nfl_pbp([2023])))
            print(t.sort("proe", descending=True).head())
    """
    schema: dict = {
        "season": pl.Int64,
        "posteam": pl.Utf8,
        "plays": pl.Int64,
        "mean_p_pass": pl.Float64,
        "pass_rate": pl.Float64,
        "proe": pl.Float64,
        **{f"share_{f}": pl.Float64 for f in FAMILIES},
    }
    if pbp.height == 0:
        out: pl.DataFrame = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    scored = nfl_play_call_probabilities(pbp, participation, models_dir=models_dir)
    assert isinstance(scored, pl.DataFrame)
    feat = playcall_features(pbp, participation).select("game_id", "play_id", "family", "is_pass")
    assert scored.schema["play_id"] == feat.schema["play_id"]
    j = scored.join(feat, on=["game_id", "play_id"], how="left")
    out = (
        j.group_by("season", "posteam")
        .agg(
            pl.len().cast(pl.Int64).alias("plays"),
            pl.col("p_pass").mean().alias("mean_p_pass"),
            pl.col("is_pass").mean().alias("pass_rate"),
            *[(pl.col("family") == f).mean().alias(f"share_{f}") for f in FAMILIES],
        )
        .with_columns((100.0 * (pl.col("pass_rate") - pl.col("mean_p_pass"))).alias("proe"))
        .select(list(schema.keys()))
        .sort("season", "posteam")
    )
    return out.to_pandas() if return_as_pandas else out
