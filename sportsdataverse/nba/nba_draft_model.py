"""Draft model ① -- draft pick -> projected NBA career value (a pick curve).

Reads a draft class from the ``nba_stats_draft`` release and applies the
bundled ``nba_draft_value.json`` pick curve, fit in
``tools/models/nba_draft/fit_pick_curve.py`` on draft classes 2003-2015
against realized career box value.

The curve ties the pick baseline by construction: it is monotone in the pick,
so it ranks a class exactly as the pick does, and only adds a value scale.
An earlier combine-measurement model lost to the pick on its own holdout
(Spearman 0.048 vs 0.455 among drafted players). Walk-forward rebuilds with
college production, college RAPM and EuroLeague production did not beat the
pick by a detectable margin. Those pre-draft columns stay available with
``pre_draft_features=True``, as information only.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, Optional, Union, overload

import pandas as pd
import polars as pl

from sportsdataverse.nba import _nba_draft_features as _features
from sportsdataverse.nba._nba_draft_features import FEATURE_SCHEMA
from sportsdataverse.nba.nba_loaders import load_nba_stats_draft
from sportsdataverse.nba.nba_stats import nba_stats_leaguedashplayerstats

__all__ = ["nba_draft_model"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "draft_year": pl.Int64,
    "overall_pick": pl.Int64,
    "player_name": pl.Utf8,
    "organization": pl.Utf8,
    "proj_career_value": pl.Float64,
    "projected_pick": pl.Int64,
    "pro_tier": pl.Utf8,
}


def _load_artifact(league: str = "nba") -> dict:
    """Load the bundled NBA pick-curve artifact (JSON).

    Args:
        league: Only ``"nba"``.

    Returns:
        The parsed artifact: ``basis``, ``curves`` (``{target: {intercept,
        coef}}``), ``train_classes``, ``holdout_classes``, ``holdout`` metrics
        and ``training_script``.
    """
    _check_league(league)
    path = resources.files("sportsdataverse.nba") / "models" / "nba_draft_value.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _check_league(league: str) -> None:
    if league != "nba":
        raise ValueError(
            f"league={league!r}: the NBA pick curves cover the NBA draft only; "
            "use sportsdataverse.wnba.wnba_draft_model / wnba_rookie_projection for the WNBA"
        )


def _apply_pick_curve(curve: dict, pick: pl.Expr) -> pl.Expr:
    """``intercept + b1*log(pick) [+ b2*(pick/60)]`` as a polars expression (``coef`` has 1 or 2 terms).

    Args:
        curve: One entry of an artifact's ``curves`` (``intercept``, ``coef``).
        pick: Expression for the overall pick (1-60).

    Returns:
        A ``Float64`` expression.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_draft_model import _apply_pick_curve
            curve = {"intercept": 100.0, "coef": [-20.0, -10.0]}
            pl.DataFrame({"overall_pick": [1, 30]}).select(_apply_pick_curve(curve, pl.col("overall_pick")))
    """
    coef = [float(c) for c in curve["coef"]]
    p = pick.cast(pl.Float64)
    out = pl.lit(float(curve["intercept"])) + coef[0] * p.log()
    return out + coef[1] * p / 60.0 if len(coef) > 1 else out


def _picks(years: list[int]) -> pl.DataFrame:
    raw = load_nba_stats_draft(seasons=years)
    if raw is None or raw.is_empty():
        return pl.DataFrame(
            schema={k: _SCHEMA[k] for k in ("player_id", "draft_year", "overall_pick", "player_name", "organization")}
        )
    # the release stamps a draft with the END year of the season it opens (the 2019 draft reads 2020)
    return (
        raw.filter(pl.col("overall_pick").is_not_null() & (pl.col("overall_pick") > 0))
        .select(
            pl.col("person_id").cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
            (pl.col("season").cast(pl.Int64) - 1).alias("draft_year"),
            pl.col("overall_pick").cast(pl.Int64),
            pl.col("player_name").cast(pl.Utf8),
            pl.col("organization").cast(pl.Utf8),
        )
        .filter(pl.col("draft_year").is_in(years))
    )


def _pro_tier(pick: pl.Expr) -> pl.Expr:
    return (
        pl.when(pick <= 14)
        .then(pl.lit("lottery"))
        .when(pick <= 30)
        .then(pl.lit("first_round"))
        .otherwise(pl.lit("second_round"))
    )


def _score(picks: pl.DataFrame, art: dict) -> pl.DataFrame:
    """Apply the career-value pick curve to a frame of picks (no network)."""
    out = picks.with_columns(
        _apply_pick_curve(art["curves"]["career_value"], pl.col("overall_pick")).alias("proj_career_value")
    )
    out = out.with_columns(
        pl.col("proj_career_value")
        .rank(method="ordinal", descending=True)
        .over("draft_year")
        .cast(pl.Int64)
        .alias("projected_pick"),
        _pro_tier(pl.col("overall_pick")).alias("pro_tier"),
    )
    return out.select(list(_SCHEMA))


def _fetch_gleague_bridge(draft_years: list[int]) -> pl.DataFrame:
    """G-League (``league_id="20"``) production for each draft year's season, best-effort."""
    frames = []
    for year in draft_years:
        season_str = f"{year}-{str(year + 1)[-2:]}"
        try:
            bulk = nba_stats_leaguedashplayerstats(season=season_str, league_id="20")
        except Exception:  # pragma: no cover - defensive, matches "never raises"
            continue
        if bulk.is_empty() or "player_id" not in bulk.columns:
            continue
        cols = [c for c in ("pts", "gp", "min") if c in bulk.columns]
        if cols:
            frames.append(
                bulk.select(
                    pl.col("player_id").cast(pl.Int64).cast(pl.Utf8),
                    pl.lit(year).cast(pl.Int64).alias("draft_year"),
                    *[pl.col(c).cast(pl.Float64).alias(f"gleague_{c}") for c in cols],
                )
            )
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


@overload
def nba_draft_model(
    draft_year: "Union[int, list[int]]",
    *,
    league: str = "nba",
    pre_draft_features: bool = False,
    college_prior: "Optional[pl.DataFrame]" = None,
    gleague_bridge: bool = False,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_draft_model(
    draft_year: "Union[int, list[int]]",
    *,
    league: str = "nba",
    pre_draft_features: bool = False,
    college_prior: "Optional[pl.DataFrame]" = None,
    gleague_bridge: bool = False,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_draft_model(
    draft_year: "Union[int, list[int]]",
    *,
    league: str = "nba",
    pre_draft_features: bool = False,
    college_prior: "Optional[pl.DataFrame]" = None,
    gleague_bridge: bool = False,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Project each draft pick's NBA career value from where he was picked.

    Reads the class from the ``nba_stats_draft`` release (no stats.nba.com
    call) and applies the bundled pick curve
    ``proj_career_value = intercept + b1*log(pick) + b2*(pick/60)``. The curve
    was fit on draft classes 2003-2015 against realized career box value
    (``tools/models/nba_draft/fit_pick_curve.py``).

    The curve ranks a class exactly as the pick does. It ties the pick baseline
    by construction (2016-2019 holdout, all 240 picks: Spearman 0.571 for both) and only adds
    a value scale. No pre-draft measurement beat the pick in the rebuild that
    replaced the combine model: combine drills, college BPM / RAPM / seasons,
    and EuroLeague production were all tried.

    Args:
        draft_year: A draft year (e.g. ``2019``) or a list of years.
        league: ``"nba"`` only. The WNBA has
            :func:`sportsdataverse.wnba.wnba_draft_model.wnba_draft_model`;
            any other value raises ``ValueError``.
        pre_draft_features: When ``True``, attach the pre-draft college and
            EuroLeague columns (``college_espn_id`` ... ``euro_top``, see
            Returns). They are read from the college player-value spine, the
            ESPN college bios, college RAPM and the EuroLeague API, and are
            information only: the curve never reads them. For a board before the draft,
            see :func:`sportsdataverse.mbb.mbb_draft_projection.mbb_draft_projection`.
        college_prior: Optional frame keyed on ``player_id:Utf8``, left-joined
            as extra columns. It does not change ``proj_career_value``.
        gleague_bridge: When ``True``, left-join G-League
            (``league_id="20"``) ``gleague_pts`` / ``gleague_gp`` /
            ``gleague_min`` for the draft year's season (live stats.nba.com,
            best-effort, never raises). It does not change
            ``proj_career_value``.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per pick (1-60) of each class.

        |col_name          |type    |
        |:-----------------|:-------|
        |player_id         |String  |
        |draft_year        |Int64   |
        |overall_pick      |Int64   |
        |player_name       |String  |
        |organization      |String  |
        |proj_career_value |Float64 |
        |projected_pick    |Int64   |
        |pro_tier          |String  |

        With ``pre_draft_features=True`` it adds: ``college_espn_id``,
        ``college_match`` (``"id"`` / ``"name"``), ``col_bpm_last``,
        ``col_obpm_last``, ``col_dbpm_last``, ``col_min_last``,
        ``col_bpm_career``, ``col_min_career``, ``col_seasons``,
        ``col_team_adj_em``, ``col_rapm_last``, ``euro_code``, ``euro_min``,
        ``euro_pir40``, ``euro_pts40`` and ``euro_top``. Each is null where the
        pick has no link to that source.

        - ``projected_pick`` equals the pick order (the curve is monotone).
        - ``pro_tier`` is ``lottery`` (1-14), ``first_round`` (15-30) or
          ``second_round``.
        - A year with no published class, or an empty ``draft_year`` list,
          returns the zero-row schema.

    Raises:
        ValueError: ``league`` is not ``"nba"``.
        SeasonNotFoundError: a draft year before 1996 (the draft release starts there).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_draft_model
            board = nba_draft_model(2019)
            print(board.select("overall_pick", "player_name", "proj_career_value").head())

        With the pre-draft college and EuroLeague columns::

            board = nba_draft_model(2019, pre_draft_features=True)

        Pipeline next step (one line)::

            board.filter(pl.col("pro_tier") == "lottery")

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college basketball (R)
            * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    _check_league(league)
    years = [draft_year] if isinstance(draft_year, int) else [int(y) for y in draft_year]
    schema = dict(_SCHEMA, **(FEATURE_SCHEMA if pre_draft_features else {}))
    picks = _picks(years) if years else pl.DataFrame()
    if picks.is_empty():
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    out = _score(picks, _load_artifact(league))
    if pre_draft_features:
        feats = _features.pre_draft_features(out.select("player_id", "draft_year", "overall_pick", "player_name"))
        out = out.join(feats.select("player_id", *FEATURE_SCHEMA), on="player_id", how="left")
    if college_prior is not None and not college_prior.is_empty():
        assert out.schema["player_id"] == college_prior.schema.get("player_id", pl.Utf8)
        out = out.join(college_prior, on="player_id", how="left", suffix="_prior")
    if gleague_bridge:
        bridge = _fetch_gleague_bridge(years)
        if not bridge.is_empty():
            assert out.schema["player_id"] == bridge.schema["player_id"]
            out = out.join(bridge, on=["player_id", "draft_year"], how="left")
    out = out.sort("draft_year", "overall_pick")
    return out.to_pandas() if return_as_pandas else out
