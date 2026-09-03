"""Native v3 expected-goals (xG) scoring -- reuse of the published fastRhockey boosters.

Faithful port of fastRhockey's NHL xG pipeline (``fastRhockey/R/helpers_nhl.R`` --
``helper_nhl_prepare_xg_data`` + ``helper_nhl_calculate_xg``, and the Python mirror at
``fastRhockey-nhl-raw/python/nhl_raw/xg.py``), adapted to the sdv-py
``load_nhl_pbp_full`` v3 loader column names. The two published boosters (5v5 / special
teams) and the penalty-shot constant are reused **verbatim** -- this module only
re-implements the feature-preparation recipe; it never retrains (see Decision D1 in the
NHL/PWHL player-impact design spec).

Danger-zone / distance / angle / rebound / rush are **descriptive output columns**, not
model features (Decision D2) -- adding them as features would force a booster retrain
and break the boosters' embedded ``feature_names``.

Attribution: this module continues fastRhockey's implementation (Apache-2.0) -- see
``THIRD_PARTY_NOTICES``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import polars as pl

from sportsdataverse.nhl.nhl_player_impact_constants import booster_cache_dir, get_constants

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["load_xg_models", "ensure_xg_models", "prepare_xg_features", "add_shot_geometry", "nhl_xg"]

_PS_DEFAULT = 0.3202197
_XG_RELEASE = "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nhl_xg_models"
_XG_FILES = ("xg_model_5v5.json", "xg_model_st.json", "xg_model_meta.json")

# secondary_type normalization spanning the 2010-2022 (Title Case) and 2023+ (lowercase
# abbreviated) NHL APIs -> the canonical values the xG models were trained on.
_SHOT_TYPE_NORM = {
    "wrist": "Wrist Shot",
    "wrist shot": "Wrist Shot",
    "snap": "Snap Shot",
    "snap shot": "Snap Shot",
    "slap": "Slap Shot",
    "slap shot": "Slap Shot",
    "backhand": "Backhand",
    "deflected": "Deflected",
    "tip-in": "Tip-In",
    "wrap-around": "Wrap-around",
    "bat": "Batted",
    "batted": "Batted",
    "poke": "Poke",
    "between-legs": "Between Legs",
    "between legs": "Between Legs",
    "cradle": "Cradle",
    "penalty shot": "Penalty Shot",
}
_SHOT_TYPE_COL = {
    "Wrist Shot": "wrist_shot",
    "Snap Shot": "snap_shot",
    "Slap Shot": "slap_shot",
    "Backhand": "backhand",
    "Wrap-around": "wrap_around",
    "Tip-In": "tip_in",
    "Deflected": "deflected",
    "Poke": "poke",
    "Batted": "batted",
    "Between Legs": "between_legs",
    "Cradle": "cradle",
}
_LAST_EVENT_COL = {
    "FACEOFF": "last_faceoff",
    "GIVEAWAY": "last_giveaway",
    "TAKEAWAY": "last_takeaway",
    "BLOCKED_SHOT": "last_blocked_shot",
    "HIT": "last_hit",
    "MISSED_SHOT": "last_missed_shot",
    "SHOT": "last_shot",
    "STOP": "last_stop",
    "PENALTY": "last_penalty",
    "GOAL": "last_goal",
}
_VALID_LAST = list(_LAST_EVENT_COL.keys())
_UNBLOCKED = ["SHOT", "MISSED_SHOT", "GOAL"]


def ensure_xg_models(model_dir: str | Path | None = None) -> Path:
    """Return a dir holding the 3 published booster files, downloading any missing ones.

    Mirrors the fastRhockey/nflverse download-on-demand + cache pattern -- the documented
    exception to "no first-use download" (the boosters are a large, already-published,
    already-validated artifact; see Decision D1 in the design spec). An explicit
    ``model_dir`` whose files already exist (e.g. the committed offline test fixtures)
    never touches the network.

    Args:
        model_dir: directory to check/populate; ``None`` resolves via
            ``booster_cache_dir()`` (env ``NHL_XG_MODEL_DIR`` override, else
            ``~/.cache/nhl_xg_models``).

    Returns:
        The resolved directory containing all 3 booster files.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_xg import ensure_xg_models
            d = ensure_xg_models()  # downloads on first use, cached after
    """
    d = booster_cache_dir(model_dir)
    missing = [fn for fn in _XG_FILES if not (d / fn).exists() or (d / fn).stat().st_size == 0]
    if missing:
        import requests

        d.mkdir(parents=True, exist_ok=True)
        for fn in missing:
            r = requests.get(
                f"{_XG_RELEASE}/{fn}",
                timeout=60,
                headers={"User-Agent": os.environ.get("SDV_PY_USER_AGENT", "sportsdataverse-py")},
            )
            r.raise_for_status()
            (d / fn).write_bytes(r.content)
    return d


def load_xg_models(model_dir: str | Path | None = None) -> dict:
    """Load the two published boosters (+ embedded feature names) and the penalty-shot constant.

    Args:
        model_dir: ``None`` downloads the canonical ``nhl_xg_models`` release on first
            use and caches under ``booster_cache_dir()``; pass a dir to use local models
            (the offline test suite always passes the committed fixture dir).

    Returns:
        dict with keys ``m5v5``/``mst`` (``xgboost.Booster``), ``feats_5v5``/``feats_st``
        (embedded feature-name lists), and ``ps`` (penalty-shot constant probability).

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_xg import load_xg_models
            models = load_xg_models("tests/fixtures/nhl_player_impact/xg_models")
    """
    import xgboost as xgb

    d = ensure_xg_models(model_dir)
    b5, bst = xgb.Booster(), xgb.Booster()
    b5.load_model(str(d / "xg_model_5v5.json"))
    bst.load_model(str(d / "xg_model_st.json"))
    meta = d / "xg_model_meta.json"
    ps = json.loads(meta.read_text()).get("xg_model_ps", _PS_DEFAULT) if meta.exists() else _PS_DEFAULT
    return {"m5v5": b5, "mst": bst, "feats_5v5": b5.feature_names, "feats_st": bst.feature_names, "ps": ps}


def _norm_secondary_type() -> pl.Expr:
    e = pl.col("secondary_type")
    expr = pl.when(e.is_null()).then(None)
    for raw, canon in _SHOT_TYPE_NORM.items():
        expr = expr.when(e.str.to_lowercase() == raw).then(pl.lit(canon))
    return expr.otherwise(e)


def _event_zone() -> pl.Expr:
    x, xf, eta = pl.col("x"), pl.col("x_fixed"), pl.col("event_team_abbr")
    home, away = pl.col("home_abbr"), pl.col("away_abbr")
    return (
        pl.when((x >= -25) & (x <= 25))
        .then(pl.lit("NZ"))
        .when(((xf < -25) & (eta == home)) | ((xf > 25) & (eta == away)))
        .then(pl.lit("DZ"))
        .when(((xf > 25) & (eta == home)) | ((xf < -25) & (eta == away)))
        .then(pl.lit("OZ"))
        .otherwise(None)
    )


def prepare_xg_features(pbp: pl.DataFrame) -> pl.DataFrame:
    """Port of ``helper_nhl_prepare_xg_data`` -- one row per unblocked shot, model features.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame (``x``, ``x_fixed``, ``strength_state``,
            ``home_skaters``/``away_skaters``, ``game_seconds``, ``event_id``,
            ``secondary_type``, ``event_team_abbr``, ``home_abbr``/``away_abbr``,
            ``season``, ``empty_net`` -- see ``load_nhl_pbp_full``'s returns table).

    Returns:
        polars.DataFrame: one row per unblocked shot (``SHOT``/``MISSED_SHOT``/``GOAL``)
        carrying every era one-hot, shot-type one-hot, last-event one-hot, and the
        derived ``rebound``/``rush``/``cross_ice_event``/``total_skaters_on``/
        ``event_team_advantage``/``empty_net`` columns the boosters expect. Empty/
        malformed input returns a zero-row frame (never raises).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_xg import prepare_xg_features
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            feat = prepare_xg_features(pbp)
            print(feat.shape)
    """
    df = pbp
    for col, default in (("strength_state", "5v5"), ("home_skaters", 5), ("away_skaters", 5)):
        if col not in df.columns:
            df = df.with_columns(pl.lit(default).alias(col))

    if df.height == 0:
        return df

    df = df.with_columns(secondary_type=_norm_secondary_type())
    df = df.filter(
        (pl.col("period_type") != "SHOOTOUT")
        & ((pl.col("secondary_type") != "Penalty Shot") | pl.col("secondary_type").is_null())
        & (pl.col("event_type") != "CHANGE")
    )
    if df.height == 0:
        return df

    grp = ["game_id", "period"]
    df = (
        df.with_columns(event_zone=_event_zone())
        .with_columns(
            last_event_type=pl.col("event_type").shift(1).over(grp),
            last_event_team=pl.col("event_team_abbr").shift(1).over(grp),
            time_since_last=(pl.col("game_seconds") - pl.col("game_seconds").shift(1).over(grp)),
            last_x=pl.col("x").shift(1).over(grp),
            last_y=pl.col("y").shift(1).over(grp),
            last_event_zone=pl.col("event_zone").shift(1).over(grp),
        )
        .with_columns(
            distance_from_last=(
                ((pl.col("y") - pl.col("last_y")) ** 2 + (pl.col("x") - pl.col("last_x")) ** 2).sqrt().round(1)
            ),
        )
    )

    df = df.filter(pl.col("event_type").is_in(_UNBLOCKED) & pl.col("last_event_type").is_in(_VALID_LAST))
    if df.height == 0:
        return df

    season = pl.col("season").cast(pl.Utf8)
    eta, home = pl.col("event_team_abbr"), pl.col("home_abbr")
    ets = pl.when(eta == home).then(pl.col("home_skaters")).otherwise(pl.col("away_skaters"))
    ots = pl.when(eta == home).then(pl.col("away_skaters")).otherwise(pl.col("home_skaters"))
    last_zone, last_type, tsl = pl.col("last_event_zone"), pl.col("last_event_type"), pl.col("time_since_last")
    df = df.with_columns(
        era_2011_2013=season.is_in(["20102011", "20112012", "20122013"]).cast(pl.Int64),
        era_2014_2018=season.is_in(["20132014", "20142015", "20152016", "20162017", "20172018"]).cast(pl.Int64),
        era_2019_2021=season.is_in(["20182019", "20192020", "20202021"]).cast(pl.Int64),
        era_2022_2024=season.is_in(["20212022", "20222023", "20232024"]).cast(pl.Int64),
        era_2025_on=(season.cast(pl.Float64) > 20232024).cast(pl.Int64),
        total_skaters_on=(ets + ots),
        event_team_advantage=(ets - ots),
        rebound=(last_type.is_in(["SHOT", "MISSED_SHOT", "GOAL"]) & (tsl <= 2)).cast(pl.Int64),
        rush=(last_zone.is_in(["NZ", "DZ"]) & (tsl <= 4)).cast(pl.Int64),
        cross_ice_event=(
            (last_zone == "OZ")
            & (((pl.col("last_y") > 3) & (pl.col("y") < -3)) | ((pl.col("last_y") < -3) & (pl.col("y") > 3)))
            & (tsl <= 2)
        ).cast(pl.Int64),
        empty_net=(pl.col("empty_net").cast(pl.Boolean).fill_null(False)).cast(pl.Int64),
    )

    onehots = {}
    for canon, col in _SHOT_TYPE_COL.items():
        onehots[col] = (pl.col("secondary_type") == canon).cast(pl.Int64)
    for raw, col in _LAST_EVENT_COL.items():
        onehots[col] = (pl.col("last_event_type") == raw).cast(pl.Int64)
    return df.with_columns(**onehots)


def add_shot_geometry(df: pl.DataFrame, *, league: str = "nhl") -> pl.DataFrame:
    """Attach ``distance_to_net`` / ``shot_angle`` / ``shot_danger`` (descriptive output only).

    Distance/angle are computed off ``x_fixed``/``y`` against the rink goal-line
    x-coordinate in ``LEAGUE_CONSTANTS[league].rink_x_goal_line``; ``shot_danger`` buckets
    into ``high``/``medium``/``low`` using the ``danger_high``/``danger_medium``
    distance+angle bands from the same config. These are output columns only -- never
    fed back into the boosters (Decision D2; a new feature would force a retrain).

    Args:
        df: any frame carrying ``x_fixed`` and ``y`` columns.
        league: ``"nhl"`` or ``"pwhl"`` -- selects the danger-zone bands.

    Returns:
        ``df`` with ``distance_to_net:Float64``, ``shot_angle:Float64``,
        ``shot_danger:Utf8`` appended.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_xg import add_shot_geometry
            out = add_shot_geometry(pl.DataFrame({"x_fixed": [80], "y": [0]}))
    """
    cfg = get_constants(league)
    goal_x = cfg.rink_x_goal_line
    dx = (goal_x - pl.col("x_fixed").abs()).abs()
    dy = pl.col("y").abs().cast(pl.Float64)
    distance = (dx**2 + dy**2).sqrt()
    angle = pl.arctan2(dy, dx) * (180.0 / np.pi)

    hi, med = cfg.danger_high, cfg.danger_medium
    danger = (
        pl.when((distance <= hi["max_distance"]) & (angle <= hi["max_angle"]))
        .then(pl.lit("high"))
        .when((distance <= med["max_distance"]) & (angle <= med["max_angle"]))
        .then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
    )
    return df.with_columns(
        distance_to_net=distance.alias("distance_to_net"),
        shot_angle=angle.alias("shot_angle"),
        shot_danger=danger.alias("shot_danger"),
    )


def nhl_xg(
    pbp: pl.DataFrame,
    *,
    model_dir: str | Path | None = None,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Score every unblocked shot in ``pbp`` with the published ``nhl_xg_models`` boosters.

    Ports fastRhockey's ``helper_nhl_calculate_xg`` -- routes 5v5 shots to the 5v5
    booster and every other strength state to the special-teams booster, overrides
    penalty shots with the constant ``xg_model_ps``, then left-joins ``xg`` back onto
    ``pbp`` by ``event_id``. Attaches the danger/distance/angle expansion
    (``add_shot_geometry``) after scoring.

    **Known issue -- the published boosters over-predict for seasons through 2023-24.**
    Measured 2026-09-02 against the 2026-04 boosters currently in the ``nhl_xg_models``
    release: observed goals / sum(``xg``) is **0.771** at 5v5 (n=1,724,290 shots) and
    **0.768** on special teams (n=349,232), where a correctly-levelled model gives 1.0 --
    i.e. ``xg`` is inflated by roughly 25-30% for every season from 2009-10 through
    2023-24. Seasons 2024-25 (0.949) and 2025-26 (0.913) are much closer. The cause is
    the boosters' training corpus, which carried no ``MISSED_SHOT`` events for the
    affected seasons; it is not a defect in the feature frame this function builds.
    Shot RANKING is far less affected (rank AUC 0.778 / 0.760), so ``xg`` is still usable
    for ordering chances -- but any SUM of ``xg`` (per game, per player, team totals,
    goals-above-expected, and ``nhl_gsax`` downstream) is inflated for pre-2024-25
    seasons. Tracking:
    `sportsdataverse-py#444 <https://github.com/sportsdataverse/sportsdataverse-py/issues/444>`_;
    evidence:
    `fastRhockey-nhl-data#11 <https://github.com/sportsdataverse/fastRhockey-nhl-data/pull/11>`_.
    To check whether this still applies to the boosters you have, sum ``xg`` over a
    season and compare against actual goals -- a corrected booster gives a ratio near
    1.0 -- or run ``nhl_data_build.xg_parity.artifact_calibration`` in
    ``fastRhockey-nhl-data``.

    Args:
        pbp: a ``load_nhl_pbp_full``-shaped frame.
        model_dir: booster directory; ``None`` downloads-and-caches on first use (see
            ``ensure_xg_models``). Offline callers should pass the committed fixture dir.
        league: ``"nhl"`` or ``"pwhl"`` -- selects the danger-zone geometry bands (the
            PWHL borrows the NHL boosters themselves; see ``xg_booster_league``).
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        ``pbp`` with ``xg:Float64``, ``distance_to_net:Float64``, ``shot_angle:Float64``,
        ``shot_danger:Utf8`` appended (null/absent for non-shot rows). Empty/malformed
        input returns the input frame with a null ``xg`` column -- never raises.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nhl.nhl_xg import nhl_xg
            pbp = pl.read_parquet("tests/fixtures/nhl_player_impact/pbp_sample.parquet")
            scored = nhl_xg(pbp, model_dir="tests/fixtures/nhl_player_impact/xg_models")
            print(scored.filter(pl.col("xg").is_not_null()).height)

        Pandas round-trip::

            scored_pd = nhl_xg(pbp, return_as_pandas=True)

    See Also:
        * `fastRhockey`_ -- R companion package; this module ports its xG pipeline.

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    import xgboost as xgb

    if pbp.height == 0:
        out = pbp.with_columns(xg=pl.lit(None, dtype=pl.Float64))
        out = add_shot_geometry(out, league=league) if {"x_fixed", "y"}.issubset(out.columns) else out
        return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out

    models = load_xg_models(model_dir)
    prep = prepare_xg_features(pbp)
    if prep.height == 0:
        out = pbp.with_columns(xg=pl.lit(None, dtype=pl.Float64))
        out = add_shot_geometry(out, league=league)
        return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out

    def _matrix(sub: pl.DataFrame, feats: list[str]) -> np.ndarray:
        arr = np.zeros((sub.height, len(feats)), dtype=np.float32)
        present = set(sub.columns)
        for j, f in enumerate(feats):
            if f in present:
                arr[:, j] = sub[f].cast(pl.Float64).fill_null(0).to_numpy()
        return arr

    parts = []
    for sub, feats, booster in (
        (prep.filter(pl.col("strength_state") == "5v5"), models["feats_5v5"], models["m5v5"]),
        (prep.filter(pl.col("strength_state") != "5v5"), models["feats_st"], models["mst"]),
    ):
        if sub.height == 0:
            continue
        dm = xgb.DMatrix(_matrix(sub, feats), feature_names=list(feats))
        preds = booster.predict(dm)
        parts.append(sub.select("game_id", "event_id").with_columns(xg=pl.Series(preds, dtype=pl.Float64)))

    xg_results = (
        pl.concat(parts)
        if parts
        else pl.DataFrame(schema={"game_id": pl.Int64, "event_id": pl.Int64, "xg": pl.Float64})
    )
    # event_id is scoped PER-GAME (not globally unique) -- the join key must be the
    # composite (game_id, event_id), or rows from different games sharing an event_id
    # fan out into duplicate joined rows.
    for key in ("game_id", "event_id"):
        assert pbp.schema[key] == xg_results.schema[key], (
            f"{key} dtype mismatch: pbp={pbp.schema[key]} vs xg_results={xg_results.schema[key]}"
        )
    # `load_nhl_pbp_full` may already carry its own (producer-side) `xg` passthrough
    # column -- drop it before joining so our computed `xg` isn't shadowed/suffixed.
    pbp_base = pbp.drop("xg") if "xg" in pbp.columns else pbp
    out = pbp_base.join(xg_results, on=["game_id", "event_id"], how="left")
    if "secondary_type" in out.columns:
        out = out.with_columns(
            xg=pl.when(pl.col("secondary_type") == "Penalty Shot").then(models["ps"]).otherwise(pl.col("xg")),
        )
    if "event_idx" in out.columns:
        out = out.sort("event_idx")
    out = add_shot_geometry(out, league=league)
    return out.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else out
