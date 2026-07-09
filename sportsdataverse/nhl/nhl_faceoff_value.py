"""NHL/PWHL faceoff-win value (T5.2 model 4).

Extracts faceoff events from a parsed pbp frame, fits a context logistic
(``P(taker_wins) ~ zone + strength + is_home``) as the expected-win
baseline, and aggregates each player's win rate above that context
expectation into a zone-weighted ``faceoff_value``. The context logistic
and the zone-weighting are league-agnostic; the zone weights come from
:func:`sportsdataverse.nhl.nhl_microstat_constants.get_constants`.

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_faceoff_value import nhl_faceoff_value

        out = nhl_faceoff_value(pbp)
        print(out.sort("faceoff_value", descending=True).head())

See Also:
    * `nhl-api-py`_ -- Python NHL API client (companion data source).

.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import overload

import pandas as pd
import polars as pl
from sklearn.linear_model import LogisticRegression

from sportsdataverse.nhl.nhl_microstat_constants import get_constants

FACEOFF_SCHEMA = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "period": pl.Int64,
    "zone_code": pl.Utf8,
    "strength_state": pl.Utf8,
    "winner_player_id": pl.Utf8,
    "loser_player_id": pl.Utf8,
    "winner_team_id": pl.Utf8,
    "is_home_win": pl.Boolean,
}

VALUE_SCHEMA = {
    "player_id": pl.Utf8,
    "faceoffs_taken": pl.Int64,
    "faceoffs_won": pl.Int64,
    "fo_win_pct": pl.Float64,
    "fo_win_pct_above_exp": pl.Float64,
    "faceoff_value": pl.Float64,
}


def _strength_state(situation_code: str | None, is_winner_home: bool) -> str | None:
    """Derive even/pp/pk from the winner-team perspective's skater counts.

    ``situation_code`` is a 4-digit string ``<away_goalie><away_skaters>
    <home_skaters><home_goalie>``. even skaters -> "even"; winner's side
    has more skaters -> "pp"; fewer -> "pk".
    """
    if situation_code is None or len(situation_code) != 4 or not situation_code.isdigit():
        return None
    away_skaters, home_skaters = int(situation_code[1]), int(situation_code[2])
    winner_skaters, opp_skaters = (home_skaters, away_skaters) if is_winner_home else (away_skaters, home_skaters)
    if winner_skaters == opp_skaters:
        return "even"
    return "pp" if winner_skaters > opp_skaters else "pk"


def extract_faceoffs(pbp: pl.DataFrame) -> pl.DataFrame:
    """Extract one row per faceoff event from a parsed pbp frame.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract): ``type_desc_key``,
            ``zone_code``, ``situation_code``, ``winning_player_id``,
            ``losing_player_id``, ``event_owner_team_id``, ``home_team_id``.

    Returns:
        One row per faceoff with columns ``game_id``, ``season``,
        ``period``, ``zone_code``, ``strength_state`` (``"even"``/``"pp"``/
        ``"pk"`` from the winner's perspective), ``winner_player_id``,
        ``loser_player_id``, ``winner_team_id``, ``is_home_win``. Zero-row
        input (or a frame missing ``type_desc_key``) returns a zero-row
        frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_faceoff_value import extract_faceoffs

            faceoffs = extract_faceoffs(pbp)
    """
    if pbp.height == 0 or "type_desc_key" not in pbp.columns:
        return pl.DataFrame(schema=FACEOFF_SCHEMA)

    fo = pbp.filter(pl.col("type_desc_key") == "faceoff")
    if fo.height == 0:
        return pl.DataFrame(schema=FACEOFF_SCHEMA)

    fo = fo.with_columns(
        pl.col("event_owner_team_id").alias("winner_team_id"),
        (pl.col("event_owner_team_id") == pl.col("home_team_id")).alias("is_home_win"),
    )
    strength = [
        _strength_state(sc, home_win)
        for sc, home_win in zip(fo["situation_code"].to_list(), fo["is_home_win"].to_list())
    ]
    fo = fo.with_columns(pl.Series("strength_state", strength, dtype=pl.Utf8))
    out = fo.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("period").cast(pl.Int64),
        pl.col("zone_code").cast(pl.Utf8),
        pl.col("strength_state").cast(pl.Utf8),
        pl.col("winning_player_id").cast(pl.Utf8).alias("winner_player_id"),
        pl.col("losing_player_id").cast(pl.Utf8).alias("loser_player_id"),
        pl.col("winner_team_id").cast(pl.Utf8),
        pl.col("is_home_win").cast(pl.Boolean),
    )
    return out


@dataclass(frozen=True)
class FaceoffContextModel:
    """Fitted context logistic for faceoff-win expectation.

    Args:
        model: Fitted :class:`~sklearn.linear_model.LogisticRegression`, or
            ``None`` for the constant-0.5 fallback (insufficient rows).
        feature_names: Ordered one-hot column names the model was fit on.
    """

    model: LogisticRegression | None
    feature_names: list[str]

    def predict(self, rows: pl.DataFrame) -> pl.Series:
        """Predict expected taker-win probability for each row.

        Args:
            rows: Frame with ``zone_code``, ``strength_state``, ``is_home``.

        Returns:
            A ``pl.Series`` named ``"expected_win"``.
        """
        if rows.height == 0:
            return pl.Series("expected_win", [], dtype=pl.Float64)
        if self.model is None:
            return pl.Series("expected_win", [0.5] * rows.height, dtype=pl.Float64)
        x = _context_design_matrix(rows, self.feature_names).to_numpy()
        proba = self.model.predict_proba(x)[:, 1]
        return pl.Series("expected_win", proba, dtype=pl.Float64)


def _context_design_matrix(rows: pl.DataFrame, feature_names: list[str]) -> pd.DataFrame:
    frame = rows.select("zone_code", "strength_state", "is_home").to_pandas()
    dummies = pd.get_dummies(frame, columns=["zone_code", "strength_state"], dtype=float)
    for col in feature_names:
        if col not in dummies.columns:
            dummies[col] = 0.0
    return dummies[feature_names]


def fit_faceoff_context(faceoffs: pl.DataFrame) -> FaceoffContextModel:
    """Fit the context logistic on a taker-perspective expansion of faceoffs.

    Each faceoff becomes two rows (winner: ``won=1``, loser: ``won=0``)
    with features ``zone_code``, ``strength_state``, ``is_home``. Falls
    back to a constant 0.5 expectation when fewer than 20 rows are present.

    Args:
        faceoffs: Output of :func:`extract_faceoffs`.

    Returns:
        A fitted :class:`FaceoffContextModel`.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_faceoff_value import extract_faceoffs, fit_faceoff_context

            model = fit_faceoff_context(extract_faceoffs(pbp))
    """
    taker_rows = _taker_perspective_rows(faceoffs)
    frame = taker_rows.select("zone_code", "strength_state", "is_home").to_pandas()
    dummies = pd.get_dummies(frame, columns=["zone_code", "strength_state"], dtype=float)
    feature_names = list(dummies.columns)
    if taker_rows.height < 20 or len(feature_names) == 0:
        return FaceoffContextModel(model=None, feature_names=feature_names)
    y = taker_rows["won"].to_numpy()
    if len(set(y.tolist())) < 2:
        return FaceoffContextModel(model=None, feature_names=feature_names)
    clf = LogisticRegression(max_iter=1000)
    clf.fit(dummies.to_numpy(), y)
    return FaceoffContextModel(model=clf, feature_names=feature_names)


def _taker_perspective_rows(faceoffs: pl.DataFrame) -> pl.DataFrame:
    if faceoffs.height == 0:
        return pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "zone_code": pl.Utf8,
                "strength_state": pl.Utf8,
                "is_home": pl.Boolean,
                "won": pl.Int64,
                "game_id": pl.Utf8,
            }
        )
    winners = faceoffs.select(
        pl.col("winner_player_id").alias("player_id"),
        "zone_code",
        "strength_state",
        pl.col("is_home_win").alias("is_home"),
        pl.lit(1).alias("won"),
        "game_id",
    )
    # Loser's zone/strength are the opposite side's perspective: zone_code and
    # strength_state are recorded from the *winner's* perspective (extract_faceoffs
    # derives strength_state via is_home_win), so both must flip for the loser's
    # row -- O/D swap (N stays N) and pp/pk swap (even stays even). Skipping the
    # strength flip is a real bug: it mislabels the loser's own special-teams
    # state as the winner's, contaminating the context logistic's zone/strength
    # buckets (caught by the Task-1.3 calibration gate on the real corpus).
    loser_zone = (
        pl.when(pl.col("zone_code") == "O")
        .then(pl.lit("D"))
        .when(pl.col("zone_code") == "D")
        .then(pl.lit("O"))
        .otherwise(pl.col("zone_code"))
    )
    loser_strength = (
        pl.when(pl.col("strength_state") == "pp")
        .then(pl.lit("pk"))
        .when(pl.col("strength_state") == "pk")
        .then(pl.lit("pp"))
        .otherwise(pl.col("strength_state"))
    )
    losers = faceoffs.select(
        pl.col("loser_player_id").alias("player_id"),
        loser_zone.alias("zone_code"),
        loser_strength.alias("strength_state"),
        (~pl.col("is_home_win")).alias("is_home"),
        pl.lit(0).alias("won"),
        "game_id",
    )
    return pl.concat([winners, losers], how="vertical_relaxed").filter(pl.col("player_id").is_not_null())


@overload
def nhl_faceoff_value(pbp: pl.DataFrame, *, league: str = ..., return_as_pandas: bool = False) -> pl.DataFrame: ...
@overload
def nhl_faceoff_value(pbp: pl.DataFrame, *, league: str = ..., return_as_pandas: bool) -> pd.DataFrame: ...
def nhl_faceoff_value(
    pbp: pl.DataFrame,
    *,
    league: str = "nhl",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Per-player context-adjusted faceoff-win value.

    Fits :func:`fit_faceoff_context` on the taker-perspective expansion of
    every faceoff in ``pbp``, then aggregates each player's win rate above
    the context expectation and a zone-weighted ``faceoff_value`` using
    ``get_constants(league).faceoff_zone_weights``.

    Args:
        pbp: Parsed pbp frame (Task-0.1 contract).
        league: League key for :func:`~sportsdataverse.nhl.nhl_microstat_constants.get_constants`
            (``"nhl"`` or ``"pwhl"``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Per-player frame: ``player_id``, ``faceoffs_taken``,
        ``faceoffs_won``, ``fo_win_pct``, ``fo_win_pct_above_exp``,
        ``faceoff_value``. Zero-row input returns a zero-row frame with
        this schema.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_faceoff_value import nhl_faceoff_value

            out = nhl_faceoff_value(pbp)

        PWHL::

            out_pwhl = nhl_faceoff_value(pwhl_pbp, league="pwhl")

    See Also:
        * `cfbfastR`_ -- shares the league-agnostic-algorithm /
          per-league-constants pattern for CFB.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    faceoffs = extract_faceoffs(pbp)
    if faceoffs.height == 0:
        empty = pl.DataFrame(schema=VALUE_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    model = fit_faceoff_context(faceoffs)
    taker_rows = _taker_perspective_rows(faceoffs)
    expected = model.predict(taker_rows)
    taker_rows = taker_rows.with_columns(expected.alias("expected_win"))

    zone_weights = get_constants(league).faceoff_zone_weights
    weight_expr = pl.col("zone_code").replace_strict(zone_weights, default=0.0, return_dtype=pl.Float64)
    taker_rows = taker_rows.with_columns(
        weight_expr.alias("zone_weight"),
        (pl.col("won") - pl.col("expected_win")).alias("residual"),
    )

    out = taker_rows.group_by("player_id").agg(
        pl.len().alias("faceoffs_taken"),
        pl.col("won").sum().alias("faceoffs_won"),
        pl.col("expected_win").mean().alias("_mean_expected"),
        (pl.col("residual") * pl.col("zone_weight")).sum().alias("faceoff_value"),
    )
    out = out.with_columns(
        (pl.col("faceoffs_won") / pl.col("faceoffs_taken")).alias("fo_win_pct"),
        (pl.col("faceoffs_won") / pl.col("faceoffs_taken") - pl.col("_mean_expected")).alias("fo_win_pct_above_exp"),
    )
    out = out.select(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("faceoffs_taken").cast(pl.Int64),
        pl.col("faceoffs_won").cast(pl.Int64),
        pl.col("fo_win_pct").cast(pl.Float64),
        pl.col("fo_win_pct_above_exp").cast(pl.Float64),
        pl.col("faceoff_value").cast(pl.Float64),
    )
    return out.to_pandas() if return_as_pandas else out
