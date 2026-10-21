"""NFL QBR feature order: the guard input for task 2.7 (gap-C-083 = W-C3, critical).

`nfl/models/qbr_model.ubj` embeds no feature names, so XGBoost cannot reject a permuted input. Its trainer is
`cfbfastR-dev/cfb_qbr/qbr_xgb.R` (cfb_qbr @ fb56ff3): the scoring call at :233 selects
`qbr_epa, pass_epa, rush_epa, sack_epa, pen_epa, spread`, and nfl-data `docs/models/qbr.qmd:188` lists the same
order. The processor feeds `model_vars.qbr_vars` = `qbr_epa, sack_epa, pass_epa, rush_epa, pen_epa, spread`
(`nfl_pbp.py` `DMatrix(pass_qbr[qbr_vars], ...)`): pass EPA is scored as the sack slot, rush as pass, sack as
rush, on every published `exp_qbr`.

The numeric check runs the real processor on three committed ESPN captures (provenance:
`tests/nfl/fixtures/README.md`) and reads each quarterback's model inputs and `exp_qbr` back from the
`advBoxScore["pass"]` rows. A passing control proves the booster on `qbr_vars` reproduces the published
`exp_qbr` exactly (so the inputs are the right ones); the strict xfails pin that the trainer order does NOT,
and that the artifact carries no names. Task 2.7 (named re-save + reorder) flips both to XPASS.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from xgboost import Booster, DMatrix

from sportsdataverse.nfl.model_vars import qbr_vars
from tests.nfl._whole_game import run

pytestmark = pytest.mark.xdist_group("nfl_whole_game")  # share 1.1's per-worker processor cache

QBR_MODEL = Path(__file__).resolve().parents[2] / "sportsdataverse" / "nfl" / "models" / "qbr_model.ubj"
TRAINER_ORDER = ["qbr_epa", "pass_epa", "rush_epa", "sack_epa", "pen_epa", "spread"]
GAMES = (401547449, 401671740, 401872922)


@pytest.fixture(scope="module")
def qb_rows() -> pl.DataFrame:
    rows = [r for g in GAMES for r in run(g).game["advBoxScore"]["pass"]]
    df = pl.DataFrame(rows).select("passer_player_name", *qbr_vars, "exp_qbr")
    assert df.height >= 6 and df["exp_qbr"].null_count() == 0
    return df


@pytest.fixture(scope="module")
def booster() -> Booster:
    b = Booster()
    b.load_model(str(QBR_MODEL))
    assert b.num_features() == 6
    return b


def _score(booster: Booster, df: pl.DataFrame, order: list[str]) -> np.ndarray:
    # Unnamed booster: the column ORDER is the only contract, exactly as in nfl_pbp.py.
    return booster.predict(DMatrix(df.select(order).to_numpy()), validate_features=False)


def test_exp_qbr_is_the_booster_on_qbr_vars(qb_rows, booster):
    """Control: the published exp_qbr is the bundled booster fed `qbr_vars` order."""
    got = _score(booster, qb_rows, qbr_vars)
    np.testing.assert_allclose(got, qb_rows["exp_qbr"].to_numpy(), atol=1e-3)
    print("QBR_CONTROL_OK", qb_rows.height, "quarterbacks")


def test_the_permutation_is_not_a_no_op(qb_rows, booster):
    """The two orders give different scores on real rows, so the xfail below measures a real defect."""
    diff = np.abs(_score(booster, qb_rows, qbr_vars) - _score(booster, qb_rows, TRAINER_ORDER))
    assert diff.max() > 1.0, diff


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gap-C-083 -> task 2.7: qbr_vars permutes the trainer's pass/rush/sack slots",
)
def test_exp_qbr_is_the_booster_on_the_trainer_order(qb_rows, booster):
    want = _score(booster, qb_rows, TRAINER_ORDER)
    np.testing.assert_allclose(qb_rows["exp_qbr"].to_numpy(), want, atol=1e-3)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="gap-C-083 -> task 2.7: qbr_model.ubj embeds no feature names (named re-save)",
)
def test_qbr_model_names_its_features_in_the_trainer_order(booster):
    assert booster.feature_names == TRAINER_ORDER
    assert list(qbr_vars) == TRAINER_ORDER
