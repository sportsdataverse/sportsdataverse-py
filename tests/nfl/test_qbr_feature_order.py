"""NFL QBR feature order (gap-C-083 = W-C3, critical; fixed by task 2.7).

The bundled `nfl/models/qbr_model.ubj` shipped without feature names, so XGBoost could not reject a permuted
input. Its trainer is `cfbfastR-dev/cfb_qbr/qbr_xgb.R` (cfb_qbr @ fb56ff3): the scoring call at :233 selects
`qbr_epa, pass_epa, rush_epa, sack_epa, pen_epa, spread`, and nfl-data `docs/models/qbr.qmd:188` lists the same
order. The processor used to feed `qbr_epa, sack_epa, pass_epa, rush_epa, pen_epa, spread`. Task 2.7 re-saved
the same trees with the trainer's names and reordered `model_vars.qbr_vars` to match.

The numeric check runs the real processor on three committed ESPN captures (provenance:
`tests/nfl/fixtures/README.md`) and reads each quarterback's model inputs and `exp_qbr` back from the
`advBoxScore["pass"]` rows: the published `exp_qbr` is the booster on the trainer order.
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
OLD_ORDER = ["qbr_epa", "sack_epa", "pass_epa", "rush_epa", "pen_epa", "spread"]  # pre-2.7 qbr_vars
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
    # Unnamed DMatrix: score a column ORDER, whatever names the booster carries.
    return booster.predict(DMatrix(df.select(order).to_numpy()), validate_features=False)


def test_exp_qbr_is_the_booster_on_qbr_vars(qb_rows, booster):
    """The published exp_qbr is the bundled booster fed `qbr_vars` order."""
    got = _score(booster, qb_rows, qbr_vars)
    np.testing.assert_allclose(got, qb_rows["exp_qbr"].to_numpy(), atol=1e-3)
    print("QBR_CONTROL_OK", qb_rows.height, "quarterbacks")


def test_the_permutation_is_not_a_no_op(qb_rows, booster):
    """The old and trainer orders give different scores on real rows, so the order test measures a real defect."""
    diff = np.abs(_score(booster, qb_rows, OLD_ORDER) - _score(booster, qb_rows, TRAINER_ORDER))
    assert diff.max() > 1.0, diff


def test_exp_qbr_is_the_booster_on_the_trainer_order(qb_rows, booster):
    want = _score(booster, qb_rows, TRAINER_ORDER)
    np.testing.assert_allclose(qb_rows["exp_qbr"].to_numpy(), want, atol=1e-3)


def test_qbr_model_names_its_features_in_the_trainer_order(booster):
    assert booster.feature_names == TRAINER_ORDER
    assert list(qbr_vars) == TRAINER_ORDER


def test_a_quarterback_with_no_sack_rush_or_penalty_gets_zero_inputs(booster):
    """R795-1: an empty play-type group is 0, as the trainer's replace_na made it (cfb_qbr@9ef6010 qbr_xgb.R:81-85).

    In the committed 2016 capture 400874485, Eli Manning has no sack, rush or penalty play, so each of those weighted
    means is 0/0. Served as NaN, the booster read them as missing, a value it never saw in training.
    """
    rows = pl.DataFrame(run(400874485).game["advBoxScore"]["pass"])
    eli = rows.filter(pl.col("passer_player_name") == "E.Manning")
    assert eli.height == 1
    assert eli.select("rush_epa", "sack_epa", "pen_epa").row(0) == (0.0, 0.0, 0.0)
    np.testing.assert_allclose(eli["exp_qbr"].to_numpy(), _score(booster, eli, TRAINER_ORDER), atol=1e-3)
