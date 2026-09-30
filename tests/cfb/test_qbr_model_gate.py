"""The bundled xQBR model ships only with a passing gate record, and serving scores it.

The record is written by cfbfastR-cfb-data's ``train-qbr``. That command gates the fit
against the previous model on ESPN QBR over a frozen holdout, and writes no model when
the gate fails (``models/qbr/PREREG_xqbr_retrain.md`` there). Pinning the bundle's
sha256 to the record means a model that failed, or was never gated, cannot be copied in
without this test going red.
"""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

import polars as pl
from xgboost import DMatrix

from sportsdataverse.cfb import cfb_pbp
from sportsdataverse.cfb.model_vars import qbr_vars

MODELS = Path(cfb_pbp.__file__).parent / "models"
FIX = Path(__file__).parent / "fixtures"


# The bundle this model replaced. The record must say it beat THIS model, not one picked
# for the occasion. At the next swap, set this to the outgoing qbr_model.ubj's sha256.
REPLACED_BUNDLE_SHA256 = "2abfde6836a27ca46ea301e8dbf8a7c4eb4b9c00dbb54548056cb78bf23f1714"


def test_the_bundled_qbr_model_is_the_gated_candidate():
    """Re-derive the pass from the record's numbers; never loosen these to ship a model."""
    rec = json.loads((MODELS / "qbr_model.gate.json").read_text())
    ubj = hashlib.sha256((MODELS / "qbr_model.ubj").read_bytes()).hexdigest()
    assert rec["passed"] is True and rec["chosen"] in rec["arms"]
    assert rec["candidate_sha256"] == ubj, "qbr_model.ubj is not the model the gate passed"
    assert rec["incumbent_sha256"] == REPLACED_BUNDLE_SHA256, "gated against the wrong incumbent"
    arm = rec["arms"][rec["chosen"]]
    assert arm["ci_hi"] < 0 and arm["n"] >= 500 and rec["holdout"]["match_rate"] >= 0.95


def test_the_booster_reads_exactly_qbr_vars_and_no_spread():
    assert cfb_pbp.qbr_model.feature_names == qbr_vars
    assert "spread" not in qbr_vars and "rush_epa" in qbr_vars


def test_the_box_score_scores_xqbr_from_the_served_rows(monkeypatch):
    """Every passer's exp_qbr is the bundled booster applied to that row's qbr_vars.

    That is the input the model was trained on (the published adv_passing rows), so
    this is the train/serve contract checked from the serving side.
    """
    summary = json.loads((FIX / "summary_401754598.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = cfb_pbp.CFBPlayProcess(gameId=401754598)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    rows = pl.DataFrame(proc.run_processing_pipeline()["advBoxScore"]["pass"])
    assert rows.height >= 2
    # the JSON-ready rows carry None where a QB had no such plays (served as NaN = missing)
    X = rows.select(pl.col(qbr_vars).cast(pl.Float64)).to_pandas()
    want = cfb_pbp.qbr_model.predict(DMatrix(X))
    for got, exp in zip(rows["exp_qbr"].to_list(), want):
        assert math.isclose(got, float(exp), abs_tol=1e-3)
