"""Phase-1 oracle gate: the bundled play-call classifier beats the shipped xpass.

Fixture provenance: tests/fixtures/nfl_scheme/README.md (captured 2026-07-08).
Eval window 2022-2023 is disjoint from the artifact's 2016-2021 training
seasons (as-of leakage boundary).
"""

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl.nfl_playcall import nfl_play_call_probabilities, playcall_features
from sportsdataverse.nfl.nfl_scheme_constants import auc_score, log_loss_score

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_scheme"


@pytest.fixture(scope="module")
def oracle_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "pbp_2021_2023_slice.parquet")


@pytest.fixture(scope="module")
def oracle_participation() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "participation_2021_2023.parquet")


def test_playcall_beats_shipped_xpass(oracle_pbp, oracle_participation):
    """Gate: run/pass log-loss <= shipped xpass; AUC >= shipped xpass (2022-2023 holdout).

    Observed at gate time (2026-07-08 fixture, n=73637 offensive plays):
    model_ll=0.4976 vs xpass_ll=0.5179; model_auc=0.8221 vs xpass_auc=0.7975.
    """
    df = oracle_pbp.filter(pl.col("season").is_in([2022, 2023]))
    scored = nfl_play_call_probabilities(df, oracle_participation)
    assert isinstance(scored, pl.DataFrame)
    assert df.schema["play_id"] == scored.schema["play_id"]
    assert df.schema["game_id"] == scored.schema["game_id"]
    j = df.join(scored.select("game_id", "play_id", "p_pass"), on=["game_id", "play_id"])
    j = j.filter(
        pl.col("xpass").is_not_null() & pl.col("p_pass").is_not_null() & ((pl.col("pass") == 1) | (pl.col("rush") == 1))
    )
    # join must cover essentially every offensive play
    assert j.height > 60000
    y = j["pass"].to_numpy()
    model_ll = log_loss_score(y, j["p_pass"].to_numpy())
    xpass_ll = log_loss_score(y, j["xpass"].to_numpy())
    model_auc = auc_score(y, j["p_pass"].to_numpy())
    xpass_auc = auc_score(y, j["xpass"].to_numpy())
    assert model_ll <= xpass_ll + 1e-4, f"model {model_ll} vs xpass {xpass_ll}"
    assert model_auc >= xpass_auc - 1e-4, f"model {model_auc} vs xpass {xpass_auc}"


def test_playcall_family_top1_accuracy(oracle_pbp, oracle_participation):
    """Gate: 5-class family top-1 accuracy >= 0.50 on the 2022-2023 holdout.

    Floor set from the observed value at gate time (0.5526 on 2026-07-08
    fixture; never lowered to pass).
    """
    df = oracle_pbp.filter(pl.col("season").is_in([2022, 2023]))
    scored = nfl_play_call_probabilities(df, oracle_participation)
    feat = playcall_features(df, oracle_participation).select("game_id", "play_id", "family")
    j = scored.join(feat, on=["game_id", "play_id"])
    assert j.height > 60000
    acc = (j["pred_family"] == j["family"]).mean()
    assert acc >= 0.50, f"family top-1 accuracy {acc}"
