"""Gates for cross-provider source-agreement, on real player-log frames.

Fixture-derived NBA player game logs are the substrate: reconcile detects
exact agreement (identity), the precise count and magnitude of injected
disagreements, tolerance-band agreement, key-presence asymmetry, and the
join-key dtype guard. The genuine same-game two-provider capture (e.g. box
vs pbp) is a follow-up fixture; these gates pin the mechanism on real data.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.modeling.integrity import agreement_summary, key_coverage, reconcile
from sportsdataverse.nba.nba_possession_sim import player_game_logs_from_pbp

KEYS = ["game_id", "player_id"]
COMPARE = ["pts", "reb", "ast"]


def _logs(game_ids: tuple[str, ...]) -> pl.DataFrame:
    frames = []
    for gid in game_ids:
        payload = json.loads(
            pathlib.Path(f"tests/fixtures/nba_engine/{gid}/playbyplayv3.json").read_text(encoding="utf-8")
        )
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return player_game_logs_from_pbp(pl.concat(frames, how="diagonal_relaxed"))


@pytest.fixture(scope="module")
def logs() -> pl.DataFrame:
    return _logs(("0022100001", "0022200001", "0022300001"))


def test_identity_is_total_agreement(logs: pl.DataFrame) -> None:
    recon = reconcile(logs, logs, keys=KEYS, compare=COMPARE)
    assert recon.height == logs.height * len(COMPARE)
    summary = agreement_summary(recon)
    assert set(summary["column"].to_list()) == set(COMPARE)
    assert (summary["agree_rate"] == 1.0).all()
    assert (summary["max_abs_diff"] == 0.0).all()


def test_injected_disagreements_are_counted_and_measured(logs: pl.DataFrame) -> None:
    k = 7
    perturbed = (
        logs.with_row_index("_i")
        .with_columns(pl.when(pl.col("_i") < k).then(pl.col("pts") + 5).otherwise(pl.col("pts")).alias("pts"))
        .drop("_i")
    )
    recon = reconcile(logs, perturbed, keys=KEYS, compare=COMPARE)
    summary = agreement_summary(recon)
    pts = summary.filter(pl.col("column") == "pts").row(0, named=True)
    assert pts["n_disagree"] == k
    assert pts["max_abs_diff"] == 5.0
    # the untouched columns still agree completely
    for col in ("reb", "ast"):
        row = summary.filter(pl.col("column") == col).row(0, named=True)
        assert row["agree_rate"] == 1.0
    # the disagreeing rows are exactly identifiable
    bad = recon.filter((pl.col("column") == "pts") & (pl.col("agree") == False))  # noqa: E712
    assert bad.height == k


def test_tolerance_absorbs_small_differences(logs: pl.DataFrame) -> None:
    perturbed = logs.with_columns((pl.col("pts") + 3).alias("pts"))
    strict = agreement_summary(reconcile(logs, perturbed, keys=KEYS, compare=["pts"]))
    lenient = agreement_summary(reconcile(logs, perturbed, keys=KEYS, compare=["pts"], tol=3.0))
    assert strict.row(0, named=True)["agree_rate"] == 0.0  # every row differs by 3
    assert lenient.row(0, named=True)["agree_rate"] == 1.0  # tol=3 absorbs it


def test_key_coverage_flags_presence_asymmetry() -> None:
    g1 = _logs(("0022100001",))
    g2 = _logs(("0022300001",))
    cov = key_coverage(g1, g2, keys=KEYS)
    assert cov["n_shared"] == 0  # disjoint games share no (game_id, player_id)
    assert cov["only_left"] == g1.select(KEYS).unique().height
    assert cov["only_right"] == g2.select(KEYS).unique().height
    assert cov["coverage"] == 0.0
    # a frame against itself is total coverage
    same = key_coverage(g1, g1, keys=KEYS)
    assert same["coverage"] == 1.0 and same["only_left"] == 0


def test_join_key_dtype_mismatch_is_an_error(logs: pl.DataFrame) -> None:
    str_ids = logs.with_columns(pl.col("player_id").cast(pl.Utf8))
    with pytest.raises(ValueError, match="dtype mismatch"):
        reconcile(logs, str_ids, keys=KEYS, compare=["pts"])
    with pytest.raises(ValueError, match="dtype mismatch"):
        key_coverage(logs, str_ids, keys=KEYS)


def test_empty_and_missing_column_paths(logs: pl.DataFrame) -> None:
    with pytest.raises(ValueError, match="no comparable columns"):
        reconcile(logs.select(KEYS), logs.select(KEYS), keys=KEYS)
    with pytest.raises(ValueError, match="missing"):
        reconcile(logs, logs, keys=KEYS, compare=["nonexistent"])
    assert agreement_summary(reconcile(logs, logs, keys=KEYS, compare=COMPARE).clear()).height == 0
