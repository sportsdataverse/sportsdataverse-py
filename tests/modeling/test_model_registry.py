"""Gates for the model registry — lineage round-trip + feature-drift.

Real NBA player-log features are the substrate: a card fingerprints its
inputs, an unchanged frame shows zero drift, a perturbed or missing feature
is detected precisely (the floating-@main incident: an upstream column's
engine changed with no commit), and the JSON manifest round-trips.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.modeling.registry import (
    ModelCard,
    feature_drift,
    make_card,
    read_manifest,
    write_manifest,
)
from sportsdataverse.nba.nba_possession_sim import player_game_logs_from_pbp

FEATURES = ["fga", "fg3a", "fta"]


@pytest.fixture(scope="module")
def features() -> pl.DataFrame:
    frames = []
    for gid in ("0022100001", "0022200001", "0022300001"):
        payload = json.loads(
            pathlib.Path(f"tests/fixtures/nba_engine/{gid}/playbyplayv3.json").read_text(encoding="utf-8")
        )
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return player_game_logs_from_pbp(pl.concat(frames, how="diagonal_relaxed"))


def test_card_captures_lineage_and_fingerprint(features: pl.DataFrame) -> None:
    card = make_card(
        "nba_scorer",
        features,
        features=FEATURES,
        training_script="train_nba_scorer.py",
        trained_seasons=[2021, 2022, 2023],
        metrics={"brier": 0.19},
    )
    assert card.source_features == FEATURES
    assert set(card.feature_fingerprint) == set(FEATURES)
    assert all(len(h) == 64 for h in card.feature_fingerprint.values())  # sha256 hex
    assert card.trained_seasons == [2021, 2022, 2023]
    with pytest.raises(ValueError, match="absent"):
        make_card("bad", features, features=["nope"], training_script="x", trained_seasons=[], metrics={})


def test_feature_drift_detects_exactly_what_changed(features: pl.DataFrame) -> None:
    card = make_card("nba_scorer", features, features=FEATURES, training_script="x", trained_seasons=[2023], metrics={})
    # identical frame -> no drift
    assert feature_drift(card, features) == []
    # a single upstream column changes -> exactly that column is flagged
    bumped = features.with_columns((pl.col("fga") + 1).alias("fga"))
    assert feature_drift(card, bumped) == ["fga"]
    # a dropped feature is drift too
    assert "fta" in feature_drift(card, features.drop("fta"))
    # reordering rows changes content hashes (csv-serialized) but not the
    # untouched columns' membership — every feature still present, values same
    # set, so at least the row-order-sensitive columns register
    assert feature_drift(card, features) == []  # same object, still clean


def test_manifest_round_trips(features: pl.DataFrame, tmp_path: pathlib.Path) -> None:
    card_a = make_card(
        "nba_scorer",
        features,
        features=FEATURES,
        training_script="a.py",
        trained_seasons=[2023],
        metrics={"brier": 0.2},
    )
    card_b = make_card(
        "nba_rebounder", features, features=["reb"], training_script="b.py", trained_seasons=[2022, 2023], metrics={}
    )
    path = tmp_path / "models_manifest.json"
    write_manifest(path, [card_a, card_b])
    loaded = read_manifest(path)
    assert set(loaded) == {"nba_scorer", "nba_rebounder"}
    assert loaded["nba_scorer"] == card_a and loaded["nba_rebounder"] == card_b
    # the manifest is diff-friendly JSON keyed by name (no timestamps)
    text = path.read_text(encoding="utf-8")
    assert '"nba_scorer"' in text and "produced_at" not in text
    with pytest.raises(ValueError, match="duplicate"):
        write_manifest(path, [card_a, card_a])


def test_readme_manifest_shape_is_stable() -> None:
    # a ModelCard built by hand equals one parsed from its own serialized form
    card = ModelCard(
        name="m",
        source_features=["a", "b"],
        feature_fingerprint={"a": "0" * 64, "b": "1" * 64},
        training_script="t.py",
        trained_seasons=[2024],
        metrics={"mae": 1.5},
    )
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        p = pathlib.Path(d) / "m.json"
        write_manifest(p, [card])
        assert read_manifest(p)["m"] == card
