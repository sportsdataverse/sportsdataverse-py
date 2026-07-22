"""Offline smoke for the multi-game shelf builder CLI (fixture inputs)."""

from __future__ import annotations

import pathlib

import polars as pl
import pytest

from sportsdataverse.modeling.registry import feature_drift, read_manifest
from tools.sim_shelves.build_shelves import main

CASES = {
    "nba": [
        "tests/fixtures/nba_engine/0022100001/playbyplayv3.json",
        "tests/fixtures/nba_engine/0022300001/playbyplayv3.json",
    ],
    "wnba": ["tests/fixtures/espn/summary_wnba.json"],
    "nfl": ["tests/fixtures/espn/summary_nfl.json"],
    "mlb": ["tests/fixtures/mlb_api/play_by_play_745282.json"],
    "nhl": ["tests/fixtures/nhl_api_web/pbp_2024_scf_g7.json"],
}


@pytest.mark.parametrize("sport", sorted(CASES))
def test_cli_builds_events_with_fingerprint(sport: str, tmp_path: pathlib.Path) -> None:
    out = tmp_path / f"{sport}_events.parquet"
    code = main([sport, "--from-files", *CASES[sport], "--out", str(out)])
    assert code == 0
    assert out.exists()
    events = pl.read_parquet(out)
    assert events.height > 50
    assert (tmp_path / f"{sport}_events.parquet.fingerprint.json").exists()
    # registry card sidecar: fingerprints the exact frame that was written,
    # so a clean reload reports zero feature drift and a perturbed column
    # is named
    cards = read_manifest(tmp_path / f"{sport}_events.parquet.card.json")
    card = cards[f"{sport}_events"]
    assert card.source_features == events.columns
    assert card.metrics["n_events"] == float(events.height)
    assert feature_drift(card, events) == []
    victim = card.source_features[-1]
    perturbed = events.with_columns(pl.col(victim).shift(1).alias(victim))
    assert victim in feature_drift(card, perturbed)
    assert victim in feature_drift(card, events.drop(victim))


def test_cli_builds_model_shelf(tmp_path: pathlib.Path) -> None:
    out = tmp_path / "nba_shelf.parquet"
    code = main(["nba", "--from-files", *CASES["nba"], "--out", str(out), "--shelf", "--models"])
    assert code == 0
    from sportsdataverse.nba.nba_possession_sim import shelf_from_parquet

    shelf = shelf_from_parquet(out)
    assert shelf.meta["shelf_kind"] == "models2shelf"
    assert len(shelf.outcome_pmfs) == 144
    cards = read_manifest(tmp_path / "nba_shelf.parquet.card.json")
    assert cards["nba_shelf"].training_script == "tools/sim_shelves/build_shelves.py"
    assert cards["nba_shelf"].metrics["n_events"] > 0


def test_cli_requires_an_input_mode(tmp_path: pathlib.Path) -> None:
    with pytest.raises(SystemExit):
        main(["nba", "--out", str(tmp_path / "x.parquet")])
