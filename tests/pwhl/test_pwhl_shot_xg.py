"""Offline tests for ``pwhl_shot_xg`` + the ``load_pwhl_xg_pbp`` loader contract.

Under ``_MIN_COORD_SHOTS`` qualifying shots the fitter deterministically falls
back to a constant-rate model at the observed goal rate, so a small synthetic
pbp exercises the full score path (context -> filter -> predict -> curated
select) with no network and no sklearn fit.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import yaml

import sportsdataverse.pwhl.pwhl_xg_proxy  # noqa: F401

_mod = sys.modules["sportsdataverse.pwhl.pwhl_xg_proxy"]

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"
_RELEASES = _ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

_POLARS_BY_NAME = {
    "Int32": pl.Int32,
    "Int64": pl.Int64,
    "Float64": pl.Float64,
    "String": pl.Utf8,
    "Utf8": pl.Utf8,
    "Boolean": pl.Boolean,
}


def _tiny_pbp() -> pl.DataFrame:
    """8 events, 4 on-net shots (1 goal) with rink-feet coordinates."""
    n = 8
    events = ["faceoff", "shot", "shot", "penalty", "shot", "goal", "shot", "faceoff"]
    return pl.DataFrame(
        {
            "game_id": pl.Series([101] * n, dtype=pl.Int32),
            "game_season": pl.Series([2025] * n, dtype=pl.Int32),
            "game_date": ["2025-01-05"] * n,
            "event": events,
            "team_id": pl.Series([1, 1, 2, 1, 2, 2, 1, 2], dtype=pl.Int32),
            "player_id": pl.Series(range(10, 10 + n), dtype=pl.Int32),
            "goalie_id": pl.Series([30] * n, dtype=pl.Int32),
            "period_of_game": ["1"] * n,
            "sec_from_start": pl.Series(range(0, n * 60, 60), dtype=pl.Int32),
            "clock": ["12:00"] * n,
            "x_coord": [None, 70.0, -60.0, None, 80.0, 85.0, 55.0, None],
            "y_coord": [None, 10.0, -5.0, None, 0.0, 2.0, -20.0, None],
            "event_type": [None, "wrist", "snap", None, "slap", "wrist", "backhand", None],
            "shot_quality": [
                None,
                "Quality on net",
                "Non quality on net",
                None,
                "Quality on net",
                "Quality goal",
                "Non quality on net",
                None,
            ],
            "power_play": pl.Series([None, None, 1, None, None, None, None, None], dtype=pl.Int32),
            "short_handed": [None] * n,
            "empty_net": [None] * n,
            "penalty_shot": [None] * n,
            "goal": [None, False, False, None, False, True, True, None],
        }
    )


def test_shot_xg_scores_shot_rows_with_the_published_schema():
    out = _mod.pwhl_shot_xg(_tiny_pbp())

    # 4 `event == "shot"` rows (the trailing goal row is not an on-net shot event)
    assert out.height == 4
    assert dict(out.schema) == dict(_mod._SHOT_XG_SCHEMA)
    # under _MIN_COORD_SHOTS the model is the constant observed-goal-rate
    # fallback: every xg equals the shot-row goal rate (1 goal / 4 shots)
    assert set(out["xg"].to_list()) == {0.25}
    assert out["shot_distance"].null_count() == 0


def test_shot_xg_empty_and_shotless_inputs_return_typed_empty():
    empty = _mod.pwhl_shot_xg(pl.DataFrame(schema={"event": pl.Utf8, "goal": pl.Boolean}))
    assert empty.height == 0
    assert dict(empty.schema) == dict(_mod._SHOT_XG_SCHEMA)

    shotless = _mod.pwhl_shot_xg(_tiny_pbp().filter(pl.col("event") == "faceoff"))
    assert shotless.height == 0
    assert dict(shotless.schema) == dict(_mod._SHOT_XG_SCHEMA)


def test_declared_loader_schema_matches_the_producer_output_schema():
    declared = yaml.safe_load(_SCHEMAS.read_text(encoding="utf-8"))["load_pwhl_xg_pbp"]
    produced = _mod._SHOT_XG_SCHEMA

    assert [c["name"] for c in declared] == list(produced.keys())
    for col in declared:
        assert _POLARS_BY_NAME[col["type"]] == produced[col["name"]], col["name"]


def test_loader_entry_points_at_the_published_tag_and_floor():
    loaders = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    entry = next(ld for ld in loaders if ld["fn"] == "load_pwhl_xg_pbp")

    assert entry["tag"] == "pwhl_xg_pbp"
    assert entry["base"] == "sdv_releases"
    assert entry["url"] == "pwhl_xg_pbp/pwhl_xg_pbp_{season}.parquet"
    # PWHL's inaugural season
    assert entry["min_season"] == 2024


def test_loader_is_exported():
    from sportsdataverse.pwhl import load_pwhl_xg_pbp

    assert callable(load_pwhl_xg_pbp)
