"""Contract tests for the 11 MLB model dataset loaders.

The declared returns-schemas were INTROSPECTED from the published parquet
footers (2026-07-17) rather than transcribed from module constants -- the
producer appends a ``season`` column to most stems. These tests pin the
loader entries and the load-bearing schema facts so future YAML edits can't
silently diverge from the published assets.
"""

from __future__ import annotations

from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMAS = _ROOT / "tools" / "codegen" / "schemas" / "loader_schemas.yaml"
_RELEASES = _ROOT / "tools" / "codegen" / "endpoints" / "releases.yaml"

#: stem -> (tag, min_season, has_trailing_season_col, load-bearing columns)
_STEMS = {
    "mlb_re24_matrix": ("mlb_game_state", 2015, True, ["base_state", "outs", "re"]),
    "mlb_we_table": ("mlb_game_state", 2015, True, ["base_state", "home_win_exp"]),
    "mlb_wpa": ("mlb_game_state", 2015, True, ["game_id", "at_bat_index", "wpa"]),
    "mlb_expected_stats": ("mlb_hitting_models", 2015, True, ["batter", "xwoba", "xba", "xslg"]),
    "mlb_expected_hr": ("mlb_hitting_models", 2015, True, ["batter", "xhr_park_adj"]),
    # the projection carries `age`, not a producer-appended season column, and
    # floors at 2016 (the first backfilled season has no prior history)
    "mlb_batter_projection": ("mlb_hitting_models", 2016, False, ["batter", "age", "proj_xwoba"]),
    "mlb_oaa": ("mlb_fielding_models", 2015, True, ["fielder_id", "position", "oaa"]),
    "mlb_catcher_framing": ("mlb_fielding_models", 2015, True, ["catcher_id", "framing_runs"]),
    "mlb_xera": ("mlb_pitching_models", 2015, True, ["pitcher", "x_era"]),
    "mlb_stuff_plus": ("mlb_pitching_models", 2015, True, ["pitcher", "pitch_type", "stuff_plus"]),
    "mlb_command_plus": ("mlb_pitching_models", 2015, True, ["pitcher", "command_plus"]),
}


def _loaders() -> dict:
    entries = yaml.safe_load(_RELEASES.read_text(encoding="utf-8"))["loaders"]
    return {e["fn"]: e for e in entries}


def _schemas() -> dict:
    return yaml.safe_load(_SCHEMAS.read_text(encoding="utf-8"))


def test_all_eleven_entries_point_at_their_tags_and_floors() -> None:
    loaders = _loaders()
    for stem, (tag, floor, _, _cols) in _STEMS.items():
        e = loaders[f"load_{stem}"]
        assert e["tag"] == tag, stem
        assert e["base"] == "sdv_releases", stem
        assert e["url"] == f"{tag}/{stem}_{{season}}.parquet", stem
        assert e["min_season"] == floor, stem


def test_declared_schemas_carry_the_load_bearing_columns() -> None:
    schemas = _schemas()
    for stem, (_tag, _floor, has_season, cols) in _STEMS.items():
        declared = {c["name"]: c["type"] for c in schemas[f"load_{stem}"]}
        for col in cols:
            assert col in declared, (stem, col)
        # the producer appends season (Int64) to every stem except the projection
        assert ("season" in declared) is has_season, stem
        if has_season:
            assert declared["season"] == "Int64", stem


def test_loaders_are_exported() -> None:
    import sportsdataverse.mlb as mlb

    for stem in _STEMS:
        assert callable(getattr(mlb, f"load_{stem}")), stem
