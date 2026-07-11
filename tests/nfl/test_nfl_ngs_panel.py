"""Unit tests for the NGS aggregate panel builder (offline, fake loader)."""

import polars as pl

from sportsdataverse.nfl.nfl_ngs_tracking import _ngs_panel


def _fake_loader(seasons, stat_type, return_as_pandas=False):
    return pl.DataFrame(
        {
            "season": [2023, 2023, 2023],
            "week": [0, 1, 2],
            "player_gsis_id": [123, 123, 123],
            "avg_separation": [3.0, 2.8, 3.2],
        }
    )


def test_season_level_keeps_week_zero():
    p = _ngs_panel([2023], "receiving", level="season", _loader=_fake_loader)
    assert p.height == 1 and p["week"][0] == 0
    assert p.schema["player_gsis_id"] == pl.Utf8 and p.schema["season"] == pl.Int64


def test_week_level_drops_week_zero():
    p = _ngs_panel([2023], "receiving", level="week", _loader=_fake_loader)
    assert p.height == 2 and p["week"].min() > 0


def test_int_id_stringifies_without_float_artifact():
    p = _ngs_panel([2023], "receiving", level="season", _loader=_fake_loader)
    assert p["player_gsis_id"][0] == "123"  # not "123.0"


def test_empty_loader_returns_empty_frame():
    def _empty(seasons, stat_type, return_as_pandas=False):
        return pl.DataFrame()

    p = _ngs_panel([2023], "receiving", level="season", _loader=_empty)
    assert p.height == 0
    assert "player_gsis_id" in p.columns and "season" in p.columns
