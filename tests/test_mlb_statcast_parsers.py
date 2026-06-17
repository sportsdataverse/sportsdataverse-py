from __future__ import annotations
from pathlib import Path
import polars as pl

FIX = Path(__file__).resolve().parent / "fixtures" / "mlb_statcast"


def test_csv_to_frame_snake_cases_and_rows():
    from sportsdataverse.mlb.mlb_statcast_parsers import _csv_to_frame

    df = _csv_to_frame((FIX / "search_small.csv").read_text())
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    for c in ("pitch_type", "game_date", "release_speed", "player_name", "events"):
        assert c in df.columns


def test_csv_to_frame_empty_is_zero_rows():
    from sportsdataverse.mlb.mlb_statcast_parsers import _csv_to_frame

    df = _csv_to_frame("")
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_html_script_json_extracts_var():
    from sportsdataverse.mlb.mlb_statcast_parsers import _html_script_json

    html = (FIX / "player_page.html").read_text()
    blob = _html_script_json(html, "serverVals")
    assert blob.get("playerId") == 592450
    assert blob["rows"][0]["metric"] == "xwoba"


def test_html_script_json_missing_returns_empty():
    from sportsdataverse.mlb.mlb_statcast_parsers import _html_script_json

    assert _html_script_json("<html></html>", "serverVals") == {}
