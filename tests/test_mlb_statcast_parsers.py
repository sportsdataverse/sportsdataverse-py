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


def test_html_script_json_handles_nested_objects():
    """Balanced-brace decoder handles nested objects and no-semicolon terminator.

    The old non-greedy regex required a trailing ';' — Baseball Savant omits it
    in some payloads, causing a silent {} return.  raw_decode handles both cases.
    """
    from sportsdataverse.mlb.mlb_statcast_parsers import _html_script_json

    # Semicoloned form — was accidentally handled by old regex
    html_semi = '<script>var serverVals = {"playerId": 1, "stats": {"hr": 30, "avg": 0.31}, "ok": true};</script>'
    blob = _html_script_json(html_semi, "serverVals")
    assert blob["stats"]["hr"] == 30
    assert blob["ok"] is True

    # No-semicolon form — old regex returned {} (silent data loss); new code must succeed
    html_no_semi = 'var serverVals = {"playerId":592450,"stats":{"hr":30},"rows":[{"metric":"xwoba"}]}'
    blob2 = _html_script_json(html_no_semi, "serverVals")
    assert blob2["stats"]["hr"] == 30
    assert blob2["rows"][0]["metric"] == "xwoba"
