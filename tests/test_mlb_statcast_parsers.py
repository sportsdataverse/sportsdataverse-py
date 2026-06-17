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
    # Savant serializes playerId as a string in serverVals.
    assert str(blob.get("playerId")) == "592450"
    # Real Savant player pages embed array-valued sections (statcast, …), not a `rows` key.
    assert isinstance(blob["statcast"], list) and "xwoba" in blob["statcast"][0]


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


def test_parse_leaderboard_csv():
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_leaderboard

    df = parse_mlb_statcast_leaderboard((FIX / "leaderboard_xstats.csv").read_text())
    assert df.height == 1 and "xwoba" in df.columns


def test_parse_leaderboard_real_capture():
    """Real Savant CSV (catcher-stance, captured) parses + snake-cases."""
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_leaderboard

    df = parse_mlb_statcast_leaderboard((FIX / "leaderboard_catcher_stance.csv").read_text())
    assert df.height == 7  # one row per season in the capture
    for c in ("name", "year", "knee_down_pct", "one_knee_framing_rv"):
        assert c in df.columns


def test_parse_leaderboard_non_str_is_empty():
    """A dict (JSON) payload — wrong shape for a CSV parser — yields zero rows, not a raise."""
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_leaderboard

    df = parse_mlb_statcast_leaderboard({"not": "csv"})
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_parse_gamefeed_pitches():
    """Real /gf payload: one row per pitch from team_home + team_away."""
    import json

    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_gamefeed

    payload = json.loads((FIX / "gamefeed.json").read_text())
    df = parse_mlb_statcast_gamefeed(payload)
    # fixture trims each side to 2 pitches -> 4 rows
    assert df.height == 4 and "pitch_type" in df.columns and "start_speed" in df.columns


def test_parse_gamefeed_falls_back_to_exit_velocity():
    """When team_home/away are absent, fall back to the exit_velocity array."""
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_gamefeed

    df = parse_mlb_statcast_gamefeed({"exit_velocity": [{"pitch_type": "FF", "launch_speed": "101.2"}]})
    assert df.height == 1 and "pitch_type" in df.columns


def test_parse_gamefeed_empty_is_zero_rows():
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_gamefeed

    assert parse_mlb_statcast_gamefeed({}).height == 0
    assert parse_mlb_statcast_gamefeed("not a dict").height == 0


def test_parse_html_leaderboard_extracts_data_array():
    """The fielding-run-value / park-factors leaderboards embed `const data = [...]`."""
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_html_leaderboard

    html = '<html><body><script> const data = [{"name":"x","frv":3.2,"n":{"a":1}}]; </script></body></html>'
    df = parse_mlb_statcast_html_leaderboard(html)
    assert df.height == 1 and "frv" in df.columns and "n_a" in df.columns
    # word-boundary anchor: must not latch onto `methods_data = [...]`
    html2 = '<script>var methods_data = [{"bogus":1}]; const data = [{"name":"y","frv":1.0}];</script>'
    df2 = parse_mlb_statcast_html_leaderboard(html2)
    assert df2.height == 1 and df2["name"][0] == "y"


def test_parse_html_leaderboard_missing_is_zero_rows():
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_html_leaderboard

    assert parse_mlb_statcast_html_leaderboard("<html></html>").height == 0


def test_parse_schedule_one_row_per_game():
    """Real /schedule payload flattens schedule.dates[].games[] to one row per game."""
    import json

    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_schedule

    payload = json.loads((FIX / "schedule.json").read_text())
    df = parse_mlb_statcast_schedule(payload)
    assert df.height == 2 and "game_pk" in df.columns and "official_date" in df.columns


def test_parse_schedule_empty_is_zero_rows():
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_schedule

    assert parse_mlb_statcast_schedule({}).height == 0
    assert parse_mlb_statcast_schedule({"schedule": {"dates": []}}).height == 0
    assert parse_mlb_statcast_schedule("not a dict").height == 0


def test_parse_player_from_html():
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_player

    # Default section "statcast" -> seasonal aggregate; fixture trims to 2 rows.
    df = parse_mlb_statcast_player((FIX / "player_page.html").read_text())
    assert df.height == 2 and "xwoba" in df.columns and "exit_velocity_avg" in df.columns


def test_parse_player_missing_section_is_zero_rows():
    from sportsdataverse.mlb.mlb_statcast_parsers import parse_mlb_statcast_player

    html = (FIX / "player_page.html").read_text()
    assert parse_mlb_statcast_player(html, section="does_not_exist").height == 0
    assert parse_mlb_statcast_player("<html></html>").height == 0
