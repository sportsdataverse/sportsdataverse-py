import json
from pathlib import Path
import polars as pl
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

FIX = Path(__file__).parent / "fixtures"


def _load(name):
    return json.loads((FIX / name).read_text(encoding="utf-8"))


def test_parse_single_result_set_to_polars():
    raw = _load("cap_leaguedashplayerstats_nba.json")
    df = parse_nba_stats_result_sets(raw, result_set="LeagueDashPlayerStats")
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    assert "player_id" in df.columns and "player_name" in df.columns


def test_parse_all_result_sets_returns_dict():
    raw = _load("cap_playercareerstats_nba.json")
    out = parse_nba_stats_result_sets(raw)
    assert isinstance(out, dict)
    assert "SeasonTotalsRegularSeason" in out
    assert isinstance(out["SeasonTotalsRegularSeason"], pl.DataFrame)


def test_parse_empty_payload_zero_row_frame():
    df = parse_nba_stats_result_sets({}, result_set="X")
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_return_as_pandas():
    import pandas as pd

    raw = _load("cap_leaguedashplayerstats_wnba.json")
    df = parse_nba_stats_result_sets(raw, result_set="LeagueDashPlayerStats", return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)


def test_parse_empty_payload_no_result_set_zero_row_frame():
    df = parse_nba_stats_result_sets({})  # no result_set, zero sets
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_parse_shotlocations_nested_headers():
    # leaguedash*shotlocations ship resultSets as a dict with 2-level (grouped) headers;
    # the parser must flatten them into composite columns with real rows.
    raw = _load("cap_shotlocations_nba.json")
    df = parse_nba_stats_result_sets(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert "player_id" in df.columns
    assert any(c.startswith("less_than_5_ft_fgm") for c in df.columns)  # grouped + snake-cased


def test_parse_scoreboardv3_game_feed():
    # scoreboardv3 has no resultSets envelope — one row per game from scoreboard.games.
    raw = _load("cap_scoreboardv3_nba.json")
    df = parse_nba_stats_result_sets(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 2
    assert any(c.startswith("hometeam") for c in df.columns)
    assert any(c.startswith("awayteam") for c in df.columns)


def test_parse_single_set_without_name_returns_frame():
    raw = _load("cap_leaguedashplayerstats_nba.json")  # exactly one result-set
    df = parse_nba_stats_result_sets(raw)  # no result_set -> single-frame short-circuit
    assert isinstance(df, pl.DataFrame) and df.height > 0


def test_parse_multi_set_return_as_pandas():
    import pandas as pd

    raw = _load("cap_playercareerstats_nba.json")  # 14 result-sets
    out = parse_nba_stats_result_sets(raw, return_as_pandas=True)
    assert isinstance(out, dict)
    assert all(isinstance(v, pd.DataFrame) for v in out.values())


def test_parse_ragged_rows_returns_zero_row_frame():
    raw = {"resultSets": [{"name": "X", "headers": ["A", "B"], "rowSet": [[1]]}]}  # row width != headers
    df = parse_nba_stats_result_sets(raw, result_set="X")
    assert isinstance(df, pl.DataFrame) and df.height == 0


def test_parse_video_envelope_videodetailsasset():
    # video endpoints ship resultSets as {Meta: {videoUrls: [...]}, playlist: [...]}
    # (a dict, not the tabular list) — the 2026-08 capture sweep misread that as dead.
    raw = _load("cap_videodetailsasset_nba.json")
    out = parse_nba_stats_result_sets(raw)
    assert isinstance(out, dict)
    assert set(out) == {"videoUrls", "playlist"}
    assert isinstance(out["videoUrls"], pl.DataFrame) and out["videoUrls"].height == 3
    assert isinstance(out["playlist"], pl.DataFrame) and out["playlist"].height == 3
    assert {"uuid", "surl"} <= set(out["videoUrls"].columns)
    assert {"gi", "ei", "dsc"} <= set(out["playlist"].columns)


def test_parse_video_envelope_named_set():
    raw = _load("cap_videoevents_nba.json")
    df = parse_nba_stats_result_sets(raw, result_set="playlist")
    assert isinstance(df, pl.DataFrame) and df.height == 1
    assert "gi" in df.columns


def test_parse_video_envelope_return_as_pandas():
    import pandas as pd

    raw = _load("cap_videoeventsasset_nba.json")
    out = parse_nba_stats_result_sets(raw, return_as_pandas=True)
    assert isinstance(out, dict)
    assert all(isinstance(v, pd.DataFrame) for v in out.values())
    assert out["videoUrls"].shape[0] == 1


def test_parse_video_envelope_empty_lists_zero_row_frames():
    raw = {"resultSets": {"Meta": {"videoUrls": []}, "playlist": []}}
    out = parse_nba_stats_result_sets(raw)
    assert isinstance(out, dict)
    assert out["videoUrls"].height == 0 and out["playlist"].height == 0


def test_parse_null_prefix_column_keeps_all_rows():
    # A column that is null for the first 100+ rows and numeric later must not
    # collapse the whole set to an empty frame (polars' default
    # infer_schema_length=100 inferred Null, then errored on the late number
    # and the never-raise contract swallowed it — observed on the WNBA 1998
    # leaguegamelog player capture, PLUS_MINUS null until late in the season).
    rows = [[i, None] for i in range(150)] + [[150, 3.5]]
    raw = {"resultSets": [{"name": "X", "headers": ["A", "PLUS_MINUS"], "rowSet": rows}]}
    df = parse_nba_stats_result_sets(raw, result_set="X")
    assert df.height == 151
    assert df["plus_minus"][150] == 3.5
