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
