"""extract.py runtime-capture extractor (build-time tool; needs the live factory)."""

from tools.codegen import extract


def test_describe_core_fn_recovers_url_params_parser():
    info = extract.describe_core_fn("scoreboard")
    assert info["short"] == "scoreboard"
    assert info["path"].endswith("/scoreboard")
    assert "dates" in info["query_params"]
    assert info["query_params"]["season_type"]["query_key"] == "seasontype"
    assert info["parser"] == "parse_scoreboard"


def test_describe_distinguishes_hosts_for_same_short_base():
    assert extract.describe_core_fn("teams_site")["host"] == "site_v2"
    assert extract.describe_core_fn("teams_core")["host"] == "core_v2"


def test_describe_detects_optional_segment():
    pi = extract.describe_core_fn("season_powerindex")
    tid = next(p for p in pi["path_params"] if p["name"] == "team_id")
    assert tid.get("optional_segment") is True
    assert pi["path"].endswith("/powerindex[/{team_id}]")


def test_describe_detects_mid_path_optional_segment():
    qbr = extract.describe_core_fn("season_qbr")
    assert "[/groups/{group_id}]/qbr/{split}" in qbr["path"]


def test_describe_detects_default_from():
    ec = extract.describe_core_fn("event_competition")
    cid = next(p for p in ec["path_params"] if p["name"] == "cid")
    assert cid.get("default_from") == "event_id"


def test_describe_detects_bool_transform():
    ai = extract.describe_core_fn("athletes_index")
    assert ai["query_params"]["active"].get("transform") == "bool_str"


def test_rename_map_cleans_teams_site_keeps_core():
    rm = extract.build_rename_map()
    assert rm.get("espn_nba_teams_site") == "espn_nba_teams"
    assert "espn_nba_teams_core" not in rm  # _core qualifier retained, no rename
