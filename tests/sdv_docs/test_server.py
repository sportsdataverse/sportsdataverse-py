import sqlite3

import pytest

from sdv_docs import server
from tests.sdv_docs.conftest import make_tiny_db


@pytest.fixture(autouse=True)
def use_tiny(tiny_db, monkeypatch):
    monkeypatch.setenv("SDV_DOCS_DB", str(tiny_db))


def test_search_lists_hits_with_urls():
    out = server.search("nhl pbp")
    assert "**load_nhl_pbp**" in out
    assert "https://py.sportsdataverse.org/docs/nhl/reference/loaders/pbp#load_nhl_pbp" in out


@pytest.mark.parametrize("text", ['"', "-", "*", "NEAR(", "", "!!", "();--"])
def test_search_survives_hostile_input(text):
    # Words in hostile input are searched literally (Task 2 covers "'; DROP TABLE functions; --"),
    # so only word-free or unmatched input is expected to find nothing here.
    assert server.search(text).startswith("No results")
    assert server.find_endpoints(text).startswith("No endpoints")
    assert server.list_datasets(query=text).startswith("No datasets")


def test_get_function_full_block():
    out = server.get_function("load_nhl_pbp")
    for s in (
        "## load_nhl_pbp",
        "load_nhl_pbp(seasons",
        "`seasons`",
        ">= 2010",
        "| `event_type` | String |",
        "release `nhl_pbp_full`, seasons from 2010",
        "Docs: https://py.sportsdataverse.org/docs/nhl/reference/loaders/pbp#load_nhl_pbp",
    ):
        assert s in out


def test_get_function_without_columns():
    out = server.get_function("load_nhl_pbp", columns=False)
    assert "event_type" not in out and "**Returns**" not in out


def test_get_function_shows_python_then_r_with_equivalents():
    out = server.get_function("load_nba_pbp")
    assert out.index("## load_nba_pbp") < out.index("## hoopR::load_nba_pbp")
    assert "**R equivalent**: `hoopR::load_nba_pbp`" in out
    assert "**Python equivalent**: `load_nba_pbp`" in out


def test_get_function_accepts_r_names_and_variants():
    out = server.get_function("hoopr::load_nba_pbp")
    assert "## hoopR::load_nba_pbp" in out and "## load_nba_pbp\n" not in out
    assert server.get_function(" load_nhl_pbp() ").startswith("## load_nhl_pbp")


def test_get_function_miss_suggests_close_names():
    out = server.get_function("load_nhl_pbpp")
    assert "not in index" in out and "`load_nhl_pbp`" in out


def test_find_columns_and_league_filter():
    out = server.find_columns("game_id")
    assert "**load_nba_pbp**" in out and "**load_nhl_pbp**" in out
    assert "load_nhl_pbp" not in server.find_columns("game_id", league="nba")
    assert "not in index" in server.find_columns("no_such_column_xyz")


def test_find_columns_caps_at_limit(tmp_path, monkeypatch):
    db = make_tiny_db(tmp_path / "big.sqlite")
    con = sqlite3.connect(db)
    con.executemany(
        "INSERT INTO columns VALUES (?,?,?,?,?)",
        [(f"fn_{i:02d}", None, "game_id", "Int64", "Game id.") for i in range(60)],
    )
    con.commit()
    con.close()
    monkeypatch.setenv("SDV_DOCS_DB", str(db))
    out = server.find_columns("game_id")
    assert "(first 50; narrow with league= or function=)" in out
    assert out.count("\n- ") == 50


def test_find_endpoints():
    out = server.find_endpoints("athlete injuries", api="espn_core_v2")
    assert "/athletes/{athlete_id}/injuries" in out and "`athlete_id`*" in out
    assert "`espn_nba_athlete_injuries`" in out
    assert "source: openapi" in server.find_endpoints("odds")


def test_find_endpoints_truncates_long_wrapper_lists(tmp_path, monkeypatch):
    db = make_tiny_db(tmp_path / "wide.sqlite")
    con = sqlite3.connect(db)
    names = " ".join(f"espn_l{i}_scoreboard" for i in range(20))
    con.execute(
        "INSERT INTO endpoints VALUES ('espn_site_v2','GET','https://x/scoreboard','Scoreboard','[]',?, '', 'codegen', NULL)",
        (names,),
    )
    con.execute("DELETE FROM search")
    from sdv_docs.schema import SEARCH_SQL

    con.executescript(SEARCH_SQL)
    con.commit()
    con.close()
    monkeypatch.setenv("SDV_DOCS_DB", str(db))
    assert "(+14 more)" in server.find_endpoints("scoreboard")


def test_list_datasets():
    out = server.list_datasets(league="nhl")
    assert "nhl_pbp_full" in out and "nhl_shifts" in out and "load_nba_pbp" not in out
    assert "| load_nhl_shifts | nhl | nhl_shifts | 2025 |" in server.list_datasets(query="shifts")


def test_index_info():
    out = server.index_info()
    assert "from sdv-py abc1234" in out and "- sdv_swagger_sha: deadbee" in out and "functions 6" in out


def test_every_tool_reports_a_missing_index(tmp_path, monkeypatch):
    monkeypatch.setenv("SDV_DOCS_DB", str(tmp_path / "missing.sqlite"))
    calls = (
        lambda: server.search("x"),
        lambda: server.get_function("x"),
        lambda: server.find_columns("x"),
        lambda: server.find_endpoints("x"),
        lambda: server.list_datasets(),
        server.index_info,
    )
    for call in calls:
        assert call().startswith("index unavailable:")


def test_tools_tuple_is_complete():
    assert [f.__name__ for f in server.TOOLS] == [
        "search",
        "get_function",
        "find_columns",
        "find_endpoints",
        "list_datasets",
        "index_info",
    ]
