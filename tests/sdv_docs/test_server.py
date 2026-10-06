import sqlite3
import subprocess
import sys

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
    assert "returned columns omitted; pass columns=True or use find_columns" in out


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


def test_filters_are_case_insensitive():
    assert "nhl_shifts" in server.list_datasets(league="NHL")
    assert "**load_nba_pbp**" in server.find_columns("game_id", league="NBA")
    assert "**espn_nba_team_roster**" in server.search("team roster", kind="Function", league="NBA", lang="Python")
    assert "/injuries" in server.find_endpoints("athlete injuries", api="ESPN_CORE_V2")
    assert "source: openapi" in server.find_endpoints("odds", api="espn core api")


def test_a_filtered_miss_says_where_the_match_is():
    out = server.find_columns("game_id", league="mlb")
    assert out == "`game_id` exists, but not for league 'mlb' (found in: nba, nhl)."
    out = server.find_columns("event_type", function="load_nba_pbp")
    assert out == "`event_type` exists, but not for function 'load_nba_pbp' (found in: load_nhl_pbp)."
    out = server.get_function("load_nhl_pbp", lang="r")
    assert out == "`load_nhl_pbp` exists, but not for lang 'r' (found in: `load_nhl_pbp` (python))."
    out = server.get_function("wehoop::load_nba_pbp")
    assert out.startswith("`load_nba_pbp` exists, but not for package 'wehoop' (found in: `load_nba_pbp` (python), ")
    assert "`hoopR::load_nba_pbp` (r)" in out
    out = server.list_datasets(league="nba", query="shifts")
    assert out == "Datasets matching 'shifts' exist, but not for league 'nba' (found in: nhl)."
    assert server.list_datasets(league="mlb") == "Datasets exist, but not for league 'mlb' (found in: nba, nhl)."
    out = server.search("shifts", league="nba")
    assert out == "Results for 'shifts' exist, but not for league 'nba' (found in: nhl)."
    out = server.find_endpoints("athlete injuries", api="espn_cdn")
    assert out == "Endpoints matching 'athlete injuries' exist, but not for api 'espn_cdn' (found in: espn_core_v2)."
    assert "not in index" in server.find_columns("no_such_column_xyz", league="nba")  # a true miss stays a miss


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


needs_mcp = pytest.mark.skipif(sys.version_info < (3, 10), reason="mcp needs Python >= 3.10")


def test_importing_sdv_docs_never_imports_sportsdataverse():
    code = "import sys, sdv_docs.server, sdv_docs.index; print('sportsdataverse' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True).stdout.strip()
    assert out == "False"


def test_main_exits_2_on_python_39(monkeypatch, capsys):
    monkeypatch.setattr(sys, "version_info", (3, 9, 18))
    assert server.main() == 2
    assert "pip install 'sportsdataverse[mcp]'" in capsys.readouterr().err


def test_main_exits_2_without_mcp(monkeypatch, capsys):
    def no_mcp():
        raise ImportError("No module named 'mcp'")

    monkeypatch.setattr(server, "build_server", no_mcp)
    assert server.main() == 2
    assert "sdv-docs needs Python >= 3.10" in capsys.readouterr().err


@needs_mcp
def test_in_memory_client_lists_six_tools_and_calls_one():
    import anyio
    from mcp import Client

    async def go():
        async with Client(server.build_server()) as client:
            tools = (await client.list_tools()).tools
            result = await client.call_tool("find_columns", {"column": "event_type"})
        return sorted(t.name for t in tools), result.content[0].text

    names, text = anyio.run(go)
    assert names == sorted(f.__name__ for f in server.TOOLS)
    assert "**load_nhl_pbp**" in text


@needs_mcp
def test_stdio_smoke(tiny_db):
    import anyio
    from mcp import Client, StdioServerParameters

    # The child gets an allow-listed environment, so SDV_DOCS_DB must be passed explicitly.
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "sdv_docs.server"], env={"SDV_DOCS_DB": str(tiny_db)}
    )

    async def go():
        async with Client(params) as client:
            tools = (await client.list_tools()).tools
            result = await client.call_tool("get_function", {"name": "load_nhl_pbp"})
        return len(tools), result.content[0].text

    n, text = anyio.run(go)
    assert n == 6
    assert "| `event_type` | String |" in text
