"""One private body behind every former copy (task 6.1: B-L26, B-L27, B-L28, B-L46, B-L52, B-GB-11, B-M29, B-M30).

Each row names the shared body and every module that used to carry its own copy. Two checks:

* identity -- each former copy's name is now bound to the one shared function, so a fix reaches
  every caller;
* equality at the merge commit -- the shared body's canonical key (``tests/_body_key.py``: sha256 of the
  module's own field-by-field serialisation of the AST, neither ``ast.dump`` nor ``ast.unparse``, after
  stripping name, decorators, docstring, annotations and parameter names, so it is the same on Python
  3.10-3.14) equals the deleted copies'. Re-pinned under that key in PRE-1008 M-3. A deliberate change to a
  shared body updates its pin; the rows marked "current body" pin a body changed on purpose after the merge.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import textwrap
from pathlib import Path

import polars as pl
import pytest

from tests._body_key import body_key

FIXTURES = Path(__file__).parent / "fixtures"


_BOX = "sportsdataverse.scrape.ncaa._box_html"
_BOX_COPIES = (
    "sportsdataverse.mbb.mbb_ncaa_box_tabs",
    "sportsdataverse.cfb.cfb_ncaa_box",
    "sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box",
)
_ROSTER = "sportsdataverse.mbb.mbb_ncaa_roster_parser"
_FOX = "sportsdataverse._fox_layout"
_LG = "sportsdataverse"
_SEVEN = ("cfb", "mbb", "nba", "nfl", "nhl", "wbb", "wnba")
_ROSTERS = "sportsdataverse._espn_game_rosters"
_SCHEDULE = "sportsdataverse._espn_schedule"
_FOX_EXT = "sportsdataverse.cfb.cfb_fox_ext"

# (shared module, shared name, [(former-copy module, name there)], hash of the deleted copies)
SHARED = [
    (_BOX, "_cells", [(m, "_cells") for m in _BOX_COPIES], "51257b82e4565011"),
    (_BOX, "_rows", [(m, "_rows") for m in _BOX_COPIES], "a6d594b191798793"),
    (_BOX, "_cid", [(m, "_cid") for m in _BOX_COPIES], "de399ce1c6eba3a5"),
    (_BOX, "_int", [(m, "_int") for m in _BOX_COPIES], "247ec235f22cc09e"),
    (_BOX, "_finish", [(m, "_finish") for m in _BOX_COPIES], "7d63b13f69c09b5b"),
    (
        _ROSTER,
        "_java_string_hash",
        [("sportsdataverse.mbb.mbb_ncaa_boxscore_parser", "_java_string_hash")],
        "d597612ef4e5fe12",
    ),
    (
        _ROSTER,
        "_hashmap_improve",
        [("sportsdataverse.mbb.mbb_ncaa_boxscore_parser", "_hashmap_improve")],
        "72257cbb55fb2cf4",
    ),
    (
        _ROSTER,
        "_hashmap_radix_key",
        [("sportsdataverse.mbb.mbb_ncaa_boxscore_parser", "_hashmap_radix_key")],
        "e9492859c6e1842d",
    ),
    (
        "sportsdataverse.scrape.ncaa.capture",
        "_parse_shard",
        [("sportsdataverse.scrape.ncaa.parse", "_parse_shard")],
        "659422bc930c3ee6",
    ),
    (_FOX, "_clean", [(_FOX_EXT, "_clean")], "ed647fea3916cb44"),
    (_FOX, "_table_rows", [(_FOX_EXT, "_table_rows")], "d17227cc5e564746"),
    (_FOX, "_cells", [(_FOX_EXT, "_cells")], "07a86db343005ad4"),
    (_FOX, "_uri_id", [(_FOX_EXT, "_uri_id")], "4afe6342d438de45"),
    (_FOX, "_full_team_name", [(_FOX_EXT, "_fox_team_fullname")], "4c321547055feb1d"),
]


@pytest.mark.parametrize(("owner", "name", "copies", "pin"), SHARED, ids=[f"{r[0]}.{r[1]}" for r in SHARED])
def test_every_former_copy_is_the_shared_body(owner: str, name: str, copies: list, pin: str) -> None:
    shared = getattr(importlib.import_module(owner), name)
    for module, copy_name in copies:
        assert getattr(importlib.import_module(module), copy_name) is shared, f"{module}.{copy_name} is a copy again"
    assert body_key(shared) == pin, f"{owner}.{name} no longer equals the copies it replaced; update the pin"


def test_cfb_fox_ext_gets_through_the_layout_getter() -> None:
    """``_fox_get``'s deleted copy differed from ``fox_get`` only in the names of the same two constants."""
    ext = importlib.import_module(_FOX_EXT)
    fox = importlib.import_module(_FOX)
    assert ext._fox_get is fox.fox_get
    assert ext.FOX_BASE == fox.API == "https://api.foxsports.com/bifrost/v1"
    assert ext.FOX_DATA_KEY is fox.DATA_KEY  # public module attribute kept (R61b-2)


def test_wbb_shot_locations_runs_the_mbb_driver_with_the_wbb_period_model(monkeypatch) -> None:
    from sportsdataverse.mbb import mbb_ncaa_shots
    from sportsdataverse.wbb import wbb_ncaa_shots
    from sportsdataverse.wbb.wbb_ncaa_shots import ncaa_wbb_shot_locations

    assert wbb_ncaa_shots._shot_locations is mbb_ncaa_shots._shot_locations
    calls: list = []
    monkeypatch.setattr(wbb_ncaa_shots, "_shot_locations", lambda *a, **k: calls.append(a) or pl.DataFrame())
    ncaa_wbb_shot_locations(["5728709"], season=2015, fetcher=object())
    ncaa_wbb_shot_locations(["5728709"], fetcher=object())
    assert [c[1] for c in calls] == [(2, 1200, 300), (4, 600, 300)]


@pytest.fixture
def crosswalk_reads(monkeypatch) -> list:
    """Serve the committed real ``cfb_schedule_crosswalk`` rows and count the reads."""
    import sportsdataverse.cfb as cfb
    from sportsdataverse.football.sources import idmap

    frame = pl.read_parquet(FIXTURES / "cfb_crosswalk" / "cfb_schedule_crosswalk_2025_head25.parquet")
    reads: list = []

    def _load(seasons):
        reads.append(tuple(seasons))
        return frame

    monkeypatch.setattr(idmap, "_CFB_CROSSWALK_CACHE", {})
    monkeypatch.setattr(cfb, "load_cfb_schedule_crosswalk", _load)
    return reads


def test_fox_and_yahoo_share_one_crosswalk_read(crosswalk_reads: list) -> None:
    from sportsdataverse.cfb.fox_pbp.to_espn_summary import _crosswalk_fox_id
    from sportsdataverse.cfb.yahoo_pbp.fetch import _crosswalk_yahoo_id

    assert _crosswalk_fox_id(401756846, (2025,)) == "42830"
    assert _crosswalk_yahoo_id("401756846", (2025,)) == "ncaaf.g.202508230021"
    assert _crosswalk_yahoo_id(401760371, (2025,)) == "ncaaf.g.202508230040"
    assert _crosswalk_fox_id(1, (2025,)) is None
    assert crosswalk_reads == [(2025,)]


def test_a_failed_crosswalk_read_is_a_miss_and_is_not_cached(monkeypatch) -> None:
    import sportsdataverse.cfb as cfb
    from sportsdataverse.cfb.fox_pbp.to_espn_summary import _crosswalk_fox_id
    from sportsdataverse.football.sources import idmap

    def _boom(seasons):
        raise OSError("release asset unreachable")

    monkeypatch.setattr(idmap, "_CFB_CROSSWALK_CACHE", {})
    monkeypatch.setattr(cfb, "load_cfb_schedule_crosswalk", _boom)
    assert _crosswalk_fox_id(401756846, (2025,)) is None
    assert idmap._CFB_CROSSWALK_CACHE == {}


def test_scoreboard_event_parsing_is_one_body_with_the_league_home_away_step(monkeypatch) -> None:
    """B-M29: the seven ``scoreboard_event_parsing`` bodies were AST-identical; each now calls the one
    ``_scoreboard_event`` with its own league's ``__extract_home_away`` (the step's defaults differ by sport, so it
    is a parameter). The pin is the current body, deliberately changed in #1001: the deleted copies called the
    module global ``__extract_home_away`` (key b7c64f4afbde572e), the shared body takes it as a parameter."""
    sched = importlib.import_module(_SCHEDULE)
    assert body_key(sched._scoreboard_event) == "ce665ba26eff3b3d"
    for lg in _SEVEN:
        mod = importlib.import_module(f"{_LG}.{lg}.{lg}_schedule")
        calls: list = []
        monkeypatch.setattr(mod, "_scoreboard_event", lambda event, step, c=calls: c.append((event, step)) or event)
        event = {"id": "1"}
        assert mod.scoreboard_event_parsing(event) is event
        assert calls == [(event, getattr(mod, "__extract_home_away"))], lg


# Task 6.1 (a), B-M29 / B-M30: the shared bodies gained type annotations the deleted copies lacked; the canonical
# key ignores annotations. nhl keeps its own roster_items (it does not drop period / for_player_id / active).
SHARED_BODY_ONLY = [
    (
        _ROSTERS,
        "game_items",
        [(f"{_LG}.{lg}.{lg}_game_rosters", f"helper_{lg}_game_items") for lg in _SEVEN],
        "945fc471dc919246",
    ),
    (
        _ROSTERS,
        "team_items",
        [(f"{_LG}.{lg}.{lg}_game_rosters", f"helper_{lg}_team_items") for lg in _SEVEN],
        # current body, not the deleted copies': those were one-line wrappers around it (shared since #878, key
        # 8f78866dd5a3dcbd); #1001 bound the names to it directly
        "dbec5a384652bebd",
    ),
    (
        _ROSTERS,
        "athlete_items",
        [(f"{_LG}.{lg}.{lg}_game_rosters", f"helper_{lg}_athlete_items") for lg in _SEVEN],
        # current body, deliberately changed in #1002 and #1006 (the duplicate snake-case key guard, shallowest key
        # wins: MLB debutYear vs debut.year); the deleted one-line wrappers keyed e01649b3176f5591
        "1ff15e20a9a0bed8",
    ),
    (
        _ROSTERS,
        "roster_items",
        [(f"{_LG}.{lg}.{lg}_game_rosters", f"helper_{lg}_roster_items") for lg in _SEVEN if lg != "nhl"],
        "db9e425267f9d4d5",
    ),
    (
        _SCHEDULE,
        "_extract_home_away_nba",
        [(f"{_LG}.{lg}.{lg}_schedule", "__extract_home_away") for lg in ("nba", "wnba")],
        "1c6c7383a3e24838",
    ),
    (
        _SCHEDULE,
        "_extract_home_away_college",
        [(f"{_LG}.{lg}.{lg}_schedule", "__extract_home_away") for lg in ("cfb", "wbb")],
        "a6802be4d276c070",
    ),
]


@pytest.mark.parametrize(
    ("owner", "name", "copies", "pin"), SHARED_BODY_ONLY, ids=[f"{r[0]}.{r[1]}" for r in SHARED_BODY_ONLY]
)
def test_every_former_espn_copy_is_the_shared_body(owner: str, name: str, copies: list, pin: str) -> None:
    shared = getattr(importlib.import_module(owner), name)
    for module, copy_name in copies:
        assert getattr(importlib.import_module(module), copy_name) is shared, f"{module}.{copy_name} is a copy again"
    assert body_key(shared) == pin, f"{owner}.{name} no longer equals the copies it replaced; update the pin"


# R61a-2: the deliberate outliers stay their own bodies. nhl's roster_items keeps period / for_player_id / active;
# mbb (two halves), nfl (no poll: null rank) and nhl (three periods, YTD record, no rank) keep their own home/away
# step. Binding any of them to a shared body would change its frame, so each asserts identity and its defaults.


def test_nhl_keeps_its_own_roster_items(monkeypatch) -> None:
    from sportsdataverse.nhl import nhl_game_rosters

    rosters = importlib.import_module(_ROSTERS)
    fn = nhl_game_rosters.helper_nhl_roster_items
    assert fn is not rosters.roster_items, "nhl's roster_items is bound to the shared body"
    entry = {"playerId": 7, "period": 0, "active": True, "forPlayerId": 0, "athlete": {"$ref": "http://core/a/7"}}

    class _Resp:
        def json(self) -> dict:
            return {"entries": [entry]}

    monkeypatch.setattr(nhl_game_rosters, "download", lambda url, **kwargs: _Resp())
    out = fn(pl.DataFrame({"team_id": [1]}), "http://core/competitors")
    assert {"period", "for_player_id", "active"} <= set(out.columns)


def _bare_event() -> dict:
    """A scoreboard event whose competitors ship no rank, linescores or records, so every default shows."""
    return {
        "competitions": [
            {
                "competitors": [
                    {"team": {"id": str(i)}, "score": "1", "homeAway": s} for i, s in ((1, "home"), (2, "away"))
                ]
            }
        ]
    }


@pytest.mark.parametrize(
    ("lg", "rank", "periods", "record_types"),
    [
        ("mbb", 99, 2, ["total", "home", "road", "vsconf"]),
        ("nfl", None, 4, ["total", "home", "road"]),
        ("nhl", "absent", 3, ["ytd", "home", "road"]),
    ],
)
def test_outlier_home_away_steps_keep_their_league_defaults(
    lg: str, rank: object, periods: int, record_types: list
) -> None:
    sched = importlib.import_module(_SCHEDULE)
    step = getattr(importlib.import_module(f"{_LG}.{lg}.{lg}_schedule"), "__extract_home_away")
    assert step not in (sched._extract_home_away_nba, sched._extract_home_away_college), f"{lg} uses a shared step"
    home = step(_bare_event(), 0, "home")["competitions"][0]["home"]
    assert len(home["linescores"]) == periods
    assert [r["type"] for r in home["records"]] == record_types
    if rank == "absent":
        assert "currentRank" not in home
    else:
        assert home["currentRank"] == rank


# --- 6.1d: parser helpers, the browser User-Agent, the mls/nwsl getter (A-N15, A-L59, A-L8) ---------------


_PARSERS = "sportsdataverse._parser_common"
_MLB_API = "sportsdataverse.mlb.mlb_api_parsers"
_NHL_WEB = "sportsdataverse.nhl.nhl_api_web_parsers"
_MLB_NHL_PARSERS = (
    _MLB_API,
    _NHL_WEB,
    "sportsdataverse.nhl.nhl_edge_parsers",
    "sportsdataverse.nhl.nhl_records_parsers",
    "sportsdataverse.nhl.nhl_stats_rest_parsers",
)
_ESPN_PARSERS = "sportsdataverse._common_espn_parsers"

# (shared name, former-copy modules, canonical key of the deleted copies at the merge commit)
PARSER_SHARED = [
    ("_snake_columns", _MLB_NHL_PARSERS, "fa95bf879b4c3477"),
    ("_empty_frame", (*_MLB_NHL_PARSERS, _ESPN_PARSERS), "21a85e7199b77600"),
    ("_as_dict", (_MLB_API, _NHL_WEB), "0ed57da7de510562"),
]


@pytest.mark.parametrize(("name", "copies", "pin"), PARSER_SHARED, ids=[f"{_PARSERS}.{r[0]}" for r in PARSER_SHARED])
def test_parser_helpers_are_one_body(name: str, copies: tuple, pin: str) -> None:
    shared = getattr(importlib.import_module(_PARSERS), name)
    for module in copies:
        assert getattr(importlib.import_module(module), name) is shared, f"{module}.{name} is a copy again"
    assert body_key(shared) == pin, f"{_PARSERS}.{name} no longer equals the copies it replaced"


def test_from_pandas_errors_is_one_tuple() -> None:
    shared = importlib.import_module(_PARSERS)._FROM_PANDAS_ERRORS
    assert shared == (TypeError, ValueError, NotImplementedError, pl.exceptions.PolarsError)
    for module in (*_MLB_NHL_PARSERS, _ESPN_PARSERS):
        assert importlib.import_module(module)._FROM_PANDAS_ERRORS is shared, module


# The 12 copies of this string that the runtimes carried before 6.1d (A-L59).
_OLD_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def test_one_browser_user_agent() -> None:
    from sportsdataverse import _codegen_runtime, _subscription_http
    from sportsdataverse.cfb import on3_runtime, sports247_runtime, sports247_site_pages_runtime
    from sportsdataverse.nfl import nfl_games, nfl_ngs, nflpro_runtime, pff_runtime
    from sportsdataverse.soccer import uefa_runtime
    from sportsdataverse.providers.yahoo import yahoo_shangrila_runtime

    ua = _codegen_runtime._CHROME_UA
    assert ua == _OLD_UA
    for sent in (
        _subscription_http.USER_AGENT,
        on3_runtime._UA,
        nflpro_runtime._UA,
        nfl_ngs._UA,
        nfl_games._DEFAULT_UA,
        sports247_runtime.rdb_headers()["User-Agent"],
        sports247_site_pages_runtime.site_headers()["User-Agent"],
        pff_runtime._pff_headers()["User-Agent"],
        uefa_runtime.UEFA_HEADERS["User-Agent"],
        yahoo_shangrila_runtime._HEADERS["User-Agent"],
    ):
        assert sent is ua
    package = Path(_codegen_runtime.__file__).parent
    copies = [p for p in package.rglob("*.py") if any(_OLD_UA in s for s in _string_literals(p))]
    assert copies == [Path(_codegen_runtime.__file__)], "a runtime spells out the User-Agent again"


def _string_literals(path: Path) -> list:
    """Every string a module spells out, as Python reads it (R61d-3).

    ``ast`` folds implicitly concatenated literals (``"...Chrome/" "124.0 ..."``) into one constant, and a ``+`` chain
    of literals is joined here, so a User-Agent split across literals is still found; a plain text search is not.
    """

    def joined(node: ast.AST) -> "str | None":
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            left, right = joined(node.left), joined(node.right)
            return None if left is None or right is None else left + right
        return None

    nodes = ast.walk(ast.parse(path.read_text(encoding="utf-8")))
    return [s for s in (joined(n) for n in nodes if isinstance(n, (ast.Constant, ast.BinOp))) if s is not None]


def test_the_user_agent_guard_sees_a_split_literal(tmp_path) -> None:
    """R61d-3: a re-copied User-Agent split across literals (implicit or ``+``) is still a copy."""
    head, tail = _OLD_UA[:60], _OLD_UA[60:]
    split = tmp_path / "runtime.py"
    split.write_text(f'UA = "{head}" "{tail}"\n', encoding="utf-8")
    added = tmp_path / "other.py"
    added.write_text(f'UA = "{head}" + "{tail}"\n', encoding="utf-8")
    assert _OLD_UA not in split.read_text(encoding="utf-8")  # a text search misses both
    assert any(_OLD_UA in s for s in _string_literals(split))
    assert any(_OLD_UA in s for s in _string_literals(added))


@pytest.mark.parametrize(
    ("runtime", "referer"),
    [
        ("sportsdataverse.soccer.mls.mls_api_runtime", "https://www.mlssoccer.com/"),
        ("sportsdataverse.soccer.nwsl.nwsl_api_runtime", "https://www.nwslsoccer.com/"),
    ],
)
def test_mls_nwsl_getters_share_one_body_and_keep_their_download_seam(runtime: str, referer: str, monkeypatch) -> None:
    from sportsdataverse._codegen_runtime import _CHROME_UA

    module = importlib.import_module(runtime)
    calls: list = []

    class _Resp:
        status_code = 200
        text = '{"ok": 1}'

        def json(self) -> dict:
            return {"ok": 1}

    monkeypatch.setattr(module, "download", lambda **kw: calls.append(kw) or _Resp())
    assert module._get("https://h/a", params={"a": None, "b": 1}) == {"ok": 1}
    assert module._get("https://h/b", headers={"Referer": "z", "X": "1"}, num_retries=0) == {"ok": 1}
    assert calls == [
        {"url": "https://h/a", "params": {"b": 1}, "headers": {"Referer": referer, "User-Agent": _CHROME_UA}},
        {
            "url": "https://h/b",
            "params": {},
            "headers": {"Referer": "z", "User-Agent": _CHROME_UA, "X": "1"},
            "num_retries": 0,
        },
    ]
    body = ast.parse(textwrap.dedent(inspect.getsource(module._get))).body[0].body[-1]
    assert ast.unparse(body) == "return _referer_get(download, REFERER, url, params, kwargs)"
