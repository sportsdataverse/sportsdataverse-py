"""``_codegen_runtime._NoContent`` never escapes a public function (task 4.1b PR 4 review, R4.1b-d-1..3, R4.1b-d-7).

A 204 / 205 is ``{}`` from a DATA getter (``@_no_content_is_empty``) and an ``AssetFetchError`` from a lookup or
bootstrap whose 204 leaves nothing to read (``@_no_content_is_unknown`` / ``with _no_content_unknown(...)``): a
token, session or key page, a season resolver, a snapshot pointer, a finished-game check. The structural test
walks every module's call graph; the behavioural tests serve a 204 to each boundary through the transports.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import pytest

from sportsdataverse._codegen_runtime import _NoContent
from sportsdataverse.errors import AssetFetchError
from tests.codegen.test_runtime_errors import _answer_every_transport

_PKG = Path(__file__).resolve().parents[2] / "sportsdataverse"
# the definitions themselves: they raise by design and every caller is checked
_DEFINERS = {"_codegen_runtime.py", "basketball_runtime.py", "euro_hockey_runtime.py"}
_BOUNDARIES = ("_no_content_is_empty", "_no_content_is_unknown")


_RAISER_CALLS = ("_check_no_content", "_no_content_transport", "_fetch_json")


def _raises_directly(node: ast.AST) -> bool:
    """A call to a ``_NoContent`` raiser, ``get_text(..., no_content=True)``, or a ``raise ..._NoContent(...)``."""
    if isinstance(node, ast.Raise):
        return node.exc is not None and "_NoContent" in ast.unparse(node.exc)
    if not isinstance(node, ast.Call):
        return False
    fn = node.func
    name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
    if name in _RAISER_CALLS:
        return True
    return name == "get_text" and any(
        k.arg == "no_content" and isinstance(k.value, ast.Constant) and k.value.value is True for k in node.keywords
    )


def _nodes(fn: ast.AST) -> List[ast.AST]:
    """The nodes of ``fn``'s own body (not its nested defs), minus a ``with ..._no_content_unknown(...)`` block."""
    out: List[ast.AST] = []

    def walk(node: ast.AST) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return
        if isinstance(node, ast.With) and any("_no_content_unknown" in ast.unparse(i.context_expr) for i in node.items):
            return
        out.append(node)
        for child in ast.iter_child_nodes(node):
            walk(child)

    for child in ast.iter_child_nodes(fn):
        walk(child)
    return out


class _Module:
    """One module's functions, imports and module-level dispatch tables, as the walk needs them."""

    def __init__(self, path: Path, root: Path) -> None:
        self.name = ".".join(path.relative_to(root.parent).with_suffix("").parts)
        self.file = path.name
        tree = ast.parse(path.read_text(encoding="utf-8"))
        defs = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
        self.funcs = {n.name: n for n in defs}
        self.top = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        self.guarded = {
            n for n, f in self.funcs.items() if any(b in ast.unparse(d) for d in f.decorator_list for b in _BOUNDARIES)
        }
        self.mods: Dict[str, str] = {}  # alias -> module
        self.fns: Dict[str, Tuple[str, str]] = {}  # alias -> (module, function)
        for node in tree.body:
            if isinstance(node, ast.Import):
                for al in node.names:
                    self.mods[al.asname or al.name] = al.name
            elif isinstance(node, ast.ImportFrom) and node.module:
                for al in node.names:
                    self.mods[al.asname or al.name] = f"{node.module}.{al.name}"
                    self.fns[al.asname or al.name] = (node.module, al.name)
        # module-level containers (dispatch tables): the functions (or factories) they hold
        self.tables: Dict[str, Set[str]] = {}
        for node in tree.body:
            if isinstance(node, ast.Assign):
                targets = node.targets
            elif isinstance(node, ast.AnnAssign):
                targets = [node.target]
            else:
                continue
            if node.value is None or not isinstance(node.value, (ast.Dict, ast.List, ast.Tuple, ast.Set, ast.Call)):
                continue
            held = {n.id for n in ast.walk(node.value) if isinstance(n, ast.Name) and n.id in self.funcs}
            for t in targets:
                if isinstance(t, ast.Name) and held:
                    self.tables[t.id] = held
        self.body = {n: _nodes(f) for n, f in self.funcs.items()}
        # a factory hands its nested defs to whoever stores and calls what it returns
        self.nested = {
            n: {c.name for c in ast.walk(f) if c is not f and isinstance(c, (ast.FunctionDef, ast.AsyncFunctionDef))}
            for n, f in self.funcs.items()
        }


def _hits(node: ast.AST, m: _Module, own: Set[str], raising: Dict[str, Set[str]]) -> bool:
    """Whether ``node`` (in module ``m``, whose raising functions are ``own``) can raise ``_NoContent``."""
    if _raises_directly(node):
        return True
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        callee = node.func.id
        if callee in own:
            return True
        if callee not in m.funcs and callee in m.fns:  # an imported function
            mod, fn = m.fns[callee]
            return fn in raising.get(mod, set())
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
        mod = m.mods.get(node.func.value.id)
        return mod is not None and node.func.attr in raising.get(mod, set())
    if isinstance(node, ast.Name) and node.id in m.tables:  # reading a dispatch table holding a raiser
        return any(h in own or m.nested.get(h, set()) & own for h in m.tables[node.id])
    return False


def _raising(modules: Dict[str, _Module]) -> Dict[str, Set[str]]:
    """``{module: raising (unguarded) function names}``, to a fixpoint across every module."""
    raising: Dict[str, Set[str]] = {name: set() for name in modules}
    changed = True
    while changed:
        changed = False
        for name, m in modules.items():
            if (
                m.file in _DEFINERS
            ):  # their raisers are matched by name (``_RAISER_CALLS``, ``get_text(no_content=True)``)
                continue
            own = raising[name]
            for fname, nodes in m.body.items():
                if fname in m.guarded or fname in own:
                    continue
                if any(_hits(node, m, own, raising) for node in nodes):
                    own.add(fname)
                    changed = True
    return raising


def _leaks_in(root: Path) -> Dict[str, List[str]]:
    modules = {m.name: m for m in (_Module(p, root) for p in sorted(root.rglob("*.py")))}
    raising = _raising(modules)
    found = {}
    for name, m in modules.items():
        if m.file in _DEFINERS:
            continue
        leaks = sorted(n for n in raising[name] & m.top if not n.startswith("_") or n == "_get")
        if leaks:
            found[name] = leaks
    return found


def test_no_public_function_can_raise_the_private_no_content():
    """R4.1b-d-2 / -3 / -9: every public function (and every runtime ``_get``) that can meet a 204 below it is a boundary.

    The walk follows bare-name calls, calls through an imported module alias (``_bb._fetch_json``, another module's
    private helper), imported functions, functions held in a module-level table (a route dispatch dict) and a
    ``raise _NoContent``. Known limit: a call through a value it cannot name statically (``getattr``, a function
    passed as an argument) is not followed; the behavioural tests below serve a 204 to those runtimes.
    """
    assert _leaks_in(_PKG) == {}


_GUARD = "@_rt._no_content_is_empty\ndef _get(u):\n    return season(u)\n"
_RAISE = "    return _rt._fetch_json(x)\n"


@pytest.mark.parametrize(
    ("files", "want"),
    [
        ({"a.py": "def season(x):\n" + _RAISE + "\n" + _GUARD}, {"sportsdataverse.a": ["season"]}),
        ({"a.py": "@_rt._no_content_is_unknown\ndef season(x):\n" + _RAISE + "\n" + _GUARD}, {}),
        # R4.1b-d-9 A1: a direct raise
        ({"a.py": "def season(x):\n    raise _rt._NoContent('x')\n"}, {"sportsdataverse.a": ["season"]}),
        # A3: another module's private raiser, through an import alias
        (
            {
                "b.py": "def _ids(x):\n" + _RAISE,
                "a.py": "from sportsdataverse import b as B\n\n\ndef season(x):\n    return B._ids(x)\n",
            },
            {"sportsdataverse.a": ["season"]},
        ),
        # A4: a dispatch table holding a raiser
        (
            {
                "a.py": "def _h(x):\n"
                + _RAISE
                + "\n_ROUTES = {'h': _h}\n\n\ndef season(k, x):\n    return _ROUTES[k](x)\n"
            },
            {"sportsdataverse.a": ["season"]},
        ),
        # a factory's nested handler held in a table (tbf's routes)
        (
            {
                "a.py": "def _route(p):\n    def handler(x):\n    "
                + _RAISE
                + "    return handler\n\n_ROUTES = {'h': _route('p')}\n\n\ndef season(k, x):\n    return _ROUTES[k](x)\n"
            },
            {"sportsdataverse.a": ["season"]},
        ),
    ],
)
def test_the_structural_check_sees_each_kind_of_leak(tmp_path, files, want):
    """The walk is not vacuous: each blind spot R4.1b-d-9 named is a leak, and a guarded lookup is not."""
    root = tmp_path / "sportsdataverse"
    for rel, src in files.items():
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        (root / rel).write_text(src, encoding="utf-8")
    assert _leaks_in(root) == want


# --- behaviour: serve a 204 to everything and call each boundary -------------------------------------------------


@pytest.fixture
def no_content(monkeypatch):
    calls, _blocked = _answer_every_transport(monkeypatch, 204, "", "application/json")
    return calls


def _resolvers() -> Dict[str, Any]:
    from sportsdataverse.basketball.atrium import atrium_runtime
    from sportsdataverse.basketball.plg import pleague_runtime
    from sportsdataverse.basketball.vtb import vtb_infobasket_runtime
    from sportsdataverse.basketball.wjbl import wjbl_runtime
    from sportsdataverse.hockey.khl import khl_webcaster_runtime
    from sportsdataverse.hockey.vhl import vhl_runtime

    return {
        "atrium_season_id": lambda: atrium_runtime.atrium_season_id(14, 2026),
        "vtb season_table": lambda: vtb_infobasket_runtime.season_table(),
        "vtb season_comp_id": lambda: vtb_infobasket_runtime.season_comp_id(2026),
        "wjbl league_id": lambda: wjbl_runtime.league_id(2026),
        "khl_webcaster season_table": lambda: khl_webcaster_runtime.season_table(),
        "khl_webcaster stage_for": lambda: khl_webcaster_runtime.stage_for(2026),
        "vhl_tn": lambda: vhl_runtime.vhl_tn(2026),
        "pleague game_record": lambda: pleague_runtime.game_record(1),
    }


@pytest.fixture
def clean_caches():
    from sportsdataverse.basketball.atrium import atrium_runtime
    from sportsdataverse.basketball.plg import pleague_runtime
    from sportsdataverse.basketball.vtb import vtb_infobasket_runtime
    from sportsdataverse.basketball.wjbl import wjbl_runtime
    from sportsdataverse.hockey.khl import khl_webcaster_runtime
    from sportsdataverse.hockey.vhl import vhl_runtime

    def clear() -> None:
        atrium_runtime._seasons_cache.clear()
        pleague_runtime.clear_cache()
        vtb_infobasket_runtime.clear_cache()
        wjbl_runtime.clear_cache()
        khl_webcaster_runtime.season_table.cache_clear()
        vhl_runtime._tournaments.cache_clear()

    clear()
    yield
    clear()


@pytest.mark.parametrize("name", sorted(_resolvers()))
def test_a_lookup_answering_no_content_is_an_unknown_answer(no_content, clean_caches, name):
    """R4.1b-d-3: a public resolver whose page answers 204 raises AssetFetchError, never ``_NoContent``."""
    with pytest.raises(AssetFetchError, match="answered HTTP 204: no content, so the answer is unknown") as err:
        _resolvers()[name]()
    assert not isinstance(err.value, _NoContent)


def test_pleague_box_data_answering_no_content_is_empty(monkeypatch, no_content, clean_caches):
    """A box body is data: its 204 is ``{}`` (the game page it is checked against is seeded)."""
    from sportsdataverse.basketball.plg import pleague_runtime

    monkeypatch.setitem(pleague_runtime._GAMES, 1, {"gid": 1, "seeded": True})
    assert pleague_runtime.box_data(1) == {}


def test_feb_token_page_answering_no_content_is_an_unknown_answer(monkeypatch, no_content):
    """R4.1b-d-1: feb's bearer-token page is a bootstrap; its 204 is AssetFetchError (it was {})."""
    from sportsdataverse.basketball.feb import feb_runtime

    monkeypatch.setitem(feb_runtime._tok, "value", None)
    with pytest.raises(AssetFetchError, match="unknown"):
        feb_runtime._get("https://intrafeb.feb.es/LiveStats.API/api/v1/TeamStats/2535725")
    assert no_content == ["https://baloncestoenvivo.feb.es/"]  # the data request was never sent


def test_khl_session_page_answering_no_content_is_an_unknown_answer(monkeypatch, no_content):
    """R4.1b-d-1: khl's session-priming page has no sessid to read; its 204 is AssetFetchError (it was {})."""
    from sportsdataverse.hockey.khl import khl_runtime

    monkeypatch.setattr(khl_runtime, "_sessions", {})
    with pytest.raises(AssetFetchError, match="without a sessid"):
        khl_runtime._get("https://www.khl.ru/rest/game/header/", {"values[tournament]": 1})


def test_khl_data_post_answering_no_content_is_empty(monkeypatch, no_content):
    """R4.1b-d-1: with the session primed, the data POST's 204 is ``{}`` (it was AssetFetchError)."""
    from sportsdataverse.hockey.khl import khl_runtime

    monkeypatch.setattr(khl_runtime, "_sessions", {"https://www.khl.ru": [khl_runtime._new_http(), "seeded"]})
    assert khl_runtime._get("https://www.khl.ru/rest/game/header/", {"values[tournament]": 1}) == {}
    assert no_content == ["https://www.khl.ru/rest/game/header/"]


def test_lkl_finished_game_check_answering_no_content_is_an_unknown_answer(monkeypatch, no_content):
    """The finished-game check before a box score is a lookup; the game-page route's own 204 is ``{}``."""
    from sportsdataverse.basketball.lkl import lkl_runtime

    monkeypatch.setattr(lkl_runtime, "_GAMES", {})
    with pytest.raises(AssetFetchError, match="finished-game check"):
        lkl_runtime._get("https://lkl.lt/api/livestream/boxscore/11446")
    assert lkl_runtime._get("https://lkl.lt/rungtynes/11446") == {}


def test_bleague_snapshot_pointer_answering_no_content_is_an_unknown_answer(no_content):
    """The ``latestid`` pointer names the snapshot to read; its 204 is AssetFetchError (it was {})."""
    from sportsdataverse.basketball.bleague import bleague_runtime

    with pytest.raises(AssetFetchError, match="latestid: the lookup answered HTTP 204"):
        bleague_runtime._get("https://b-league.s3.amazonaws.com/web_json/contexts/504734")


def test_elh_listing_answering_no_content_stays_a_failed_fetch(no_content):
    """R4.1b-d-2: ``get_text`` raises ``_NoContent`` only for a caller that opts in; elh's S3 listing does not."""
    from sportsdataverse.hockey.elh import elh_runtime

    with pytest.raises(AssetFetchError) as err:
        elh_runtime.elh_game_ids(2026)
    assert not isinstance(err.value, _NoContent)


def test_magnus_game_sheet_answering_no_content_is_empty(no_content):
    """R4.1b-d-7: get_text's plain-download branch (no ``transport``), opted in by magnus's game sheet."""
    from sportsdataverse.hockey.magnus import magnus_runtime

    assert magnus_runtime._get("https://liguemagnus.com/rencontre/12345/") == {}
    assert no_content == ["https://liguemagnus.com/rencontre/12345/"]


def test_basketball_proxy_path_answering_no_content_is_empty(monkeypatch, no_content):
    """The proxy branch of ``basketball_runtime._fetch_json`` raises ``_NoContent`` too; ``_get`` answers ``{}``."""
    from sportsdataverse.basketball import basketball_runtime

    monkeypatch.setenv("SDV_TEST_BB_PROXY", "http://proxy.invalid:3128")
    with pytest.raises(_NoContent):
        basketball_runtime._fetch_json("https://example.test/x", proxy_env="SDV_TEST_BB_PROXY")
    assert basketball_runtime._get("https://example.test/x", proxy_env="SDV_TEST_BB_PROXY") == {}


# --- R4.1b-d-8: a dependency request inside a data getter; only the route's own data request may answer {} -------


@pytest.fixture
def routed(monkeypatch):
    """Serve ``answers[substring]`` to the first URL containing it, else ``default``; return the URLs served."""
    import requests

    import sportsdataverse.cache as cache

    calls: List[str] = []
    state: Dict[str, Any] = {"answers": {}, "default": (200, "{}")}

    def request(self, method, url, *args, **kwargs):
        calls.append(url)
        status, body = next((a for k, a in state["answers"].items() if k in url), state["default"])
        resp = requests.Response()
        resp.status_code, resp._content, resp.encoding, resp.url = status, body.encode("utf-8"), "utf-8", url
        resp.headers["Content-Type"] = "application/json"
        return resp

    monkeypatch.setattr(requests.Session, "request", request)
    monkeypatch.setattr(cache, "get_cache_mode", lambda: "off")
    monkeypatch.setattr("time.sleep", lambda *a, **k: None)

    def install(answers: Dict[str, Any], default: Any) -> List[str]:
        state["answers"], state["default"] = answers, default
        return calls

    return install


def test_kzs_rows_are_never_discarded_for_a_dependency_that_answered_no_content(monkeypatch, routed):
    """The reviewer's probe: the data request answered rows, its standings scope check answered 204."""
    from sportsdataverse.basketball.kzs import kzs_runtime as R

    monkeypatch.setattr(R, "_CACHE", {})
    calls = routed({"/matches/standings": (204, "")}, (200, '{"status": "OK", "data": [{"id": 1, "name": "x"}]}'))
    with pytest.raises(AssetFetchError, match="the lookup answered HTTP 204"):
        R._get(R.BASE + "/teams/", {"competitionId": 5})
    assert any("/teams/" in c for c in calls) and any("/matches/standings" in c for c in calls)


def test_easl_seasons_list_answering_no_content_is_an_unknown_answer(monkeypatch, routed):
    from sportsdataverse.basketball.easl import easl_runtime as R

    monkeypatch.setattr(R, "_CACHE", {})
    calls = routed({"/metadata/seasons": (204, "")}, (200, "[]"))
    with pytest.raises(AssetFetchError, match="the lookup answered HTTP 204"):
        R._get(R.BASE + "/matches", {"season": 2026})
    assert calls == [R.BASE + "/metadata/seasons"]  # the data request is never sent on an unknown season


def test_tpbl_season_list_answering_no_content_is_an_unknown_answer(monkeypatch, routed):
    from sportsdataverse.basketball.tpbl import tpbl_runtime as R

    monkeypatch.setattr(R, "_SEASONS", [])
    routed({f"{R.API}/seasons": (204, "")}, (200, "[]"))
    with pytest.raises(AssetFetchError, match="the lookup answered HTTP 204"):
        R._get(f"{R.API}/seasons/2025/games")


def test_tbf_league_list_answering_no_content_is_an_unknown_answer(monkeypatch, routed):
    from sportsdataverse.basketball.tbf import tbf_runtime as R

    monkeypatch.setattr(R, "_PREFIX", {})
    calls = routed({"get-leagues-and-seasons-by-prefix": (204, "")}, (200, '{"data": [], "isSuccess": true}'))
    with pytest.raises(AssetFetchError, match="the lookup answered HTTP 204"):
        R._get(R.API + "/League/get-standings-table", {"season": 2025, "competition": "bsl"})
    assert not any("get-standings-table" in c for c in calls)


def test_tbf_schedule_week_answering_no_content_is_an_unknown_answer(monkeypatch, routed):
    """A season schedule is one page per week: a 204 on one leaves it incomplete."""
    from sportsdataverse.basketball.tbf import tbf_runtime as R

    monkeypatch.setattr(R, "_league", lambda season, competition: (1, 2, "bsl", 2025))
    monkeypatch.setattr(R, "_weeks_list", lambda sid, aid: [{"sezon_Hafta": 1}, {"sezon_Hafta": 2}])
    routed({"get-all-matches-for-filter": (204, "")}, (200, "{}"))
    with pytest.raises(AssetFetchError, match="a season's weeks"):
        R._get(R.API + "/Match/get-all-matches-for-filter", {"season": 2025})


def _no_content(*args: Any, **kwargs: Any) -> Any:
    raise _NoContent("answered HTTP 204: no content")


def _mod(name: str) -> Any:
    import importlib

    return importlib.import_module(f"sportsdataverse.{name}")


# (module, what to make answer 204, how to reach the lookup) -- each guard of R4.1b-d-8, one row each
_LOOKUPS = {
    "kzs _competition": ("basketball.kzs.kzs_runtime", "_fetch", lambda R: R._competition(5)),
    "kzs _team_ids": ("basketball.kzs.kzs_runtime", "_fetch", lambda R: R._team_ids(5)),
    "kzs _header": ("basketball.kzs.kzs_runtime", "_fetch", lambda R: R._header(7)),
    "kzs _season_competitions": ("basketball.kzs.kzs_runtime", "_fetch", lambda R: R._season_competitions(27)),
    "easl _season": ("basketball.easl.easl_runtime", "_fetch", lambda R: R._season(2026)),
    "easl _no_stale": ("basketball.easl.easl_runtime", "_fetch", lambda R: R._no_stale([])),
    "tpbl _season_row": ("basketball.tpbl.tpbl_runtime", "_list_body", lambda R: R._season_row(2025)),
    "tpbl _verify_games": ("basketball.tpbl.tpbl_runtime", "_list_body", lambda R: R._verify_games("u", {"id": 1}, [])),
    "tbf _league": ("basketball.tbf.tbf_runtime", "_fetch", lambda R: R._league(2025, "bsl")),
    "cba _info": ("basketball.cba.cba_runtime", "_fetch", lambda R: R._info(1)),
    "wjbl _final_header": ("basketball.wjbl.wjbl_runtime", "_fetch", lambda R: R._final_header(1)),
    # lookups at a ``with _no_content_unknown(...)`` site inside a route
    "easl header check": (
        "basketball.easl.easl_runtime",
        "_header",
        lambda R: R._get(R.BASE + "/matches/10af8fd4-106e-11f1-960b-5798087c7aa3/boxscore"),
    ),
    "tpbl game check (stats)": ("basketball.tpbl.tpbl_runtime", "_game", lambda R: R._get(f"{R.API}/games/1/stats")),
    "tbf finished-game check": (
        "basketball.tbf.tbf_runtime",
        "_game",
        lambda R: R._get(R.API + "/Match/mac-istatistik", {"match_id": 1}),
    ),
    "bnxt finished-game gate": (
        "basketball.bnxt.bnxt_runtime",
        "_game",
        lambda R: R._get(R.API + "/schedule/overview/1"),
    ),
    "kbl final-game check": (
        "basketball.kbl.kbl_runtime",
        "_game",
        lambda R: R._get(R.BASE + "/match/S47G01N49/player-stat"),
    ),
    "kbl a window of several": (
        "basketball.kbl.kbl_runtime",
        "_fetch",
        lambda R: R._get(R.BASE + "/match/list", {"season": 2025}),
    ),
    "aba helper's match lookup": (
        "basketball.aba.aba_runtime",
        "_page",
        lambda R: R._get("https://www.aba-liga.com" + R.LIVE + "/create_json.php", {"game_no": 1, "season": 2025}),
    ),
    "fiba page-URL lookups": (
        "basketball.fiba_hub.fiba_runtime",
        "_page_url",
        lambda R: R._get("https://www.fiba.basketball/en/games/100720", {"comp": 208182}),
    ),
}


@pytest.mark.parametrize("name", sorted(_LOOKUPS))
def test_a_dependency_answering_no_content_is_an_unknown_answer(monkeypatch, name):
    """R4.1b-d-8: inside a data getter, only the route's own data request may answer {}; a lookup's 204 is unknown."""
    mod, target, call = _LOOKUPS[name]
    R = _mod(mod)
    for cache in ("_CACHE", "_SEASONS", "_PREFIX", "_INFO", "_GAMES"):
        if isinstance(getattr(R, cache, None), (dict, list)):
            monkeypatch.setattr(R, cache, type(getattr(R, cache))())
    monkeypatch.setattr(R, target, _no_content)
    with pytest.raises(AssetFetchError, match="the lookup answered HTTP 204") as err:
        call(R)
    assert not isinstance(err.value, _NoContent)


def test_kbl_one_window_answering_no_content_is_the_routes_empty_answer(monkeypatch):
    """A schedule asked for one window is one request: its 204 is the route's ``{}``."""
    R = _mod("basketball.kbl.kbl_runtime")
    monkeypatch.setattr(R, "_fetch", _no_content)
    assert R._get(R.BASE + "/match/list", {"fromDate": "20250101", "toDate": "20250131"}) == {}


def test_fiba_alternate_url_lookups_answering_no_content_are_an_unknown_answer(monkeypatch):
    """The game page answered the not-found shell; the lookups that build the alternate URLs then answered 204."""
    R = _mod("basketball.fiba_hub.fiba_runtime")

    def shell(*args: Any, **kwargs: Any) -> Any:
        raise R._NotFoundShell("shell")

    monkeypatch.setattr(R, "_page_url", lambda game_id, comp: f"{R.SITE_URL}/en/history/x/{comp}/games/{game_id}-A-B")
    monkeypatch.setattr(R, "_fetch_game_page", shell)
    monkeypatch.setattr(R, "_team_codes", _no_content)
    with pytest.raises(AssetFetchError, match=r"alternate-URL lookups\): the lookup answered HTTP 204") as err:
        R._get("https://www.fiba.basketball/en/games/100720", {"comp": 208182})
    assert not isinstance(err.value, (_NoContent, R._NotFoundShell))


_FID = "10af8fd4-106e-11f1-960b-5798087c7aa3"
_CTX = {"body": {"fixtureId": _FID}, "teams": ["A", "B"], "scores": {"A": 80, "B": 70}}


def test_easl_period_table_is_the_period_routes_own_answer(monkeypatch):
    """R4.1b-d-11: the per-period route's own 204 is {}; the same table checked by the play-by-play is a lookup."""
    R = _mod("basketball.easl.easl_runtime")
    monkeypatch.setattr(R, "_header", lambda fid, **kw: _CTX)
    monkeypatch.setattr(R, "_fetch", _no_content)
    assert R._get(f"{R.BASE}/matches/{_FID}/teamstats/by/period") == {}
    pbp = {"playbyplay": [{"periodId": 4, "scores": {"A": 80, "B": 70}}], "teams": [{"teamId": "A"}, {"teamId": "B"}]}
    monkeypatch.setattr(R, "_fetch", lambda path, params=None, **kw: pbp)
    monkeypatch.setattr(R, "_period_table", _no_content)
    with pytest.raises(AssetFetchError, match=r"period check\): the lookup answered HTTP 204"):
        R._get(f"{R.BASE}/matches/{_FID}/playbyplay")


def _goalie_page(n: int) -> Dict[str, Any]:
    rows = [{"cols": {"gp": 1, "w": 0}, "first": [{"link": f"/p/{n}-{i}", "val": f"G{n}{i}"}]} for i in range(20)]
    return {"status": "success", "data": {"list": [{"list": rows}]}}  # a full page, no nav.max


def test_khl_goalie_paging_a_204_after_page_one_is_a_partial_list(monkeypatch):
    """R4.1b-d-10: a full page 1 with no nav.max, then a 204 on page 2, is unknown, never page 1 alone."""
    R = _mod("hockey.khl.khl_runtime")
    pages = iter([_goalie_page(1)])
    monkeypatch.setattr(R, "_post", lambda *a, **k: next(pages, None) or _no_content())
    with pytest.raises(AssetFetchError, match="page 2 answered HTTP 204 mid-list"):
        R._all_goalies("https://www.khl.ru/rest/stat/players/", {}, None, 1, {})


def test_khl_goalie_paging_a_204_on_page_one_is_the_routes_empty_answer(monkeypatch):
    """A 204 on page 1 is the route's own answer: ``_NoContent``, which ``_get`` turns into {}."""
    R = _mod("hockey.khl.khl_runtime")
    monkeypatch.setattr(R, "_post", _no_content)
    with pytest.raises(_NoContent):
        R._all_goalies("https://www.khl.ru/rest/stat/players/", {}, None, 1, {})
