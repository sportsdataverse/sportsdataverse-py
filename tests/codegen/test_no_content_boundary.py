"""``_codegen_runtime._NoContent`` never escapes a public function (task 4.1b PR 4 review, R4.1b-d-1..3, R4.1b-d-7).

A 204 / 205 is ``{}`` from a DATA getter (``@_no_content_is_empty``) and an ``AssetFetchError`` from a lookup or
bootstrap whose 204 leaves nothing to read (``@_no_content_is_unknown`` / ``with _no_content_unknown(...)``): a
token, session or key page, a season resolver, a snapshot pointer, a finished-game check. The structural test
walks every module's call graph; the behavioural tests serve a 204 to each boundary through the transports.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Dict, List, Set

import pytest

from sportsdataverse._codegen_runtime import _NoContent
from sportsdataverse.errors import AssetFetchError
from tests.codegen.test_runtime_errors import _answer_every_transport

_PKG = Path(__file__).resolve().parents[2] / "sportsdataverse"
# the definitions themselves: they raise by design and every caller is checked
_DEFINERS = {"_codegen_runtime.py", "basketball_runtime.py", "euro_hockey_runtime.py"}
_BOUNDARIES = ("_no_content_is_empty", "_no_content_is_unknown")


def _raises_directly(call: ast.Call) -> bool:
    fn = call.func
    name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
    if name in ("_check_no_content", "_no_content_transport", "_fetch_json"):
        return True
    return name == "get_text" and any(
        k.arg == "no_content" and isinstance(k.value, ast.Constant) and k.value.value is True for k in call.keywords
    )


def _calls(fn: ast.AST) -> List[ast.Call]:
    """The calls in ``fn``, minus those inside a ``with ..._no_content_unknown(...)`` block (mapped there)."""
    out: List[ast.Call] = []

    def walk(node: ast.AST) -> None:
        if isinstance(node, ast.With) and any("_no_content_unknown" in ast.unparse(i.context_expr) for i in node.items):
            return
        if isinstance(node, ast.Call):
            out.append(node)
        for child in ast.iter_child_nodes(node):
            walk(child)

    for child in ast.iter_child_nodes(fn):
        walk(child)
    return out


def _leaks(path: Path) -> List[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    guarded = {n for n, f in funcs.items() if any(b in ast.unparse(d) for d in f.decorator_list for b in _BOUNDARIES)}
    calls = {n: _calls(f) for n, f in funcs.items()}
    raising: Set[str] = {n for n, cs in calls.items() if n not in guarded and any(_raises_directly(c) for c in cs)}
    changed = True
    while changed:
        changed = False
        for n, cs in calls.items():
            if n in raising or n in guarded:
                continue
            names = {c.func.id for c in cs if isinstance(c.func, ast.Name)}
            if names & raising:
                raising.add(n)
                changed = True
    top = {n.name for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    return sorted(n for n in raising & top if not n.startswith("_") or n == "_get")


def test_no_public_function_can_raise_the_private_no_content():
    """R4.1b-d-2 / -3: every public function (and every runtime ``_get``) that can meet a 204 below it is a boundary."""
    found: Dict[str, List[str]] = {}
    for path in sorted(_PKG.rglob("*.py")):
        if path.name in _DEFINERS:
            continue
        leaks = _leaks(path)
        if leaks:
            found[path.relative_to(_PKG).as_posix()] = leaks
    assert found == {}


def test_the_structural_check_sees_a_leak(tmp_path):
    """The walk above is not vacuous: an undecorated function that calls a raiser is a leak; a guarded one is not."""
    src = "def season(x):\n    return _rt._fetch_json(x)\n\n@_rt._no_content_is_empty\ndef _get(u):\n    return season(u)\n"
    path = tmp_path / "probe.py"
    path.write_text(src, encoding="utf-8")
    assert _leaks(path) == ["season"]
    path.write_text("@_rt._no_content_is_unknown\n" + src, encoding="utf-8")
    assert _leaks(path) == []


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
