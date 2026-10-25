"""One private body behind every former copy (task 6.1: B-L26, B-L27, B-L28, B-L46, B-L52, B-GB-11).

Each row names the shared body and every module that used to carry its own copy. Two checks:

* identity -- each former copy's name is now bound to the one shared function, so a fix reaches
  every caller;
* equality at the merge commit -- the shared body's AST (docstring and name stripped) hashes to the
  value the deleted copies hashed to. A deliberate change to a shared body updates its pin here.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import textwrap
from pathlib import Path

import polars as pl
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def _body_hash(fn: object) -> str:
    """sha256[:16] of a function's signature + body AST, docstring and name excluded."""
    node = ast.parse(textwrap.dedent(inspect.getsource(fn))).body[0]
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    body = node.body
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    sig = ast.dump(node.args) + (ast.dump(node.returns) if node.returns else "")
    return hashlib.sha256((sig + "".join(ast.dump(b) for b in body)).encode()).hexdigest()[:16]


_BOX = "sportsdataverse.scrape.ncaa._box_html"
_BOX_COPIES = (
    "sportsdataverse.mbb.mbb_ncaa_box_tabs",
    "sportsdataverse.cfb.cfb_ncaa_box",
    "sportsdataverse.baseball.college_baseball.college_baseball_ncaa_box",
)
_ROSTER = "sportsdataverse.mbb.mbb_ncaa_roster_parser"
_FOX = "sportsdataverse._fox_layout"
_FOX_EXT = "sportsdataverse.cfb.cfb_fox_ext"

# (shared module, shared name, [(former-copy module, name there)], hash of the deleted copies)
SHARED = [
    (_BOX, "_cells", [(m, "_cells") for m in _BOX_COPIES], "f715d1baca8170d0"),
    (_BOX, "_rows", [(m, "_rows") for m in _BOX_COPIES], "12d05e090f3fb7d2"),
    (_BOX, "_cid", [(m, "_cid") for m in _BOX_COPIES], "5d3c6c7c83c81af1"),
    (_BOX, "_int", [(m, "_int") for m in _BOX_COPIES], "9bc0ee738dfa364f"),
    (_BOX, "_finish", [(m, "_finish") for m in _BOX_COPIES], "9bbf594d900f05ff"),
    (
        _ROSTER,
        "_java_string_hash",
        [("sportsdataverse.mbb.mbb_ncaa_boxscore_parser", "_java_string_hash")],
        "81c2c4d7e26b28a0",
    ),
    (
        _ROSTER,
        "_hashmap_improve",
        [("sportsdataverse.mbb.mbb_ncaa_boxscore_parser", "_hashmap_improve")],
        "f346f43cf43e684d",
    ),
    (
        _ROSTER,
        "_hashmap_radix_key",
        [("sportsdataverse.mbb.mbb_ncaa_boxscore_parser", "_hashmap_radix_key")],
        "518a1da8e4a8c911",
    ),
    (
        "sportsdataverse.scrape.ncaa.capture",
        "_parse_shard",
        [("sportsdataverse.scrape.ncaa.parse", "_parse_shard")],
        "5125db40c5d192dd",
    ),
    (_FOX, "_clean", [(_FOX_EXT, "_clean")], "696d5113317fffa5"),
    (_FOX, "_table_rows", [(_FOX_EXT, "_table_rows")], "809c702af8147d4c"),
    (_FOX, "_cells", [(_FOX_EXT, "_cells")], "e4433f1643bd6b1a"),
    (_FOX, "_uri_id", [(_FOX_EXT, "_uri_id")], "6c9ee881f9c919b2"),
    (_FOX, "_full_team_name", [(_FOX_EXT, "_fox_team_fullname")], "0c2148ef5a4ca870"),
]


@pytest.mark.parametrize(("owner", "name", "copies", "pin"), SHARED, ids=[f"{r[0]}.{r[1]}" for r in SHARED])
def test_every_former_copy_is_the_shared_body(owner: str, name: str, copies: list, pin: str) -> None:
    shared = getattr(importlib.import_module(owner), name)
    for module, copy_name in copies:
        assert getattr(importlib.import_module(module), copy_name) is shared, f"{module}.{copy_name} is a copy again"
    assert _body_hash(shared) == pin, f"{owner}.{name} no longer equals the copies it replaced; update the pin"


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
