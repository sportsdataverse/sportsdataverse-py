"""Every ``schemas/native/<stem>`` returns schema re-derives from its committed capture (A-N14).

``--check`` never re-derives a returns schema, and ``--schemas`` prints a parser crash on its
own capture and moves on, so a stale schema could not fail CI. This sweep runs each
endpoint's parser on every committed capture of it and compares the result with the
committed schema: frame names, column names and order exactly, and the type of every
column that holds a value. A parser crash on a committed capture fails the sweep.

Stems are discovered from ``tools/codegen/schemas/native/``, captures from the conventions
the capture tooling writes (``native_fixture_map.yaml``, ``tests/fixtures/<stem>/<short>.json``,
``.../endpoints/<short>.json`` and the nba_stats overrides), so a new stem or a new capture
is swept without a test edit. That is more than ``--schemas`` reads (only
``native_fixture_map.yaml``), which is why the sweep finds schemas ``--schemas`` never re-derives.
``_doc_type`` is one more copy of the polars-to-docs type map task 3.9 unifies (A-L44); import that
one when it lands. A stem whose captures live only in sdv-internal-refs is listed
in ``_NO_COMMITTED_CAPTURE`` and must stay capture-free, or the sweep asks for its removal.
"""

from __future__ import annotations

import functools
import importlib
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
import yaml

from tools.codegen import gen_nba_stats, generate, spec

ROOT = Path(__file__).resolve().parents[2]
NATIVE = ROOT / "tools/codegen/schemas/native"
FIX = ROOT / "tests/fixtures"
_NAT_MAP: Dict[str, Dict[str, str]] = (
    yaml.safe_load((ROOT / "tools/codegen/native_fixture_map.yaml").read_text(encoding="utf-8")) or {}
)
_PARAMS = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")


@functools.lru_cache(maxsize=None)
def _api(stem: str) -> spec.FlatApi:
    return spec.load_flat_api(generate.ENDPOINTS / f"{stem}.yaml", _PARAMS)


def _captures(stem: str, short: str) -> List[Path]:
    """Every committed capture of ``stem``/``short``, by the conventions the capture tooling writes."""
    found = [FIX / stem / f for f, s in (_NAT_MAP.get(stem) or {}).items() if s == short]
    found += [FIX / stem / f"{short}.json", FIX / stem / "endpoints" / f"{short}.json"]
    if stem in gen_nba_stats.STEMS:
        found.append(gen_nba_stats.capture_path(stem, short))
    return sorted({p for p in found if p.exists()})


def _cases() -> Tuple[List[Any], Dict[str, int]]:
    cases, per_stem = [], {}
    for d in sorted(p for p in NATIVE.iterdir() if p.is_dir()):
        api = _api(d.name)
        by_short = {e.short: e for e in api.endpoints}
        per_stem[d.name] = 0
        for f in sorted(d.glob("*.yaml")):
            ep = by_short.get(f.stem)
            if ep is None or not ep.parser:
                continue
            for cap in _captures(d.name, f.stem):
                per_stem[d.name] += 1
                stale = _STALE.get((d.name, f.stem))
                marks = (
                    [pytest.mark.xfail(strict=True, raises=AssertionError, reason=f"A-N14: {stale}; {_OWNER}")]
                    if stale
                    else []
                )
                cases.append(pytest.param(d.name, f.stem, cap, id=f"{d.name}/{f.stem}/{cap.name}", marks=marks))
    return cases, per_stem


_OWNER = "regenerated in task 3.9 (and --schemas fails on a parser crash in task 4.9)"
# Schemas that do not re-derive from the committed capture today (measured 2026-10-11).
_STALE = {
    ("kalshi", "series"): "the schema names product_metadata_scope, which the parser does not emit on the capture",
    (
        "mlb_statcast",
        "gamefeed",
    ): "the schema documents 9 batted-ball columns; the parser emits 39 pitch columns it lacks",
    ("nfl_api", "game_details_by_slug"): "the schema names replays, which the parser does not emit on the capture",
    ("pff", "player_passing_summary"): "no columns published (note: player_detail); the parser emits 65",
    ("pff", "player_position_pivot"): "no columns published (note: opaque); the parser emits 3",
    ("pff", "player_seasons"): "no columns published; the parser emits value",
    ("pff_api", "player_offense_pass_blocking"): "20 schema columns are missing from the parse of PFF's spec example",
    # main 10-17 (#826), measured 2026-10-18 (EX-SYNC-e)
    ("liiga", "games"): "EX-SYNC-e: the schema names end, play_off_pair, play_off_phase, play_off_req_wins and "
    "buy_tickets_url, which the parser does not emit on tests/fixtures/liiga/games.json",
}


_CASES, _PER_STEM = _cases()
# Stems whose schemas were built from sdv-internal-refs captures; none is committed here.
_NO_COMMITTED_CAPTURE = {
    "bart_wbb",
    "cbs_napi",
    "football_data",
    "mls_api",
    "nwsl_api",
    "openligadb",
    "polymarket",
    "sleeper",
    "sports247",
    "sports247_site_pages",
    "thesportsdb",
    "torvik",
    "uefa",
    "yahoo_shangrila",
}

# Main's families merged 10-17 (#826, #859, #862): their captures ARE committed, under tests/fixtures/<stem>/
# (ibba_segev's under tests/fixtures/ibba/), but in layouts the sweep does not read and with no
# native_fixture_map.yaml entry, so their schemas are not re-derived here (EX-SYNC-f; task 3.9 maps them).
_UNMAPPED_CAPTURES = {
    "aba", "acb", "atrium", "bleague", "cba", "cebl", "del_esports", "easl", "eihl_esports", "elh", "feb",
    "fiba_gamepage", "fiba_gdap", "genius", "ha", "ibba", "ibba_segev", "icehl_esports", "kbl", "khl_rest",
    "khl_webcaster", "kzs", "legabasket", "lkl", "magnus", "metal_esports", "nationalleague", "nbl_rosetta",
    "pleague", "pleague_preciser", "scb", "shl", "sportality", "sportpress", "tbf", "tpbl", "vhl",
    "vtb_infobasket", "vtb_league", "wjbl",
}  # fmt: skip


def _doc_type(dtype: Any) -> str:
    s = str(dtype)
    if s.startswith(("Int", "UInt")):
        return "integer"
    if s.startswith("Float"):
        return "number"
    if s == "Boolean":
        return "logical"
    if s in ("String", "Utf8", "Categorical", "Null"):
        return "character"
    return ""  # Date, List, Struct, ...: writers spell these differently, so names only


_SCHEMA_TYPE = {"double": "number", "numeric": "number"}
_PRIMITIVE = {"integer", "number", "logical", "character"}  # list / Date: writers differ, names only


def _frames(parsed: Any) -> Dict[Any, Any]:
    return parsed if isinstance(parsed, dict) else {None: parsed}


def _want(doc: dict) -> Dict[Any, List[dict]]:
    if doc.get("kind") == "frames":
        return {b["section"]: b["columns"] for b in doc["frames"]}
    return {None: doc["columns"]}


def _load(cap: Path) -> Any:
    text = cap.read_text(encoding="utf-8")
    return json.loads(text) if cap.suffix == ".json" else text


def test_every_native_stem_is_swept_or_has_no_committed_capture():
    stems = {d.name for d in NATIVE.iterdir() if d.is_dir()}
    assert {s for s, n in _PER_STEM.items() if n == 0} == _NO_COMMITTED_CAPTURE | _UNMAPPED_CAPTURES
    assert [s for s in sorted(_UNMAPPED_CAPTURES - {"ibba_segev"}) if not (FIX / s).is_dir()] == []
    assert any((FIX / "ibba" / "captures" / "segev").glob("*.json"))  # ibba_segev's captures live under ibba/
    assert stems - _NO_COMMITTED_CAPTURE and len(_CASES) > 500


@pytest.mark.parametrize(("stem", "short", "cap"), _CASES)
def test_native_schema_rederives_from_its_capture(stem, short, cap):
    api = _api(stem)
    ep = next(e for e in api.endpoints if e.short == short)
    parser = getattr(importlib.import_module(f"sportsdataverse.{api.parser_module}"), ep.parser)
    doc = yaml.safe_load((NATIVE / stem / f"{short}.yaml").read_text(encoding="utf-8"))
    got = _frames(parser(_load(cap)))  # a crash on a committed capture fails here
    if doc.get("unverified"):  # nothing published: the parser must still emit nothing
        assert not any(df.width for df in got.values()), f"{short}: unverified, but the parser emits columns"
        return
    want = _want(doc)
    if doc.get("frames_by"):  # one frame per value of an argument: the capture is one of them
        (df,) = got.values()
        sec = next((s for s, cols in want.items() if [c["name"] for c in cols] == df.columns), None)
        assert sec is not None, f"columns match no {doc['frames_by']} frame of the schema"
        got = {sec: df}
    assert list(got) == list(want) or doc.get("frames_by"), f"frames {list(got)} != schema {list(want)}"
    for sec, df in got.items():
        cols = {c["name"]: _SCHEMA_TYPE.get(c["type"], c["type"]) for c in want[sec]}
        assert list(df.columns) == list(cols), f"{sec}: columns differ from the schema"
        bad = {
            col: (cols[col], _doc_type(dt))
            for col, dt in df.schema.items()
            if _doc_type(dt)
            and cols[col] in _PRIMITIVE
            and df[col].null_count() < df.height
            and (cols[col], _doc_type(dt)) != ("number", "integer")  # integral values of a numeric column
            and cols[col] != _doc_type(dt)
        }
        assert not bad, f"{sec}: (schema, parser) {bad}"
