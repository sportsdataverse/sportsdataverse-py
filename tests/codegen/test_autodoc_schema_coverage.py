"""Every DataFrame-returning autodoc function has a captured returns table, or keeps its prose."""

from __future__ import annotations

import polars as pl
import pytest
import yaml

from tools.codegen import generate

SCHEMAS = generate._AUTODOC_SCHEMA_DIR


def test_every_dataframe_autodoc_function_has_a_schema():
    missing = [
        f"{scope}.{fn}"
        for scope, fn in generate._autodoc_dataframe_names()
        if not generate._autodoc_return_columns(scope, fn) and not generate._autodoc_unverified(scope, fn)
    ]
    assert not missing, f"{len(missing)} autodoc DataFrame functions with no returns table: {missing[:25]}"


# Captured before autodoc_example_args.yaml covered them (2026-10-07): their tables exist but a
# refresh cannot re-call them. A cap, not an allowlist -- lower it as entries are added.
_CAPTURED_WITHOUT_ARGS_CAP = 65


def test_every_dataframe_autodoc_function_has_example_args_or_takes_none():
    """A function the capture must call with arguments needs an entry, so a refresh can update its table.

    Exempt: an ``unverified:`` schema (no working input exists, its reason says why) and a
    ``hand_authored`` one (the capture never rewrites it)."""
    import inspect

    args = generate._autodoc_example_args()
    bad, captured_without_args = [], []
    for scope, fn in generate._autodoc_dataframe_names():
        obj = generate._scope_callable("global" if scope == "global" else scope, fn)
        if obj is None:
            continue
        required = [
            p.name
            for p in inspect.signature(obj).parameters.values()
            if p.default is inspect.Parameter.empty and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        ]
        if not required or fn in (args.get(scope) or {}):
            continue
        path = SCHEMAS / scope / f"{fn}.yaml"
        doc = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}) if path.exists() else {}
        if "unverified" in doc or doc.get("hand_authored"):
            continue
        (captured_without_args if doc.get("columns") else bad).append(f"{scope}.{fn} needs {required}")
    assert not bad, "autodoc_example_args.yaml is missing entries:\n" + "\n".join(bad)
    assert len(captured_without_args) <= _CAPTURED_WITHOUT_ARGS_CAP, (
        "a newly captured function has no example args:\n" + "\n".join(captured_without_args)
    )


@pytest.mark.parametrize("path", sorted(SCHEMAS.rglob("*.yaml")), ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_a_captured_schema_has_columns_and_no_blank_names(path):
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if "unverified" in doc:
        pytest.skip("unverified schemas are checked by test_unverified_schemas.py")
    assert doc.get("kind") == "dataframe"
    assert doc.get("columns"), f"{path.name} publishes no columns -- it should not exist"
    assert all(str(c.get("name", "")).strip() for c in doc["columns"])


def test_failed_capture_leaves_prose_and_writes_no_table(tmp_path, monkeypatch):
    """A live call that raises, returns a non-frame, or returns a 0-column frame writes NOTHING."""
    monkeypatch.setattr(generate, "_AUTODOC_SCHEMA_DIR", tmp_path)
    generate._autodoc_return_columns.cache_clear()
    monkeypatch.setattr(generate, "_autodoc_names_by_scope", lambda: {"nfl": ["boom", "notaframe"]})

    class _Mod:
        @staticmethod
        def boom(**kw) -> pl.DataFrame:
            raise RuntimeError("403 Forbidden")

        @staticmethod
        def notaframe(**kw) -> pl.DataFrame:
            return {"a": 1}

    monkeypatch.setitem(__import__("sys").modules, "sportsdataverse.nfl", _Mod)
    generate.refresh_autodoc_schemas()
    assert not list(tmp_path.rglob("*.yaml")), "a failed capture must not write a schema file"
    assert generate._autodoc_return_columns("nfl", "boom") == ()
    assert generate._autodoc_return_columns("nfl", "notaframe") == ()


def test_a_recapture_keeps_an_authored_description():
    """Some committed autodoc schemas carry authored descriptions (the MLB hitting spine, #209);
    merging a fresh capture must keep them, and a new column arrives blank."""
    committed = [{"name": "batter", "type": "integer", "description": "MLBAM batter id."}]
    cols, new, *_ = generate._merge_autodoc_columns(committed, pl.DataFrame({"batter": [1], "xba": [0.3]}))
    assert cols[0]["description"] == "MLBAM batter id." and new == 1
    assert not str(cols[1].get("description", "")).strip()


def test_a_call_reference_is_resolved_once_and_can_pick_a_key(monkeypatch):
    calls = []

    def load(season):
        calls.append(season)
        return {"plays": f"plays-{season}", "drives": "d"}

    monkeypatch.setattr(generate, "_scope_callable", lambda scope, fn: load)
    ref = {"$call": "cfb.load_x", "args": {"season": 2024}, "key": "plays"}
    memo: dict = {}
    assert generate._resolve_autodoc_arg(ref, memo) == "plays-2024"
    assert generate._resolve_autodoc_arg(dict(ref), memo) == "plays-2024"
    assert calls == [2024]  # memoized: one load feeds every transformer that names it
    assert generate._resolve_autodoc_arg(7, memo) == 7


def test_a_stalled_call_times_out():
    import time

    with pytest.raises(TimeoutError, match="SDV_AUTODOC_CALL_TIMEOUT"):
        generate._call_with_timeout(lambda: time.sleep(5), 0.2)
    assert generate._call_with_timeout(lambda: 3, 0.2) == 3


def test_an_unannotated_function_without_args_or_schema_is_never_called(tmp_path, monkeypatch):
    """The capture calls blind only what returns a DataFrame; anything else could write or clear state."""
    monkeypatch.setattr(generate, "_AUTODOC_SCHEMA_DIR", tmp_path)
    monkeypatch.setattr(generate, "_autodoc_names_by_scope", lambda: {"nfl": ["side_effect"]})
    called = []

    class _Mod:
        @staticmethod
        def side_effect(**kw):
            called.append(1)

    monkeypatch.setitem(__import__("sys").modules, "sportsdataverse.nfl", _Mod)
    generate.refresh_autodoc_schemas()
    assert not called


def test_an_unverified_autodoc_schema_states_its_reason_instead_of_a_table(tmp_path, monkeypatch):
    monkeypatch.setattr(generate, "_AUTODOC_SCHEMA_DIR", tmp_path)
    (tmp_path / "wnba").mkdir()
    reason = "no capture: stats.wnba.com answers HTTP 403 to this datacenter IP"
    (tmp_path / "wnba" / "f.yaml").write_text(yaml.safe_dump({"schema": "f", "columns": [], "unverified": reason}))
    assert generate._autodoc_unverified("wnba", "f") == reason
    assert generate._autodoc_unverified("wnba", "absent") == ""


@pytest.mark.parametrize(
    ("annotation", "frame"),
    [
        ("pl.DataFrame", True),
        ("Union[pl.DataFrame, pd.DataFrame]", True),
        ("pl.DataFrame | dict[str, Any]", True),
        ("'pl.DataFrame | pd.DataFrame'", True),
        ("dict[str, pl.DataFrame]", False),
        ("dict[str, pl.DataFrame] | dict[str, pd.DataFrame] | dict[str, Any]", False),
        ("'dict[str, Union[pl.DataFrame, pd.DataFrame]]'", False),
        ("list[pl.DataFrame]", False),
        ("int", False),
        (None, False),
    ],
)
def test_only_a_top_level_frame_counts_as_a_dataframe_return(annotation, frame):
    assert generate._returns_a_frame(annotation) is frame
