"""Red-path tests for every ``tools/codegen/generate.py --check`` gate (A-M1).

A gate that is only ever run on a clean tree is a gate nobody has seen fail. Each test
below builds the stale state in a COPY of the repo (``tree``: every ``sportsdataverse``
module, the committed staging tree and the schemas the render reads), points
``generate`` at the copy, runs one gate and asserts it goes red naming the file. The
real checkout is never written. The docs gate gets a synthetic corpus instead of the
~40 s docs render: what is under test is its comparison, and the clean-tree
``--check`` run (``test_codegen_drift_clean``) already pins the real corpus.

Open defects are pinned ``xfail(strict=True)`` with their finding id, so the fix turns
them XPASS (red) and forces the marker's removal.
"""

from __future__ import annotations

import ast
import contextlib
import datetime
import functools
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Callable, Iterator

import pytest

from tests.codegen.conftest import CLEARED_GENERATE_CACHES
from tools.codegen import generate, spec

pytestmark = pytest.mark.xdist_group("codegen_render")

ROOT = generate.ROOT


def _py_only(src: str, names: list[str]) -> list[str]:
    return [n for n in names if n == "__pycache__" or (not (Path(src) / n).is_dir() and not n.endswith(".py"))]


@pytest.fixture(scope="module")
def tree(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A copy of the repo files the stale gates read or compare, laid out like the repo."""
    root = tmp_path_factory.mktemp("repo")
    shutil.copy2(ROOT / "pyproject.toml", root / "pyproject.toml")  # ruff finds the repo's format config
    shutil.copytree(ROOT / "sportsdataverse", root / "sportsdataverse", ignore=_py_only)
    shutil.copytree(ROOT / "tools/codegen/_generated", root / "tools/codegen/_generated", ignore=_py_only)
    shutil.copytree(ROOT / "tools/codegen/schemas", root / "tools/codegen/schemas")
    return root


@pytest.fixture
def in_tree(tree: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every path the gates read at the copy; the inputs (ENDPOINTS, templates) stay real."""
    monkeypatch.setattr(generate, "ROOT", tree)
    monkeypatch.setattr(generate, "LIVE", tree / "sportsdataverse")
    monkeypatch.setattr(generate, "OUT", tree / "tools/codegen/_generated")
    monkeypatch.setattr(generate, "DOCS", tree / "docs/docs")
    return tree


@contextlib.contextmanager
def _restored(path: Path) -> Iterator[Path]:
    """Yield ``path``; put its original bytes back (or remove it) afterwards."""
    before = path.read_bytes() if path.exists() else None
    try:
        yield path
    finally:
        if before is None:
            path.unlink(missing_ok=True)
        else:
            path.write_bytes(before)


def _rel(p: str) -> str:
    return str(Path(p))  # the gates report ``str(path.relative_to(ROOT))``: OS separators


# One real generated file per gate. The flat one is a grouped league (soccer/mls) so the
# nested destination is exercised, and the live set includes a sport-group container init.
_GATES: dict[str, tuple[Callable[[], list[str]], list[str]]] = {
    "live": (
        lambda: generate._live_stale(),
        ["sportsdataverse/nba/nba_espn_ext.py", "sportsdataverse/soccer/epl/epl_espn_ext.py"],
    ),
    "live_container_init": (lambda: generate._live_stale(), ["sportsdataverse/soccer/__init__.py"]),
    "parsed": (lambda: generate._parsed_stale(), ["sportsdataverse/parsed/nba.py"]),
    "flat": (
        lambda: generate._flat_stale(),
        ["sportsdataverse/nba/nba_stats.py", "sportsdataverse/soccer/mls/mls_api.py"],
    ),
    "loaders": (lambda: generate._loaders_stale(), ["sportsdataverse/nba/nba_loaders.py"]),
}


@pytest.mark.parametrize("gate", sorted(_GATES))
def test_stale_gate_is_clean_on_the_copied_tree(in_tree, gate):
    """The copy is faithful: every gate is green on it before anything is mutated."""
    run, _ = _GATES[gate]
    assert run() == []


@pytest.mark.parametrize(("gate", "rel"), [(g, r) for g, (_, rels) in sorted(_GATES.items()) for r in rels])
def test_red_path_stale_gate_lists_a_mutated_file(in_tree, gate, rel):
    run, _ = _GATES[gate]
    with _restored(in_tree / rel) as f:
        f.write_text(f.read_text(encoding="utf-8") + "\n# hand edit\n", encoding="utf-8", newline="\n")
        assert run() == [_rel(rel)]


@pytest.mark.parametrize(("gate", "rel"), [(g, rels[0]) for g, (_, rels) in sorted(_GATES.items())])
def test_red_path_stale_gate_lists_a_deleted_file(in_tree, gate, rel):
    run, _ = _GATES[gate]
    with _restored(in_tree / rel) as f:
        f.unlink()
        assert run() == [_rel(rel)]


def test_red_path_staging_check_fails_on_a_mutated_and_a_deleted_module(in_tree, capsys):
    out = in_tree / "tools/codegen/_generated"
    assert generate.check() == 0
    with _restored(out / "nba_espn_ext.py") as f, _restored(out / "wnba_espn_ext.py") as gone:
        f.write_text(f.read_text(encoding="utf-8") + "\nX = 1\n", encoding="utf-8", newline="\n")
        gone.unlink()
        assert generate.check() == 1
    err = capsys.readouterr().err
    assert "stale/missing generated files: nba_espn_ext.py, wnba_espn_ext.py" in err


def test_red_path_docs_stale_lists_mutated_deleted_and_orphan_pages(in_tree, monkeypatch):
    rendered = {"nba/index.md": "# NBA\n", "nba/reference/a.md": "a\n", "reference/parameters.md": "p\n"}
    monkeypatch.setattr(generate, "_generated_docs_roots", lambda: {"nba", "reference"})
    docs = in_tree / "docs/docs"
    try:
        for rel, body in rendered.items():
            (docs / rel).parent.mkdir(parents=True, exist_ok=True)
            (docs / rel).write_text(body, encoding="utf-8", newline="\n")
        (docs / "intro.md").write_text("hand-written, outside the generated roots\n", encoding="utf-8")
        assert generate._docs_stale(rendered) == []
        (docs / "nba/index.md").write_text("# edited\n", encoding="utf-8")
        (docs / "reference/parameters.md").unlink()
        (docs / "nba/reference/old.md").write_text("left behind\n", encoding="utf-8")
        assert generate._docs_stale(rendered) == [
            "nba/index.md",
            "nba/reference/old.md (orphan)",
            "reference/parameters.md",
        ]
    finally:
        shutil.rmtree(docs, ignore_errors=True)


def test_red_path_coverage_gaps_lists_an_undocumented_name(monkeypatch):
    per_league = {lg: set() for lg in generate._COVERAGE_LEAGUES}
    per_league["nba"] = {"espn_nba_scoreboard", "zz_never_documented_fn"}
    monkeypatch.setattr(generate, "_coverage_scope_names", lambda: (per_league, set()))
    monkeypatch.setattr(generate, "_coverage_allowlist", lambda: {})
    assert generate._coverage_gaps() == [("nba", ["zz_never_documented_fn"])]


def test_red_path_is_documented_needs_a_reference_block_not_a_word_mention():
    corpus = "| column | type | description |\n|---|---|---|\n| source | str | rows come from load_pbp |\n"
    assert not generate._is_documented("load_pbp", corpus)


# --------------------------------------------------------------------------------------
# main(["--check"]): every gate's verdict reaches the exit code
# --------------------------------------------------------------------------------------

_CLEAN = {
    "check": 0,
    "_live_stale": [],
    "_parsed_stale": [],
    "_flat_stale": [],
    "_loaders_stale": [],
    "_docs_stale": [],
    "_coverage_gaps": [],
    "_source_gaps": [],
    "_returns_gaps": [],
}
_RED = {
    "check": (1, ""),
    "_live_stale": (["sportsdataverse/x/x_espn_ext.py"], "stale live files: sportsdataverse/x/x_espn_ext.py"),
    "_parsed_stale": (["sportsdataverse/parsed/x.py"], "stale parsed files: sportsdataverse/parsed/x.py"),
    "_flat_stale": (["sportsdataverse/x/x_api.py"], "stale flat-API files: sportsdataverse/x/x_api.py"),
    "_loaders_stale": (["sportsdataverse/x/x_loaders.py"], "stale loader files: sportsdataverse/x/x_loaders.py"),
    "_docs_stale": (["x/index.md"], "stale doc files: x/index.md"),
    "_coverage_gaps": ([("x", ["fn_a"])], "undocumented user-facing functions: fn_a"),
    "_source_gaps": ([("x", "fn_b", "why")], "public functions with no single source: x.fn_b: why"),
    "_returns_gaps": ([("x", "fn_c")], "public functions with no documented return: x.fn_c"),
}


def _main_check_gates() -> set:
    """The gate calls inside ``main()``'s ``if args.check:`` block, read from its AST."""
    import inspect
    import textwrap

    main = ast.parse(textwrap.dedent(inspect.getsource(generate.main))).body[0]
    block = next(n for n in ast.walk(main) if isinstance(n, ast.If) and ast.unparse(n.test) == "args.check")
    return {
        n.func.id
        for n in ast.walk(block)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and hasattr(generate, n.func.id)
    }


def test_main_check_rc_covers_every_gate_main_runs():
    """A tenth gate added to ``main()`` must join ``_CLEAN``/``_RED`` (or it runs for real, unaggregated, here)."""
    assert _main_check_gates() == set(_CLEAN) == set(_RED)


def _patch_gates(monkeypatch: pytest.MonkeyPatch, red: str = "") -> None:
    for name, clean in _CLEAN.items():
        value = _RED[name][0] if name == red else clean
        monkeypatch.setattr(generate, name, lambda *a, _v=value, **k: _v)


def test_main_check_rc_is_zero_when_every_gate_is_clean(monkeypatch, capsys):
    _patch_gates(monkeypatch)
    assert generate.main(["--check"]) == 0
    assert "all generated files current" in capsys.readouterr().out


@pytest.mark.parametrize("red", sorted(_RED))
def test_main_check_rc_is_one_when_any_single_gate_is_red(monkeypatch, capsys, red):
    _patch_gates(monkeypatch, red)
    assert generate.main(["--check"]) == 1
    out, err = capsys.readouterr()
    assert "all generated files current" not in out
    assert _RED[red][1] in err


# --------------------------------------------------------------------------------------
# open gate holes (A-L3, A-N6, A-L9), and the per-run temp dirs (A-L2)
# --------------------------------------------------------------------------------------

_ORPHANS = {
    "check": ("tools/codegen/_generated/zz_espn_ext.py", "tools/codegen/_generated/nba_espn_ext.py"),
    "live": ("sportsdataverse/zz/zz_espn_ext.py", "sportsdataverse/nba/nba_espn_ext.py"),
    "parsed": ("sportsdataverse/parsed/zz.py", "sportsdataverse/parsed/nba.py"),
    "flat": ("sportsdataverse/nba/zz_stats.py", "sportsdataverse/nba/nba_stats.py"),
    "loaders": ("sportsdataverse/zz/zz_loaders.py", "sportsdataverse/nba/nba_loaders.py"),
}


def _red(gate: str) -> bool:
    if gate == "check":
        return generate.check() != 0
    return bool(_GATES[gate][0]())


@pytest.mark.parametrize("gate", sorted(_ORPHANS))
@pytest.mark.xfail(
    strict=True, raises=AssertionError, reason="A-L3: only _docs_stale reports an orphaned generated module; task 3.9"
)
def test_red_path_stale_gate_reports_an_orphaned_generated_module(in_tree, gate):
    orphan, model = (in_tree / p for p in _ORPHANS[gate])
    try:
        orphan.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(model, orphan)
        assert _red(gate)
    finally:
        orphan.unlink(missing_ok=True)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="A-N6: _euroleague_schemas.py (gen_euroleague.py) is owned by no --check gate; task 3.9",
)
def test_red_path_flat_stale_owns_euroleague_schemas(in_tree):
    with _restored(in_tree / "sportsdataverse/euroleague/_euroleague_schemas.py") as f:
        f.write_text(f.read_text(encoding="utf-8") + "\nX = 1\n", encoding="utf-8", newline="\n")
        assert generate._flat_stale()


@pytest.mark.xfail(
    strict=True, raises=pytest.fail.Exception, reason="A-L9: _ruff_format_dir ignores ruff's exit code; task 4.9"
)
def test_red_path_ruff_failure_is_raised(tmp_path):
    shutil.copy2(ROOT / "pyproject.toml", tmp_path / "pyproject.toml")
    (tmp_path / "broken.py").write_text("def f(:\n    pass\n", encoding="utf-8")
    with pytest.raises(Exception):  # noqa: B017 -- any loud failure; today it returns None
        generate._ruff_format_dir(tmp_path)


_FIXED_TMP_NAMES = ["_check_tmp", "_check_live_tmp", "_check_parsed_tmp", "_check_flat_tmp", "_check_loaders_tmp"]


def test_stale_gates_never_delete_another_runs_tmp_dir(in_tree):
    """A-L2: a concurrent ``--check`` run's temp dir survives this run, and this run leaves none."""
    out = in_tree / "tools/codegen/_generated"
    planted = [out / name / "busy.txt" for name in _FIXED_TMP_NAMES]
    try:
        for p in planted:
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("another run is using this dir\n", encoding="utf-8")
        assert generate.check() == 0
        for gate in ("live", "parsed", "flat", "loaders"):
            assert _GATES[gate][0]() == []
        assert all(p.exists() for p in planted), [str(p) for p in planted if not p.exists()]
        assert sorted(d.name for d in out.iterdir() if d.is_dir() and d.name.startswith("_check_")) == sorted(
            _FIXED_TMP_NAMES
        )
    finally:
        for p in planted:
            shutil.rmtree(p.parent, ignore_errors=True)


def test_stale_gate_runs_that_overlap_both_stay_green(in_tree, monkeypatch):
    """A-L2, for ANY fixed temp-dir name: a second ``check()`` starts and finishes inside the first one's render.

    With a shared fixed dir the nested run deletes the outer run's rendered files, and the outer run then
    reports them stale (or crashes); with a per-run ``mkdtemp`` both are green.
    """
    real = generate._render_all
    nested: list = []

    def render_and_overlap() -> dict:
        out = real()
        if not nested:  # the outer run's temp dir exists now; mark first, so the nested run does not recurse
            nested.append(None)
            nested[0] = generate.check()
        return out

    monkeypatch.setattr(generate, "_render_all", render_and_overlap)
    assert generate.check() == 0
    assert nested == [0]


# --------------------------------------------------------------------------------------
# star-import wiring, read by AST (A-L53)
# --------------------------------------------------------------------------------------


def _star_imports(init: Path) -> set[str]:
    if not init.exists():
        return set()
    return {
        node.module or ""
        for node in ast.walk(ast.parse(init.read_text(encoding="utf-8")))
        if isinstance(node, ast.ImportFrom) and [a.name for a in node.names] == ["*"]
    }


def _unwired(live: Path) -> list[str]:
    """``pkg/__init__.py -> dotted`` for every generated module its league package does not star-import."""
    expected = []
    for lg in spec.load_leagues(generate.ENDPOINTS / "leagues.yaml").leagues:
        parts = [lg.group, lg.prefix] if lg.group else [lg.prefix]
        expected.append((Path(*parts), ".".join(["sportsdataverse", *parts, f"{lg.prefix}_espn_ext"])))
    for lg in sorted(generate._render_loaders_all()):
        expected.append((Path(lg), f"sportsdataverse.{lg}.{lg}_loaders"))
    return [
        f"{pkg.as_posix()}/__init__.py -> {dotted}"
        for pkg, dotted in expected
        if dotted not in _star_imports(live / pkg / "__init__.py")
    ]


def test_every_generated_module_is_star_imported_by_its_package():
    assert _unwired(generate.LIVE) == []


def test_red_path_a_commented_star_import_is_unwired(tree):
    init = tree / "sportsdataverse/nba/__init__.py"
    with _restored(init):
        text = init.read_text(encoding="utf-8")
        line = "from sportsdataverse.nba.nba_espn_ext import *"
        assert line in text
        init.write_text(text.replace(line, "# " + line), encoding="utf-8", newline="\n")
        assert _unwired(tree / "sportsdataverse") == ["nba/__init__.py -> sportsdataverse.nba.nba_espn_ext"]


# --------------------------------------------------------------------------------------
# the clean-tree subprocess run (moved here from test_mlb_statcast_codegen.py, A-N5)
# --------------------------------------------------------------------------------------


def test_codegen_drift_clean():
    r = subprocess.run(
        [sys.executable, "tools/codegen/generate.py", "--check"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr


# --------------------------------------------------------------------------------------
# render helpers only the byte drift gate pinned (A-L47), cache hygiene (A-L49),
# example-season pinning (A-L50)
# --------------------------------------------------------------------------------------

_SEASON_TYPE = spec.Param("season_type", "seasontype", choices=["reg", "post"])
_EP = spec.Endpoint(
    short="fake",
    path="/v1/fake",
    summary="Fetch a fake thing.",
    parser="parse_fake",
    query_params=[spec.Param("season", "season", description="The season.\nSecond line."), _SEASON_TYPE],
    example_args={"season_type": "reg"},
)
_NBA = spec.League(prefix="nba", sport="basketball", league="nba", scopes=["universal"])


def _doc(**kw) -> str:
    return generate._build_docstring(_EP, "basketball", "nba", "https://h", "https://h/v1/fake", "fn()", **kw)


def test_build_docstring_is_an_indented_block_that_compiles():
    doc = _doc()
    assert all(not ln or ln.startswith("    ") for ln in doc.splitlines())
    fn = ast.parse("def f():\n" + doc + "\n    pass\n").body[0]
    assert ast.get_docstring(fn).startswith("Fetch a fake thing.")
    assert "        season: The season. Second line." in doc.splitlines()  # an embedded newline is collapsed


def test_build_docstring_yaml_text_with_a_backslash_or_triple_quote_compiles_verbatim():
    """A-L40: YAML text was spliced unescaped, so a backslash warned and a triple quote ended the docstring."""
    import dataclasses
    import warnings

    # R901-3: a run of 4+ quotes must not leave three bare quotes after the escape
    summary = r'Match r"\d+" ids; a """quoted""" word, and """" four.'
    ep = dataclasses.replace(_EP, summary=summary)
    note = r"C:\path\to"
    doc = generate._build_docstring(ep, "basketball", "nba", "https://h", "", "fn()", doc_extras={"notes": [note]})
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        code = compile("def f():\n" + doc + "\n    pass\n", "<doc>", "exec")
    ns: dict = {}
    exec(code, ns)
    assert ns["f"].__doc__.startswith(summary)
    assert note in ns["f"].__doc__


def test_build_docstring_flags_change_only_what_they_document():
    assert "Bound to sport='basketball', league='nba'." in _doc()
    assert "Bound to sport" not in _doc(flat=True)
    assert "``league`` is a required argument" in _doc(league_param=True)
    assert "    headers: " in _doc(auth=True) and "    headers: " not in _doc()
    assert "-> dict of polars DataFrames" in _doc(returns_frames=True)
    assert "-> polars DataFrame" in _doc() and "dict of" not in _doc()


def test_build_docstring_vocab_raises_adds_only_the_errors_not_already_named():
    doc = _doc(vocab_raises=True, doc_extras={"raises": ["NoDataError: custom."]})
    raises = doc.split("Raises:")[1].split("Example:")[0]
    assert raises.count("NoDataError") == 1 and "custom." in raises
    assert "ValueError: The host answered 400 / 422" in raises and "AssetFetchError: The fetch failed" in raises
    assert "Raises:" not in _doc()


def test_build_docstring_example_import_and_trailing_notes():
    doc = _doc(doc_extras={"example_import": True, "notes": ["a note"]}, import_from="sportsdataverse.nba")
    assert "from sportsdataverse.nba import fn" in doc
    assert doc.index("Example:") < doc.index("Notes:")  # the reference renderer stops at Notes:


def test_endpoint_view_choice_checks_raise_before_the_request():
    view = generate._EndpointView(_EP, "espn_nba_fake", "https://h", _NBA)
    assert len(view.choice_checks) == 1
    ns: dict = {}
    exec("def f(season_type):\n    " + "\n    ".join(view.choice_checks) + "\n    return season_type\n", ns)  # noqa: S102
    assert ns["f"]("post") == "post"
    with pytest.raises(ValueError, match=r"season_type must be one of \('reg', 'post'\); got 'bad'"):
        ns["f"]("bad")


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        (
            "Quick start::\n\n    from x import f\n    f()\n\nSee Also:\n    * `a`_ -- b\n\n.. _a: https://a",
            "from x import f\nf()",
        ),
        ("    from x import f\n    f()", "from x import f\nf()"),
        ("    f()\nNotes:\n    blah", "f()"),
        ("Quick start::\n\n", ""),
    ],
)
def test_clean_example_strips_the_intro_and_trailing_prose(raw, want):
    assert generate._clean_example(raw) == want


def _sample(a: int, b: str = "x", *args, **kwargs) -> int:
    """Short line.

    Longer description.

    Args:
        a: The a value.
        b: The b value
            continued.

    Returns:
        int: The result.

    Example:
        Quick start::

            _sample(1)
    """
    return a


class _Klass:
    """A class.

    Args:
        z: zed.
    """

    def __init__(self, z: int) -> None:
        self.z = z

    def meth(self, q: int) -> int:
        """Do it.

        Args:
            q: cue.
        """
        return q


def test_doc_view_reads_params_returns_and_example():
    v = generate._doc_view(_sample)
    assert (v["short"], v["long"], v["returns"], v["example"], v["methods"]) == (
        "Short line.",
        "Longer description.",
        "The result.",
        "_sample(1)",
        [],
    )
    assert v["params"] == [  # *args/**kwargs dropped; annotation types; whitespace collapsed
        {"name": "a", "type": "int", "default": "", "description": "The a value."},
        {"name": "b", "type": "str", "default": "'x'", "description": "The b value continued."},
    ]


def test_doc_view_never_raises_and_lists_class_methods():
    assert generate._doc_view(lambda x: x)["params"] == [{"name": "x", "type": "", "default": "", "description": ""}]
    assert generate._doc_view(len)["short"]  # a builtin: signature and docstring still read
    v = generate._doc_view(_Klass)
    assert [p["name"] for p in v["params"]] == ["z"] and [m["name"] for m in v["methods"]] == ["meth"]


# Cleared by tests/codegen/conftest.py before and after every test: they read config a test monkeypatches.
_CLEARED_CACHES = set(CLEARED_GENERATE_CACHES)
# Kept warm across tests on purpose: each is a pure function of committed files (or of its own
# arguments), and clearing them would re-pay the ~40 s cold render in every codegen test.
_KEPT_CACHES = {
    "_autodoc_example_args",
    "_autodoc_return_columns",
    "_companions_map",
    "_corpus_headings",
    "_docs_corpus",
    "_docstring_param_descs",
    "_highlights_map",
    "_league_module",
    "_loader_schemas",
    "_manual_col_descs",
    "_no_r_fallback_schemas",
    "_r_col_descs",
    "_r_exports",
    "_r_parity_aliases",
    "_return_table",
    "_schema_doc",
}


def test_every_generate_cache_is_classified_cleared_or_kept():
    """A-L49: a new ``lru_cache`` reader must be classified, so a monkeypatched input cannot leak by test order."""
    caches = {n for n, v in vars(generate).items() if isinstance(v, functools._lru_cache_wrapper)}
    assert caches == _CLEARED_CACHES | _KEPT_CACHES


class _FrozenDate(datetime.date):
    today_value = datetime.date(2026, 1, 15)

    @classmethod
    def today(cls) -> datetime.date:
        return cls.today_value


def _transform_endpoints() -> list[tuple[str, spec.Endpoint, str]]:
    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    out = []
    for stem, _prefix in generate.FLAT_APIS:
        y = generate.ENDPOINTS / f"{stem}.yaml"
        if y.exists():
            api = spec.load_flat_api(y, params)
            out += [(api.host, ep, api.getter_module) for ep in api.endpoints]
    for name in generate.ESPN_APIS:
        api = spec.load_espn_api(generate.ENDPOINTS / f"{name}.yaml", params)
        out += [(api.host, ep, "sportsdataverse._codegen_runtime") for ep in api.endpoints]
    return [(h, ep, g) for h, ep, g in out if any(p.transform for p in [*ep.path_params, *ep.query_params])]


def _example_urls(monkeypatch, endpoints, day: datetime.date) -> list[str]:
    from sportsdataverse.nba import nba_stats_runtime

    monkeypatch.setattr(_FrozenDate, "today_value", day)
    monkeypatch.setattr(nba_stats_runtime, "date", _FrozenDate)
    return [generate._example_url(host, ep, "basketball", "nba", getter) for host, ep, getter in endpoints]


def test_example_urls_do_not_depend_on_the_render_date(monkeypatch):
    """A-L50: every example pins the arguments a transform would otherwise date at render time."""
    endpoints = _transform_endpoints()
    assert len(endpoints) > 100
    winter = _example_urls(monkeypatch, endpoints, datetime.date(2026, 1, 15))
    summer = _example_urls(monkeypatch, endpoints, datetime.date(2027, 7, 15))
    assert [(ep.short, a, b) for (_, ep, _), a, b in zip(endpoints, winter, summer) if a != b] == []


def test_red_path_an_unpinned_example_season_depends_on_the_date(monkeypatch):
    import dataclasses

    host, ep, getter = next(
        (h, e, g)
        for h, e, g in _transform_endpoints()
        if any(p.transform == "season_latest_with_data" and p.python_name in e.example_args for p in e.query_params)
    )
    season = next(p.python_name for p in ep.query_params if p.transform == "season_latest_with_data")
    unpinned = dataclasses.replace(ep, example_args={k: v for k, v in ep.example_args.items() if k != season})
    winter, summer = (
        _example_urls(monkeypatch, [(host, unpinned, getter)], day)[0]
        for day in (datetime.date(2026, 1, 15), datetime.date(2027, 7, 15))
    )
    assert winter != summer


def test_a_full_run_renders_parsed_and_docs_in_a_fresh_interpreter(monkeypatch):
    """A-L10: one run must converge. The parsed aliases and the docs read the package's live
    docstrings, which this process imported before the module stages rewrote them, so they run
    in a child interpreter, after every module stage."""
    calls: list = []
    for name in ("build", "build_live", "build_flat_live", "build_loaders_live"):
        monkeypatch.setattr(generate, name, lambda _n=name: calls.append(_n) or [])
    for name in ("build_parsed_live", "build_docs"):
        monkeypatch.setattr(generate, name, lambda _n=name: pytest.fail(f"{_n} ran in the stale process"))
    monkeypatch.setattr(generate.subprocess, "run", lambda cmd, **kw: calls.append(cmd[1:]))
    assert generate.main([]) == 0
    assert calls == [
        "build",
        "build_live",
        "build_flat_live",
        "build_loaders_live",
        [str(Path(generate.__file__).resolve()), "--parsed-and-docs"],
    ]


def test_parsed_and_docs_flag_runs_only_those_two_stages(monkeypatch):
    calls: list = []
    for name in ("build_parsed_live", "build_docs"):
        monkeypatch.setattr(generate, name, lambda _n=name: calls.append(_n) or [])
    for name in ("build", "build_live", "build_flat_live", "build_loaders_live"):
        monkeypatch.setattr(generate, name, lambda _n=name: pytest.fail(f"{_n} ran"))
    assert generate.main(["--parsed-and-docs"]) == 0
    assert calls == ["build_parsed_live", "build_docs"]
