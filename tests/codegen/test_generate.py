import importlib
import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tools.codegen import generate

OUT = Path("tools/codegen/_generated")


def _load(mod_path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, mod_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_build_emits_one_module_per_league():
    generate.build()
    for prefix in ("nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "nhl"):
        assert (OUT / f"{prefix}_espn_ext.py").exists()


def test_generated_nba_module_imports_and_exposes_functions():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba")
    assert hasattr(mod, "espn_nba_scoreboard")
    assert hasattr(mod, "espn_nba_teams_site")
    assert hasattr(mod, "espn_nba_standings")
    assert "espn_nba_scoreboard" in mod.__all__


def test_generated_function_builds_correct_url_and_strips_none():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba2")

    class FakeResp:
        def json(self):
            return {"events": []}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        # return_parsed=False: this test is about URL/param construction, so we
        # want the raw payload passed through (0.0.54 default is the DataFrame).
        out = mod.espn_nba_scoreboard(dates="20240115", return_parsed=False)
    assert out == {"events": []}
    called = dl.call_args.kwargs
    assert called["url"] == "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    assert called["params"] == {"dates": "20240115", "limit": 500}  # None week/seasontype/groups stripped


def test_returns_schema_resolves_for_authored_endpoints():
    from tools.codegen import spec

    for name in ("scoreboard", "teams", "standings", "leaders", "team_roster"):
        d = spec._read_yaml(Path(f"tools/codegen/schemas/{name}.yaml"))
        assert d["columns"], f"{name} schema has no columns"
        assert all(c["description"] for c in d["columns"]), f"{name} has blank descriptions"


def test_summary_schema_has_frames():
    from tools.codegen import spec

    d = spec._read_yaml(Path("tools/codegen/schemas/summary.yaml"))
    assert d["kind"] == "frames"
    assert any(f["section"] == "boxscore_player" for f in d["frames"])
    assert all(f["columns"] for f in d["frames"] if f["section"] in ("header", "game_info"))


def test_standings_uses_alt_host():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba3")

    class FakeResp:
        def json(self):
            return {}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        mod.espn_nba_standings(season=2024)
    assert dl.call_args.kwargs["url"] == "https://site.api.espn.com/apis/v2/sports/basketball/nba/standings"


# ===========================================================================
# refresh_autodoc_schemas -- prune guard
# ===========================================================================


def _autodoc_tree(tmp_path: Path) -> Path:
    """Three committed schemas: in-scope, removed, and hand-authored."""
    d = tmp_path / "autodoc" / "nfl"
    d.mkdir(parents=True)
    (d / "boom.yaml").write_text("schema: boom\nkind: dataframe\ncolumns: []\n", encoding="utf-8")
    (d / "removed.yaml").write_text("schema: removed\nkind: dataframe\ncolumns: []\n", encoding="utf-8")
    (d / "handmade.yaml").write_text("schema: handmade\nhand_authored: true\ncolumns: []\n", encoding="utf-8")
    return d


def test_autodoc_prune_keeps_in_scope_schema_whose_capture_failed(tmp_path, monkeypatch, capsys):
    """A failed capture must NOT delete a still-documented function's schema.

    "Did not capture this run" is not "no longer exists" -- a rate limit, an
    off-season endpoint or an unreachable host fails the call while the function
    stays in the autodoc set. Deleting on that condition silently drops a
    rendered Returns table, so only a name that left the set may be pruned.
    """
    d = _autodoc_tree(tmp_path)

    def boom():
        raise RuntimeError("simulated rate limit")

    stub = SimpleNamespace(boom=boom)
    monkeypatch.setattr(generate, "_AUTODOC_SCHEMA_DIR", tmp_path / "autodoc")
    monkeypatch.setattr(generate, "_autodoc_names_by_scope", lambda: {"nfl": ["boom"]})
    monkeypatch.setattr(generate, "_autodoc_example_args", lambda: {})
    monkeypatch.setattr(importlib, "import_module", lambda name: stub)

    assert generate.refresh_autodoc_schemas() == 0

    assert (d / "boom.yaml").exists(), "in-scope function whose capture failed was pruned"
    assert (d / "handmade.yaml").exists(), "hand-authored schema was pruned"
    assert not (d / "removed.yaml").exists(), "schema for a name outside the set was not pruned"

    out = capsys.readouterr().out
    assert "1 pruned" in out and "1 kept (in scope, capture failed)" in out


# ===========================================================================
# refresh_autodoc_schemas -- one capture is not the whole schema
# ===========================================================================


def _capture(tmp_path: Path, monkeypatch, committed, frame) -> dict:
    """Run the capture over one stubbed ``nfl.f`` returning ``frame``; return the written YAML."""
    import yaml

    d = tmp_path / "autodoc" / "nfl"
    d.mkdir(parents=True)
    if committed is not None:
        (d / "f.yaml").write_text(yaml.safe_dump(committed, sort_keys=False), encoding="utf-8")
    monkeypatch.setattr(generate, "_AUTODOC_SCHEMA_DIR", tmp_path / "autodoc")
    monkeypatch.setattr(generate, "_autodoc_names_by_scope", lambda: {"nfl": ["f"]})
    monkeypatch.setattr(generate, "_autodoc_example_args", lambda: {})
    monkeypatch.setattr(importlib, "import_module", lambda name: SimpleNamespace(f=lambda: frame))
    assert generate.refresh_autodoc_schemas() == 0
    return yaml.safe_load((d / "f.yaml").read_text(encoding="utf-8"))


def _cols(*pairs) -> list:
    return [{"name": n, "type": t, "description": ""} for n, t in pairs]


def test_autodoc_capture_retains_committed_columns_absent_from_fresh_frame(tmp_path, monkeypatch):
    """A column the live call did not return this time stays, in committed order.

    One call is one moment: an off-season scoreboard has no ``home_team_score``
    and a pre-tournament schedule no ``tournament_id``. Overwriting with that
    frame dropped 33 columns across 11 schemas on the 2026-09-17 capture and
    orphaned their rendered descriptions. New columns still append.
    """
    import polars as pl

    committed = {
        "schema": "f",
        "kind": "dataframe",
        "columns": _cols(("a", "integer"), ("b", "integer"), ("c", "character")),
    }
    out = _capture(tmp_path, monkeypatch, committed, pl.DataFrame({"a": [1], "c": ["x"], "d": [2.0]}))
    assert [c["name"] for c in out["columns"]] == ["a", "b", "c", "d"]
    assert out["columns"][3]["type"] == "double"


def test_autodoc_capture_keeps_committed_type_for_all_null_and_list_columns(tmp_path, monkeypatch):
    """An all-null fresh column carries no type evidence; a committed ``list`` beats
    the mapper's ``List(Int64)`` -> ``integer`` fold.

    ``rank_delta`` is null before the season and ``highlights`` is empty on most
    days; inferring from those rows wrote ``character`` over ``integer`` on 20
    columns. ``_pl_to_doc_type`` cannot express a list, so a fresh ``integer``
    for a committed ``list`` is the mapper, not a change.
    """
    import polars as pl

    committed = {"schema": "f", "kind": "dataframe", "columns": _cols(("x", "integer"), ("y", "list"))}
    frame = pl.DataFrame({"x": pl.Series([None, None], dtype=pl.Null), "y": [[1, 2], [3]]})
    out = _capture(tmp_path, monkeypatch, committed, frame)
    assert {c["name"]: c["type"] for c in out["columns"]} == {"x": "integer", "y": "list"}


def test_autodoc_capture_keeps_real_dtype_change(tmp_path, monkeypatch):
    """A populated fresh column with a different scalar type IS a change and wins."""
    import polars as pl

    committed = {"schema": "f", "kind": "dataframe", "columns": _cols(("z", "character"))}
    out = _capture(tmp_path, monkeypatch, committed, pl.DataFrame({"z": [1, None]}))
    assert out["columns"] == _cols(("z", "integer"))


def test_autodoc_capture_never_overwrites_hand_authored_schema(tmp_path, monkeypatch, capsys):
    """``hand_authored: true`` exempts a schema from the write, not just the prune (#483)."""
    import polars as pl

    committed = {"schema": "f", "kind": "dataframe", "hand_authored": True, "columns": _cols(("k", "integer"))}
    out = _capture(tmp_path, monkeypatch, committed, pl.DataFrame({"other": ["v"]}))
    assert out == committed
    assert "1 hand_authored kept" in capsys.readouterr().out


def test_autodoc_capture_summary_reports_thin_capture(tmp_path, monkeypatch, capsys):
    """The summary line must make a thin capture visible, like #483's kept count."""
    import polars as pl

    committed = {
        "schema": "f",
        "kind": "dataframe",
        "columns": _cols(("a", "integer"), ("b", "integer"), ("c", "list")),
    }
    frame = pl.DataFrame({"a": pl.Series([None], dtype=pl.Null), "c": [[1]], "n": [1]})
    _capture(tmp_path, monkeypatch, committed, frame)
    out = capsys.readouterr().out
    assert "1 captured" in out
    assert "1 columns retained (absent from capture)" in out
    assert "2 types preserved (all-null or list)" in out
    assert "autodoc merge nfl.f: 1 new, 1 retained, 2 types preserved" in out


def test_loader_schema_refresh_captures_the_richest_season(tmp_path, monkeypatch):
    """Capture the season with the most columns, not the newest one that resolves.

    Mid-season the newest asset is partial: in September 2026
    ``load_cfb_game_rosters`` had 73 columns for 2026 and 77 for 2025, so taking
    the newest season dropped the 2025-only ``draft_*`` columns from the docs.
    A tie goes to the newer season, and a season whose read raises is skipped.
    """
    import polars as pl
    import yaml

    from tools.codegen import spec

    cols = {
        # partial newest season: fewer columns than the full prior season
        "rich_2026.parquet": ["a", "b"],
        "rich_2025.parquet": ["a", "b", "draft_round"],
        "rich_2024.parquet": ["a", "b"],
        # tie between 2025 and 2023; 2026 and 2024 raise
        "tie_2025.parquet": ["a", "new"],
        "tie_2023.parquet": ["a", "old"],
        "tie_2022.parquet": ["a"],
    }

    def fake_schema(url):
        name = url.rsplit("/", 1)[-1]
        if name not in cols:
            raise OSError(f"404 {url}")
        return {c: pl.Int64 for c in cols[name]}

    rel = spec.ReleasesConfig(
        bases={"b": "https://example.invalid/"},
        loaders=[
            spec.Loader(fn="load_rich", league="cfb", base="b", url="rich_{season}.parquet", tag="t"),
            spec.Loader(fn="load_tie", league="cfb", base="b", url="tie_{season}.parquet", tag="t"),
        ],
    )
    monkeypatch.setattr(generate, "ENDPOINTS", tmp_path / "endpoints")
    monkeypatch.setattr(generate.spec, "load_releases", lambda path: rel)
    monkeypatch.setattr(pl, "read_parquet_schema", fake_schema)

    assert generate.refresh_loader_schemas() == 0

    written = yaml.safe_load((tmp_path / "schemas" / "loader_schemas.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in written["load_rich"]] == ["a", "b", "draft_round"], "richest season not captured"
    assert [c["name"] for c in written["load_tie"]] == ["a", "new"], "tie did not go to the newest season"


def test_loader_notes_reach_the_docstring_and_the_page():
    """A `notes:` caveat must render in BOTH generated surfaces.

    A coverage caveat is only useful where someone reads it. The docstring
    serves help() and IDEs; the reference page serves the docs site. Wiring one
    and not the other is the failure this guards -- the caveat looks documented
    while the audience that would act on it never sees it.
    """
    from tools.codegen import spec

    ld = spec.Loader(
        fn="load_demo_thing",
        league="mlb",
        base="sdv",
        url="demo/demo_{season}.parquet",
        tag="demo",
        min_season=1988,
        notes="``demo_field`` is absent before 2008 and only 45.7% populated in 2007.",
    )

    doc = generate._build_loader_docstring(ld)
    assert "Note:" in doc
    assert "45.7% populated in 2007" in doc
    # Note must precede Raises/Example so it is visible without scrolling.
    assert doc.index("Note:") < doc.index("Raises:")

    template = generate.render.ENV.get_template("loaders_page.md.jinja")
    page = template.render(
        prefix="mlb",
        sidebar_position=1,
        loaders=[
            {
                "fn": ld.fn,
                "notes": ld.notes,
                "tag": ld.tag,
                "tag_url": "",
                "url": "",
                "automation": {"repo": "", "workflow": ""},
                "return_table": "",
                "example_seasons": 2024,
            }
        ],
    )
    assert ":::caution Coverage" in page
    assert "45.7% populated in 2007" in page


def test_loader_without_notes_emits_no_empty_caveat_block():
    """No `notes:` must mean no Note section and no empty admonition."""
    from tools.codegen import spec

    ld = spec.Loader(
        fn="load_demo_plain",
        league="mlb",
        base="sdv",
        url="demo/demo_{season}.parquet",
        tag="demo",
    )
    assert "Note:" not in generate._build_loader_docstring(ld)

    template = generate.render.ENV.get_template("loaders_page.md.jinja")
    page = template.render(
        prefix="mlb",
        sidebar_position=1,
        loaders=[
            {
                "fn": ld.fn,
                "notes": "",
                "tag": ld.tag,
                "tag_url": "",
                "url": "",
                "automation": {"repo": "", "workflow": ""},
                "return_table": "",
                "example_seasons": 2024,
            }
        ],
    )
    assert ":::caution" not in page
