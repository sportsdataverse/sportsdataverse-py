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
