import importlib.util
from pathlib import Path
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
    assert hasattr(mod, "espn_nba_teams")
    assert hasattr(mod, "espn_nba_standings")
    assert "espn_nba_scoreboard" in mod.__all__


def test_generated_function_builds_correct_url_and_strips_none():
    generate.build()
    mod = _load(OUT / "nba_espn_ext.py", "_gen_nba2")

    class FakeResp:
        def json(self):
            return {"events": []}

    with patch("sportsdataverse._codegen_runtime.download", return_value=FakeResp()) as dl:
        out = mod.espn_nba_scoreboard(dates="20240115")
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
