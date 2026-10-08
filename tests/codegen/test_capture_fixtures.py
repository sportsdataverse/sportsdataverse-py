"""The fixture-capture CLI: path conventions, the missing-schema list, and failure handling."""

from __future__ import annotations

import json

from tools.codegen import capture_fixtures as cf


def test_missing_schema_endpoints_lists_endpoint_shorts():
    missing = cf.missing_schema_endpoints("espn_core_v2")
    assert all(isinstance(s, str) for s in missing)
    assert "scoreboard" not in cf.missing_schema_endpoints("espn_site_v2")  # it has a returns_schema


def test_representative_league_prefers_the_big_leagues_and_can_reach_the_endpoint():
    lg = cf.representative_league("espn_core_v2", "seasons")
    assert lg == "nba"
    assert cf._views("espn_core_v2")["seasons"][lg]


def test_fixture_path_for_an_espn_api_is_short_league_json():
    p = cf.fixture_path("espn_core_v2", "venues", "nba")
    assert (p.parent.name, p.name) == ("espn", "venues_nba.json")


def test_fixture_path_for_a_flat_api_is_under_its_own_dir():
    p = cf.fixture_path("fox_api", "scorechip")
    assert (p.parent.name, p.name) == ("fox_api", "scorechip.json")


def test_capture_is_a_no_op_under_dry_run(tmp_path, monkeypatch):
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)
    ok, reason = cf.capture("espn_core_v2", "venues", "nba", dry_run=True)
    assert ok and reason.startswith("dry-run")
    assert not list(tmp_path.rglob("*.json"))


def test_a_failed_capture_writes_nothing_and_names_the_cause(tmp_path, monkeypatch):
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)

    def boom(*a, **kw):
        raise RuntimeError("403 Forbidden")

    monkeypatch.setattr(cf, "_call", boom)
    ok, reason = cf.capture("espn_core_v2", "venues", "nba")
    assert not ok and "403 Forbidden" in reason
    assert not list(tmp_path.rglob("*.json"))


def test_an_empty_payload_is_a_failure_not_an_empty_fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)
    monkeypatch.setattr(cf, "_call", lambda *a, **kw: {})
    ok, reason = cf.capture("espn_core_v2", "venues", "nba")
    assert not ok and "empty" in reason
    assert not list(tmp_path.rglob("*.json"))


def test_capture_writes_lf_json(tmp_path, monkeypatch):
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)
    monkeypatch.setattr(cf, "_call", lambda *a, **kw: {"items": [{"$ref": "x"}]})
    ok, _ = cf.capture("espn_core_v2", "venues", "nba")
    raw = (tmp_path / "espn" / "venues_nba.json").read_bytes()
    assert ok and b"\r\n" not in raw
    assert json.loads(raw) == {"items": [{"$ref": "x"}]}


def test_main_dry_run_exits_zero_and_lists_each_endpoint(capsys):
    assert cf.main(["--api", "espn_web_v3", "--endpoints", "athlete_stats", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "dry-run" in out and "capture_fixtures:" in out


def test_an_auto_espn_schema_is_written_generic_from_its_one_capture(tmp_path, monkeypatch):
    """A captured ESPN endpoint whose returns_schema is its own short gets schemas/<short>.yaml,
    parsed by its wrapper's own parser, so every league page falls back to it."""
    import yaml

    from tools.codegen import generate

    fix, out = tmp_path / "fix", tmp_path / "schemas"
    fix.mkdir()
    (fix / "venues_nba.json").write_text(json.dumps({"items": [{"$ref": "http://x/1"}, {"$ref": "http://x/2"}]}))
    monkeypatch.setattr(generate, "_auto_espn_schemas", lambda: {"venues": generate._espn_parser("venues")})
    assert generate._write_auto_espn_schemas(fix, out) == 1
    doc = yaml.safe_load((out / "venues.yaml").read_text())
    assert doc["kind"] == "dataframe" and [c["name"] for c in doc["columns"]] == ["$ref"]


def test_an_auto_espn_schema_with_no_capture_writes_nothing(tmp_path, monkeypatch):
    from tools.codegen import generate

    (tmp_path / "fix").mkdir()
    monkeypatch.setattr(generate, "_auto_espn_schemas", lambda: {"venues": generate._espn_parser("venues")})
    assert generate._write_auto_espn_schemas(tmp_path / "fix", tmp_path / "schemas") == 0
    assert not (tmp_path / "schemas").exists()


def test_a_failed_espn_capture_is_retried_in_the_next_league(tmp_path, monkeypatch, capsys):
    """season_group's example group 80 is college football: the NBA call finds nothing, CFB answers."""
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)
    seen = []

    def call(api, short, league):
        seen.append(league)
        if league == "nba":
            raise RuntimeError("NoDataError: No data found")
        return {"id": "80"}

    monkeypatch.setattr(cf, "_call", call)
    monkeypatch.setattr(cf, "_candidate_leagues", lambda api, short, limit=3: ["nba", "cfb", "nfl"])
    assert cf.main(["--api", "espn_core_v2", "--endpoints", "season_group", "--sleep", "0"]) == 0
    assert seen == ["nba", "cfb"]
    assert (tmp_path / "espn" / "season_group_cfb.json").exists()
    assert "1 captured, 0 skipped" in capsys.readouterr().out


def test_every_espn_wrapper_calls_the_parser_the_registry_names():
    """Generated wrappers call the endpoint YAML's ``parser:`` by name; ENDPOINT_PARSERS is what
    the schema builder and the parsed-docs use. If they disagree the docs describe a parser the
    wrapper never runs (transactions once called parse_items while the registry said otherwise)."""
    from sportsdataverse._common_espn_parsers import ENDPOINT_PARSERS
    from tools.codegen import generate, spec

    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    bad = [
        f"{api}/{ep.short}: yaml={ep.parser} registry={ENDPOINT_PARSERS[ep.short].__name__}"
        for api in generate.ESPN_APIS
        for ep in spec.load_espn_api(generate.ENDPOINTS / f"{api}.yaml", params).endpoints
        if ep.parser and ep.short in ENDPOINT_PARSERS and ep.parser != ENDPOINT_PARSERS[ep.short].__name__
    ]
    assert not bad, "\n".join(bad)


def test_register_native_scopes_to_its_own_api_block(tmp_path, monkeypatch):
    """Two APIs may share an endpoint short, and `fox_api:` appears inside `wbb_fox_api:`."""
    import yaml

    m = tmp_path / "native_fixture_map.yaml"
    m.write_text("wbb_fox_api:\n  scoreboard.json: scoreboard\nfox_api:\n  other.json: other\n")
    monkeypatch.setattr(cf, "NATIVE_MAP", m)
    cf._register_native("fox_api", "scoreboard", "scoreboard.json")
    cf._register_native("fox_api", "scoreboard", "scoreboard.json")  # idempotent
    cf._register_native("new_api", "x", "x.json")
    doc = yaml.safe_load(m.read_text())
    assert doc["fox_api"] == {"scoreboard.json": "scoreboard", "other.json": "other"}
    assert doc["wbb_fox_api"] == {"scoreboard.json": "scoreboard"}
    assert doc["new_api"] == {"x.json": "x"}


def test_an_unserializable_payload_is_a_reported_failure_not_a_crash(tmp_path, monkeypatch):
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)
    monkeypatch.setattr(cf, "_call", lambda *a, **kw: {"a": object()})
    ok, reason = cf.capture("espn_core_v2", "venues", "nba")
    assert not ok and "TypeError" in reason
    assert not list(tmp_path.rglob("*.json"))


def test_a_failure_in_every_league_reports_the_representative_leagues_cause(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cf, "FIXTURES", tmp_path)

    def call(api, short, league):
        raise RuntimeError(f"boom in {league}")

    monkeypatch.setattr(cf, "_call", call)
    monkeypatch.setattr(cf, "_candidate_leagues", lambda api, short, limit=8: ["nba", "cfb", "wch"])
    cf.main(["--api", "espn_core_v2", "--endpoints", "season_group", "--sleep", "0"])
    out = capsys.readouterr().out
    assert "[nba] RuntimeError: boom in nba" in out and "2 more league(s)" in out and "boom in wch" not in out
