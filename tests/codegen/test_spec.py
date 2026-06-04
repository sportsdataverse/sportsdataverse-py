from pathlib import Path

from tools.codegen import spec

ENDPOINTS = Path("tools/codegen/endpoints")


def test_load_parameters_registry():
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    p = params["limit"]
    assert p.python_name == "limit"
    assert p.api == "limit"
    assert p.default == 500


def test_load_leagues_resolves_hosts_and_scopes():
    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    assert cfg.hosts["site_v2"].startswith("https://site.api.espn.com")
    nba = next(lg for lg in cfg.leagues if lg.prefix == "nba")
    assert nba.sport == "basketball" and nba.league == "nba"
    assert "universal" in nba.scopes


def test_load_espn_api_resolves_param_keys_and_validates_path_tokens():
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    api = spec.load_espn_api(ENDPOINTS / "espn_site_v2.yaml", params)
    sb = next(e for e in api.endpoints if e.short == "scoreboard")
    assert sb.scope == "universal"
    assert sb.parser == "parse_scoreboard"
    names = {qp.python_name for qp in sb.query_params}
    assert {"dates", "limit", "season_type"} <= names
    st = next(qp for qp in sb.query_params if qp.python_name == "season_type")
    assert st.api == "seasontype"


def test_validate_rejects_unknown_param_key(tmp_path):
    bad = tmp_path / "bad.yaml"
    bad.write_text(
        "api: x\nhost: site_v2\nname_pattern: 'espn_{prefix}_{short}'\n"
        "endpoints:\n  - short: foo\n    path: '/{sport}/{league}/foo'\n"
        "    params: [does_not_exist]\n",
        encoding="utf-8",
    )
    params = spec.load_parameters(ENDPOINTS / "parameters.yaml")
    try:
        spec.load_espn_api(bad, params)
        raise AssertionError("expected SpecError")
    except spec.SpecError as e:
        assert "does_not_exist" in str(e)
